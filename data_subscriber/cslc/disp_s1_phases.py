"""Phase model for processing-mode-annotated DISP-S1 consistent burst DBs.

An annotated consistent burst DB stores each frame's sensing_time_list as a
mapping of sensing time -> processing-mode label instead of a plain list:

    "sensing_time_list": {"2016-07-09T01:33:16": "historical_01", ...}

The labels partition a frame's timeline into contiguous phases:

- ``historical_NN``: full k-sized ministacks processed as historical batches
- ``forward_NN``: the sub-k remainder of a chunk, processed one date at a
  time through the forward pipeline
- ``no_run``: a chunk too short to bootstrap a ministack; never processed

``NN`` numbers the gap-separated *chunk* (consecutive sensing times further
apart than the labeler's gap threshold start a new chunk). Within a chunk the
historical phase precedes its forward phase and both share the chunk's
ordinal. ``no_run`` carries no ordinal, so consecutive unusable chunks appear
merged as a single ``no_run`` run and the first processed ordinal of a frame
may be greater than 1.

This module is pure: no settings, no I/O, no ES. Callers decide whether the
labels apply (feature gating) before asking for phases.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import dateutil.parser

NO_RUN_LABEL = "no_run"

_PROCESSED_LABEL_RE = re.compile(r"^(historical|forward)_(\d+)$")


class PhaseKind(Enum):
    HISTORICAL = "historical"
    FORWARD = "forward"
    NO_RUN = "no_run"


class PhaseValidationError(ValueError):
    """A frame's label sequence violates the labeler's contract."""


@dataclass(frozen=True)
class ProcessingPhase:
    label: str
    kind: PhaseKind
    ordinal: Optional[int]  # chunk number; None for no_run
    start_pos: int  # inclusive index into the frame's sorted sensing list
    end_pos: int  # exclusive
    is_new_lineage: bool  # historical phase that must start a fresh CCSLC lineage

    @property
    def length(self):
        return self.end_pos - self.start_pos


def parse_sensing_time_list(sensing_time_list):
    """Split a burst-DB sensing_time_list into (datetimes, labels).

    Accepts both formats: the legacy list of timestamp strings (labels come
    back None) and the annotated mapping of timestamp -> label. Pairs are
    sorted by parsed timestamp so labels stay aligned with the chronological
    order used everywhere else, even if the mapping's insertion order is not
    sorted.
    """
    if isinstance(sensing_time_list, dict):
        pairs = sorted(
            (dateutil.parser.isoparse(ts), label)
            for ts, label in sensing_time_list.items()
        )
        return [dt for dt, _ in pairs], [label for _, label in pairs]
    return sorted(dateutil.parser.isoparse(ts) for ts in sensing_time_list), None


def segment_phases(labels, k):
    """Segment a frame's label sequence into validated ProcessingPhases.

    ``k`` is the ministack size the labels were generated for (the annotated
    DB records it as metadata.processing_mode_params.batch_size).

    Raises PhaseValidationError on any violation of the labeler's contract.
    Callers are expected to quarantine the offending frame rather than let
    one bad frame stop a whole batch.
    """
    if not labels:
        return []

    runs = []  # [label, start_pos, length]
    for i, label in enumerate(labels):
        if runs and runs[-1][0] == label:
            runs[-1][2] += 1
        else:
            runs.append([label, i, 1])

    phases = []
    seen_processed = set()
    prev_ordinal = None
    have_processed = False
    for label, start, length in runs:
        if label == NO_RUN_LABEL:
            phases.append(ProcessingPhase(
                label=label, kind=PhaseKind.NO_RUN, ordinal=None,
                start_pos=start, end_pos=start + length, is_new_lineage=False))
            continue

        match = _PROCESSED_LABEL_RE.match(label)
        if not match:
            raise PhaseValidationError(
                f"unrecognized processing-mode label {label!r} at position {start}")
        if label in seen_processed:
            raise PhaseValidationError(
                f"label {label!r} recurs at position {start} after other labels")
        seen_processed.add(label)

        ordinal = int(match.group(2))
        if prev_ordinal is not None and ordinal < prev_ordinal:
            raise PhaseValidationError(
                f"chunk ordinal decreases at {label!r} (position {start}): "
                f"{prev_ordinal} -> {ordinal}")
        prev_ordinal = ordinal

        if match.group(1) == "historical":
            if length % k != 0:
                raise PhaseValidationError(
                    f"{label!r} has {length} dates, not a multiple of k={k}")
            phases.append(ProcessingPhase(
                label=label, kind=PhaseKind.HISTORICAL, ordinal=ordinal,
                start_pos=start, end_pos=start + length,
                is_new_lineage=have_processed))
            have_processed = True
        else:
            prev = phases[-1] if phases else None
            if (prev is None or prev.kind is not PhaseKind.HISTORICAL
                    or prev.ordinal != ordinal):
                raise PhaseValidationError(
                    f"{label!r} at position {start} must directly follow the "
                    f"historical phase of its own chunk")
            if not 1 <= length <= k - 1:
                raise PhaseValidationError(
                    f"{label!r} has {length} dates; forward phases hold 1..{k - 1}")
            phases.append(ProcessingPhase(
                label=label, kind=PhaseKind.FORWARD, ordinal=ordinal,
                start_pos=start, end_pos=start + length, is_new_lineage=False))

    return phases


def phase_for_position(phases, pos):
    """Return the phase containing sensing-list position ``pos``.

    Raises PhaseValidationError for positions outside the annotated range —
    callers handle the leading edge (positions past the last annotation)
    explicitly rather than processing unlabeled dates.
    """
    if phases and 0 <= pos < phases[-1].end_pos:
        for phase in phases:
            if phase.start_pos <= pos < phase.end_pos:
                return phase
    raise PhaseValidationError(
        f"position {pos} is outside the annotated range "
        f"[0, {phases[-1].end_pos if phases else 0})")


def lineage_start_pos(phases, pos):
    """Start position of the CCSLC lineage containing position ``pos``.

    The lineage boundary is the start of the containing chunk's historical
    phase: a forward phase chains onto the compressed CSLCs of its own
    chunk's historical stacks. Positions at or past the annotated range
    (leading-edge dates appended from CMR after the DB was generated) belong
    to the last chunk. For a ``no_run`` position the phase's own start is
    returned, isolating the unusable chunk.
    """
    if not phases:
        return 0
    if pos >= phases[-1].end_pos:
        phase = phases[-1]
    else:
        phase = phase_for_position(phases, pos)
    if phase.kind is PhaseKind.FORWARD:
        for candidate in phases:
            if (candidate.kind is PhaseKind.HISTORICAL
                    and candidate.ordinal == phase.ordinal):
                return candidate.start_pos
    return phase.start_pos


def all_no_run(phases):
    """True when every phase is no_run (nothing to process for the frame)."""
    return bool(phases) and all(p.kind is PhaseKind.NO_RUN for p in phases)
