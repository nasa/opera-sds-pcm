from datetime import datetime

import pytest

from data_subscriber.cslc.disp_s1_phases import (
    PhaseKind,
    PhaseValidationError,
    all_no_run,
    lineage_start_pos,
    parse_sensing_time_list,
    phase_for_position,
    segment_phases,
)

K = 15


def _labels(*runs):
    """Expand (label, count) pairs into a flat label list."""
    out = []
    for label, count in runs:
        out.extend([label] * count)
    return out


# Shapes taken from the real 2026-04-25 annotated DB (see frame numbers).
F24726 = _labels(("historical_01", 75), ("forward_01", 11),
                 ("historical_02", 15), ("forward_02", 8))
F5127 = _labels(("historical_01", 135), ("historical_02", 15), ("forward_02", 11))
F3611_LIKE = _labels(("no_run", 3), ("historical_02", 15), ("forward_02", 2))
F17235_LIKE = _labels(("historical_01", 15), ("forward_01", 1),
                      ("historical_02", 15), ("forward_02", 2), ("no_run", 9))


# ---------------------------------------------------------------------------
# parse_sensing_time_list
# ---------------------------------------------------------------------------

def test_parse_legacy_list_returns_no_labels():
    times = ["2016-07-09T01:33:16", "2016-08-02T01:33:17", "2016-09-19T01:33:19"]
    datetimes, labels = parse_sensing_time_list(times)
    assert labels is None
    assert datetimes == sorted(datetimes)
    assert datetimes[0] == datetime(2016, 7, 9, 1, 33, 16)


def test_parse_dict_returns_aligned_labels():
    stl = {
        "2016-07-09T01:33:16": "historical_01",
        "2016-08-02T01:33:17": "historical_01",
        "2016-09-19T01:33:19": "forward_01",
    }
    datetimes, labels = parse_sensing_time_list(stl)
    assert labels == ["historical_01", "historical_01", "forward_01"]
    assert datetimes == sorted(datetimes)


def test_parse_dict_sorts_pairs_with_unsorted_insertion_order():
    # The deployed 2026-04-25 DB has one frame (30173) whose sensing list is
    # not sorted in-file; label alignment must survive re-sorting.
    stl = {
        "2016-09-19T01:33:19": "forward_01",
        "2016-07-09T01:33:16": "historical_01",
        "2016-08-02T01:33:17": "historical_01",
    }
    datetimes, labels = parse_sensing_time_list(stl)
    assert labels == ["historical_01", "historical_01", "forward_01"]
    assert datetimes == sorted(datetimes)


# ---------------------------------------------------------------------------
# segment_phases: real shapes
# ---------------------------------------------------------------------------

def test_segment_four_phase_gap_frame():
    phases = segment_phases(F24726, K)
    assert [(p.label, p.start_pos, p.end_pos) for p in phases] == [
        ("historical_01", 0, 75),
        ("forward_01", 75, 86),
        ("historical_02", 86, 101),
        ("forward_02", 101, 109),
    ]
    assert [p.kind for p in phases] == [
        PhaseKind.HISTORICAL, PhaseKind.FORWARD,
        PhaseKind.HISTORICAL, PhaseKind.FORWARD,
    ]
    assert [p.ordinal for p in phases] == [1, 1, 2, 2]
    assert [p.is_new_lineage for p in phases] == [False, False, True, False]


def test_segment_adjacent_historical_phases():
    phases = segment_phases(F5127, K)
    assert [p.label for p in phases] == ["historical_01", "historical_02", "forward_02"]
    assert phases[1].is_new_lineage is True
    assert phases[1].start_pos == 135


def test_segment_no_run_first_starts_greenfield():
    phases = segment_phases(F3611_LIKE, K)
    assert [p.kind for p in phases] == [
        PhaseKind.NO_RUN, PhaseKind.HISTORICAL, PhaseKind.FORWARD,
    ]
    # The first processed phase is greenfield even with ordinal > 1.
    assert phases[1].ordinal == 2
    assert phases[1].is_new_lineage is False


def test_segment_trailing_no_run():
    phases = segment_phases(F17235_LIKE, K)
    assert [p.label for p in phases] == [
        "historical_01", "forward_01", "historical_02", "forward_02", "no_run",
    ]
    assert phases[2].is_new_lineage is True
    assert phases[4].kind is PhaseKind.NO_RUN


def test_segment_pure_no_run_and_merged_chunks():
    # Consecutive unusable chunks merge into one run (label has no ordinal).
    phases = segment_phases(_labels(("no_run", 19)), K)
    assert len(phases) == 1
    assert phases[0].length == 19
    assert all_no_run(phases)
    assert not all_no_run(segment_phases(F24726, K))


def test_segment_empty():
    assert segment_phases([], K) == []


# ---------------------------------------------------------------------------
# segment_phases: validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("labels,message", [
    (_labels(("historic_01", 15)), "unrecognized"),
    (_labels(("Historical_01", 15)), "unrecognized"),
    (_labels(("forward01", 3)), "unrecognized"),
    (_labels(("historical_01", 15), ("forward_01", 2), ("historical_01", 15)),
     "recurs"),
    (_labels(("historical_02", 15), ("historical_01", 15)), "ordinal decreases"),
    (_labels(("forward_01", 3)), "must directly follow"),
    (_labels(("no_run", 2), ("forward_02", 3)), "must directly follow"),
    (_labels(("historical_01", 15), ("forward_02", 3)), "must directly follow"),
    (_labels(("historical_01", 15), ("forward_01", 2), ("forward_02", 3)),
     "must directly follow"),
    (_labels(("historical_01", 14)), "not a multiple"),
    (_labels(("historical_01", 15), ("forward_01", 15)), "forward phases hold"),
])
def test_segment_validation_errors(labels, message):
    with pytest.raises(PhaseValidationError, match=message):
        segment_phases(labels, K)


# ---------------------------------------------------------------------------
# phase_for_position / lineage_start_pos
# ---------------------------------------------------------------------------

def test_phase_for_position_boundaries():
    phases = segment_phases(F24726, K)
    assert phase_for_position(phases, 0).label == "historical_01"
    assert phase_for_position(phases, 74).label == "historical_01"
    assert phase_for_position(phases, 75).label == "forward_01"
    assert phase_for_position(phases, 85).label == "forward_01"
    assert phase_for_position(phases, 86).label == "historical_02"
    assert phase_for_position(phases, 100).label == "historical_02"
    assert phase_for_position(phases, 101).label == "forward_02"
    assert phase_for_position(phases, 108).label == "forward_02"


@pytest.mark.parametrize("pos", [-1, 109, 500])
def test_phase_for_position_out_of_range(pos):
    phases = segment_phases(F24726, K)
    with pytest.raises(PhaseValidationError, match="outside the annotated range"):
        phase_for_position(phases, pos)


def test_lineage_start_pos_follows_chunks():
    phases = segment_phases(F24726, K)
    # Chunk 1: historical_01 and its forward tail share one lineage.
    assert lineage_start_pos(phases, 0) == 0
    assert lineage_start_pos(phases, 74) == 0
    assert lineage_start_pos(phases, 80) == 0
    # Chunk 2: fresh lineage from the historical_02 start.
    assert lineage_start_pos(phases, 86) == 86
    assert lineage_start_pos(phases, 100) == 86
    assert lineage_start_pos(phases, 105) == 86
    # Leading-edge dates past the annotated range belong to the last chunk.
    assert lineage_start_pos(phases, 200) == 86


def test_lineage_start_pos_no_run_isolates():
    phases = segment_phases(F3611_LIKE, K)
    assert lineage_start_pos(phases, 1) == 0  # inside the no_run block
    assert lineage_start_pos(phases, 3) == 3  # historical_02 start
    assert lineage_start_pos(phases, 18) == 3  # forward_02 chains to chunk 2


def test_lineage_start_pos_empty():
    assert lineage_start_pos([], 40) == 0
