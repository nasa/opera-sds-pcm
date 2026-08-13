#!/usr/bin/env python
"""Assert that a phased DISP-S1 historical run actually took the phased path.

Dataset counts alone cannot distinguish a correct phased walk from a regression
that reverted to the absolute grid but happened to land on the same total. These
checks read the compressed CSLC lineage and the k-cycle state configs directly.

The frame's phase structure is derived from the deployed burst database rather
than hard-coded, so this stays correct when the database vintage changes.

Writes SUCCESS/ERROR lines in the same format check_datasets_file.py uses, so
check_pcm.py can assert on the result file.
"""

import argparse
import re
import sys
from collections import defaultdict

from data_subscriber import cslc_utils
from opera_commons.es_connection import get_grq_es

CCSLC_INDEX = "grq_*_l2_cslc_s1_compressed*"
KSC_INDEX = "grq_*_disp_s1-kcycle*"
L3_INDEX = "grq_*_l3_disp_s1*"

# ..._<sensing>T..Z_<lineage_start>T..Z_<boundary>T..Z_<created>T..Z_
CCSLC_DATE_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")


def frame_search(eu, index, frame_id, size=5000):
    body = {"query": {"bool": {"must": [{"term": {"metadata.frame_id": frame_id}}]}}, "size": size}
    try:
        return eu.query(index=index, body=body) or []
    except Exception as e:
        print(f"WARNING: query on {index} failed: {e}", file=sys.stderr)
        return []


def expected_boundaries(frame, k):
    """The last date of every whole k-set of every historical phase.

    ProcessingPhase.end_pos is exclusive; use .length rather than a span.
    """
    out = []
    for phase in frame.phases:
        if not phase.label.startswith("historical_"):
            continue
        for start in range(phase.start_pos, phase.start_pos + phase.length - k + 1, k):
            out.append((phase.label, frame.sensing_datetimes[start + k - 1].strftime("%Y%m%d")))
    return out


def dates_in(frame, predicate):
    """Sensing dates, as YYYYMMDD, of every phase matching predicate(label)."""
    out = set()
    for phase in frame.phases:
        if predicate(phase.label):
            for pos in range(phase.start_pos, phase.end_pos):
                out.add(frame.sensing_datetimes[pos].strftime("%Y%m%d"))
    return out


def no_run_dates(frame):
    return dates_in(frame, lambda label: label == "no_run")


def historical_dates(frame):
    return dates_in(frame, lambda label: label.startswith("historical_") or label == "no_run")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--frame-id", type=int, required=True)
    parser.add_argument("--k", type=int, default=15)
    parser.add_argument("--out", default="/tmp/phases.txt")
    args = parser.parse_args()

    results = []

    def record(ok, msg):
        results.append(("SUCCESS: " if ok else "ERROR: ") + msg)

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    frame = frame_to_bursts.get(args.frame_id)

    if frame is None:
        record(False, f"frame {args.frame_id} is not in the deployed burst database")
    elif getattr(frame, "phases", None) is None:
        reason = getattr(frame, "phase_error", None) or (
            "the deployed database carries no processing-mode annotations, or "
            "DISP_S1_PROCESSING_MODE_ENABLED is off")
        record(False, f"frame {args.frame_id} has no phases: {reason}")
    else:
        eu = get_grq_es()
        labels = ", ".join(f"{p.label}[{p.length}]" for p in frame.phases)
        record(True, f"frame {args.frame_id} phases: {labels}")

        # 1. one compressed CSLC boundary per whole k-set of each historical phase,
        #    at the phase-relative position -- not the absolute grid's.
        want = expected_boundaries(frame, args.k)
        want_dates = sorted({d for _, d in want})
        got = defaultdict(set)
        for doc in frame_search(eu, CCSLC_INDEX, args.frame_id):
            m = CCSLC_DATE_RE.search(doc.get("_id", "") or doc.get("_source", {}).get("id", ""))
            if m:
                got[m.group(3)].add(doc.get("_id"))
        got_dates = sorted(got)
        if got_dates == want_dates:
            record(True, f"compressed CSLC boundaries at {want_dates} "
                         f"({len(want)} k-set(s) across {len({l for l, _ in want})} historical phase(s))")
        else:
            record(False, f"compressed CSLC boundaries {got_dates} != expected {want_dates}")

        # 2. a no_run block produces nothing at all
        skipped = no_run_dates(frame)
        produced = set()
        for doc in frame_search(eu, L3_INDEX, args.frame_id):
            m = re.search(r"_(\d{8})T\d+Z_", doc.get("_id", ""))
            if m and m.group(1) in skipped:
                produced.add(m.group(1))
        if skipped and not produced:
            record(True, f"{len(skipped)} no_run date(s) produced no products")
        elif not skipped:
            record(True, "frame has no no_run block to skip")
        else:
            record(False, f"no_run dates produced products: {sorted(produced)}")

        # 3. every KSC in a historical or no_run phase is superseded, so no forward
        #    SCIFLO races the batch job for a mid-stack date
        supersede_me = historical_dates(frame)
        not_superseded = []
        for doc in frame_search(eu, KSC_INDEX, args.frame_id):
            src = doc.get("_source", {})
            md = src.get("metadata", {}) or {}
            date = str(md.get("sensing_time", ""))[:10].replace("-", "")
            if date and date in supersede_me and not md.get("superseded_by"):
                not_superseded.append(date)
        if not not_superseded:
            record(True, "every historical/no_run phase KSC is superseded")
        else:
            record(False, f"KSCs not superseded for historical/no_run dates: {sorted(set(not_superseded))}")

    with open(args.out, "w") as f:
        for line in results:
            print(line)
            f.write(line + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
