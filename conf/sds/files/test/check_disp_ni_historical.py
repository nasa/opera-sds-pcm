#!/usr/bin/env python
"""Assert the state a DISP-NI historical walk should reach after a number of k-sets.

run_disp_ni_historical_processing.py submits one gslc_query_hist job per k-set, and each job
catalogs the k-set's GSLCs in gslc_catalog. This polls until the frame's catalog, its batch proc
cursor and its blocked/finished state all match the expectation, then reports each check:

  - one catalog doc per date of the submitted k-sets, all in the mode and coverage the DISP-NI frame
    database sets for the frame, and none on a date it leaves out
  - each k-set's docs carry the m the walk ramped to (1, 2, ... up to the batch proc's m)
  - --expect blocked: the frame waits on the compressed GSLC of its last k-set (frame_blocked_since)
  - --expect finished: the frame is no longer blocked and the batch proc disabled itself

Writes SUCCESS/ERROR lines in the same format check_datasets_file.py uses, so check_pcm.py can
assert on the result file.
"""

import argparse
import logging
import sys
import time
from collections import defaultdict

from data_subscriber.gslc.gslc_utils import localize_disp_ni_frame_db
from opera_commons.es_connection import get_grq_es

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("disp_ni_historical")

BATCH_PROC_INDEX = "batch_proc"
GSLC_CATALOG_INDEX = "gslc_catalog*"
JOB_TYPE = "gslc_query_hist"


class Report:
    def __init__(self, path):
        self.path = path
        self.lines = []

    def success(self, message):
        logger.info(message)
        self.lines.append(f"SUCCESS: {message}")

    def error(self, message):
        logger.error(message)
        self.lines.append(f"ERROR: {message}")

    def write(self):
        with open(self.path, "w") as f:
            f.write("\n".join(self.lines) + "\n")


def find_batch_proc(eu, label):
    procs = [hit for hit in (eu.query(index=BATCH_PROC_INDEX) or [])
             if hit["_source"].get("job_type") == JOB_TYPE and hit["_source"].get("label") == label]
    return procs[0]["_source"] if procs else None


def catalog_docs(eu, frame_id):
    body = {"query": {"term": {"frame_id": frame_id}}, "size": 1000}
    try:
        return [hit["_source"] for hit in (eu.query(index=GSLC_CATALOG_INDEX, body=body) or [])]
    except Exception as e:
        logger.warning(f"query on {GSLC_CATALOG_INDEX} failed: {e}")
        return []


def evaluate(args, proc, docs, frame):
    """Return (successes, errors) for the current state."""
    successes, errors = [], []
    frame_key = str(args.frame_id)
    expected_position = args.k * args.k_sets

    if proc is None:
        return successes, [f"no {JOB_TYPE} batch proc labeled {args.label!r}"]

    position = (proc.get("frame_states") or {}).get(frame_key)
    if position == expected_position:
        successes.append(f"batch proc cursor for frame {frame_key} is at {position} ({args.k_sets} k-sets of {args.k})")
    else:
        errors.append(f"batch proc cursor for frame {frame_key} is {position}, expected {expected_position}")

    if len(docs) == expected_position:
        successes.append(f"gslc_catalog has {len(docs)} GSLCs for frame {frame_key}")
    else:
        errors.append(f"gslc_catalog has {len(docs)} GSLCs for frame {frame_key}, expected {expected_position}")

    frame_dates = {dt.strftime("%Y-%m-%d") for dt in frame.sensing_datetimes}
    off_db = sorted({str(d.get("acquisition_ts"))[:10] for d in docs} - frame_dates)
    if off_db:
        errors.append(f"gslc_catalog has GSLCs on dates the DISP-NI frame database leaves out: {off_db}")
    elif docs:
        successes.append("every cataloged GSLC is on a DISP-NI frame database date")

    expected = (frame.mode, frame.coverage)
    found = sorted({(str(d.get("mode")), d.get("coverage")) for d in docs})
    if any(f != expected for f in found):
        errors.append(f"gslc_catalog has GSLCs outside the frame's mode/coverage {expected}: {found}")
    elif docs:
        successes.append(f"every cataloged GSLC is in the frame's mode {frame.mode}, coverage {frame.coverage}")

    cycles_by_m = defaultdict(set)
    for d in docs:
        cycles_by_m[d.get("m")].add(d.get("acquisition_cycle"))
    cycles = sorted(c for cs in cycles_by_m.values() for c in cs)
    expected_m = {cycle: min(i // args.k, args.m - 1) + 1 for i, cycle in enumerate(cycles)}
    wrong_m = sorted(c for m, cs in cycles_by_m.items() for c in cs if expected_m.get(c) != m)
    cycles_per_m = {m: sorted(cs) for m, cs in sorted(cycles_by_m.items(), key=lambda x: str(x[0]))}
    if wrong_m:
        errors.append(f"GSLCs of cycles {wrong_m} were queried with the wrong m: {cycles_per_m}")
    elif docs:
        successes.append(f"each k-set was queried with its ramped m: {cycles_per_m}")

    blocked_since = (proc.get("frame_blocked_since") or {}).get(frame_key)
    if args.expect == "blocked":
        if blocked_since:
            successes.append(f"frame {frame_key} is waiting on compressed GSLCs since {blocked_since}")
        else:
            errors.append(f"frame {frame_key} is not recorded as blocked in frame_blocked_since")
        if proc.get("enabled") is False:
            errors.append("batch proc disabled itself while the frame still has k-sets to process")
    else:
        if blocked_since:
            errors.append(f"frame {frame_key} is still recorded as blocked since {blocked_since}")
        else:
            successes.append(f"frame {frame_key} is no longer blocked")
        if proc.get("enabled") is False and proc.get("progress_percentage") == 100:
            successes.append("batch proc finished at 100% and disabled itself")
        else:
            errors.append(f"batch proc is enabled={proc.get('enabled')} at "
                          f"{proc.get('progress_percentage')}%, expected disabled at 100%")

    return successes, errors


def run(args, eu, report):
    frame = localize_disp_ni_frame_db().get(args.frame_id)
    if frame is None:
        report.error(f"frame {args.frame_id} is not in the deployed DISP-NI frame database")
        return

    deadline = time.time() + args.timeout_mins * 60
    while True:
        successes, errors = evaluate(args, find_batch_proc(eu, args.label), catalog_docs(eu, args.frame_id),
                                     frame)
        if not errors or time.time() >= deadline:
            break
        logger.info(f"waiting: {errors}")
        time.sleep(args.poll_secs)

    for message in successes:
        report.success(message)
    for message in errors:
        report.error(message)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True, help="label of the gslc_query_hist batch proc")
    ap.add_argument("--frame-id", type=int, required=True)
    ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--m", type=int, required=True)
    ap.add_argument("--k-sets", type=int, required=True, help="number of k-sets that should have been submitted")
    ap.add_argument("--expect", choices=["blocked", "finished"], required=True)
    ap.add_argument("--timeout-mins", type=int, default=30)
    ap.add_argument("--poll-secs", type=int, default=30)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    report = Report(args.out)
    try:
        run(args, get_grq_es(logger), report)
    except Exception as e:
        report.error(f"check aborted: {e!r}")
    report.write()
    print(open(args.out).read())
    return 1 if any(line.startswith("ERROR") for line in report.lines) else 0


if __name__ == "__main__":
    sys.exit(main())
