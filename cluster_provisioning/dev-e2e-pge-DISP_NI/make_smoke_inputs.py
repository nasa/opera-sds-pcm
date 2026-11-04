#!/usr/bin/env python
"""Write the DISP-NI smoke test's batch proc and expected dataset counts for a given k, m and number of k-sets.

The smoke test walks each of its frames through n k-sets in order: each k-set waits on the compressed
GSLCs of the ones before it (up to m-1 of them) and then runs with them. The frames walk side by side
in one batch proc. Its data_end_date is set right after the latest frame's n-th k-set, so the walk
stops there whatever k is. That needs at least n*k dates for every frame, no frame with an extra full
k-set before that date, and m of at least 2 so each k-set after the first depends on the one before.

Per ICS 2.6.2 the first k-set publishes k-1 L3_DISP_NI products (its first date is the reference) and
every later one k, each k-set publishing one compressed GSLC. The counts are written per frame and
k-set, as f<frame>_k_set_1 ... f<frame>_k_set_<n>, each the frame's running total after that k-set.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--m", type=int, required=True)
    ap.add_argument("--k-sets", type=int, required=True, help="number of k-sets the walk processes per frame")
    ap.add_argument("--frame-ids", type=int, nargs="+", required=True)
    ap.add_argument("--frame-db", required=True, help="DISP-NI frame database the smoke test deploys")
    ap.add_argument("--batch-proc", required=True, help="batch proc to start from")
    ap.add_argument("--out-batch-proc", required=True)
    ap.add_argument("--out-datasets", required=True, help="expected counts, in check_datasets_file.py's format")
    args = ap.parse_args()

    k, n = args.k, args.k_sets
    if k < 1 or args.m < 2 or n < 2:
        sys.exit(f"ERROR: k={k}, m={args.m}, k-sets={n}: the smoke test needs k >= 1, m >= 2 and at least "
                 f"2 k-sets, so that a later k-set waits on an earlier one's compressed GSLC")

    with open(args.frame_db) as f:
        frame_db = json.load(f)["data"]
    dates = {}
    for frame_id in args.frame_ids:
        dates[frame_id] = sorted(datetime.fromisoformat(t) for t in frame_db[str(frame_id)]["sensing_time_list"])
        if len(dates[frame_id]) < n * k:
            sys.exit(f"ERROR: frame {frame_id} has {len(dates[frame_id])} dates in {args.frame_db}, fewer than the "
                     f"{n * k} that {n} k-sets of k={k} need")

    data_end_date = max(frame_dates[n * k - 1] for frame_dates in dates.values()) + timedelta(days=1)
    for frame_id, frame_dates in dates.items():
        if len(frame_dates) >= (n + 1) * k and frame_dates[(n + 1) * k - 1] < data_end_date:
            sys.exit(f"ERROR: frame {frame_id} would walk more than {n} k-sets before {data_end_date:%Y-%m-%d}, "
                     f"where another frame's {n}-th k-set ends")

    with open(args.batch_proc) as f:
        batch_proc = json.load(f)
    batch_proc.update(k=k, m=args.m, frames=list(args.frame_ids),
                      data_end_date=data_end_date.strftime("%Y-%m-%dT%H:%M:%S"))
    with open(args.out_batch_proc, "w") as f:
        json.dump(batch_proc, f, indent=4)

    def counts(frame_id, l3, compressed):
        return [{"dataset": "L3_DISP_NI", "metadata.frame_id": frame_id, "count": l3},
                {"dataset": "L2_GSLC_COMPRESSED", "metadata.frame_id": frame_id, "count": compressed}]

    datasets = {"datasets": {f"f{frame_id}_k_set_{i}": counts(frame_id, i * k - 1, i)
                             for frame_id in args.frame_ids for i in range(1, n + 1)}}
    with open(args.out_datasets, "w") as f:
        json.dump(datasets, f, indent=4)

    for frame_id, frame_dates in dates.items():
        for i in range(n):
            first, last = frame_dates[i * k], frame_dates[(i + 1) * k - 1]
            m = min(i, args.m - 1) + 1
            print(f"frame {frame_id} k-set {i + 1}: {first:%Y-%m-%d}..{last:%Y-%m-%d} with m={m} "
                  f"({m - 1} compressed GSLCs), {(i + 1) * k - 1} L3_DISP_NI products in total after it")
    print(f"data_end_date {batch_proc['data_end_date']}")


if __name__ == "__main__":
    main()
