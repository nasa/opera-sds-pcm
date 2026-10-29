#!/usr/bin/env python
"""Write the DISP-NI smoke test's batch proc and expected dataset counts for a given k, m and number of k-sets.

The smoke test walks a frame through n k-sets in order: each waits on the compressed GSLCs of the ones
before it (up to m-1 of them) and then runs with them. The batch proc's data_end_date is set right after
the last k-set's last date, so the walk stops there whatever k is. That needs at least n*k dates in the
frame database, and m of at least 2 so each k-set after the first depends on the one before.

Per ICS 2.6.2 the first k-set publishes k-1 L3_DISP_NI products (its first date is the reference) and
every later one k, each k-set publishing one compressed GSLC. The counts are written per k-set, as
k_set_1 ... k_set_<n>, each the running total after that k-set.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--m", type=int, required=True)
    ap.add_argument("--k-sets", type=int, required=True, help="number of k-sets the walk processes")
    ap.add_argument("--frame-id", type=int, required=True)
    ap.add_argument("--frame-db", required=True, help="DISP-NI frame database the smoke test deploys")
    ap.add_argument("--batch-proc", required=True, help="batch proc to start from")
    ap.add_argument("--out-batch-proc", required=True)
    ap.add_argument("--out-datasets", required=True, help="expected counts, in check_datasets_file.py's format")
    args = ap.parse_args()

    with open(args.frame_db) as f:
        sensing_times = json.load(f)["data"][str(args.frame_id)]["sensing_time_list"]
    dates = sorted(datetime.fromisoformat(t) for t in sensing_times)
    n = args.k_sets

    if args.k < 1 or args.m < 2 or n < 2:
        sys.exit(f"ERROR: k={args.k}, m={args.m}, k-sets={n}: the smoke test needs k >= 1, m >= 2 and at least "
                 f"2 k-sets, so that a later k-set waits on an earlier one's compressed GSLC")
    if len(dates) < n * args.k:
        sys.exit(f"ERROR: frame {args.frame_id} has {len(dates)} dates in {args.frame_db}, fewer than the "
                 f"{n * args.k} that {n} k-sets of k={args.k} need")

    with open(args.batch_proc) as f:
        batch_proc = json.load(f)
    last_date = dates[n * args.k - 1]
    batch_proc.update(k=args.k, m=args.m, frames=[args.frame_id],
                      data_end_date=(last_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"))
    with open(args.out_batch_proc, "w") as f:
        json.dump(batch_proc, f, indent=4)

    def counts(l3, compressed):
        return [{"dataset": "L3_DISP_NI", "metadata.frame_id": args.frame_id, "count": l3},
                {"dataset": "L2_GSLC_COMPRESSED", "metadata.frame_id": args.frame_id, "count": compressed}]

    datasets = {"datasets": {f"k_set_{i}": counts(i * args.k - 1, i) for i in range(1, n + 1)}}
    with open(args.out_datasets, "w") as f:
        json.dump(datasets, f, indent=4)

    for i in range(n):
        first, last = dates[i * args.k], dates[(i + 1) * args.k - 1]
        m = min(i, args.m - 1) + 1
        print(f"k-set {i + 1}: {first:%Y-%m-%d}..{last:%Y-%m-%d} with m={m} ({m - 1} compressed GSLCs), "
              f"{(i + 1) * args.k - 1} L3_DISP_NI products in total after it")
    print(f"data_end_date {batch_proc['data_end_date']}")


if __name__ == "__main__":
    main()
