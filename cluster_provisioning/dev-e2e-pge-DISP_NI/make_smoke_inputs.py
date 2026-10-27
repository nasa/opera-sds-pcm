#!/usr/bin/env python
"""Write the DISP-NI smoke test's batch proc and expected dataset counts for a given k and m.

The smoke test walks a frame through two k-sets: the first runs alone, the second waits on the first's
compressed GSLC and then runs with it. The batch proc's data_end_date is set right after the second
k-set's last date, so the walk stops there whatever k is. That needs at least 2k dates in the frame
database, and m of at least 2 so the second k-set depends on the first.

Per ICS 2.6.2 the first k-set publishes k-1 L3_DISP_NI products (its first date is the reference) and
every later one k, each k-set publishing one compressed GSLC.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta

K_SETS = 2


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--m", type=int, required=True)
    ap.add_argument("--frame-id", type=int, required=True)
    ap.add_argument("--frame-db", required=True, help="DISP-NI frame database the smoke test deploys")
    ap.add_argument("--batch-proc", required=True, help="batch proc to start from")
    ap.add_argument("--out-batch-proc", required=True)
    ap.add_argument("--out-datasets", required=True, help="expected counts, in check_datasets_file.py's format")
    args = ap.parse_args()

    with open(args.frame_db) as f:
        sensing_times = json.load(f)["data"][str(args.frame_id)]["sensing_time_list"]
    dates = sorted(datetime.fromisoformat(t) for t in sensing_times)

    if args.k < 1 or args.m < 2:
        sys.exit(f"ERROR: k={args.k}, m={args.m}: the smoke test needs k >= 1 and m >= 2, so that its second "
                 f"k-set waits on the first's compressed GSLC")
    if len(dates) < K_SETS * args.k:
        sys.exit(f"ERROR: frame {args.frame_id} has {len(dates)} dates in {args.frame_db}, fewer than the "
                 f"{K_SETS * args.k} that {K_SETS} k-sets of k={args.k} need")

    with open(args.batch_proc) as f:
        batch_proc = json.load(f)
    last_date = dates[K_SETS * args.k - 1]
    batch_proc.update(k=args.k, m=args.m, frames=[args.frame_id],
                      data_end_date=(last_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"))
    with open(args.out_batch_proc, "w") as f:
        json.dump(batch_proc, f, indent=4)

    def counts(l3, compressed):
        return [{"dataset": "L3_DISP_NI", "metadata.frame_id": args.frame_id, "count": l3},
                {"dataset": "L2_GSLC_COMPRESSED", "metadata.frame_id": args.frame_id, "count": compressed}]

    datasets = {"datasets": {"k_set_1": counts(args.k - 1, 1),
                             "hist": counts(K_SETS * args.k - 1, K_SETS)}}
    with open(args.out_datasets, "w") as f:
        json.dump(datasets, f, indent=4)

    print(f"k={args.k} m={args.m}: k-sets {dates[0]:%Y-%m-%d}..{dates[args.k - 1]:%Y-%m-%d} and "
          f"{dates[args.k]:%Y-%m-%d}..{last_date:%Y-%m-%d}, data_end_date {batch_proc['data_end_date']}, "
          f"expecting {args.k - 1} then {K_SETS * args.k - 1} L3_DISP_NI products")


if __name__ == "__main__":
    main()
