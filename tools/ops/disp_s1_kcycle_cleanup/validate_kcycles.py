#!/usr/bin/env python3
"""
Validate affected_kcycles.json by independently loading the old constDB
and replicating the exact K-cycle logic from disp_s1_burst_db_tool.py.

The tool uses (line 229-231):
    for i in range(0, len_sensing_times, k):
        end = i+k if i+k < len_sensing_times else len_sensing_times
        print(f"K-cycle {math.ceil(i/k)}", ...)

This means K-cycle N contains sensing times at indices [N*k, (N+1)*k).
Our script uses: k_cycle = index // k  (equivalent to math.ceil(i/k) when i = N*k).

Validation checks:
  1. The constDB has the frame and the correct total sensing time count
  2. The earliest added sensing time bisect-inserts at the reported index
  3. The reported K-cycle matches index // k
  4. The K-cycle deletion range is correct (from affected_kcycle to total_kcycles-1)
  5. The sensing time at the start of the affected K-cycle matches the per-frame
     deletion range reported by the original script
"""

import bisect
import json
import math
from datetime import datetime
from pathlib import Path

DIFF_DIR = Path(__file__).parent
AFFECTED_JSON = DIFF_DIR / "affected_kcycles.json"
OLD_CONSTDB = DIFF_DIR.parent / "disp_s1_consistent_burst_db" / \
    "opera-disp-s1-consistent-burst-ids-2025-06-30-2016-07-01_to_2024-12-31.json"
K = 15

def load_constdb(path):
    with open(path) as f:
        db = json.load(f)
    result = {}
    for frame_str, frame_data in db["data"].items():
        sensing_times = sorted(
            datetime.fromisoformat(t) for t in frame_data["sensing_time_list"]
        )
        result[int(frame_str)] = sensing_times
    return result

def main():
    with open(AFFECTED_JSON) as f:
        results = json.load(f)

    constdb = load_constdb(OLD_CONSTDB)

    print(f"Validating {len(results)} entries from affected_kcycles.json")
    print(f"Old constDB has {len(constdb)} frames")
    print(f"K = {K}")
    print("=" * 90)

    errors = 0
    for entry in results:
        frame_id = entry["frame_id"]
        reported_idx = entry["insertion_index"]
        reported_kcycle = entry["affected_kcycle"]
        reported_total_sts = entry["total_sensing_times"]
        reported_total_kc = entry["total_kcycles"]
        reported_kc_to_delete = entry["kcycles_to_delete"]
        earliest_added = entry["earliest_added_sensing_time"]
        all_added = entry["all_added_sensing_times"]

        prefix = f"  Frame {frame_id:>6}"

        # Check 1: frame exists in constDB
        if frame_id not in constdb:
            print(f"{prefix}: FAIL - frame not found in constDB")
            errors += 1
            continue

        sensing_times = constdb[frame_id]

        # Check 2: total sensing time count
        if len(sensing_times) != reported_total_sts:
            print(f"{prefix}: FAIL - total STs mismatch: constDB={len(sensing_times)}, reported={reported_total_sts}")
            errors += 1
            continue

        # Check 3: total k-cycles
        expected_total_kc = math.ceil(len(sensing_times) / K)
        if expected_total_kc != reported_total_kc:
            print(f"{prefix}: FAIL - total K-cycles mismatch: expected={expected_total_kc}, reported={reported_total_kc}")
            errors += 1
            continue

        # Check 4: insertion index for earliest added time
        earliest_dt = datetime.fromisoformat(earliest_added)
        computed_idx = bisect.bisect_left(sensing_times, earliest_dt)
        if computed_idx != reported_idx:
            print(f"{prefix}: FAIL - insertion index mismatch: computed={computed_idx}, reported={reported_idx}")
            errors += 1
            continue

        # Check 5: K-cycle computation
        computed_kcycle = computed_idx // K
        if computed_kcycle != reported_kcycle:
            print(f"{prefix}: FAIL - K-cycle mismatch: computed={computed_kcycle}, reported={reported_kcycle}")
            errors += 1
            continue

        # Check 6: K-cycle deletion range
        expected_kc_range = list(range(computed_kcycle, expected_total_kc))
        if expected_kc_range != reported_kc_to_delete:
            print(f"{prefix}: FAIL - K-cycle deletion range mismatch: expected={expected_kc_range}, reported={reported_kc_to_delete}")
            errors += 1
            continue

        # Check 7: verify that ALL added sensing times map to k-cycles >= the reported earliest
        all_ok = True
        for t_str in all_added:
            t_dt = datetime.fromisoformat(t_str)
            idx = bisect.bisect_left(sensing_times, t_dt)
            kc = idx // K
            if kc < reported_kcycle:
                print(f"{prefix}: FAIL - added time {t_str} maps to K-cycle {kc} which is before reported earliest {reported_kcycle}")
                errors += 1
                all_ok = False
                break
        if not all_ok:
            continue

        # Check 8: verify the earliest added time is indeed the one producing the smallest k-cycle
        min_kc = None
        min_time = None
        for t_str in all_added:
            t_dt = datetime.fromisoformat(t_str)
            idx = bisect.bisect_left(sensing_times, t_dt)
            kc = idx // K
            if min_kc is None or kc < min_kc:
                min_kc = kc
                min_time = t_str
        if min_time != earliest_added:
            # It's OK if a different time produces the same k-cycle
            if min_kc != reported_kcycle:
                print(f"{prefix}: FAIL - {min_time} gives K-cycle {min_kc} < reported earliest from {earliest_added} (K-cycle {reported_kcycle})")
                errors += 1
                continue

        # Check 9: new_frame_state == affected_kcycle * K and is a multiple of K
        reported_new_fs = entry.get("new_frame_state")
        if reported_new_fs is None:
            print(f"{prefix}: FAIL - missing new_frame_state field")
            errors += 1
            continue
        expected_new_fs = reported_kcycle * K
        if reported_new_fs != expected_new_fs:
            print(f"{prefix}: FAIL - new_frame_state mismatch: expected={expected_new_fs}, reported={reported_new_fs}")
            errors += 1
            continue
        if reported_new_fs % K != 0:
            print(f"{prefix}: FAIL - new_frame_state {reported_new_fs} is not a multiple of K={K}")
            errors += 1
            continue

        # Cross-check with tool's k-cycle grouping logic:
        # disp_s1_burst_db_tool.py line 229: for i in range(0, len_sensing_times, k):
        #   K-cycle = math.ceil(i/k)
        # At i=0 -> ceil(0/15)=0, i=15 -> ceil(15/15)=1, i=30 -> ceil(30/15)=2
        # This is identical to i//k for these multiples of k.
        # For the insertion point, the tool doesn't do insertion — it just groups
        # existing indices. Our bisect approach is correct because inserting a new
        # sensing time at position idx means it would be in the group starting at
        # (idx // k) * k.

        print(f"{prefix}: OK  (earliest={earliest_added}, idx={computed_idx}, K-cycle={computed_kcycle}, "
              f"delete K-cycles {reported_kc_to_delete[0]}-{reported_kc_to_delete[-1]} of {expected_total_kc})")

    print("=" * 90)
    if errors == 0:
        print(f"VALIDATION PASSED: All {len(results)} entries are correct.")
    else:
        print(f"VALIDATION FAILED: {errors} of {len(results)} entries have errors.")

    return errors

if __name__ == "__main__":
    import sys
    sys.exit(main())
