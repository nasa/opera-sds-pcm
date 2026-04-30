#!/usr/bin/env python3
"""
Parse diff_priority*.txt files to find frames where File2 has more sensing times
(i.e., the new constDB gained sensing times). For each such frame, use the OLD
constDB (File1) to determine which K-cycle the earliest added sensing time would
belong to. This identifies the K-cycle (and all subsequent ones) whose DISP-S1
and CCSLC products need to be deleted and regenerated.

Logic:
  - The old constDB has an ordered list of sensing times per frame.
  - The new constDB adds sensing times that were previously missing.
  - We insert each added sensing time into the old list (sorted position) to find
    its would-be index, then compute k_cycle = index // k.
  - The earliest affected K-cycle is the one we report; all subsequent K-cycles
    are also affected because indices shift.
"""

import json
import re
import sys
import math
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# ---------- Configuration ----------
K = 15  # default k parameter for DISP-S1
DIFF_DIR = Path(__file__).parent
OLD_CONSTDB = DIFF_DIR.parent / "disp_s1_consistent_burst_db" / \
    "opera-disp-s1-consistent-burst-ids-2025-06-30-2016-07-01_to_2024-12-31.json"
DIFF_FILES = sorted(DIFF_DIR.glob("diff_priority*.txt"))

# ---------- Parse diff files ----------
def parse_diff_files(diff_files):
    """
    Returns dict: { frame_id: [list of added sensing time strings] }
    Only includes frames where File2 has MORE sensing times (<<< FILE2 MORE).
    """
    frames = defaultdict(list)
    current_frame = None
    in_file2_more_block = False

    for fpath in diff_files:
        with open(fpath) as f:
            current_frame = None
            in_file2_more_block = False
            for line in f:
                line = line.rstrip()

                # Match frame header
                m = re.match(r'^FRAME (\d+)', line)
                if m:
                    current_frame = int(m.group(1))
                    in_file2_more_block = False
                    continue

                # Detect FILE2 MORE
                if current_frame and "FILE2 MORE" in line:
                    in_file2_more_block = True
                    continue

                # Detect FILE1 MORE — skip these frames
                if current_frame and "FILE1 MORE" in line:
                    in_file2_more_block = False
                    continue

                # Collect added sensing times
                if in_file2_more_block and current_frame:
                    m = re.match(r'^\s+\+\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', line)
                    if m:
                        frames[current_frame].append(m.group(1))
                        continue

                # Separator resets
                if line.startswith("---"):
                    current_frame = None
                    in_file2_more_block = False

    return dict(frames)


# ---------- Load old constDB ----------
def load_constdb(path):
    """Returns dict: { frame_id (int): sorted list of datetime objects }"""
    with open(path) as f:
        db = json.load(f)

    result = {}
    data = db["data"]
    for frame_str, frame_data in data.items():
        sensing_times = sorted(
            datetime.fromisoformat(t) for t in frame_data["sensing_time_list"]
        )
        result[int(frame_str)] = sensing_times
    return result


# ---------- Find K-cycle for an inserted sensing time ----------
def find_kcycle_for_insertion(sensing_times, new_time_str, k):
    """
    Given the old sorted sensing time list and a new sensing time to insert,
    find the insertion index (bisect) and return the K-cycle it falls into.
    """
    import bisect
    new_dt = datetime.fromisoformat(new_time_str)
    idx = bisect.bisect_left(sensing_times, new_dt)
    k_cycle = idx // k
    return k_cycle, idx


# ---------- Main ----------
def main():
    k = K

    print(f"Parsing diff files: {[f.name for f in DIFF_FILES]}")
    added_map = parse_diff_files(DIFF_FILES)
    print(f"Found {len(added_map)} frames with added sensing times (FILE2 MORE)\n")

    print(f"Loading old constDB: {OLD_CONSTDB}")
    constdb = load_constdb(OLD_CONSTDB)
    print(f"Loaded {len(constdb)} frames from constDB\n")

    print(f"Using K = {k}")
    print("=" * 110)
    print(f"{'Frame':>8}  {'Priority':>8}  {'Earliest Added':>24}  {'Insert Idx':>10}  "
          f"{'K-cycle':>7}  {'Total STs':>9}  {'Total K-cycles':>14}")
    print("-" * 110)

    # Determine priority for each frame by checking which file it came from
    frame_priority = {}
    for fpath in DIFF_FILES:
        # Extract priority from filename like diff_priority3a.txt -> "3a"
        m = re.search(r'priority(\w+)', fpath.stem)
        priority = m.group(1) if m else "?"
        # Re-parse just for priority mapping
        with open(fpath) as f:
            for line in f:
                fm = re.match(r'^FRAME (\d+)', line.strip())
                if fm:
                    fid = int(fm.group(1))
                    if fid in added_map:
                        frame_priority[fid] = priority

    results = []
    for frame_id in sorted(added_map.keys()):
        added_times = added_map[frame_id]
        if frame_id not in constdb:
            print(f"{frame_id:>8}  {'N/A':>8}  Frame not found in old constDB — skipping")
            continue

        sensing_times = constdb[frame_id]
        total_sts = len(sensing_times)
        total_kcycles = math.ceil(total_sts / k)

        # Find insertion point for each added time, get earliest k-cycle
        earliest_kcycle = None
        earliest_time = None
        earliest_idx = None
        for t in added_times:
            kc, idx = find_kcycle_for_insertion(sensing_times, t, k)
            if earliest_kcycle is None or kc < earliest_kcycle:
                earliest_kcycle = kc
                earliest_time = t
                earliest_idx = idx

        priority = frame_priority.get(frame_id, "?")
        print(f"{frame_id:>8}  {priority:>8}  {earliest_time:>24}  {earliest_idx:>10}  "
              f"{earliest_kcycle:>7}  {total_sts:>9}  {total_kcycles:>14}")

        results.append({
            "frame_id": frame_id,
            "priority": priority,
            "earliest_added_sensing_time": earliest_time,
            "insertion_index": earliest_idx,
            "affected_kcycle": earliest_kcycle,
            "total_sensing_times": total_sts,
            "total_kcycles": total_kcycles,
            "all_added_sensing_times": added_times,
            "kcycles_to_delete": list(range(earliest_kcycle, total_kcycles)),
            "new_frame_state": k * earliest_kcycle,
        })

    print("=" * 110)
    print(f"\nTotal affected frames: {len(results)}")

    # Summary: how many k-cycles need deletion across all frames
    total_kcycles_to_delete = sum(len(r["kcycles_to_delete"]) for r in results)
    print(f"Total K-cycles to delete/regenerate: {total_kcycles_to_delete}")

    # Write detailed JSON output
    output_file = DIFF_DIR / "affected_kcycles.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results written to: {output_file}")

    # Print per-frame deletion summary
    print("\n" + "=" * 110)
    print("Per-frame K-cycle deletion ranges:")
    print("-" * 110)
    for r in results:
        frame = r["frame_id"]
        kc_start = r["affected_kcycle"]
        kc_end = r["total_kcycles"] - 1
        n_delete = len(r["kcycles_to_delete"])
        st_start_idx = kc_start * k
        # Sensing time range that needs deletion
        st_list = constdb[frame]
        if st_start_idx < len(st_list):
            first_st = st_list[st_start_idx].isoformat()
        else:
            first_st = "(beyond current list)"
        last_st = st_list[-1].isoformat()

        print(f"  Frame {frame:>6}: K-cycles {kc_start}-{kc_end} "
              f"({n_delete} cycles, sensing times from {first_st} to {last_st})")


if __name__ == "__main__":
    main()
