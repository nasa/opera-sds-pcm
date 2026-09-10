#!/usr/bin/env python3
"""Compare the deployed DISP-S1 region database with the consistent burst database.

Every cycle and k-cycle state config is stamped with the region its frame belongs to,
read from the region database (settings DISP_S1_FRAME_REGION_DB). A frame the region
database does not list is stamped UNKNOWN, and the trigger-SCIFLO_L3_DISP_S1 rule's
region whitelist can never match it: its state configs complete and nothing runs.

This reports the frames the burst database processes but the region database does not
list, and the reverse. With --frames it also resolves each named frame and exits 1 if
any is UNKNOWN, which is the check to run before whitelisting regions for a test.

    python tools/check_disp_s1_region_db.py
    python tools/check_disp_s1_region_db.py --frames 15422,42810
    python tools/check_disp_s1_region_db.py --json /tmp/region_db_audit.json
"""

import argparse
import json
import sys
from collections import Counter

UNKNOWN = "UNKNOWN"


def compare_frame_sets(burst_db_frames, frame_region_map):
    """(frames with no region, region-db frames absent from the burst database), both sorted."""
    burst_db_frames = {int(f) for f in burst_db_frames}
    region_frames = {int(f) for f in frame_region_map}
    return sorted(burst_db_frames - region_frames), sorted(region_frames - burst_db_frames)


def region_counts(frame_region_map):
    return dict(sorted(Counter(str(r) for r in frame_region_map.values()).items()))


def resolve_frames(frame_ids, frame_region_map):
    return {int(f): str(frame_region_map.get(int(f), UNKNOWN)) for f in frame_ids}


def parse_frames(text):
    return [int(f) for f in text.replace(" ", "").split(",") if f]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frames", type=parse_frames, help="comma-separated frame ids to resolve")
    ap.add_argument("--json", help="also write the full report to this file")
    args = ap.parse_args()

    from data_subscriber import cslc_utils
    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    frame_region_map = cslc_utils._localize_region_db()

    missing_region, extra_region = compare_frame_sets(frame_to_bursts.keys(), frame_region_map)
    report = {
        "burst_db_frames": len(frame_to_bursts),
        "region_db_frames": len(frame_region_map),
        "region_counts": region_counts(frame_region_map),
        "burst_db_frames_without_region": missing_region,
        "region_db_frames_not_in_burst_db": extra_region,
    }

    print(f"burst database frames: {report['burst_db_frames']}")
    print(f"region database frames: {report['region_db_frames']}  {report['region_counts']}")
    print(f"burst database frames with no region ({len(missing_region)}; stamped {UNKNOWN}):")
    print("   " + (", ".join(str(f) for f in missing_region) or "none"))
    print(f"region database frames not in the burst database ({len(extra_region)}):")
    print("   " + (", ".join(str(f) for f in extra_region) or "none"))

    status = 0
    if args.frames:
        resolved = resolve_frames(args.frames, frame_region_map)
        report["frames"] = resolved
        print("requested frames:")
        for frame_id, region in resolved.items():
            print(f"   {frame_id}: {region}")
        if UNKNOWN in resolved.values():
            print(f"ERROR: at least one requested frame resolves to {UNKNOWN}")
            status = 1

    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)
        print(f"wrote {args.json}")

    return status


if __name__ == "__main__":
    sys.exit(main())
