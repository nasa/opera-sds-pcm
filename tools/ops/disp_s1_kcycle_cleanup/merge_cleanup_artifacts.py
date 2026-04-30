#!/usr/bin/env python3
"""Merge DISP-S1 K-cycle cleanup artifacts into a single operator manifest.

Reads:
  - affected_kcycles.json (50 frames with K-cycle analysis)
  - disp_products_to_delete.json (GRQ products + CMR granules)
  - ccslc_products_to_delete.json (CCSLC GRQ products)

Writes:
  - cleanup_manifest.json (per-frame operator document)
"""

import json
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def load_inputs():
    with open(SCRIPT_DIR / "affected_kcycles.json") as f:
        affected = json.load(f)

    with open(SCRIPT_DIR / "disp_products_to_delete.json") as f:
        disp = json.load(f)

    with open(SCRIPT_DIR / "ccslc_products_to_delete.json") as f:
        ccslcs = json.load(f)

    return affected, disp, ccslcs


def group_by_frame(products, key="frame_id"):
    grouped = defaultdict(list)
    for p in products:
        grouped[p[key]].append(p)
    return grouped


def build_manifest(affected, disp, ccslcs):
    # Group products by frame
    disp_grq_by_frame = group_by_frame(disp["grq_products"])
    disp_cmr_by_frame = group_by_frame(disp["cmr_granules"])
    ccslc_by_frame = group_by_frame(ccslcs)

    manifest = []
    for frame_info in sorted(affected, key=lambda x: x["frame_id"]):
        fid = frame_info["frame_id"]

        disp_grq = disp_grq_by_frame.get(fid, [])
        disp_cmr = disp_cmr_by_frame.get(fid, [])
        ccslc_grq = ccslc_by_frame.get(fid, [])

        # Keep only relevant fields for operator doc
        disp_grq_clean = [
            {"id": p["id"], "index": p["index"], "s3_dir": p["s3_dir"]}
            for p in disp_grq
        ]
        disp_cmr_clean = [
            {"title": p["title"], "concept_id": p["concept_id"], "s3_dir": p["s3_dir"]}
            for p in disp_cmr
        ]
        ccslc_grq_clean = [
            {"id": p["id"], "index": p["index"], "s3_dir": p["s3_dir"]}
            for p in ccslc_grq
        ]

        manifest.append({
            "frame_id": fid,
            "priority": frame_info["priority"],
            "new_frame_state": frame_info["new_frame_state"],
            "affected_kcycle": frame_info["affected_kcycle"],
            "kcycles_to_delete": frame_info["kcycles_to_delete"],
            "disp_grq_products": disp_grq_clean,
            "disp_cmr_granules": disp_cmr_clean,
            "ccslc_grq_products": ccslc_grq_clean,
            "summary": {
                "disp_grq_count": len(disp_grq_clean),
                "disp_cmr_count": len(disp_cmr_clean),
                "ccslc_grq_count": len(ccslc_grq_clean),
            },
        })

    return manifest


def print_summary(manifest, disp):
    total_disp_grq = sum(f["summary"]["disp_grq_count"] for f in manifest)
    total_disp_cmr = sum(f["summary"]["disp_cmr_count"] for f in manifest)
    total_ccslc = sum(f["summary"]["ccslc_grq_count"] for f in manifest)

    print(f"\n{'='*80}")
    print(f"DISP-S1 K-Cycle Cleanup Manifest — {len(manifest)} frames")
    print(f"{'='*80}")
    print(f"\n{'Frame':>7}  {'Pri':>3}  {'State':>5}  {'K-del':>10}  "
          f"{'DISP GRQ':>8}  {'DISP CMR':>8}  {'CCSLC':>6}")
    print(f"{'-'*7}  {'-'*3}  {'-'*5}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*6}")

    for f in manifest:
        s = f["summary"]
        kcycles = f["kcycles_to_delete"]
        if not kcycles:
            k_range = "N/A"
        elif len(kcycles) > 1:
            k_range = f"{kcycles[0]}-{kcycles[-1]}"
        else:
            k_range = str(kcycles[0])
        print(f"{f['frame_id']:>7}  {f['priority']:>3}  {f['new_frame_state']:>5}  "
              f"{k_range:>10}  {s['disp_grq_count']:>8}  {s['disp_cmr_count']:>8}  "
              f"{s['ccslc_grq_count']:>6}")

    print(f"{'-'*7}  {'-'*3}  {'-'*5}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*6}")
    print(f"{'TOTAL':>7}  {'':>3}  {'':>5}  {'':>10}  {total_disp_grq:>8}  "
          f"{total_disp_cmr:>8}  {total_ccslc:>6}")

    # Verification against source files
    expected_disp_grq = len(disp["grq_products"])
    expected_disp_cmr = len(disp["cmr_granules"])
    print(f"\n--- Verification ---")
    print(f"Frames in manifest:  {len(manifest)}")
    print(f"DISP GRQ products:   {total_disp_grq}  (source: {expected_disp_grq})"
          f"{'  OK' if total_disp_grq == expected_disp_grq else '  MISMATCH'}")
    print(f"DISP CMR granules:   {total_disp_cmr}  (source: {expected_disp_cmr})"
          f"{'  OK' if total_disp_cmr == expected_disp_cmr else '  MISMATCH'}")
    print(f"CCSLC GRQ products:  {total_ccslc}")

    # Check for frames with no products in any category
    empty_ccslc = [f["frame_id"] for f in manifest if f["summary"]["ccslc_grq_count"] == 0]
    if empty_ccslc:
        print(f"\nFrames with no CCSLCs: {empty_ccslc}")

    empty_all = [f["frame_id"] for f in manifest
                 if all(v == 0 for v in f["summary"].values())]
    if empty_all:
        print(f"Frames with no products at all: {empty_all}")


def main():
    affected, disp, ccslcs = load_inputs()
    manifest = build_manifest(affected, disp, ccslcs)

    output_path = SCRIPT_DIR / "cleanup_manifest.json"
    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {output_path}")

    print_summary(manifest, disp)


if __name__ == "__main__":
    main()
