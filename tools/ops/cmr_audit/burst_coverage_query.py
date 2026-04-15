#!/usr/bin/env python3
"""
Query burst coverage status for a specific SLC granule.

Parses JSONL output from cmr_audit_burst_coverage.py and displays the
coverage status for all bursts from a given SLC.

Usage:
    python burst_coverage_query.py <jsonl_file> <slc_id> [--frame-to-burst <path>]

Examples:
    python burst_coverage_query.py coverage.jsonl S1A_IW_SLC__1SDV_20240115T000509_20240115T000539_052110_064C5C_96CA-SLC

    # Partial match (finds SLCs containing the string)
    python burst_coverage_query.py coverage.jsonl 20240115T000509

    # Include frame IDs (requires frame-to-burst mapping file)
    python burst_coverage_query.py coverage.jsonl 20240115T000509 \\
        --frame-to-burst opera-s1-disp-frame-to-burst.json
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

# Default S3 URL for frame-to-burst mapping (static frame definitions, no sensing times)
DEFAULT_FRAME_TO_BURST_URL = "https://opera-ancillaries.s3.us-west-2.amazonaws.com/disp_frames/disp-s1/0.5.10/opera-s1-disp-0.9.0-frame-to-burst.json"


def load_frame_to_burst(db_path):
    """
    Load frame-to-burst mapping and return burst_to_frames dict.

    Supports two formats:
    - frame-to-burst.json: uses 'burst_ids' field (static frame definitions)
    - consistent-burst-ids.json: uses 'burst_id_list' field

    Returns dict mapping burst_id (e.g., 'T018-036765-IW1') -> list of frame IDs.
    """
    burst_to_frames = {}

    try:
        with open(db_path) as f:
            data = json.load(f).get("data", {})

        for frame_id, frame_data in data.items():
            # Support both field names: 'burst_ids' (frame-to-burst) and 'burst_id_list' (consistent db)
            burst_ids = frame_data.get("burst_ids") or frame_data.get("burst_id_list", [])
            for burst_id in burst_ids:
                # Normalize burst ID to uppercase with hyphens
                burst_id = burst_id.upper().replace("_", "-")
                if burst_id not in burst_to_frames:
                    burst_to_frames[burst_id] = []
                burst_to_frames[burst_id].append(int(frame_id))

        # Sort frame lists
        for burst_id in burst_to_frames:
            burst_to_frames[burst_id].sort()

    except Exception as e:
        print(f"Warning: Could not load frame-to-burst mapping: {e}", file=sys.stderr)
        return None

    return burst_to_frames


def download_frame_to_burst(url, cache_dir=None):
    """Download frame-to-burst mapping from URL and cache locally."""
    import urllib.request

    if cache_dir is None:
        cache_dir = Path.home() / ".cache" / "cmr_audit_burst"
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_file = cache_dir / "frame-to-burst.json"

    # Use cached version if it exists and is less than 30 days old (static data)
    if cache_file.exists():
        import time
        age_days = (time.time() - cache_file.stat().st_mtime) / 86400
        if age_days < 30:
            return cache_file

    try:
        print(f"Downloading frame-to-burst mapping from {url}...", file=sys.stderr)
        urllib.request.urlretrieve(url, cache_file)
        print(f"Cached to {cache_file}", file=sys.stderr)
        return cache_file
    except Exception as e:
        print(f"Warning: Could not download frame-to-burst mapping: {e}", file=sys.stderr)
        if cache_file.exists():
            print(f"Using cached version", file=sys.stderr)
            return cache_file
        return None


def normalize_burst_id(burst_id):
    """Normalize burst ID to hyphenated uppercase format for database lookup."""
    return burst_id.upper().replace("_", "-")


def get_frame_ids(burst_id, burst_to_frames):
    """Get frame IDs for a burst, or None if not in database."""
    if burst_to_frames is None:
        return None
    normalized = normalize_burst_id(burst_id)
    return burst_to_frames.get(normalized)


def load_jsonl(jsonl_path):
    """Load and parse JSONL file, returning chunks."""
    chunks = []
    with open(jsonl_path) as f:
        for line in f:
            record = json.loads(line)
            if record.get("_type") == "chunk_result":
                chunks.append(record)
    return chunks


def find_bursts_for_slc(chunks, slc_id, exact_match=False):
    """
    Find all bursts from a specific SLC.

    Returns dict with 'found' and 'missing' lists of burst records.
    """
    results = {"found": [], "missing": []}

    for chunk in chunks:
        # Check found bursts
        for burst in chunk.get("found", []):
            slc_native_id = burst.get("slc_native_id", "")
            if exact_match:
                if slc_native_id == slc_id:
                    results["found"].append(burst)
            else:
                if slc_id in slc_native_id:
                    results["found"].append(burst)

        # Check missing bursts
        for burst in chunk.get("missing", []):
            slc_native_id = burst.get("slc_native_id", "")
            if exact_match:
                if slc_native_id == slc_id:
                    results["missing"].append(burst)
            else:
                if slc_id in slc_native_id:
                    results["missing"].append(burst)

    return results


def get_unique_slcs(results):
    """Get unique SLC IDs from results."""
    slcs = set()
    for burst in results["found"] + results["missing"]:
        slc_id = burst.get("slc_native_id")
        if slc_id:
            slcs.add(slc_id)
    return sorted(slcs)


def format_frame_ids(frame_ids):
    """Format frame IDs for display."""
    if frame_ids is None:
        return "-"
    if not frame_ids:
        return "(none)"
    return ",".join(str(f) for f in frame_ids)


def is_in_frame(frame_ids):
    """Check if burst is in any DISP-S1 frame."""
    return frame_ids is not None and len(frame_ids) > 0


def print_slc_coverage(slc_id, results, burst_to_frames=None):
    """Print coverage details for a single SLC."""
    # Group bursts by burst_id
    bursts = {}
    for burst in results["found"]:
        bid = burst.get("burst_id", "unknown")
        frame_ids = get_frame_ids(bid, burst_to_frames)
        bursts[bid] = {
            "status": "FOUND",
            "burst_id": bid,
            "burst_pattern": burst.get("burst_pattern", ""),
            "acquisition_time": burst.get("acquisition_time", ""),
            "polarization": burst.get("polarization", ""),
            "opera_product_id": burst.get("opera_product_id", ""),
            "frame_ids": frame_ids,
            "in_frame": is_in_frame(frame_ids) if burst_to_frames else None,
        }

    for burst in results["missing"]:
        bid = burst.get("burst_id", "unknown")
        frame_ids = get_frame_ids(bid, burst_to_frames)
        bursts[bid] = {
            "status": "MISSING",
            "burst_id": bid,
            "burst_pattern": burst.get("burst_pattern", ""),
            "acquisition_time": burst.get("acquisition_time", ""),
            "polarization": burst.get("polarization", ""),
            "opera_product_id": None,
            "frame_ids": frame_ids,
            "in_frame": is_in_frame(frame_ids) if burst_to_frames else None,
        }

    # Sort by burst_id
    sorted_bursts = sorted(bursts.values(), key=lambda x: x["burst_id"])

    # Check if we have frame info
    has_frames = burst_to_frames is not None

    # Calculate stats
    found_count = len(results["found"])
    missing_count = len(results["missing"])
    total = found_count + missing_count
    coverage = (found_count / total * 100) if total > 0 else 0

    # Calculate frame-aware stats if we have frame info
    if has_frames:
        found_in_frame = sum(1 for b in sorted_bursts if b["status"] == "FOUND" and b["in_frame"])
        found_not_in_frame = sum(1 for b in sorted_bursts if b["status"] == "FOUND" and not b["in_frame"])
        missing_in_frame = sum(1 for b in sorted_bursts if b["status"] == "MISSING" and b["in_frame"])
        missing_not_in_frame = sum(1 for b in sorted_bursts if b["status"] == "MISSING" and not b["in_frame"])
        in_frame_total = found_in_frame + missing_in_frame
        frame_coverage = (found_in_frame / in_frame_total * 100) if in_frame_total > 0 else 0

    # Print header
    print("=" * 100)
    print(f"SLC: {slc_id}")
    print("=" * 100)
    print(f"Total bursts: {found_count} found, {missing_count} missing ({coverage:.1f}% coverage)")

    if has_frames:
        print()
        print(f"Frame coverage (bursts in DISP-S1 frames):")
        print(f"  In frames:     {found_in_frame} found, {missing_in_frame} missing ({frame_coverage:.1f}% coverage)")
        print(f"  Not in frames: {found_not_in_frame} found, {missing_not_in_frame} missing (not used by DISP-S1)")
    print()

    if not sorted_bursts:
        print("No bursts found for this SLC.")
        return

    # Group by subswath for nicer output
    by_subswath = defaultdict(list)
    for b in sorted_bursts:
        # Extract subswath from burst_id (e.g., T063_133460_IW1 -> IW1)
        parts = b["burst_id"].split("_")
        subswath = parts[-1] if len(parts) >= 3 else "unknown"
        by_subswath[subswath].append(b)

    # Print by subswath
    for subswath in sorted(by_subswath.keys()):
        swath_bursts = by_subswath[subswath]
        swath_found = sum(1 for b in swath_bursts if b["status"] == "FOUND")
        print(f"{subswath}: {swath_found}/{len(swath_bursts)} found")
        print("-" * 100)
        if has_frames:
            print(f"  {'Status':<10} {'Burst ID':<20} {'Frame(s)':<12} {'Acquisition Time':<24} {'Pol':<4}")
        else:
            print(f"  {'Status':<10} {'Burst ID':<20} {'Acquisition Time':<24} {'Pol':<4}")
        print("-" * 100)

        for b in swath_bursts:
            # Determine status marker based on found/missing and in-frame
            if has_frames:
                if b["status"] == "FOUND":
                    if b["in_frame"]:
                        status_marker = "✓"
                        status_text = "FOUND"
                    else:
                        status_marker = "○"  # Found but not in any frame
                        status_text = "FOUND*"
                else:  # MISSING
                    if b["in_frame"]:
                        status_marker = "✗"
                        status_text = "MISSING"
                    else:
                        status_marker = "·"  # Missing but not needed
                        status_text = "MISSING*"
            else:
                status_marker = "✓" if b["status"] == "FOUND" else "✗"
                status_text = b["status"]

            if has_frames:
                frames = format_frame_ids(b["frame_ids"])
                print(f"  {status_marker} {status_text:<8} {b['burst_id']:<20} {frames:<12} {b['acquisition_time']:<24} {b['polarization']:<4}")
            else:
                print(f"  {status_marker} {status_text:<8} {b['burst_id']:<20} {b['acquisition_time']:<24} {b['polarization']:<4}")
        print()

    # Print legend if we have frame info
    if has_frames:
        print("Legend: ✓ Found (in frame)  ○ Found (not in frame)  ✗ Missing (in frame)  · Missing (not in frame)")
        print("        * = burst not in any DISP-S1 frame (not needed for processing)")
        print()

    # Print missing burst IDs that are in frames (actionable items)
    missing_in_frames = [b for b in sorted_bursts if b["status"] == "MISSING" and (not has_frames or b["in_frame"])]
    if missing_in_frames:
        if has_frames:
            print("Missing burst IDs (in DISP-S1 frames - need reprocessing):")
        else:
            print("Missing burst IDs:")
        print("-" * 100)
        for b in missing_in_frames:
            frames = format_frame_ids(b["frame_ids"]) if has_frames else ""
            if has_frames:
                print(f"  {b['burst_id']:<20} (frame: {frames})")
            else:
                print(f"  {b['burst_id']}")
        print()

    # Print missing bursts not in frames (informational)
    if has_frames:
        missing_not_in_frames = [b for b in sorted_bursts if b["status"] == "MISSING" and not b["in_frame"]]
        if missing_not_in_frames:
            print("Missing burst IDs (NOT in any DISP-S1 frame - no action needed):")
            print("-" * 100)
            for b in missing_not_in_frames:
                print(f"  {b['burst_id']:<20}")
            print()


def main():
    parser = argparse.ArgumentParser(
        description="Query burst coverage status for a specific SLC granule",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s coverage.jsonl S1A_IW_SLC__1SDV_20240115T000509_20240115T000539_052110_064C5C_96CA-SLC
  %(prog)s coverage.jsonl 20240115T000509
  %(prog)s coverage.jsonl 052110 --list-slcs
  %(prog)s coverage.jsonl 20240115 --frame-to-burst /path/to/frame-to-burst.json
        """,
    )
    parser.add_argument("jsonl_file", help="JSONL file from cmr_audit_burst_coverage.py")
    parser.add_argument("slc_id", help="SLC granule ID (or partial match)")
    parser.add_argument("--exact", action="store_true",
                        help="Require exact match for SLC ID")
    parser.add_argument("--list-slcs", action="store_true",
                        help="List matching SLC IDs without showing burst details")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON")
    parser.add_argument("--frame-to-burst", type=Path, metavar="FILE",
                        help="Path to frame-to-burst JSON file (for frame ID lookup)")
    parser.add_argument("--download-frames", action="store_true",
                        help="Download frame-to-burst mapping from S3 if not specified")
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl_file)
    if not jsonl_path.exists():
        print(f"File not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)

    # Load frame-to-burst mapping for frame lookup
    burst_to_frames = None
    if args.frame_to_burst:
        if not args.frame_to_burst.exists():
            print(f"Frame-to-burst file not found: {args.frame_to_burst}", file=sys.stderr)
            sys.exit(1)
        burst_to_frames = load_frame_to_burst(args.frame_to_burst)
        if burst_to_frames:
            print(f"Loaded frame-to-burst mapping: {len(burst_to_frames)} bursts", file=sys.stderr)
    elif args.download_frames:
        db_path = download_frame_to_burst(DEFAULT_FRAME_TO_BURST_URL)
        if db_path:
            burst_to_frames = load_frame_to_burst(db_path)
            if burst_to_frames:
                print(f"Loaded frame-to-burst mapping: {len(burst_to_frames)} bursts", file=sys.stderr)

    # Load data
    chunks = load_jsonl(jsonl_path)
    if not chunks:
        print("No chunk data found in JSONL file.", file=sys.stderr)
        sys.exit(1)

    # Find bursts
    results = find_bursts_for_slc(chunks, args.slc_id, args.exact)
    unique_slcs = get_unique_slcs(results)

    if not unique_slcs:
        print(f"No SLCs found matching: {args.slc_id}", file=sys.stderr)
        sys.exit(1)

    # List SLCs mode
    if args.list_slcs:
        print(f"Found {len(unique_slcs)} matching SLC(s):")
        for slc in unique_slcs:
            # Count bursts for this SLC
            slc_results = find_bursts_for_slc(chunks, slc, exact_match=True)
            found = len(slc_results["found"])
            missing = len(slc_results["missing"])
            total = found + missing
            pct = (found / total * 100) if total > 0 else 0
            status = "COMPLETE" if missing == 0 else f"{missing} MISSING"
            print(f"  {slc}  [{found}/{total} = {pct:.0f}%] {status}")
        sys.exit(0)

    # JSON output mode
    if args.json:
        output = {
            "query": args.slc_id,
            "slcs": {}
        }
        for slc in unique_slcs:
            slc_results = find_bursts_for_slc(chunks, slc, exact_match=True)
            # Add frame IDs to burst records
            for burst in slc_results["found"] + slc_results["missing"]:
                burst["frame_ids"] = get_frame_ids(burst.get("burst_id"), burst_to_frames)
            output["slcs"][slc] = {
                "found": slc_results["found"],
                "missing": slc_results["missing"],
                "found_count": len(slc_results["found"]),
                "missing_count": len(slc_results["missing"]),
            }
        print(json.dumps(output, indent=2))
        sys.exit(0)

    # Standard output - show details for each matching SLC
    if len(unique_slcs) > 1:
        print(f"Found {len(unique_slcs)} SLCs matching '{args.slc_id}':")
        print()

    for slc in unique_slcs:
        slc_results = find_bursts_for_slc(chunks, slc, exact_match=True)
        print_slc_coverage(slc, slc_results, burst_to_frames)


if __name__ == "__main__":
    main()
