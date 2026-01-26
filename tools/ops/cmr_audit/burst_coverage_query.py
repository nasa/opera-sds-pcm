#!/usr/bin/env python3
"""
Query burst coverage status for a specific SLC granule.

Parses JSONL output from cmr_audit_burst_coverage.py and displays the
coverage status for all bursts from a given SLC.

Usage:
    python burst_coverage_query.py <jsonl_file> <slc_id>

Examples:
    python burst_coverage_query.py coverage.jsonl S1A_IW_SLC__1SDV_20240115T000509_20240115T000539_052110_064C5C_96CA-SLC

    # Partial match (finds SLCs containing the string)
    python burst_coverage_query.py coverage.jsonl 20240115T000509
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


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


def print_slc_coverage(slc_id, results):
    """Print coverage details for a single SLC."""
    # Group bursts by burst_id
    bursts = {}
    for burst in results["found"]:
        bid = burst.get("burst_id", "unknown")
        bursts[bid] = {
            "status": "FOUND",
            "burst_id": bid,
            "burst_pattern": burst.get("burst_pattern", ""),
            "acquisition_time": burst.get("acquisition_time", ""),
            "polarization": burst.get("polarization", ""),
            "opera_product_id": burst.get("opera_product_id", ""),
        }

    for burst in results["missing"]:
        bid = burst.get("burst_id", "unknown")
        bursts[bid] = {
            "status": "MISSING",
            "burst_id": bid,
            "burst_pattern": burst.get("burst_pattern", ""),
            "acquisition_time": burst.get("acquisition_time", ""),
            "polarization": burst.get("polarization", ""),
            "opera_product_id": None,
        }

    # Sort by burst_id
    sorted_bursts = sorted(bursts.values(), key=lambda x: x["burst_id"])

    # Calculate stats
    found_count = len(results["found"])
    missing_count = len(results["missing"])
    total = found_count + missing_count
    coverage = (found_count / total * 100) if total > 0 else 0

    # Print header
    print("=" * 80)
    print(f"SLC: {slc_id}")
    print("=" * 80)
    print(f"Coverage: {found_count}/{total} bursts ({coverage:.1f}%)")
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
        print("-" * 80)
        print(f"  {'Status':<8} {'Burst ID':<20} {'Acquisition Time':<24} {'Pol':<4}")
        print("-" * 80)

        for b in swath_bursts:
            status_marker = "✓" if b["status"] == "FOUND" else "✗"
            print(f"  {status_marker} {b['status']:<6} {b['burst_id']:<20} {b['acquisition_time']:<24} {b['polarization']:<4}")
        print()

    # Print missing burst IDs for easy copy/paste
    if missing_count > 0:
        print("Missing burst IDs:")
        print("-" * 80)
        for b in sorted_bursts:
            if b["status"] == "MISSING":
                print(f"  {b['burst_id']}")
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
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl_file)
    if not jsonl_path.exists():
        print(f"File not found: {jsonl_path}", file=sys.stderr)
        sys.exit(1)

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
        print_slc_coverage(slc, slc_results)


if __name__ == "__main__":
    main()
