#!/usr/bin/env python3
"""
Check status of the CMR burst coverage audit.

Parses JSONL output from cmr_audit_burst_coverage.py and displays progress,
coverage statistics, and optionally lists SLCs that need reprocessing.

Usage:
    python burst_coverage_status.py [options] [jsonl_file]

Options:
    --by-day       Show coverage aggregated by day instead of by chunk
    --by-month     Show coverage aggregated by month instead of by chunk
    --start-date   Only include data from this date onwards (YYYY-MM-DD)
    --end-date     Only include data up to this date (YYYY-MM-DD)
    --show-slcs    Show unique SLCs that need reprocessing for missing CSLCs
    --output-slcs  Write SLC list to file (one per line)

Examples:
    # Basic status
    python burst_coverage_status.py coverage.jsonl

    # Coverage by month for 2024
    python burst_coverage_status.py coverage.jsonl --by-month \\
        --start-date 2024-01-01 --end-date 2024-12-31

    # Export SLCs needing reprocessing
    python burst_coverage_status.py coverage.jsonl --output-slcs slcs_to_reprocess.txt
"""

import argparse
import gzip
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def aggregate_by_period(chunks, period="chunk"):
    """Aggregate found/missing counts by period (chunk, day, or month)."""
    if period == "chunk":
        # Return as-is
        results = []
        for c in chunks:
            results.append({
                "period": f"{c['chunk_start'][:10]} to {c['chunk_end'][:10]}",
                "found": c["found_count"],
                "missing": c["missing_count"],
            })
        return results

    # Aggregate by day or month from individual burst records
    aggregated = defaultdict(lambda: {"found": 0, "missing": 0})

    for chunk in chunks:
        # Process found bursts
        for burst in chunk.get("found", []):
            acq_time = burst.get("acquisition_time", "")[:10]  # YYYY-MM-DD
            if period == "month":
                key = acq_time[:7]  # YYYY-MM
            else:  # day
                key = acq_time
            if key:
                aggregated[key]["found"] += 1

        # Process missing bursts
        for burst in chunk.get("missing", []):
            acq_time = burst.get("acquisition_time", "")[:10]
            if period == "month":
                key = acq_time[:7]
            else:  # day
                key = acq_time
            if key:
                aggregated[key]["missing"] += 1

    # Convert to sorted list
    results = []
    for key in sorted(aggregated.keys()):
        results.append({
            "period": key,
            "found": aggregated[key]["found"],
            "missing": aggregated[key]["missing"],
        })
    return results


def extract_unique_slcs(chunks):
    """Extract unique SLC native IDs from missing bursts."""
    slc_ids = set()
    for chunk in chunks:
        for burst in chunk.get("missing", []):
            slc_id = burst.get("slc_native_id")
            if slc_id:
                slc_ids.add(slc_id)
    return sorted(slc_ids)


def filter_chunks_by_date(chunks, start_date=None, end_date=None):
    """Filter chunk data to only include bursts within date range."""
    if not start_date and not end_date:
        return chunks

    filtered_chunks = []
    for chunk in chunks:
        filtered_found = []
        filtered_missing = []

        # Filter found bursts
        for burst in chunk.get("found", []):
            acq_date = burst.get("acquisition_time", "")[:10]
            if start_date and acq_date < start_date:
                continue
            if end_date and acq_date > end_date:
                continue
            filtered_found.append(burst)

        # Filter missing bursts
        for burst in chunk.get("missing", []):
            acq_date = burst.get("acquisition_time", "")[:10]
            if start_date and acq_date < start_date:
                continue
            if end_date and acq_date > end_date:
                continue
            filtered_missing.append(burst)

        # Only include chunk if it has data after filtering
        if filtered_found or filtered_missing:
            filtered_chunks.append({
                **chunk,
                "found": filtered_found,
                "missing": filtered_missing,
                "found_count": len(filtered_found),
                "missing_count": len(filtered_missing),
            })

    return filtered_chunks


def main():
    parser = argparse.ArgumentParser(
        description="Check CMR burst coverage audit status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s coverage.jsonl
  %(prog)s coverage.jsonl --by-month
  %(prog)s coverage.jsonl --start-date 2024-01-01 --show-slcs
  %(prog)s coverage.jsonl --output-slcs slcs_to_reprocess.txt
        """,
    )
    parser.add_argument("jsonl_file", help="JSONL file from cmr_audit_burst_coverage.py")
    parser.add_argument("--by-day", action="store_true",
                        help="Show coverage aggregated by day")
    parser.add_argument("--by-month", action="store_true",
                        help="Show coverage aggregated by month")
    parser.add_argument("--start-date", type=str, metavar="YYYY-MM-DD",
                        help="Only include data from this date onwards")
    parser.add_argument("--end-date", type=str, metavar="YYYY-MM-DD",
                        help="Only include data up to this date")
    parser.add_argument("--show-slcs", action="store_true",
                        help="Show unique SLCs that need reprocessing for missing CSLCs")
    parser.add_argument("--output-slcs", type=str, metavar="FILE",
                        help="Write SLC list to file (one per line)")
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl_file)

    if not jsonl_path.exists():
        print(f"File not found: {jsonl_path}")
        sys.exit(1)

    # Parse JSONL (supports gzipped files)
    metadata = None
    chunks = []
    summary = None

    opener = gzip.open if str(jsonl_path).endswith('.gz') else open
    with opener(jsonl_path, 'rt', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line)
            if record.get("_type") == "metadata":
                metadata = record
            elif record.get("_type") == "chunk_result":
                chunks.append(record)
            elif record.get("_type") == "summary":
                summary = record

    # Track original chunk count for progress
    original_chunk_count = len(chunks)

    # Calculate expected chunks from metadata if available
    total_chunks = None
    if metadata:
        # Check if total_chunks is in metadata
        if "total_chunks" in metadata:
            total_chunks = metadata["total_chunks"]
        # Otherwise calculate from date range and chunk_days
        elif "start_datetime" in metadata and "end_datetime" in metadata:
            start_str = metadata["start_datetime"].replace("Z", "+00:00")
            end_str = metadata["end_datetime"].replace("Z", "+00:00")
            start = datetime.fromisoformat(start_str)
            end = datetime.fromisoformat(end_str)
            chunk_days = metadata.get("chunk_days", 30)
            total_days = (end - start).days
            total_chunks = (total_days + chunk_days - 1) // chunk_days  # Ceiling division

    # Apply date filter if specified
    if args.start_date or args.end_date:
        chunks = filter_chunks_by_date(chunks, args.start_date, args.end_date)

    # Calculate totals (after filtering)
    total_found = sum(c["found_count"] for c in chunks)
    total_missing = sum(c["missing_count"] for c in chunks)
    total_expected = total_found + total_missing
    overall_coverage = (total_found / total_expected * 100) if total_expected > 0 else 0

    # Print status
    print("=" * 70)
    print("OPERA Burst Coverage Audit Status")
    print("=" * 70)
    print()
    print(f"JSONL file: {jsonl_path}")
    if metadata and "start_datetime" in metadata and "end_datetime" in metadata:
        print(f"Date range: {metadata['start_datetime'][:10]} to {metadata['end_datetime'][:10]}")
    if total_chunks:
        print(f"Progress:   {original_chunk_count}/{total_chunks} chunks ({original_chunk_count/total_chunks*100:.1f}%)")
    else:
        print(f"Chunks:     {original_chunk_count}")

    # Show date filter if active
    if args.start_date or args.end_date:
        filter_desc = []
        if args.start_date:
            filter_desc.append(f"from {args.start_date}")
        if args.end_date:
            filter_desc.append(f"to {args.end_date}")
        print(f"Filter:     {' '.join(filter_desc)}")

    print()
    print(f"Cumulative totals{' (filtered)' if args.start_date or args.end_date else ''}:")
    print(f"  Found:    {total_found:,}")
    print(f"  Missing:  {total_missing:,}")
    print(f"  Coverage: {overall_coverage:.2f}%")
    print()

    # Determine aggregation period
    if args.by_day:
        period = "day"
        period_label = "day"
    elif args.by_month:
        period = "month"
        period_label = "month"
    else:
        period = "chunk"
        period_label = "chunk"

    # Aggregate data
    aggregated = aggregate_by_period(chunks, period)

    # Show results
    if aggregated:
        print(f"Coverage by {period_label} ({len(aggregated)} {period_label}s):")
        print("-" * 70)

        if period == "chunk":
            print(f"{'#':>3} {'Period':<27} {'Found':>10} {'Missing':>10} {'Coverage':>10}")
        else:
            print(f"{'#':>5} {'Period':<12} {'Found':>10} {'Missing':>10} {'Coverage':>10}")

        print("-" * 70)

        for i, row in enumerate(aggregated, 1):
            found = row["found"]
            missing = row["missing"]
            total = found + missing
            pct = (found / total * 100) if total > 0 else 0

            if period == "chunk":
                print(f"{i:>3} {row['period']:<27} {found:>10,} {missing:>10,} {pct:>9.1f}%")
            else:
                print(f"{i:>5} {row['period']:<12} {found:>10,} {missing:>10,} {pct:>9.1f}%")

        print("-" * 70)

    # Show summary if complete
    if summary:
        print()
        print("AUDIT COMPLETE")
        print(json.dumps(summary, indent=2))

    # Extract and show/output unique SLCs for reprocessing
    if args.show_slcs or args.output_slcs:
        unique_slcs = extract_unique_slcs(chunks)

        if args.show_slcs:
            print()
            print(f"SLCs requiring reprocessing ({len(unique_slcs)} unique):")
            print("-" * 70)
            for slc in unique_slcs:
                print(slc)
            print("-" * 70)

        if args.output_slcs:
            with open(args.output_slcs, "w") as f:
                for slc in unique_slcs:
                    f.write(slc + "\n")
            print(f"\nWrote {len(unique_slcs)} SLC IDs to {args.output_slcs}")

    print()


if __name__ == "__main__":
    main()
