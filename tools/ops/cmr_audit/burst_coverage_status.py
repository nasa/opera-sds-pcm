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
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


BURST_LISTS = ("found", "missing", "unverified")


def load_records(jsonl_path):
    """Parse a coverage JSONL into (metadata, chunks, errors, summary).

    ``chunks`` are the chunk_result records, ``errors`` the chunk_error records (chunks the audit
    could not list from CMR and skipped; they need a re-run with --recheck-dates).
    """
    metadata = None
    chunks = []
    errors = []
    summary = None
    with open(jsonl_path) as f:
        for line in f:
            record = json.loads(line)
            kind = record.get("_type")
            if kind == "metadata":
                metadata = record
            elif kind == "chunk_result":
                chunks.append(record)
            elif kind == "chunk_error":
                errors.append(record)
            elif kind == "summary":
                summary = record
    return metadata, chunks, errors, summary


def aggregate_by_period(chunks, period="chunk"):
    """Aggregate found/missing/unverified counts by period (chunk, day, or month)."""
    if period == "chunk":
        # Return as-is
        results = []
        for c in chunks:
            results.append({
                "period": f"{c['chunk_start'][:10]} to {c['chunk_end'][:10]}",
                "found": c["found_count"],
                "missing": c["missing_count"],
                "unverified": c.get("unverified_count", 0),
            })
        return results

    # Aggregate by day or month from individual burst records
    aggregated = defaultdict(lambda: {"found": 0, "missing": 0, "unverified": 0})

    for chunk in chunks:
        for status in BURST_LISTS:
            for burst in chunk.get(status, []):
                acq_time = burst.get("acquisition_time", "")[:10]  # YYYY-MM-DD
                if period == "month":
                    key = acq_time[:7]  # YYYY-MM
                else:  # day
                    key = acq_time
                if key:
                    aggregated[key][status] += 1

    # Convert to sorted list
    results = []
    for key in sorted(aggregated.keys()):
        results.append({
            "period": key,
            "found": aggregated[key]["found"],
            "missing": aggregated[key]["missing"],
            "unverified": aggregated[key]["unverified"],
        })
    return results


def extract_unique_slcs(chunks, status="missing"):
    """Extract unique SLC native IDs from the bursts of one status ("missing" or "unverified")."""
    slc_ids = set()
    for chunk in chunks:
        for burst in chunk.get(status, []):
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
        filtered = {}
        for status in BURST_LISTS:
            filtered[status] = []
            for burst in chunk.get(status, []):
                acq_date = burst.get("acquisition_time", "")[:10]
                if start_date and acq_date < start_date:
                    continue
                if end_date and acq_date > end_date:
                    continue
                filtered[status].append(burst)

        # Only include chunk if it has data after filtering
        if any(filtered.values()):
            filtered_chunks.append({
                **chunk,
                **filtered,
                "found_count": len(filtered["found"]),
                "missing_count": len(filtered["missing"]),
                "unverified_count": len(filtered["unverified"]),
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

    metadata, chunks, errors, summary = load_records(jsonl_path)

    # Track original chunk count for progress; a skipped chunk is processed, just with no result
    original_chunk_count = len(chunks)
    processed_chunk_count = original_chunk_count + len(errors)

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

    # Calculate totals (after filtering); coverage is over verified bursts
    total_found = sum(c["found_count"] for c in chunks)
    total_missing = sum(c["missing_count"] for c in chunks)
    total_unverified = sum(c.get("unverified_count", 0) for c in chunks)
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
    skipped_note = f", {len(errors)} skipped" if errors else ""
    if total_chunks:
        print(f"Progress:   {processed_chunk_count}/{total_chunks} chunks "
              f"({processed_chunk_count/total_chunks*100:.1f}%){skipped_note}")
    else:
        print(f"Chunks:     {processed_chunk_count}{skipped_note}")
    if errors:
        print()
        print(f"Skipped chunks (no complete SLC listing from CMR; re-run these dates with --recheck-dates): {len(errors)}")
        for e in errors:
            print(f"  {e['chunk_start'][:10]} to {e['chunk_end'][:10]}: {e.get('error', '')[:100]}")

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
    print(f"  Found:      {total_found:,}")
    print(f"  Missing:    {total_missing:,}")
    print(f"  Unverified: {total_unverified:,}" + ("  (CMR gave no complete answer; re-run those dates)" if total_unverified else ""))
    print(f"  Coverage:   {overall_coverage:.2f}% (of verified bursts)")
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
            print(f"{'#':>3} {'Period':<27} {'Found':>10} {'Missing':>10} {'Unverif.':>9} {'Coverage':>10}")
        else:
            print(f"{'#':>5} {'Period':<12} {'Found':>10} {'Missing':>10} {'Unverif.':>9} {'Coverage':>10}")

        print("-" * 70)

        for i, row in enumerate(aggregated, 1):
            found = row["found"]
            missing = row["missing"]
            unverified = row.get("unverified", 0)
            total = found + missing
            pct = (found / total * 100) if total > 0 else 0

            if period == "chunk":
                print(f"{i:>3} {row['period']:<27} {found:>10,} {missing:>10,} {unverified:>9,} {pct:>9.1f}%")
            else:
                print(f"{i:>5} {row['period']:<12} {found:>10,} {missing:>10,} {unverified:>9,} {pct:>9.1f}%")

        print("-" * 70)

    # Show summary if complete
    if summary:
        print()
        print("AUDIT COMPLETE")
        print(json.dumps(summary, indent=2))

    # Extract and show/output unique SLCs for reprocessing. SLCs that only have unverified bursts are
    # not reprocessing candidates yet: CMR never gave a complete answer for them, so they are listed apart.
    if args.show_slcs or args.output_slcs:
        unique_slcs = extract_unique_slcs(chunks, "missing")
        unverified_slcs = sorted(set(extract_unique_slcs(chunks, "unverified")) - set(unique_slcs))

        if args.show_slcs:
            print()
            print(f"SLCs requiring reprocessing ({len(unique_slcs)} unique):")
            print("-" * 70)
            for slc in unique_slcs:
                print(slc)
            print("-" * 70)
            if unverified_slcs:
                print()
                print(f"SLCs with unverified bursts only ({len(unverified_slcs)} unique; re-run their dates with "
                      "--recheck-dates before deciding):")
                print("-" * 70)
                for slc in unverified_slcs:
                    print(slc)
                print("-" * 70)

        if args.output_slcs:
            with open(args.output_slcs, "w") as f:
                for slc in unique_slcs:
                    f.write(slc + "\n")
            print(f"\nWrote {len(unique_slcs)} SLC IDs to {args.output_slcs}")
            if unverified_slcs:
                print(f"Not written: {len(unverified_slcs)} SLCs with unverified bursts only (recheck first)")

    print()


if __name__ == "__main__":
    main()
