#!/usr/bin/env python
"""
Memory-efficient detection of duplicate CSLC and DISP-S1 products in CMR.

Duplicate definitions:
- CSLC: Same burst_id + acquisition_datetime but different production times
- DISP-S1 (exact): Same frame + BeginningDateTime + EndingDateTime but different production times
- DISP-S1 (end conflict): Same frame + EndingDateTime but different BeginningDateTime

This script is optimized for large-scale queries by:
- Extracting only GranuleUR strings (not full UMM objects)
- Processing in batches with garbage collection
- Using memory-efficient data structures

Usage:
    python detect_cmr_duplicates.py --product-type CSLC --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z
    python detect_cmr_duplicates.py --product-type DISP-S1 --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z
    python detect_cmr_duplicates.py --product-type both --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z

Examples:
    # Check for CSLC duplicates in 2024
    python detect_cmr_duplicates.py --product-type CSLC --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z

    # Check for DISP-S1 duplicates for specific frames
    python detect_cmr_duplicates.py --product-type DISP-S1 --frames 9154,8622 --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z

    # Output to JSON file
    python detect_cmr_duplicates.py --product-type both --start 2024-01-01T00:00:00Z --end 2024-12-31T23:59:59Z --output duplicates.json
"""

import argparse
import gc
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import tqdm

from report.opera_validator.opv_util import retrieve_r3_products

# Product type constants
CSLC_SHORT_NAME = "OPERA_L2_CSLC-S1_V1"
DISP_S1_SHORT_NAME = "OPERA_L3_DISP-S1_V1"

# Regex patterns for parsing product IDs
# CSLC format: OPERA_L2_CSLC-S1_T151-322284-IW1_20160701T005554Z_20240611T005333Z_S1A_VV_v1.1
# Fields: burst_id, acquisition_ts, creation_ts (production time), satellite, polarization, version
CSLC_PATTERN = re.compile(
    r'OPERA_L2_CSLC-S1_'
    r'(?P<burst_id>T\d{3}-\d{6}-IW\d)'
    r'_(?P<acquisition_ts>\d{8}T\d{6}Z)'
    r'_(?P<creation_ts>\d{8}T\d{6}Z)'
    r'_(?P<satellite>S1[A-D])'
    r'_(?P<pol>VV|VH|HH|HV)'
    r'_v(?P<version>\d+\.\d+)'
)

DISP_S1_PATTERN = re.compile(
    r'OPERA_L3_DISP-S1_IW_'
    r'F(?P<frame_id>\d{5})'
    r'_(?P<pol>VV|HH)'
    r'_(?P<begin_dt>\d{8}T\d{6}Z)'
    r'_(?P<end_dt>\d{8}T\d{6}Z)'
    r'_v(?P<version>\d+\.\d+)'
    r'_(?P<production_dt>\d{8}T\d{6}Z)'
)


def parse_cslc_id(granule_id):
    """
    Parse CSLC granule ID to extract key fields.

    CSLC format: OPERA_L2_CSLC-S1_T151-322284-IW1_20160701T005554Z_20240611T005333Z_S1A_VV_v1.1
    Fields: burst_id, acquisition_ts, creation_ts (production datetime), satellite, pol, version

    Returns:
        tuple: (burst_id, acquisition_ts, creation_ts, version) or None if parsing fails
    """
    match = CSLC_PATTERN.match(granule_id)
    if match:
        return (
            match.group('burst_id'),
            match.group('acquisition_ts'),
            match.group('creation_ts'),
            match.group('version')
        )
    return None


def parse_disp_s1_id(granule_id):
    """
    Parse DISP-S1 granule ID to extract key fields.

    Returns:
        tuple: (frame_id, begin_dt, end_dt, version, production_dt) or None if parsing fails
    """
    match = DISP_S1_PATTERN.match(granule_id)
    if match:
        return (
            int(match.group('frame_id')),
            match.group('begin_dt'),
            match.group('end_dt'),
            match.group('version'),
            match.group('production_dt')
        )
    return None


def extract_granule_ids_from_response(products):
    """
    Extract only GranuleUR strings from CMR response to minimize memory usage.

    Args:
        products: List of UMM product objects from CMR

    Returns:
        List of GranuleUR strings
    """
    granule_ids = []
    for product in products:
        granule_id = product.get("umm", {}).get("GranuleUR", "")
        if granule_id:
            granule_ids.append(granule_id)
    return granule_ids


def generate_time_chunks(start_date, end_date, chunk_days=30):
    """
    Generate time chunks to avoid CMR's 1M result / 1000 page limit.

    Args:
        start_date: Start datetime
        end_date: End datetime
        chunk_days: Number of days per chunk (default: 30)

    Yields:
        Tuples of (chunk_start, chunk_end) datetimes
    """
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        yield (current, chunk_end)
        current = chunk_end


def detect_cslc_duplicates_memory_efficient(start_date, end_date, endpoint="OPS", burst_ids=None,
                                             batch_size=100000, chunk_days=30):
    """
    Detect duplicate CSLC products in CMR with memory-efficient processing.

    Duplicates are defined as: same burst_id + acquisition_datetime but different production times.

    Uses time-based chunking to avoid CMR's 1M result / 1000 page limit.

    Args:
        start_date: Start datetime for query
        end_date: End datetime for query
        endpoint: CMR endpoint ('OPS' or 'UAT')
        burst_ids: Optional list of specific burst IDs to check
        batch_size: Number of granule IDs to process before garbage collection
        chunk_days: Number of days per time chunk for CMR queries (default: 30)

    Returns:
        dict with duplicate information
    """
    logging.info(f"Querying CMR for CSLC products from {start_date} to {end_date}...")

    # Generate time chunks to avoid CMR page limit
    time_chunks = list(generate_time_chunks(start_date, end_date, chunk_days))
    logging.info(f"Split query into {len(time_chunks)} time chunks of ~{chunk_days} days each")

    # Use a set to deduplicate granule IDs (in case products appear in multiple time chunks)
    all_granule_ids_set = set()
    total_products_fetched = 0

    with tqdm.tqdm(total=len(time_chunks), desc="Querying CSLC time chunks", unit="chunks") as chunk_pbar:
        for chunk_start, chunk_end in time_chunks:
            # Query CMR for this time chunk
            products_raw = retrieve_r3_products(chunk_start, chunk_end, endpoint, CSLC_SHORT_NAME)
            chunk_count = len(products_raw)
            total_products_fetched += chunk_count

            # Immediately extract only GranuleUR strings and add to set (deduplicates)
            granule_ids = extract_granule_ids_from_response(products_raw)
            all_granule_ids_set.update(granule_ids)

            # Clear the raw products from memory
            del products_raw
            del granule_ids
            gc.collect()

            chunk_pbar.set_postfix(
                chunk=f"{chunk_start.strftime('%Y-%m-%d')}",
                fetched=chunk_count,
                unique=len(all_granule_ids_set)
            )
            chunk_pbar.update(1)

    # Convert set to list for processing
    granule_ids = list(all_granule_ids_set)
    del all_granule_ids_set
    gc.collect()

    total_products = len(granule_ids)
    logging.info(f"Retrieved {total_products_fetched} CSLC products from CMR ({total_products} unique)")
    logging.info(f"Processing {total_products} granule IDs for duplicates...")

    # Group by burst_id + acquisition_ts
    # CSLC duplicates: same burst_id + acquisition_ts but different creation_ts (production time) or version
    # Store: (creation_ts, version, granule_id)
    grouped = defaultdict(list)
    parse_failures = 0
    burst_ids_set = set(burst_ids) if burst_ids else None

    # Process with progress bar
    with tqdm.tqdm(total=len(granule_ids), desc="Processing CSLC granules", unit="granules") as pbar:
        for batch_start in range(0, len(granule_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(granule_ids))
            batch = granule_ids[batch_start:batch_end]

            for granule_id in batch:
                parsed = parse_cslc_id(granule_id)

                if parsed:
                    burst_id, acquisition_ts, creation_ts, version = parsed

                    # Filter by burst_ids if specified
                    if burst_ids_set and burst_id not in burst_ids_set:
                        continue

                    key = (burst_id, acquisition_ts)
                    # Store: (creation_ts, version, granule_id)
                    grouped[key].append((creation_ts, version, granule_id))
                else:
                    parse_failures += 1

            pbar.update(len(batch))

            # Garbage collection after each batch
            if batch_start > 0 and batch_start % (batch_size * 5) == 0:
                gc.collect()

    # Clear granule_ids list
    del granule_ids
    gc.collect()

    if parse_failures > 0:
        logging.warning(f"Failed to parse {parse_failures} CSLC granule IDs")

    # Find duplicates (groups with more than one product)
    # Duplicates = same burst_id + acquisition_ts appearing multiple times (different creation_ts/version)
    duplicates = {}
    total_duplicate_products = 0

    for key, items in grouped.items():
        if len(items) > 1:
            burst_id, acquisition_ts = key
            # Sort by creation_ts (production time) to show oldest first
            items_sorted = sorted(items, key=lambda x: x[0])
            dup_key = f"{burst_id}_{acquisition_ts}"
            duplicates[dup_key] = {
                'burst_id': burst_id,
                'acquisition_ts': acquisition_ts,
                'count': len(items),
                'products': [item[2] for item in items_sorted],
                'creation_times': [item[0] for item in items_sorted],
                'versions': [item[1] for item in items_sorted]
            }
            total_duplicate_products += len(items)

    # Clear grouped dict
    del grouped
    gc.collect()

    return {
        'product_type': 'CSLC',
        'total_products_scanned': total_products,
        'total_duplicates_found': len(duplicates),
        'total_duplicate_products': total_duplicate_products,
        'duplicates': duplicates,
        'parse_failures': parse_failures
    }


def detect_disp_s1_duplicates_memory_efficient(start_date, end_date, endpoint="OPS", frames=None,
                                                 batch_size=50000, chunk_days=90):
    """
    Detect duplicate DISP-S1 products in CMR with memory-efficient processing.

    Detects two types of duplicates:
    1. Exact duplicates: same frame + BeginningDateTime + EndingDateTime (different production times)
    2. End conflicts: same frame + EndingDateTime but different BeginningDateTime

    Uses time-based chunking to avoid CMR's 1M result / 1000 page limit.

    Args:
        start_date: Start datetime for query
        end_date: End datetime for query
        endpoint: CMR endpoint ('OPS' or 'UAT')
        frames: Optional list of specific frame IDs to check
        batch_size: Number of granule IDs to process before garbage collection
        chunk_days: Number of days per time chunk for CMR queries (default: 90)

    Returns:
        dict with duplicate information
    """
    logging.info(f"Querying CMR for DISP-S1 products from {start_date} to {end_date}...")

    # Use a set to deduplicate granule IDs (DISP-S1 products span long time ranges
    # and can appear in multiple time-chunked queries)
    all_granule_ids_set = set()
    total_products_fetched = 0

    if frames:
        # Query specific frames with progress bar
        with tqdm.tqdm(total=len(frames), desc="Querying frames", unit="frames") as pbar:
            for frame_id in frames:
                extra_params = {"attribute[]": f"int,FRAME_NUMBER,{frame_id}"}
                products_raw = retrieve_r3_products(start_date, end_date, endpoint, DISP_S1_SHORT_NAME,
                                                   extra_params=extra_params)
                # Immediately extract GranuleUR strings
                granule_ids = extract_granule_ids_from_response(products_raw)
                all_granule_ids_set.update(granule_ids)
                total_products_fetched += len(granule_ids)

                pbar.set_postfix(frame=frame_id, products=len(granule_ids), unique=len(all_granule_ids_set))
                pbar.update(1)

                # Clear raw products
                del products_raw
                del granule_ids

                # Garbage collection periodically
                if total_products_fetched % 10000 == 0:
                    gc.collect()

        gc.collect()
    else:
        # Query all DISP-S1 products using time chunks to avoid CMR page limit
        time_chunks = list(generate_time_chunks(start_date, end_date, chunk_days))
        logging.info(f"Split query into {len(time_chunks)} time chunks of ~{chunk_days} days each")

        with tqdm.tqdm(total=len(time_chunks), desc="Querying DISP-S1 time chunks", unit="chunks") as chunk_pbar:
            for chunk_start, chunk_end in time_chunks:
                products_raw = retrieve_r3_products(chunk_start, chunk_end, endpoint, DISP_S1_SHORT_NAME)
                chunk_count = len(products_raw)
                total_products_fetched += chunk_count

                # Immediately extract GranuleUR strings and add to set (deduplicates)
                granule_ids = extract_granule_ids_from_response(products_raw)
                all_granule_ids_set.update(granule_ids)

                # Clear raw products
                del products_raw
                del granule_ids
                gc.collect()

                chunk_pbar.set_postfix(
                    chunk=f"{chunk_start.strftime('%Y-%m-%d')}",
                    fetched=chunk_count,
                    unique=len(all_granule_ids_set)
                )
                chunk_pbar.update(1)

    # Convert set to list for processing
    all_granule_ids = list(all_granule_ids_set)
    del all_granule_ids_set
    gc.collect()

    total_products = len(all_granule_ids)
    logging.info(f"Retrieved {total_products_fetched} DISP-S1 granule IDs from CMR ({total_products} unique)")

    # Group for exact duplicates: frame + begin_dt + end_dt
    # Store minimal info: (production_dt, version, granule_id)
    exact_grouped = defaultdict(list)
    # Group for end conflicts: frame + end_dt
    # Store minimal info: (begin_dt, production_dt, version, granule_id)
    end_grouped = defaultdict(list)
    parse_failures = 0

    # Process with progress bar
    with tqdm.tqdm(total=len(all_granule_ids), desc="Processing DISP-S1 granules", unit="granules") as pbar:
        for batch_start in range(0, len(all_granule_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(all_granule_ids))
            batch = all_granule_ids[batch_start:batch_end]

            for granule_id in batch:
                parsed = parse_disp_s1_id(granule_id)

                if parsed:
                    frame_id, begin_dt, end_dt, version, production_dt = parsed
                    exact_key = (frame_id, begin_dt, end_dt)
                    end_key = (frame_id, end_dt)

                    # Store minimal info for each group type
                    exact_grouped[exact_key].append((production_dt, version, granule_id))
                    end_grouped[end_key].append((begin_dt, production_dt, version, granule_id))
                else:
                    parse_failures += 1

            pbar.update(len(batch))

            # Garbage collection after each batch
            if batch_start > 0 and batch_start % (batch_size * 5) == 0:
                gc.collect()

    # Clear granule_ids list
    del all_granule_ids
    gc.collect()

    if parse_failures > 0:
        logging.warning(f"Failed to parse {parse_failures} DISP-S1 granule IDs")

    # Find exact duplicates (same frame + begin + end, different production times)
    exact_duplicates = {}
    exact_total_products = 0

    for key, items in exact_grouped.items():
        if len(items) > 1:
            frame_id, begin_dt, end_dt = key
            items_sorted = sorted(items, key=lambda x: x[0])  # Sort by production_dt
            dup_key = f"F{frame_id:05d}_{begin_dt}_{end_dt}"
            exact_duplicates[dup_key] = {
                'frame_id': frame_id,
                'begin_dt': begin_dt,
                'end_dt': end_dt,
                'count': len(items),
                'products': [item[2] for item in items_sorted],
                'production_times': [item[0] for item in items_sorted],
                'versions': [item[1] for item in items_sorted]
            }
            exact_total_products += len(items)

    # Clear exact_grouped
    del exact_grouped
    gc.collect()

    # Find end conflicts (same frame + end_dt, but different begin_dt)
    end_conflicts = {}
    conflicts_total_products = 0

    for key, items in end_grouped.items():
        if len(items) > 1:
            frame_id, end_dt = key
            # Check if there are different begin_dt values
            begin_dts = set(item[0] for item in items)
            if len(begin_dts) > 1:
                items_sorted = sorted(items, key=lambda x: (x[0], x[1]))  # Sort by begin_dt, then production_dt
                conflict_key = f"F{frame_id:05d}_{end_dt}"
                end_conflicts[conflict_key] = {
                    'frame_id': frame_id,
                    'end_dt': end_dt,
                    'begin_dts': sorted(list(begin_dts)),
                    'count': len(items),
                    'products': [item[3] for item in items_sorted],
                    'production_times': [item[1] for item in items_sorted],
                    'versions': [item[2] for item in items_sorted]
                }
                conflicts_total_products += len(items)

    # Clear end_grouped
    del end_grouped
    gc.collect()

    return {
        'product_type': 'DISP-S1',
        'total_products_scanned': total_products,
        'exact_duplicates': {
            'description': 'Same frame + BeginningDateTime + EndingDateTime (different production times)',
            'total_found': len(exact_duplicates),
            'total_products': exact_total_products,
            'duplicates': exact_duplicates
        },
        'end_conflicts': {
            'description': 'Same frame + EndingDateTime but different BeginningDateTime',
            'total_found': len(end_conflicts),
            'total_products': conflicts_total_products,
            'conflicts': end_conflicts
        },
        'parse_failures': parse_failures
    }


def print_cslc_report(results):
    """Print human-readable report for CSLC duplicates."""
    print()
    print("=" * 100)
    print("CSLC DUPLICATE REPORT")
    print("=" * 100)
    print(f"Total products scanned: {results['total_products_scanned']}")
    print(f"Duplicate groups found: {results['total_duplicates_found']}")
    print(f"Total duplicate products: {results['total_duplicate_products']}")
    if results.get('parse_failures', 0) > 0:
        print(f"Parse failures: {results['parse_failures']}")
    print()

    if not results['duplicates']:
        print("No duplicates found.")
        return

    print("-" * 100)
    print(f"{'Burst ID':<25} | {'Acquisition Time':<20} | {'Count':<6} | Creation Times (Production) / Versions")
    print("-" * 100)

    for key, dup in sorted(results['duplicates'].items()):
        # Show creation_times (production times) and versions
        details = ", ".join(f"{t} (v{v})" for t, v in zip(dup['creation_times'], dup['versions']))
        print(f"{dup['burst_id']:<25} | {dup['acquisition_ts']:<20} | {dup['count']:<6} | {details}")

    print("-" * 100)
    print()

    # Show detailed list of first few duplicates
    print("DETAILED DUPLICATE LIST (first 10 groups):")
    print("-" * 100)
    for i, (key, dup) in enumerate(sorted(results['duplicates'].items())[:10]):
        print(f"\nGroup {i+1}: {dup['burst_id']} @ {dup['acquisition_ts']}")
        for product in dup['products']:
            print(f"  - {product}")

    if len(results['duplicates']) > 10:
        print(f"\n... and {len(results['duplicates']) - 10} more duplicate groups")


def print_disp_s1_report(results):
    """Print human-readable report for DISP-S1 duplicates."""
    print()
    print("=" * 100)
    print("DISP-S1 DUPLICATE REPORT")
    print("=" * 100)
    print(f"Total products scanned: {results['total_products_scanned']}")
    if results.get('parse_failures', 0) > 0:
        print(f"Parse failures: {results['parse_failures']}")
    print()

    # Exact duplicates section
    exact = results['exact_duplicates']
    print("-" * 100)
    print(f"EXACT DUPLICATES: {exact['description']}")
    print("-" * 100)
    print(f"Duplicate groups found: {exact['total_found']}")
    print(f"Total duplicate products: {exact['total_products']}")
    print()

    if exact['duplicates']:
        print(f"{'Frame':<8} | {'Begin DateTime':<20} | {'End DateTime':<20} | {'Count':<6} | Production Times")
        print("-" * 100)
        for key, dup in sorted(exact['duplicates'].items()):
            prod_times = ", ".join(dup['production_times'])
            print(f"F{dup['frame_id']:05d}  | {dup['begin_dt']:<20} | {dup['end_dt']:<20} | {dup['count']:<6} | {prod_times}")
        print()

        # Detailed list
        print("DETAILED EXACT DUPLICATES (first 10 groups):")
        print("-" * 100)
        for i, (key, dup) in enumerate(sorted(exact['duplicates'].items())[:10]):
            print(f"\nGroup {i+1}: Frame {dup['frame_id']} | {dup['begin_dt']} to {dup['end_dt']}")
            for product in dup['products']:
                print(f"  - {product}")

        if len(exact['duplicates']) > 10:
            print(f"\n... and {len(exact['duplicates']) - 10} more duplicate groups")
    else:
        print("No exact duplicates found.")

    print()
    print()

    # End conflicts section
    conflicts = results['end_conflicts']
    print("-" * 100)
    print(f"END DATETIME CONFLICTS: {conflicts['description']}")
    print("-" * 100)
    print(f"Conflict groups found: {conflicts['total_found']}")
    print(f"Total conflicting products: {conflicts['total_products']}")
    print()

    if conflicts['conflicts']:
        print(f"{'Frame':<8} | {'End DateTime':<20} | {'Count':<6} | Begin DateTimes")
        print("-" * 100)
        for key, conf in sorted(conflicts['conflicts'].items()):
            begin_dts = ", ".join(conf['begin_dts'])
            print(f"F{conf['frame_id']:05d}  | {conf['end_dt']:<20} | {conf['count']:<6} | {begin_dts}")
        print()

        # Detailed list
        print("DETAILED END CONFLICTS (first 10 groups):")
        print("-" * 100)
        for i, (key, conf) in enumerate(sorted(conflicts['conflicts'].items())[:10]):
            print(f"\nConflict {i+1}: Frame {conf['frame_id']} | End: {conf['end_dt']}")
            print(f"  Different begin times: {conf['begin_dts']}")
            for product in conf['products']:
                print(f"  - {product}")

        if len(conflicts['conflicts']) > 10:
            print(f"\n... and {len(conflicts['conflicts']) - 10} more conflict groups")
    else:
        print("No end datetime conflicts found.")

    print()


def main():
    parser = argparse.ArgumentParser(
        description='Detect duplicate CSLC and DISP-S1 products in CMR (memory-efficient)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--product-type', required=True, choices=['CSLC', 'DISP-S1', 'both'],
                        help='Product type to check for duplicates')
    parser.add_argument('--start', required=True,
                        help='Start datetime (ISO format, e.g., 2024-01-01T00:00:00Z)')
    parser.add_argument('--end', required=True,
                        help='End datetime (ISO format, e.g., 2024-12-31T23:59:59Z)')
    parser.add_argument('--endpoint', default='OPS', choices=['OPS', 'UAT'],
                        help='CMR endpoint (default: OPS)')
    parser.add_argument('--frames', type=str, default=None,
                        help='Comma-separated list of frame IDs for DISP-S1 (default: all frames)')
    parser.add_argument('--burst-ids', type=str, default=None,
                        help='Comma-separated list of burst IDs for CSLC (default: all bursts)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output JSON file path (optional)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='[%(levelname)s] %(message)s')

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        print(f"Error parsing dates: {e}", file=sys.stderr)
        print("Use ISO format: YYYY-MM-DDTHH:MM:SSZ", file=sys.stderr)
        sys.exit(1)

    # Parse optional filters
    frames = None
    if args.frames:
        frames = [int(f.strip()) for f in args.frames.split(',')]

    burst_ids = None
    if args.burst_ids:
        burst_ids = [b.strip() for b in args.burst_ids.split(',')]

    results = {}

    # Run duplicate detection
    if args.product_type in ['CSLC', 'both']:
        cslc_results = detect_cslc_duplicates_memory_efficient(start_date, end_date, args.endpoint, burst_ids)
        results['CSLC'] = cslc_results
        print_cslc_report(cslc_results)
        gc.collect()

    if args.product_type in ['DISP-S1', 'both']:
        disp_results = detect_disp_s1_duplicates_memory_efficient(start_date, end_date, args.endpoint, frames)
        results['DISP-S1'] = disp_results
        print_disp_s1_report(disp_results)
        gc.collect()

    # Output to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to: {args.output}")

    # Summary
    print()
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)

    if 'CSLC' in results:
        r = results['CSLC']
        print(f"CSLC: {r['total_duplicates_found']} duplicate groups ({r['total_duplicate_products']} products)")

    if 'DISP-S1' in results:
        r = results['DISP-S1']
        exact = r['exact_duplicates']
        conflicts = r['end_conflicts']
        print(f"DISP-S1 exact duplicates: {exact['total_found']} groups ({exact['total_products']} products)")
        print(f"DISP-S1 end conflicts: {conflicts['total_found']} groups ({conflicts['total_products']} products)")

    print()


if __name__ == '__main__':
    main()
