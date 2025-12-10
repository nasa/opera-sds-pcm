#!/usr/bin/env python
"""
Detect duplicate CSLC and DISP-S1 products in CMR.

Duplicate definitions:
- CSLC: Same burst_id + acquisition_datetime but different production times
- DISP-S1 (exact): Same frame + BeginningDateTime + EndingDateTime but different production times
- DISP-S1 (end conflict): Same frame + EndingDateTime but different BeginningDateTime

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
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from report.opera_validator.opv_util import retrieve_r3_products

# Product type constants
CSLC_SHORT_NAME = "OPERA_L2_CSLC-S1_V1"
DISP_S1_SHORT_NAME = "OPERA_L3_DISP-S1_V1"

# Regex patterns for parsing product IDs
CSLC_PATTERN = re.compile(
    r'OPERA_L2_CSLC-S1_'
    r'(?P<burst_id>T\d{3}-\d{6}-IW\d)'
    r'_(?P<acquisition_dt>\d{8}T\d{6}Z)'
    r'_\d{8}T\d{6}Z'  # validity start
    r'_S1[AB]'
    r'_VV'
    r'_v\d+\.\d+'
    r'_(?P<production_dt>\d{8}T\d{6}Z)'
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

    Returns:
        dict with burst_id, acquisition_dt, production_dt or None if parsing fails
    """
    match = CSLC_PATTERN.match(granule_id)
    if match:
        return {
            'burst_id': match.group('burst_id'),
            'acquisition_dt': match.group('acquisition_dt'),
            'production_dt': match.group('production_dt'),
            'granule_id': granule_id
        }
    return None


def parse_disp_s1_id(granule_id):
    """
    Parse DISP-S1 granule ID to extract key fields.

    Returns:
        dict with frame_id, begin_dt, end_dt, version, production_dt or None if parsing fails
    """
    match = DISP_S1_PATTERN.match(granule_id)
    if match:
        return {
            'frame_id': int(match.group('frame_id')),
            'begin_dt': match.group('begin_dt'),
            'end_dt': match.group('end_dt'),
            'version': match.group('version'),
            'production_dt': match.group('production_dt'),
            'granule_id': granule_id
        }
    return None


def detect_cslc_duplicates(start_date, end_date, endpoint="OPS", burst_ids=None, max_workers=10):
    """
    Detect duplicate CSLC products in CMR.

    Duplicates are defined as: same burst_id + acquisition_datetime but different production times.

    Args:
        start_date: Start datetime for query
        end_date: End datetime for query
        endpoint: CMR endpoint ('OPS' or 'UAT')
        burst_ids: Optional list of specific burst IDs to check
        max_workers: Number of parallel workers for querying

    Returns:
        dict with duplicate information
    """
    logging.info(f"Querying CMR for CSLC products from {start_date} to {end_date}...")

    # Query CMR for CSLC products
    products = retrieve_r3_products(start_date, end_date, endpoint, CSLC_SHORT_NAME)
    logging.info(f"Retrieved {len(products)} CSLC products from CMR")

    # Group by burst_id + acquisition_dt
    grouped = defaultdict(list)
    parse_failures = []

    for product in products:
        granule_id = product.get("umm", {}).get("GranuleUR", "")
        parsed = parse_cslc_id(granule_id)

        if parsed:
            # Filter by burst_ids if specified
            if burst_ids and parsed['burst_id'] not in burst_ids:
                continue
            key = (parsed['burst_id'], parsed['acquisition_dt'])
            grouped[key].append(parsed)
        else:
            parse_failures.append(granule_id)

    if parse_failures:
        logging.warning(f"Failed to parse {len(parse_failures)} CSLC granule IDs")
        logging.debug(f"Parse failures: {parse_failures[:10]}")

    # Find duplicates (groups with more than one product)
    duplicates = {}
    for key, items in grouped.items():
        if len(items) > 1:
            burst_id, acquisition_dt = key
            # Sort by production time to show oldest first
            items_sorted = sorted(items, key=lambda x: x['production_dt'])
            duplicates[f"{burst_id}_{acquisition_dt}"] = {
                'burst_id': burst_id,
                'acquisition_dt': acquisition_dt,
                'count': len(items),
                'products': [item['granule_id'] for item in items_sorted],
                'production_times': [item['production_dt'] for item in items_sorted]
            }

    return {
        'product_type': 'CSLC',
        'total_products_scanned': len(products),
        'total_duplicates_found': len(duplicates),
        'total_duplicate_products': sum(d['count'] for d in duplicates.values()),
        'duplicates': duplicates,
        'parse_failures': parse_failures[:100] if parse_failures else []
    }


def detect_disp_s1_duplicates(start_date, end_date, endpoint="OPS", frames=None):
    """
    Detect duplicate DISP-S1 products in CMR.

    Detects two types of duplicates:
    1. Exact duplicates: same frame + BeginningDateTime + EndingDateTime (different production times)
    2. End conflicts: same frame + EndingDateTime but different BeginningDateTime

    Args:
        start_date: Start datetime for query
        end_date: End datetime for query
        endpoint: CMR endpoint ('OPS' or 'UAT')
        frames: Optional list of specific frame IDs to check

    Returns:
        dict with duplicate information
    """
    logging.info(f"Querying CMR for DISP-S1 products from {start_date} to {end_date}...")

    all_products = []

    if frames:
        # Query specific frames
        for frame_id in frames:
            extra_params = {"attribute[]": f"int,FRAME_NUMBER,{frame_id}"}
            products = retrieve_r3_products(start_date, end_date, endpoint, DISP_S1_SHORT_NAME,
                                           extra_params=extra_params)
            all_products.extend(products)
            logging.info(f"Frame {frame_id}: {len(products)} products")
    else:
        # Query all DISP-S1 products
        products = retrieve_r3_products(start_date, end_date, endpoint, DISP_S1_SHORT_NAME)
        all_products = products

    logging.info(f"Retrieved {len(all_products)} DISP-S1 products from CMR")

    # Group for exact duplicates: frame + begin_dt + end_dt
    exact_grouped = defaultdict(list)
    # Group for end conflicts: frame + end_dt
    end_grouped = defaultdict(list)
    parse_failures = []

    for product in all_products:
        granule_id = product.get("umm", {}).get("GranuleUR", "")
        parsed = parse_disp_s1_id(granule_id)

        if parsed:
            exact_key = (parsed['frame_id'], parsed['begin_dt'], parsed['end_dt'])
            end_key = (parsed['frame_id'], parsed['end_dt'])
            exact_grouped[exact_key].append(parsed)
            end_grouped[end_key].append(parsed)
        else:
            parse_failures.append(granule_id)

    if parse_failures:
        logging.warning(f"Failed to parse {len(parse_failures)} DISP-S1 granule IDs")
        logging.debug(f"Parse failures: {parse_failures[:10]}")

    # Find exact duplicates (same frame + begin + end, different production times)
    exact_duplicates = {}
    for key, items in exact_grouped.items():
        if len(items) > 1:
            frame_id, begin_dt, end_dt = key
            items_sorted = sorted(items, key=lambda x: x['production_dt'])
            dup_key = f"F{frame_id:05d}_{begin_dt}_{end_dt}"
            exact_duplicates[dup_key] = {
                'frame_id': frame_id,
                'begin_dt': begin_dt,
                'end_dt': end_dt,
                'count': len(items),
                'products': [item['granule_id'] for item in items_sorted],
                'production_times': [item['production_dt'] for item in items_sorted],
                'versions': [item['version'] for item in items_sorted]
            }

    # Find end conflicts (same frame + end_dt, but different begin_dt)
    end_conflicts = {}
    for key, items in end_grouped.items():
        if len(items) > 1:
            frame_id, end_dt = key
            # Check if there are different begin_dt values
            begin_dts = set(item['begin_dt'] for item in items)
            if len(begin_dts) > 1:
                items_sorted = sorted(items, key=lambda x: (x['begin_dt'], x['production_dt']))
                conflict_key = f"F{frame_id:05d}_{end_dt}"
                end_conflicts[conflict_key] = {
                    'frame_id': frame_id,
                    'end_dt': end_dt,
                    'begin_dts': sorted(list(begin_dts)),
                    'count': len(items),
                    'products': [item['granule_id'] for item in items_sorted],
                    'production_times': [item['production_dt'] for item in items_sorted],
                    'versions': [item['version'] for item in items_sorted]
                }

    return {
        'product_type': 'DISP-S1',
        'total_products_scanned': len(all_products),
        'exact_duplicates': {
            'description': 'Same frame + BeginningDateTime + EndingDateTime (different production times)',
            'total_found': len(exact_duplicates),
            'total_products': sum(d['count'] for d in exact_duplicates.values()),
            'duplicates': exact_duplicates
        },
        'end_conflicts': {
            'description': 'Same frame + EndingDateTime but different BeginningDateTime',
            'total_found': len(end_conflicts),
            'total_products': sum(d['count'] for d in end_conflicts.values()),
            'conflicts': end_conflicts
        },
        'parse_failures': parse_failures[:100] if parse_failures else []
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
    print()

    if not results['duplicates']:
        print("No duplicates found.")
        return

    print("-" * 100)
    print(f"{'Burst ID':<25} | {'Acquisition Time':<20} | {'Count':<6} | Production Times")
    print("-" * 100)

    for key, dup in sorted(results['duplicates'].items()):
        prod_times = ", ".join(dup['production_times'])
        print(f"{dup['burst_id']:<25} | {dup['acquisition_dt']:<20} | {dup['count']:<6} | {prod_times}")

    print("-" * 100)
    print()

    # Show detailed list of first few duplicates
    print("DETAILED DUPLICATE LIST (first 10 groups):")
    print("-" * 100)
    for i, (key, dup) in enumerate(sorted(results['duplicates'].items())[:10]):
        print(f"\nGroup {i+1}: {dup['burst_id']} @ {dup['acquisition_dt']}")
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
        description='Detect duplicate CSLC and DISP-S1 products in CMR',
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
        cslc_results = detect_cslc_duplicates(start_date, end_date, args.endpoint, burst_ids)
        results['CSLC'] = cslc_results
        print_cslc_report(cslc_results)

    if args.product_type in ['DISP-S1', 'both']:
        disp_results = detect_disp_s1_duplicates(start_date, end_date, args.endpoint, frames)
        results['DISP-S1'] = disp_results
        print_disp_s1_report(disp_results)

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
