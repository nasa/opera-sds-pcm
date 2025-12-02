#!/usr/bin/env python
"""
Diagnose DISP-S1 products for a specific frame to identify products causing
unexpected frame state values (e.g., non-multiples of k).

Usage:
    python diagnose_disp_s1_frame_products.py --frame <FRAME_ID>

Example:
    PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python diagnose_disp_s1_frame_products.py --frame 8622
"""

import sys
import argparse
import dateutil.parser
from datetime import datetime

from data_subscriber.cslc_utils import localize_disp_frame_burst_hist
from report.opera_validator.opv_disp_s1 import retrieve_disp_s1_from_cmr


def diagnose_frame(frame_id, start_date, end_date, k=15, show_last_n=20):
    """
    Query CMR for DISP-S1 products of a specific frame and analyze their positions.
    """
    # Load the disp burst map
    print(f"Loading DISP burst map...")
    frame_to_bursts, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()

    if frame_id not in frame_to_bursts:
        print(f"ERROR: Frame {frame_id} not found in DISP burst map")
        sys.exit(1)

    # Query CMR for frame's DISP-S1 products
    print(f"Querying CMR for frame {frame_id} DISP-S1 products...")
    print(f"  Date range: {start_date} to {end_date}")

    frames_to_check = {frame_id}
    cmr_products = retrieve_disp_s1_from_cmr(
        datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ"),
        datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ"),
        "OPS",
        frames_to_check,
        return_full_umm=True
    )

    print(f"\nFound {len(cmr_products)} DISP-S1 products for frame {frame_id}\n")

    if not cmr_products:
        print("No products found. Check date range or frame ID.")
        return

    # Get frame info
    frame = frame_to_bursts[frame_id]
    sensing_days_index = frame.sensing_datetime_days_index

    print(f"Frame {frame_id} info:")
    print(f"  Total sensing times in database: {len(sensing_days_index)}")
    print(f"  First sensing time: {frame.sensing_datetimes[0]}")
    print(f"  Last sensing time: {frame.sensing_datetimes[-1]}")
    print()

    # Extract and sort products by their end date / day index
    products_info = []
    for product in cmr_products:
        umm = product.get('umm', {})

        # Get product ID
        granule_ur = umm.get('GranuleUR', 'Unknown')

        # Get end date
        temporal = umm.get('TemporalExtent', {})
        range_dt = temporal.get('RangeDateTime', {})
        end_date_str = range_dt.get('EndingDateTime', '')

        if end_date_str:
            end_date_parsed = dateutil.parser.isoparse(end_date_str)

            # Calculate day index
            first_sensing = frame.sensing_datetimes[0]
            delta = end_date_parsed.replace(tzinfo=None) - first_sensing.replace(tzinfo=None)
            day_index = int(round(delta.total_seconds() / (24 * 3600)))

            # Find position in list
            try:
                index_position = sensing_days_index.index(day_index)
            except ValueError:
                index_position = -1

            products_info.append({
                'granule_ur': granule_ur,
                'end_date': end_date_str,
                'day_index': day_index,
                'index_position': index_position,
                'k_cycle': index_position // k if index_position >= 0 else -1,
                'position_in_k': index_position % k if index_position >= 0 else -1
            })

    # Sort by index position
    products_info.sort(key=lambda x: x['index_position'], reverse=True)

    # Find max index position
    max_index = max(p['index_position'] for p in products_info)
    expected_k_boundary = ((max_index // k) + 1) * k if max_index >= 0 else 0

    print("=" * 120)
    print(f"ANALYSIS FOR FRAME {frame_id}")
    print("=" * 120)
    print(f"Highest index position found: {max_index}")
    print(f"This means frame_state = {max_index + 1}")
    print(f"Expected k-aligned state (k={k}): {(max_index // k) * k} or {((max_index // k) + 1) * k}")
    print(f"Is frame_state a multiple of k? {'YES' if (max_index + 1) % k == 0 else 'NO'}")
    print()

    if (max_index + 1) % k != 0:
        remainder = (max_index + 1) % k
        print(f"⚠️  Frame state {max_index + 1} is NOT a multiple of {k}")
        print(f"   It is {remainder} positions past the last complete k-cycle boundary ({(max_index + 1) - remainder})")
        print(f"   This suggests {remainder} product(s) from an incomplete k-cycle or from forward/reprocessing")
        print()

    # Show the last N products (highest index positions)
    print("=" * 120)
    print(f"LAST {show_last_n} DISP-S1 PRODUCTS FOR FRAME {frame_id} (sorted by sensing time index, descending)")
    print("=" * 120)
    print(f"{'Index Pos':>10} | {'K-Cycle':>8} | {'Pos in K':>8} | {'Day Index':>10} | {'End Date':>25} | Product ID")
    print("-" * 120)

    for p in products_info[:show_last_n]:
        print(f"{p['index_position']:>10} | {p['k_cycle']:>8} | {p['position_in_k']:>8} | {p['day_index']:>10} | {p['end_date']:>25} | {p['granule_ur'][:55]}")

    print("-" * 120)
    print()

    # Find products beyond the last complete k-cycle
    last_complete_k_cycle = (max_index + 1) // k - 1 if (max_index + 1) % k == 0 else (max_index + 1) // k - 1
    boundary_index = (last_complete_k_cycle + 1) * k  # First index of incomplete k-cycle

    problematic_products = [p for p in products_info if p['index_position'] >= boundary_index]

    if problematic_products:
        print("=" * 120)
        print(f"PRODUCTS BEYOND LAST COMPLETE K-CYCLE (index >= {boundary_index})")
        print("These products are causing the non-k-aligned frame state")
        print("=" * 120)
        for p in sorted(problematic_products, key=lambda x: x['index_position']):
            print(f"Index {p['index_position']:>3} (K-cycle {p['k_cycle']}, position {p['position_in_k']} within cycle)")
            print(f"  Product: {p['granule_ur']}")
            print(f"  End date: {p['end_date']}")
            print(f"  Day index: {p['day_index']}")
            print()


def main():
    parser = argparse.ArgumentParser(
        description='Diagnose DISP-S1 products for a specific frame',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python diagnose_disp_s1_frame_products.py --frame 8622
    python diagnose_disp_s1_frame_products.py --frame 8622 --k 15 --show-last 30
    python diagnose_disp_s1_frame_products.py --frame 8622 --start-datetime 2020-01-01T00:00:00Z
        """
    )
    parser.add_argument('--frame', type=int, required=True, help='Frame ID to diagnose')
    parser.add_argument('--start-datetime', default='2016-07-01T00:00:00Z',
                        help='Start date for CMR query (default: 2016-07-01T00:00:00Z)')
    parser.add_argument('--end-datetime', default='2025-12-01T00:00:00Z',
                        help='End date for CMR query (default: 2025-12-01T00:00:00Z)')
    parser.add_argument('--k', type=int, default=15, help='K parameter (default: 15)')
    parser.add_argument('--show-last', type=int, default=20,
                        help='Number of most recent products to display (default: 20)')

    args = parser.parse_args()

    diagnose_frame(
        args.frame,
        args.start_datetime,
        args.end_datetime,
        args.k,
        args.show_last
    )


if __name__ == '__main__':
    main()
