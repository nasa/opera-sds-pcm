#!/usr/bin/env python
"""
Diagnose DISP-S1 products for a specific frame to identify products causing
unexpected frame state values (e.g., non-multiples of k), detect gaps in
product index sequences, and identify forward processing products mixed
with historical processing.

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


def analyze_index_gaps(products_info, k=15):
    """
    Analyze gaps in the product index sequence to identify missing products
    and forward processing products mixed with historical data.

    Args:
        products_info: List of product info dicts with 'index_position', 'k_cycle', etc.
        k: The k parameter (default 15)

    Returns:
        dict: Analysis results including gaps, missing indices, and recommendations
    """
    if not products_info:
        return None

    # Get all valid index positions (exclude -1 for products not in historical database)
    valid_products = [p for p in products_info if p['index_position'] >= 0]
    if not valid_products:
        return None

    # Sort by index position ascending
    sorted_products = sorted(valid_products, key=lambda x: x['index_position'])

    # Find all gaps (jumps of more than 1 between consecutive products)
    gaps = []
    for i in range(1, len(sorted_products)):
        prev_idx = sorted_products[i-1]['index_position']
        curr_idx = sorted_products[i]['index_position']

        if curr_idx - prev_idx > 1:
            gap_size = curr_idx - prev_idx - 1
            missing_indices = list(range(prev_idx + 1, curr_idx))

            # Determine what k-cycles are affected
            affected_k_cycles = set()
            for idx in missing_indices:
                affected_k_cycles.add(idx // k)

            gaps.append({
                'from_index': prev_idx,
                'to_index': curr_idx,
                'gap_size': gap_size,
                'missing_indices': missing_indices,
                'from_product': sorted_products[i-1],
                'to_product': sorted_products[i],
                'affected_k_cycles': sorted(affected_k_cycles),
                'from_date': sorted_products[i-1]['end_date'],
                'to_date': sorted_products[i]['end_date']
            })

    # Identify the last contiguous block of historical processing
    # and potential forward processing products
    max_index = max(p['index_position'] for p in valid_products)
    min_index = min(p['index_position'] for p in valid_products)

    # Find last complete k-cycle before any significant gap
    last_historical_index = None
    forward_processing_products = []

    if gaps:
        # Find the largest gap - often indicates transition from historical to forward
        largest_gap = max(gaps, key=lambda g: g['gap_size'])

        # If the gap is significant (>= k-1, i.e., nearly a full k-cycle or more),
        # it likely indicates forward processing mixed with historical.
        if largest_gap['gap_size'] >= k - 1:
            last_historical_index = largest_gap['from_index']
            # Products after the gap are likely forward processing
            for p in sorted_products:
                if p['index_position'] > largest_gap['from_index']:
                    forward_processing_products.append(p)

    # Calculate recommended frame state for batch proc
    # Should be the frame_state after the last complete k-cycle of historical data
    if last_historical_index is not None:
        # Align to k-boundary at or before the last historical index
        recommended_frame_state = ((last_historical_index // k) + 1) * k
        if recommended_frame_state > last_historical_index + 1:
            # If the k-cycle is incomplete, use the actual last historical position + 1
            recommended_frame_state = last_historical_index + 1
    else:
        # No large gaps, use max_index + 1
        recommended_frame_state = max_index + 1

    # Analyze gaps within k-cycles (indicates missing historical products)
    # This is different from forward processing gaps - these are holes in historical data
    gaps_within_k_cycles = []
    for gap in gaps:
        # Check if the gap is entirely within a single k-cycle or spans k-cycles
        from_k_cycle = gap['from_index'] // k
        to_k_cycle = gap['to_index'] // k

        # If gap spans multiple k-cycles or is significant, it's likely forward processing (handled above)
        # But if it's within a k-cycle or at the boundary, it indicates missing historical products
        if gap['gap_size'] < k - 1:
            gaps_within_k_cycles.append({
                **gap,
                'from_k_cycle': from_k_cycle,
                'to_k_cycle': to_k_cycle,
                'spans_k_cycles': from_k_cycle != to_k_cycle
            })

    return {
        'gaps': gaps,
        'total_gaps': len(gaps),
        'total_missing_indices': sum(g['gap_size'] for g in gaps),
        'min_index': min_index,
        'max_index': max_index,
        'last_historical_index': last_historical_index,
        'forward_processing_products': forward_processing_products,
        'recommended_frame_state': recommended_frame_state,
        'has_mixed_processing': len(forward_processing_products) > 0,
        'gaps_within_k_cycles': gaps_within_k_cycles,
        'has_historical_gaps': len(gaps_within_k_cycles) > 0
    }


def print_gap_analysis(analysis, frame_id, k=15):
    """
    Print detailed gap analysis in a readable format.
    """
    if not analysis:
        print("No gap analysis available (no valid products found).")
        return

    print()
    print("=" * 120)
    print(f"INDEX GAP ANALYSIS FOR FRAME {frame_id}")
    print("=" * 120)

    if analysis['total_gaps'] == 0:
        print("✓ No gaps detected in product index sequence.")
        print(f"  Products span continuously from index {analysis['min_index']} to {analysis['max_index']}")
        print()
        return

    print(f"⚠️  Found {analysis['total_gaps']} gap(s) in product index sequence")
    print(f"   Total missing product indices: {analysis['total_missing_indices']}")
    print()

    for i, gap in enumerate(analysis['gaps'], 1):
        print(f"GAP {i}: Index {gap['from_index']} → {gap['to_index']} ({gap['gap_size']} missing products)")
        print("-" * 80)
        print(f"  Before gap (last product before jump):")
        print(f"    Index position: {gap['from_index']}")
        print(f"    K-cycle: {gap['from_product']['k_cycle']}, Position in k-cycle: {gap['from_product']['position_in_k']}")
        print(f"    End date: {gap['from_date']}")
        print(f"    Product: {gap['from_product']['granule_ur'][:70]}")
        print()
        print(f"  After gap (first product after jump):")
        print(f"    Index position: {gap['to_index']}")
        print(f"    K-cycle: {gap['to_product']['k_cycle']}, Position in k-cycle: {gap['to_product']['position_in_k']}")
        print(f"    End date: {gap['to_date']}")
        print(f"    Product: {gap['to_product']['granule_ur'][:70]}")
        print()
        print(f"  Missing indices: {gap['from_index'] + 1} through {gap['to_index'] - 1}")
        print(f"  Affected k-cycles: {gap['affected_k_cycles']}")

        # Calculate time gap
        try:
            from_dt = dateutil.parser.isoparse(gap['from_date'])
            to_dt = dateutil.parser.isoparse(gap['to_date'])
            time_delta = to_dt - from_dt
            days_gap = time_delta.days
            print(f"  Time gap: ~{days_gap} days ({from_dt.strftime('%Y-%m-%d')} to {to_dt.strftime('%Y-%m-%d')})")
        except:
            pass

        print()

    # Mixed processing analysis
    if analysis['has_mixed_processing']:
        print("=" * 120)
        print("FORWARD PROCESSING PRODUCTS DETECTED")
        print("=" * 120)
        print(f"Found {len(analysis['forward_processing_products'])} product(s) that appear to be from forward processing")
        print(f"Last historical processing index: {analysis['last_historical_index']}")
        print()
        print("These products are beyond the main historical processing sequence and")
        print("are likely from forward (real-time) processing that ran while historical")
        print("processing was paused or stopped.")
        print()
        print("Forward processing products:")
        for p in analysis['forward_processing_products'][:10]:  # Show first 10
            print(f"  Index {p['index_position']:>4}: {p['end_date'][:10]} - {p['granule_ur'][:60]}")
        if len(analysis['forward_processing_products']) > 10:
            print(f"  ... and {len(analysis['forward_processing_products']) - 10} more")
        print()

    # Historical gaps warning (gaps within k-cycles that indicate missing products)
    if analysis.get('has_historical_gaps'):
        print("=" * 120)
        print("⚠️  MISSING HISTORICAL PRODUCTS DETECTED")
        print("=" * 120)
        print("The following gaps indicate missing products within historical processing:")
        print("These are NOT forward processing - they are holes in the historical data.")
        print()
        for gap in analysis['gaps_within_k_cycles']:
            print(f"  Gap: Index {gap['from_index']} → {gap['to_index']} ({gap['gap_size']} missing)")
            if gap['spans_k_cycles']:
                print(f"       Spans k-cycles {gap['from_k_cycle']} to {gap['to_k_cycle']}")
            else:
                print(f"       Within k-cycle {gap['from_k_cycle']}")
            print(f"       Missing indices: {gap['from_index'] + 1} through {gap['to_index'] - 1}")
            print()
        print("⚠️  WARNING: The frame_state may appear correct (k-aligned) but there are")
        print("   missing products that should have been generated by historical processing.")
        print("   These gaps need investigation - historical processing may have failed")
        print("   or been interrupted for these specific sensing times.")
        print()

    # Recommendation
    print("=" * 120)
    print("BATCH PROC RECOMMENDATION")
    print("=" * 120)
    if analysis['has_mixed_processing']:
        print(f"For historical processing batch proc, recommended frame_state: {analysis['recommended_frame_state']}")
        print()
        print("Explanation:")
        print(f"  - Historical processing appears to have stopped at index {analysis['last_historical_index']}")
        print(f"  - This corresponds to frame_state = {analysis['last_historical_index'] + 1}")
        print(f"  - Forward processing products (index {analysis['forward_processing_products'][0]['index_position']}+) should be ignored")
        print(f"    when calculating historical batch proc state")
        print()
        print("To resume historical processing:")
        print(f"  1. Set frame_state to {analysis['last_historical_index'] + 1} in your batch proc JSON")
        print(f"  2. This will resume from the k-cycle containing index {analysis['last_historical_index']}")
        print(f"  3. Products at indices {analysis['last_historical_index'] + 1} through {analysis['max_index']} ")
        print(f"     will need to be regenerated with correct historical inputs")
    elif analysis.get('has_historical_gaps'):
        actual_frame_state = analysis['max_index'] + 1
        print(f"Current frame_state based on CMR products: {actual_frame_state}")
        print()
        if actual_frame_state % k == 0:
            print(f"Frame state {actual_frame_state} appears k-aligned, BUT there are gaps!")
            print()
            print("⚠️  CAUTION: Although the frame_state looks correct, there are missing")
            print("   products within the historical sequence. You have two options:")
            print()
            print("   Option 1: Accept gaps and continue")
            print(f"     - Keep frame_state at {actual_frame_state}")
            print("     - Missing products will remain missing")
            print()
            print("   Option 2: Backfill missing products")
            print("     - Identify the first gap and set frame_state before it")
            first_gap = analysis['gaps_within_k_cycles'][0]
            backfill_state = first_gap['from_index'] + 1
            print(f"     - Set frame_state to {backfill_state} to reprocess from index {first_gap['from_index']}")
            print("     - This will regenerate products from that point forward")
        else:
            remainder = actual_frame_state % k
            last_k_boundary = actual_frame_state - remainder
            print(f"⚠️  Frame state {actual_frame_state} is not k-aligned AND has gaps")
            print(f"   Last complete k-cycle ends at state {last_k_boundary}")
            print(f"   {remainder} additional product(s) in incomplete k-cycle")
    else:
        actual_frame_state = analysis['max_index'] + 1
        print(f"Current frame_state based on CMR products: {actual_frame_state}")
        print()
        if actual_frame_state % k == 0:
            print(f"✓ Frame state {actual_frame_state} is aligned to k={k} boundary")
            print("✓ No gaps detected - historical processing appears complete")
        else:
            remainder = actual_frame_state % k
            last_k_boundary = actual_frame_state - remainder
            print(f"⚠️  Frame state {actual_frame_state} is not k-aligned")
            print(f"   Last complete k-cycle ends at state {last_k_boundary}")
            print(f"   {remainder} additional product(s) in incomplete k-cycle")
    print()


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

    # Count products by index validity
    valid_products = [p for p in products_info if p['index_position'] >= 0]
    invalid_products = [p for p in products_info if p['index_position'] < 0]

    print("=" * 120)
    print(f"ANALYSIS FOR FRAME {frame_id}")
    print("=" * 120)
    print(f"Products found in CMR: {len(cmr_products)}")
    print(f"  - With valid index position: {len(valid_products)}")
    print(f"  - Outside historical database (forward processing): {len(invalid_products)}")
    print()
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

    # Perform gap analysis
    gap_analysis = analyze_index_gaps(products_info, k)
    print_gap_analysis(gap_analysis, frame_id, k)

    # Consistency check
    if gap_analysis:
        expected_products = (max_index + 1) - gap_analysis['total_missing_indices']
        actual_products = len(valid_products)
        if expected_products != actual_products:
            print("=" * 120)
            print("⚠️  CONSISTENCY CHECK FAILED")
            print("=" * 120)
            print(f"Expected products (max_index + 1 - gaps): {expected_products}")
            print(f"Actual products with valid index: {actual_products}")
            print(f"Discrepancy: {actual_products - expected_products}")
            print()
            if actual_products < expected_products:
                print("This suggests there may be additional gaps not detected,")
                print("or duplicate index positions in the product list.")
            else:
                print("This suggests there may be duplicate products at the same index position.")
            print()

            # Check for duplicate index positions
            index_counts = {}
            for p in valid_products:
                idx = p['index_position']
                index_counts[idx] = index_counts.get(idx, 0) + 1

            duplicates = {idx: count for idx, count in index_counts.items() if count > 1}
            if duplicates:
                print("Duplicate index positions found:")
                for idx, count in sorted(duplicates.items()):
                    print(f"  Index {idx}: {count} products")
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
