#!/usr/bin/env python
"""
Comprehensive DISP-S1 frame audit tool.

For a given frame (or list of frames), this tool:
1. Queries all CSLC products from CMR for the frame's bursts
2. Calculates expected DISP-S1 products based on K-cycle logic
3. Queries actual DISP-S1 products from CMR
4. Validates input completeness for existing products via ISO XML
5. Handles duplicates by selecting the product with most complete inputs
6. Reports missing products with time range, day index, and K-cycle info

Usage:
    python audit_disp_s1_completeness.py --frames 9154
    python audit_disp_s1_completeness.py --frames 9154,8622,831
    python audit_disp_s1_completeness.py --frames 9154 --output audit_report.json

Examples:
    # Audit a single frame
    python audit_disp_s1_completeness.py --frames 9154

    # Audit multiple frames with JSON output
    python audit_disp_s1_completeness.py --frames 9154,8622 --output report.json

    # Use UAT endpoint
    python audit_disp_s1_completeness.py --frames 9154 --endpoint UAT
"""

import argparse
import gc
import json
import logging
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import tqdm

from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    parse_cslc_native_id,
)
from report.opera_validator.opv_util import retrieve_r3_products
from tools.ops.cmr_audit.cmr_iso_xml_utils import (
    get_iso_xml_url_from_umm,
    fetch_cslc_input_granules_from_iso_xml,
    configure_iso_xml_cache,
    get_cache_stats,
)

# Constants
CSLC_SHORT_NAME = "OPERA_L2_CSLC-S1_V1"
DISP_S1_SHORT_NAME = "OPERA_L3_DISP-S1_V1"
DEFAULT_K = 15
DEFAULT_MAX_WORKERS = 20
CMR_PAGE_LIMIT_DAYS = 30  # Time chunk size to avoid CMR 1000 page limit

# Regex for parsing DISP-S1 product IDs
DISP_S1_PATTERN = re.compile(
    r'OPERA_L3_DISP-S1_IW_'
    r'F(?P<frame_id>\d{5})'
    r'_(?P<pol>VV|HH)'
    r'_(?P<begin_dt>\d{8}T\d{6}Z)'
    r'_(?P<end_dt>\d{8}T\d{6}Z)'
    r'_v(?P<version>\d+\.\d+)'
    r'_(?P<production_dt>\d{8}T\d{6}Z)'
)


def parse_disp_s1_id(granule_id):
    """Parse DISP-S1 granule ID to extract key fields."""
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


def generate_time_chunks(start_date, end_date, chunk_days=CMR_PAGE_LIMIT_DAYS):
    """Generate time chunks to avoid CMR's 1M result / 1000 page limit."""
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        yield (current, chunk_end)
        current = chunk_end


def extract_granule_ids_from_response(products):
    """Extract only GranuleUR strings from CMR response."""
    return [p.get("umm", {}).get("GranuleUR", "") for p in products if p.get("umm", {}).get("GranuleUR")]


def query_cslcs_for_frame(frame_id, frame_to_bursts, start_date, end_date, endpoint="OPS",
                          chunk_days=CMR_PAGE_LIMIT_DAYS, max_workers=DEFAULT_MAX_WORKERS):
    """
    Query all CSLC products for a frame's bursts in parallel.

    Uses time chunking to avoid CMR page limits and deduplicates results.
    Parallelizes queries across burst/time-chunk combinations for speed.
    """
    frame = frame_to_bursts[frame_id]
    burst_ids = frame.burst_ids

    # Generate time chunks
    time_chunks = list(generate_time_chunks(start_date, end_date, chunk_days))

    # Build list of all (burst, chunk) pairs to query
    query_tasks = []
    for burst_id in burst_ids:
        for chunk_start, chunk_end in time_chunks:
            query_tasks.append((burst_id, chunk_start, chunk_end))

    logging.info(f"Frame {frame_id}: Querying CSLCs for {len(burst_ids)} bursts × {len(time_chunks)} time chunks = {len(query_tasks)} queries")

    def query_burst_chunk(task):
        """Query a single burst for a time chunk."""
        burst_id, chunk_start, chunk_end = task
        extra_params = {
            "options[native-id][pattern]": "true",
            "native-id[]": f"OPERA_L2_CSLC-S1_{burst_id}*"
        }
        try:
            products = retrieve_r3_products(
                chunk_start, chunk_end, endpoint, CSLC_SHORT_NAME,
                extra_params=extra_params
            )
            return extract_granule_ids_from_response(products)
        except Exception as e:
            logging.warning(f"Failed to query CSLCs for burst {burst_id}: {e}")
            return []

    # Use set for deduplication
    all_cslc_ids = set()
    total_fetched = 0

    with tqdm.tqdm(total=len(query_tasks), desc=f"Querying CSLCs for frame {frame_id}", unit="queries") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(query_burst_chunk, task): task for task in query_tasks}

            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_cslc_ids.update(result)
                    total_fetched += len(result)
                pbar.update(1)
                pbar.set_postfix(unique=len(all_cslc_ids), fetched=total_fetched)

    gc.collect()
    logging.info(f"Frame {frame_id}: Found {len(all_cslc_ids)} unique CSLCs (fetched {total_fetched})")
    return list(all_cslc_ids)


def query_cslcs_for_frames_parallel(frame_ids, frame_to_bursts, burst_to_frames, start_date, end_date,
                                     endpoint="OPS", max_workers=DEFAULT_MAX_WORKERS):
    """
    Query CSLCs for multiple frames in parallel, grouped by burst to avoid duplicate queries.
    """
    # Collect all unique burst IDs across all frames
    all_burst_ids = set()
    for frame_id in frame_ids:
        if frame_id in frame_to_bursts:
            all_burst_ids.update(frame_to_bursts[frame_id].burst_ids)

    logging.info(f"Querying CSLCs for {len(all_burst_ids)} unique bursts across {len(frame_ids)} frames")

    # Use set for deduplication
    all_cslc_ids = set()
    total_fetched = 0

    # Generate time chunks
    time_chunks = list(generate_time_chunks(start_date, end_date, CMR_PAGE_LIMIT_DAYS))

    def query_burst_chunk(burst_id, chunk_start, chunk_end):
        """Query a single burst for a time chunk."""
        extra_params = {
            "options[native-id][pattern]": "true",
            "native-id[]": f"OPERA_L2_CSLC-S1_{burst_id}*"
        }
        try:
            products = retrieve_r3_products(
                chunk_start, chunk_end, endpoint, CSLC_SHORT_NAME,
                extra_params=extra_params
            )
            return extract_granule_ids_from_response(products)
        except Exception as e:
            logging.warning(f"Failed to query CSLCs for burst {burst_id}: {e}")
            return []

    # Build list of all (burst, chunk) pairs to query
    query_tasks = []
    for burst_id in all_burst_ids:
        for chunk_start, chunk_end in time_chunks:
            query_tasks.append((burst_id, chunk_start, chunk_end))

    logging.info(f"Running {len(query_tasks)} burst/time-chunk queries with {max_workers} workers")

    with tqdm.tqdm(total=len(query_tasks), desc="Querying CSLCs", unit="queries") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(query_burst_chunk, burst_id, chunk_start, chunk_end): (burst_id, chunk_start)
                for burst_id, chunk_start, chunk_end in query_tasks
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_cslc_ids.update(result)
                    total_fetched += len(result)
                pbar.update(1)
                pbar.set_postfix(unique=len(all_cslc_ids), fetched=total_fetched)

    gc.collect()
    logging.info(f"Found {len(all_cslc_ids)} unique CSLCs (fetched {total_fetched})")
    return list(all_cslc_ids)


def calculate_expected_disp_s1(cslc_ids, frame_id, frame_to_bursts, burst_to_frames, k=DEFAULT_K):
    """
    Calculate expected DISP-S1 products based on CSLC inputs and K-cycle logic.

    Returns a dict mapping day_index to expected product info.
    """
    frame = frame_to_bursts[frame_id]
    sensing_days_index = frame.sensing_datetime_days_index
    num_bursts = len(frame.burst_ids)

    # Group CSLCs by day index
    day_index_to_cslcs = defaultdict(list)

    for cslc_id in cslc_ids:
        try:
            burst_id, acquisition_dt, acquisition_cycles, cslc_frame_ids = parse_cslc_native_id(
                cslc_id, burst_to_frames, frame_to_bursts
            )

            # Check if this CSLC belongs to our frame
            if frame_id in acquisition_cycles:
                day_index = acquisition_cycles[frame_id]
                day_index_to_cslcs[day_index].append({
                    'cslc_id': cslc_id,
                    'burst_id': burst_id,
                    'acquisition_dt': acquisition_dt
                })
        except Exception as e:
            logging.debug(f"Could not parse CSLC {cslc_id}: {e}")

    # Determine expected DISP-S1 products based on K-cycle boundaries
    # A DISP-S1 product is expected at each K-cycle boundary (every k sensing times)
    expected_products = {}

    if not day_index_to_cslcs:
        return expected_products

    # First sensing time for this frame
    first_sensing_dt = frame.sensing_datetimes[0]

    # Build a mapping of index_position -> CSLC count for K-window validation
    # This lets us check if all K sensing times in a window have complete coverage
    idx_position_to_cslc_count = {}
    for day_idx, cslcs in day_index_to_cslcs.items():
        try:
            idx_pos = sensing_days_index.index(day_idx)
            idx_position_to_cslc_count[idx_pos] = len(cslcs)
        except ValueError:
            continue

    # For each day_index where we have CSLCs
    for day_index in sorted(day_index_to_cslcs.keys()):
        # Get the index position (position in sensing_datetimes list) for this day_index
        try:
            idx_position = sensing_days_index.index(day_index)
            sensing_dt = frame.sensing_datetimes[idx_position]
        except (ValueError, IndexError):
            # Day index not in the historical database - skip
            continue

        # DISP-S1 is produced at each sensing time after the first k
        # So we expect products at index_position >= k-1
        if idx_position < k - 1:
            continue  # Not enough history for DISP-S1

        # Count CSLCs available for this day index
        cslcs_at_day = day_index_to_cslcs.get(day_index, [])

        # Calculate K-cycle info based on index_position (not day_index!)
        # This matches how diagnose_disp_s1_frame_products.py calculates it
        k_cycle = idx_position // k
        position_in_k = idx_position % k

        # K-window validation: check if all K sensing times have complete CSLC coverage
        # The K-window for a product at index N spans indices (N - k + 1) to N
        k_window_start = idx_position - k + 1
        k_window_missing = []  # List of index positions missing complete CSLCs
        for win_idx in range(k_window_start, idx_position + 1):
            cslc_count = idx_position_to_cslc_count.get(win_idx, 0)
            if cslc_count < num_bursts:
                # This position doesn't have complete CSLC coverage
                win_day_index = sensing_days_index[win_idx] if win_idx < len(sensing_days_index) else None
                win_sensing_dt = frame.sensing_datetimes[win_idx] if win_idx < len(frame.sensing_datetimes) else None
                k_window_missing.append({
                    'index_position': win_idx,
                    'day_index': win_day_index,
                    'sensing_datetime': win_sensing_dt.isoformat() if win_sensing_dt else None,
                    'cslc_count': cslc_count,
                    'expected_count': num_bursts
                })
        k_window_complete = len(k_window_missing) == 0

        expected_products[day_index] = {
            'day_index': day_index,
            'index_position': idx_position,
            'k_cycle': k_cycle,
            'position_in_k': position_in_k,
            'sensing_datetime': sensing_dt.isoformat() if sensing_dt else None,
            'first_sensing_datetime': first_sensing_dt.isoformat() if first_sensing_dt else None,
            'expected_cslc_count': num_bursts,
            'available_cslc_count': len(cslcs_at_day),
            'available_cslcs': [c['cslc_id'] for c in cslcs_at_day],
            'is_complete': len(cslcs_at_day) >= num_bursts,
            'k_window_complete': k_window_complete,
            'k_window_missing': k_window_missing
        }

    return expected_products


def query_disp_s1_for_frame(frame_id, start_date, end_date, endpoint="OPS"):
    """
    Query actual DISP-S1 products for a frame from CMR.

    Returns list of UMM objects with full metadata.
    """
    extra_params = {"attribute[]": f"int,FRAME_NUMBER,{frame_id}"}

    # Use time chunking for large date ranges
    all_products = []
    time_chunks = list(generate_time_chunks(start_date, end_date, chunk_days=90))

    for chunk_start, chunk_end in time_chunks:
        try:
            products = retrieve_r3_products(
                chunk_start, chunk_end, endpoint, DISP_S1_SHORT_NAME,
                extra_params=extra_params
            )
            all_products.extend(products)
        except Exception as e:
            logging.warning(f"Failed to query DISP-S1 for frame {frame_id}: {e}")

    # Deduplicate by GranuleUR
    seen = set()
    unique_products = []
    for p in all_products:
        granule_ur = p.get("umm", {}).get("GranuleUR", "")
        if granule_ur and granule_ur not in seen:
            seen.add(granule_ur)
            unique_products.append(p)

    logging.info(f"Frame {frame_id}: Found {len(unique_products)} DISP-S1 products in CMR")
    return unique_products


def fetch_iso_xml_inputs_parallel(products, max_workers=DEFAULT_MAX_WORKERS):
    """
    Fetch CSLC input granules from ISO XML for multiple products in parallel.

    Returns dict mapping granule_id to list of CSLC inputs.
    """
    product_to_inputs = {}

    def fetch_inputs(product):
        granule_ur = product.get("umm", {}).get("GranuleUR", "")
        iso_xml_url = get_iso_xml_url_from_umm(product)

        if not iso_xml_url:
            return granule_ur, []

        try:
            cslc_inputs = fetch_cslc_input_granules_from_iso_xml(iso_xml_url)
            return granule_ur, cslc_inputs
        except Exception as e:
            logging.warning(f"Failed to fetch ISO XML for {granule_ur}: {e}")
            return granule_ur, []

    with tqdm.tqdm(total=len(products), desc="Fetching ISO XML", unit="products") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_inputs, p): p for p in products}

            for future in as_completed(futures):
                granule_ur, inputs = future.result()
                product_to_inputs[granule_ur] = inputs
                pbar.update(1)

    return product_to_inputs


def analyze_disp_s1_products(products, product_to_inputs, frame_to_bursts, burst_to_frames, frame_id, k=DEFAULT_K):
    """
    Analyze DISP-S1 products, grouping by end datetime and selecting the most complete.

    Returns dict mapping day_index to best product info.
    """
    frame = frame_to_bursts[frame_id]
    sensing_days_index = frame.sensing_datetime_days_index
    sensing_datetimes = frame.sensing_datetimes
    num_bursts = len(frame.burst_ids)
    first_sensing = sensing_datetimes[0]

    # Group products by end datetime (day index)
    day_index_to_products = defaultdict(list)

    for product in products:
        umm = product.get("umm", {})
        granule_ur = umm.get("GranuleUR", "")

        # Get end datetime
        temporal = umm.get("TemporalExtent", {})
        range_dt = temporal.get("RangeDateTime", {})
        end_dt_str = range_dt.get("EndingDateTime", "")
        begin_dt_str = range_dt.get("BeginningDateTime", "")

        if not end_dt_str:
            continue

        try:
            import dateutil.parser
            end_dt = dateutil.parser.isoparse(end_dt_str).replace(tzinfo=None)
            begin_dt = dateutil.parser.isoparse(begin_dt_str).replace(tzinfo=None) if begin_dt_str else None

            # Calculate day index for end datetime
            delta = end_dt - first_sensing.replace(tzinfo=None)
            day_index = int(round(delta.total_seconds() / (24 * 3600)))

            # Calculate day index for begin datetime (for k-cycle reference validation)
            actual_begin_day_index = None
            if begin_dt:
                begin_delta = begin_dt - first_sensing.replace(tzinfo=None)
                actual_begin_day_index = int(round(begin_delta.total_seconds() / (24 * 3600)))

            # Validate k-cycle reference against current burst database
            # The BeginningDateTime should match the expected k-cycle reference
            has_valid_k_cycle_ref = False
            expected_begin_day_index = None
            expected_begin_datetime = None
            actual_begin_index_position = None
            expected_begin_index_position = None

            try:
                # Get index position for this product's end datetime
                idx_position = sensing_days_index.index(day_index)
                k_cycle = idx_position // k

                # Calculate expected BeginningDateTime based on k-cycle
                # For k-cycle 0: reference is first sensing time (index 0)
                # For k-cycle N > 0: reference is last sensing of k-cycle N-1 (index N*k - 1)
                if k_cycle == 0:
                    expected_begin_index_position = 0
                else:
                    expected_begin_index_position = k_cycle * k - 1

                expected_begin_day_index = sensing_days_index[expected_begin_index_position]
                expected_begin_datetime = sensing_datetimes[expected_begin_index_position]

                # Check if actual BeginningDateTime matches expected
                if actual_begin_day_index is not None:
                    # Allow small tolerance for datetime comparison (within same day)
                    has_valid_k_cycle_ref = (actual_begin_day_index == expected_begin_day_index)

                    # Also get the actual begin index position for reporting
                    try:
                        actual_begin_index_position = sensing_days_index.index(actual_begin_day_index)
                    except ValueError:
                        actual_begin_index_position = None  # Not in burst database
            except (ValueError, IndexError):
                # day_index not in burst database
                pass

            # Get CSLC inputs for this product
            cslc_inputs = product_to_inputs.get(granule_ur, [])

            # Count inputs that belong to this frame AND match the product's end datetime
            # A DISP-S1 product contains k sensing times of CSLCs, but for completeness
            # we only check if it has all bursts for the ending sensing time (day_index)
            frame_cslc_inputs = []
            end_sensing_cslc_inputs = []
            for cslc_id in cslc_inputs:
                try:
                    _, _, acquisition_cycles, _ = parse_cslc_native_id(cslc_id, burst_to_frames, frame_to_bursts)
                    if frame_id in acquisition_cycles:
                        frame_cslc_inputs.append(cslc_id)
                        # Check if this CSLC is for the end sensing time
                        cslc_day_index = acquisition_cycles[frame_id]
                        if cslc_day_index == day_index:
                            end_sensing_cslc_inputs.append(cslc_id)
                except:
                    pass

            parsed = parse_disp_s1_id(granule_ur)
            production_dt = parsed['production_dt'] if parsed else ""

            # Completeness is based on CSLCs for the end sensing time only
            day_index_to_products[day_index].append({
                'granule_ur': granule_ur,
                'end_datetime': end_dt_str,
                'begin_datetime': begin_dt_str,
                'production_datetime': production_dt,
                'total_cslc_inputs': len(cslc_inputs),
                'frame_cslc_inputs': len(frame_cslc_inputs),
                'end_sensing_cslc_inputs': len(end_sensing_cslc_inputs),
                'cslc_input_ids': end_sensing_cslc_inputs,
                'is_complete': len(end_sensing_cslc_inputs) >= num_bursts,
                'completeness_pct': (len(end_sensing_cslc_inputs) / num_bursts * 100) if num_bursts > 0 else 0,
                # K-cycle reference validation
                'has_valid_k_cycle_ref': has_valid_k_cycle_ref,
                'actual_begin_day_index': actual_begin_day_index,
                'actual_begin_index_position': actual_begin_index_position,
                'expected_begin_day_index': expected_begin_day_index,
                'expected_begin_index_position': expected_begin_index_position,
                'expected_begin_datetime': expected_begin_datetime.isoformat() if expected_begin_datetime else None
            })
        except Exception as e:
            logging.debug(f"Could not analyze product {granule_ur}: {e}")

    # For each day index, select the best product (most complete inputs)
    best_products = {}
    duplicates = {}

    for day_index, prods in day_index_to_products.items():
        if len(prods) == 1:
            best_products[day_index] = prods[0]
        else:
            # Sort by completeness (descending), then by production date (newest first for ties)
            sorted_prods = sorted(prods, key=lambda x: (-x['end_sensing_cslc_inputs'], -x.get('production_datetime', '')))
            best_products[day_index] = sorted_prods[0]
            duplicates[day_index] = {
                'best': sorted_prods[0],
                'others': sorted_prods[1:],
                'count': len(prods)
            }

    return best_products, duplicates


def generate_audit_report(frame_id, expected_products, actual_products, duplicates, frame_to_bursts, k=DEFAULT_K):
    """
    Generate comprehensive audit report for a frame.
    """
    frame = frame_to_bursts[frame_id]
    num_bursts = len(frame.burst_ids)
    sensing_days_index = frame.sensing_datetime_days_index

    report = {
        'frame_id': frame_id,
        'num_bursts': num_bursts,
        'k': k,
        'first_sensing_datetime': frame.sensing_datetimes[0].isoformat() if frame.sensing_datetimes else None,
        'last_sensing_datetime': frame.sensing_datetimes[-1].isoformat() if frame.sensing_datetimes else None,
        'expected_products_count': len(expected_products),
        'actual_products_count': len(actual_products),
        'duplicates_count': len(duplicates),
        'missing': [],
        'not_triggerable': [],  # Missing due to CSLC gaps in K-window
        'incomplete': [],
        'complete': [],
        'stale_reference': [],
        'unexpected': [],
        'duplicates': []
    }

    # Analyze each expected product
    for day_index, expected in expected_products.items():
        actual = actual_products.get(day_index)

        if actual is None:
            # Product not found - check if it's truly missing or not triggerable
            k_window_complete = expected.get('k_window_complete', True)
            k_window_missing = expected.get('k_window_missing', [])

            product_info = {
                'day_index': day_index,
                'index_position': expected.get('index_position', -1),
                'k_cycle': expected['k_cycle'],
                'position_in_k': expected['position_in_k'],
                'sensing_datetime': expected['sensing_datetime'],
                'num_bursts': num_bursts,
                'cslcs_found': expected['available_cslc_count'],
                'cslc_ids': expected.get('available_cslcs', []),
            }

            if k_window_complete:
                # Truly missing - all K sensing times have complete CSLCs but product wasn't generated
                report['missing'].append(product_info)
            else:
                # Not triggerable - CSLC gaps in K-window prevented job from triggering
                product_info['k_window_missing'] = k_window_missing
                product_info['k_window_gap_count'] = len(k_window_missing)
                report['not_triggerable'].append(product_info)
        elif not actual['is_complete']:
            # Incomplete product
            report['incomplete'].append({
                'day_index': day_index,
                'index_position': expected.get('index_position', -1),
                'k_cycle': expected['k_cycle'],
                'position_in_k': expected['position_in_k'],
                'granule_ur': actual['granule_ur'],
                'end_datetime': actual['end_datetime'],
                'num_bursts': num_bursts,
                'cslc_inputs_for_end_sensing': actual['end_sensing_cslc_inputs'],
                'completeness_pct': actual['completeness_pct'],
                'production_datetime': actual['production_datetime'],
                'has_valid_k_cycle_ref': actual.get('has_valid_k_cycle_ref', True)
            })
        else:
            # Complete product - but check if k-cycle reference is stale
            has_valid_ref = actual.get('has_valid_k_cycle_ref', True)

            product_info = {
                'day_index': day_index,
                'index_position': expected.get('index_position', -1),
                'k_cycle': expected['k_cycle'],
                'granule_ur': actual['granule_ur'],
                'end_datetime': actual['end_datetime'],
                'begin_datetime': actual['begin_datetime'],
                'cslc_inputs_for_end_sensing': actual['end_sensing_cslc_inputs'],
                'completeness_pct': actual['completeness_pct'],
                'has_valid_k_cycle_ref': has_valid_ref
            }

            if not has_valid_ref:
                # Product has stale k-cycle reference - needs reprocessing
                product_info['actual_begin_index_position'] = actual.get('actual_begin_index_position')
                product_info['expected_begin_index_position'] = actual.get('expected_begin_index_position')
                product_info['actual_begin_day_index'] = actual.get('actual_begin_day_index')
                product_info['expected_begin_day_index'] = actual.get('expected_begin_day_index')
                product_info['expected_begin_datetime'] = actual.get('expected_begin_datetime')
                report['stale_reference'].append(product_info)
            else:
                report['complete'].append(product_info)

    # Find unexpected products (in CMR but not expected based on CSLC availability)
    expected_day_indices = set(expected_products.keys())
    for day_index, actual in actual_products.items():
        if day_index not in expected_day_indices:
            # Determine reason for being unexpected
            try:
                idx_position = sensing_days_index.index(day_index)
                in_burst_map = True
                k_cycle = idx_position // k
                position_in_k = idx_position % k
                if idx_position < k - 1:
                    reason = "index_position < k-1 (insufficient history)"
                else:
                    reason = "no CSLCs found in CMR for this sensing time"
            except ValueError:
                in_burst_map = False
                idx_position = -1
                k_cycle = -1
                position_in_k = -1
                reason = "day_index not in burst map (forward processing or out of range)"

            report['unexpected'].append({
                'day_index': day_index,
                'index_position': idx_position,
                'k_cycle': k_cycle,
                'position_in_k': position_in_k,
                'in_burst_map': in_burst_map,
                'reason': reason,
                'granule_ur': actual['granule_ur'],
                'end_datetime': actual['end_datetime'],
                'cslc_input_count': actual['frame_cslc_inputs'],
                'completeness_pct': actual['completeness_pct'],
                'production_datetime': actual['production_datetime']
            })

    # Add duplicate info
    for day_index, dup_info in duplicates.items():
        report['duplicates'].append({
            'day_index': day_index,
            'count': dup_info['count'],
            'best_product': dup_info['best']['granule_ur'],
            'best_completeness': dup_info['best']['completeness_pct'],
            'other_products': [p['granule_ur'] for p in dup_info['others']]
        })

    # Summary stats
    # Coverage is now based on expected products only (not inflated by unexpected)
    # Note: stale_reference products are counted as "found" but flagged for reprocessing
    # Note: not_triggerable products are excluded from coverage since they couldn't be generated
    matched_count = len(report['complete']) + len(report['incomplete']) + len(report['stale_reference'])
    # Effective expected = total expected minus not_triggerable (those couldn't be generated)
    effective_expected = len(expected_products) - len(report['not_triggerable'])
    report['summary'] = {
        'total_expected': len(expected_products),
        'total_found': len(actual_products),
        'missing_count': len(report['missing']),
        'not_triggerable_count': len(report['not_triggerable']),
        'incomplete_count': len(report['incomplete']),
        'complete_count': len(report['complete']),
        'stale_reference_count': len(report['stale_reference']),
        'unexpected_count': len(report['unexpected']),
        'duplicates_count': len(report['duplicates']),
        'coverage_pct': (matched_count / effective_expected * 100) if effective_expected > 0 else 0
    }

    return report


def print_audit_report(report):
    """Print human-readable audit report."""
    print()
    print("=" * 120)
    print(f"DISP-S1 COMPLETENESS AUDIT - FRAME {report['frame_id']}")
    print("=" * 120)
    print(f"Number of bursts: {report['num_bursts']}")
    print(f"K parameter: {report['k']}")
    print(f"Sensing range: {report['first_sensing_datetime'][:10] if report['first_sensing_datetime'] else 'N/A'} to {report['last_sensing_datetime'][:10] if report['last_sensing_datetime'] else 'N/A'}")
    print()

    summary = report['summary']
    print("-" * 120)
    print("SUMMARY")
    print("-" * 120)
    print(f"Expected products: {summary['total_expected']}")
    print(f"Found in CMR: {summary['total_found']}")
    print(f"  - Complete: {summary['complete_count']}")
    print(f"  - Incomplete: {summary['incomplete_count']}")
    print(f"  - Stale Reference (needs reprocessing): {summary.get('stale_reference_count', 0)}")
    print(f"  - Unexpected: {summary.get('unexpected_count', 0)}")
    print(f"Not Triggerable (CSLC gaps in K-window): {summary.get('not_triggerable_count', 0)}")
    print(f"Missing (actionable): {summary['missing_count']}")
    print(f"Duplicates: {summary['duplicates_count']}")
    print(f"Coverage: {summary['coverage_pct']:.1f}%")
    print()

    # Missing products
    if report['missing']:
        print("-" * 120)
        print(f"MISSING PRODUCTS ({len(report['missing'])})")
        print("-" * 120)
        print(f"{'Index Pos':>10} | {'Day Index':>10} | {'K-Cycle':>8} | {'Pos':>4} | {'Sensing Date':>12} | {'CSLCs Found':>12}")
        print("-" * 120)
        for m in sorted(report['missing'], key=lambda x: x.get('index_position', x['day_index']))[:50]:
            sensing_date = m['sensing_datetime'][:10] if m['sensing_datetime'] else 'N/A'
            idx_pos = m.get('index_position', -1)
            idx_pos_str = str(idx_pos) if idx_pos >= 0 else 'N/A'
            cslcs_found = m.get('cslcs_found', m.get('available_cslc_count', 0))
            print(f"{idx_pos_str:>10} | {m['day_index']:>10} | {m['k_cycle']:>8} | {m['position_in_k']:>4} | {sensing_date:>12} | {cslcs_found:>12}")
        if len(report['missing']) > 50:
            print(f"... and {len(report['missing']) - 50} more")
        print()

    # Not triggerable products (CSLC gaps in K-window)
    if report.get('not_triggerable'):
        print("-" * 120)
        print(f"NOT TRIGGERABLE ({len(report['not_triggerable'])})")
        print("(CSLC gaps in K-window prevented job from triggering - not actionable without upstream CSLC)")
        print("-" * 120)
        print(f"{'Index Pos':>10} | {'Day Index':>10} | {'K-Cycle':>8} | {'Pos':>4} | {'Sensing Date':>12} | {'K-Window Gaps':>13}")
        print("-" * 120)
        for nt in sorted(report['not_triggerable'], key=lambda x: x.get('index_position', x['day_index']))[:50]:
            sensing_date = nt['sensing_datetime'][:10] if nt['sensing_datetime'] else 'N/A'
            idx_pos = nt.get('index_position', -1)
            idx_pos_str = str(idx_pos) if idx_pos >= 0 else 'N/A'
            k_window_gap_count = nt.get('k_window_gap_count', len(nt.get('k_window_missing', [])))
            print(f"{idx_pos_str:>10} | {nt['day_index']:>10} | {nt['k_cycle']:>8} | {nt['position_in_k']:>4} | {sensing_date:>12} | {k_window_gap_count:>13}")
        if len(report['not_triggerable']) > 50:
            print(f"... and {len(report['not_triggerable']) - 50} more")
        print()

    # Incomplete products
    if report['incomplete']:
        print("-" * 120)
        print(f"INCOMPLETE PRODUCTS ({len(report['incomplete'])})")
        print("-" * 120)
        print(f"{'Index Pos':>10} | {'K-Cycle':>8} | {'CSLCs':>10} | {'Complete%':>10} | Product ID")
        print("-" * 120)
        for inc in sorted(report['incomplete'], key=lambda x: x.get('index_position', x['day_index']))[:30]:
            cslc_inputs = inc.get('cslc_inputs_for_end_sensing', inc.get('cslc_inputs_in_product', 0))
            num_bursts = inc.get('num_bursts', 0)
            cslcs = f"{cslc_inputs}/{num_bursts}"
            idx_pos = inc.get('index_position', -1)
            idx_pos_str = str(idx_pos) if idx_pos >= 0 else 'N/A'
            print(f"{idx_pos_str:>10} | {inc['k_cycle']:>8} | {cslcs:>10} | {inc['completeness_pct']:>9.1f}% | {inc['granule_ur'][:60]}")
        if len(report['incomplete']) > 30:
            print(f"... and {len(report['incomplete']) - 30} more")
        print()

    # Stale reference products (need reprocessing due to k-cycle boundary shift)
    if report.get('stale_reference'):
        print("-" * 120)
        print(f"STALE K-CYCLE REFERENCE ({len(report['stale_reference'])})")
        print("(Products generated with outdated k-cycle boundaries - need reprocessing)")
        print("-" * 120)
        print(f"{'Index Pos':>10} | {'K-Cycle':>8} | {'Actual Begin':>12} | {'Expected Begin':>14} | Product ID")
        print("-" * 120)
        for stale in sorted(report['stale_reference'], key=lambda x: x.get('index_position', x['day_index']))[:30]:
            idx_pos = stale.get('index_position', -1)
            idx_pos_str = str(idx_pos) if idx_pos >= 0 else 'N/A'
            actual_begin_idx = stale.get('actual_begin_index_position')
            expected_begin_idx = stale.get('expected_begin_index_position')
            actual_str = str(actual_begin_idx) if actual_begin_idx is not None else 'N/A'
            expected_str = str(expected_begin_idx) if expected_begin_idx is not None else 'N/A'
            print(f"{idx_pos_str:>10} | {stale['k_cycle']:>8} | {actual_str:>12} | {expected_str:>14} | {stale['granule_ur'][:55]}")
        if len(report['stale_reference']) > 30:
            print(f"... and {len(report['stale_reference']) - 30} more")
        print()

    # Unexpected products
    if report.get('unexpected'):
        print("-" * 120)
        print(f"UNEXPECTED PRODUCTS ({len(report['unexpected'])})")
        print("(Products in CMR but not expected based on CSLC availability)")
        print("-" * 120)
        print(f"{'Index Pos':>10} | {'Day Index':>10} | {'K-Cycle':>8} | {'End Date':>12} | Reason")
        print("-" * 120)
        for unexp in sorted(report['unexpected'], key=lambda x: x.get('index_position', x['day_index']))[:30]:
            idx_pos = unexp.get('index_position', -1)
            idx_pos_str = str(idx_pos) if idx_pos >= 0 else 'N/A'
            k_cycle = unexp.get('k_cycle', -1)
            k_cycle_str = str(k_cycle) if k_cycle >= 0 else 'N/A'
            end_date = unexp['end_datetime'][:10] if unexp.get('end_datetime') else 'N/A'
            print(f"{idx_pos_str:>10} | {unexp['day_index']:>10} | {k_cycle_str:>8} | {end_date:>12} | {unexp['reason']}")
        if len(report['unexpected']) > 30:
            print(f"... and {len(report['unexpected']) - 30} more")
        print()

    # Duplicates
    if report['duplicates']:
        print("-" * 120)
        print(f"DUPLICATES ({len(report['duplicates'])})")
        print("-" * 120)
        for dup in sorted(report['duplicates'], key=lambda x: x['day_index'])[:20]:
            print(f"Day Index {dup['day_index']}: {dup['count']} products")
            print(f"  Best ({dup['best_completeness']:.1f}% complete): {dup['best_product']}")
            for other in dup['other_products'][:3]:
                print(f"  Other: {other}")
            if len(dup['other_products']) > 3:
                print(f"  ... and {len(dup['other_products']) - 3} more")
        if len(report['duplicates']) > 20:
            print(f"... and {len(report['duplicates']) - 20} more duplicate groups")
        print()

    print("=" * 120)
    print()


def audit_frame(frame_id, frame_to_bursts, burst_to_frames, start_date, end_date,
                endpoint="OPS", k=DEFAULT_K, max_workers=DEFAULT_MAX_WORKERS):
    """
    Perform complete audit for a single frame.
    """
    logging.info(f"Starting audit for frame {frame_id}")

    # 1. Query CSLCs for this frame
    cslc_ids = query_cslcs_for_frame(frame_id, frame_to_bursts, start_date, end_date, endpoint)

    if not cslc_ids:
        logging.warning(f"No CSLCs found for frame {frame_id}")
        return None

    # 2. Calculate expected DISP-S1 products
    expected_products = calculate_expected_disp_s1(cslc_ids, frame_id, frame_to_bursts, burst_to_frames, k)
    logging.info(f"Frame {frame_id}: {len(expected_products)} expected DISP-S1 products based on K-cycle logic")

    # 3. Query actual DISP-S1 products
    disp_s1_products = query_disp_s1_for_frame(frame_id, start_date, end_date, endpoint)

    # 4. Fetch ISO XML to get CSLC inputs for each product
    product_to_inputs = fetch_iso_xml_inputs_parallel(disp_s1_products, max_workers)

    # 5. Analyze products and handle duplicates
    actual_products, duplicates = analyze_disp_s1_products(
        disp_s1_products, product_to_inputs, frame_to_bursts, burst_to_frames, frame_id, k
    )

    # 6. Generate report
    report = generate_audit_report(frame_id, expected_products, actual_products, duplicates, frame_to_bursts, k)

    return report


def main():
    parser = argparse.ArgumentParser(
        description='Comprehensive DISP-S1 frame completeness audit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--frames', required=True,
                        help='Comma-separated list of frame IDs to audit')
    parser.add_argument('--start', default='2016-07-01T00:00:00Z',
                        help='Start datetime (ISO format, default: 2016-07-01T00:00:00Z)')
    parser.add_argument('--end', default='2025-12-31T00:00:00Z',
                        help='End datetime (ISO format, default: 2025-12-31T00:00:00Z)')
    parser.add_argument('--endpoint', default='OPS', choices=['OPS', 'UAT'],
                        help='CMR endpoint (default: OPS)')
    parser.add_argument('--k', type=int, default=DEFAULT_K,
                        help=f'K parameter for K-cycle logic (default: {DEFAULT_K})')
    parser.add_argument('--max-workers', type=int, default=DEFAULT_MAX_WORKERS,
                        help=f'Max parallel workers for ISO XML fetching (default: {DEFAULT_MAX_WORKERS})')
    parser.add_argument('--output', type=str, default=None,
                        help='Output JSON file path (optional)')
    parser.add_argument('--iso-cache-dir', type=str, default=None,
                        help='Directory for caching ISO XML files (speeds up re-runs)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='[%(levelname)s] %(message)s')

    # Configure ISO XML cache if specified
    if args.iso_cache_dir:
        configure_iso_xml_cache(args.iso_cache_dir)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        print(f"Error parsing dates: {e}", file=sys.stderr)
        sys.exit(1)

    # Parse frame IDs
    frame_ids = [int(f.strip()) for f in args.frames.split(',')]

    # Load DISP burst map
    logging.info("Loading DISP burst map...")
    frame_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()

    # Validate frame IDs
    invalid_frames = [f for f in frame_ids if f not in frame_to_bursts]
    if invalid_frames:
        print(f"Error: Invalid frame IDs: {invalid_frames}", file=sys.stderr)
        sys.exit(1)

    # Audit each frame
    all_reports = {}

    for frame_id in frame_ids:
        report = audit_frame(
            frame_id, frame_to_bursts, burst_to_frames,
            start_date, end_date, args.endpoint, args.k, args.max_workers
        )

        if report:
            all_reports[frame_id] = report
            print_audit_report(report)

    # Save JSON output if requested
    if args.output:
        # Convert to JSON-serializable format
        output_data = {
            'audit_datetime': datetime.now().isoformat(),
            'parameters': {
                'frames': frame_ids,
                'start': args.start,
                'end': args.end,
                'endpoint': args.endpoint,
                'k': args.k
            },
            'reports': all_reports
        }

        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        print(f"Report saved to: {args.output}")

    # Print overall summary
    if len(frame_ids) > 1:
        print()
        print("=" * 120)
        print("OVERALL SUMMARY")
        print("=" * 120)
        total_expected = sum(r['summary']['total_expected'] for r in all_reports.values())
        total_found = sum(r['summary']['total_found'] for r in all_reports.values())
        total_complete = sum(r['summary']['complete_count'] for r in all_reports.values())
        total_incomplete = sum(r['summary']['incomplete_count'] for r in all_reports.values())
        total_stale_ref = sum(r['summary'].get('stale_reference_count', 0) for r in all_reports.values())
        total_unexpected = sum(r['summary'].get('unexpected_count', 0) for r in all_reports.values())
        total_missing = sum(r['summary']['missing_count'] for r in all_reports.values())

        # Coverage based on matched (complete + incomplete + stale_reference) vs expected
        total_matched = total_complete + total_incomplete + total_stale_ref
        coverage = (total_matched / total_expected * 100) if total_expected else 0

        print(f"Frames audited: {len(all_reports)}")
        print(f"Total expected products: {total_expected}")
        print(f"Total found in CMR: {total_found}")
        print(f"  - Complete: {total_complete}")
        print(f"  - Incomplete: {total_incomplete}")
        print(f"  - Stale Reference (needs reprocessing): {total_stale_ref}")
        print(f"  - Unexpected: {total_unexpected}")
        print(f"Total missing: {total_missing}")
        print(f"Overall coverage: {coverage:.1f}%")
        print()

    # Print cache stats if caching was enabled
    if args.iso_cache_dir:
        cache_stats = get_cache_stats()
        print()
        print("-" * 60)
        print("ISO XML CACHE STATISTICS")
        print("-" * 60)
        print(f"Cache directory: {args.iso_cache_dir}")
        print(f"Cache hits: {cache_stats['hits']}")
        print(f"Cache misses: {cache_stats['misses']}")
        print(f"Hit rate: {cache_stats['hit_rate']:.1f}%")
        print()


if __name__ == '__main__':
    main()
