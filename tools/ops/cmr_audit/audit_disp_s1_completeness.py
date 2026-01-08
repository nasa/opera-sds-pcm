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


def calculate_expected_disp_s1(cslc_ids, frame_id, frame_to_bursts, burst_to_frames, k=DEFAULT_K):
    """
    Calculate expected DISP-S1 products based on CSLC inputs and K-cycle logic.

    DISP-S1 processing works as follows:
    - A job triggers at K-cycle boundaries (when index_position + 1 is a multiple of k)
    - The job only triggers if ALL k sensing times in the cycle have complete CSLC coverage
    - When triggered, the job generates products for ALL k sensing times in that cycle

    Returns a tuple of:
    - dict mapping day_index to expected product info
    - dict with skipped CSLCs info (sensing dates not in burst database)
    """
    frame = frame_to_bursts[frame_id]
    sensing_days_index = frame.sensing_datetime_days_index
    sensing_datetimes = frame.sensing_datetimes
    num_bursts = len(frame.burst_ids)

    # Group CSLCs by day index
    cslcs_by_day_index = defaultdict(list)
    # Track CSLCs that couldn't be parsed
    parse_errors = []

    for cslc_id in cslc_ids:
        try:
            burst_id, acquisition_dt, acquisition_cycles, _ = parse_cslc_native_id(
                cslc_id, burst_to_frames, frame_to_bursts
            )

            # Check if this CSLC belongs to our frame
            if frame_id in acquisition_cycles:
                day_index = acquisition_cycles[frame_id]
                cslcs_by_day_index[day_index].append({
                    'cslc_id': cslc_id,
                    'burst_id': burst_id,
                    'acquisition_dt': acquisition_dt
                })
        except Exception as e:
            logging.debug(f"Could not parse CSLC {cslc_id}: {e}")
            parse_errors.append({'cslc_id': cslc_id, 'error': str(e)})

    if not cslcs_by_day_index:
        return {}, {'skipped_sensing_times': [], 'parse_errors': parse_errors, 'total_skipped': 0}

    first_sensing_dt = sensing_datetimes[0]

    # Build mapping of index_position -> CSLC completeness info
    # Track CSLCs for sensing times not in the database
    completeness_by_idx = {}
    skipped_sensing_times = []

    for day_idx, cslcs in cslcs_by_day_index.items():
        try:
            idx_pos = sensing_days_index.index(day_idx)
            completeness_by_idx[idx_pos] = {
                'day_index': day_idx,
                'cslc_count': len(cslcs),
                'is_complete': len(cslcs) >= num_bursts,
                'cslcs': [c['cslc_id'] for c in cslcs]
            }
        except ValueError:
            # Sensing time not in database - track it
            # Calculate approximate date from day_index
            approx_date = first_sensing_dt + timedelta(days=day_idx)
            skipped_sensing_times.append({
                'day_index': day_idx,
                'approx_date': approx_date.strftime('%Y-%m-%d'),
                'cslc_count': len(cslcs),
                'cslc_ids': [c['cslc_id'] for c in cslcs[:5]],  # First 5 for reference
                'reason': 'sensing time not in burst database'
            })

    # Process K-cycles
    total_sensing_times = len(sensing_days_index)
    total_k_cycles = (total_sensing_times + k - 1) // k  # Ceiling division
    expected_products = {}

    for k_cycle in range(total_k_cycles):
        cycle_start_idx = k_cycle * k
        cycle_end_idx = min((k_cycle + 1) * k, total_sensing_times)
        cycle_indices = list(range(cycle_start_idx, cycle_end_idx))

        # Check if ALL sensing times in this cycle have complete CSLC coverage
        cycle_gaps = []
        for idx in cycle_indices:
            info = completeness_by_idx.get(idx)
            if info is None or not info['is_complete']:
                cycle_gaps.append({
                    'index_position': idx,
                    'day_index': sensing_days_index[idx],
                    'sensing_datetime': sensing_datetimes[idx].isoformat(),
                    'cslc_count': info['cslc_count'] if info else 0,
                    'expected_count': num_bursts
                })

        is_triggerable = len(cycle_gaps) == 0

        # Create expected product entries for all sensing times in this cycle
        for idx in cycle_indices:
            day_index = sensing_days_index[idx]
            sensing_dt = sensing_datetimes[idx]
            info = completeness_by_idx.get(idx, {})

            expected_products[day_index] = {
                'day_index': day_index,
                'index_position': idx,
                'k_cycle': k_cycle,
                'position_in_k': idx % k,
                'sensing_datetime': sensing_dt.isoformat(),
                'first_sensing_datetime': first_sensing_dt.isoformat(),
                'expected_cslc_count': num_bursts,
                'available_cslc_count': info.get('cslc_count', 0),
                'available_cslcs': info.get('cslcs', []),
                'is_triggerable': is_triggerable,
                'cycle_gaps': cycle_gaps if not is_triggerable else []
            }

    # Calculate total skipped CSLCs
    total_skipped_cslcs = sum(st['cslc_count'] for st in skipped_sensing_times)

    skipped_info = {
        'skipped_sensing_times': sorted(skipped_sensing_times, key=lambda x: x['day_index']),
        'parse_errors': parse_errors,
        'total_skipped_cslcs': total_skipped_cslcs,
        'total_skipped_sensing_times': len(skipped_sensing_times)
    }

    return expected_products, skipped_info


def query_disp_s1_for_frame(frame_id, start_date, end_date, endpoint="OPS"):
    """Query actual DISP-S1 products for a frame from CMR."""
    extra_params = {"attribute[]": f"int,FRAME_NUMBER,{frame_id}"}

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
    """Fetch CSLC input granules from ISO XML for multiple products in parallel."""
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
    Analyze DISP-S1 products for completeness and anomalies.

    For each product, analyzes ALL CSLC inputs from ISO XML (no filtering):
    - Checks if all K sensing times have all bursts
    - Validates K-cycle reference (BeginningDateTime)
    - Reports anomalous CSLCs (wrong frame, unexpected sensing times, parse errors)

    Returns (best_products, duplicates) where best_products maps day_index to product info.
    """
    frame = frame_to_bursts[frame_id]
    sensing_days_index = frame.sensing_datetime_days_index
    sensing_datetimes = frame.sensing_datetimes
    num_bursts = len(frame.burst_ids)
    frame_burst_ids = set(frame.burst_ids)
    first_sensing = sensing_datetimes[0]

    products_by_day_index = defaultdict(list)

    for product in products:
        umm = product.get("umm", {})
        granule_ur = umm.get("GranuleUR", "")

        # Extract temporal info
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

            # Calculate day indices
            delta = end_dt - first_sensing.replace(tzinfo=None)
            day_index = int(round(delta.total_seconds() / (24 * 3600)))

            begin_day_index = None
            if begin_dt:
                begin_delta = begin_dt - first_sensing.replace(tzinfo=None)
                begin_day_index = int(round(begin_delta.total_seconds() / (24 * 3600)))

            # Determine K-cycle info
            try:
                idx_position = sensing_days_index.index(day_index)
                k_cycle = idx_position // k

                # Expected sensing time indices for this K-cycle
                cycle_start = k_cycle * k
                cycle_end = min((k_cycle + 1) * k, len(sensing_days_index))
                expected_indices = list(range(cycle_start, cycle_end))

                # Expected begin index (K-cycle reference)
                expected_begin_idx = (k_cycle * k - 1) if k_cycle > 0 else 0
                expected_begin_day = sensing_days_index[expected_begin_idx]
                expected_begin_dt = sensing_datetimes[expected_begin_idx]

                # Validate K-cycle reference
                has_valid_ref = (begin_day_index == expected_begin_day) if begin_day_index is not None else True
                actual_begin_idx = None
                if begin_day_index is not None:
                    try:
                        actual_begin_idx = sensing_days_index.index(begin_day_index)
                    except ValueError:
                        pass

            except ValueError:
                idx_position = -1
                k_cycle = -1
                expected_indices = []
                expected_begin_idx = None
                expected_begin_day = None
                expected_begin_dt = None
                has_valid_ref = True
                actual_begin_idx = None

            # Analyze ALL CSLC inputs (no filtering)
            cslc_inputs = product_to_inputs.get(granule_ur, [])
            cslcs_by_sensing_time = defaultdict(list)
            anomalous_cslcs = []

            for cslc_id in cslc_inputs:
                try:
                    burst_id, _, acquisition_cycles, _ = parse_cslc_native_id(
                        cslc_id, burst_to_frames, frame_to_bursts
                    )

                    if frame_id in acquisition_cycles:
                        cslc_day_index = acquisition_cycles[frame_id]
                        cslcs_by_sensing_time[cslc_day_index].append({
                            'cslc_id': cslc_id,
                            'burst_id': burst_id,
                            'day_index': cslc_day_index
                        })

                        # Check if sensing time is expected for this K-cycle
                        try:
                            cslc_idx = sensing_days_index.index(cslc_day_index)
                            if expected_indices and cslc_idx not in expected_indices:
                                anomalous_cslcs.append({
                                    'cslc_id': cslc_id,
                                    'burst_id': burst_id,
                                    'reason': f'unexpected sensing time (idx {cslc_idx}, expected {expected_indices[0]}-{expected_indices[-1]})'
                                })
                        except ValueError:
                            anomalous_cslcs.append({
                                'cslc_id': cslc_id,
                                'burst_id': burst_id,
                                'reason': 'sensing time not in burst database'
                            })
                    else:
                        anomalous_cslcs.append({
                            'cslc_id': cslc_id,
                            'burst_id': burst_id,
                            'reason': 'burst not in frame'
                        })
                except Exception as e:
                    anomalous_cslcs.append({
                        'cslc_id': cslc_id,
                        'burst_id': None,
                        'reason': f'parse error: {e}'
                    })

            # Check completeness: all K sensing times should have all bursts
            complete_times = 0
            incomplete_times = []
            for exp_idx in expected_indices:
                if exp_idx < len(sensing_days_index):
                    exp_day = sensing_days_index[exp_idx]
                    cslcs_at_time = cslcs_by_sensing_time.get(exp_day, [])
                    bursts_found = set(c['burst_id'] for c in cslcs_at_time)

                    if len(bursts_found) >= num_bursts:
                        complete_times += 1
                    else:
                        incomplete_times.append({
                            'index_position': exp_idx,
                            'day_index': exp_day,
                            'bursts_found': len(bursts_found),
                            'bursts_expected': num_bursts
                        })

            is_complete = (complete_times == len(expected_indices) == k)
            total_frame_cslcs = sum(len(v) for v in cslcs_by_sensing_time.values())

            parsed = parse_disp_s1_id(granule_ur)
            production_dt = parsed['production_dt'] if parsed else ""

            products_by_day_index[day_index].append({
                'granule_ur': granule_ur,
                'end_datetime': end_dt_str,
                'begin_datetime': begin_dt_str,
                'production_datetime': production_dt,
                'index_position': idx_position,
                'k_cycle': k_cycle,
                # CSLC analysis
                'total_cslc_inputs': len(cslc_inputs),
                'cslcs_for_frame': total_frame_cslcs,
                'sensing_times_found': len(cslcs_by_sensing_time),
                'complete_sensing_times': complete_times,
                'incomplete_sensing_times': incomplete_times,
                'anomalous_cslcs': anomalous_cslcs,
                # Completeness
                'is_complete': is_complete,
                'completeness_pct': (complete_times / k * 100) if k > 0 else 0,
                # K-cycle reference validation
                'has_valid_k_cycle_ref': has_valid_ref,
                'actual_begin_day_index': begin_day_index,
                'actual_begin_idx': actual_begin_idx,
                'expected_begin_day_index': expected_begin_day,
                'expected_begin_idx': expected_begin_idx,
                'expected_begin_datetime': expected_begin_dt.isoformat() if expected_begin_dt else None
            })

        except Exception as e:
            logging.debug(f"Could not analyze product {granule_ur}: {e}")

    # Select best product for each day_index (most complete sensing times, then newest)
    best_products = {}
    duplicates = {}

    for day_index, prods in products_by_day_index.items():
        if len(prods) == 1:
            best_products[day_index] = prods[0]
        else:
            sorted_prods = sorted(prods, key=lambda x: (-x['complete_sensing_times'], -x.get('production_datetime', '')))
            best_products[day_index] = sorted_prods[0]
            duplicates[day_index] = {
                'best': sorted_prods[0],
                'others': sorted_prods[1:],
                'count': len(prods)
            }

    return best_products, duplicates


def generate_audit_report(frame_id, expected_products, actual_products, duplicates, frame_to_bursts,
                          k=DEFAULT_K, skipped_cslcs=None):
    """Generate comprehensive audit report for a frame."""
    frame = frame_to_bursts[frame_id]
    num_bursts = len(frame.burst_ids)
    sensing_days_index = frame.sensing_datetime_days_index

    # Default skipped_cslcs if not provided
    if skipped_cslcs is None:
        skipped_cslcs = {'skipped_sensing_times': [], 'parse_errors': [], 'total_skipped_cslcs': 0, 'total_skipped_sensing_times': 0}

    report = {
        'frame_id': frame_id,
        'num_bursts': num_bursts,
        'k': k,
        'first_sensing_datetime': frame.sensing_datetimes[0].isoformat() if frame.sensing_datetimes else None,
        'last_sensing_datetime': frame.sensing_datetimes[-1].isoformat() if frame.sensing_datetimes else None,
        'expected_count': len(expected_products),
        'actual_count': len(actual_products),
        # Product categories
        'missing': [],
        'not_triggerable': [],
        'incomplete': [],
        'complete': [],
        'stale_reference': [],
        'unexpected': [],
        'duplicates': [],
        'anomalies': [],
        # Skipped CSLCs (sensing times not in database)
        'skipped_cslcs': skipped_cslcs
    }

    expected_day_indices = set(expected_products.keys())

    # Categorize expected products
    for day_index, expected in expected_products.items():
        actual = actual_products.get(day_index)

        base_info = {
            'day_index': day_index,
            'index_position': expected['index_position'],
            'k_cycle': expected['k_cycle'],
            'position_in_k': expected['position_in_k'],
            'sensing_datetime': expected['sensing_datetime']
        }

        if actual is None:
            # No product found
            if expected['is_triggerable']:
                report['missing'].append({
                    **base_info,
                    'cslcs_available': expected['available_cslc_count'],
                    'cslcs_expected': num_bursts
                })
            else:
                report['not_triggerable'].append({
                    **base_info,
                    'cycle_gaps': expected['cycle_gaps']
                })
        else:
            # Product found
            # Check if product exists despite K-cycle appearing not triggerable
            # This can happen if:
            # - Burst database was updated after product generation (new gaps introduced)
            # - Product was generated with different CSLC availability
            found_despite_untriggerable = not expected['is_triggerable']

            product_info = {
                **base_info,
                'granule_ur': actual['granule_ur'],
                'end_datetime': actual['end_datetime'],
                'total_cslc_inputs': actual['total_cslc_inputs'],
                'cslcs_for_frame': actual['cslcs_for_frame'],
                'complete_sensing_times': actual['complete_sensing_times'],
                'completeness_pct': actual['completeness_pct'],
                'found_despite_untriggerable': found_despite_untriggerable
            }

            if found_despite_untriggerable:
                product_info['cycle_gaps'] = expected['cycle_gaps']

            # Track anomalies
            if actual['anomalous_cslcs']:
                report['anomalies'].append({
                    'granule_ur': actual['granule_ur'],
                    'day_index': day_index,
                    'anomalous_cslcs': actual['anomalous_cslcs']
                })

            if not actual['is_complete']:
                product_info['incomplete_sensing_times'] = actual['incomplete_sensing_times']
                report['incomplete'].append(product_info)
            elif not actual['has_valid_k_cycle_ref']:
                product_info['actual_begin_idx'] = actual['actual_begin_idx']
                product_info['expected_begin_idx'] = actual['expected_begin_idx']
                product_info['expected_begin_datetime'] = actual['expected_begin_datetime']
                report['stale_reference'].append(product_info)
            else:
                report['complete'].append(product_info)

    # Find unexpected products (day_index not in expected)
    for day_index, actual in actual_products.items():
        if day_index not in expected_day_indices:
            try:
                idx_position = sensing_days_index.index(day_index)
                k_cycle = idx_position // k
                reason = "sensing time in database but not in expected (K-cycle not evaluated)"
            except ValueError:
                idx_position = -1
                k_cycle = -1
                reason = "day_index not in burst database (forward processing)"

            report['unexpected'].append({
                'day_index': day_index,
                'index_position': idx_position,
                'k_cycle': k_cycle,
                'reason': reason,
                'granule_ur': actual['granule_ur'],
                'end_datetime': actual['end_datetime'],
                'completeness_pct': actual['completeness_pct']
            })

    # Add duplicates
    for day_index, dup_info in duplicates.items():
        report['duplicates'].append({
            'day_index': day_index,
            'count': dup_info['count'],
            'best': dup_info['best']['granule_ur'],
            'best_completeness': dup_info['best']['completeness_pct'],
            'others': [p['granule_ur'] for p in dup_info['others']]
        })

    # Summary statistics
    matched = len(report['complete']) + len(report['incomplete']) + len(report['stale_reference'])
    triggerable_expected = len(expected_products) - len(report['not_triggerable'])

    # Count products found despite K-cycle appearing untriggerable
    found_despite_untriggerable = (
        sum(1 for p in report['complete'] if p.get('found_despite_untriggerable')) +
        sum(1 for p in report['incomplete'] if p.get('found_despite_untriggerable')) +
        sum(1 for p in report['stale_reference'] if p.get('found_despite_untriggerable'))
    )

    report['summary'] = {
        'expected': len(expected_products),
        'found': len(actual_products),
        'complete': len(report['complete']),
        'incomplete': len(report['incomplete']),
        'stale_reference': len(report['stale_reference']),
        'missing': len(report['missing']),
        'not_triggerable': len(report['not_triggerable']),
        'unexpected': len(report['unexpected']),
        'duplicates': len(report['duplicates']),
        'anomalies': len(report['anomalies']),
        'found_despite_untriggerable': found_despite_untriggerable,
        'skipped_cslcs': skipped_cslcs['total_skipped_cslcs'],
        'skipped_sensing_times': skipped_cslcs['total_skipped_sensing_times'],
        'coverage_pct': (matched / triggerable_expected * 100) if triggerable_expected > 0 else 0
    }

    return report


def print_audit_report(report):
    """Print human-readable audit report."""
    print()
    print("=" * 120)
    print(f"DISP-S1 COMPLETENESS AUDIT - FRAME {report['frame_id']}")
    print("=" * 120)
    print(f"Bursts: {report['num_bursts']}  |  K: {report['k']}  |  "
          f"Range: {report['first_sensing_datetime'][:10] if report['first_sensing_datetime'] else 'N/A'} to "
          f"{report['last_sensing_datetime'][:10] if report['last_sensing_datetime'] else 'N/A'}")
    print()

    s = report['summary']
    print("-" * 120)
    print("SUMMARY")
    print("-" * 120)
    print(f"Expected: {s['expected']}  |  Found: {s['found']}  |  Coverage: {s['coverage_pct']:.1f}%")
    print(f"  Complete: {s['complete']}  |  Incomplete: {s['incomplete']}  |  Stale Reference: {s['stale_reference']}")
    print(f"  Missing: {s['missing']}  |  Not Triggerable: {s['not_triggerable']}  |  Unexpected: {s['unexpected']}")
    if s['duplicates'] > 0:
        print(f"  Duplicates: {s['duplicates']}")
    if s['anomalies'] > 0:
        print(f"  Products with Anomalous Inputs: {s['anomalies']}")
    if s.get('found_despite_untriggerable', 0) > 0:
        print(f"  Found Despite Untriggerable K-cycle: {s['found_despite_untriggerable']} (CSLC gaps in current data)")
    if s.get('skipped_cslcs', 0) > 0:
        print(f"  Skipped CSLCs (not in database): {s['skipped_cslcs']} CSLCs across {s['skipped_sensing_times']} sensing times")
    print()

    # Missing products (actionable)
    if report['missing']:
        print("-" * 120)
        print(f"MISSING PRODUCTS ({len(report['missing'])}) - Actionable")
        print("-" * 120)
        print(f"{'Idx':>6} | {'Day':>8} | {'K-Cyc':>6} | {'Pos':>4} | {'Sensing Date':>12} | {'CSLCs':>10}")
        print("-" * 120)
        for m in sorted(report['missing'], key=lambda x: x['index_position'])[:50]:
            date = m['sensing_datetime'][:10] if m['sensing_datetime'] else 'N/A'
            cslcs = f"{m['cslcs_available']}/{m['cslcs_expected']}"
            print(f"{m['index_position']:>6} | {m['day_index']:>8} | {m['k_cycle']:>6} | {m['position_in_k']:>4} | {date:>12} | {cslcs:>10}")
        if len(report['missing']) > 50:
            print(f"... and {len(report['missing']) - 50} more")
        print()

    # Not triggerable (CSLC gaps)
    if report['not_triggerable']:
        print("-" * 120)
        print(f"NOT TRIGGERABLE ({len(report['not_triggerable'])}) - CSLC gaps in K-cycle")
        print("-" * 120)
        print(f"{'Idx':>6} | {'Day':>8} | {'K-Cyc':>6} | {'Pos':>4} | {'Sensing Date':>12} | {'Gaps':>6}")
        print("-" * 120)
        for nt in sorted(report['not_triggerable'], key=lambda x: x['index_position'])[:50]:
            date = nt['sensing_datetime'][:10] if nt['sensing_datetime'] else 'N/A'
            gaps = len(nt.get('cycle_gaps', []))
            print(f"{nt['index_position']:>6} | {nt['day_index']:>8} | {nt['k_cycle']:>6} | {nt['position_in_k']:>4} | {date:>12} | {gaps:>6}")
        if len(report['not_triggerable']) > 50:
            print(f"... and {len(report['not_triggerable']) - 50} more")
        print()

    # Incomplete products
    if report['incomplete']:
        print("-" * 120)
        print(f"INCOMPLETE PRODUCTS ({len(report['incomplete'])})")
        print("-" * 120)
        print(f"{'Idx':>6} | {'K-Cyc':>6} | {'Times':>8} | {'Pct':>6} | Product ID")
        print("-" * 120)
        for inc in sorted(report['incomplete'], key=lambda x: x['index_position'])[:30]:
            times = f"{inc['complete_sensing_times']}/{report['k']}"
            print(f"{inc['index_position']:>6} | {inc['k_cycle']:>6} | {times:>8} | {inc['completeness_pct']:>5.1f}% | {inc['granule_ur'][:65]}")
        if len(report['incomplete']) > 30:
            print(f"... and {len(report['incomplete']) - 30} more")
        print()

    # Stale reference (needs reprocessing)
    if report['stale_reference']:
        print("-" * 120)
        print(f"STALE K-CYCLE REFERENCE ({len(report['stale_reference'])}) - Needs reprocessing")
        print("-" * 120)
        print(f"{'Idx':>6} | {'K-Cyc':>6} | {'Actual Begin':>12} | {'Expected':>10} | Product ID")
        print("-" * 120)
        for stale in sorted(report['stale_reference'], key=lambda x: x['index_position'])[:30]:
            actual = str(stale.get('actual_begin_idx', 'N/A'))
            expected = str(stale.get('expected_begin_idx', 'N/A'))
            print(f"{stale['index_position']:>6} | {stale['k_cycle']:>6} | {actual:>12} | {expected:>10} | {stale['granule_ur'][:55]}")
        if len(report['stale_reference']) > 30:
            print(f"... and {len(report['stale_reference']) - 30} more")
        print()

    # Unexpected products
    if report['unexpected']:
        print("-" * 120)
        print(f"UNEXPECTED PRODUCTS ({len(report['unexpected'])})")
        print("-" * 120)
        print(f"{'Idx':>6} | {'Day':>8} | {'End Date':>12} | Reason")
        print("-" * 120)
        for unexp in sorted(report['unexpected'], key=lambda x: x.get('index_position', -1))[:30]:
            idx = str(unexp['index_position']) if unexp['index_position'] >= 0 else 'N/A'
            date = unexp['end_datetime'][:10] if unexp.get('end_datetime') else 'N/A'
            print(f"{idx:>6} | {unexp['day_index']:>8} | {date:>12} | {unexp['reason']}")
        if len(report['unexpected']) > 30:
            print(f"... and {len(report['unexpected']) - 30} more")
        print()

    # Anomalies
    if report['anomalies']:
        print("-" * 120)
        print(f"PRODUCTS WITH ANOMALOUS CSLC INPUTS ({len(report['anomalies'])})")
        print("-" * 120)
        for anom in report['anomalies'][:10]:
            print(f"Product: {anom['granule_ur'][:70]}")
            for cslc in anom['anomalous_cslcs'][:5]:
                print(f"  - {cslc['reason']}: {cslc['cslc_id'][:50]}")
            if len(anom['anomalous_cslcs']) > 5:
                print(f"  ... and {len(anom['anomalous_cslcs']) - 5} more")
        if len(report['anomalies']) > 10:
            print(f"... and {len(report['anomalies']) - 10} more products with anomalies")
        print()

    # Products found despite K-cycle appearing untriggerable
    found_untriggerable = [
        p for category in ['complete', 'incomplete', 'stale_reference']
        for p in report.get(category, [])
        if p.get('found_despite_untriggerable')
    ]
    if found_untriggerable:
        print("-" * 120)
        print(f"FOUND DESPITE UNTRIGGERABLE K-CYCLE ({len(found_untriggerable)})")
        print("(Products exist but current CSLC data shows gaps in their K-cycle)")
        print("-" * 120)
        print(f"{'Idx':>6} | {'K-Cyc':>6} | {'Gaps':>5} | {'Status':>12} | Product ID")
        print("-" * 120)
        for p in sorted(found_untriggerable, key=lambda x: x['index_position'])[:30]:
            gaps = len(p.get('cycle_gaps', []))
            if p in report.get('complete', []):
                status = 'complete'
            elif p in report.get('incomplete', []):
                status = 'incomplete'
            else:
                status = 'stale_ref'
            print(f"{p['index_position']:>6} | {p['k_cycle']:>6} | {gaps:>5} | {status:>12} | {p['granule_ur'][:55]}")
        if len(found_untriggerable) > 30:
            print(f"... and {len(found_untriggerable) - 30} more")
        print()

    # Skipped CSLCs (sensing times not in database)
    skipped = report.get('skipped_cslcs', {})
    skipped_times = skipped.get('skipped_sensing_times', [])
    if skipped_times:
        print("-" * 120)
        print(f"SKIPPED CSLCs ({skipped['total_skipped_cslcs']} CSLCs) - Sensing times not in burst database")
        print("-" * 120)
        print(f"{'Day Index':>10} | {'Approx Date':>12} | {'CSLCs':>8} | Sample CSLC ID")
        print("-" * 120)
        for st in skipped_times[:30]:
            sample_cslc = st['cslc_ids'][0][:50] if st['cslc_ids'] else 'N/A'
            print(f"{st['day_index']:>10} | {st['approx_date']:>12} | {st['cslc_count']:>8} | {sample_cslc}")
        if len(skipped_times) > 30:
            print(f"... and {len(skipped_times) - 30} more sensing times")
        print()

    # Duplicates
    if report['duplicates']:
        print("-" * 120)
        print(f"DUPLICATES ({len(report['duplicates'])})")
        print("-" * 120)
        for dup in sorted(report['duplicates'], key=lambda x: x['day_index'])[:20]:
            print(f"Day {dup['day_index']}: {dup['count']} products")
            print(f"  Best ({dup['best_completeness']:.1f}%): {dup['best']}")
            for other in dup['others'][:3]:
                print(f"  Other: {other}")
        if len(report['duplicates']) > 20:
            print(f"... and {len(report['duplicates']) - 20} more")
        print()

    print("=" * 120)
    print()


def audit_frame(frame_id, frame_to_bursts, burst_to_frames, start_date, end_date,
                endpoint="OPS", k=DEFAULT_K, max_workers=DEFAULT_MAX_WORKERS):
    """Perform complete audit for a single frame."""
    logging.info(f"Starting audit for frame {frame_id}")

    # 1. Query CSLCs for this frame
    cslc_ids = query_cslcs_for_frame(frame_id, frame_to_bursts, start_date, end_date, endpoint)

    if not cslc_ids:
        logging.warning(f"No CSLCs found for frame {frame_id}")
        return None

    # 2. Calculate expected DISP-S1 products
    expected_products, skipped_cslcs = calculate_expected_disp_s1(cslc_ids, frame_id, frame_to_bursts, burst_to_frames, k)
    logging.info(f"Frame {frame_id}: {len(expected_products)} expected DISP-S1 products")
    if skipped_cslcs['total_skipped_sensing_times'] > 0:
        logging.info(f"Frame {frame_id}: {skipped_cslcs['total_skipped_cslcs']} CSLCs skipped "
                     f"({skipped_cslcs['total_skipped_sensing_times']} sensing times not in database)")

    # 3. Query actual DISP-S1 products
    disp_s1_products = query_disp_s1_for_frame(frame_id, start_date, end_date, endpoint)

    # 4. Fetch ISO XML inputs
    product_to_inputs = fetch_iso_xml_inputs_parallel(disp_s1_products, max_workers)

    # 5. Analyze products
    actual_products, duplicates = analyze_disp_s1_products(
        disp_s1_products, product_to_inputs, frame_to_bursts, burst_to_frames, frame_id, k
    )

    # 6. Generate report
    report = generate_audit_report(frame_id, expected_products, actual_products, duplicates,
                                   frame_to_bursts, k, skipped_cslcs)

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
                        help=f'Max parallel workers (default: {DEFAULT_MAX_WORKERS})')
    parser.add_argument('--output', type=str, default=None,
                        help='Output JSON file path (optional)')
    parser.add_argument('--iso-cache-dir', type=str, default=None,
                        help='Directory for caching ISO XML files')
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

    # Print overall summary for multiple frames
    if len(frame_ids) > 1:
        print()
        print("=" * 120)
        print("OVERALL SUMMARY")
        print("=" * 120)
        total_expected = sum(r['summary']['expected'] for r in all_reports.values())
        total_found = sum(r['summary']['found'] for r in all_reports.values())
        total_complete = sum(r['summary']['complete'] for r in all_reports.values())
        total_incomplete = sum(r['summary']['incomplete'] for r in all_reports.values())
        total_stale = sum(r['summary']['stale_reference'] for r in all_reports.values())
        total_missing = sum(r['summary']['missing'] for r in all_reports.values())

        matched = total_complete + total_incomplete + total_stale
        coverage = (matched / total_expected * 100) if total_expected else 0

        print(f"Frames: {len(all_reports)}  |  Expected: {total_expected}  |  Found: {total_found}")
        print(f"Complete: {total_complete}  |  Incomplete: {total_incomplete}  |  Stale: {total_stale}  |  Missing: {total_missing}")
        print(f"Overall Coverage: {coverage:.1f}%")
        print()

    # Print cache stats
    if args.iso_cache_dir:
        stats = get_cache_stats()
        print("-" * 60)
        print(f"ISO XML Cache: {stats['hits']} hits, {stats['misses']} misses ({stats['hit_rate']:.1f}% hit rate)")
        print()


if __name__ == '__main__':
    main()
