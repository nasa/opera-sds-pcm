"""CRUD operations for DISP-S1 evaluator state-configs.

Two state-config types:
  - CSC (per-cycle): tracks burst completeness for a single frame + sensing date
  - KSC (K-cycle): tracks completeness across K sensing dates for a frame

Follows the NISAR evaluator pattern (find_state_config / create_state_config_dataset).
# ES/OS template priorities: CSC=1, KSC=2 (higher than default=0, distinct patterns prevent conflicts).
"""

import logging
import os
import shutil

from data_subscriber.cslc import disp_s1_constants as c
from util.common_util import backoff_wrapper, create_state_config_dataset

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def make_csc_id(frame_id, sensing_date):
    """Generate the dataset ID for a per-cycle state-config (CSC).

    Format: cslc_s1-cycle-f{frame_id}-{YYYYMMDD}-state-config
    Example: cslc_s1-cycle-f14883-20240801-state-config
    """
    return f"cslc_s1-cycle-f{frame_id}-{sensing_date}-state-config"


def make_ksc_id(frame_id, sensing_date, k, m):
    """Generate the dataset ID for a K-cycle state-config (KSC).

    Format: disp_s1-kcycle-k{k}-m{m}-f{frame_id}-{YYYYMMDD}-state-config
    Example: disp_s1-kcycle-k15-m6-f14883-20240801-state-config
    """
    return f"disp_s1-kcycle-k{int(k)}-m{int(m)}-f{frame_id}-{sensing_date}-state-config"


# ---------------------------------------------------------------------------
# ES queries (read)
# ---------------------------------------------------------------------------

def find_csc(es_conn, state_config_id):
    """Look up a per-cycle state-config (CSC) in ES by _id.

    Returns (metadata_dict, index_name) if found, ({}, None) otherwise.
    """
    return _find_state_config(es_conn, state_config_id, c.CSLC_S1_CYCLE_STATE_CONFIG)


def find_ksc(es_conn, state_config_id):
    """Look up a K-cycle state-config (KSC) in ES by _id.

    Returns (metadata_dict, index_name) if found, ({}, None) otherwise.
    """
    return _find_state_config(es_conn, state_config_id, c.DISP_S1_KCYCLE_STATE_CONFIG)


def _find_state_config(es_conn, state_config_id, state_config_type):
    """Query ES for a state-config document by _id.

    Uses search_by_id (which wraps _search API) rather than get_by_id so it
    works with index aliases / wildcards (grq_*_{type}).
    """
    result = {}
    state_config_index = None

    existing_document = backoff_wrapper(
        es_conn.search_by_id,
        id=state_config_id,
        index=f"grq_*_{state_config_type}*",
        ignore=[404],
    )

    if existing_document.get("found", False):
        result = existing_document.get("_source", {}).get("metadata", {})
        state_config_index = existing_document.get("_index")

    return result, state_config_index


def query_cscs_for_frame(es_conn, frame_id, max_results=1000):
    """Query ES for all CSCs for a given frame, sorted by sensing_date descending.

    Returns list of dicts with _id and metadata.
    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"dataset_type.keyword": c.CSLC_S1_CYCLE_STATE_CONFIG}},
                    {"term": {"metadata.frame_id": frame_id}},
                ]
            }
        },
        "sort": [{"metadata.sensing_date": {"order": "desc"}}],
        "size": max_results,
    }

    result = backoff_wrapper(
        es_conn.query,
        body=body,
        index=f"grq_*_{c.CSLC_S1_CYCLE_STATE_CONFIG}*",
    )

    return result if result else []


def query_incomplete_kscs_with_sensing_date(es_conn, frame_id, k, m, sensing_date,
                                            exclude_reference_date=None):
    """Query ES for incomplete KSCs that contain a given sensing_date in their window.

    Used for cascade re-evaluation: when a CSC becomes complete, find all
    incomplete KSCs whose window_sensing_dates includes that CSC's sensing_date.

    Args:
        es_conn: ES connection
        frame_id: frame ID to filter on
        k: k parameter
        m: m parameter
        sensing_date: the sensing_date (YYYY-MM-DD) to search for in window_sensing_dates
        exclude_reference_date: if set, exclude KSCs whose sensing_date matches this value

    Returns list of ES hits.
    """
    must_clauses = [
        {"term": {"dataset_type.keyword": c.DISP_S1_KCYCLE_STATE_CONFIG}},
        {"term": {"metadata.frame_id": frame_id}},
        {"term": {"metadata.k": k}},
        {"term": {"metadata.m": m}},
        {"term": {"metadata.is_complete": False}},
        {"term": {"metadata.window_sensing_dates": sensing_date}},
    ]

    must_not_clauses = []
    if exclude_reference_date:
        must_not_clauses.append(
            {"term": {"metadata.sensing_date": exclude_reference_date}}
        )

    body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "must_not": must_not_clauses,
            }
        },
        "size": 100,
    }

    result = backoff_wrapper(
        es_conn.query,
        body=body,
        index=f"grq_*_{c.DISP_S1_KCYCLE_STATE_CONFIG}*",
    )

    return result if result else []


def query_blocked_kscs_for_frame(es_conn, frame_id):
    """Query ES for incomplete KSCs where all cycles are complete.

    These are KSCs blocked on CCSLCs, static layers, or ionosphere.
    Used to re-evaluate when new CCSLCs are ingested.

    Returns list of ES hits.
    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"dataset_type.keyword": c.DISP_S1_KCYCLE_STATE_CONFIG}},
                    {"term": {"metadata.frame_id": frame_id}},
                    {"term": {"metadata.all_cycles_complete": True}},
                    {"term": {"metadata.is_complete": False}},
                ]
            }
        },
        "size": 100,
    }

    result = backoff_wrapper(
        es_conn.query,
        body=body,
        index=f"grq_*_{c.DISP_S1_KCYCLE_STATE_CONFIG}*",
    )

    return result if result else []


# ---------------------------------------------------------------------------
# Per-cycle state-config (CSC): create
# ---------------------------------------------------------------------------

def create_csc(frame_id, acquisition_cycle, sensing_date, expected_burst_ids,
               found_burst_ids, cslc_product_paths, start_time, geojson=None):
    """Create a per-cycle state-config (CSC) dataset on the filesystem.

    HySDS post-processing (publish_datasets_parallel) picks up the
    {dataset_id}/ directory and indexes into ES.

    Always re-creates from scratch (no incremental updates).
    """
    state_config_id = make_csc_id(frame_id, sensing_date)

    expected = sorted(expected_burst_ids)
    found = sorted(found_burst_ids)
    missing = sorted(set(expected) - set(found))
    coverage_actual = len(found)
    coverage_expected = len(expected)

    is_complete = len(missing) == 0
    if is_complete:
        completeness_reason = f"complete: {coverage_actual}/{coverage_expected} bursts"
    else:
        completeness_reason = f"incomplete: {coverage_actual}/{coverage_expected} bursts, missing {len(missing)}"

    metadata = {
        c.STATE_CONFIG_TYPE: c.CSLC_S1_CYCLE_STATE_CONFIG,
        c.FRAME_ID: frame_id,
        c.ACQUISITION_CYCLE: acquisition_cycle,
        c.SENSING_DATE: sensing_date,
        c.EXPECTED_BURST_IDS: expected,
        c.FOUND_BURST_IDS: found,
        c.MISSING_BURST_IDS: missing,
        c.CSLC_PRODUCT_PATHS: sorted(cslc_product_paths),
        c.COVERAGE_ACTUAL: coverage_actual,
        c.COVERAGE_EXPECTED: coverage_expected,
        c.IS_COMPLETE: is_complete,
        c.COMPLETENESS_REASON: completeness_reason,
    }

    # Remove existing dataset dir if present (will be recreated)
    if os.path.isdir(state_config_id):
        shutil.rmtree(state_config_id)

    logger.info(f"Creating CSC: {state_config_id} "
                f"(coverage: {coverage_actual}/{coverage_expected}, "
                f"is_complete: {is_complete})")

    create_state_config_dataset(
        dataset_name=state_config_id,
        metadata=metadata,
        start_time=start_time,
        dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
        geojson=geojson,
    )

    return state_config_id, metadata


# ---------------------------------------------------------------------------
# K-cycle state-config (KSC): create
# ---------------------------------------------------------------------------

def create_ksc(frame_id, sensing_date, k, m, window_sensing_dates,
               window_entries, product_paths, compressed_cslc_satisfied,
               compressed_cslc_ids, bounding_box, save_compressed_cslc,
               start_time, ccslc_detail="",
               static_layers_satisfied=True, ionosphere_satisfied=True,
               geojson=None):
    """Create a K-cycle state-config (KSC) dataset on the filesystem.

    Standalone — contains full copies of all k CSC bodies so the DISP-S1 job
    needs only the KSC (no dereferencing CSC IDs).

    Always re-creates from scratch (no incremental updates).
    """
    state_config_id = make_ksc_id(frame_id, sensing_date, k, m)

    cycles_complete = sum(
        1 for csc in window_entries if csc.get(c.IS_COMPLETE, False)
    )
    cycles_expected = k
    all_complete = cycles_complete == cycles_expected

    is_complete = (all_complete and compressed_cslc_satisfied
                   and static_layers_satisfied and ionosphere_satisfied)
    if is_complete:
        ccslc_info = ccslc_detail if ccslc_detail else f"{len(compressed_cslc_ids)} CCSLCs"
        completeness_reason = (
            f"ready: {cycles_complete} CSLCs + {ccslc_info}"
        )
    elif not all_complete:
        completeness_reason = (
            f"K-window incomplete: {cycles_complete}/{cycles_expected} CSCs complete"
        )
    else:
        missing_parts = []
        if not compressed_cslc_satisfied:
            ccslc_info = ccslc_detail if ccslc_detail else "missing CCSLCs"
            missing_parts.append(ccslc_info)
        if not static_layers_satisfied:
            missing_parts.append("missing static layers")
        if not ionosphere_satisfied:
            missing_parts.append("missing ionosphere")
        completeness_reason = (
            f"incomplete: {cycles_complete}/{cycles_expected} CSCs complete, "
            f"{'; '.join(missing_parts)}"
        )

    # Reference acquisition_cycle from the last (newest) window entry
    ref_acquisition_cycle = (
        window_entries[-1].get(c.ACQUISITION_CYCLE) if window_entries else None
    )

    metadata = {
        "id": state_config_id,
        c.STATE_CONFIG_TYPE: c.DISP_S1_KCYCLE_STATE_CONFIG,
        c.FRAME_ID: frame_id,
        c.ACQUISITION_CYCLE: ref_acquisition_cycle,
        c.SENSING_DATE: sensing_date,
        c.K: k,
        c.M: m,
        c.WINDOW_SENSING_DATES: sorted(window_sensing_dates),
        c.WINDOW_ENTRIES: window_entries,
        c.CYCLES_COMPLETE: cycles_complete,
        c.CYCLES_EXPECTED: cycles_expected,
        c.ALL_CYCLES_COMPLETE: all_complete,
        c.PRODUCT_PATHS: product_paths,
        c.COMPRESSED_CSLC_SATISFIED: compressed_cslc_satisfied,
        c.COMPRESSED_CSLC_IDS: compressed_cslc_ids,
        c.BOUNDING_BOX: bounding_box,
        c.SAVE_COMPRESSED_CSLC: save_compressed_cslc,
        c.STATIC_LAYERS_SATISFIED: static_layers_satisfied,
        c.IONOSPHERE_SATISFIED: ionosphere_satisfied,
        c.IS_COMPLETE: is_complete,
        c.COMPLETENESS_REASON: completeness_reason,
    }

    # Remove existing dataset dir if present (will be recreated)
    if os.path.isdir(state_config_id):
        shutil.rmtree(state_config_id)

    logger.info(f"Creating KSC: {state_config_id} "
                f"(cycles: {cycles_complete}/{cycles_expected}, "
                f"ccslc_satisfied: {compressed_cslc_satisfied}, "
                f"is_complete: {is_complete})")

    create_state_config_dataset(
        dataset_name=state_config_id,
        metadata=metadata,
        start_time=start_time,
        dataset_type=c.DISP_S1_KCYCLE_STATE_CONFIG,
        geojson=geojson,
    )

    return state_config_id, metadata
