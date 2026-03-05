"""CRUD operations for DISP-S1 evaluator state-configs.

Two state-config types:
  - Per-cycle: tracks burst completeness for a single frame + acquisition cycle
  - K-group: tracks completeness across K acquisition cycles for a frame

Follows the NISAR evaluator pattern (find_state_config / create_state_config_dataset).
"""

import logging
import os

from data_subscriber.cslc import disp_s1_constants as c
from util.common_util import backoff_wrapper, create_state_config_dataset

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def make_cycle_state_config_id(frame_id, acquisition_cycle):
    """Generate the dataset ID for a per-cycle state-config.

    Format: disp-s1_f{frame_id}_a{acquisition_cycle}_state-config
    Example: disp-s1_f7098_a5_state-config
    """
    return f"disp-s1_f{frame_id}_a{acquisition_cycle}_state-config"


def make_k_group_state_config_id(frame_id, k_group_index):
    """Generate the dataset ID for a K-group state-config.

    Format: disp-s1_f{frame_id}_k{k_group_index}_state-config
    Example: disp-s1_f7098_k1_state-config
    """
    return f"disp-s1_f{frame_id}_k{k_group_index}_state-config"


# ---------------------------------------------------------------------------
# ES queries (read)
# ---------------------------------------------------------------------------

def find_cycle_state_config(es_conn, state_config_id):
    """Look up a per-cycle state-config in ES by _id.

    Returns (metadata_dict, index_name) if found, ({}, None) otherwise.
    """
    return _find_state_config(es_conn, state_config_id, c.DISP_S1_CYCLE_STATE_CONFIG)


def find_k_group_state_config(es_conn, state_config_id):
    """Look up a K-group state-config in ES by _id.

    Returns (metadata_dict, index_name) if found, ({}, None) otherwise.
    """
    return _find_state_config(es_conn, state_config_id, c.DISP_S1_K_GROUP_STATE_CONFIG)


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
        index=f"grq_*_{state_config_type}",
        ignore=[404],
    )

    if existing_document.get("found", False):
        result = existing_document.get("_source", {}).get("metadata", {})
        state_config_index = existing_document.get("_index")

    return result, state_config_index


# ---------------------------------------------------------------------------
# Per-cycle state-config: create / update
# ---------------------------------------------------------------------------

def create_cycle_state_config(frame_id, acquisition_cycle, expected_burst_ids,
                              found_burst_ids, found_cslc_granule_ids,
                              cslc_product_paths, start_time):
    """Create a per-cycle state-config dataset on the filesystem.

    HySDS post-processing (publish_datasets_parallel) picks up the
    {dataset_id}/ directory and indexes into ES.
    """
    state_config_id = make_cycle_state_config_id(frame_id, acquisition_cycle)

    expected = sorted(expected_burst_ids)
    found = sorted(found_burst_ids)
    missing = sorted(set(expected) - set(found))
    coverage_actual = len(found)
    coverage_expected = len(expected)
    coverage_pct = round(coverage_actual / coverage_expected * 100, 1) if coverage_expected > 0 else 0.0

    metadata = {
        c.STATE_CONFIG_TYPE: c.DISP_S1_CYCLE_STATE_CONFIG,
        c.FRAME_ID: frame_id,
        c.ACQUISITION_CYCLE: acquisition_cycle,
        c.DOWNLOAD_BATCH_ID: f"f{frame_id}_a{acquisition_cycle}",
        c.EXPECTED_BURST_IDS: expected,
        c.FOUND_BURST_IDS: found,
        c.MISSING_BURST_IDS: missing,
        c.FOUND_CSLC_GRANULE_IDS: sorted(found_cslc_granule_ids),
        c.CSLC_PRODUCT_PATHS: sorted(cslc_product_paths),
        c.COVERAGE_ACTUAL: coverage_actual,
        c.COVERAGE_EXPECTED: coverage_expected,
        c.COVERAGE_PERCENTAGE: coverage_pct,
        c.CYCLE_COMPLETE: len(missing) == 0,
    }

    logger.info(f"Creating per-cycle state-config: {state_config_id} "
                f"(coverage: {coverage_actual}/{coverage_expected} = {coverage_pct}%)")

    create_state_config_dataset(
        dataset_name=state_config_id,
        metadata=metadata,
        start_time=start_time,
    )

    return state_config_id, metadata


def update_cycle_state_config(existing_metadata, new_burst_id,
                              new_cslc_granule_id, new_cslc_product_path,
                              frame_id, acquisition_cycle, start_time):
    """Update an existing per-cycle state-config with a newly arrived burst.

    Reads existing metadata, merges in the new burst, recomputes coverage,
    and re-creates the dataset files on the filesystem.  HySDS post-processing
    will overwrite the ES document (same _id).
    """
    state_config_id = make_cycle_state_config_id(frame_id, acquisition_cycle)

    found_burst_ids = list(existing_metadata.get(c.FOUND_BURST_IDS, []))
    found_cslc_granule_ids = list(existing_metadata.get(c.FOUND_CSLC_GRANULE_IDS, []))
    cslc_product_paths = list(existing_metadata.get(c.CSLC_PRODUCT_PATHS, []))

    if new_burst_id not in found_burst_ids:
        found_burst_ids.append(new_burst_id)
    if new_cslc_granule_id not in found_cslc_granule_ids:
        found_cslc_granule_ids.append(new_cslc_granule_id)
    if new_cslc_product_path not in cslc_product_paths:
        cslc_product_paths.append(new_cslc_product_path)

    expected_burst_ids = existing_metadata.get(c.EXPECTED_BURST_IDS, [])

    # Remove existing dataset dir if present (will be recreated)
    if os.path.isdir(state_config_id):
        import shutil
        shutil.rmtree(state_config_id)

    return create_cycle_state_config(
        frame_id=frame_id,
        acquisition_cycle=acquisition_cycle,
        expected_burst_ids=expected_burst_ids,
        found_burst_ids=found_burst_ids,
        found_cslc_granule_ids=found_cslc_granule_ids,
        cslc_product_paths=cslc_product_paths,
        start_time=start_time,
    )


# ---------------------------------------------------------------------------
# K-group state-config: create / update
# ---------------------------------------------------------------------------

def create_k_group_state_config(frame_id, k_group_index, k, m,
                                acquisition_cycles, cycle_state_config_ids,
                                cycle_completeness, total_cslcs_found,
                                total_cslcs_expected, compressed_cslc_satisfied,
                                compressed_cslc_ids, start_time):
    """Create a K-group state-config dataset on the filesystem."""
    state_config_id = make_k_group_state_config_id(frame_id, k_group_index)

    cycles_complete = sum(1 for v in cycle_completeness.values() if v)
    cycles_expected = len(acquisition_cycles)
    all_complete = cycles_complete == cycles_expected

    metadata = {
        c.STATE_CONFIG_TYPE: c.DISP_S1_K_GROUP_STATE_CONFIG,
        c.FRAME_ID: frame_id,
        c.K_GROUP_INDEX: k_group_index,
        c.K: k,
        c.M: m,
        c.ACQUISITION_CYCLES: sorted(acquisition_cycles),
        c.CYCLE_STATE_CONFIG_IDS: sorted(cycle_state_config_ids),
        c.CYCLE_COMPLETENESS: cycle_completeness,
        c.CYCLES_COMPLETE: cycles_complete,
        c.CYCLES_EXPECTED: cycles_expected,
        c.ALL_CYCLES_COMPLETE: all_complete,
        c.TOTAL_CSLCS_FOUND: total_cslcs_found,
        c.TOTAL_CSLCS_EXPECTED: total_cslcs_expected,
        c.COMPRESSED_CSLC_SATISFIED: compressed_cslc_satisfied,
        c.COMPRESSED_CSLC_IDS: compressed_cslc_ids,
        c.IS_COMPLETE: all_complete and compressed_cslc_satisfied,
        c.FORCE_SUBMIT: False,
        c.DOWNLOAD_JOB_ID: None,
    }

    logger.info(f"Creating K-group state-config: {state_config_id} "
                f"(cycles: {cycles_complete}/{cycles_expected}, "
                f"ccslc_satisfied: {compressed_cslc_satisfied}, "
                f"is_complete: {metadata[c.IS_COMPLETE]})")

    create_state_config_dataset(
        dataset_name=state_config_id,
        metadata=metadata,
        start_time=start_time,
    )

    return state_config_id, metadata


def update_k_group_state_config(existing_metadata, cycle_completeness,
                                total_cslcs_found, total_cslcs_expected,
                                compressed_cslc_satisfied, compressed_cslc_ids,
                                frame_id, k_group_index, start_time):
    """Update an existing K-group state-config with new cycle completeness data.

    Re-creates the dataset files on the filesystem.  HySDS post-processing
    will overwrite the ES document (same _id).
    """
    state_config_id = make_k_group_state_config_id(frame_id, k_group_index)

    # Remove existing dataset dir if present (will be recreated)
    if os.path.isdir(state_config_id):
        import shutil
        shutil.rmtree(state_config_id)

    return create_k_group_state_config(
        frame_id=frame_id,
        k_group_index=k_group_index,
        k=existing_metadata.get(c.K),
        m=existing_metadata.get(c.M),
        acquisition_cycles=existing_metadata.get(c.ACQUISITION_CYCLES),
        cycle_state_config_ids=existing_metadata.get(c.CYCLE_STATE_CONFIG_IDS),
        cycle_completeness=cycle_completeness,
        total_cslcs_found=total_cslcs_found,
        total_cslcs_expected=total_cslcs_expected,
        compressed_cslc_satisfied=compressed_cslc_satisfied,
        compressed_cslc_ids=compressed_cslc_ids,
        start_time=start_time,
    )
