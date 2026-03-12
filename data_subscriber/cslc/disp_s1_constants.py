"""Constants for DISP-S1 evaluator state-config metadata fields."""

# State-config types (used as ES index suffix via grq_*_{type})
CSLC_S1_CYCLE_STATE_CONFIG = "cslc_s1-cycle-state-config"
DISP_S1_KCYCLE_STATE_CONFIG = "disp_s1-kcycle-state-config"

# Shared fields
STATE_CONFIG_TYPE = "state_config_type"
FRAME_ID = "frame_id"
ACQUISITION_CYCLE = "acquisition_cycle"
SENSING_DATE = "sensing_date"
IS_COMPLETE = "is_complete"
COMPLETENESS_REASON = "completeness_reason"

# CSC fields
EXPECTED_BURST_IDS = "expected_burst_ids"
FOUND_BURST_IDS = "found_burst_ids"
MISSING_BURST_IDS = "missing_burst_ids"
CSLC_PRODUCT_PATHS = "cslc_product_paths"
COVERAGE_ACTUAL = "coverage_actual"
COVERAGE_EXPECTED = "coverage_expected"

# KSC fields
K = "k"
M = "m"
WINDOW_SENSING_DATES = "window_sensing_dates"
WINDOW_ENTRIES = "window_entries"
CYCLES_COMPLETE = "cycles_complete"
CYCLES_EXPECTED = "cycles_expected"
ALL_CYCLES_COMPLETE = "all_cycles_complete"
PRODUCT_PATHS = "product_paths"
COMPRESSED_CSLC_SATISFIED = "compressed_cslc_satisfied"
COMPRESSED_CSLC_IDS = "compressed_cslc_ids"
BOUNDING_BOX = "bounding_box"
SAVE_COMPRESSED_CSLC = "save_compressed_cslc"
FORCE_PUBLISH = "force_publish"
