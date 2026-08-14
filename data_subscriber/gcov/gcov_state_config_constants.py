GCOV_BATCH = 'L2_GCOV_NI_BATCH'
MGRS_SET_STATE_CONFIG = 'dswx_ni-state-config'
MGRS_SET_EXPIRED_STATE_CONFIG = 'dswx_ni-expired-state-config'

GCOV_DATASET_ES_PATTERN = "grq_*_l2_gcov_ni-*"
GCOV_BATCH_DATASET_ES_PATTERN = "grq_*_l2_gcov_ni_batch-*"
MGRS_SET_STATE_CONFIG_ES_PATTERN = f'grq_*_{MGRS_SET_STATE_CONFIG}-*'
MGRS_SET_EXPIRED_STATE_CONFIG_ES_PATTERN = f'grq_*_{MGRS_SET_EXPIRED_STATE_CONFIG}-*'

STATE_CONFIG_TYPE = "state_config_type"
MGRS_SET_ID = "mgrs_set_id"
CYCLE_NUMBER = "cycle_number"
IS_COMPLETE = "is_complete"
COMPLETENESS_REASON = "completeness_reason"
EXPECTED_TRACK_FRAMES = "expected_track_frames"
FOUND_TRACK_FRAMES = "found_track_frame"
EXCLUDED_TRACK_FRAMES = "excluded_track_frame"
MISSING_TRACK_FRAMES = "missing_track_frame"
POLARIZATION = "polarization"
LAND_OCEAN_FLAG = "land_ocean_flag"
BOUNDING_BOX = "bounding_box"
COVERAGE_AREA = "coverage_area"
GCOV_HTTPS_PRODUCT_PATHS = "gcov_https_product_paths"
GCOV_S3_PRODUCT_PATHS = "gcov_s3_product_paths"
COVERAGE_ACTUAL = "coverage_actual"
COVERAGE_EXPECTED = "coverage_expected"
EXPIRATION_DATE = "expiration_date"
IS_EXPIRED = "is_expired"
IS_SKIPPED = "is_skipped"
SKIPPED_REASON = "skipped_reason"

VALID_POLS = {"DH", "DV", "QP"}
VALID_MODES = {"40", "20"}
