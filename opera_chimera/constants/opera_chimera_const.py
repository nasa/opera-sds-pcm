from chimera.commons.constants import ChimeraConstants
from job_accountability.catalog import ES_INDEX as job_accountability_index


class OperaChimeraConstants(ChimeraConstants):
    def __init__(self):
        ChimeraConstants.__init__(self)

    GET_PRODUCT_COUNTER = "get_product_counter"

    CRID = "CRID"

    PRODUCT_TYPES = "PRODUCT_TYPES"

    PATTERN = "Pattern"

    FORCE_INGEST = "FORCE_INGEST"

    CNM_VERSION = "CNM_VERSION"

    PROCESSING_MODE_KEY = "processing_mode"

    GET_PRODUCT_METADATA = "get_product_metadata"

    GET_METADATA = "get_metadata"

    GET_ORBIT_EPHEMERIS = "get_orbit_ephemeris"

    DYN_ANCILLARY_FILES = "getDynamicAncillaryFileTypes"

    LATEST_PRODUCTS_OVER_TIME_RANGE = "get_latest_products_over_time_range"

    MOST_RECENT_FILES = "get_product_most_recent"

    MOST_RECENT_VERSION_FILES = "get_product_most_recent_version"

    ANC_MATCHING_GRANULE = "get_anc_matching_granule"

    SET_PGE_JOB_NAME = "set_pge_job_name"

    SET_MOCK_METADATA = "set_mock_metadata"

    SET_BASE_NAME = "set_base_name"

    SET_EXTRA_PGE_OUTPUT_METADATA = "set_extra_pge_output_metadata"

    GET_OBSERVATIONS_DATA = "get_observations_data"

    GET_HARDCODED_METADATA = "get_hardcoded_metadata"

    SET_PCM_RETRIEVAL_ID = "set_pcm_retrieval_id"

    SET_DAAC_PRODUCT_TYPE = "set_daac_product_type"

    TBD_PRECONDITIONS = "undefinedPreconditions"

    ATTRIBUTE_NAMES_KEY = "attribute_names"

    INPUT_DATASET_ID = "input_dataset_id"

    DATASET_TYPE = "dataset_type"

    # Run Config attribute names
    ALGORITHM_VERSION_ID = "AlgorithmVersionID"

    PARAMETER_VERSION_ID = "ParameterVersionID"

    COMPOSITE_RELEASE_ID = "CompositeReleaseID"

    SW_VERSION_ID = "SWVersionID"

    INPUT_FILE_PATH = "InputFilePath"

    INPUT_FILE_GROUP = "InputFileGroup"

    STATIC_ANCILLARY_FILE_GROUP = "StaticAncillaryFileGroup"

    DYNAMIC_ANCILLARY_FILE_GROUP = "DynamicAncillaryFileGroup"

    PRODUCTION_DATE_TIME = "ProductionDateTime"

    PRODUCT_PATH = "ProductPath"

    DEBUG_PATH = "DebugPath"

    PRODUCT_COUNTER = "ProductCounter"

    PRODUCT_VERSION = "product_version"

    FILE_NAME = "FileName"

    ORBIT_EPHEMERIS_FILE = "OrbitEphemerisFile"

    OBSERVATION = "Observation"

    OBSERVATIONS = "Observations"

    FRAME_NUMBER = "FrameNumber"

    PGE_JOB_NAME = "pge_job_name"

    MOCK_METADATA = "mock_metadata"

    BASE_NAME = "base_name"

    SAFE_FILE_PATH = "safe_file_path"

    ORBIT_FILE_PATH = "orbit_file_path"

    BURST_ID = "burst_id"

    DEM_FILE = "dem_file"

    LANDCOVER_FILE = "landcover_file"

    WORLDCOVER_FILE = "worldcover_file"

    WORLDCOVER_VER = "worldcover_version"

    WORLDCOVER_YEAR = "worldcover_year"

    PLANNED_OBSERVATION_ID = "PlannedObservationId"

    PLANNED_OBSERVATION_TIMESTAMP = "PlannedObservationTimestamp"

    IS_URGENT_OBSERVATION = "IsUrgentObservation"

    CONFIGURATION_ID = "ConfigurationId"

    MISSION_CYCLE = "MissionCycle"

    START_TIME = "StartTime"

    END_TIME = "EndTime"

    IS_URGENT = "is_urgent"

    URGENT_RESPONSE_FIELD = "is_urgent_response"

    ORBIT = "Orbit"

    ABSOLUTE_ORBIT_NUMBER = "AbsoluteOrbitNumber"

    RELATIVE_ORBIT_NUMBER = "RelativeOrbitNumber"

    ORBIT_DIRECTION = "OrbitDirection"

    LOOK_DIRECTION = "LookDirection"

    SCLKSCET = "sclkscet"

    REFINED_POINTING = "Pointing"

    ANT_PATTERN = "AntennaPattern"

    WAVEFORM = "Waveform"

    EXT_CALIBRATION = "ExternalCalibration"

    INT_CALIBRATION = "InternalCalibration"

    POL_CALIBRATION = "PolarimetricCalibration"

    BOOK_CALIBRATION = "BookendCalibration"

    PROCESSINGTYPE = "ProcessingType"

    PROCESSING_TYPE = "processing_type"

    FILE_SIZE_LIMIT = "FILE_SIZE_LIMIT"

    FILESIZELIMIT = "FileSizeLimit"

    NUMBEROFTHREADS = "NumberOfThreads"

    GPU_ENABLED = "gpu_enabled"

    S3_BUCKET = "s3_bucket"

    S3_KEY = "s3_key"

    # PGE names
    #L3_DSWX_HLS = "L3_DSWX_HLS"

    # Other Constants
    JOB_ACCOUNTABILITY_INDEX = job_accountability_index

    TASK_INDEX = "task_status-current"

    TASK_ID_FIELD = "task_id"

    JOB_INDEX = "job_status-current"

    JOB_ID_FIELD = "job_id"

    OUTPUT_DATASETS = "output_datasets"

    LAST_MOD = "last_modified"

    PRIMARY_INPUT = "primary_input"

    PRIMARY_OUTPUT = "primary_output"

    EXTRA_PGE_OUTPUT_METADATA = "extra_pge_output_metadata"

    GET_PRODUCTS = "get_products"

    IS_STATE_CONFIG_TRIGGER = "is_state_config_trigger"

    FILE_NAMES_KEY = "file_names_key"

    VERSION_KEY = "version_key"

    CAST_STRING_TO_INT = "cast_string_to_int"

    BBOX = "bbox"

    GET_SLC_S1_BURST_ID = "get_slc_s1_burst_id"

    GET_SLC_S1_SAFE_FILE = "get_slc_s1_safe_file"

    GET_SLC_S1_ORBIT_FILE = "get_slc_s1_orbit_file"

    GET_SLC_S1_DEM = "get_slc_s1_dem"

    GET_DEM_BBOX = "get_dem_bbox"

    GET_PRODUCT_VERSION = "get_product_version"

    GET_DSWX_HLS_DEM = "get_dswx_hls_dem"

    GET_LANDCOVER = "get_landcover"

    GET_WORLDCOVER = "get_worldcover"

    GET_PGE_SETTINGS_VALUES = "get_pge_settings_values"
