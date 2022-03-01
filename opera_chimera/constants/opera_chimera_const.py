from chimera.commons.constants import ChimeraConstants
#from pass_accountability.catalog import ES_INDEX as pass_accountability_index
#from observation_accountability.catalog import ES_INDEX as observation_accountability_index
#from Track_Frame_Accountability.catalog import ES_INDEX as track_frame_accountability_index


class OperaChimeraConstants(ChimeraConstants):
    def __init__(self):
        ChimeraConstants.__init__(self)

    GET_PRODUCT_COUNTER = "get_product_counter"
    CRID = "CRID"
    PRODUCT_TYPES = "PRODUCT_TYPES"
    PATTERN = "Pattern"
    FORCE_INGEST = "FORCE_INGEST"

    # processing mode key
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

    SET_L0B_MOCK_METADATA = "set_l0b_mock_metadata"

    SET_BASE_NAME = "set_base_name"

    SET_L0B_BASE_NAMES = "set_l0b_base_names"

    SET_EXTRA_PGE_OUTPUT_METADATA = "set_extra_pge_output_metadata"

    GET_OBSERVATIONS_DATA = "get_observations_data"

    GET_L0B_ANCILLAY_FILES = "get_l0b_ancillary_files"

    GET_HARDCODED_METADATA = "get_hardcoded_metadata"

    SET_PCM_RETRIEVAL_ID = "set_pcm_retrieval_id"

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
    FILE_NAME = "FileName"
    ORBIT_EPHEMERIS_FILE = "OrbitEphemerisFile"
    OBSERVATION = "Observation"
    OBSERVATIONS = "Observations"
    FRAME_NUMBER = "FrameNumber"
    PGE_JOB_NAME = "pge_job_name"
    MOCK_METADATA = "mock_metadata"
    BASE_NAME = "base_name"

    OROST = ""
    LRCLK_FILE = "LRCLKFile"
    DEM_FILE = "DEMFile"
    RADAR_CONFIGURATION_FILE = "RadarConfigurationFile"
    CHIRP_PARAMETER_FILE = "ChirpParameterFile"
    WAVE_CONFIGURATION_FILE = "WaveformConfigurationFile"

    PLANNED_DATATAKE_ID = "PlannedDatatakeId"
    PLANNED_OBSERVATION_ID = "PlannedObservationId"
    PLANNED_DATATAKE_TIMESTAMP = "PlannedDatatakeTimestamp"
    PLANNED_OBSERVATION_TIMESTAMP = "PlannedObservationTimestamp"
    IS_URGENT_OBSERVATION = "IsUrgentObservation"
    CONFIGURATION_ID = "ConfigurationId"
    MISSION_CYCLE = "MissionCycle"
    TOTAL_NUMBER_RANGELINES = "TotalNumberRangelines"
    RANGELINES_TO_SKIP = "RangelinesToSkip"
    TOTAL_RANGELINES_FAILED_CHECKSUM = "TotalNumberOfRangelinesFailedChecksum"
    START_TIME = "StartTime"
    END_TIME = "EndTime"
    IS_URGENT = "is_urgent"
    URGENT_RESPONSE_FIELD = "is_urgent_response"

    ORBIT = "Orbit"
    ABSOLUTE_ORBIT_NUMBER = "AbsoluteOrbitNumber"
    RELATIVE_ORBIT_NUMBER = "RelativeOrbitNumber"
    ORBIT_DIRECTION = "OrbitDirection"
    LOOK_DIRECTION = "LookDirection"

    OROST = "orost"
    SROST = "srost"
    OFS = "ofs"

    CHIRP_PARAM = "chirp_param"
    SCLKSCET = "sclkscet"
    RADAR_CFG = "radar_cfg"

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

    # PGE names
    #L0A = "L0A"
    #TIME_EXTRACTOR = "Time_Extractor"
    #L0B = "L0B"

    # Other Constants
    #PASS_ACCOUNTABILITY_INDEX = pass_accountability_index
    #OBSERVATION_ACCOUNTABILITY_INDEX = observation_accountability_index
    #TRACK_FRAME_ACCOUNTABILITY_INDEX = track_frame_accountability_index

    # state config dataset types
    #DATATAKE_STATE_CONFIG_DOC_TYPE = "datatake-state-config"
    #DATATAKE_EXP_STATE_CONFIG_DOC_TYPE = "datatake-expired-state-config"
    #DATATAKE_UR_EXP_STATE_CONFIG_DOC_TYPE = "datatake-urgent_response_expired-state-config"
    #DATATAKE_UR_STATE_CONFIG_DOC_TYPE = "datatake-urgent_response_state-config"

    TASK_INDEX = "task_status-current"
    TASK_ID_FIELD = "task_id"
    JOB_INDEX = "job_status-current"
    JOB_ID_FIELD = "job_id"
    OUTPUT_DATASETS = "output_datasets"
    LAST_MOD = "last_modified"
    PRIMARY_INPUT = "primary_input"
    PRIMARY_OUTPUT = "primary_output"

    EXTRA_PGE_OUTPUT_METADATA = "extra_pge_output_metadata"

    # job-specific parameter extraction/coercion from context
    #GET_GCOV_JOB_PARAMS_FROM_CONTEXT = "get_gcov_job_params_from_context"

    GET_PRODUCTS = "get_products"
    IS_STATE_CONFIG_TRIGGER = "is_state_config_trigger"
    FILE_NAMES_KEY = "file_names_key"

    CAST_STRING_TO_INT = "cast_string_to_int"
    GET_DEM_BBOX = "get_dem_bbox"
    GET_DEMS = "get_dems"

    GET_PGE_SETTINGS_VALUES = "get_pge_settings_values"
