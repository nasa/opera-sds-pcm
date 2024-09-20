from chimera.commons.constants import ChimeraConstants

class OperaChimeraConstants(ChimeraConstants):
    def __init__(self):
        ChimeraConstants.__init__(self)

    ALGORITHM_PARAMETERS = "algorithm_parameters"

    AMPLITUDE_DISPERSION_FILES = "amplitude_dispersion_files"

    AMPLITUDE_MEAN_FILES = "amplitude_mean_files"

    BBOX = "bbox"

    BURST_DATABASE_FILE = "burst_database_file"

    BURST_ID = "burst_id"

    CNM_VERSION = "CNM_VERSION"

    COMPRESSED_CSLC_PATHS = "compressed_cslc_paths"

    DATASET_TYPE = "dataset_type"

    DEM_FILE = "dem_file"

    DISP_S1_FORWARD = "DISP_S1_FORWARD"

    DISP_S1_HISTORICAL = "DISP_S1_HISTORICAL"

    ESTIMATED_GEOMETRIC_ACCURACY = "ESTIMATED_GEOMETRIC_ACCURACY"

    EXTRA_PGE_OUTPUT_METADATA = "extra_pge_output_metadata"

    FORCE_INGEST = "FORCE_INGEST"

    FRAME_ID = "frame_id"

    GET_CSLC_PRODUCT_SPECIFICATION_VERSION = "get_cslc_product_specification_version"

    GET_DISP_S1_ALGORITHM_PARAMETERS = "get_disp_s1_algorithm_parameters"

    GET_DISP_S1_DEM = "get_disp_s1_dem"

    GET_DISP_S1_MASK_FILE = "get_disp_s1_mask_file"

    GET_DISP_S1_TROPOSPHERE_FILES = "get_disp_s1_troposphere_files"

    GET_DSWX_HLS_DEM = "get_dswx_hls_dem"

    GET_DSWX_S1_ALGORITHM_PARAMETERS = "get_dswx_s1_algorithm_parameters"

    GET_DSWX_S1_DEM = "get_dswx_s1_dem"

    GET_DSWX_S1_DYNAMIC_ANCILLARY_MAPS = "get_dswx_s1_dynamic_ancillary_maps"

    GET_LANDCOVER = "get_landcover"

    GET_METADATA = "get_metadata"

    GET_PRODUCT_METADATA = "get_product_metadata"

    GET_PRODUCT_VERSION = "get_product_version"

    GET_SHORELINE_SHAPEFILES = "get_shoreline_shapefiles"

    GET_SLC_S1_BURST_DATABASE = "get_slc_s1_burst_database"

    GET_SLC_S1_DEM = "get_slc_s1_dem"

    GET_SLC_S1_ORBIT_FILE = "get_slc_s1_orbit_file"

    GET_SLC_S1_POLARIZATION = "get_slc_s1_polarization"

    GET_SLC_S1_SAFE_FILE = "get_slc_s1_safe_file"

    GET_STATIC_ANCILLARY_FILES = "get_static_ancillary_files"

    GET_STATIC_PRODUCT_VERSION = "get_static_product_version"

    GET_WORLDCOVER = "get_worldcover"

    GPU_ENABLED = "gpu_enabled"

    INGEST_STAGED = "INGEST_STAGED"

    INPUT_DATASET_ID = "input_dataset_id"

    INPUT_FILE_PATHS = "input_file_paths"

    INPUT_MGRS_COLLECTION_ID = "input_mgrs_collection_id"

    INSTANTIATE_ALGORITHM_PARAMETERS_TEMPLATE = "instantiate_algorithm_parameters_template"

    INUNDATED_VEGETATION_ENABLED = "inundated_vegetation_enabled"

    IONOSPHERE_FILES = "ionosphere_files"

    MASK_FILE = "mask_file"

    L2_CSLC_S1 = "L2_CSLC_S1"

    L2_CSLC_S1_COMPRESSED = "L2_CSLC_S1_COMPRESSED"

    L2_RTC_S1 = "L2_RTC_S1"

    L3_DISP_S1 = "L3_DISP_S1"

    L3_DSWX_HLS = "L3_DSWX_HLS"

    L3_DSWx_S1 = "L3_DSWx_S1"

    LANDCOVER_FILE = "landcover_file"

    ORBIT_FILE_PATH = "orbit_file_path"

    POLARIZATION = "polarization"

    PROCESSING_MODE_FORWARD = "forward"

    PROCESSING_MODE_HISTORICAL = "historical"

    PROCESSING_MODE_KEY = "processing_mode"

    PROCESSING_MODE_REPROCESSING = "reprocessing"

    PRODUCT_SPEC_VER = "PRODUCT_SPEC_VER"

    PRODUCT_TYPE = "product_type"

    PRODUCT_VERSION = "product_version"

    S3_BUCKET = "s3_bucket"

    S3_KEY = "s3_key"

    S3_KEYS = "s3_keys"

    SAFE_FILE_PATH = "safe_file_path"

    SET_DAAC_PRODUCT_TYPE = "set_daac_product_type"

    SET_EXTRA_PGE_OUTPUT_METADATA = "set_extra_pge_output_metadata"

    SET_SAMPLE_PRODUCT_METADATA = "set_sample_product_metadata"

    SHORELINE_SHAPEFILE = "shoreline_shapefile"

    STATIC_PRODUCT_VERSION = "static_product_version"

    STATIC_LAYERS_FILES = "static_layers_files"

    TEC_FILE = "tec_file"

    TEMPLATE_MAPPING = "template_mapping"

    TROPOSPHERE_FILES = "troposphere_files"

    VERSION_KEY = "version_key"

    WORLDCOVER_FILE = "worldcover_file"

    WORLDCOVER_VER = "worldcover_version"

    WORLDCOVER_YEAR = "worldcover_year"
