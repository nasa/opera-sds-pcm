from chimera.commons.constants import ChimeraConstants

class OperaChimeraConstants(ChimeraConstants):
    def __init__(self):
        ChimeraConstants.__init__(self)

    FORCE_INGEST = "FORCE_INGEST"

    CNM_VERSION = "CNM_VERSION"

    PROCESSING_MODE_KEY = "processing_mode"

    PROCESSING_MODE_FORWARD = "forward"

    PROCESSING_MODE_HISTORICAL = "historical"

    PROCESSING_MODE_REPROCESSING = "reprocessing"

    GET_PRODUCT_METADATA = "get_product_metadata"

    GET_METADATA = "get_metadata"

    SET_EXTRA_PGE_OUTPUT_METADATA = "set_extra_pge_output_metadata"

    SET_DAAC_PRODUCT_TYPE = "set_daac_product_type"

    INPUT_DATASET_ID = "input_dataset_id"

    DATASET_TYPE = "dataset_type"

    PRODUCT_VERSION = "product_version"

    POLARIZATION = "polarization"

    SAFE_FILE_PATH = "safe_file_path"

    ORBIT_FILE_PATH = "orbit_file_path"

    BURST_ID = "burst_id"

    BURST_DATABASE_FILE = "burst_database_file"

    DEM_FILE = "dem_file"

    TEC_FILE = "tec_file"

    LANDCOVER_FILE = "landcover_file"

    WORLDCOVER_FILE = "worldcover_file"

    WORLDCOVER_VER = "worldcover_version"

    WORLDCOVER_YEAR = "worldcover_year"

    SHORELINE_SHAPEFILE = "shoreline_shapefile"

    GPU_ENABLED = "gpu_enabled"

    S3_BUCKET = "s3_bucket"

    S3_KEY = "s3_key"

    S3_KEYS = "s3_keys"

    L3_DSWX_HLS = "L3_DSWX_HLS"

    L2_RTC_S1 = "L2_RTC_S1"

    L2_CSLC_S1 = "L2_CSLC_S1"

    L3_DSWx_S1 = "L3_DSWx_S1"

    L3_DISP_S1 = "L3_DISP_S1"

    EXTRA_PGE_OUTPUT_METADATA = "extra_pge_output_metadata"

    VERSION_KEY = "version_key"

    BBOX = "bbox"

    GET_SLC_S1_POLARIZATION = "get_slc_s1_polarization"

    GET_SLC_S1_BURST_DATABASE = "get_slc_s1_burst_database"

    GET_SLC_S1_SAFE_FILE = "get_slc_s1_safe_file"

    GET_SLC_S1_ORBIT_FILE = "get_slc_s1_orbit_file"

    GET_SLC_S1_DEM = "get_slc_s1_dem"

    GET_PRODUCT_VERSION = "get_product_version"

    GET_DSWX_HLS_DEM = "get_dswx_hls_dem"

    GET_LANDCOVER = "get_landcover"

    GET_WORLDCOVER = "get_worldcover"

    GET_SHORELINE_SHAPEFILES = "get_shoreline_shapefiles"
