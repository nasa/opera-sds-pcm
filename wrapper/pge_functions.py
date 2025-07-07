"""
PGE-specific functions for use with the OPERA PGE Wrapper
"""
import glob
import os
from itertools import chain
from os.path import basename, splitext
from typing import Dict

def slc_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the CSLC-S1 and RTC-S1 PGEs"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    for input_filepath in run_config["input_file_group"].values():
        # By now Chimera has localized any files from S3, so we need to modify
        # s3 URI's to point to the local location on disk
        if isinstance(input_filepath, list):
            input_filepaths = [os.path.join(work_dir, basename(input_file))
                               for input_file in input_filepath
                               if input_file.startswith('s3://')]
            lineage_metadata.extend(input_filepaths)
        else:
            if input_filepath.startswith('s3://'):
                input_filepath = os.path.join(work_dir, basename(input_filepath))

            lineage_metadata.append(input_filepath)

    # Copy the ancillaries downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    # Legacy Ionosphere files
    local_tec_filepaths = glob.glob(os.path.join(work_dir, "jp*.*i"))
    lineage_metadata.extend(local_tec_filepaths)

    # New Ionosphere files
    local_tec_filepaths = glob.glob(os.path.join(work_dir, "JPL*.INX"))
    lineage_metadata.extend(local_tec_filepaths)

    local_burstdb_filepaths = glob.glob(os.path.join(work_dir, "*.sqlite*"))
    lineage_metadata.extend(local_burstdb_filepaths)

    return lineage_metadata


def dswx_hls_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DSWx-HLS PGE"""
    run_config: Dict = context.get("run_config")

    # We need to convert the S3 urls specified in the run config to local paths and also
    # capture the inputs, so we can store the lineage in the output dataset metadata
    lineage_metadata = []
    for s3_input_filepath in run_config["product_paths"]["L2_HLS"]:
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
        lineage_metadata.append(local_input_filepath)

    # Copy the ancillaries downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_landcover_filepath = os.path.join(work_dir, "landcover.tif")
    lineage_metadata.append(local_landcover_filepath)

    local_worldcover_filepaths = glob.glob(os.path.join(work_dir, "worldcover*.*"))
    lineage_metadata.extend(local_worldcover_filepaths)

    shoreline_shape_filename = run_config["dynamic_ancillary_file_group"]["shoreline_shapefile"]
    shoreline_shape_basename = splitext(basename(shoreline_shape_filename))[0]
    local_shoreline_filepaths = glob.glob(os.path.join(work_dir, f"{shoreline_shape_basename}.*"))
    lineage_metadata.extend(local_shoreline_filepaths)

    return lineage_metadata


def dswx_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DSWx-S1 PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    for s3_input_filepath in run_config["input_file_group"]["input_file_paths"]:
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
        lineage_metadata.append(local_input_filepath)

    # Copy the ancillaries downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_hand_filepaths = glob.glob(os.path.join(work_dir, "hand_file*.*"))
    lineage_metadata.extend(local_hand_filepaths)

    local_worldcover_filepaths = glob.glob(os.path.join(work_dir, "worldcover_file*.*"))
    lineage_metadata.extend(local_worldcover_filepaths)

    local_ref_water_filepaths = glob.glob(os.path.join(work_dir, "reference_water_file*.*"))
    lineage_metadata.extend(local_ref_water_filepaths)

    local_glad_classification_filepaths = glob.glob(os.path.join(work_dir, "glad_classification_file*.*"))
    lineage_metadata.extend(local_glad_classification_filepaths)

    local_db_filepaths = glob.glob(os.path.join(work_dir, "*.sqlite*"))
    lineage_metadata.extend(local_db_filepaths)

    return lineage_metadata


def dswx_ni_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DSWx-NI PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    # TODO: update paths as necessary as sample inputs are phased out
    gcov_data_dir = os.path.join(work_dir, 'dswx_ni_beta_0.2.1_expected_input', 'input_dir', 'GCOV')

    lineage_metadata.extend(
        [os.path.join(gcov_data_dir, gcov_file) for gcov_file in os.listdir(gcov_data_dir)]
    )

    ancillary_data_dir = os.path.join(work_dir, 'dswx_ni_beta_0.2.1_expected_input', 'input_dir', 'ancillary_data')

    lineage_metadata.extend(
        [os.path.join(ancillary_data_dir, ancillary) for ancillary in os.listdir(ancillary_data_dir)]
    )

    return lineage_metadata


def disp_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DISP-S1 PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    input_file_group = run_config["input_file_group"]
    dynamic_ancillary_file_group = run_config["dynamic_ancillary_file_group"]

    s3_input_filepaths = input_file_group["input_file_paths"] + input_file_group["compressed_cslc_paths"]
    s3_input_filepaths.append(dynamic_ancillary_file_group["algorithm_parameters_file"])

    # Reassign all S3 URI's in the runconfig to where the files now reside on the local worker
    for s3_input_filepath in s3_input_filepaths:
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))

        if os.path.isdir(local_input_filepath):
            lineage_metadata.extend(
                [os.path.join(local_input_filepath, file_name)
                 for file_name in os.listdir(local_input_filepath)
                 if file_name.endswith(".h5")]
            )
        else:
            lineage_metadata.append(local_input_filepath)

    for dynamic_ancillary_key in ("static_layers_files", "ionosphere_files"):
        if dynamic_ancillary_key in run_config["dynamic_ancillary_file_group"]:
            for s3_input_filepath in run_config["dynamic_ancillary_file_group"][dynamic_ancillary_key]:
                local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
                lineage_metadata.append(local_input_filepath)

    # Copy the pre-downloaded ancillaries for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_mask_filepaths = glob.glob(os.path.join(work_dir, "*mask*.*"))
    lineage_metadata.extend(local_mask_filepaths)

    # Algorithm parameters overrides has already been downloaded to local disk
    local_algorithm_parameters_overrides_filepath = run_config["static_ancillary_file_group"]["algorithm_parameters_overrides_json"]
    lineage_metadata.append(local_algorithm_parameters_overrides_filepath)

    local_frame_database_filepath = os.path.join(
        work_dir, basename(run_config["static_ancillary_file_group"]["frame_to_burst_json"])
    )
    lineage_metadata.append(local_frame_database_filepath)

    local_reference_date_database = os.path.join(
        work_dir, basename(run_config["static_ancillary_file_group"]["reference_date_database_json"])
    )
    lineage_metadata.append(local_reference_date_database)

    return lineage_metadata


def disp_s1_static_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DISP-S1-STATIC PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    input_file_group = run_config["input_file_group"]
    s3_input_filepaths = input_file_group["input_file_paths"]

    lineage_metadata.extend(s3_input_filepaths)
    for dynamic_ancillary_key in ("rtc_static_layers_files",):
        if dynamic_ancillary_key in run_config["dynamic_ancillary_file_group"]:
            lineage_metadata.extend(run_config["dynamic_ancillary_file_group"][dynamic_ancillary_key])

    # Copy the pre-downloaded ancillaries for this job to the pge input directory
    lineage_metadata.append(run_config["dynamic_ancillary_file_group"]["dem_file"])
    lineage_metadata.append(run_config["static_ancillary_file_group"]["frame_to_burst_json"])

    # Reassign all S3 URI's in the runconfig to where the files now reside on the local worker
    lineage_metadata = [os.path.join(work_dir, basename(uri)) for uri in lineage_metadata]

    return lineage_metadata


def dist_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DIST-S1 PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    input_file_group = run_config["input_file_group"]

    for s3_input_filepath in chain(input_file_group['pre_rtc_copol'], input_file_group['pre_rtc_crosspol'],
                                   input_file_group['post_rtc_copol'], input_file_group['post_rtc_crosspol']):
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
        lineage_metadata.append(local_input_filepath)

    if 'prev_product' in input_file_group and input_file_group['prev_product']:
        lineage_metadata.extend(input_file_group['prev_product'])

    if 'water_mask_path' in run_config and run_config["water_mask_path"]:
        local_input_filepath = os.path.join(work_dir, basename(run_config["water_mask_path"]))
        lineage_metadata.append(local_input_filepath)

    return lineage_metadata

def tropo_lineage_metadata(context, work_dir):
    """
    Generates the lineage metadata for the TROPO PGE"""
    run_config = context.get("run_config")
    lineage_metadata = []
    input_file_group = run_config["input_file_group"]
    s3_input_filepaths = input_file_group["input_file_paths"]

    # Reassign all S3 URI's in the runconfig to where the files now reside on the local worker
    for s3_input_filepath in s3_input_filepaths:
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
        lineage_metadata.append(local_input_filepath)

    return lineage_metadata

def update_slc_s1_runconfig(context, work_dir):
    """Updates a runconfig for use with the CSLC-S1 and RTC-S1 PGEs"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']

    safe_file_path = run_config["input_file_group"]["safe_file_path"]
    orbit_file_path = run_config["input_file_group"]["orbit_file_path"]

    if isinstance(safe_file_path, list):
        run_config["input_file_group"]["safe_file_path"] = [
            f'{container_home}/input_dir/{basename(safe_file)}'
            for safe_file in safe_file_path
        ]
    else:
        run_config["input_file_group"]["safe_file_path"] = f'{container_home}/input_dir/{basename(safe_file_path)}'

    if isinstance(orbit_file_path, list):
        run_config["input_file_group"]["orbit_file_path"] = [
            f'{container_home}/input_dir/{basename(orbit_file)}'
            for orbit_file in orbit_file_path
        ]
    else:
        run_config["input_file_group"]["orbit_file_path"] = f'{container_home}/input_dir/{basename(orbit_file_path)}'

    # TODO: update once better naming is implemented for ancillary files
    run_config["dynamic_ancillary_file_group"]["dem_file"] = f'{container_home}/input_dir/dem.vrt'

    if "tec_file" in run_config["dynamic_ancillary_file_group"]:
        tec_file_path = run_config["dynamic_ancillary_file_group"]["tec_file"]
        run_config["dynamic_ancillary_file_group"]["tec_file"] = f'{container_home}/input_dir/{basename(tec_file_path)}'

    burst_db_file_path = run_config["static_ancillary_file_group"]["burst_database_file"]
    run_config["static_ancillary_file_group"]["burst_database_file"] = f'{container_home}/input_dir/{basename(burst_db_file_path)}'

    return run_config


def update_dswx_hls_runconfig(context, work_dir):
    """Updates a runconfig for use with the DSWx-HLS PGE"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']

    # Point the PGE to the input directory and ancillary files,
    # they should already have been made locally available by PCM
    run_config["input_file_group"]["input_file_path"] = [f'{container_home}/input_dir']

    # TODO: update once better naming is implemented for ancillary files
    run_config["dynamic_ancillary_file_group"]["dem_file"] = f'{container_home}/input_dir/dem.vrt'
    run_config["dynamic_ancillary_file_group"]["landcover_file"] = f'{container_home}/input_dir/landcover.tif'
    run_config["dynamic_ancillary_file_group"]["worldcover_file"] = f'{container_home}/input_dir/worldcover.vrt'

    shoreline_shape_filename = basename(run_config["dynamic_ancillary_file_group"]["shoreline_shapefile"])
    run_config["dynamic_ancillary_file_group"]["shoreline_shapefile"] = f'{container_home}/input_dir/{shoreline_shape_filename}'

    return run_config


def update_dswx_s1_runconfig(context, work_dir):
    """Updates a runconfig for use with the DSWx-S1 PGE"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'

    input_file_paths = run_config["input_file_group"]["input_file_paths"]
    updated_input_file_paths = [os.path.join(container_home_prefix, basename(input_file_path))
                                for input_file_path in input_file_paths]
    run_config["input_file_group"]["input_file_paths"] = updated_input_file_paths

    dynamic_ancillary_file_paths = run_config["dynamic_ancillary_file_group"]
    updated_dynamic_ancillary_file_paths = {
        ancillary_file_type: os.path.join(container_home_prefix, basename(ancillary_file_path))
        for ancillary_file_type, ancillary_file_path in dynamic_ancillary_file_paths.items()
    }
    run_config["dynamic_ancillary_file_group"] = updated_dynamic_ancillary_file_paths

    static_ancillary_file_paths = run_config["static_ancillary_file_group"]
    updated_static_ancillary_file_paths = {
        ancillary_file_type: os.path.join(container_home_prefix, basename(ancillary_file_path))
        for ancillary_file_type, ancillary_file_path in static_ancillary_file_paths.items()
    }
    run_config["static_ancillary_file_group"] = updated_static_ancillary_file_paths

    return run_config


def update_dswx_ni_runconfig(context, work_dir):
    """Updates a runconfig for use with the DSWx-S1 PGE"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'
    gcov_data_prefix = os.path.join(work_dir, 'dswx_ni_beta_0.2.1_expected_input', 'input_dir', 'GCOV')

    input_file_paths = run_config["input_file_group"]["input_file_paths"]
    input_file_paths = list(map(lambda x: x.replace(gcov_data_prefix, container_home_prefix), input_file_paths))

    run_config["input_file_group"]["input_file_paths"] = input_file_paths

    # TODO update these once we move away from sample inputs
    run_config["dynamic_ancillary_file_group"]["dem_file"] = f'{container_home_prefix}/dem.vrt'
    run_config["dynamic_ancillary_file_group"]["hand_file"] = f'{container_home_prefix}/hand.vrt'
    run_config["dynamic_ancillary_file_group"]["worldcover_file"] = f'{container_home_prefix}/worldcover.vrt'
    run_config["dynamic_ancillary_file_group"]["reference_water_file"] = f'{container_home_prefix}/reference_water.vrt'
    run_config["dynamic_ancillary_file_group"]["glad_classification_file"] = f'{container_home_prefix}/glad.vrt'

    run_config["static_ancillary_file_group"]["mgrs_database_file"] = f'{container_home_prefix}/MGRS_tile.sqlite'
    run_config["static_ancillary_file_group"]["mgrs_collection_database_file"] = f'{container_home_prefix}/MGRS_collection_db_DSWx-NI_v0.1.sqlite'

    run_config["processing"]["algorithm_parameters"] = f'{container_home_prefix}/algorithm_parameter_ni.yaml'

    return run_config


def update_disp_s1_runconfig(context, work_dir):
    """Updates a runconfig for use with the DISP-S1 PGE"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'

    # TODO: kludge, assumes input dir will always be named pge_input_dir
    local_input_dir = os.path.join(work_dir, "pge_input_dir")

    updated_input_file_paths = []

    for input_file_path in glob.glob(os.path.join(local_input_dir, "*CSLC-S1_*.h5")):
        updated_input_file_paths.append(os.path.join(container_home_prefix, basename(input_file_path)))

    run_config["input_file_group"]["input_file_paths"] = updated_input_file_paths

    dynamic_ancillary_file_group = run_config["dynamic_ancillary_file_group"]

    for dynamic_ancillary_key in ("static_layers_files", "ionosphere_files", "troposphere_files"):
        if dynamic_ancillary_key in dynamic_ancillary_file_group:
            dynamic_ancillary_file_group[dynamic_ancillary_key] = [
                os.path.join(container_home_prefix, basename(input_file_path))
                for input_file_path in dynamic_ancillary_file_group[dynamic_ancillary_key]
            ]

    dynamic_ancillary_file_group["algorithm_parameters_file"] = os.path.join(
        container_home_prefix, basename(dynamic_ancillary_file_group["algorithm_parameters_file"])
    )

    static_ancillary_file_group = run_config["static_ancillary_file_group"]

    for static_ancillary_key in ("algorithm_parameters_overrides_json", "frame_to_burst_json",
                                 "reference_date_database_json"):
        static_ancillary_file_group[static_ancillary_key] = os.path.join(
            container_home_prefix, basename(static_ancillary_file_group[static_ancillary_key])
        )

    if "dem_file" in run_config["dynamic_ancillary_file_group"]:
        run_config["dynamic_ancillary_file_group"]["dem_file"] = (
            os.path.join(container_home_prefix,
                         os.path.basename(run_config["dynamic_ancillary_file_group"]["dem_file"]))
        )

    if "mask_file" in run_config["dynamic_ancillary_file_group"]:
        run_config["dynamic_ancillary_file_group"]["mask_file"] = (
            os.path.join(container_home_prefix,
                         os.path.basename(run_config["dynamic_ancillary_file_group"]["mask_file"]))
        )

    return run_config


def update_disp_s1_static_runconfig(context, work_dir):
    """Updates a runconfig for use with the DISP-S1-STATIC PGE"""

    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'

    run_config["input_file_group"]["input_file_paths"] = list(map(
        lambda x: os.path.join(container_home_prefix, basename(x)),
        run_config["input_file_group"]["input_file_paths"]
    ))

    dynamic_ancillary_file_group = run_config["dynamic_ancillary_file_group"]

    for dynamic_ancillary_key in ("rtc_static_layers_files",):
        if dynamic_ancillary_key in dynamic_ancillary_file_group:
            dynamic_ancillary_file_group[dynamic_ancillary_key] = [
                os.path.join(container_home_prefix, basename(input_file_path))
                for input_file_path in dynamic_ancillary_file_group[dynamic_ancillary_key]
            ]

    static_ancillary_file_group = run_config["static_ancillary_file_group"]

    for static_ancillary_key in ("frame_to_burst_json",):
        static_ancillary_file_group[static_ancillary_key] = os.path.join(
            container_home_prefix, basename(static_ancillary_file_group[static_ancillary_key])
        )

    if "dem_file" in run_config["dynamic_ancillary_file_group"]:
        run_config["dynamic_ancillary_file_group"]["dem_file"] = (
            os.path.join(container_home_prefix,
                         os.path.basename(run_config["dynamic_ancillary_file_group"]["dem_file"]))
        )

    return run_config


def update_dist_s1_runconfig(context, work_dir):
    """Updates a runconfig for use with the DIST-S1 PGE"""

    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'

    local_input_dir = os.path.join(work_dir, "pge_input_dir")

    run_config['input_file_group']['pre_rtc_copol'] = list(map(
        lambda x: os.path.join(container_home_prefix, basename(x)),
        run_config['input_file_group']['pre_rtc_copol']
    ))

    run_config['input_file_group']['pre_rtc_crosspol'] = list(map(
        lambda x: os.path.join(container_home_prefix, basename(x)),
        run_config['input_file_group']['pre_rtc_crosspol']
    ))

    run_config['input_file_group']['post_rtc_copol'] = list(map(
        lambda x: os.path.join(container_home_prefix, basename(x)),
        run_config['input_file_group']['post_rtc_copol']
    ))

    run_config['input_file_group']['post_rtc_crosspol'] = list(map(
        lambda x: os.path.join(container_home_prefix, basename(x)),
        run_config['input_file_group']['post_rtc_crosspol']
    ))

    if 'prev_product' in run_config['input_file_group'] and run_config['input_file_group']['prev_product']:
        run_config['input_file_group']['prev_product'] = list(map(
            lambda x: os.path.join(container_home_prefix, basename(x)),
            run_config['input_file_group']['prev_product']
        ))

    if 'water_mask_path' in run_config and run_config["water_mask_path"]:
        run_config["water_mask_path"] = os.path.join(container_home_prefix, basename(run_config["water_mask_path"]))

    return run_config

def update_tropo_runconfig(context, work_dir):
    """Updates a runconfig for use with the TROPO PGE"""
    run_config: Dict = context.get("run_config")
    job_spec: Dict = context.get("job_specification")

    container_home_param = list(
        filter(lambda param: param['name'] == 'container_home', job_spec['params'])
    )[0]
    pge_input_dir_param = list(
        filter(lambda param: param['name'] == 'pge_input_dir', job_spec['params'])
    )[0]

    container_home: str = container_home_param['value']
    container_home_prefix = f'{container_home}/input_dir'

    local_input_dir = os.path.join(work_dir, pge_input_dir_param['value'])

    updated_input_file_paths = []

    for input_file_path in glob.glob(os.path.join(local_input_dir, "*.nc")):
        updated_input_file_paths.append(os.path.join(container_home_prefix, basename(input_file_path)))

    run_config["input_file_group"]["input_file_paths"] = updated_input_file_paths

    return run_config