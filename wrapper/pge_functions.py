"""
PGE-specific functions for use with the OPERA PGE Wrapper
"""
import glob
import os
from os.path import basename, dirname, splitext
from typing import Dict

from commons.logger import logger

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

    local_burstdb_filepaths = glob.glob(os.path.join(work_dir, "*.sqlite3"))
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

        # TODO: kludge to support current version of DSWx-S1 SAS, which wants
        #  a "_layover_shadow_mask.tif" file, remove for next patch release
        mask_files = glob.glob(os.path.join(local_input_filepath, "*_mask.tif"))
        mask_file = mask_files[0]
        old_name = mask_file
        new_name = mask_file.replace("_mask.tif", "_layover_shadow_mask.tif")
        logger.info(f"Renaming {old_name} to {new_name}")
        os.rename(old_name, new_name)

    # Copy the ancillaries downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_hand_filepaths = glob.glob(os.path.join(work_dir, "hand_file*.*"))
    lineage_metadata.extend(local_hand_filepaths)

    local_worldcover_filepaths = glob.glob(os.path.join(work_dir, "worldcover_file*.*"))
    lineage_metadata.extend(local_worldcover_filepaths)

    local_ref_water_filepaths = glob.glob(os.path.join(work_dir, "reference_water_file*.*"))
    lineage_metadata.extend(local_ref_water_filepaths)

    local_db_filepaths = glob.glob(os.path.join(work_dir, "*.sqlite*"))
    lineage_metadata.extend(local_db_filepaths)

    local_algorithm_parameters_filepath = os.path.join(
        work_dir, basename(run_config["processing"]["algorithm_parameters"])
    )
    lineage_metadata.append(local_algorithm_parameters_filepath)

    # TODO: enable if shoreline files are needed by DSWx-S1 SAS
    #shoreline_shape_filename = run_config["dynamic_ancillary_file_group"]["shoreline_shapefile"]
    #shoreline_shape_basename = splitext(basename(shoreline_shape_filename))[0]
    #local_shoreline_filepaths = glob.glob(os.path.join(work_dir, f"{shoreline_shape_basename}.*"))
    #lineage_metadata.extend(local_shoreline_filepaths)

    return lineage_metadata


def disp_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the DISP-S1 PGE"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    # Reassign all S3 URI's in the runconfig to where the files now reside on the local worker
    for s3_input_filepath in run_config["input_file_group"]["input_file_paths"]:
        local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))

        if os.path.isdir(local_input_filepath):
            lineage_metadata.extend(
                [os.path.join(local_input_filepath, file_name)
                 for file_name in os.listdir(local_input_filepath)
                 if file_name.endswith(".h5")]
            )
        else:
            lineage_metadata.append(local_input_filepath)

    for dynamic_ancillary_key in ("amplitude_dispersion_files", "amplitude_mean_files",
                                  "static_layers_files", "ionosphere_files", "troposphere_files"):
        for s3_input_filepath in run_config["dynamic_ancillary_file_group"][dynamic_ancillary_key]:
            local_input_filepath = os.path.join(work_dir, basename(s3_input_filepath))
            lineage_metadata.append(local_input_filepath)

    # Copy the pre-downloaded ancillaries for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_mask_filepaths = glob.glob(os.path.join(work_dir, "*mask*.*"))
    lineage_metadata.extend(local_mask_filepaths)

    local_algorithm_parameters_filepath = os.path.join(
        work_dir, basename(run_config["processing"]["algorithm_parameters"])
    )
    lineage_metadata.append(local_algorithm_parameters_filepath)

    local_frame_database_filepath = os.path.join(
        work_dir, basename(run_config["static_ancillary_file_group"]["frame_to_burst_json"])
    )
    lineage_metadata.append(local_frame_database_filepath)

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

    algorithm_parameters_filename = basename(run_config["processing"]["algorithm_parameters"])
    run_config["processing"]["algorithm_parameters"] = f'{container_home_prefix}/{algorithm_parameters_filename}'

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

    for input_file_path in glob.glob(os.path.join(local_input_dir, "*.h5")):
        updated_input_file_paths.append(os.path.join(container_home_prefix, basename(input_file_path)))

    run_config["input_file_group"]["input_file_paths"] = updated_input_file_paths

    dynamic_ancillary_file_group = run_config["dynamic_ancillary_file_group"]

    for dynamic_ancillary_key in ("amplitude_dispersion_files", "amplitude_mean_files",
                                  "static_layers_files", "ionosphere_files", "troposphere_files"):
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

    if "mask_file" in run_config["dynamic_ancillary_file_group"]:
        run_config["dynamic_ancillary_file_group"]["mask_file"] = (
            os.path.join(container_home_prefix,
                         os.path.basename(run_config["dynamic_ancillary_file_group"]["mask_file"]))
        )

    run_config["processing"]["algorithm_parameters"] = (
        os.path.join(container_home_prefix, os.path.basename(run_config["processing"]["algorithm_parameters"]))
    )

    return run_config
