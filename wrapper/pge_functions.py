"""
PGE-specific functions for use with the OPERA PGE Wrapper
"""
import glob
import os
from os.path import basename
from typing import Dict


def slc_s1_lineage_metadata(context, work_dir):
    """Gathers the lineage metadata for the CSLC-S1 and RTC-S1 PGEs"""
    run_config: Dict = context.get("run_config")

    lineage_metadata = []

    for input_filepath in run_config["input_file_group"].values():
        # By now Chimera has localized any files from S3, so we need to modify
        # s3 URI's to point to the local location on disk
        if input_filepath.startswith('s3://'):
            input_filepath = os.path.join(work_dir, basename(input_filepath))

        lineage_metadata.append(input_filepath)

    # Copy the DEM (vrt and tif) downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

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
    run_config["input_file_group"]["safe_file_path"] = f'{container_home}/input_dir/{basename(safe_file_path)}'
    run_config["input_file_group"]["orbit_file_path"] = f'{container_home}/input_dir/{basename(orbit_file_path)}'

    # TODO: update once better naming is implemented for ancillary files
    run_config["dynamic_ancillary_file_group"]["dem_file"] = f'{container_home}/input_dir/dem.vrt'

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

    return run_config
