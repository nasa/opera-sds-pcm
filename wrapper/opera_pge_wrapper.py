"""
OPERA PCM-PGE Wrapper. Used for doing the actual PGE runs
"""
import argparse
import glob
import json
import os
import shutil
from functools import partial
from typing import Dict, Tuple, List, Union

from commons.logger import logger
from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as opera_chimera_const
from product2dataset import product2dataset
from util import pge_util
from util.conf_util import RunConfig
from util.ctx_util import JobContext, DockerParams
from util.exec_util import exec_wrapper, call_noerr

to_json = partial(json.dumps, indent=2)


@exec_wrapper
def main(context_file: str, workdir: str):
    jc = JobContext(context_file)
    context = jc.ctx
    logger.debug(f"context={to_json(context)}")

    # set additional files to triage
    jc.set('_triage_additional_globs', ["output", "RunConfig.yaml", "pge_output_dir"])
    jc.save()

    run_pipeline(context=context, work_dir=workdir)


def run_pipeline(context: Dict, work_dir: str) -> List[Union[bytes, str]]:
    """
    Run the PGE in OPERA land
    :param context: Path to HySDS _context.json
    :param work_dir: PGE working directory

    :return:
    """

    logger.info(f"Preparing Working Directory: {work_dir}")

    input_hls_dir, output_dir, runconfig_dir = create_required_directories(work_dir, context)

    run_config: Dict = context.get("run_config")

    # We need to convert the S3 urls specified in the run config to local paths and also
    # capture the inputs, so we can store the lineage in the output dataset metadata
    lineage_metadata = []
    for s3_input_filepath in run_config["product_paths"]["L2_HLS"]:
        local_input_filepath = os.path.join(work_dir, os.path.basename(s3_input_filepath))
        lineage_metadata.append(local_input_filepath)

    # Copy the ancillaries downloaded for this job to the pge input directory
    local_dem_filepaths = glob.glob(os.path.join(work_dir, "dem*.*"))
    lineage_metadata.extend(local_dem_filepaths)

    local_landcover_filepath = os.path.join(work_dir, "landcover.tif")
    lineage_metadata.append(local_landcover_filepath)

    local_worldcover_filepaths = glob.glob(os.path.join(work_dir, "worldcover*.*"))
    lineage_metadata.extend(local_worldcover_filepaths)

    logger.info("Copying input files to input directories.")
    for local_input_filepath in lineage_metadata:
        shutil.copy(local_input_filepath, input_hls_dir)

    logger.info("Updating run config for use with PGE.")
    run_config["input_file_group"]["input_file_path"] = ['/home/conda/input_dir']
    run_config["dynamic_ancillary_file_group"]["dem_file"] = '/home/conda/input_dir/dem.vrt'
    run_config["dynamic_ancillary_file_group"]["landcover_file"] = '/home/conda/input_dir/landcover.tif'
    run_config["dynamic_ancillary_file_group"]["worldcover_file"] = '/home/conda/input_dir/worldcover.vrt'

    # create RunConfig.yaml
    logger.debug(f"Run config to transform to YAML is: {to_json(run_config)}")
    pge_config: Dict = context.get("pge_config")
    pge_name = pge_config.get(opera_chimera_const.PGE_NAME)
    rc = RunConfig(run_config, pge_name)
    rc_file = os.path.join(work_dir, 'RunConfig.yaml')
    rc.dump(rc_file)

    logger.info("Copying run config to run config input directory.")
    shutil.copy(rc_file, runconfig_dir)

    logger.debug(f"Run Config: {to_json(run_config)}")
    logger.debug(f"PGE Config: {to_json(pge_config)}")

    # Run the PGE
    should_simulate_pge = context.get(opera_chimera_const.SIMULATE_OUTPUTS)

    if should_simulate_pge:
        logger.info("Simulating PGE run....")
        pge_util.simulate_run_pge(run_config, pge_config, context, output_dir)
    else:
        logger.info("Running PGE...")
        exec_pge_command(
            context=context,
            work_dir=work_dir,
            input_hls_dir=input_hls_dir,
            runconfig_dir=runconfig_dir,
            output_dir=output_dir
        )
    logger.debug(f"{os.listdir(output_dir)=}")

    extra_met = {
        "lineage": lineage_metadata,
        "runconfig": run_config
    }

    logger.info("Converting output product to HySDS-style datasets")
    created_datasets = product2dataset.convert(output_dir, pge_name, rc_file, extra_met=extra_met)

    return created_datasets


def create_required_directories(work_dir: str, context: Dict) -> Tuple[str, str, str]:
    """Creates the requisite directories per PGE-PCM ICS for L3_DSWx_HLS."""
    logger.info("Creating directories for PGE.")

    runconfig_dir = os.path.join(work_dir, job_param_by_name(context, "pge_runconfig_dir"))
    os.makedirs(runconfig_dir, 0o755, exist_ok=True)

    input_hls_dir = os.path.join(work_dir, job_param_by_name(context, "pge_input_dir"))
    os.makedirs(input_hls_dir, 0o755, exist_ok=True)

    output_dir = os.path.join(work_dir, job_param_by_name(context, "pge_output_dir"))
    os.makedirs(output_dir, 0o755, exist_ok=True)

    return input_hls_dir, output_dir, runconfig_dir


def job_param_by_name(context: Dict, name: str):
    """
    Gets the job specification parameter from the _context.json file.
    :param context: the dict representation of _context.json.
    :param name: the name of the job specification parameter.
    """

    for param in context["job_specification"]["params"]:
        if param["name"] == name:
            return param["value"]
    raise Exception(f"param ({name}) not found in _context.json")


def exec_pge_command(context: Dict, work_dir: str, input_hls_dir: str, runconfig_dir: str, output_dir: str):
    logger.info("Preparing PGE docker command.")

    # get dependency image
    dep_img = context.get('job_specification')['dependency_images'][0]
    dep_img_name = dep_img['container_image_name']
    logger.info(f"{dep_img_name=}")

    # get docker params
    docker_params_file = os.path.join(work_dir, "_docker_params.json")
    dp = DockerParams(docker_params_file)
    docker_params = dp.params
    logger.info(f"docker_params={to_json(docker_params)}")
    docker_img_params = docker_params[dep_img_name]
    uid = docker_img_params["uid"]
    gid = docker_img_params["gid"]

    # parse runtime options
    runtime_options = [f"--{k} {v}" for k, v in docker_img_params.get('runtime_options', {}).items()]

    # create directory to house PGE's _docker_stats.json
    pge_stats_dir = os.path.join(work_dir, 'pge_stats')
    logger.debug(f"Making PGE Stats Directory: {pge_stats_dir}")
    os.makedirs(pge_stats_dir, 0o755)

    cmd = [
        f"docker run --init --rm -u {uid}:{gid}",
        " ".join(runtime_options),
        f"-v {runconfig_dir}:/home/conda/runconfig:ro",
        f"-v {input_hls_dir}:/home/conda/input_dir:ro",
        f"-v {output_dir}:/home/conda/output_dir",
        dep_img_name,
        f"--file /home/conda/runconfig/RunConfig.yaml",
    ]

    cmd_line = " ".join(cmd)

    logger.info(f"Calling PGE: {cmd_line}")
    call_noerr(cmd_line, work_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="The context file in the workspace. Typically \"_context.json\".")
    parser.add_argument("workdir", help="The absolute pathname of the current working directory.")
    args = parser.parse_args()

    main(args.context_file, args.workdir)
