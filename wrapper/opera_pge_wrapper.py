"""
OPERA PCM-PGE Wrapper. Used for doing the actual PGE runs
"""
import argparse
import json
import os
import shutil
from functools import partial
from pathlib import Path
from typing import Dict, Tuple, List, Union

from .pge_functions import (slc_s1_lineage_metadata,
                            dswx_hls_lineage_metadata,
                            update_slc_s1_runconfig,
                            update_dswx_hls_runconfig)
from commons.logger import logger
from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as opera_chimera_const
from product2dataset import product2dataset
from util import pge_util
from util.conf_util import RunConfig
from util.ctx_util import JobContext, DockerParams
from util.exec_util import exec_wrapper, call_noerr

to_json = partial(json.dumps, indent=2)

lineage_metadata_functions = {
    'L2_CSLC_S1': slc_s1_lineage_metadata,
    'L2_RTC_S1': slc_s1_lineage_metadata,
    'L3_DSWx_HLS': dswx_hls_lineage_metadata
}
"""Maps PGE Name to a specific function used to gather lineage metadata for that PGE"""

runconfig_update_functions = {
    'L2_CSLC_S1': update_slc_s1_runconfig,
    'L2_RTC_S1': update_slc_s1_runconfig,
    'L3_DSWx_HLS': update_dswx_hls_runconfig
}
"""Maps PGE Name to a specific function used to perform last-minute updates to the RunConfig for that PGE"""


@exec_wrapper
def main(job_json_file: str, workdir: str):
    jc = JobContext(job_json_file)
    job_context = jc.ctx

    # set additional files to triage
    jc.set('_triage_additional_globs', ["output", "RunConfig.yaml", "pge_output_dir"])
    jc.save()

    run_pipeline(job_json_dict=job_context, work_dir=workdir)


def run_pipeline(job_json_dict: Dict, work_dir: str) -> List[Union[bytes, str]]:
    """
    Run the PGE in OPERA land
    :param job_json_dict: HySDS _job.json
    :param work_dir: PGE working directory

    :return:
    """
    logger.info(f"Starting OPERA PGE wrapper with job_context={to_json(job_json_dict)}")

    logger.info(f"Preparing Working Directory: {work_dir}")
    logger.debug(f"{list(Path(work_dir).iterdir())=}")

    input_dir, output_dir, runconfig_dir = create_required_directories(work_dir, job_json_dict)

    run_config: Dict = job_json_dict.get("run_config")
    pge_config: Dict = job_json_dict.get("pge_config")
    pge_name = pge_config.get(opera_chimera_const.PGE_NAME)

    try:
        lineage_metadata = lineage_metadata_functions[pge_name](job_json_dict, work_dir)
    except KeyError as err:
        raise RuntimeError(f'No lineage metadata function available for PGE {str(err)}')

    logger.info("Moving input files to input directories.")
    for local_input_filepath in lineage_metadata:
        shutil.move(local_input_filepath, input_dir)

    if pge_name in runconfig_update_functions:
        logger.info("Updating run config for use with PGE.")
        run_config = runconfig_update_functions[pge_name](job_json_dict, work_dir)

    # create RunConfig.yaml
    logger.info(f"Run config to transform to YAML is: {to_json(run_config)}")
    logger.info(f"PGE Config: {to_json(pge_config)}")

    rc = RunConfig(run_config, pge_name)
    rc_file = os.path.join(work_dir, 'RunConfig.yaml')
    rc.dump(rc_file)

    logger.info("Copying run config to run config input directory.")
    shutil.copy(rc_file, runconfig_dir)

    # Run the PGE
    should_simulate_pge = job_json_dict.get(opera_chimera_const.SIMULATE_OUTPUTS)

    if should_simulate_pge:
        logger.info("Simulating PGE run....")
        pge_util.simulate_run_pge(run_config, pge_config, job_json_dict, output_dir)
    else:
        logger.info("Running PGE...")
        exec_pge_command(
            context=job_json_dict,
            work_dir=work_dir,
            input_dir=input_dir,
            runconfig_dir=runconfig_dir,
            output_dir=output_dir
        )
    logger.debug(f"{os.listdir(output_dir)=}")

    extra_met = {
        "lineage": lineage_metadata,
        "runconfig": run_config
    }

    product_metadata: Dict = pge_util.get_product_metadata(job_json_dict)

    logger.info("Converting output product to HySDS-style datasets")
    created_datasets = product2dataset.convert(
        work_dir, output_dir, pge_name, rc_file, extra_met=extra_met,
        product_metadata=product_metadata
    )

    return created_datasets


def create_required_directories(work_dir: str, context: Dict) -> Tuple[str, str, str]:
    """Creates the requisite directories per the PGE-PCM ICS"""
    logger.info("Creating directories for PGE.")

    runconfig_dir = os.path.join(work_dir, job_param_by_name(context, "pge_runconfig_dir"))
    os.makedirs(runconfig_dir, 0o755, exist_ok=True)

    input_dir = os.path.join(work_dir, job_param_by_name(context, "pge_input_dir"))
    os.makedirs(input_dir, 0o755, exist_ok=True)

    output_dir = os.path.join(work_dir, job_param_by_name(context, "pge_output_dir"))
    os.makedirs(output_dir, 0o755, exist_ok=True)

    return input_dir, output_dir, runconfig_dir


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


def exec_pge_command(context: Dict, work_dir: str, input_dir: str, runconfig_dir: str, output_dir: str):
    logger.info("Preparing PGE docker command.")

    # get dependency image
    dep_img = context.get('job_specification')['dependency_images'][0]
    dep_img_name = dep_img['container_image_name']
    logger.debug(f"{dep_img_name=}")

    # get docker params
    docker_params_file = os.path.join(work_dir, "_docker_params.json")
    dp = DockerParams(docker_params_file)
    docker_params = dp.params
    logger.debug(f"docker_params={to_json(docker_params)}")
    docker_img_params = docker_params[dep_img_name]
    uid = docker_img_params["uid"]
    gid = docker_img_params["gid"]

    # parse runtime options
    runtime_options = [f"--{k} {v}" for k, v in docker_img_params.get('runtime_options', {}).items()]

    # create directory to house PGE's _docker_stats.json
    pge_stats_dir = os.path.join(work_dir, 'pge_stats')
    logger.info(f"Making PGE Stats Directory: {pge_stats_dir}")
    os.makedirs(pge_stats_dir, 0o755)

    # get the location of the home directory within the container
    container_home = job_param_by_name(context, 'container_home')

    cmd = [
        f"docker run --init --rm -u {uid}:{gid}",
        " ".join(runtime_options),
        f"-v {runconfig_dir}:{container_home}/runconfig:ro",
        f"-v {input_dir}:{container_home}/input_dir:ro",
        f"-v {output_dir}:{container_home}/output_dir",
        dep_img_name,
        f"--file {container_home}/runconfig/RunConfig.yaml",
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
