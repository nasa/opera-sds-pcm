"""
OPERA PCM-PGE Wrapper. Used for doing the actual PGE runs
"""
import os
import sys
from datetime import datetime
import json
import glob
import re
import shutil

from util import pge_util
from util.exec_util import exec_wrapper, call_noerr
from util.conf_util import RunConfig
from product2dataset import product2dataset
from util.ctx_util import JobContext, DockerParams

from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as opera_chimera_const

from commons.logger import logger

timestamp = datetime.now()
PAYLOAD_KEY = "run_config"
LOCALIZE_KEY = "localize"

L0A_L_PGE_OUTPUT_REGEX = "/(NISAR_(?P<Type>[LSJ]0_RRST)_.*)$"
L0A_L_PGE_OUTPUT_PATTERN = re.compile(L0A_L_PGE_OUTPUT_REGEX)


def get_pge_error_message(logfile):
    """
    Intended to parse a PGE log file, look for errors and propagate it up to the UI

    :param logfile:
    :return:
    """
    default_msg = "PGE Failed with a FATAL error, please check PGE log file"
    if logfile is None:
        return default_msg
    msg = ""
    # Read the log file and grep for FATAL
    fatal_string = "FATAL"
    lines_after_fatal_string = 4  # No. of lines to print after FATAL string
    is_countdown = False
    with open(logfile, 'r') as fr:
        for line in fr:
            if is_countdown and lines_after_fatal_string > 0:
                lines_after_fatal_string -= 1
                msg += line
            elif lines_after_fatal_string == 0:
                return msg
            if fatal_string in line:
                print("Found FATAL")
                msg += line
                is_countdown = True
    if len(msg) < 1:
        return default_msg


def process_inputs(run_config, work_dir, output_dir):
    """
    Process the inputs:

        - Convert inputs to reference local file paths instead of S3 urls.
        - Capture inputs so the lineage can be captured in the output dataset metadata

    :param run_config:
    :param work_dir:
    :param output_dir:

    :return:
    """

    lineage_metadata = list()
    input_groups = [
        opera_chimera_const.INPUT_FILE_PATH,
        opera_chimera_const.STATIC_ANCILLARY_FILE_GROUP,
        opera_chimera_const.DYNAMIC_ANCILLARY_FILE_GROUP,
    ]

    if run_config.get("name") == "NISAR_L1-L-RSLC_RUNCONFIG":
        input_groups.remove(opera_chimera_const.DYNAMIC_ANCILLARY_FILE_GROUP)

    # Add work and output directories
    run_config[opera_chimera_const.PRODUCT_PATH] = output_dir
    run_config[opera_chimera_const.DEBUG_PATH] = work_dir

    localized_groups = {}
    for input_group in input_groups:
        if input_group in run_config:
            localized_groups[input_group] = {}
            for product_type in run_config[input_group].keys():
                value = run_config[input_group][product_type]
                if isinstance(value, list):
                    local_paths = []
                    for url in value:
                        local_path = os.path.join(work_dir, os.path.basename(url))
                        local_paths.append(local_path)
                    lineage_metadata.extend(local_paths)
                else:
                    local_paths = os.path.join(work_dir, os.path.basename(value))
                    lineage_metadata.append(local_paths)

                localized_groups[input_group][product_type] = local_paths

    run_config.update(localized_groups)
    return run_config, lineage_metadata


def run_pipeline(context, work_dir):
    """
    Run the PGE in OPERA land
    :param context: Path to HySDS _context.json
    :param work_dir: PGE working directory

    :return:
    """
    run_config = context.get("run_config")
    pge_config = context.get("pge_config")

    # get depedency image
    dep_img = context.get('job_specification')['dependency_images'][0]
    dep_img_name = dep_img['container_image_name']
    logger.info("dep_img_name: {}".format(dep_img_name))

    logger.info("Working Directory: {}".format(work_dir))
    output_dir = os.path.join(work_dir, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, 0o755)

    # create directory to house PGE's _docker_stats.json
    pge_stats_dir = os.path.join(work_dir, 'pge_stats')
    logger.debug("PGE Stats Directory: {}".format(pge_stats_dir))
    os.makedirs(pge_stats_dir, 0o755)

    run_config = json.loads(json.dumps(run_config))

    # We need to convert the S3 urls specified in the run config to local paths and also
    # capture the inputs so we can store the lineage in the output dataset metadata
    run_config, lineage_metadata = process_inputs(run_config, work_dir, output_dir)

    # Set the docker image name and version?
    dep_img_name_tokens = dep_img_name.split(":", 1)
    logger.debug("Splitting the PGE Docker Image Name: {}".format(dep_img_name_tokens))

    # Run the PGE
    logger.debug("Runconfig to transform to YAML is: {}".format(json.dumps(run_config)))
    pge_name = pge_config.get(opera_chimera_const.PGE_NAME)
    rc = RunConfig(run_config, pge_name)
    rc_file = os.path.join(work_dir, 'RunConfig.yaml')
    rc.dump(rc_file)
    logger.debug("Run Config: {}".format(json.dumps(run_config)))

    logger.debug("PGE Config: {}".format(json.dumps(pge_config)))
    if opera_chimera_const.SIMULATE_OUTPUTS in context and context[opera_chimera_const.SIMULATE_OUTPUTS]:
        logger.info("Simulate PGE run....")
        pge_util.simulate_run_pge(run_config, output_dir, pge_config, context)
    else:
        # get docker params
        docker_params_file = os.path.join(work_dir, "_docker_params.json")

        dp = DockerParams(docker_params_file)
        docker_params = dp.params
        logger.info("docker_params: {}".format(json.dumps(docker_params, indent=2)))

        docker_img_params = docker_params[dep_img_name]
        uid = docker_img_params["uid"]
        gid = docker_img_params["gid"]

        # parse runtime options
        runtime_options = []
        for k, v in docker_img_params.get('runtime_options', {}).items():
            runtime_options.extend(["--{}".format(k), "{}".format(v)])

        cmd = [
            "docker run --init --rm -u {uid}:{gid} -v {work_dir}:/pge/run -w /pge/run".format(
                uid=uid, gid=gid, work_dir=work_dir
            ),
            " ".join(runtime_options),
            "-v", "/data/work/jobs:/data/work/jobs",
            "-v", "/data/work/cache:/data/work/cache:ro",
            "-v", "/home/ops/verdi/etc/datasets.json:/home/ops/verdi/etc/datasets.json:ro",
            dep_img_name,
            "--file", rc_file.split("/")[-1],
            "--stats", "/pge/run/pge_stats/_docker_stats.json",
        ]

        cmd_line = " ".join(cmd)
        logger.info("Calling PGE: {}".format(cmd_line))
        try:
            call_noerr(cmd_line, work_dir)
        except Exception as e:
            logger.error("PGE failure: {}".format(e))
            raise

    # For Time_Extractor PGE, we need to copy the input product to the output dataset
    if pge_name == opera_chimera_const.TIME_EXTRACTOR:
        logger.info("run_config: {}".format(json.dumps(run_config, indent=2)))
        logger.info("pge_config: {}".format(json.dumps(pge_config, indent=2)))
        product_counter = run_config.get(opera_chimera_const.PRODUCT_COUNTER)
        logger.info("product_counter: {}".format(product_counter))
        l0a_bins = run_config.get(opera_chimera_const.INPUT_FILE_PATH, {}).get(
            pge_config.get(opera_chimera_const.PRIMARY_INPUT, None))

        if l0a_bins:
            for l0a_bin in l0a_bins:
                # Current working directory should have the L0A bin file if we've made it
                # this far
                ext = os.path.splitext(l0a_bin)[1]
                base_name, ext = os.path.splitext(os.path.basename(l0a_bin))
                if ext != ".bin":
                    raise NotImplementedError("Cannot handle file {}.".format(l0a_bin))
                base_name_match = re.search(r'^(.+)_\d{3}$', base_name)
                if not base_name_match:
                    raise RuntimeError("Unrecognized base name format: {}".format(base_name))
                base_name = "{}_{:03d}".format(base_name_match.group(1), product_counter)
                src = os.path.join(work_dir, os.path.basename(l0a_bin))
                dest = os.path.join(output_dir, "{}{}".format(base_name, ext))
                logger.info("copying {} to {}".format(src, dest))
                shutil.copyfile(src, dest)
        else:
            raise RuntimeError("Could not find the input L0A file(s) to move to the output directory")

    extra_met = {
        "lineage": lineage_metadata,
        "runconfig": run_config
    }
    if opera_chimera_const.EXTRA_PGE_OUTPUT_METADATA in run_config:
        for met_key in run_config[opera_chimera_const.EXTRA_PGE_OUTPUT_METADATA].keys():
            extra_met[met_key] = run_config[opera_chimera_const.EXTRA_PGE_OUTPUT_METADATA][met_key]

    logger.info("Converting output product to HySDS-style datasets")
    created_datasets = product2dataset.convert(output_dir, pge_name, rc_file, extra_met=extra_met)

    # For the L0A_L_PGE, append a "PP" to the dataset directory, .met.json, and dataset.json
    # Seems like we can't do this at the post processor level in Chimera since this renaming
    # has to occur prior to dataset publishing.
    if pge_name == opera_chimera_const.L0A:
        glob_patterns = ["*", "*/*.met.json", "*/*.dataset.json"]
        output_dataset_dir = os.path.join(output_dir, product2dataset.DATASETS_DIR_NAME)
        for glob_pattern in glob_patterns:
            results = glob.glob(os.path.join(output_dataset_dir, glob_pattern))
            if results:
                for result in results:
                    match = L0A_L_PGE_OUTPUT_PATTERN.search(result)
                    if match:
                        if "Type" in list(match.groupdict().keys()):
                            type = match.groupdict()["Type"]
                            renamed_type = "{}_PP".format(type)
                            if os.path.isdir(result):
                                renamed_result = result.replace(type, renamed_type)
                            else:
                                # Preserve directory name if renaming files
                                file_name = os.path.basename(result)
                                renamed_file = file_name.replace(type, renamed_type)
                                renamed_result = os.path.join(os.path.dirname(result), renamed_file)
                            logger.info("Renaming {} to {}".format(result, renamed_result))
                            os.rename(result, renamed_result)
                        else:
                            raise ValueError("Could not find the Type field in the output file/dir: {}".format(result))
            else:
                raise ValueError("Could not find glob pattern from output directory {}: {}".format(output_dataset_dir,
                                                                                                   glob_pattern))

    # For the L0B_PGE, update the OBS_ID and DATATAKE_ID to be string. It is temp fix
    if pge_name == opera_chimera_const.L0B:
        glob_pattern = "*/*.met.json"
        output_dataset_dir = os.path.join(output_dir, product2dataset.DATASETS_DIR_NAME)
        results = glob.glob(os.path.join(output_dataset_dir, glob_pattern))
        print("PGE_WRAPPER : results : {}".format(results))
        if results:
            for result in results:
                print("PGE_WRAPPER : processing : {}".format(result))

                with open(result, "r") as jsonFile:
                    data = json.load(jsonFile)
                if "OBS_ID" in data:
                    data["OBS_ID"] = str(data["OBS_ID"])
                if "DATATAKE_ID" in data:
                    data["DATATAKE_ID"] = str(data["DATATAKE_ID"])
                with open(result, "w") as jsonFile:
                    json.dump(data, jsonFile, indent=4)
        else:
            raise ValueError("Could not find glob pattern from output directory {}: {}".format(output_dataset_dir,
                                                                                               glob_pattern))

    return created_datasets


@exec_wrapper
def main(args):
    context_file = args[1]
    workdir = sys.argv[2]
    jc = JobContext(context_file)
    ctx = jc.ctx
    logger.debug(json.dumps(ctx, indent=2))

    # set additional files to triage
    jc.set('_triage_additional_globs', ["output", "RunConfig.yaml"])
    jc.save()

    run_pipeline(context=ctx, work_dir=workdir)


if __name__ == '__main__':
    main(sys.argv)
