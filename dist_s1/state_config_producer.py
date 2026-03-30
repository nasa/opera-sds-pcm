"""This script handles the DIST-S1 state config as part of DIST-S1 historical processing"""

import argparse
import base64
import json
import sys
import tempfile
from datetime import datetime
from functools import partial
from pathlib import Path

import opensearchpy
from more_itertools import one, only

from dist_s1.dataset_util import (create_dataset, create_ds_dataset_json, write_ds_dataset_json, write_ds_met_json)
from opera_commons.es_connection import get_grq_es
from opera_commons.logger import get_logger, configure_library_loggers
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.grq_client import get_body
from util.job_submitter import try_submit_mozart_job
from util.job_util import supply_job_id
from util.pge_util import get_product_metadata

logger = None
args = None

to_json = partial(json.dumps, indent=2)
"""json.dumps with default params"""

@exec_wrapper
def main():
    global logger
    global args

    parser = create_arg_parser()
    args = parser.parse_args(sys.argv[1:])
    logger = init_opera_pcm_logger()
    logger.info(f"{__file__} invoked with {sys.argv=}")
    logger.info(f"{args=}")

    run()

def run():
    logger.info("BEGIN")
    if args.producer:
        on_dist_s1_publish()
    elif args.consumer:
        on_state_config_publish()
    logger.info("END")

def on_dist_s1_publish():
    job_id = supply_job_id()
    settings = SettingsConf().cfg

    # state config steps
    #  1. Note the tile ID and acquisition group (number) from the produced DIST-S1 product
    #  2. Create a state-config product that includes that tile ID and acq group to mark the next one as ready to produce

    # 1
    logger.info("Loading job context")
    if args.context_file:
        logger.info("Custom _context.json provided.")
        jc = JobContext(str(args.context_file))
    elif args.b64_context:
        logger.info("Custom _context.json contents provided.")
        tp = tempfile.NamedTemporaryFile()
        tp.write(base64.b64decode(args.b64_context).decode("utf-8"))
        tp.flush()
        jc = JobContext(tp)
    else:
        jc = JobContext(str(Path("_context.json").absolute()))
    context = context_dict = job_context = jc.ctx
    logger.info(f"job_context={to_json(context_dict)}")

    # work_dir = workdir = str(args.workdir)
    work_dir = str(Path("_job.json").absolute().parent)
    logger.info(f"Preparing Working Directory: {work_dir}")
    logger.info(f"{list(Path(work_dir).iterdir())=}")

    logger.info("Reading product_metadata")
    if args.b64_product_metadata:
        logger.info("Custom product_metadata provided.")
        product_metadata = base64.b64decode(args.b64_product_metadata).decode("utf-8")
    elif product_metadata_override := context_dict.get("product_metadata_override"):
        logger.info("Using product_metadata override from _context.json.")
        product_metadata = product_metadata_override
    else:
        product_metadata = get_product_metadata(context_dict)
    logger.info(f"{product_metadata=}")
    source_product_metadata = product_metadata

    # 2. Create state-config product
    logger.info("Creating state-config update metadata")
    if output_state_config_override := context.get("output_state_config_override"):
        logger.info("Using provided output state-config")
        target_product_metadata = output_state_config_override
        assert target_product_metadata["batch_id"], "User error. Please supply batch_id in the override."
        target_product_metadata["is_complete"] = True  # only support this operation
    else:
        target_product_metadata = {
            "version": "test",
            "is_complete": True,
            # "batch_id": source_product_metadata["input_granule_id"],
            "batch_id": source_product_metadata["accountability"]["L3_DIST_S1"]["trigger_dataset_id"],
            # "mgrs_tile_id": source_product_metadata["mgrs_tile_id"],
            "input_granule_id": source_product_metadata["input_granule_id"],  # "p12TYQ_3_S1A_a369"
            "mgrs_tile_id": source_product_metadata["input_granule_id"].split("_")[0].removeprefix("p"),
            "acquisition_group": source_product_metadata["input_granule_id"].split("_")[1],
            "instrument": source_product_metadata["input_granule_id"].split("_")[2],
            "acquisition_cycle_index": source_product_metadata["input_granule_id"].split("_")[3].removeprefix("a"),  # get suffix
        }
    logger.info(f"{target_product_metadata=}")

    # output_dir = str(PurePath(work_dir) / job_param_by_name(context, "pge_output_dir"))
    batch_id = source_product_metadata["input_granule_id"]  # derive from source product (DIST-S1)
    batch_id = batch_id.removeprefix("p")
    batch_id = batch_id.replace("_a", "_")
    target_product_metadata["batch_id"] = batch_id

    state_config_metadata_existing = only(state_configs_by_batch_id(batch_id=batch_id), default={}).get("_source", {}).get("metadata", {})
    state_config_metadata_to_update = {}
    state_config_metadata_to_update.update(state_config_metadata_existing)
    state_config_metadata_to_update.update(target_product_metadata)
    state_config_metadata_to_update.update({"batch_id": batch_id, "status": "complete", "is_complete": True, "random": datetime.now().isoformat(timespec="seconds")})

    target_product_metadata = state_config_metadata_to_update

    # create "current" state-config dataset
    logger.info(f"Creating state-config files locally for post-job publishing")
    dataset_id = f"DIST_S1_state-config_{batch_id}"
    ds_dataset_json = create_ds_dataset_json(version="1.0")
    ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
    ds_met_json_path = write_ds_met_json(target_product_metadata, dataset_id)
    dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-STATE-CONFIG")
    logger.info(f"Created state-config files locally for post-job publishing. {dataset_dir=}")

    logger.info(f"{list(Path(work_dir).iterdir())=}")
    return


def on_state_config_publish():
    #  construct `--product-id-time` param using state-config
    #  1. parse state-config
    #  2. submit rtc_for_dist job with `--product-id-time`

    job_id = supply_job_id()
    settings = SettingsConf().cfg

    logger.info("Loading job context")
    if args.context_file:
        logger.info("Custom _context.json provided.")
        jc = JobContext(str(args.context_file))
    elif args.b64_context:
        logger.info("Custom _context.json contents provided.")
        tp = tempfile.NamedTemporaryFile()
        tp.write(base64.b64decode(args.b64_context).decode("utf-8"))
        tp.flush()
        jc = JobContext(tp)
    else:
        jc = JobContext(str(Path("_context.json").absolute()))
    context = context_dict = job_context = jc.ctx
    logger.info(f"job_context={to_json(context_dict)}")

    # work_dir = workdir = str(args.workdir)
    work_dir = str(Path("_job.json").absolute().parent)
    logger.info(f"Preparing Working Directory: {work_dir}")
    logger.info(f"{list(Path(work_dir).iterdir())=}")

    logger.info("Reading product_metadata")
    if args.b64_product_metadata:
        logger.info("Custom product_metadata provided.")
        state_config_metadata = product_metadata = base64.b64decode(args.b64_product_metadata).decode("utf-8")
    elif args.use_sample_product_metadata:
        logger.info("Using sample product_metadata.")
        state_config_metadata = product_metadata = create_sample_state_config()
    if product_metadata_override := context_dict.get("product_metadata_override"):
        logger.info("Using product_metadata override from _context.json.")
        state_config_metadata = product_metadata = product_metadata_override
    else:
        state_config_metadata = product_metadata = get_product_metadata(context_dict)

    state_config_metadata_existing = state_config_metadata = product_metadata = one(state_configs_by_batch_id(batch_id=state_config_metadata["batch_id"]))["_source"]["metadata"]
    logger.info(f"{product_metadata=}")

    # 1.
    product_type = "rtc_for_dist"
    if state_config_metadata.get("first"):
        product_id_time = state_config_metadata["product_id_time"]
    else:
        if not state_config_metadata.get("next_product_id_time"):
            logger.info("No next_product_id_time. Reached end of chain. Nothing further to do.")
            return
        else:
            product_id_time = state_config_metadata["next_product_id_time"]
    params = [
        {
            "name": "product_id_time",
            "from": "value",
            "type": "text",
            "value": f"--product-id-time={product_id_time}"
        }
    ]
    logger.info(f"{params=}")
    query_job_id = try_submit_mozart_job(product={},
                                            params=params,
                                            job_queue="opera-job_worker-rtc_for_dist_data_query_hist",
                                            rule_name=f"trigger-{product_type}_query_hist",
                                            job_spec=f"job-{product_type}_query_hist:{settings['RELEASE_VERSION']}",
                                            job_type=f"{product_type}_query_hist",  # stem of job-spec.json file
                                            job_name=f"job-WF-{product_type}_query_hist-{product_id_time}")
    logger.info(f"{query_job_id=}")
    return


def state_configs_by_batch_id(batch_id):
    grq_es = get_grq_es()
    body = get_body(match_all=False)
    body["query"]["bool"]["must"].append({"term": {"metadata.batch_id.keyword": batch_id}})
    try:
        results = grq_es.search(body=body, index="grq_1.0_dist_s1-state-config")
    except opensearchpy.exceptions.NotFoundError as e:
        # return []  # intentionally commented out and left in for context to reader
        raise e
    return results["hits"]["hits"]


def create_sample_state_config():
    return {
        "version": "test",
        "is_complete": True,
        "mgrs_tile_id": "12TYQ",
        "status": "complete",
        "batch_id": "12TYQ_3_S1A_369",
        "product_id_time": "30RUU_0,20260313T061308Z",
        "product_id_times": ["30RUU_0,20260313T061308Z"],
        "input_granule_id": "p12TYQ_3_S1A_a369",
        "acquisition_group": "3",
        "instrument": "S1A",
        "acquisition_cycle_index": "369"
    }


def create_product_metadata_sample():
    return {
        "id": "OPERA_L3_DIST-ALERT-S1_T12TYQ_20260225T131640Z_20260225T193352Z_S1A_30_v0.1",
        "objectid": "OPERA_L3_DIST-ALERT-S1_T12TYQ_20260225T131640Z_20260225T193352Z_S1A_30_v0.1",
        "dataset": "L3_DIST_S1",
        "metadata": {
            "id": "OPERA_L3_DIST-ALERT-S1_T12TYQ_20260225T131640Z_20260225T193352Z_S1A_30_v0.1",
            "input_granule_id": "p12TYQ_3_S1A_a369",
            "mgrs_tile_id": "12TYQ",
            "accountability": {
                "L3_DIST_S1": {
                    "id": "OPERA_L3_DIST-ALERT-S1_T12TYQ_20260225T131640Z_20260225T193352Z_S1A_30_v0.1",
                    "input_data_type": "L2_RTC_S1",
                    "trigger_dataset_type": "L2_RTC_S1",
                    "trigger_dataset_id": "p12TYQ_3_S1A_a369",
                    "inputs": ["OPERA_L2_RTC-S1_T129-275741-IW2_20260225T131640Z_20260225T172838Z_S1A_30_v1.0_VH.tif"],
                    # TODO chrisjrd: get from input file group. post copol/crosspol
                }
            }
        },
        "ipath": "hysds::data/L3_DIST_S1",
        "system_version": "v0.1",
        "dataset_level": "L3",
        "dataset_type": "L3_DIST_S1",
        "version": "v0.1",
        "creation_timestamp": "2026-02-25T19:34:09.324",
        "@timestamp": "2026-02-25T19:34:45.836928Z"
    }


def init_opera_pcm_logger():
    logger = get_logger(log_format_override="%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s")
    configure_library_loggers()
    return logger


def create_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("workdir", help="The absolute pathname of the current working directory.", type=lambda p: Path(p).absolute())
    group_producer_consumer = parser.add_mutually_exclusive_group(required=True)
    group_producer_consumer.add_argument("--producer", action="store_true", help="Run in producer mode. This tool will, given a product publication, upsert a state-config document.")
    group_producer_consumer.add_argument("--consumer", action="store_true", help="Run in consumer mode. This tool will, given a state-config document upsert, submit an RTC query job.")

    parser.add_argument("--smoke-run", action="store_true", help="Toggle for processing a single output.")
    parser.add_argument("--dry-run", action="store_true", help="Toggle for skipping network transfers.")

    group_context = parser.add_mutually_exclusive_group()
    group_context.add_argument("--context-file", dest="context_file", required=False, help="The context file in the workspace. Typically \"_context.json\".", type=lambda p: Path(p).absolute())
    group_context.add_argument("--b64-context", dest="b64_context", help="Custom _context.json contents. base64-encoded.")

    group_product_metadata = parser.add_mutually_exclusive_group()
    group_product_metadata.add_argument("--b64-product-metadata", dest="b64_product_metadata", help="Custom product metadata JSON typically found in _context.json. base64-encoded.")
    group_product_metadata.add_argument("--use-sample-product-metadata", dest="use_sample_product_metadata", action="store_true", help="Use hardcoded product_metadata.")

    return parser


if __name__ == '__main__':
    main()
