import asyncio
import logging
import re
import time
from asyncio import AbstractEventLoop
from functools import partial
from pathlib import Path
from typing import Callable

import boto3
import pytest
import requests
from botocore.config import Config
from mypy_boto3_s3 import S3Client
from requests import Response

import conftest
from benchmark_test_util import \
    get_es_host as get_mozart_ip, \
    upload_file, \
    wait_for_l2, \
    wait_for_l3

config = conftest.config

mozart_ip = get_mozart_ip()
s3_client: S3Client = boto3.client("s3", config=(Config(max_pool_connections=30)))

#######################################################################
# TEST INPUTS - SYSTEM
#######################################################################
instance_type_queues = [
    # "opera-job_worker-large",  # configured for t2.medium (Primary), t3.medium, t3a.medium
    # "opera-job_worker-t3_medium",
    # "opera-job_worker-t3_large",
    # "opera-job_worker-t3_xlarge",
    # "opera-job_worker-c5d_large",
    # "opera-job_worker-c5d_xlarge",
    # "opera-job_worker-m5d_large",
    # "opera-job_worker-m5d_xlarge",
    # "opera-job_worker-r5d_large",
    # "opera-job_worker-r5d_xlarge",
]


def setup_module():
    create_job_queues(instance_type_queues)
    pass


def setup_function():
    pass


@pytest.mark.asyncio
async def test_s30(event_loop: AbstractEventLoop):
    # NOTE:
    #  total number of PGE jobs should not exceed max job queue size (dev=10) as to not affect EBS timers
    #  e.g. assert num_runs * len(instance_type_queues) <= 10
    run_start_index = 2030  # a YYYY year. Note that the sample set already uses 2021
    num_runs = 10

    input_filenames = [
        "HLS.S30.T15SXR.2021250T163901.v2.0.B02.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.B03.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.B04.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.B11.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.B12.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.B8A.tif",
        "HLS.S30.T15SXR.2021250T163901.v2.0.Fmask.tif"  # can submit Fmask separately to treat as signal file
    ]
    old_ts = '2021'

    # input_files_dir = Path(config["S30_INPUT_DIR"]).expanduser()
    # reupload_input_files(input_files_dir, input_filenames)
    copy_and_submit_input_files_s30 = partial(
        copy_and_submit_input_files,
        l2_index="grq_1_l2_hls_s30", get_l3_id=get_l3_s30_id,
        event_loop=event_loop
    )

    grq_user_rule_name = "trigger-SCIFLO_L3_DSWx_HLS_S30"
    run_stop_index = run_start_index + num_runs
    for new_instance_type_queue_name in instance_type_queues:
        swap_instance_type(grq_user_rule_name, new_instance_type_queue_name)
        await copy_and_submit_input_files_s30(input_filenames, old_ts, run_start_index, run_stop_index)

        # update indexes for next iteration
        run_start_index = run_stop_index
        run_stop_index = run_start_index + num_runs


def create_job_queues(queues):
    logging.info("CREATE RABBITMQ QUEUES")

    for queue in queues:
        logging.info(f"Creating {queue=}")

        r: Response = requests.put(
            f"https://{mozart_ip}:15673/api/queues/%2F/{queue}",
            auth=(config["RABBITMQ_USER"], config["RABBITMQ_PASSWORD"]),
            json={
                "vhost": "/",
                "durable": True,
                "auto_delete": False,
                "arguments": {
                    "x-max-priority": 10
                }
            },
            verify=False
        )
        assert r.status_code in [201, 204]


def reupload_input_files(download_dir, input_filenames):
    logging.info("(RE)UPLOADING INPUT FILES")

    input_filepaths = [download_dir / input_filename for input_filename in input_filenames]
    for i, input_filepath in enumerate(input_filepaths):
        logging.info(f"Uploading file {i + 1} of {len(input_filepaths)}")
        logging.info(f"Uploading {input_filepath.name}")
        upload_file(input_filepath)


def swap_instance_type(grq_user_rule, instance_type_queue):
    logging.info("SWITCHING INSTANCE TYPES")

    r: Response = requests.get(
        f"https://{mozart_ip}/grq/api/v0.1/grq/user-rules?rule_name={grq_user_rule}",
        auth=(config["GRQ_USER"], config["GRQ_PASSWORD"]),
        verify=False
    )
    assert r.status_code in [200]
    res = r.json()
    old_instance_type_queue = res["rule"]["queue"]
    if old_instance_type_queue == instance_type_queue:
        logging.info(f"instance types identical. Skipping swap. {old_instance_type_queue=} {instance_type_queue=}")
        return

    logging.info(f"Swapping {old_instance_type_queue=} with {instance_type_queue=}")

    data = {
        "queue": instance_type_queue,
        "enable_dedup": True,

        "id": res["rule"]["_id"],
        "rule_name": res["rule"]["rule_name"],
        "tags": res["rule"].get("tags"),
        "query_string": res["rule"]["query_string"],
        "priority": res["rule"]["priority"],
        "workflow": res["rule"]["workflow"],
        "job_spec": res["rule"]["job_spec"],
        "kwargs": res["rule"]["kwargs"],

        "time_limit": None,
        "soft_time_limit": None,
        "disk_usage": None,
    }
    r: Response = requests.put(
        f"https://{mozart_ip}/grq/api/v0.1/grq/user-rules?rule_name={grq_user_rule}",
        auth=(config["GRQ_USER"], config["GRQ_PASSWORD"]),
        json=data,
        verify=False
    )
    assert r.status_code in [200]

    r: Response = requests.get(
        f"https://{mozart_ip}/grq/api/v0.1/grq/user-rules?rule_name={grq_user_rule}",
        auth=(config["GRQ_USER"], config["GRQ_PASSWORD"]),
        verify=False
    )
    assert r.status_code in [200]

    res = r.json()
    updated_queue = res["rule"]["queue"]
    assert updated_queue == instance_type_queue


async def copy_and_submit_input_files(
        input_filenames,
        old_ts,
        tile_id_start_index: int,
        tile_id_stop_index: int,
        l2_index: str,
        get_l3_id: Callable[[str], str],
        event_loop: AbstractEventLoop
):
    logging.info("COPYING AND SUBMITTING INPUT FILES")

    copy_tasks = []

    runs = range(tile_id_start_index, tile_id_stop_index)
    new_tss = []
    for run in runs:
        new_ts = f"{run:04d}"  # generate a new year
        new_tss.append(new_ts)
        logging.info(f"Copying files for run (tile ID) {new_ts}")

        for i, input_filename in enumerate(input_filenames):
            logging.info(f"Copying file {i + 1} of {len(input_filenames)}")
            copy_tasks.append(event_loop.run_in_executor(
                func=partial(copy_s3_file, input_filename=input_filename, old_ts=old_ts, new_ts=new_ts),
                executor=None,
            ))

    await asyncio.gather(*copy_tasks)

    logging.info("Sleeping for L2 ingestion...")
    sleep_for(180)

    # check that all input files were ingested
    for new_ts in new_tss:
        l2_tasks = []
        ids = []
        for i, input_filename in enumerate(input_filenames):
            id = get_l2_id(input_filename, old_ts, new_ts)
            ids.append(id)
            l2_tasks.append(event_loop.run_in_executor(
                func=partial(wait_for_l2, _id=id, index=l2_index),
                executor=None
            ))

        responses = await asyncio.gather(*l2_tasks, return_exceptions=False)
        for response in responses:
            assert response.hits[0]["id"] in ids

    logging.info("Sleeping for PGE execution...")
    sleep_for(300)

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")
    l3_tasks = []
    ids = []
    for new_ts in new_tss:
        id = get_l3_id(new_ts)
        ids.append(id)
        l3_tasks.append(event_loop.run_in_executor(
            func=partial(wait_for_l3, _id=id, index="grq_v2.0_l3_dswx_hls"),
            executor=None
        ))

    responses = await asyncio.gather(*l3_tasks)
    for response in responses:
        assert response.hits[0]["id"] in ids


def copy_s3_file(input_filename, old_ts, new_ts):
    copy_source = {"Bucket": config["ISL_BUCKET"], "Key": input_filename}
    destination = f"{input_filename.replace(old_ts, new_ts)}"
    logging.info(f'Copying s3://.../{copy_source["Key"]} to s3://.../{destination}')
    s3_client.copy(copy_source, copy_source["Bucket"], destination)


def get_l2_id(input_filename, old_ts, new_ts):
    return input_filename.replace(old_ts, new_ts).removesuffix(".tif")


def get_l3_s30_id(new_ts):
    return f"OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_{new_ts}0907T163901_v2.0_001"


def get_l3_l30_id(new_ts):
    return f"OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_{new_ts}0907T163901_v2.0_001"


def sleep_for(sec=None):
    logging.info(f"Sleeping for {sec} seconds...")
    time.sleep(sec)
    logging.info("Done sleeping.")
