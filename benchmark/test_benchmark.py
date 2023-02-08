import logging
import time
from asyncio import AbstractEventLoop

import boto3
import pytest
import requests
from botocore.config import Config
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
from mypy_boto3_s3 import S3Client
from requests import Response

import conftest
import integration.conftest
from benchmark_test_util import get_es_host as get_mozart_ip
from tosca import wait_for_pge_jobs_to_finish, wait_for_download_jobs_to_finish, wait_for_query_jobs_to_finish

config = conftest.config

mozart_ip = get_mozart_ip()
s3_client: S3Client = boto3.client("s3", config=(Config(max_pool_connections=30)))

#######################################################################
# TEST INPUTS - SYSTEM
#######################################################################
instance_type_queues = [
    # "opera-job_worker-sciflo-l3_dswx_hls",  # configured for t2.medium (Primary), t3.medium, t3a.medium
    # "opera-job_worker-t3_large",
    # "opera-job_worker-t3a_large",
    # "opera-job_worker-m3_large",
    # "opera-job_worker-m4_large",
    # "opera-job_worker-m5_large",
    # "opera-job_worker-m6i_large",
    # "opera-job_worker-m5a_large",
    # "opera-job_worker-m6a_large",
]


def setup_module():
    create_job_queues(instance_type_queues)
    pass


def setup_function():
    pass


@pytest.mark.asyncio
async def test_s30(event_loop: AbstractEventLoop):
    branch = "issue_319"

    for new_instance_type_queue_name in instance_type_queues:
        logging.info(f"{new_instance_type_queue_name=}")

        integration.conftest.clear_pcm_test_state()
        swap_instance_type("trigger-SCIFLO_L3_DSWx_HLS_S30", new_instance_type_queue_name)

        query_timer_lambda_response = await invoke_s30_subscriber_query_lambda()
        query_job_id = query_timer_lambda_response["Payload"].read().decode().strip("\"")
        logging.info(f"{query_job_id=}")

        wait_for_query_jobs_to_finish(job_type=f"job-hlss30_query:{branch}")
        wait_for_download_jobs_to_finish(job_type=f"job-hls_download:{branch}")
        wait_for_pge_jobs_to_finish(job_type=f"job-SCIFLO_L3_DSWx_HLS:{branch}")
        # wait_for_jobs_to_finish(job_type=f"job-send_notify_msg:develop")


async def invoke_s30_subscriber_query_lambda():
    aws_lambda: LambdaClient = boto3.client("lambda")
    query_timer_lambda_response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName="opera-crivas-1-hlss30-query-timer",
        Payload=b"""
                {
                  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                  "detail-type": "Scheduled Event",
                  "source": "aws.events",
                  "account": "123456789012",
                  "time": "2022-01-02T06:00:00Z",
                  "region": "us-east-1",
                  "resources": [
                    "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
                  ],
                  "detail": {}
                }
            """
    )
    assert query_timer_lambda_response["StatusCode"] == 200
    return query_timer_lambda_response


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


def sleep_for(sec=None):
    logging.info(f"Sleeping for {sec} seconds...")
    time.sleep(sec)
    logging.info("Done sleeping.")
