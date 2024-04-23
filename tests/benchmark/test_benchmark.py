import logging
import time
from asyncio import AbstractEventLoop

import boto3
import pytest
import requests
from botocore.config import Config
from mypy_boto3_autoscaling import AutoScalingClient
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
from mypy_boto3_s3 import S3Client
from requests import Response

from . import conftest
import tests.integration.conftest
from .benchmark_test_util import get_es_host as get_mozart_ip
from tests.benchmark.tosca import wait_for_pge_jobs_to_finish, wait_for_download_jobs_to_finish, wait_for_query_jobs_to_finish, \
    wait_for_slc_pge_jobs_to_finish

config = conftest.config

mozart_ip = get_mozart_ip()
s3_client: S3Client = boto3.client("s3", config=(Config(max_pool_connections=30)))

#######################################################################
# TEST INPUTS - SYSTEM
#######################################################################
instance_type_queues = [
    # "opera-job_worker-sciflo-l3_dswx_s1",
    # "opera-job_worker-t3a_2xlarge",  # 8 vCPU burst
    # "opera-job_worker-m2_2xlarge",  # 4 vCPUs, 34.2 GB, moderate
    # "opera-job_worker-t3_2xlarge",  # 8 vCPU burst
    # "opera-job_worker-c6i_2xlarge",
    # "opera-job_worker-c7i_2xlarge",
    # "opera-job_worker-m6a_2xlarge",
    # "opera-job_worker-c5_2xlarge",
    # "opera-job_worker-c6a_2xlarge",
    # "opera-job_worker-m5d_2xlarge",
    # "opera-job_worker-m6id_2xlarge",
    # "opera-job_worker-t2_2xlarge",  # 8 vCPU burst, moderate
    # "opera-job_worker-c3_2xlarge",  # 15 GB, high
    # "opera-job_worker-c5ad_2xlarge",
    # "opera-job_worker-m5_2xlarge",
    # "opera-job_worker-c6id_2xlarge",
    # "opera-job_worker-c4_2xlarge",  # 15 GB, high
    # "opera-job_worker-c5d_2xlarge",
    # "opera-job_worker-m7i_2xlarge",
    # "opera-job_worker-c5a_2xlarge",
    # "opera-job_worker-c5n_2xlarge",
    # "opera-job_worker-m7i_flex_2xlarge",
    # "opera-job_worker-m6i_2xlarge",
    # "opera-job_worker-c7a_2xlarge",
    # "opera-job_worker-m7a_2xlarge",
    # "opera-job_worker-m6in_2xlarge",
    # "opera-job_worker-c6in_2xlarge",
    # "opera-job_worker-m4_2xlarge",
    # "opera-job_worker-m5a_2xlarge",
    # "opera-job_worker-m6idn_2xlarge",

]


def setup_module():
    create_job_queues(instance_type_queues)
    pass


def setup_function():
    pass


@pytest.mark.asyncio
async def test_s30(event_loop: AbstractEventLoop):
    branch = "develop"

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
        # wait_for_jobs_to_finish(job_type=f"job-send_notify_msg:{branch}")


@pytest.mark.asyncio
async def test_slc(event_loop: AbstractEventLoop):
    branch = "develop"

    for new_instance_type_queue_name in instance_type_queues:
        logging.info(f"{new_instance_type_queue_name=}")

        integration.conftest.clear_pcm_test_state()
        swap_instance_type("trigger-SCIFLO_L2_CSLC_S1", new_instance_type_queue_name)
        swap_instance_type("trigger-SCIFLO_L2_RTC_S1", new_instance_type_queue_name)

        query_timer_lambda_response = await invoke_slc_subscriber_query_lambda()
        query_job_id = query_timer_lambda_response["Payload"].read().decode().strip("\"")
        logging.info(f"{query_job_id=}")

        wait_for_query_jobs_to_finish(job_type=f"job-slcs1a_query:{branch}")
        wait_for_download_jobs_to_finish(job_type=f"job-slc_download:{branch}")
        wait_for_slc_pge_jobs_to_finish(job_type=f"job-SCIFLO_L2_CSLC_S1:{branch}")
        wait_for_slc_pge_jobs_to_finish(job_type=f"job-SCIFLO_L2_RTC_S1:{branch}")
        # wait_for_jobs_to_finish(job_type=f"job-send_notify_msg:{branch}")


@pytest.mark.asyncio
async def test_rtc(event_loop: AbstractEventLoop):
    branch = "issue_704"

    instance_types = [
        # "t3a.2xlarge",  # 8 vCPU burst
        # "m2.2xlarge",  # 4 vCPUs, 34.2 GB, moderate
        # "t3.2xlarge",  # 8 vCPU burst
        # "c6i.2xlarge",
        # "c7i.2xlarge",  # AWS no availability
        # "m6a.2xlarge",
        # "c5.2xlarge",
        # "c6a.2xlarge",
        # "m5d.2xlarge",  # AWS no availability. TERRIBLE
        # "m6id.2xlarge",  # AWS no availability
        # "t2.2xlarge",  # 8 vCPU burst, moderate
        # "c3.2xlarge",  # 15 GB, high
        # "c5ad.2xlarge",
        # "m5.2xlarge",
        # "c6id.2xlarge",
        # "c4.2xlarge",  # 15 GB, high
        # "c5d.2xlarge",
        # "m7i.2xlarge",
        # "c5a.2xlarge",
        # "c5n.2xlarge",
        # "m7i-flex.2xlarge",
        # "m6i.2xlarge",
        # "c7a.2xlarge",
        # "m7a.2xlarge",
        # "m6in.2xlarge",
        # "c6in.2xlarge",  # AWS no availability
        # "m4.2xlarge",  # AWS no availability
        # "m5a.2xlarge",  # 33 GB
        # "m6idn.2xlarge",
    ]

    for new_instance_type in instance_types:
        logging.info(f"{new_instance_type=}")

        integration.conftest.clear_pcm_test_state()
        swap_instance_type_asg(asg_name="opera-crivas-1-opera-job_worker-sciflo-l3_dswx_s1", instance_type=new_instance_type, max_size=3)

        query_timer_lambda_response = await invoke_rtc_subscriber_query_lambda()
        query_job_id = query_timer_lambda_response["Payload"].read().decode().strip("\"")
        logging.info(f"{query_job_id=}")

        wait_for_query_jobs_to_finish(job_type=f"job-rtc_query:{branch}")
        wait_for_download_jobs_to_finish(job_type=f"job-rtc_download:{branch}")
        wait_for_slc_pge_jobs_to_finish(job_type=f"job-SCIFLO_L3_DSWx_S1:{branch}")
        # wait_for_jobs_to_finish(job_type=f"job-send_notify_msg:{branch}")


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


async def invoke_slc_subscriber_query_lambda():
    aws_lambda: LambdaClient = boto3.client("lambda")
    query_timer_lambda_response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName="opera-crivas-1-slcs1a-query-timer",
        Payload=b"""
                {
                  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                  "detail-type": "Scheduled Event",
                  "source": "aws.events",
                  "account": "123456789012",
                  "time": "2022-11-17T07:00:00Z",
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


async def invoke_rtc_subscriber_query_lambda():
    aws_lambda: LambdaClient = boto3.client("lambda")
    query_timer_lambda_response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName="opera-crivas-1-rtc-query-timer",
        Payload=b"""
                {
                  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                  "detail-type": "Scheduled Event",
                  "source": "aws.events",
                  "account": "123456789012",
                  "time": "2023-10-20T01:00:00Z",
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


def swap_instance_type_asg(asg_name, instance_type, max_size=30):
    logging.info("SWITCHING INSTANCE TYPES")

    autoscaling: AutoScalingClient = boto3.client("autoscaling")

    logging.info(f"Swapping with {instance_type=}")

    update_asg_response = autoscaling.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        MaxSize=0,
        DesiredCapacity=0,
        MixedInstancesPolicy={
            "LaunchTemplate": {
                "Overrides": [{"InstanceType": instance_type}]
            }
        }
    )
    assert update_asg_response["ResponseMetadata"]["HTTPStatusCode"] == 200

    logging.info("TRUNCATING ASG")
    sleep_for(60)
    logging.info("SCALING ASG")

    update_asg_response = autoscaling.update_auto_scaling_group(
        AutoScalingGroupName=asg_name,
        MaxSize=max_size,
        DesiredCapacity=max_size
    )
    assert update_asg_response["ResponseMetadata"]["HTTPStatusCode"] == 200


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

    autoscaling: AutoScalingClient = boto3.client("autoscaling")
    update_asg_response = autoscaling.update_auto_scaling_group(
        AutoScalingGroupName=f"opera-crivas-1-{instance_type_queue}",
        MaxSize=30,
        DesiredCapacity=30
    )
    assert update_asg_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def sleep_for(sec=None):
    logging.info(f"Sleeping for {sec} seconds...")
    time.sleep(sec)
    logging.info("Done sleeping.")
