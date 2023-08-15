import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import backoff
import boto3
import elasticsearch
import elasticsearch_dsl.response
import elasticsearch_dsl.response
import requests
from botocore.config import Config
from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Hit
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef, FunctionConfigurationResponseTypeDef
from requests import Response

import conftest
from int_test_util import success_handler, raise_, get_es_host, get_es_client, index_not_found

config = conftest.config

aws_lambda: LambdaClient = boto3.client("lambda", config=(Config(max_pool_connections=30)))


def invoke_l30_subscriber_query_lambda():
    logging.info("Invoking data subscriber query timer lambda")

    response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName=config["L30_DATA_SUBSCRIBER_QUERY_LAMBDA"],
        Payload=generate_payload_cloudwatch_scheduled_event()
    )
    return response


def invoke_s30_subscriber_query_lambda():
    logging.info("Invoking data subscriber query timer lambda")

    response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName=config["S30_DATA_SUBSCRIBER_QUERY_LAMBDA"],
        Payload=generate_payload_cloudwatch_scheduled_event()
    )
    return response


def invoke_slc_subscriber_query_lambda():
    logging.info("Invoking data subscriber query timer lambda")

    response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName=config["SLC_DATA_SUBSCRIBER_QUERY_LAMBDA"],
        Payload=generate_payload_cloudwatch_scheduled_event_slc()
    )
    return response


def invoke_slc_subscriber_ionosphere_download_lambda():
    logging.info("Invoking data subscriber query timer lambda")

    response: InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName=config["SLC_DATA_SUBSCRIBER_IONOSPHERE_DOWNLOAD_LAMBDA"],
        Payload=generate_payload_cloudwatch_scheduled_event_current_time()
    )
    return response


def update_env_vars_l30_subscriber_query_lambda():
    logging.info("updating data subscriber query timer lambda environment variables")
    update_env_vars_subscriber_query_lambda(FunctionName=config["L30_DATA_SUBSCRIBER_QUERY_LAMBDA"])


def update_env_vars_s30_subscriber_query_lambda():
    logging.info("updating data subscriber query timer lambda environment variables")
    update_env_vars_subscriber_query_lambda(FunctionName=config["S30_DATA_SUBSCRIBER_QUERY_LAMBDA"])


def update_env_vars_slc_subscriber_query_lambda():
    logging.info("updating data subscriber query timer lambda environment variables")
    update_env_vars_subscriber_query_lambda(
        FunctionName=config["SLC_DATA_SUBSCRIBER_QUERY_LAMBDA"],
        additional_environment_variable_updates={
            "BOUNDING_BOX": "-124.763068,24.523096,-66.949895,47.459686"  # mainland USA
        }
    )


def update_env_vars_subscriber_query_lambda(FunctionName: str, additional_environment_variable_updates: Optional[dict] = None):
    if additional_environment_variable_updates is None:
        additional_environment_variable_updates = {}

    response: FunctionConfigurationResponseTypeDef = aws_lambda.get_function_configuration(FunctionName=FunctionName)
    environment_variables: dict = response["Environment"]["Variables"]

    environment_variables["SMOKE_RUN"] = "true"
    environment_variables["DRY_RUN"] = "false"
    environment_variables["NO_SCHEDULE_DOWNLOAD"] = "false"
    environment_variables["MINUTES"] = "rate(60 minutes)"
    environment_variables["USE_TEMPORAL"] = "true"
    environment_variables["TEMPORAL_START_DATETIME_MARGIN_DAYS"] = ""

    environment_variables.update(additional_environment_variable_updates)

    aws_lambda.update_function_configuration(
        FunctionName=FunctionName,
        Environment={"Variables": environment_variables}
    )


def update_env_vars_subscriber_slc_ionosphere_download_lambda():
    logging.info("updating data subscriber SLC ionosphere download timer lambda environment variables")
    update_env_vars_subscriber_slc_ionosphere_download_lambda_helper(
        FunctionName=config["SLC_DATA_SUBSCRIBER_IONOSPHERE_DOWNLOAD_LAMBDA"],
        additional_environment_variable_updates={
            "QUERY_END_DATETIME_OFFSET_HOURS": "0"  # ensure no offset so ionosphere download timerange covers integration test data
        }
    )


def update_env_vars_subscriber_slc_ionosphere_download_lambda_helper(FunctionName: str, additional_environment_variable_updates: Optional[dict] = None):
    if additional_environment_variable_updates is None:
        additional_environment_variable_updates = {}

    response: FunctionConfigurationResponseTypeDef = aws_lambda.get_function_configuration(FunctionName=FunctionName)
    environment_variables: dict = response["Environment"]["Variables"]

    environment_variables.update(additional_environment_variable_updates)

    aws_lambda.update_function_configuration(
        FunctionName=FunctionName,
        Environment={"Variables": environment_variables}
    )


def reset_env_vars_l30_subscriber_query_lambda():
    logging.info("reseting data subscriber query timer lambda environment variables")
    reset_env_vars_subscriber_query_lambda(FunctionName=config["L30_DATA_SUBSCRIBER_QUERY_LAMBDA"])


def reset_env_vars_s30_subscriber_query_lambda():
    logging.info("reseting data subscriber query timer lambda environment variables")
    reset_env_vars_subscriber_query_lambda(FunctionName=config["S30_DATA_SUBSCRIBER_QUERY_LAMBDA"])


def reset_env_vars_slc_subscriber_query_lambda():
    logging.info("reseting data subscriber query timer lambda environment variables")
    reset_env_vars_subscriber_query_lambda(
        FunctionName=config["SLC_DATA_SUBSCRIBER_QUERY_LAMBDA"],
        additional_environment_variable_updates={"BOUNDING_BOX": ""}
    )


def reset_env_vars_subscriber_query_lambda(FunctionName: str, additional_environment_variable_updates: Optional[dict] = None):
    if additional_environment_variable_updates is None:
        additional_environment_variable_updates = {}

    response: FunctionConfigurationResponseTypeDef = aws_lambda.get_function_configuration(FunctionName=FunctionName)
    environment_variables: dict = response["Environment"]["Variables"]

    environment_variables["SMOKE_RUN"] = "false"
    environment_variables["DRY_RUN"] = "false"
    environment_variables["NO_SCHEDULE_DOWNLOAD"] = "false"
    environment_variables["MINUTES"] = "rate(60 minutes)"
    environment_variables["USE_TEMPORAL"] = "false"
    environment_variables["TEMPORAL_START_DATETIME_MARGIN_DAYS"] = "3"

    environment_variables.update(additional_environment_variable_updates)

    aws_lambda.update_function_configuration(
        FunctionName=FunctionName,
        Environment={"Variables": environment_variables}
    )


def wait_for_query_job(job_id):
    logging.info(f"Checking query job status. {job_id=}")

    return wait_for_job(job_id)


@backoff.on_predicate(
    backoff.constant,
    lambda job_status: job_status["success"] is not True or job_status["status"] != "job-completed",
    max_time=60 * 10,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30,
    jitter=None
)
@backoff.on_exception(
    backoff.expo,
    elasticsearch.exceptions.NotFoundError,
    max_time=60 * 10,
    giveup=index_not_found,
    jitter=None
)
def wait_for_job(job_id):
    logging.info(f"Checking job status. {job_id=}")

    response: Response
    if config.get("ES_USER") and config.get("ES_PASSWORD"):
        response = requests.get(
            f"https://{get_es_host()}/mozart/api/v0.1/job/status?id={job_id}",
            verify=False,
            auth=(config["ES_USER"], config["ES_PASSWORD"])
        )
    else:
        # attempt no-cred connection. typically when running within the cluster
        response = requests.get(f"https://{get_es_host()}/mozart/api/v0.1/job/status?id={job_id}", verify=False)
    job_status = response.json()
    return job_status


@backoff.on_predicate(
    backoff.constant,
    lambda is_truthy_status: not is_truthy_status,
    max_time=60 * 10,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30,
    jitter=None
)
@backoff.on_exception(
    backoff.expo,
    elasticsearch.exceptions.NotFoundError,
    max_time=60 * 10,
    giveup=index_not_found,
    jitter=None
)
def wait_for_download_job(job_id, index):
    logging.info(f"Checking download job status. {job_id=}")

    response: elasticsearch_dsl.response.Response = Search(using=get_es_client(),
                                                           index=index) \
        .query("match", query_job_id=job_id) \
        .query("match", downloaded=True) \
        .execute()

    if len(response.hits) == 0:
        return False

    hit: Hit
    for hit in response:
        logging.info(f"{hit=}")
    return True


def generate_payload_cloudwatch_scheduled_event():
    # using known datetime range that has data
    return b"""
        {
          "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
          "detail-type": "Scheduled Event",
          "source": "aws.events",
          "account": "123456789012",
          "time": "2022-01-01T01:00:00Z",
          "region": "us-east-1",
          "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
          ],
          "detail": {}
        }
    """


def generate_payload_cloudwatch_scheduled_event_slc():
    # using known datetime range that has data
    return b"""
        {
          "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
          "detail-type": "Scheduled Event",
          "source": "aws.events",
          "account": "123456789012",
          "time": "2022-11-17T01:00:00Z",
          "region": "us-east-1",
          "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
          ],
          "detail": {}
        }
    """


def generate_payload_cloudwatch_scheduled_event_current_time():
    return (f"""
        {{
          "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
          "detail-type": "Scheduled Event",
          "source": "aws.events",
          "account": "123456789012",
          "time": "{(datetime.now(timezone.utc) - timedelta(hours=0)).isoformat(timespec="seconds").replace("+00:00", "Z")}",
          "region": "us-east-1",
          "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
          ],
          "detail": {{}}
        }}
    """).encode("utf-8")
