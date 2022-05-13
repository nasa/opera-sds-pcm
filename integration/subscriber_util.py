import logging

import backoff
import boto3
import elasticsearch_dsl.response
import elasticsearch_dsl.response
import mypy_boto3_lambda
import requests
from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Hit
from mypy_boto3_lambda import LambdaClient
from requests import Response

from int_test_util import \
    success_handler, raise_, get_es_host, get_es_client
from integration import conftest

config = conftest.config

aws_lambda: LambdaClient = boto3.client("lambda")


def invoke_subscriber_query_lambda():
    logging.info("Invoking data subscriber query timer lambda")

    response: mypy_boto3_lambda.type_defs.InvocationResponseTypeDef = aws_lambda.invoke(
        FunctionName=config["DATA_SUBSCRIBER_QUERY_LAMBDA"],
        Payload=generate_dummy_payload_cloudwatch_scheduled_event()
    )
    return response


@backoff.on_predicate(
    backoff.constant,
    lambda job_status: job_status["success"] is not True or job_status["status"] != "job-completed",
    max_time=60 * 1,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30,
)
def wait_for_query_job(job_id):
    logging.info(f"Checking query job status. {job_id=}")

    response: Response = requests.get(
        f"https://{get_es_host()}/mozart/api/v0.1/job/status?id={job_id}",
        verify=False,
        auth=(config["ES_USER"], config["ES_PASSWORD"])
    )
    job_status = response.json()
    return job_status


@backoff.on_predicate(
    backoff.constant,
    lambda is_truthy_status: not is_truthy_status,
    max_time=60 * 10,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30,
)
def wait_for_download_jobs(job_id):
    logging.info(f"Checking download job status. {job_id=}")

    response: elasticsearch_dsl.response.Response = Search(using=get_es_client(),
                                                           index="data_subscriber_product_catalog") \
        .query("match", query_job_id=job_id) \
        .query("match", downloaded=True) \
        .execute()

    if len(response.hits) == 0:
        return False

    hit: Hit
    for hit in response:
        logging.info(f"{hit=}")
    return True


def generate_dummy_payload_cloudwatch_scheduled_event():
    return b"""
        {
          "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
          "detail-type": "Scheduled Event",
          "source": "aws.events",
          "account": "123456789012",
          "time": "1970-01-01T00:00:00Z",
          "region": "us-east-1",
          "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
          ],
          "detail": {}
        }
    """
