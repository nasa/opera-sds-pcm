import contextlib
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Union

import backoff
import boto3
import elasticsearch
from botocore.config import Config

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search, Index
from elasticsearch_dsl.response import Response

import conftest

config = conftest.config

s3_client = boto3.client("s3", config=(Config(max_pool_connections=30)))
sqs_client = boto3.client("sqs")


def index_not_found(e: elasticsearch.exceptions.NotFoundError):
    return e.error != "index_not_found_exception"


def success_handler(details):
    if details["tries"] > 1:
        logging.info(f'Successfully called {details["target"].__name__}(...) after {details["tries"]} tries and {details["elapsed"]:f} seconds')


def raise_(ex: Exception):
    raise ex


@backoff.on_predicate(
    backoff.constant,
    lambda r: len(r) != 1,
    max_time=60*10,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30,
)
@backoff.on_exception(
    backoff.expo,
    elasticsearch.exceptions.NotFoundError,
    max_time=60*10,
    giveup=index_not_found
)
def wait_for_l2(_id, index):
    return search_es(index, _id)


@backoff.on_predicate(
    backoff.constant,
    lambda r: len(r) != 1,
    max_time=60*10,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
    interval=30
)
@backoff.on_exception(
    backoff.expo,
    elasticsearch.exceptions.NotFoundError,
    max_time=60*10,
    giveup=index_not_found
)
def wait_for_l3(_id, index):
    return search_es(index, _id)


@backoff.on_predicate(
    backoff.constant,
    lambda r: get(r, "daac_CNM_S_status") != "SUCCESS",
    # 60 seconds to queue, 300 seconds to start, 180 seconds to finish
    max_time=60*10,
    on_giveup=lambda _: raise_(Exception()),
    interval=60
)
def wait_for_cnm_s_success(_id, index):
    logging.info(f"Waiting for CNM-S success (id={_id})")
    response = search_es(_id=_id, index=index)
    return response


@backoff.on_predicate(
    backoff.constant,
    lambda r: get(r, "daac_delivery_status") != "SUCCESS",
    max_time=60*10,
    on_giveup=lambda _: raise_(Exception()),
    interval=60,
)
def wait_for_cnm_r_success(_id, index):
    logging.info(f"Waiting for CNM-R success ({_id=})")
    response = search_es(_id=_id, index=index)
    return response


def mock_cnm_r_success(id):
    logging.info(f"Mocking CNM-R success ({id=})")

    sqs_client.send_message(
        QueueUrl=config["CNMR_QUEUE"],
        # body text is dynamic, so we can skip any de-dupe logic
        MessageBody=f"""{{
            "version": "1.0",
            "provider": "JPL-OPERA",
            "collection": "SWOT_Prod_l2:1",
            "processCompleteTime": "{datetime.now().isoformat()}Z",
            "submissionTime": "2017-09-30T03:42:29.791198Z",
            "receivedTime": "2017-09-30T03:42:31.634552Z",
            "identifier": "{id}",
            "response": {{
                "status": "SUCCESS",
                "ingestionMetadata": {{
                  "catalogId": "G1238611022-POCUMULUS",
                  "catalogUrl": "https://cmr.uat.earthdata.nasa.gov/search/granules.json?concept_id=G1238611022-POCUMULUS"
                }}
            }}
        }}"""
    )


def search_es(index, _id):
    logging.info(f"Searching for {_id=}")

    search = Search(using=get_es_client(), index=index) \
        .query("match", _id=_id)

    response: Response = search.execute()
    return response


def es_index_delete(index):
    logging.info(f"Deleting {index=}")
    with contextlib.suppress(elasticsearch.exceptions.NotFoundError):
        Index(name=index, using=get_es_client()).delete()


def get(response: Response, key: str):
    try:
        return response.hits.hits[0]["_source"][key]
    except (IndexError, KeyError):
        # intentionally ignore
        logging.debug("Couldn't retrieve document attribute. Returning None.")
        return None


def get_es_client():
    return Elasticsearch(
        hosts=[f"https://{get_es_host()}/grq_es/"],
        http_auth=(config["ES_USER"], config["ES_PASSWORD"]),
        connection_class=RequestsHttpConnection,
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False
    )


def get_es_host() -> str:
    result = subprocess.run([
        "terraform output mozart_pub_ip"],
        cwd=Path.cwd() / "cluster_provisioning/dev",
        stdout=subprocess.PIPE,
        shell=True,
        text=True
    )

    es_host = result.stdout.strip().strip("\"")
    return es_host


def upload_file(filepath: Union[Path, str], bucket=config["ISL_BUCKET"], object_name=None):
    """Upload a file to an S3 bucket

    :param filepath: File to upload
    :param bucket: destination S3 bucket name
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    logging.info(f"Uploading {filepath}")

    if isinstance(filepath, Path):
        filepath = str(filepath)

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(filepath)

    # Upload the file
    s3_client.upload_file(filepath, bucket, object_name)


def delete_output_files(bucket=None, prefix=None):
    """
    :param bucket: bucket name
    :param prefix: S3 object prefix. e.g. "folder1/"
    :return:
    """
    logging.info(f"Deleting S3 objects at s3://{bucket}/{prefix}")

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    try:
        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    except KeyError:
        logging.warning("Error while deleting objects. Ignoring.")
