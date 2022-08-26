#!/usr/bin/env python
"""
Purge files from the ISL
"""

import os
import logging
import json
from functools import cache
from typing import Union

import boto3

from urllib.parse import urlparse

from more_itertools import always_iterable

from util.ctx_util import JobContext
from util.exec_util import exec_wrapper


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

BASE_PATH = os.path.dirname(__file__)


@exec_wrapper
def checked_main():
    main()


def main():
    """Main."""
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.info(f"job_context: {json.dumps(job_context, indent=2)}")

    purge_isl_urls(job_context["isl_urls"])


def purge_isl_urls(isl_urls: Union[str, list[str]]):
    isl_urls = [isl_url for isl_url in always_iterable(isl_urls) if isl_url]
    for isl_url in isl_urls:
        purge_isl_url(isl_url)


def purge_isl_url(isl_url: str):
    logger.info(f"Purging ISL: {isl_url}")

    parsed_url = urlparse(isl_url)
    region = parsed_url.netloc.split(".", 1)[0].split("s3-")[1]
    tokens = parsed_url.path.strip("/").split("/", 1)
    bucket = tokens[0]
    key = tokens[1]
    logger.info(f"region={region}, bucket={bucket}, key={key}")
    s3 = get_cached_s3_client(region)

    response = s3.delete_object(Bucket=bucket, Key=key)
    logging.info(f"Delete object response: {response}")


@cache
def get_cached_s3_client(region_name: str):
    return boto3.client("s3", region_name=region_name)


if __name__ == "__main__":
    checked_main()
