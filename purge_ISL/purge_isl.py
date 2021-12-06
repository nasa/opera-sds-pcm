#!/usr/bin/env python
"""
Purge files from the ISL
"""

import os
import logging
import json
import boto3

from urllib.parse import urlparse
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

BASE_PATH = os.path.dirname(__file__)


@exec_wrapper
def main():
    """Main."""
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.info("job_context: {}".format(json.dumps(job_context, indent=2)))
    isl_urls = job_context["isl_urls"]
    # Convert to list if needed
    if not isinstance(isl_urls, list):
        isl_urls = [isl_urls]
    for isl_url in isl_urls:
        if isl_url:
            logger.info("Purging ISL: {}".format(isl_url))
            parsed_url = urlparse(isl_url)
            region = parsed_url.netloc.split(".", 1)[0].split("s3-")[1]
            tokens = parsed_url.path.strip("/").split("/", 1)
            bucket = tokens[0]
            key = tokens[1]
            logger.info("region={}, bucket={}, key={}".format(region, bucket, key))
            s3 = boto3.client("s3", region_name=region)
            response = s3.delete_object(Bucket=bucket, Key=key)
            logging.info("Delete object response: {}".format(response))
        else:
            logger.info("Detected empty string in the list of ISLs to purge")


if __name__ == "__main__":
    main()
