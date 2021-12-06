#!/usr/bin/env python
"""
Verification that the ISL bucket is empty.
"""
import boto3
import backoff
import logging
import os
import argparse


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


# backoff configuration
BACKOFF_CONF = {}


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(
    backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time
)
def is_bucket_empty(bucket):
    s3 = boto3.client("s3")
    response = s3.list_objects(Bucket=bucket)
    if (
        response.get("Contents") is None or
        (
            len(response.get("Contents")) == 1
            and response.get("Contents")[0]["Key"] == "met_required/"
        )
    ):
        return True
    else:
        raise RuntimeError("ERROR: Files still exist in the ISL Bucket '{}'".format(bucket))


def main(bucket, res_file):
    is_empty = False
    try:
        is_empty = is_bucket_empty(bucket)
    except Exception as e:
        logger.error(str(e))
    with open(res_file, "w") as f:
        if is_empty:
            f.write("SUCCESS: No files exist in the ISL Bucket: {}".format(bucket))
        else:
            f.write("ERROR: files still exist in the ISL Bucket '{}'".format(bucket))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bucket", help="ISL bucket")
    parser.add_argument("res_file", help="result file")
    parser.add_argument(
        "--max_value", type=int, default=64, help="maximum backoff time"
    )
    parser.add_argument("--max_time", type=int, default=180, help="maximum total time")
    args = parser.parse_args()
    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time
    main(args.bucket, args.res_file)
