#!/usr/bin/env python
"""Verification and validation of end-to-end integration test."""
from builtins import str
import os
import logging
import json
import backoff
import argparse

from hysds.es_util import get_grq_es


log_format = (
    "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"  # set logger
)
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

BACKOFF_CONF = {}  # back-off configuration


grq_es = get_grq_es()


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(
    backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time
)
def check_count(query, idx, expected_count):
    """Query ES index."""

    count = grq_es.get_count(index=idx, body=query)
    logger.info("count: {}/{}".format(count, expected_count))

    if count == expected_count:
        return True
    raise RuntimeError


def check_num_docs(idx, expected_count, res_file):
    """Check for expected count of documents in each GBE index."""
    query = {"query": {"match_all": {}}}
    all_found = False

    logger.info("index: {}".format(idx))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    try:
        all_found = check_count(query, idx, expected_count)
    except Exception as e:
        logger.error(str(e))
    with open(res_file, "w") as f:
        if all_found:
            msg = "SUCCESS: Found {} expected number of documents in {}\n".format(
                expected_count, idx
            )
        else:
            msg = "ERROR: Failed to find {} expected number of documents in {}\n".format(
                expected_count, idx
            )
        logger.info(msg)
        f.write(msg)
        if not all_found:
            raise RuntimeError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("index", help="Elasticsearch index")
    parser.add_argument("expected_count", type=int, help="expected count")
    parser.add_argument("res_file", help="result file")
    parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
    parser.add_argument("--max_time", type=int, default=600, help="maximum total time")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    check_num_docs(args.index, args.expected_count, args.res_file)
