#!/usr/bin/env python
"""Verification and validation of end-to-end integration test."""
import os
import json
import logging
import argparse
import backoff

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


def check_statuses(idx, query):
    results = grq_es.search(index=idx, body=query)
    results = results['hits']['hits']
    statuses = []

    all_completed = True

    for result in results:
        items = result.get("_source", {}).items()
        statuses = list(filter(lambda x: "_status" in x[0], items))
        for key, status in statuses:
            if status != "job-completed":
                all_completed = False
                return

    return all_completed


def validate_accountability_indeces(idx, expected_count, res_file):
    query = {"query": {"match_all": {}}}
    all_found = False

    logger.info("index: {}".format(idx))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    try:
        all_found = check_count(query, idx, expected_count)
        all_validated = check_statuses(idx, query)
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

        if all_validated:
            msg += "SUCCESS: Validated all {} statuses as job-completed in {}\n".format(
                expected_count, idx
            )
        else:
            msg += "ERROR: Failed to validate all {} statuses as being job-completed in {}\n".format(
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

    validate_accountability_indeces(args.index, args.expected_count, args.res_file)
