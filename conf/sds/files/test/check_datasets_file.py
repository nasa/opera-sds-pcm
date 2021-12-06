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
total_count = 0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def check_count(query, idx, expected_count):
    """Query ES index."""
    count = grq_es.get_count(index=idx, body=query)
    logger.info("count: {}/{}".format(count, expected_count))
    if count == expected_count:
        return True
    raise RuntimeError


def check_datasets(dataset, crid, f):
    """Check for expected count of datasets."""
    global total_count

    ds = dataset["dataset"]
    version = dataset["system_version"]
    expected_count = int(dataset["count"])

    query = {"query": {"bool": {}}}
    values = version.split(",")
    condition = []

    for value in values:
        if value == "CRID_VAL":
            value = crid
        term = {"term": {"system_version.keyword": value}}
        condition.append(term)

    term = {"term": {"dataset.keyword": ds}}
    condition.append(term)

    query["query"]["bool"]["must"] = condition

    index = "grq"
    all_found = False

    logger.info("index: {}".format(index))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    try:
        all_found = check_count(query, index, expected_count)
        total_count += expected_count
    except Exception as e:
        logger.error(str(e))
    if all_found:
        msg = "SUCCESS: Found {} expected {} datasets of version '{}'.\n".format(
            expected_count, ds, version
        )
    else:
        msg = "ERROR: Failed to find {} expected {} datasets of version '{}'.\n".format(
            expected_count, ds, version
        )
    logger.info(msg)
    f.write(msg)
    if not all_found:
        raise RuntimeError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("dataset_file", help="dataset json file containing all the info")
    parser.add_argument("data_segment", help="dataset segment(s)")
    parser.add_argument("res_file", help="result file")
    parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
    parser.add_argument("--max_time", type=int, default=1800, help="maximum total time")
    parser.add_argument("--crid", default=None, help="crid value")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    datasets_file = os.path.join(BASE_DIR, args.dataset_file)
    logger.info("datasets_file : {}".format(datasets_file))
    with open(datasets_file) as f:
        data = json.load(f)
    print(json.dumps(data, indent=4))
    segments = args.data_segment.split(",")
    crid = args.crid

    if not crid:
        raise Exception("Crid value must be supplied")

    with open(args.res_file, "w") as f:
        for segment in segments:
            for dataset in data["datasets"][segment]:
                check_datasets(dataset, crid, f)
    logger.info("total found count : {}".format(total_count))
