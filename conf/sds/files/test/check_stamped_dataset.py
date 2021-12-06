#!/usr/bin/env python
"""Verification and validation of stamped datasets in end-to-end integration test."""

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


def check_status(query, idx, dataset_id, delivered_keys):
    """Query ES index."""
    results = grq_es.search(index=idx, body=query)
    results = results['hits']['hits']

    count = len(results)
    if count > 0:
        ds_info = results[0]
        pass_flag = True
        for key in delivered_keys:
            if key not in ds_info["_source"]:
                logger.error("Missing {} from metadata of {}".format(key, dataset_id))
                pass_flag = False
        return pass_flag
    else:
        logger.error("ERROR: Dataset does not exist in ES: {}".format(dataset_id))
        return False


@backoff.on_exception(
    backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time
)
def check_datasets(dataset_ids, delivered_keys, res_file):
    index = "grq"
    status = dict()

    logger.info("index: {}".format(index))

    for dataset_id in dataset_ids:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "_id": dataset_id
                            }
                        }
                    ]
                }
            },
            "_source": {"includes": delivered_keys},
        }
        logger.info("query: {}".format(json.dumps(query, indent=2)))
        found_status = check_status(query, index, dataset_id, delivered_keys)
        status[dataset_id] = found_status

    overall_status = True
    for k in status:
        overall_status &= status[k]

    with open(res_file, "w") as out_f:
        if overall_status:
            msg = "SUCCESS: Found {} in datasets {}.\n".format(
                delivered_keys, dataset_ids
            )
        else:
            msg = "ERROR: Failed to find {} in datasets {}.\n".format(
                delivered_keys, dataset_ids
            )
        logger.info(msg)
        out_f.write(msg)
        if not overall_status:
            raise RuntimeError(msg)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("dataset_file", help="dataset file")
        parser.add_argument("delivered_key", help="delivered key")
        parser.add_argument("res_file", help="result file")
        parser.add_argument(
            "--max_value", type=int, default=64, help="maximum backoff time"
        )
        parser.add_argument(
            "--max_time", type=int, default=900, help="maximum total time"
        )

        args = parser.parse_args()

        BACKOFF_CONF["max_value"] = args.max_value
        BACKOFF_CONF["max_time"] = args.max_time

        with open(args.dataset_file, "r") as f:
            dataset_ids = json.load(f)

        if len(dataset_ids) > 0:
            delivery_keys = [args.delivered_key, "daac_catalog_id"]
            check_datasets(dataset_ids, delivery_keys, args.res_file)
        else:
            raise RuntimeError("No dataset id read from file: {}".format(args.dataset_file))

    except Exception as e:
        raise RuntimeError(e)
