#!/usr/bin/env python
"""Checks for expected job failures with a given error message."""

import json
import backoff
import argparse

from hysds.es_util import get_mozart_es
from commons.logger import logger


BACKOFF_CONF = {}  # back-off configuration

mozart_es = get_mozart_es()


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def check_count(query, expected_count):
    """Query Mozart index."""
    count = mozart_es.get_count(index="job_status-current", body=query)
    logger.info("count: {}/{}".format(count, expected_count))
    if count == expected_count:
        return True
    raise RuntimeError


def check_expected_failure(error_message, job_tag, expected_count, res_file):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "default_operator": "AND",
                            "query": "{}".format(error_message)
                        }
                    },
                    {
                        "match": {
                            "tags": job_tag
                        }
                    }
                ]
            }
        }
    }
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    all_found = False
    try:
        all_found = check_count(query, expected_count)
    except Exception as e:
        logger.error(str(e))

    with open(res_file, "w") as out_f:
        if all_found:
            msg = "SUCCESS: Found {} expected job failures with error message: {}.\n".format(expected_count,
                                                                                             error_message)
        else:
            msg = "ERROR: Failed to find {} job failures with error message: {}.\n".format(expected_count,
                                                                                           error_message)
        logger.info(msg)
        out_f.write(msg)
        if not all_found:
            raise RuntimeError(msg)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("error_message", help="expected error message")
        parser.add_argument("expected_count", type=int, help="expected count")
        parser.add_argument("res_file", help="result file")
        parser.add_argument("--job_tag", help="job tag associated with expected error message")
        parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
        parser.add_argument("--max_time", type=int, default=900, help="maximum total time")

        args = parser.parse_args()

        BACKOFF_CONF["max_value"] = args.max_value
        BACKOFF_CONF["max_time"] = args.max_time

        check_expected_failure(args.error_message, args.job_tag, args.expected_count, args.res_file)

    except Exception as e:
        raise RuntimeError(e)
