#!/usr/bin/env python
"""Dump job status."""

import os
import logging
from tabulate import tabulate
from hysds.es_util import get_mozart_es

log_format = (
    "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"  # set logger
)
logging.basicConfig(format=log_format, level=logging.INFO)

mozart_es = get_mozart_es()


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def query_failed_jobs():
    """
    localhost:9200/job_status-*/_search?q=NOT(status:job-completed)&pretty&_source=uuid,job_id,status,error,traceback
    :param es_url:
    :return:
    """
    query = {
        "_source": {
            "includes": ["uuid", "job_id", "status", "error", "traceback"],
        },
        "query": {
            "bool": {
                "must_not": {
                    "term": {
                        "status": "job-completed"
                    }
                }
            }
        }
    }
    failed_jobs = mozart_es.query(body=query, index="job_status-*")
    return failed_jobs


def dump_job_statuses():
    """Dump all job statuses."""

    # get list of job ids
    failed_jobs = query_failed_jobs()

    headers = ["uuid\n---\njob_id\n---\nstatus\n---\nerror\n---\ntraceback"]

    # accumulate job statuses
    status_table = []
    for job in failed_jobs:
        job_info = job['_source']
        row = "{}\n---\n{}\n---\n{}\n---\n{}\n---\n{}".format(
            job_info["uuid"],
            job_info["job_id"],
            job_info["status"],
            job_info.get("error", ""),
            job_info.get("traceback", ""),
        )
        status_table.append([row])

    tbl = tabulate(status_table, headers=headers, tablefmt="fancy_grid")
    print(tbl)


if __name__ == "__main__":
    dump_job_statuses()
