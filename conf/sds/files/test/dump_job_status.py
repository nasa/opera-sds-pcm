#!/usr/bin/env python
"""Dump job status."""

import os
import sys
import logging
import requests
import json
from tabulate import tabulate

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


def dump_job_statuses(mozart_rest_url):
    """Dump all job statuses."""

    # get list of job ids
    r = requests.get("{}/api/v0.1/job/list".format(mozart_rest_url), verify=False)
    r.raise_for_status()
    res = r.json()
    if res["success"] is False:
        raise RuntimeError("ERROR: {}".format(json.dumps(res, indent=2)))
    job_ids = res["result"]

    headers = ["uuid\n---\njob_id\n---\nstatus\n---\nerror\n---\ntraceback"]

    # accumulate job statuses
    status_table = []
    for job_id in job_ids:
        job_url = "{}/api/v0.1/job/info".format(mozart_rest_url)
        r = requests.get(job_url, params={"id": job_id}, verify=False)
        r.raise_for_status()

        res = r.json()
        if res["success"] is False:
            raise RuntimeError("ERROR: {}".format(json.dumps(res, indent=2)))

        job_info = res["result"]
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
    rest_url = sys.argv[1]
    dump_job_statuses(rest_url)
