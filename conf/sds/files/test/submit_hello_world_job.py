#!/usr/bin/env python

"""
Python script to submit a hello world job
"""

import logging
import sys
from hysds_commons.job_utils import submit_mozart_job

# this python code needs to query ES with job_id
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")


def submit_to_mozart(pge, release_version):
    rule = {
        "rule_name": "trigger_{}".format(pge),
        "queue": "opera-job_worker-small",
        "priority": "5",
        "kwargs": "{}",
        "enable_dedup": False
    }

    mozart_job_id = submit_mozart_job(
        {},
        rule,
        hysdsio={
            "id": "internal-temporary-wiring",
            "params": [],
            "job-specification": "{}:{}".format(pge, release_version),
        },
        job_name="job_%s-%s" % (pge, release_version)
    )

    LOGGER.info("Job ID: {}".format(mozart_job_id))
    print("Job ID: {}".format(mozart_job_id))
    return mozart_job_id


def submit_job(release_version):
    submit_to_mozart("job-hello_world", release_version)


if __name__ == "__main__":
    submit_job(sys.argv[1])
