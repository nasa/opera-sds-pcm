#!/usr/bin/env python3

'''List and show all pending jobs'''

import logging
import sys

from data_subscriber import es_conn_util
from data_subscriber.cslc_utils import get_pending_download_jobs
from util.exec_util import exec_wrapper

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

@exec_wrapper
def main():
    run(sys.argv)

def run(argv: list[str]):
    es = es_conn_util.get_es_connection(logger)

    # Get unsubmitted jobs from Elasticsearch GRQ
    unsubmitted = get_pending_download_jobs(es)
    print(f"Found {len(unsubmitted)=} Pending CSLC Download Jobs")

    # For each of the unsubmitted jobs, check if their compressed cslcs have been generated
    count = 1
    for job in unsubmitted:
        s = job['_source']
        print("%05d" % count, "Type:", s['job_type'], ", Queue:", s['job_queue'], ", Job Params:", [f"{f['value']}" for f in s['job_params']])
if __name__ == "__main__":
    main()
