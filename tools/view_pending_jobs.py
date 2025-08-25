#!/usr/bin/env python3

'''List and show all pending jobs'''

import logging
import sys

from data_subscriber import es_conn_util
from data_subscriber.cslc_utils import get_pending_download_jobs
from util.exec_util import exec_wrapper
from data_subscriber.cslc_utils import PENDING_TYPE_CSLC_DOWNLOAD
from data_subscriber.dist_s1_utils import PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

@exec_wrapper
def main():
    run(sys.argv)

def run(argv: list[str]):
    es = es_conn_util.get_es_connection(logger)

    # Get unsubmitted jobs from Elasticsearch GRQ
    unsubmitted = get_pending_download_jobs(es)
    print(f"Found {len(unsubmitted)=} Pending Jobs")

    # For each of the unsubmitted jobs, check if their conditions have been met
    for count, job in enumerate(unsubmitted):
        s = job['_source']
        if s['job_type'] == PENDING_TYPE_CSLC_DOWNLOAD:
            print("%05d" % count, "Type:", s['job_type'], ", Queue:", s['job_queue'], "Submitted:", s['submitted'], "k=%d" % s["k"] if "k" in s else "", "m=%d" % s["m"] if "m" in s else "",  ", Job Params:", [f"{f['value']}" for f in s['job_params']])
        elif s['job_type'] == PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD:
            print("%05d" % count, "Type:", s['job_type'], ", Queue:", s['job_queue'], "Submitted:", s['submitted'], "Previous Tile Job ID:", s['previous_tile_job_id'], "Download Batch ID:", s['download_batch_id'])#, ", Job Params:", [f"{f['value']}" for f in s['job_params']])

    print(f"Found {len(unsubmitted)=} Pending Jobs")
if __name__ == "__main__":
    main()
