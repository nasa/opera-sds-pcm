#!/usr/bin/env python3

from __future__ import print_function

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from tabulate import tabulate

from build.lib.hysds.es_util import get_mozart_es
from commons.es_connection import get_mozart_es

from util.conf_util import SettingsConf

from data_subscriber.cslc_utils import localize_disp_frame_burst_hist, get_nearest_sensing_datetime

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('validate_cslc_downloads')
LOGGER.setLevel(logging.INFO)

eu_mzt = get_mozart_es(LOGGER)
LOGGER.info("Connected to %s" % str(eu_mzt.es_url))

if __name__ == '__main__':
    # Process the default disp s1 burst hist file
    frames_to_bursts, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()

    parser = argparse.ArgumentParser()
    parser.add_argument("frame", help="Frame to validate, comma-separated list like \"1,2,3\"")
    parser.add_argument("--k", type=int, help="k parameter for the processing", default=15)
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args(sys.argv[1:])

    frames = [int(f.strip()) for f in args.frame.split(",")]

    all_frames = set(frames)
    job_id_prefixes = {}

    for frame in frames:

        # Determine the latest acq index for this frame that should have been processed
        num_jobs = len(frames_to_bursts[frame].sensing_datetimes) // args.k
        if num_jobs == 0:
            LOGGER.info("No jobs to process for frame %d" % frame)
            all_frames.remove(frame)
            continue

        acq_index = frames_to_bursts[frame].sensing_datetime_days_index[num_jobs * args.k - 1]

        job_id_prefix = f"job-WF-SCIFLO_L3_DISP_S1-frame-{frame}-latest_acq_index-{acq_index}_hist"
        job_id_prefixes[frame] = job_id_prefix

    for frame, job_id_prefix in job_id_prefixes.items():
        if args.verbose:
            logging.info(f"Checking for {job_id_prefix}")
        query = {"query": {"bool": {"must": [{"prefix": {"job_id": job_id_prefix}}]}}}
        sciflo_jobs = eu_mzt.query(body=query, index="job_status*")
        #print(sciflo_jobs)
        if len(sciflo_jobs) == 1: # Don't care the state of the job, just that it exists. Should only ever get one back
            #print(j)
            if args.verbose:
                logging.info(f"Found job {sciflo_jobs[0]['_id']}: {sciflo_jobs[0]['job_id']} for frame {frame}")
            all_frames.remove(frame)

    if all_frames:
        LOGGER.info(f"FAIL: Frames {all_frames} have not been triggered fully throughout the entire historical time period!")
    else:
        LOGGER.info("PASS: All frames have been triggered fully throughout the entire historical time period.")