#!/usr/bin/env python3

import logging

import json
import os
import re
from distutils.util import strtobool
from typing import Dict
import dateutil.parser
import requests
import boto3
from collections import defaultdict

from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
import logging
from data_subscriber import cslc_utils
import argparse
from util.conf_util import SettingsConf
from data_subscriber.cmr import get_cmr_token
from data_subscriber.parser import create_parser

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"
ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Requires these 5 env variables
_ENV_MOZART_IP = "MOZART_IP"
_ENV_GRQ_IP = "GRQ_IP"
_ENV_GRQ_ES_PORT = "GRQ_ES_PORT"
_ENV_ENDPOINT = "ENDPOINT"
_ENV_JOB_RELEASE = "JOB_RELEASE"
_ENV_ANC_BUCKET = "ANC_BUCKET"
ES_INDEX = "batch_proc"

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

CSLC_COLLECTIONS = ["OPERA_L2_CSLC-S1_V1"]

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

def proc_once(eu, dryrun = False):
    procs = eu.query(index=ES_INDEX)  # TODO: query for only enabled docs
    for proc in procs:
        doc_id = proc['_id']
        proc = proc['_source']
        p = SimpleNamespace(**proc)

        # If this batch proc is disabled, continue TODO: this goes away when we change the query above
        if p.enabled == False:
            continue

        # Only process cslc query jobs, which is for DISP-S1 processing
        if p.job_type != "cslc_query":
            continue

        if "frame_list" not in vars(p):
            p.frame_list = generate_initial_frame_states(p.frame)

        print(p.frame_list)

        now = datetime.utcnow()
        if "last_run_date" not in p:
            new_last_run_date = datetime.strptime("1900-01-01T00:00:00", ES_DATETIME_FORMAT)
        else:
            new_last_run_date = datetime.strptime(p.last_run_date, ES_DATETIME_FORMAT) + timedelta(
                minutes=p.wait_between_acq_cycles_mins)

        # If it's not time to run yet, just continue
        if new_last_run_date > now:
            continue

        # Update last_run_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_run_date": now.strftime(ES_DATETIME_FORMAT), }},
                           index=ES_INDEX)

        # Compute job parameters
        job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
            form_job_params(p)

        # See if we've reached the end of this batch proc. If so, disable it.
        if finished:
            print(p.label, "Batch Proc completed processing. It is now disabled")
            eu.update_document(id=doc_id,
                               body={"doc_as_upsert": True,
                                     "doc": {
                                         "enabled": False, }},
                               index=ES_INDEX)
            continue

        # update last_attempted_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_attempted_proc_data_date": last_proc_date, }},
                           index=ES_INDEX)

        # TODO: we need to update proc_frame info
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_attempted_proc_frame": last_proc_frame, }},
                           index=ES_INDEX)

        # submit mozart job
        print("Submitting query job for", p.label,
              "with start date", job_params["start_datetime"].split("=")[1],
              "and end date", job_params["end_datetime"].split("=")[1])
        if (last_proc_frame is not None):
            print("Last proc frame", last_proc_frame)

        if not dryrun:
            job_success = submit_job(job_name, job_spec, job_params, p.job_queue, job_tags)
        else:
            job_success = True

        # Update last_successful_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_successful_proc_data_date": last_proc_date, }},
                           index=ES_INDEX)

        # If we are processing CSLC we need to update proc_frame info. last_proc_frame was defined earlier
        if "frame_range" in job_params:
            eu.update_document(id=doc_id,
                               body={"doc_as_upsert": True,
                                     "doc": {
                                         "last_successful_proc_frame": last_proc_frame, }},
                               index=ES_INDEX)

        return job_success

def form_job_params(p, disp_frame_map):
    finished = False
    end_point = ENDPOINT
    download_job_queue = p.download_job_queue
    try:
        if p.temporal is True:
            temporal = True
        else:
            temporal = False
    except:
        print("Temporal parameter not found in batch proc. Defaulting to false.")
        temporal = False

    processing_mode = p.processing_mode
    if p.processing_mode == "historical":
        temporal = True  # temporal is always true for historical processing

    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
    frame_range = ""
    last_proc_date = None
    last_proc_frame = None

    # Start date time is when the last successful process data time.
    # If this is before the data start time, which may be the case when this batch_proc is first run,
    # change it to the data start time.
    s_date = datetime.strptime(p.last_successful_proc_data_date, ES_DATETIME_FORMAT)
    if s_date < data_start_date:
        s_date = data_start_date

    # For CSLC input data, which is for DISP-S1 production, we need to do perform more logic
    # Frame numbers are 1-based and inclusive on both ends of the range
    if p.collection_short_name in CSLC_COLLECTIONS:

        sorted_frame_list = sorted(disp_frame_map.keys())
        max_frame = sorted_frame_list[-1]

        try:
            last_frame = p.last_successful_proc_frame
        except Exception:
            last_frame = sorted_frame_list[0]

        # For CSLC only we add some time buffer because sensing time varies by a few minutes for each burst within the frame
        # and each acquisition cycle is at least 6 days
        s_date = s_date + timedelta(minutes=30)

        # If the last processed time is the end of the time series for this frame, it's time to go to the next frame
        if s_date > disp_frame_map[last_frame].sensing_datetimes[-1]:
            frame_id = next_disp_frame(last_frame, sorted_frame_list)
            s_date = data_start_date
        else:
            frame_id = last_frame

        frame_range = f'--frame-range={frame_id},{frame_id}'

        # For CSLC historical processing we increment the data time by k number of sensing dates
        # If there isn't enough date to make K, we are done for this batch proc completely
        e_date = s_date + timedelta(days=p.k * CSLC_DAYS_PER_COLLECTION_CYCLE)
        if e_date > data_end_date:
            e_date = s_date = s_date - timedelta(days=p.k * CSLC_DAYS_PER_COLLECTION_CYCLE)
            last_proc_date = s_date
            last_proc_frame = max_frame
            finished = True
        else:
            last_proc_date = s_date
            last_proc_frame = frame_id

    else:
        raise RuntimeError("Unknown collection %s ." % p.collection_short_name)

    job_spec = "job-%s:%s" % (p.job_type, JOB_RELEASE)
    job_params = {
        "start_datetime": f"--start-date={convert_datetime(s_date)}",
        "end_datetime": f"--end-date={convert_datetime(e_date)}",
        "endpoint": f'--endpoint={end_point}',
        "bounding_box": "",
        "download_job_release": f'--release-version={JOB_RELEASE}',
        "download_job_queue": f'--job-queue={download_job_queue}',
        "chunk_size": f'--chunk-size={p.chunk_size}',
        "processing_mode": f'--processing-mode={processing_mode}',
        "frame_range": frame_range,
        "smoke_run": "",
        "dry_run": "",
        "no_schedule_download": "",
        "use_temporal": f'--use-temporal' if temporal is True else ''
    }

    # Add include and exclude regions
    includes = p.include_regions
    if len(includes.strip()) > 0:
        job_params["include_regions"] = f'--include-regions={includes}'

    excludes = p.exclude_regions
    if len(excludes.strip()) > 0:
        job_params["exclude_regions"] = f'--exclude-regions={excludes}'

    if p.collection_short_name in CSLC_COLLECTIONS:
        job_params["k"] = f"--k={p.k}"
        job_params["m"] = f"--m={p.m}"

    tags = ["data-subscriber-query-timer"]
    if processing_mode == 'historical':
        tags.append("historical_processing")
    else:
        tags.append("batch_processing")
    job_name = "data-subscriber-query-timer-{}_{}-{}".format(p.label, s_date.strftime(ES_DATETIME_FORMAT),
                                                             e_date.strftime(ES_DATETIME_FORMAT))

    return job_name, job_spec, job_params, tags, last_proc_date, last_proc_frame, finished

def submit_job(job_name, job_spec, job_params, queue, tags, priority=0):
    """Submit job to mozart via REST API."""

    # setup params
    params = {
        "queue": queue,
        "priority": priority,
        "tags": json.dumps(tags),
        "type": job_spec,
        "params": json.dumps(job_params),
        "name": job_name,
    }

    # submit job
    print("Job params: %s" % json.dumps(params))
    print("Job URL: %s" % JOB_SUBMIT_URL)
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)

    print("Request code: %s" % req.status_code)
    print("Request text: %s" % req.text)

    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    print("Request Result: %s" % result)

    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            print("submitted job: %s job_id: %s" % (job_spec, job_id))
            return job_id
        else:
            print("job not submitted successfully: %s" % result)
            raise Exception("job not submitted successfully: %s" % result)
    else:
        raise Exception("job not submitted successfully: %s" % result)

def generate_initial_frame_states(frames):
    '''frame_list is a list of frame number or a range of frame numbers'''

    frame_states = []

    for frame in frames:
        if type(frame) == list:
            if len(frame) != 2:
                raise ValueError("Frame range must have two elements")
            if frame[0] > frame[1]:
                raise ValueError("Frame range must be in ascending order")

            for f in range(frame[0], frame[1] + 1):
                if f not in disp_burst_map.keys():
                    logger.warning(f"Frame number {f} does not exist. Skipping.")
                frame_states.append({f: 0})

        else:
            if frame not in disp_burst_map.keys():
                logger.warning(f"Frame number {frame} does not exist. Skipping.")
            frame_states.append({frame: 0})

    return frame_states


def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)

def get_elasticsearch_utility():
    for ev in [_ENV_MOZART_IP, _ENV_GRQ_IP, _ENV_ENDPOINT, _ENV_JOB_RELEASE, _ENV_GRQ_ES_PORT]:
        if ev not in os.environ:
            raise RuntimeError("Need to specify %s in environment." % ev)
    MOZART_IP = os.environ[_ENV_MOZART_IP]
    GRQ_IP = os.environ[_ENV_GRQ_IP]
    GRQ_ES_PORT = os.environ[_ENV_GRQ_ES_PORT]
    ENDPOINT = os.environ[_ENV_ENDPOINT]
    JOB_RELEASE = os.environ[_ENV_JOB_RELEASE]
    ANC_BUCKET = os.environ[_ENV_ANC_BUCKET]

    MOZART_URL = 'https://%s/mozart' % MOZART_IP
    JOB_SUBMIT_URL = "%s/api/v0.1/job/submit?enable_dedup=false" % MOZART_URL


    eu = ElasticsearchUtility('http://%s:%s' % (GRQ_IP, str(GRQ_ES_PORT)), logger)

    return eu

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", dest="verbose",
                        help="If true, print out verbose information, mainly cmr queries and k-cycle calculation.",
                        required=False, default=False)
    parser.add_argument("--sleep-secs", dest="sleep_secs", help="Sleep between running for a cycle in seconds",
                        required=False, default=1)
    parser.add_argument("--dry-run", dest="dry_run", help="If true, do not submit jobs", required=False, default=False)

    args = parser.parse_args()

    eu = get_elasticsearch_utility()

    while (True):
        print(eu, proc_once(args.dry_run))
        time.sleep(args.sleep_secs)