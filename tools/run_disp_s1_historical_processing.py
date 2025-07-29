#!/usr/bin/env python3

import logging
import json
from pathlib import Path
import requests
from types import SimpleNamespace
import time
from datetime import datetime, timedelta
from opera_commons.es_connection import get_grq_es
from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_dependency import CSLCDependency
from data_subscriber.cslc.cslc_blackout import DispS1BlackoutDates, localize_disp_blackout_dates
import argparse
from util.conf_util import SettingsConf

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"
ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

_ENV_GRQ_ES_PORT = "GRQ_ES_PORT"
_ENV_ENDPOINT = "ENDPOINT"
_ENV_JOB_RELEASE = "JOB_RELEASE"
ES_INDEX = "batch_proc"
JOB_TYPE = "cslc_query_hist"

logging.basicConfig(level="INFO",
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("DISP-S1-HISTORICAL")

CSLC_COLLECTION = "OPERA_L2_CSLC-S1_V1"

def proc_once(eu, procs, args):
    dryrun = args.dry_run
    job_success = True

    for proc in procs:
        doc_id = proc['_id']
        proc = proc['_source']
        p = SimpleNamespace(**proc)

        # If this batch proc is disabled, continue TODO: this goes away when we change the query above
        if p.enabled == False:
            continue

        # Only process cslc query jobs, which is for DISP-S1 processing
        if p.job_type != JOB_TYPE:
            continue

        if "frame_states" not in vars(p):
            p.frame_states = generate_initial_frame_states(p.frames)

        now = datetime.utcnow()
        if "last_run_date" not in vars(p):
            p.last_run_date = "2000-01-01T00:00:00"
        new_last_run_date = (datetime.strptime(p.last_run_date, ES_DATETIME_FORMAT) +
                             timedelta(minutes=p.wait_between_acq_cycles_mins))

        # If it's not time to run yet, just continue
        if new_last_run_date > now:
            continue

        # Update last_run_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_run_date": now.strftime(ES_DATETIME_FORMAT), }},
                           index=ES_INDEX)

        proc_finished = True # It's actually false here but need to set it to True for the boolean logic to work
        for frame_id, last_frame_processed in p.frame_states.items():
            logger.info(f"{frame_id=}, {last_frame_processed=}")

            # If the last_frame_processed is the same as the length of all sensing times, we'd already completed processing
            if last_frame_processed == len(disp_burst_map[frame_id].sensing_datetimes):
                finished = True
                do_submit = False
            else:
                # Compute job parameters, whether to process or not, and if we're finished
                do_submit, job_name, job_spec, job_params, job_tags, next_frame_pos, finished = \
                    form_job_params(p, int(frame_id), last_frame_processed, args, eu)

            proc_finished = proc_finished & finished # All frames must be finished for this batch proc to be finished

            # submit mozart job
            if do_submit:
                logger.info(f"Submitting query job for {p.label} {frame_id=} with start date \
{job_params['start_datetime'].split('=')[1]} and end date {job_params['end_datetime'].split('=')[1]}")
                logger.info(job_params)

                if dryrun:
                    job_success = True
                else:
                    job_id = submit_job(job_name, job_spec, job_params, p.job_queue, job_tags)
                    if job_id is False:
                        job_success = False
                    else:
                        logger.info("Job submitted successfully. Job ID: %s" % job_id)
                    job_success = job_success & job_success

                if job_success:
                    p.frame_states[frame_id] = next_frame_pos
                    eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": { "frame_states": p.frame_states, }},
                           index=ES_INDEX)

                    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
                    progress_percentage, frame_completion, last_processed_datetimes \
                        = cslc_utils.calculate_historical_progress(p.frame_states, data_end_date, disp_burst_map, p.k)

                    # If we've finshed the frame, then set the progress percentage to 100. Because we process only full k-sets,
                    # it's possible to be finished when there are a few datetimes left in which case the progress percentage
                    # would be less than 100
                    if finished is True:
                        progress_percentage = 100
                    eu.update_document(id=doc_id,
                                       body={"doc_as_upsert": True,
                                             "doc": {"progress_percentage": progress_percentage,
                                                     "frame_completion_percentages": frame_completion,
                                                     "last_processed_datetimes": last_processed_datetimes, }},
                                       index=ES_INDEX)

                else:
                    logger.error("Job submission failed for %s" % job_name)

        if proc_finished:
            # See if we've reached the end of this batch proc. If so, disable it.
            logger.info(f"{p.label} Batch Proc completed processing. It is now disabled")
            eu.update_document(id=doc_id,
                               body={"doc_as_upsert": True,
                                     "doc": {
                                         "enabled": False, }},
                               index=ES_INDEX)

        # Update last job run time. This is on a per batch_proc basis
        if job_success is True:
            eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_run_date": now.strftime(ES_DATETIME_FORMAT), }},
                           index=ES_INDEX)

    return job_success

def form_job_params(p, frame_id, sensing_time_position_zero_based, args, eu):

    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)

    do_submit = True
    finished = False
    download_job_queue = p.download_job_queue
    try:
        if p.temporal is True:
            temporal = True
        else:
            temporal = False
    except:
        temporal = True
        logger.info(f"Temporal parameter not found in batch proc. Defaulting to {temporal}.")

    processing_mode = p.processing_mode
    if p.processing_mode == "historical":
        temporal = True  # temporal is always true for historical processing

    frame_sensing_datetimes = disp_burst_map[frame_id].sensing_datetimes

    '''start and end data datetime is basically 1 hour window around the total k frame sensing time window.
    TRICKY! the sensing time position is in user-friendly 1-based index, but we need to use 0-based index in code'''
    try:
        logger.info(f"Attempting to process frame {frame_id} at sensing time position {sensing_time_position_zero_based}")
        s_date = frame_sensing_datetimes[sensing_time_position_zero_based] - timedelta(minutes=30)
    except IndexError:
        finished = True
        do_submit = False
        s_date = datetime.strptime("2000-01-01T00:00:00", ES_DATETIME_FORMAT)
        logger.info(f"{frame_id=} reached end of historical processing. No reprocessing needed")

    # If we are outside of the database sensing time range, we are done with this frame
    # Submit reprocessing job for any remainder within this incomplete k-cycle
    try:
        e_date = frame_sensing_datetimes[sensing_time_position_zero_based + p.k - 1] + timedelta(minutes=30)
    except IndexError:
        finished = True
        do_submit = False
        e_date = datetime.strptime("2000-01-01T00:00:00", ES_DATETIME_FORMAT)

        '''
        # Print out all the reprocessing job commands. This is temporary until it can be automated
        # As of Dec 2024, the team's decision is that we will not perform any sub-k historical processing.
        logger.info(f"{frame_id=} reached end of historical processing. The rest of sensing times will be submitted as reprocessing jobs.")
        for i in range(sensing_time_position_zero_based, len(frame_sensing_datetimes)):
            s_date = frame_sensing_datetimes[i] - timedelta(minutes=30)
            e_date = frame_sensing_datetimes[i] + timedelta(minutes=30)
            logger.info(f"python ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c {CSLC_COLLECTION} \
--chunk-size=1 --k={p.k} --m={p.m} --job-queue={p.download_job_queue} --processing-mode=reprocessing --grace-mins=0 \
--start-date={convert_datetime(s_date)} --end-date={convert_datetime(e_date)} --frame-id={frame_id} ")'''

    if s_date < data_start_date:
        do_submit = False
    if e_date > data_end_date:
        do_submit = False
        finished = True

    '''Query GRQ ES for the previous sensing time day index compressed cslc. If this doesn't exist, we can't process
    this frame sensing time yet. So we will not submit job and increment next_sensing_time_position
    
    NOTE! While args, token, cmr, and settings are necessary arguments for CSLCDependency, they will not be used in
    historical processing because all CSLC dependency information is contained in the disp_burst_map'''
    logger.info(f"Checking Compressed CSLC satiety for frame {frame_id} at sensing time position {sensing_time_position_zero_based}")
    try:
        cslc_dependency = CSLCDependency(p.k, p.m, disp_burst_map, None, None, None, None, blackout_dates_obj)
        if cslc_dependency.compressed_cslc_satisfied(frame_id,
                                     disp_burst_map[frame_id].sensing_datetime_days_index[sensing_time_position_zero_based], eu):
            next_sensing_time_position = sensing_time_position_zero_based + p.k
        else:
            do_submit = False
            next_sensing_time_position = sensing_time_position_zero_based
            logger.info("Compressed CSLC not satisfied for frame %s at sensing time position %s. \
    Skipping now but will be retried in the future." % (frame_id, sensing_time_position_zero_based))

    except Exception as e:
        logger.error(f"Error checking compressed cslc satiety for frame {frame_id} at sensing time position {sensing_time_position_zero_based}. Error: {e}")
        do_submit = False
        next_sensing_time_position = sensing_time_position_zero_based

    # If we are at the end of the frame sensing times, we are done with this frame
    if next_sensing_time_position >= len(frame_sensing_datetimes):
        finished = True

    # Create job parameters used to submit query job into Mozart
    # Note that if do_submit is False, none of this is actually used
    job_spec = f"job-{p.job_type}:{JOB_RELEASE}"
    job_params = {
        "start_datetime": f"--start-date={convert_datetime(s_date)}",
        "end_datetime": f"--end-date={convert_datetime(e_date)}",
        "endpoint": f'--endpoint=OPS',
        "bounding_box": "",
        "download_job_queue": f'--job-queue={download_job_queue}',
        "download_job_release": f'--release-version={JOB_RELEASE}', #TODO: remove this after removing from jobspec docker files
        "chunk_size": f'--chunk-size={p.chunk_size}',
        "processing_mode": f'--processing-mode={processing_mode}',
        "frame_id": f"--frame-id={frame_id}",
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

    job_params["k"] = f"--k={p.k}"

    # We need to adjust the m parameter early in the sensing time series
    # For example, if this is the very first k-set, there won't be compressed cslc and therefore m should be 1
    if sensing_time_position_zero_based < p.k * (p.m-1):
        job_params["m"] = f"--m={(sensing_time_position_zero_based // p.k) + 1}"
    else:
        job_params["m"] = f"--m={p.m}"

    tags = ["data-subscriber-query-timer"]
    if processing_mode == 'historical':
        tags.append("historical_processing")
    else:
        tags.append("batch_processing")
    job_name = "data-subscriber-query-timer-{}_f{}-{}-{}".format(p.label, frame_id, s_date.strftime(ES_DATETIME_FORMAT),
                                                             e_date.strftime(ES_DATETIME_FORMAT))

    ''' frame sensing time list position is 1-based index so adding 1 to it'''
    return do_submit, job_name, job_spec, job_params, tags, next_sensing_time_position, finished

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

    return False

def generate_initial_frame_states(frames):
    '''
    Generate initial frame states for historical processing

    Args:
        frames (list): a list of frame number or a range of frame numbers
    Returns:
        frame_states (dict): a dictionary with frame number as key and
        the value is the last processed location in the frame sensing times list
    '''

    frame_states = {}

    for frame in frames:
        if type(frame) == list:
            if len(frame) != 2:
                raise ValueError("Frame range must have two elements")
            if frame[0] > frame[1]:
                raise ValueError("Frame range must be in ascending order")

            for f in range(frame[0], frame[1] + 1):
                if f not in disp_burst_map.keys():
                    logger.warning(f"Frame number {f} does not exist. Skipping.")
                frame_states[f] = 0

        else:
            if frame not in disp_burst_map.keys():
                logger.warning(f"Frame number {frame} does not exist. Skipping.")
            frame_states[frame] = 0

    return frame_states

def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)

if __name__ == "__main__":

    disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist()
    blackout_dates = localize_disp_blackout_dates()
    blackout_dates_obj = DispS1BlackoutDates(blackout_dates, disp_burst_map, burst_to_frames)

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", dest="verbose", required=False, default=False, action="store_true",
                        help="If true, print out verbose information, mainly INFO logs from elasticsearch module... it's a lot!")
    parser.add_argument("--sleep-secs", dest="sleep_secs", help="Sleep between running for a cycle in seconds",
                        required=False, default=60)
    parser.add_argument("--dry-run", dest="dry_run", help="If true, do not submit jobs", required=False, default=False, action="store_true")

    args = parser.parse_args()

    eu_logger = logging.getLogger("disp_s1_historical")
    eu_logger.setLevel(logging.INFO)

    # Suppress all logs from elasticsearch except for warnings and errors if not in verbose mode
    if not args.verbose:
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)
        eu_logger.setLevel(logging.WARNING)

    SETTINGS = SettingsConf(file=str(Path("/export/home/hysdsops/.sds/config"))).cfg
    MOZART_IP = SETTINGS["MOZART_PVT_IP"]
    JOB_RELEASE = SETTINGS["STAGING_AREA"]["JOB_RELEASE"]

    MOZART_URL = 'https://%s/mozart' % MOZART_IP
    JOB_SUBMIT_URL = "%s/api/v0.1/job/submit?enable_dedup=false" % MOZART_URL

    eu = get_grq_es(eu_logger)

    while (True):
        batch_procs = eu.query(index=ES_INDEX)  # TODO: query for only enabled docs
        proc_once(eu, batch_procs, args)
        time.sleep(int(args.sleep_secs))

else:
    BURST_MAP = Path(__file__).parent.parent/ "tests" / "data_subscriber" / "opera-disp-s1-consistent-burst-ids-2025-02-13-2016-07-01_to_2024-12-31.json"
    disp_burst_map, burst_to_frames, datetime_to_frames = cslc_utils.process_disp_frame_burst_hist(BURST_MAP)
    blackout_dates = localize_disp_blackout_dates()
    blackout_dates_obj = DispS1BlackoutDates(blackout_dates, disp_burst_map, burst_to_frames)