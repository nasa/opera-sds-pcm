#!/usr/bin/env python3

"""Drives DISP-NI historical processing from gslc_query_hist batch procs.

For each enabled gslc_query_hist batch proc in the GRQ batch_proc index, this walks every frame's
sensing dates from the DISP-NI frame database k at a time and submits one gslc_query_hist job per
k-set. A k-set is submitted only once the compressed GSLCs of the k-sets before it (up to m - 1)
are cataloged, because the DISP-NI job for it consumes them; until then the frame's cursor stays
put, and the time it first blocked is recorded in frame_blocked_since so a frame that will never
be unblocked (its previous DISP-NI job failed) is visible in `pcm_batch.py view`.

This is the un-phased walk of run_disp_s1_historical_processing.py: no phases, forward blocks or
blackout dates. Leftover dates at the end of a frame that do not fill a k-set are not processed.
"""

import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import requests

from opera_commons.es_connection import get_grq_es
from data_subscriber import cslc_utils
from data_subscriber.gslc import gslc_utils
from data_subscriber.gslc.gslc_dependency import GSLCDependency
from util.conf_util import SettingsConf

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = "batch_proc"
JOB_TYPE = "gslc_query_hist"
WINDOW_MARGIN = timedelta(minutes=30)

logging.basicConfig(level="INFO",
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("DISP-NI-HISTORICAL")

# Set from the cluster's .sds config when run as a script
JOB_RELEASE = None
JOB_SUBMIT_URL = None


@dataclass
class KSetAction:
    """What to do with one frame's k-set this round."""
    submit: bool
    next_position: int
    finished: bool = False
    blocked: bool = False      # waiting on compressed GSLCs of earlier k-sets
    job_name: str = None
    job_spec: str = None
    job_params: dict = field(default_factory=dict)
    tags: list = field(default_factory=list)


def proc_once(eu, procs, args, frame_db, now=None):
    '''Run one round over every batch proc. Returns False if any job submission failed.'''

    job_success = True
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)

    for proc in procs:
        doc_id = proc['_id']
        p = SimpleNamespace(**proc['_source'])

        if p.enabled is False or p.job_type != JOB_TYPE:
            continue

        if "frame_states" not in vars(p):
            p.frame_states = generate_initial_frame_states(p.frames, frame_db)

        last_run_date = datetime.strptime(getattr(p, "last_run_date", "2000-01-01T00:00:00"), ES_DATETIME_FORMAT)
        if last_run_date + timedelta(minutes=p.wait_between_acq_cycles_mins) > now:
            continue

        if not args.dry_run:
            update_batch_proc(eu, doc_id, last_run_date=now.strftime(ES_DATETIME_FORMAT))

        previous_blocked = getattr(p, "frame_blocked_since", None) or {}
        blocked_since = {frame: since for frame, since in previous_blocked.items() if since is not None}

        proc_finished = True
        for frame_key, position in p.frame_states.items():
            frame_key = str(frame_key)
            action = form_job_params(p, int(frame_key), position, eu, frame_db)

            if action.blocked:
                blocked_since.setdefault(frame_key, now.strftime(ES_DATETIME_FORMAT))

            if action.submit:
                logger.info(f"Submitting query job for {p.label} frame {frame_key}: {action.job_params['start_datetime']} "
                            f"{action.job_params['end_datetime']} {action.job_params['m']}")
                logger.info(action.job_params)

                if not args.dry_run:
                    try:
                        job_id = submit_job(action.job_name, action.job_spec, action.job_params, p.job_queue,
                                            action.tags)
                    except Exception as e:
                        logger.error(f"Job submission failed for {action.job_name}: {e}")
                        job_success = False
                        proc_finished = False
                        continue

                    logger.info(f"Job submitted successfully. Job ID: {job_id}")
                    blocked_since.pop(frame_key, None)
            elif action.next_position != position:
                # A k-set before data_start_date: nothing to submit, so step past it
                logger.info(f"Skipping frame {frame_key} k-set at position {position}: before data_start_date")

            proc_finished = proc_finished and action.finished

            if action.next_position != position and not args.dry_run:
                p.frame_states[frame_key] = action.next_position
                update_batch_proc(eu, doc_id, frame_states=p.frame_states)

        if args.dry_run:
            continue

        data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
        known_frame_states = {frame: state for frame, state in p.frame_states.items() if int(frame) in frame_db}
        progress_percentage, frame_completion, last_processed_datetimes = \
            cslc_utils.calculate_historical_progress(known_frame_states, data_end_date, frame_db, p.k)

        # Only whole k-sets are processed, so a finished batch proc can have a few dates left over
        if proc_finished:
            progress_percentage = 100

        update_batch_proc(eu, doc_id,
                          frame_blocked_since=with_tombstones(blocked_since, previous_blocked),
                          progress_percentage=progress_percentage,
                          frame_completion_percentages=frame_completion,
                          last_processed_datetimes=last_processed_datetimes)

        if proc_finished:
            logger.info(f"{p.label} Batch Proc completed processing. It is now disabled")
            update_batch_proc(eu, doc_id, enabled=False)

    return job_success


def update_batch_proc(eu, doc_id, **fields):
    '''Merge fields into a batch proc document.'''

    eu.update_document(id=doc_id, body={"doc_as_upsert": True, "doc": fields}, index=ES_INDEX)


def with_tombstones(current, previous):
    '''Return the per-frame map to write so that entries dropped from it actually disappear.

    The document update merges objects instead of replacing them, so a cleared entry is written as null.'''

    return {**{key: None for key in previous if key not in current}, **current}


def form_job_params(p, frame_id, position, eu, frame_db) -> KSetAction:
    '''Decide what to do with the frame's k-set starting at this position.'''

    frame = frame_db.get(frame_id)
    if frame is None:
        logger.error(f"Frame {frame_id} is not in the DISP-NI frame database. Treating it as finished.")
        return KSetAction(submit=False, next_position=position, finished=True)

    sensing_datetimes = frame.sensing_datetimes
    if position + p.k > len(sensing_datetimes):
        logger.info(f"Frame {frame_id} has {len(sensing_datetimes) - position} sensing dates left, fewer than "
                    f"k={p.k}. It has reached the end of historical processing.")
        return KSetAction(submit=False, next_position=position, finished=True)

    s_date = sensing_datetimes[position] - WINDOW_MARGIN
    e_date = sensing_datetimes[position + p.k - 1] + WINDOW_MARGIN
    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)

    if e_date > data_end_date + WINDOW_MARGIN:
        return KSetAction(submit=False, next_position=position, finished=True)

    next_position = position + p.k
    finished = next_position + p.k > len(sensing_datetimes)

    if s_date < data_start_date:
        return KSetAction(submit=False, next_position=next_position, finished=finished)

    dependency = GSLCDependency(p.k, p.m, frame_db, logger)
    try:
        satisfied = dependency.compressed_gslc_satisfied(frame_id, position, eu)
    except Exception as e:
        logger.error(f"Error checking compressed GSLCs for frame {frame_id} at position {position}: {e}")
        satisfied = False

    if not satisfied:
        # TODO(OPERA-2673): ICS 2.6.2.1 stops the frame and reports an error when a compressed GSLC will never
        #  be produced, e.g. because its DISP-NI job failed. Detect that once the SCIFLO job name is defined.
        logger.info(f"Compressed GSLCs not satisfied for frame {frame_id} at position {position}. "
                    f"Skipping now but will be retried in the future.")
        return KSetAction(submit=False, next_position=position, blocked=True)

    return KSetAction(
        submit=True,
        next_position=next_position,
        finished=finished,
        job_name=f"data-subscriber-query-timer-{p.label}_f{frame_id}-{s_date.strftime(ES_DATETIME_FORMAT)}-"
                 f"{e_date.strftime(ES_DATETIME_FORMAT)}",
        job_spec=f"job-{p.job_type}:{JOB_RELEASE}",
        job_params=build_query_job_params(p, frame_id, s_date, e_date, dependency.m_for_position(position)),
        tags=["data-subscriber-query-timer", "historical_processing"],
    )


def build_query_job_params(p, frame_id, s_date, e_date, m):
    '''Build the parameters of a gslc_query_hist job for one k-set window.'''

    job_params = {
        "start_datetime": f"--start-date={convert_datetime(s_date)}",
        "end_datetime": f"--end-date={convert_datetime(e_date)}",
        "endpoint": "--endpoint=OPS",
        "bounding_box": "",
        "download_job_queue": f"--job-queue={p.download_job_queue}",
        "download_job_release": f"--release-version={JOB_RELEASE}",
        "chunk_size": f"--chunk-size={p.chunk_size}",
        "processing_mode": "--processing-mode=historical",
        "frame_id": f"--frame-id={frame_id}",
        "smoke_run": "",
        "dry_run": "",
        "no_schedule_download": "--no-schedule-download" if getattr(p, "no_schedule_download", False) is True else "",
        "use_temporal": "--use-temporal",
        "k": f"--k={p.k}",
        "m": f"--m={m}",
        "provider": "--provider=ASF",
    }

    if len(getattr(p, "include_regions", "").strip()) > 0:
        job_params["include_regions"] = f"--include-regions={p.include_regions}"

    if len(getattr(p, "exclude_regions", "").strip()) > 0:
        job_params["exclude_regions"] = f"--exclude-regions={p.exclude_regions}"

    return job_params


def submit_job(job_name, job_spec, job_params, queue, tags, priority=0):
    """Submit job to mozart via REST API. Returns the job id."""

    params = {
        "queue": queue,
        "priority": priority,
        "tags": json.dumps(tags),
        "type": job_spec,
        "params": json.dumps(job_params),
        "name": job_name,
    }

    logger.info(f"Job params: {json.dumps(params)}")
    logger.info(f"Job URL: {JOB_SUBMIT_URL}")
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)
    logger.info(f"Request code: {req.status_code}")
    logger.info(f"Request text: {req.text}")

    req.raise_for_status()
    result = req.json()

    if result.get("success") is True and "result" in result:
        logger.info(f"submitted job: {job_spec} job_id: {result['result']}")
        return result["result"]

    raise Exception(f"job not submitted successfully: {result}")


def generate_initial_frame_states(frames, frame_db):
    '''Map every frame of the batch proc to position 0 of its sensing dates.

    Frames are a list of frame numbers or inclusive [start, end] ranges.'''

    frame_states = {}

    for frame in cslc_utils.expand_batch_proc_frames(frames):
        if frame not in frame_db:
            logger.warning(f"Frame number {frame} does not exist in the DISP-NI frame database.")
        frame_states[str(frame)] = 0

    return frame_states


def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", dest="verbose", required=False, default=False, action="store_true",
                        help="If true, print out verbose information, mainly INFO logs from elasticsearch module")
    parser.add_argument("--sleep-secs", dest="sleep_secs", help="Sleep between running for a cycle in seconds",
                        required=False, default=60)
    parser.add_argument("--dry-run", dest="dry_run", required=False, default=False, action="store_true",
                        help="If true, log the jobs that would be submitted but submit nothing and write nothing "
                             "to the batch procs")

    args = parser.parse_args()

    eu_logger = logging.getLogger("disp_ni_historical")
    eu_logger.setLevel(logging.INFO)

    # Suppress all logs from elasticsearch except for warnings and errors if not in verbose mode
    if not args.verbose:
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)
        eu_logger.setLevel(logging.WARNING)

    SETTINGS = SettingsConf(file=str(Path("/export/home/hysdsops/.sds/config"))).cfg
    JOB_RELEASE = SETTINGS["STAGING_AREA"]["JOB_RELEASE"]
    JOB_SUBMIT_URL = f"https://{SETTINGS['MOZART_PVT_IP']}/mozart/api/v0.1/job/submit?enable_dedup=false"

    disp_ni_frame_db = gslc_utils.localize_disp_ni_frame_db()
    eu = get_grq_es(eu_logger)

    while True:
        batch_procs = eu.query(index=ES_INDEX)
        proc_once(eu, batch_procs, args, disp_ni_frame_db)
        time.sleep(int(args.sleep_secs))
