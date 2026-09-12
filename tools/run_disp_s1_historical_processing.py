#!/usr/bin/env python3

import logging
import json
from dataclasses import dataclass, field
from pathlib import Path
import requests
from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from opera_commons.es_connection import get_grq_es
from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_dependency import CSLCDependency
from data_subscriber.cslc.cslc_blackout import DispS1BlackoutDates, localize_disp_blackout_dates, \
    process_disp_blackout_dates
from data_subscriber.cslc.disp_s1_phases import PhaseKind, PhaseValidationError, phase_for_position
import argparse
from util.conf_util import SettingsConf

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"
ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
SENSING_DATE_FORMAT = "%Y%m%d"

_ENV_GRQ_ES_PORT = "GRQ_ES_PORT"
_ENV_ENDPOINT = "ENDPOINT"
_ENV_JOB_RELEASE = "JOB_RELEASE"
ES_INDEX = "batch_proc"
JOB_TYPE = "cslc_query_hist"

# Forward blocks of a phased batch proc are driven one date at a time through the same job the
# validated serial forward driver uses. "One at a time" is about the CURSOR, not about
# concurrency: a date is done with the walk as soon as its k-cycle state config reaches a
# terminal disposition, and firing is terminal -- the SCIFLO it triggered is not waited
# for. When the CSLCs are already in hand the dispositions resolve fast enough that a whole
# forward block goes out in one poll cycle, leaving N SCIFLOs running at once.
FORWARD_JOB_TYPE = "cslc_catalog_ingest"
KSC_ES_INDEX_PATTERN = "grq_1_disp_s1-kcycle-state-config*"

# Dispositions of a forward date, from the k-cycle state config the cascade writes for it.
FIRE_DISPOSITIONS = ("fire", "fire-boundary")
NO_FIRE_DISPOSITIONS = ("no-fire-superseded", "no-fire-gap", "no-fire-incomplete")

# How long a forward date may sit in flight before the frame is reported as stalled, and how long
# a still-filling k-window must stop changing before the date is called terminal.
DEFAULT_FORWARD_STALL_MINS = 240
DEFAULT_FORWARD_SETTLE_MINS = 30

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

        now = datetime.now(timezone.utc).replace(tzinfo=None)
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

        phased = batch_proc_is_phased(p)
        proc_finished = True # It's actually false here but need to set it to True for the boolean logic to work
        for frame_id, last_frame_processed in p.frame_states.items():
            logger.info(f"{frame_id=}, {last_frame_processed=}")

            # A phased batch proc walks each frame's timeline phase by phase instead of stepping k
            # dates at a time from the beginning of the series
            if phased:
                finished, frame_job_success = proc_phased_frame(
                    eu, doc_id, p, frame_id, last_frame_processed, args, now)
                proc_finished = proc_finished & finished
                job_success = job_success & frame_job_success
                continue

            # If the last_frame_processed is the same as the length of all sensing times, we'd already completed processing
            # NOTE: frame_states keys are strings (from ES JSON) but disp_burst_map is keyed by int. Cast here so the
            # lookup hits the real Frame instead of silently creating an empty defaultdict entry (len 0), which would
            # make a state-0 frame compare 0 == 0 and be wrongly treated as already finished.
            if last_frame_processed == len(disp_burst_map[int(frame_id)].sensing_datetimes):
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

def update_batch_proc(eu, doc_id, **fields):
    '''Merge fields into a batch proc document.'''

    eu.update_document(id=doc_id, body={"doc_as_upsert": True, "doc": fields}, index=ES_INDEX)

def live_entries(mapping):
    '''Read a per-frame map from the batch proc, dropping entries that were cleared.

    Clearing writes a null rather than removing the key, because the document update merges
    objects instead of replacing them.'''

    return {key: value for key, value in (mapping or {}).items() if value is not None}

def with_tombstones(current, previous):
    '''Return the per-frame map to write so that entries dropped from it actually disappear.'''

    return {**{key: None for key in previous if key not in current}, **current}

def proc_phased_frame(eu, doc_id, p, frame_key, position, args, now):
    '''Advance one frame of a phased batch proc by at most one action.

    Returns (finished, job_success). All of the frame's state -- cursor, the forward date in
    flight, the lineage transitions taken, quarantines and stalls -- lives on the batch proc
    document, so a daemon that is killed and restarted picks up exactly where it left off.'''

    frame_id = int(frame_key)
    # Per-frame maps round-trip through JSON, which has no integer keys; a freshly generated
    # frame_states still has them, so normalize before using one as a key
    frame = str(frame_key)
    action = plan_frame_action(p, frame_id, position, eu, now)
    updates = {}

    if args.dry_run:
        # Report what the poll would do and change nothing: writing state here would leave a forward
        # date recorded as in flight that no cascade will ever resolve
        logger.info(f"DRY RUN {frame_id=} phase={action.phase_label or 'n/a'} "
                    f"submit={action.submit} {action.job_spec} next_position={action.next_position} "
                    f"finished={action.finished} quarantine={action.quarantine_reason!r}")
        if action.submit:
            logger.info(action.job_params)
        return action.finished, True

    previous_quarantined = live_entries(getattr(p, "quarantined_frames", None))
    quarantined = dict(previous_quarantined)
    if action.quarantine_reason:
        if quarantined.get(frame) != action.quarantine_reason:
            logger.error(f"{frame_id=} quarantined: {action.quarantine_reason}")
            quarantined[frame] = action.quarantine_reason
            p.quarantined_frames = quarantined
            update_batch_proc(eu, doc_id, quarantined_frames=quarantined)
        return False, True
    if quarantined.pop(frame, None) is not None:
        p.quarantined_frames = quarantined
        updates["quarantined_frames"] = with_tombstones(quarantined, previous_quarantined)

    if action.submit:
        logger.info(f"Submitting {action.job_spec} for {p.label} {frame_id=} phase={action.phase_label}")
        logger.info(action.job_params)
        job_id = submit_job(action.job_name, action.job_spec, action.job_params, action.job_queue,
                            action.job_tags)
        if job_id is False:
            # Not finished either: the frame's remaining work is exactly what just failed to submit,
            # and reporting it done would let the batch proc disable itself with work outstanding
            logger.error("Job submission failed for %s" % action.job_name)
            return False, False
        logger.info("Job submitted successfully. Job ID: %s" % job_id)
        if action.inflight is not None:
            action.inflight = dict(action.inflight, job_id=job_id)

    if action.new_lineage:
        transitions = list(getattr(p, "lineage_transitions", None) or [])
        if not any(t.get("frame") == frame_id and t.get("phase") == action.new_lineage for t in transitions):
            logger.info(f"NEW LINEAGE frame={frame_id} phase={action.new_lineage} -- compressed CSLC reset "
                        f"in effect; compressed CSLCs of earlier phases are excluded by the lineage bound")
            transitions.append({"frame": frame_id, "phase": action.new_lineage,
                                "timestamp": now.strftime(ES_DATETIME_FORMAT)})
            p.lineage_transitions = transitions
            updates["lineage_transitions"] = transitions

    previous_inflight = live_entries(getattr(p, "forward_inflight", None))
    inflight_map = dict(previous_inflight)
    if action.clear_inflight:
        inflight_map.pop(frame, None)
    elif action.inflight is not None:
        inflight_map[frame] = action.inflight
    if inflight_map != previous_inflight:
        p.forward_inflight = inflight_map
        updates["forward_inflight"] = with_tombstones(inflight_map, previous_inflight)

    previous_stalled = live_entries(getattr(p, "stalled_frames", None))
    stalled = dict(previous_stalled)
    if action.stall_reason:
        stalled[frame] = action.stall_reason
    else:
        stalled.pop(frame, None)
    if stalled != previous_stalled:
        p.stalled_frames = stalled
        updates["stalled_frames"] = with_tombstones(stalled, previous_stalled)

    if action.phase_label:
        frame_phases = dict(getattr(p, "frame_phases", None) or {})
        if frame_phases.get(frame) != action.phase_label:
            frame_phases[frame] = action.phase_label
            p.frame_phases = frame_phases
            updates["frame_phases"] = frame_phases

    if action.next_position != position:
        p.frame_states[frame_key] = action.next_position
        updates["frame_states"] = p.frame_states

        data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
        progress_percentage, frame_completion, last_processed_datetimes = \
            cslc_utils.calculate_historical_progress(p.frame_states, data_end_date, disp_burst_map, p.k,
                                                     phased=True)
        updates.update(progress_percentage=progress_percentage,
                       frame_completion_percentages=frame_completion,
                       last_processed_datetimes=last_processed_datetimes)

    if updates:
        update_batch_proc(eu, doc_id, **updates)

    return action.finished, True

@dataclass
class FrameAction:
    '''What the daemon does for one frame of a phased batch proc in one poll.

    Each poll advances a frame by at most one action: submit a job, record that a forward date
    reached a terminal disposition, skip over an unprocessable block, or nothing at all while
    waiting on the cascade.'''

    next_position: int
    finished: bool = False
    submit: bool = False
    job_name: str = ""
    job_spec: str = ""
    job_params: dict = field(default_factory=dict)
    job_tags: list = field(default_factory=list)
    job_queue: str = ""
    phase_label: str = ""
    inflight: dict = None          # forward date awaiting a disposition, persisted as-is
    clear_inflight: bool = False
    quarantine_reason: str = ""    # the frame is skipped until an operator intervenes
    new_lineage: str = ""          # label of a phase whose compressed CSLC lineage starts here
    stall_reason: str = ""         # the frame is not progressing and an operator should look

def batch_proc_is_phased(p):
    '''A batch proc opts in to the phase walk with "phased": true.

    Without the opt-in -- or with the DISP_S1_PROCESSING_MODE_ENABLED master switch off, which
    leaves every frame without phases -- processing is exactly as it was before phases existed.'''

    return getattr(p, "phased", False) is True

def ksc_fires(meta):
    '''Would the k-cycle state config fire a forward SCIFLO?'''

    return bool(meta
                and meta.get("is_complete")
                and meta.get("compressed_cslc_final")
                and not meta.get("gap_unresolved")
                and not meta.get("superseded_by"))

def classify_ksc(meta):
    '''Disposition of a forward date from one k-cycle state config snapshot.

    'fire'/'fire-boundary' mean a SCIFLO fires (the boundary variant also writes compressed
    CSLCs), 'no-fire-*' mean the cascade decided against firing, 'incomplete' means the window is
    still resolving, and 'pending' means the cascade has not created the state config yet.'''

    if not meta:
        return "pending"
    if ksc_fires(meta):
        return "fire-boundary" if meta.get("save_compressed_cslc") else "fire"
    if meta.get("superseded_by"):
        return "no-fire-superseded"
    if meta.get("gap_unresolved"):
        return "no-fire-gap"
    return "incomplete"

def window_full(meta):
    '''True when the k-window of a state config has all the cycles it will ever have.'''

    expected = (meta or {}).get("cycles_expected")
    return expected is not None and (meta or {}).get("cycles_complete") == expected

def get_ksc_metadata(eu, frame_id, sensing_date):
    '''Return the metadata of the k-cycle state config for one frame and sensing date, or None.'''

    body = {"query": {"bool": {"must": [
                {"term": {"metadata.frame_id": frame_id}},
                {"term": {"metadata.sensing_date": int(sensing_date)}}]}},
            "size": 1}
    try:
        hits = eu.query(index=KSC_ES_INDEX_PATTERN, body=body)
    except Exception as e:
        logger.warning(f"Could not query k-cycle state config for {frame_id=} {sensing_date=}: {e}")
        return None

    if not hits:
        return None

    return hits[0].get("_source", {}).get("metadata", {})

def minutes_since(timestamp, now):
    return (now - datetime.strptime(timestamp, ES_DATETIME_FORMAT)).total_seconds() / 60

def check_forward_disposition(p, eu, frame_id, sensing_date, inflight, now):
    '''Has an in-flight forward date reached a terminal disposition?

    Returns (disposition, inflight, stall_reason). An empty disposition means the cascade is still
    working on the date; the daemon leaves the cursor where it is and checks again next poll.'''

    settle_mins = getattr(p, "forward_settle_mins", DEFAULT_FORWARD_SETTLE_MINS)
    stall_mins = getattr(p, "forward_stall_mins", DEFAULT_FORWARD_STALL_MINS)

    meta = get_ksc_metadata(eu, frame_id, sensing_date)
    disposition = classify_ksc(meta)

    if disposition in FIRE_DISPOSITIONS or disposition in ("no-fire-superseded", "no-fire-gap"):
        return disposition, inflight, ""

    if disposition == "incomplete" and not window_full(meta):
        # A window that is still filling is terminal only once it stops changing: that is an early
        # date whose window will never fill, as opposed to one waiting on ancillary inputs.
        signature = [meta.get("cycles_complete"), meta.get("completeness_reason")]
        if inflight.get("signature") != signature:
            inflight = dict(inflight, signature=signature, stable_since=now.strftime(ES_DATETIME_FORMAT))
        elif minutes_since(inflight.get("stable_since", inflight["submitted_at"]), now) >= settle_mins:
            return "no-fire-incomplete", inflight, ""

    stall_reason = ""
    waiting_mins = minutes_since(inflight["submitted_at"], now)
    if waiting_mins >= stall_mins:
        stall_reason = (f"forward date {sensing_date} has been in flight for {int(waiting_mins)} minutes "
                        f"with disposition '{disposition}' (ingest job {inflight.get('job_id')})")
        logger.warning(f"{frame_id=}: {stall_reason}")

    return "", inflight, stall_reason

def plan_historical_action(p, frame_id, phase, position, frame, eu):
    '''Plan the next k-set query job of a historical phase.'''

    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
    phase_position = position - phase.start_pos

    if position + p.k > phase.end_pos:
        return FrameAction(
            next_position=position, phase_label=phase.label,
            quarantine_reason=(f"phase {phase.label} has {phase.length} dates, which is not a whole "
                               f"number of k={p.k} sets"))

    s_date = frame.sensing_datetimes[position] - timedelta(minutes=30)
    e_date = frame.sensing_datetimes[position + p.k - 1] + timedelta(minutes=30)
    next_position = position + p.k

    if e_date > (data_end_date + timedelta(minutes=30)):
        return FrameAction(next_position=position, finished=True, phase_label=phase.label)

    # A k-set entirely before the batch proc's start date is not work for this batch proc
    if s_date < data_start_date:
        return FrameAction(next_position=next_position, phase_label=phase.label)

    '''Query GRQ ES for the compressed CSLCs of the previous k-set of this lineage. Until they
    exist this k-set cannot be processed, so leave the cursor where it is and retry next poll. The
    first k-set of a new historical phase depends on nothing -- that is the CCSLC reset.'''
    try:
        cslc_dependency = CSLCDependency(p.k, p.m, disp_burst_map, None, None, None, None, blackout_dates_obj)
        if not cslc_dependency.compressed_cslc_satisfied(
                frame_id, frame.sensing_datetime_days_index[position], eu):
            logger.info(f"Compressed CSLC not satisfied for frame {frame_id} at sensing time position "
                        f"{position} of phase {phase.label}. Skipping now but will be retried in the future.")
            return FrameAction(next_position=position, phase_label=phase.label)
    except Exception as e:
        logger.error(f"Error checking compressed cslc satiety for frame {frame_id} at sensing time "
                     f"position {position}. Error: {e}")
        return FrameAction(next_position=position, phase_label=phase.label)

    job_params = build_query_job_params(p, frame_id, s_date, e_date,
                                        m=min(phase_position // p.k + 1, p.m))
    job_name = "data-subscriber-query-timer-{}_f{}-{}-{}".format(
        p.label, frame_id, s_date.strftime(ES_DATETIME_FORMAT), e_date.strftime(ES_DATETIME_FORMAT))

    return FrameAction(
        next_position=next_position,
        finished=next_position >= len(frame.sensing_datetimes),
        submit=True,
        job_name=job_name,
        job_spec=f"job-{p.job_type}:{JOB_RELEASE}",
        job_params=job_params,
        job_tags=["data-subscriber-query-timer", "historical_processing", "phased_historical",
                  f"frame_{frame_id}", phase.label],
        job_queue=p.job_queue,
        phase_label=phase.label,
        new_lineage=phase.label if (phase.is_new_lineage and phase_position == 0) else "")

def plan_forward_action(p, frame_id, phase, position, frame, eu, now):
    '''Plan one date of a forward phase: submit it, or check the one already in flight.

    Forward dates are driven one at a time and only advance once the cascade has decided the
    date's fate, which keeps k-boundaries from being counted out of order.'''

    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)
    sensing_datetime = frame.sensing_datetimes[position]
    sensing_date = sensing_datetime.strftime(SENSING_DATE_FORMAT)
    inflight = live_entries(getattr(p, "forward_inflight", None)).get(str(frame_id))

    if inflight and inflight.get("position") == position:
        disposition, inflight, stall_reason = check_forward_disposition(
            p, eu, frame_id, sensing_date, inflight, now)
        if not disposition:
            return FrameAction(next_position=position, phase_label=phase.label,
                               inflight=inflight, stall_reason=stall_reason)

        logger.info(f"{frame_id=} forward date {sensing_date} reached disposition '{disposition}'")
        next_position = position + 1
        return FrameAction(next_position=next_position, phase_label=phase.label, clear_inflight=True,
                           finished=next_position >= len(frame.sensing_datetimes))

    if sensing_datetime > data_end_date:
        return FrameAction(next_position=position, finished=True, phase_label=phase.label)

    if sensing_datetime < data_start_date:
        return FrameAction(next_position=position + 1, phase_label=phase.label)

    '''A forward date continues its own chunk's lineage, so the compressed CSLCs of the preceding
    historical k-sets must exist before the cascade can produce anything for it.'''
    try:
        cslc_dependency = CSLCDependency(p.k, p.m, disp_burst_map, None, None, None, None, blackout_dates_obj)
        if not cslc_dependency.compressed_cslc_satisfied(
                frame_id, frame.sensing_datetime_days_index[position], eu):
            logger.info(f"Compressed CSLC not satisfied for frame {frame_id} forward date {sensing_date}. "
                        f"Skipping now but will be retried in the future.")
            return FrameAction(next_position=position, phase_label=phase.label)
    except Exception as e:
        logger.error(f"Error checking compressed cslc satiety for frame {frame_id} forward date "
                     f"{sensing_date}. Error: {e}")
        return FrameAction(next_position=position, phase_label=phase.label)

    s_date = sensing_datetime.replace(hour=0, minute=0, second=0)
    e_date = sensing_datetime.replace(hour=23, minute=59, second=59)
    job_params = {
        "frame_ids": str(frame_id),
        "start_date": convert_datetime(s_date),
        "end_date": convert_datetime(e_date),
    }

    return FrameAction(
        next_position=position,   # only a terminal disposition advances a forward date
        submit=True,
        job_name=f"cslc_catalog_ingest-{p.label}_f{frame_id}-{sensing_date}",
        job_spec=f"job-{FORWARD_JOB_TYPE}:{JOB_RELEASE}",
        job_params=job_params,
        job_tags=["phased_forward", f"frame_{frame_id}", p.label, phase.label],
        # The catalog ingest job spec recommends its own queue, but that queue has no workers on
        # every cluster; the batch proc's download queue always does, since historical processing
        # cannot run without it. forward_job_queue overrides when a venue does deploy one.
        job_queue=getattr(p, "forward_job_queue", None) or p.download_job_queue,
        phase_label=phase.label,
        # Every field of the entry is written, including the ones this date has no value for yet:
        # the document update merges objects, so an omitted field would keep the previous date's
        # value and could settle this one the moment it is first checked
        inflight={"position": position, "sensing_date": sensing_date,
                  "submitted_at": now.strftime(ES_DATETIME_FORMAT),
                  "job_id": None, "signature": None, "stable_since": None})

def plan_frame_action(p, frame_id, position, eu, now):
    '''Plan this poll's action for one frame of a phased batch proc.'''

    frame = disp_burst_map[frame_id]

    if frame.phases is None:
        return FrameAction(
            next_position=position,
            quarantine_reason=(frame.phase_error or "frame has no processing-mode annotations; a phased "
                               "batch proc requires an annotated burst database"))

    if position >= len(frame.sensing_datetimes):
        return FrameAction(next_position=position, finished=True)

    try:
        phase = phase_for_position(frame.phases, position)
    except PhaseValidationError as e:
        # The cursor is past the last annotated date: the annotations lag the burst database and
        # processing unlabeled dates would be guesswork.
        return FrameAction(next_position=position,
                           quarantine_reason=f"awaiting_annotations: {e}")

    if phase.kind is PhaseKind.NO_RUN:
        logger.info(f"{frame_id=} skipping {phase.length} dates of {phase.label} "
                    f"(too few full-coverage acquisitions to process)")
        return FrameAction(next_position=phase.end_pos, phase_label=phase.label,
                           finished=phase.end_pos >= len(frame.sensing_datetimes))

    if phase.kind is PhaseKind.HISTORICAL:
        return plan_historical_action(p, frame_id, phase, position, frame, eu)

    return plan_forward_action(p, frame_id, phase, position, frame, eu, now)

def build_query_job_params(p, frame_id, s_date, e_date, m):
    '''Build the parameters of a historical cslc query job for one k-set window.'''

    try:
        if p.temporal is True:
            temporal = True
        else:
            temporal = False
    except:
        temporal = True
        logger.info(f"Temporal parameter not found in batch proc. Defaulting to {temporal}.")

    if p.processing_mode == "historical":
        temporal = True  # temporal is always true for historical processing

    job_params = {
        "start_datetime": f"--start-date={convert_datetime(s_date)}",
        "end_datetime": f"--end-date={convert_datetime(e_date)}",
        "endpoint": f'--endpoint=OPS',
        "bounding_box": "",
        "download_job_queue": f'--job-queue={p.download_job_queue}',
        "download_job_release": f'--release-version={JOB_RELEASE}', #TODO: remove this after removing from jobspec docker files
        "chunk_size": f'--chunk-size={p.chunk_size}',
        "processing_mode": f'--processing-mode={p.processing_mode}',
        "frame_id": f"--frame-id={frame_id}",
        "smoke_run": "",
        "dry_run": "",
        "no_schedule_download": "",
        "use_temporal": f'--use-temporal' if temporal is True else '',
        "k": f"--k={p.k}",
        "m": f"--m={m}",
    }

    if len(p.include_regions.strip()) > 0:
        job_params["include_regions"] = f'--include-regions={p.include_regions}'

    if len(p.exclude_regions.strip()) > 0:
        job_params["exclude_regions"] = f'--exclude-regions={p.exclude_regions}'

    provider = getattr(p, 'provider_name', 'ASF')
    if provider not in {'GRQ', 'ASF'}:
        print(f'WARN: provider_name {provider} not a valid value (GRQ or ASF). Using default ASF instead')
        provider = 'ASF'

    job_params['provider'] = f'--provider={provider}'

    return job_params

def form_job_params(p, frame_id, sensing_time_position_zero_based, args, eu):

    data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
    data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)

    do_submit = True
    finished = False
    processing_mode = p.processing_mode

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
    if e_date > (data_end_date + timedelta(minutes=30)):
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

    # We need to adjust the m parameter early in the sensing time series
    # For example, if this is the very first k-set, there won't be compressed cslc and therefore m should be 1
    if sensing_time_position_zero_based < p.k * (p.m-1):
        m = (sensing_time_position_zero_based // p.k) + 1
    else:
        m = p.m

    job_params = build_query_job_params(p, frame_id, s_date, e_date, m)

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

    for frame in cslc_utils.expand_batch_proc_frames(frames):
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
    # Imported rather than run: load checked-in fixtures so importing this module never needs S3 or
    # a cluster. Tests that exercise the phase walk replace disp_burst_map with an annotated map.
    TESTS = Path(__file__).parent.parent / "tests"
    BURST_MAP = TESTS / "tools" / "test_consistent_db.json"
    disp_burst_map, burst_to_frames, datetime_to_frames = \
        cslc_utils.process_disp_frame_burst_hist(str(BURST_MAP))
    blackout_dates = process_disp_blackout_dates(TESTS / "data_subscriber" / "empty_disp_s1_blackout.json")
    blackout_dates_obj = DispS1BlackoutDates(blackout_dates, disp_burst_map, burst_to_frames)
