#!/usr/bin/env python3

from __future__ import print_function

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from opera_commons.es_connection import get_grq_es

from data_subscriber.cslc_utils import (localize_disp_frame_burst_hist, get_nearest_sensing_datetime,
                                        _phased_progress_counts)
from data_subscriber.cslc.disp_s1_phases import PhaseKind, all_no_run

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ES_INDEX = 'batch_proc'
SETTINGS_FILE = "/export/home/hysdsops/mozart/ops/opera-pcm/conf/settings.yaml"

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('disp_s1_hist_status')
LOGGER.setLevel(logging.INFO)

def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def summarize_phases(phases):
    """
    One-line rendering of a frame's phases, e.g. "historical_01[15]@0 forward_01[1]@15 no_run[9]@33"
    """
    return " ".join(f"{phase.label}[{phase.length}]@{phase.start_pos}" for phase in phases)


def phase_details(phases):
    """
    Structured per-phase breakdown for the geojson. end_position is exclusive, matching the phase model
    """
    return [{'label': phase.label,
             'kind': phase.kind.value,
             'ordinal': phase.ordinal,
             'start_position': phase.start_pos,
             'end_position': phase.end_pos,
             'length': phase.length} for phase in phases]


def phased_status(frame, num_triggered, k):
    """
    Completion accounting for a frame whose burst database carries processing-mode labels.

    The frame_states cursor of a phased batch proc walks the whole timeline: it steps k dates at a
    time through historical_NN blocks, one date at a time through forward_NN blocks and straight
    over no_run blocks. The un-phased denominator (whole k-sets across every sensing datetime)
    therefore does not match the cursor at all - a finished frame 17235 reads 140%. Only whole
    k-sets of historical_NN dates plus every forward_NN date are ever processed, and a finished
    frame leaves its cursor at len(sensing_datetimes), so counting only those dates makes a
    finished frame report exactly 100%.

    Returns (completion_percentage, processed, possible, status).
    """
    possible, processed = _phased_progress_counts(frame.phases, len(frame.sensing_datetimes), num_triggered, k)

    if possible == 0:
        # Every phase is no_run: there is no work to do, so this is neither 0% nor really complete
        return 100, processed, possible, 'no_run'

    p = int(processed / possible * 100)
    if p >= 100:
        status = 'complete'
    elif processed == 0:
        status = 'not_started'
    else:
        status = 'in_progress'
    return p, processed, possible, status


def add_status_info(frames, verbose, eu, frames_to_bursts):

    query = {"query": {"term": {"job_type": "cslc_query_hist"}}}
    procs = eu.es.search(body=query, index=ES_INDEX, size=1000)
    for hit in procs['hits']['hits']:
        proc = hit['_source']
        k = proc['k']

        # A phased batch proc walks each frame phase by phase instead of stepping k across the
        # whole timeline, so its progress has to be measured against the phases
        phased = proc.get("phased", False) is True

        for frame, _ in proc["frame_completion_percentages"].items():
            num_triggered = proc['frame_states'][frame]
            frame_int = int(frame)

            if frame_int not in frames:
                if verbose:
                    LOGGER.info(f"Skipping updating frame {frame_int} because it is not in the input geojson")
                continue

            frame_obj = frames_to_bursts[frame_int]

            # phases is None for an un-annotated burst database, for a frame whose labels were
            # rejected, or when processing modes are switched off; fall back to the k-set formula
            phases = frame_obj.phases if phased else None

            if not phases:
                phases = None
                # Normalize the completion percentage according to the number of sensing datetimes that is actually triggerable
                possible_triggered = len(frame_obj.sensing_datetime_days_index) // k * k
                if possible_triggered == 0:
                    p = 0
                else:
                    p = int(num_triggered / possible_triggered * 100)
                triggered = num_triggered
                status = None
            else:
                p, triggered, possible_triggered, status = phased_status(frame_obj, num_triggered, k)

            if verbose:
                LOGGER.info(f"Updating status for frame {frame_int}: completion_percentage {p}, num_triggered {triggered}, possible_triggered {possible_triggered}, last_processed_time {proc['last_processed_datetimes'][frame]}")
            frames[frame_int]['processing_status'] = {
                'completion_percentage': p,
                'sensing_datetimes_triggered': triggered,
                'possible_sending_datetimes_to_trigger': possible_triggered,
                'sensing_datetime_count': len(frame_obj.sensing_datetime_days_index),
                'last_triggered_sensing_datetime': proc['last_processed_datetimes'][frame]
            }

            if phases is None:
                continue

            # Phase information goes into the properties so that the render step can show it
            frames[frame_int]['processing_status'].update({
                'phased': True,
                'frame_state_cursor': num_triggered,
                'status': status,
                'phases': phase_details(phases)
            })
            frames[frame_int]['properties']['phase_status'] = status
            frames[frame_int]['properties']['phase_summary'] = summarize_phases(phases)
            frames[frame_int]['properties']['no_run_sensing_datetimes'] = \
                sum(phase.length for phase in phases if phase.kind is PhaseKind.NO_RUN)
            if all_no_run(phases):
                LOGGER.info(f"Frame {frame_int} has nothing to process: every sensing datetime is no_run")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Append the given geojson with DISP-S1 Historical Processing Status information')
    parser.add_argument('input_json', type=str, help='Input geojson that lists all the frames')
    parser.add_argument('--verbose', action='store_true', help='Verbose output', default=False)
    parser.add_argument('--skip-frames-file', type=str, help='File containing a list of frames to skip, one frame per line', default=None)
    parser.add_argument('--output-filename', type=str, help='Output geojson filename', default='opera_disp_s1_hist_status.geojson')
    args = parser.parse_args(sys.argv[1:])

    with open(args.input_json) as f:
        data = json.load(f)

    frames = {}
    for feature in data['features']:
        frame_id = feature['properties']['frame_id']
        frames[frame_id] = feature

    if args.skip_frames_file:
        try:
            f = open(args.skip_frames_file)
            skip_frames = []
            for line in f.readlines():
                if line.strip():
                    skip_frames.append(int(line.strip()))
            for frame in skip_frames:
                if frame in frames:
                    LOGGER.info(f"Skipping frame {frame}")
                    del frames[frame]
        except Exception as e:
            LOGGER.error(f"Error reading skip frames file {args.skip_frames_file} {e}")

    eu = get_grq_es(LOGGER)
    LOGGER.debug("Connected to %s" % str(eu.es_url))

    # Process the default disp s1 burst hist file. Whether its processing-mode labels, if any, are
    # honoured is governed by the DISP_S1_PROCESSING_MODE_ENABLED setting
    frames_to_bursts, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist(str(Path(SETTINGS_FILE)))

    add_status_info(frames, args.verbose, eu, frames_to_bursts)

    # Write out the updated geojson
    data['features'] = list(frames.values())
    data['status_update_datetime'] = datetime.now().isoformat()
    with open(args.output_filename, 'w') as f:
        json.dump(data, f, indent=2)
