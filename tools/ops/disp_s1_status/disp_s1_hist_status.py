#!/usr/bin/env python3

from __future__ import print_function

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

from util.conf_util import SettingsConf

from data_subscriber.cslc_utils import localize_disp_frame_burst_hist, get_nearest_sensing_datetime

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

SETTINGS = SettingsConf(file=str(Path("/export/home/hysdsops/.sds/config"))).cfg
GRQ_IP = SETTINGS["GRQ_PVT_IP"]

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('disp_s1_hist_status')
LOGGER.setLevel(logging.INFO)

eu = ElasticsearchUtility('http://%s:9200' % GRQ_IP, LOGGER)
LOGGER.debug("Connected to %s" % str(eu.es_url))

# Process the default disp s1 burst hist file
frames_to_bursts, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()

def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def add_status_info(frames, verbose):

    query = {"query": {"term": {"job_type": "cslc_query_hist"}}}
    procs = eu.es.search(body=query, index=ES_INDEX, size=1000)
    for hit in procs['hits']['hits']:
        proc = hit['_source']
        k = proc['k']
        for frame, _ in proc["frame_completion_percentages"].items():
            num_triggered = proc['frame_states'][frame]
            frame_int = int(frame)

            # Normalize the completion percentage according to the number of sensing datetimes that is actually triggerable
            possible_triggered = len(frames_to_bursts[frame_int].sensing_datetime_days_index) // k * k
            p = int(num_triggered / possible_triggered * 100)

            if frame_int not in frames:
                if verbose:
                    LOGGER.info(f"Skipping updating frame {frame_int} because it is not in the input geojson")
                continue
            if verbose:
                LOGGER.info(f"Updating status for frame {frame_int}: completion_percentage {p}, num_triggered {num_triggered}, possible_triggered {possible_triggered}, last_processed_time {proc['last_processed_datetimes'][frame]}")
            frames[frame_int]['processing_status'] = {
                'completion_percentage': p,
                'sensing_datetimes_triggered': num_triggered,
                'possible_sending_datetimes_to_trigger': possible_triggered,
                'sensing_datetime_count': len(frames_to_bursts[frame_int].sensing_datetime_days_index),
                'last_triggered_sensing_datetime': proc['last_processed_datetimes'][frame]
            }

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Append the given geojson with DISP-S1 Historical Processing Status information')
    parser.add_argument('input_json', type=str, help='Input geojson that lists all the frames')
    parser.add_argument('--verbose', action='store_true', help='Verbose output', default=False)
    parser.add_argument('--output-filename', type=str, help='Output geojson filename', default='opera_disp_s1_hist_status.geojson')
    args = parser.parse_args(sys.argv[1:])

    with open(args.input_json) as f:
        data = json.load(f)

    frames = {}
    for feature in data['features']:
        frame_id = feature['properties']['frame_id']
        frames[frame_id] = feature

    add_status_info(frames, args.verbose)

    # Write out the updated geojson
    data['features'] = list(frames.values())
    data['status_update_datetime'] = datetime.now().isoformat()
    with open(args.output_filename, 'w') as f:
        json.dump(data, f, indent=2)
