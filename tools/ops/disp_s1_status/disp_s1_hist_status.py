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
        for frame, p in proc["frame_completion_percentages"].items():
            frame_state = proc['frame_states'][frame] - 1  # 1-based vs 0-based
            acq_index = frames_to_bursts[int(frame)].sensing_datetime_days_index[frame_state]
            frame_int = int(frame)
            if frame_int not in frames:
                if verbose:
                    LOGGER.info(f"Skipping {frame_int=} completion_percentage {p}")
                continue
            if verbose:
                LOGGER.info(f"Updating {frame_int=} completion_percentage {p}")
            frames[frame_int]['properties']['processing_status'] = {
                'completion_percentage': p
            }
        for frame, d in proc["last_processed_datetimes"].items():
            frame_int = int(frame)
            if frame_int not in frames:
                if verbose:
                    LOGGER.info(f"Skipping {frame_int=} last_processed_time {d}")
                continue
            if verbose:
                LOGGER.info(f"Updating {frame_int=} last_processed_time {d}")
            frames[frame_int]['properties']['processing_status']['last_processed_datetime'] = d

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Append the given geojson with DISP-S1 Historical Processing Status information')
    parser.add_argument('input_json', type=str, help='Input geojson that lists all the frames')
    parser.add_argument('--verbose', action='store_true', help='Verbose output', default=False)
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
    data['update_datetime'] = datetime.now().isoformat()
    with open(args.input_json+ ".mod.geojson", 'w') as f:
        json.dump(data, f, indent=2)
