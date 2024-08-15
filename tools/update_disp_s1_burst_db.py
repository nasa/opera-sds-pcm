#!/usr/bin/env python3

import logging
import json
from collections import defaultdict
from datetime import datetime
import argparse

from util.conf_util import SettingsConf
from data_subscriber.cmr import get_cmr_token
from data_subscriber.parser import create_parser
from data_subscriber.query import DateTimeRange
from data_subscriber.cslc.cslc_query import CslcCmrQuery
from data_subscriber import cslc_utils

''' Tool to update the DISP S1 burst database sensing_time_list with latest data from CMR. 
    Writes out the new file with .mod added to the end of the file name'''

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("file", help="The DISP S1 burst database file to update")

prog_args = parser.parse_args()
disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist(prog_args.file)
j = json.load(open(prog_args.file))

# Query the CMR for the frame_id between the first and the last sensing datetime
subs_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--k=1", "--m=1", "--use-temporal", "--processing-mode=forward"])
settings = SettingsConf().cfg
cmr, token, username, password, edl = get_cmr_token(subs_args.endpoint, settings)
cslc_query = CslcCmrQuery(subs_args, token, None, cmr, None, settings)

now = datetime.now()
start_date = "2016-07-01T00:00:00Z" # This is the start of DISP-S1 processing time for the OPERA program
end_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
timerange = DateTimeRange(start_date, end_date)

for frame in j:
    new_sensing_time_list = []
    logger.info(f"Updating {frame=}")
    subs_args.frame_id = frame
    all_granules = cslc_query.query_cmr_by_frame_and_dates(subs_args, token, cmr, settings, now, timerange, silent=True)

    # Group them by acquisition cycle
    acq_cycles = defaultdict(set)
    acq_ts_map = defaultdict(list)
    for g in all_granules:
        if '_VV_' not in g["granule_id"]: # We only want to process VV polarization data
            continue
        acq_cycles[g["acquisition_cycle"]].add(g["burst_id"])
        acq_ts_map[g["acquisition_cycle"]].append(g["acquisition_ts"])

    bursts_we_want = disp_burst_map[int(frame)].burst_ids

    sorted_acq_cycles = sorted(acq_cycles.keys())
    for acq_cycle in sorted_acq_cycles:
        if acq_cycles[acq_cycle].issuperset(bursts_we_want):
            newtime = acq_ts_map[acq_cycle][0].strftime("%Y-%m-%dT%H:%M:%S")
            new_sensing_time_list.append(newtime) # we just need one representative datetime for each acq cycle

    old_time_list = j[frame]["sensing_time_list"]
    print(f"{len(old_time_list)} in Old sensing_time_list for {frame=}")
    print(f"{old_time_list}")
    print(f"{len(new_sensing_time_list)} in New sensing_time_list for {frame=}")
    print(f"{new_sensing_time_list}")
    j[frame]["sensing_time_list"] = new_sensing_time_list

new_file = prog_args.file + ".mod"
with open(new_file, "w") as f:
    json.dump(j, f, indent=4)
    print(f"Truncated file written to {new_file}")
