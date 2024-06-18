#!/usr/bin/env python3

import asyncio
import logging
from data_subscriber import cslc_utils
from datetime import datetime
import argparse
from util.conf_util import SettingsConf
from data_subscriber.cmr import get_cmr_token
from data_subscriber.parser import create_parser

''' Tool to query the DISP S1 burst database 
    The burst database file must be in the same directory as this script'''

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="subparser_name", required=True)

server_parser = subparsers.add_parser("list", help="List all frame numbers")

server_parser = subparsers.add_parser("summary", help="List all frame numbers, number of bursts, and sensing datetimes")

server_parser = subparsers.add_parser("native_id", help="Print information based on native_id")
server_parser.add_argument("id", help="The CSLC native id from CMR")
server_parser.add_argument("--k", dest="k", help="If the k parameter is provided, the k-cycle of this granule is computed", required=False)

server_parser = subparsers.add_parser("frame", help="Print information based on frame")
server_parser.add_argument("number", help="The frame number")

server_parser = subparsers.add_parser("burst", help="Print information based on burst id.")
server_parser.add_argument("burst_id", help="Burst id looks like T175-374393-IW1.")

asg_parser = subparsers.add_parser("time_range", help="Print frame that are available within a time range")
asg_parser.add_argument("start_time", help="Start time looks like 2023-10-01T00:00:00")
asg_parser.add_argument("end_time", help="End time looks like 2023-10-25T00:00:00")

server_parser = subparsers.add_parser("unique_id", help="Print information based on unique_id... unique_id is combination of burst patern and acquisition time")
server_parser.add_argument("burst", help="The Burst ID T175-374393-IW1")
server_parser.add_argument("date_time", help="The Acquisition Datetime looks like 2023-10-01T00:00:00")

#TODO
server_parser = subparsers.add_parser("simulate", help="Simulate a historical processing run")

args = parser.parse_args()

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.process_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

async def get_k_cycle(acquisition_dts, frame_id, disp_burst_map, k):

    subs_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward"])

    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(subs_args.endpoint, settings)

    k_cycle = await cslc_utils.determine_k_cycle(acquisition_dts, None, frame_id, disp_burst_map, k, subs_args, token, cmr, settings)

    return k_cycle

if args.subparser_name == "list":
    l = list(disp_burst_map.keys())
    print("Frame numbers (%d): \n" % len(l), l)

elif args.subparser_name == "summary":
    l = list(disp_burst_map.keys())
    print([(f, len(disp_burst_map[f].burst_ids), len(disp_burst_map[f].sensing_datetimes))  for f in l])

    print("Frame numbers: %d" % len(l))

    # Add up all the sensing times and print it out
    total_sensing_times = 0
    for f in l:
        total_sensing_times += len(disp_burst_map[f].sensing_datetimes)
    print("Total sensing times: ", total_sensing_times)

    # Add up and print out the total number of granules.
    total_granules = 0
    for f in l:
        total_granules += len(disp_burst_map[f].burst_ids) * len(disp_burst_map[f].sensing_datetimes)
    print("Total granules: ", total_granules)

elif args.subparser_name == "native_id":
    burst_id, acquisition_dts, acquisition_cycles, frame_ids = cslc_utils.parse_cslc_native_id(args.id, burst_to_frames, disp_burst_map)
    print("Burst id: ", burst_id)
    print("Acquisition datetime: ", acquisition_dts)
    print("Acquisition cycles: ", acquisition_cycles)
    print("Frame ids: ", frame_ids)

    if args.k:
        k = int(args.k)

        k_cycle = asyncio.run(get_k_cycle(acquisition_dts, frame_ids[0], disp_burst_map, k))
        if (k_cycle >= 0):
            print(f"K-cycle: {k_cycle} out of {k}")
        else:
            print("K-cycle can not computed")

elif args.subparser_name == "frame":
    frame_number = int(args.number)
    if frame_number not in disp_burst_map.keys():
        print("Frame number: ", frame_number, "does not exist")
        exit(-1)

    print("Frame number: ", frame_number)
    print("Burst ids (%d): " % len(disp_burst_map[frame_number].burst_ids))
    print(disp_burst_map[frame_number].burst_ids)
    print("Sensing datetimes (%d): " % len(disp_burst_map[frame_number].sensing_datetimes))
    print([t.isoformat() for t in disp_burst_map[frame_number].sensing_datetimes])
    print("Day indices: ", disp_burst_map[frame_number].sensing_datetime_days_index)

elif args.subparser_name == "burst":
    burst_id = args.burst_id
    if burst_id not in burst_to_frames.keys():
        print("Burst id: ", burst_id, "does not exist")
        exit(-1)

    print("Burst id: ", burst_id)
    frame_numbers = burst_to_frames[burst_id]
    print("Frame numbers: ", frame_numbers)
    print("Sensing datetimes: ")
    for f in frame_numbers:
        print("(%d): " % len(disp_burst_map[f].sensing_datetimes))
        print([t.isoformat() for t in disp_burst_map[f].sensing_datetimes])

elif args.subparser_name == "time_range":
    start_time = datetime.fromisoformat(args.start_time)
    end_time = datetime.fromisoformat(args.end_time)

    for frame_number in disp_burst_map.keys():
        for t in disp_burst_map[frame_number].sensing_datetimes:
            if start_time <= t <= end_time:
                print("Frame number: ", frame_number)
                print("\tSensing datetime: ", t.isoformat())
                print("\tBurst ids (%d):" % len(disp_burst_map[frame_number].burst_ids))
                print("\t", disp_burst_map[frame_number].burst_ids)
