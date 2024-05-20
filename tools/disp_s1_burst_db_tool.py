#!/usr/bin/env python3

from data_subscriber import cslc_utils
from datetime import datetime
import argparse

''' Tool to query the DISP S1 burst database 
    The burst database file must be in the same directory as this script'''

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="subparser_name", required=True)

server_parser = subparsers.add_parser("list", help="List all frame numbers")

server_parser = subparsers.add_parser("native_id", help="Print information based on native_id")
server_parser.add_argument("id", help="The CSLC native id from CMR")

server_parser = subparsers.add_parser("frame", help="Print information based on frame")
server_parser.add_argument("number", help="The frame number")

server_parser = subparsers.add_parser("burst", help="Print information based on burst id.")
server_parser.add_argument("burst_id", help="Burst id looks like T175-374393-IW1.")

asg_parser = subparsers.add_parser("time_range", help="Print frame that are available within a time range")
asg_parser.add_argument("start_time", help="Start time looks like 2023-10-01T00:00:00")
asg_parser.add_argument("end_time", help="End time looks like 2023-10-25T00:00:00")

args = parser.parse_args()

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.process_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

if args.subparser_name == "list":
    print(list(disp_burst_map.keys()))

elif args.subparser_name == "native_id":
    burst_id, acquisition_dts, acquisition_cycles, frame_ids = cslc_utils.parse_cslc_native_id(args.id, burst_to_frames, disp_burst_map)
    print("Burst id: ", burst_id)
    print("Acquisition datetime: ", acquisition_dts)
    print("Acquisition cycles: ", acquisition_cycles)
    print("Frame ids: ", frame_ids)

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

