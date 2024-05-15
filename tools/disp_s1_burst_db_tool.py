#!/usr/bin/env python3

from data_subscriber import cslc_utils
from datetime import datetime
import argparse

''' Tool to query the DISP S1 burst database 
    The burst database file must be in the same directory as this script'''

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="subparser_name", required=True)

server_parser = subparsers.add_parser("list", help="List all frame numbers")

server_parser = subparsers.add_parser("frame", help="Print information about frame")
server_parser.add_argument("number", help="The frame number", nargs='?', default="all")

server_parser = subparsers.add_parser("burst", help="Print information about burst.")
server_parser.add_argument("burst_id", help="Burst id looks like T175-374393-IW1.", nargs='?')

asg_parser = subparsers.add_parser("time_range", help="Print frame that are available within a time range")
asg_parser.add_argument("start_time", help="Start time looks like 2023-10-01T00:00:00", nargs='?')
asg_parser.add_argument("end_time", help="End time looks like 2023-10-25T00:00:00", nargs='?')

args = parser.parse_args()

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.process_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

if args.subparser_name == "list":
    print(list(disp_burst_map.keys()))

elif args.subparser_name == "frame":
    frame_number = int(args.number)
    if frame_number not in disp_burst_map.keys():
        print("Frame number: ", frame_number, "does not exist")
        exit(-1)

    print("Frame number: ", frame_number)
    print("Burst ids: ", disp_burst_map[frame_number].burst_ids)
    print("Sensing datetimes: ", [t.isoformat() for t in disp_burst_map[frame_number].sensing_datetimes])

elif args.subparser_name == "burst":
    burst_id = args.burst_id
    if burst_id not in burst_to_frames.keys():
        print("Burst id: ", burst_id, "does not exist")
        exit(-1)

    print("Burst id: ", burst_id)
    print("Frame numbers: ", burst_to_frames[burst_id].frame_number)
    print("Sensing datetimes: ")
    for t in burst_to_frames[burst_id].sensing_datetimes:
        print(t.isoformat())

elif args.subparser_name == "time_range":
    start_time = datetime.fromisoformat(args.start_time)
    end_time = datetime.fromisoformat(args.end_time)

    for frame_number in disp_burst_map.keys():
        for t in disp_burst_map[frame_number].sensing_datetimes:
            if start_time <= t <= end_time:
                print("Frame number: ", frame_number)
                print("Sensing datetime: ", t.isoformat())
                print("Burst ids:", disp_burst_map[frame_number].burst_ids)

