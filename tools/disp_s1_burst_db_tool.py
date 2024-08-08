#!/usr/bin/env python3

from collections import defaultdict
import logging
from data_subscriber import cslc_utils
from data_subscriber.cslc_utils import CSLCDependency
from datetime import datetime, timedelta
import argparse
from util.conf_util import SettingsConf
from data_subscriber.cmr import get_cmr_token
from data_subscriber.parser import create_parser
from data_subscriber.query import DateTimeRange
from data_subscriber.cslc.cslc_query import CslcCmrQuery

''' Tool to query the DISP S1 burst database 
    The burst database file must be in the same directory as this script'''

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", dest="verbose", help="If true, print out verbose information, mainly cmr queries and k-cycle calculation.", required=False, default=False)
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

server_parser = subparsers.add_parser("validate", help="Validates the burst database file against the CMR")
server_parser.add_argument("frame_id", help="The frame id to validate")

args = parser.parse_args()

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

def get_k_cycle(acquisition_dts, frame_id, disp_burst_map, k, verbose):

    subs_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward"])

    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(subs_args.endpoint, settings)

    cslc_dependency = CSLCDependency(k, None, disp_burst_map, subs_args, token, cmr, settings) # we don't care about m here
    k_cycle: int = cslc_dependency.determine_k_cycle(acquisition_dts, None, frame_id, silent = not verbose)

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

    if len(frame_ids) == 0:
        print("Frame ids not found for burst id: ", burst_id, " this burst likely is not valid for DISP-S1 processing")
        exit(-1)

    if args.k:
        k = int(args.k)

        k_cycle = get_k_cycle(acquisition_dts, frame_ids[0], disp_burst_map, k, args.verbose)
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

elif args.subparser_name == "unique_id":
    print("This feature is not implemented yet")

elif args.subparser_name == "validate":
    frame_id = int(args.frame_id)
    if frame_id not in disp_burst_map.keys():
        print("Frame id: ", frame_id, "does not exist")
        exit(-1)

    start_date = (disp_burst_map[frame_id].sensing_datetimes[0]-timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = (disp_burst_map[frame_id].sensing_datetimes[-1]+timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    query_timerange = DateTimeRange(start_date, end_date)

    # Query the CMR for the frame_id between the first and the last sensing datetime
    subs_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--k=4", "--m=2", "--use-temporal", "--processing-mode=forward"])
    subs_args.frame_id = frame_id
    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(subs_args.endpoint, settings)
    cslc_query = CslcCmrQuery(subs_args, token, None, cmr, None, settings)
    all_granules = cslc_query.query_cmr_by_frame_and_dates(subs_args, token, cmr, settings, datetime.now(), query_timerange)

    print(len(all_granules), " granules found in the CMR")

    # Group them by acquisition cycle
    acq_cycles = defaultdict(set)
    granules_map = defaultdict(list)
    for g in all_granules:
        acq_cycles[g["acquisition_cycle"]].add(g["burst_id"])
        granules_map[g["acquisition_cycle"]].append(g["granule_id"])

    '''
    Validation fails if the following conditions are true. Otherwise, it succeeds
    1. If we did not find any complete acquisition cycle that is in the expected list
    2. If we found any complete acquisition cycle that is not in the expected list
        Complete acq cycle is the one that has all the bursts according to the burst pattern 
    '''
    missing_cycles = False
    unexpected_cycles = False
    bursts_expected = disp_burst_map[frame_id].burst_ids
    for i in disp_burst_map[frame_id].sensing_datetime_days_index:
        bursts_found = acq_cycles[i]
        delta = bursts_expected - bursts_found
        if delta:
            missing_cycles = True
            print(f"Acquisition cycle {i} is missing {len(delta)} bursts: ", delta)
            print(f"Granules for acquisition cycle {i} found:", granules_map[i])
        else:
            print(f"Acquisition cycle {i} is good")

    new_cycles = acq_cycles.keys() - disp_burst_map[frame_id].sensing_datetime_days_index
    for i in new_cycles:
        if acq_cycles[i].issuperset(bursts_expected):
            if ("HH" in granules_map[i][0]): # We don't process HH polarization so it's not in the database on purpose
                pass
            else:
                unexpected_cycles = True
                print(f"Complete acquisition cycle {i} was found in CMR but was not in the database json")
                print(f"Granules for acquisition cycle {i} found:", granules_map[i])

    if not missing_cycles:
        print("All acquisition cycles in the database json are complete in CMR")
    if not unexpected_cycles:
        print("Did not find any complete acquisition cycles in CMR that is not in the database json")

    if missing_cycles or unexpected_cycles:
        print(f"FAIL: frame_id {frame_id} validation failed")
    else:
        print(f"SUCCESS: frame_id {frame_id} validation succeeded!")

