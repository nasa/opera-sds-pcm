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
MOZART_IP = SETTINGS["MOZART_PVT_IP"]

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('pcm_batch')
LOGGER.setLevel(logging.INFO)

eu = ElasticsearchUtility('http://%s:9200' % GRQ_IP, LOGGER)
LOGGER.debug("Connected to %s" % str(eu.es_url))

eu_mzt = ElasticsearchUtility('http://%s:9200' % MOZART_IP, LOGGER)
LOGGER.debug("Connected to %s" % str(eu_mzt.es_url))

FILE_OPTION = '--file'

# Process the default disp s1 burst hist file
frames_to_bursts, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()

def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def view_proc(id):

    # If id is all or ALL then get all batch procs that are currently enabled
    if id.lower() == "all":
        query = {"query": {"term": {"enabled": True}}}
        procs = eu.es.search(body=query, index=ES_INDEX, size=1000)
        rows = []
        for hit in procs['hits']['hits']:
            proc = hit['_source']
            if proc['job_type'] == "cslc_query_hist":

                data_end_date = datetime.strptime(proc['data_end_date'], ES_DATETIME_FORMAT)

                try:
                    pp = f"{proc['progress_percentage']}%"
                except:
                    pp = "UNKNOWN"
                try:
                    fcp = [f"{frame}: {p}%" for frame, p in sorted(proc["frame_completion_percentages"].items(), key=lambda x: int(x[0]))]
                    #fcp = [f"{frame}: {p}%" for frame, p in proc["frame_completion_percentages"].items()]

                    # Every frame that has 100% frame_completion_percentage, check in Mozart ES to see if the last SCIFLO has been completed
                    cf = []
                    ucf = [] # for the uncomplted frame
                    job_id_prefixes = {}
                    for frame, p in sorted(proc["frame_completion_percentages"].items(), key=lambda x: int(x[0])):
                        frame_state = proc['frame_states'][frame] - 1  # 1-based vs 0-based
                        sddi = frames_to_bursts[int(frame)].sensing_datetime_days_index
                        if 0 <= frame_state < len(sddi):
                            acq_index = sddi[frame_state]
                        else:
                            print(f"Frame state {frame_state} out of range for frame {frame} (len={len(sddi)})")
                            continue  # or handle differently

                        job_id_prefix = f"job-WF-SCIFLO_L3_DISP_S1-frame-{frame}-latest_acq_index-{acq_index}_hist"
                        if p == 100:
                            job_id_prefixes[frame] = job_id_prefix
                        else:
                            # check to see if there is a last acq index job for this frame that exists
                            num_sensing_times, _ = get_nearest_sensing_datetime(frames_to_bursts[int(frame)].sensing_datetimes,
                                                                                data_end_date)
                            num_sensing_times = num_sensing_times - (num_sensing_times % proc['k']) # Round down to the nearest k
                            if num_sensing_times == frame_state + 1:
                                job_id_prefixes[frame] = job_id_prefix

                    for frame, job_id_prefix in job_id_prefixes.items():
                        logging.debug(f"Checking for {job_id_prefix}")
                        query = {"query": {"bool": {"must": [{"prefix": {"job_id": job_id_prefix}}]}}}
                        sciflo_jobs = eu_mzt.query(body=query, index="job_status*")
                        for j in sciflo_jobs:
                            if j['_source']['status'] == "job-completed":
                                cf.append(int(frame))
                    ucf = list(set(proc["frames"]) - set(cf))
                except:
                    #print(f"Error in frame processing: {e}", exc_info=True)
                    fcp = "UNKNOWN"
                    cf = "UNKNOWN"
                    ucf = "UNKNOWN"

                rows.append([hit['_id'], proc["label"], pp,  proc["frames"], fcp, cf, ucf])
            else:
                # progress percentage is the ratio of last_successful_proc_data_date in the range between data_start_date and data_end_date
                total_time = convert_datetime(proc["data_end_date"], ES_DATETIME_FORMAT) - convert_datetime(proc["data_start_date"], ES_DATETIME_FORMAT)
                processed_time = convert_datetime(proc["last_successful_proc_data_date"], ES_DATETIME_FORMAT) - convert_datetime(proc["data_start_date"], ES_DATETIME_FORMAT)
                progress_percentage = (processed_time / total_time) * 100
                rows.append([hit['_id'], proc["label"], f"{progress_percentage:.0f}%", "N/A", "N/A", "N/A", "N/A"])

        print(" --- Showing Summary of Enabled Batch Procs --- ")
        if len(rows) == 0:
            print("No enabled batch procs found")
            return
        print(tabulate(rows, headers=["ID (enabled only)", "Label", "Progress", "Frames", "Frame Completion Percentages", "Completed Frames", "Uncompleted frames"], tablefmt="grid", maxcolwidths=[None, None, 5, 30, 55, 30, 25]))

        return

    query = {"query": {"term": {"_id": id}}}
    procs = eu.es.search(body=query, index=ES_INDEX, size=1)

    try:
        hit = procs['hits']['hits'][0]
    except:
        print("No batch proc with id %s found" % id)
        return

    proc = hit['_source']
    doc_id = hit['_id']
    print("Batch Proc ID", doc_id)
    for k, v in proc.items():
        if k == "progress_percentage":
            print(k, ' ' * (30 - len(k)), f"{v}%")
        elif k == "frame_completion_percentages":
            print(k, ' ' * (30 - len(k)), [f"{f}: {p}%" for f, p in v.items()])
        else:
            print(k, ' ' * (30 - len(k)), v)

    if proc["job_type"] == "cslc_query_hist":
        print("\n -- Legend -- ")
        print("frame_states: The number of sensing datetimes that'd been submitted thus far (An internal house keeping construct.)")
        print("frame_completion_percentages: The percentage of sensing datetimes that have been submitted thus far.")
        print("last_processed_datetimes: The last sensing datetime that was submitted.")
        print("progress_percentage: The percentage of the entire job that has been submitted thus far.\n")


def _validate_proc(proc):
    # If this is for DISP-S1 processing, make sure all the frames exist
    if proc["job_type"] == "cslc_query_hist":
        all_frames = set()
        for f in list(frames_to_bursts.keys()):
            all_frames.add(f)

        for f in proc["frames"]:
            if type(f) == list:
                for frame in range(f[0], f[1]):
                    if frame not in all_frames:
                        return f"Frame {frame} not found in DISP-S1 Burst ID Database JSON"
            else:
                if f not in all_frames:
                    return f"Frame {f} not found in DISP-S1 Burst ID Database JSON"

    return True

def batch_proc_once():
    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])

    if args.subparser_name == "list":
        procs = eu.query(index=ES_INDEX)

        print("%d Batch Procs Found" % len(procs))

        print_list = []
        for proc in procs:
            doc_id = proc['_id']
            proc = proc['_source']
            p = SimpleNamespace(**proc)
            print_list.append([doc_id, p.label, p.enabled])

        print(tabulate(print_list, headers=["ID", "Label", "Enabled"], tablefmt="pretty"))

    elif args.subparser_name == "view":
        print("")
        view_proc(args.id)

    elif args.subparser_name == "enable":
        print("")
        view_proc(args.id)
        print("")
        enable = True if args.t_f == "true" else False
        eu.update_document(id=args.id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "enabled": enable, }},
                           index=ES_INDEX)
        print("Batch proc id %s has been %s" % (args.id, ("enabled" if enable == True else "disabled")))

    elif args.subparser_name == "delete":
        print("")
        view_proc(args.id)
        print("")
        print("Are you sure you want to delete batch proc id %s (only 'yes' will be accepted)?" % args.id)
        yes = input()
        if (yes == 'yes'):
            print("Deleting batch proc id %s but sleeping for 5 seconds first. ctrl-c to abort" % args.id)
            time.sleep(5)
            eu.delete_by_id(index=ES_INDEX, id=args.id)
            print("Batch proc id %s has been deleted" % args.id)
        else:
            print("Batch proc deletion aborted")

    elif args.subparser_name == 'create':
        if args.file is not None:
            print("Creating batch proc using file %s" % args.file)
            with open(args.file) as f:
                proc = json.load(f)
                print(proc)
                potential_error_str = _validate_proc(proc)
                if potential_error_str is not True:
                    print("\n FAIL! Creation FAILED: Invalid batch proc", potential_error_str)
                    return
                else:
                    print("This batch_proc seems valid")

                print(eu.index_document(body=proc, index=ES_INDEX))
        else:
            print("Creating batch proc using parameters")
            print(args)

def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)

    verbose = {"positionals": ["-v", "--verbose"],
               "kwargs": {"dest": "verbose",
                          "action": "store_true",
                          "help": "Verbose mode."}}

    file = {"positionals": [FILE_OPTION],
            "kwargs": {"dest": "file",
                       "help": "Single json file that contains all batch proc information"}}

    provider = {"positionals": ["-p", "--provider"],
                "kwargs": {"dest": "provider",
                           "choices": ["LPCLOUD", "ASF"],
                           "default": "LPCLOUD",
                           "help": "Specify a provider for collection search. Default is LPCLOUD."}}

    collection = {"positionals": ["-c", "--collection-shortname"],
                  "kwargs": {"dest": "collection",
                             "choices": ["HLSL30", "HLSS30", "SENTINEL-1A_SLC", "SENTINEL-1B_SLC"],
                             "help": "The collection shortname for which you want to retrieve data."}}

    start_date = {"positionals": ["-s", "--start-date"],
                  "kwargs": {"dest": "start_date",
                             "default": None,
                             "help": "The ISO date time after which data should be retrieved. For Example, "
                                     "--start-date 2021-01-14T00:00:00Z"}}

    end_date = {"positionals": ["-e", "--end-date"],
                "kwargs": {"dest": "end_date",
                           "default": None,
                           "help": "The ISO date time before which data should be retrieved. For Example, --end-date "
                                   "2021-01-14T00:00:00Z"}}

    bbox = {"positionals": ["-b", "--bounds"],
            "kwargs": {"dest": "bbox",
                       "default": "-180,-90,180,90",
                       "help": "The bounding rectangle to filter result in. Format is W Longitude,S Latitude,"
                               "E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, "
                               "to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from "
                               "the command line. Default: \"-180,-90,180,90\"."}}

    job_queue = {"positionals": ["--job-queue"],
                 "kwargs": {"dest": "job_queue",
                            "help": "The queue to use for the scheduled download job."}}

    chunk_size = {"positionals": ["--chunk-size"],
                  "kwargs": {"dest": "chunk_size",
                             "type": int,
                             "help": "chunk-size = 1 means 1 tile per job. chunk-size > 1 means multiple (N) tiles "
                                     "per job"}}

    list_parser = subparsers.add_parser("list")

    view_parser = subparsers.add_parser("view")
    _add_id_arg(view_parser)

    delete_parser = subparsers.add_parser("delete")
    _add_id_arg(delete_parser)

    enable_parser = subparsers.add_parser("enable")
    _add_id_arg(enable_parser)
    enable_parser.add_argument("t_f", help="Enable or disable an existing batch proc", choices=["true", "false"])

    create_parser = subparsers.add_parser("create")
    create_parser_arg_list = [provider, collection, start_date, end_date, bbox, job_queue, chunk_size]
    _add_arguments(create_parser, [file], True)
    _add_arguments(create_parser, create_parser_arg_list)

    return parser


def _add_arguments(parser, arg_list, force_optional=False):
    for argument in arg_list:
        if (force_optional):
            parser.add_argument(*argument["positionals"], **argument["kwargs"], required=False)
        else:
            parser.add_argument(*argument["positionals"], **argument["kwargs"], required=FILE_OPTION not in sys.argv)


def _add_id_arg(parser):
    parser.add_argument("id", help="ElasticSearch ID of the batch process document.")


if __name__ == '__main__':
    batch_proc_once()
