#!/usr/bin/env python3

from __future__ import print_function
import sys
import json
import os
import re
from distutils.util import strtobool
from typing import Dict
import dateutil.parser
import requests
import argparse

from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
import logging

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

# Requires these 4 env variables
_ENV_GRQ_IP = "GRQ_IP"

for ev in [_ENV_GRQ_IP]:
    if ev not in os.environ:
        raise RuntimeError("Need to specify %s in environment." % ev)
GRQ_IP = os.environ[_ENV_GRQ_IP]

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('pcm_batch')
LOGGER.setLevel(logging.INFO)

eu = ElasticsearchUtility('http://%s:9200' % GRQ_IP, LOGGER)
LOGGER.info("Connected to %s" % str(eu.es_url))


def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def view_proc(id):
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
        print(k, ' ' * (30 - len(k)), v)


def batch_proc_once():
    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])

    if args.subparser_name == "list":
        procs = eu.query(index=ES_INDEX)

        print("%d Batch Procs Found" % len(procs))

        for proc in procs:
            doc_id = proc['_id']
            proc = proc['_source']
            p = SimpleNamespace(**proc)
            print(doc_id, p.label, p.enabled)

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

        exit(0)

    exit(0)

    procs = eu.query(index=ES_INDEX)  # TODO: query for only enabled docs
    for proc in procs:
        doc_id = proc['_id']
        proc = proc['_source']
        p = SimpleNamespace(**proc)

        # If this batch proc is disabled, continue TODO: this goes away when we change the query above
        if p.enabled == False:
            continue

        now = datetime.utcnow()
        new_last_run_date = datetime.strptime(p.last_run_date, ES_DATETIME_FORMAT) + timedelta(
            minutes=p.run_interval_mins)

        # If it's not time to run yet, just continue
        if new_last_run_date > now:
            continue

        # Update last_run_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "enabled": True, }},
                           index=ES_INDEX)

        data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
        data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)

        # Start date time is when the last successful process data time.
        # If this is before the data start time, which may be the case when this batch_proc is first run,
        # change it to the data start time.
        s_date = datetime.strptime(p.last_successful_proc_data_date, ES_DATETIME_FORMAT)
        if s_date < data_start_date:
            s_date = data_start_date

        # End date time is when the start data time plus data increment time in minutes.
        # If this is after the data end time, which would be the case when this is the very last iteration of this proc,
        # change it to the data end time.
        e_date = s_date + timedelta(minutes=p.data_date_incr_mins)
        if e_date > data_end_date:
            e_date = data_end_date

        # See if we've reached the end of this batch proc. If so, disable it.
        if s_date >= data_end_date:
            print(p.label, "Batch Proc completed processing. It is now disabled")
            eu.update_document(id=doc_id,
                               body={"doc_as_upsert": True,
                                     "doc": {
                                         "enabled": False, }},
                               index=ES_INDEX)
            continue

        # update last_attempted_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_attempted_proc_data_date": e_date, }},
                           index=ES_INDEX)

        job_name = "data-subscriber-query-timer-{}_{}-{}".format(p.label, s_date.strftime(ES_DATETIME_FORMAT),
                                                                 e_date.strftime(ES_DATETIME_FORMAT))
        # submit mozart job
        job_success = submit_job(job_name, job_spec, job_params, queue, tags)

        # Update last_successful_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_successful_proc_data_date": e_date, }},
                           index=ES_INDEX)

        return job_success


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)

    verbose = {"positionals": ["-v", "--verbose"],
               "kwargs": {"dest": "verbose",
                          "action": "store_true",
                          "help": "Verbose mode."}}

    file = {"positionals": ["-f", "--file"],
            "kwargs": {"dest": "file",
                       "help": "Path to file with newline-separated URIs to ingest into data product ES index (to be downloaded later)."}}

    provider = {"positionals": ["-p", "--provider"],
                "kwargs": {"dest": "provider",
                           "choices": ["LPCLOUD", "ASF"],
                           "default": "LPCLOUD",
                           "help": "Specify a provider for collection search. Default is LPCLOUD."}}

    collection = {"positionals": ["-c", "--collection-shortname"],
                  "kwargs": {"dest": "collection",
                             "choices": ["HLSL30", "HLSS30", "SENTINEL-1A_SLC", "SENTINEL-1B_SLC"],
                             "required": True,
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

    minutes = {"positionals": ["-m", "--minutes"],
               "kwargs": {"dest": "minutes",
                          "type": int,
                          "default": 60,
                          "help": "How far back in time, in minutes, should the script look for data. If running this "
                                  "script as a cron, this value should be equal to or greater than how often your "
                                  "cron runs (default: 60 minutes)."}}

    job_queue = {"positionals": ["--job-queue"],
                 "kwargs": {"dest": "job_queue",
                            "help": "The queue to use for the scheduled download job."}}

    chunk_size = {"positionals": ["--chunk-size"],
                  "kwargs": {"dest": "chunk_size",
                             "type": int,
                             "help": "chunk-size = 1 means 1 tile per job. chunk-size > 1 means multiple (N) tiles "
                                     "per job"}}

    use_temporal = {"positionals": ["--use-temporal"],
                    "kwargs": {"dest": "use_temporal",
                               "action": "store_true",
                               "help": "Toggle for using temporal range rather than revision date (range) in the query."}}

    temporal_start_date = {"positionals": ["--temporal-start-date"],
                           "kwargs": {"dest": "temporal_start_date",
                                      "default": None,
                                      "help": "The ISO date time after which data should be retrieved. Only valid when --use-temporal is false/omitted. For Example, "
                                              "--temporal-start-date 2021-01-14T00:00:00Z"}}

    list_parser = subparsers.add_parser("list")

    view_parser = subparsers.add_parser("view")
    _add_id_arg(view_parser)

    delete_parser = subparsers.add_parser("delete")
    _add_id_arg(delete_parser)

    enable_parser = subparsers.add_parser("enable")
    _add_id_arg(enable_parser)
    enable_parser.add_argument("t_f", help="Enable or disable an existing batch proc", choices=["true", "false"])

    create_parser = subparsers.add_parser("create")
    create_parser_arg_list = [verbose, provider, collection, start_date, end_date, bbox, minutes,
                              job_queue,
                              chunk_size, use_temporal, temporal_start_date]
    _add_arguments(create_parser, create_parser_arg_list)

    return parser


def _add_arguments(parser, arg_list):
    for argument in arg_list:
        parser.add_argument(*argument["positionals"], **argument["kwargs"])


def _add_id_arg(parser):
    parser.add_argument("id", help="ElasticSearch ID of the batch process document.")


if __name__ == '__main__':
    batch_proc_once()