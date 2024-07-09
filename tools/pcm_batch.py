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

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

from util.conf_util import SettingsConf

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

SETTINGS = SettingsConf(file=str(Path("/export/home/hysdsops/.sds/config"))).cfg
GRQ_IP = SETTINGS["GRQ_PVT_IP"]

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
LOGGER = logging.getLogger('pcm_batch')
LOGGER.setLevel(logging.INFO)

eu = ElasticsearchUtility('http://%s:9200' % GRQ_IP, LOGGER)
LOGGER.info("Connected to %s" % str(eu.es_url))

FILE_OPTION = '--file'


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


def _validate_proc(proc):
    return True


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

    elif args.subparser_name == 'create':
        if args.file is not None:
            print("Creating batch proc using file %s" % args.file)
            with open(args.file) as f:
                proc = json.load(f)
                print(proc)

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
