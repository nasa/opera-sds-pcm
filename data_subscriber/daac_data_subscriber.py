#!/usr/bin/env python3


import argparse
import asyncio
import json
import logging
import netrc
import sys
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from smart_open import open

from data_subscriber.download import run_download
from data_subscriber.hls.hls_catalog_connection import get_hls_catalog_connection
from data_subscriber.query import update_url_index, run_query
from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection
from data_subscriber.aws_token import supply_token
from util.conf_util import SettingsConf

PRODUCT_PROVIDER_MAP = {"HLSL30": "LPCLOUD",
                        "HLSS30": "LPCLOUD",
                        "SENTINEL-1A_SLC": "ASF",
                        "SENTINEL-1B_SLC": "ASF"}


async def run(argv: list[str]):
    parser = create_parser()
    args = parser.parse_args(argv[1:])
    try:
        validate(args)
    except ValueError as v:
        raise v

    settings = SettingsConf().cfg
    edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
    cmr = settings["DAAC_ENVIRONMENTS"][args.endpoint]["BASE_URL"]
    netloc = urlparse(f"https://{edl}").netloc
    provider = PRODUCT_PROVIDER_MAP[args.collection] if hasattr(args, "collection") else args.provider

    if provider == "LPCLOUD":
        es_conn = get_hls_catalog_connection(logging.getLogger(__name__))
    elif provider == "ASF":
        es_conn = get_slc_catalog_connection(logging.getLogger(__name__))
    else:
        raise Exception("Unreachable")

    if args.file:
        with open(args.file, "r") as f:
            update_url_index(es_conn, f.readlines(), None, None, None)
        exit(0)

    loglevel = "DEBUG" if args.verbose else "INFO"
    logging.basicConfig(level=loglevel)
    logging.info("Log level set to " + loglevel)

    logging.info(f"{argv=}")

    is_running_outside_verdi_worker_context = not Path("_job.json").exists()
    if is_running_outside_verdi_worker_context:
        logging.info("Running outside of job context. Generating random job ID")
        job_id = uuid.uuid4()
    else:
        with open("_job.json", "r+") as job:
            logging.info("job_path: {}".format(job))
            local_job_json = json.load(job)
            logging.info(f"{local_job_json=!s}")
        job_id = local_job_json["job_info"]["job_payload"]["payload_task_id"]
    logging.info(f"{job_id=}")

    logging.info(f"{args.subparser_name=}")
    if not (args.subparser_name == "query" or args.subparser_name == "download" or args.subparser_name == "full"):
        raise Exception(f"Unsupported operation. {args.subparser_name=}")

    username, _, password = netrc.netrc().authenticators(edl)
    token = supply_token(edl, username, password)

    results = {}
    if args.subparser_name == "query" or args.subparser_name == "full":
        results["query"] = await run_query(args, token, es_conn, cmr, job_id, settings)
    if args.subparser_name == "download" or args.subparser_name == "full":
        results["download"] = run_download(args, token, es_conn, netloc, username, password, job_id)  # return None

    logging.info(f"{results=}")
    logging.info("END")

    return results


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

    endpoint = {"positionals": ["--endpoint"],
                "kwargs": {"dest": "endpoint",
                           "choices": ["OPS", "UAT"],
                           "default": "OPS",
                           "help": "Specify DAAC endpoint to use. Defaults to OPS."}}

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

    dry_run = {"positionals": ["--dry-run"],
               "kwargs": {"dest": "dry_run",
                          "action": "store_true",
                          "help": "Toggle for skipping physical downloads."}}

    smoke_run = {"positionals": ["--smoke-run"],
                 "kwargs": {"dest": "smoke_run",
                            "action": "store_true",
                            "help": "Toggle for processing a single tile."}}

    no_schedule_download = {"positionals": ["--no-schedule-download"],
                            "kwargs": {"dest": "no_schedule_download",
                                       "action": "store_true",
                                       "help": "Toggle for query only operation (no downloads)."}}

    release_version = {"positionals": ["--release-version"],
                       "kwargs": {"dest": "release_version",
                                  "help": "The release version of the download job-spec."}}

    job_queue = {"positionals": ["--job-queue"],
                 "kwargs": {"dest": "job_queue",
                            "help": "The queue to use for the scheduled download job."}}

    chunk_size = {"positionals": ["--chunk-size"],
                  "kwargs": {"dest": "chunk_size",
                             "type": int,
                             "help": "chunk-size = 1 means 1 tile per job. chunk-size > 1 means multiple (N) tiles "
                                     "per job"}}

    batch_ids = {"positionals": ["--batch-ids"],
                 "kwargs": {"dest": "batch_ids",
                            "nargs": "*",
                            "help": "A list of target tile IDs pending download."}}

    use_temporal = {"positionals": ["--use-temporal"],
                    "kwargs": {"dest": "use_temporal",
                               "action": "store_true",
                               "help": "Toggle for using temporal range rather than revision date (range) in the query."}}

    temporal_start_date = {"positionals": ["--temporal-start-date"],
                           "kwargs": {"dest": "temporal_start_date",
                                      "default": None,
                                      "help": "The ISO date time after which data should be retrieved. Only valid when --use-temporal is false/omitted. For Example, "
                                              "--temporal-start-date 2021-01-14T00:00:00Z"}}

    native_id = {"positionals": ["--native-id"],
                 "kwargs": {"dest": "native_id",
                            "help": "The native ID of a single product granule to be queried, overriding other query arguments if present. "
                                    "The native ID value supports the '*' and '?' wildcards."}}

    parser_arg_list = [verbose, file]
    _add_arguments(parser, parser_arg_list)

    full_parser = subparsers.add_parser("full")
    full_parser_arg_list = [verbose, endpoint, collection, start_date, end_date, bbox, minutes,
                            dry_run, smoke_run, no_schedule_download, release_version, job_queue,
                            chunk_size, batch_ids, use_temporal, temporal_start_date, native_id]
    _add_arguments(full_parser, full_parser_arg_list)

    query_parser = subparsers.add_parser("query")
    query_parser_arg_list = [verbose, endpoint, collection, start_date, end_date, bbox, minutes,
                             dry_run, smoke_run, no_schedule_download, release_version, job_queue, chunk_size,
                             native_id, use_temporal, temporal_start_date]
    _add_arguments(query_parser, query_parser_arg_list)

    download_parser = subparsers.add_parser("download")
    download_parser_arg_list = [verbose, file, endpoint, dry_run, smoke_run, provider,
                                batch_ids, start_date, end_date, use_temporal, temporal_start_date]
    _add_arguments(download_parser, download_parser_arg_list)

    return parser


def _add_arguments(parser, arg_list):
    for argument in arg_list:
        parser.add_argument(*argument["positionals"], **argument["kwargs"])


def validate(args):
    if hasattr(args, "bbox") and args.bbox:
        _validate_bounds(args.bbox)

    if hasattr(args, "start_date") and args.start_date:
        _validate_date(args.start_date, "start")

    if hasattr(args, "end_date") and args.end_date:
        _validate_date(args.end_date, "end")

    if hasattr(args, "minutes") and args.minutes:
        _validate_minutes(args.minutes)


def _validate_bounds(bbox):
    bounds = bbox.split(",")
    value_error = ValueError(
        f"Error parsing bounds: {bbox}. Format is <W Longitude>,<S Latitude>,<E Longitude>,<N Latitude> without spaces")

    if len(bounds) != 4:
        raise value_error

    for b in bounds:
        try:
            float(b)
        except ValueError:
            raise value_error


def _validate_date(date, prefix="start"):
    try:
        datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise ValueError(
            f"Error parsing {prefix} date: {date}. Format must be like 2021-01-14T00:00:00Z")


def _validate_minutes(minutes):
    try:
        int(minutes)
    except ValueError:
        raise ValueError(f"Error parsing minutes: {minutes}. Number must be an integer.")


if __name__ == "__main__":
    asyncio.run(run(sys.argv))
