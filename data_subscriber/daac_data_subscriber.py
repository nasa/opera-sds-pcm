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

from commons.logger import NoJobUtilsFilter, NoBaseFilter
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import CMR_COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.download import run_download
from data_subscriber.hls.hls_catalog_connection import get_hls_catalog_connection
from data_subscriber.query import update_url_index, run_query
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection
from data_subscriber.survey import run_survey
from util.conf_util import SettingsConf
from util.exec_util import exec_wrapper


@exec_wrapper
async def run(argv: list[str]):
    try:
        validate(args)
    except ValueError as v:
        raise v

    es_conn = supply_es_conn(args)

    if args.file:
        with open(args.file, "r") as f:
            update_url_index(es_conn, f.readlines(), None, None, None)
        exit(0)

    logger.info(f"{argv=}")

    job_id = supply_job_id()
    logger.info(f"{job_id=}")

    settings = SettingsConf().cfg
    cmr = settings["DAAC_ENVIRONMENTS"][args.endpoint]["BASE_URL"]

    results = {}
    if args.subparser_name == "survey":
        edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
        username, _, password = netrc.netrc().authenticators(edl)
        token = supply_token(edl, username, password)

        await run_survey(args, token, cmr, settings)
    if args.subparser_name == "query" or args.subparser_name == "full":
        edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
        username, _, password = netrc.netrc().authenticators(edl)
        token = supply_token(edl, username, password)

        results["query"] = await run_query(args, token, es_conn, cmr, job_id, settings)
    if args.subparser_name == "download" or args.subparser_name == "full":
        edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
        username, _, password = netrc.netrc().authenticators(edl)
        token = supply_token(edl, username, password)
        netloc = urlparse(f"https://{edl}").netloc

        results["download"] = run_download(args, token, es_conn, netloc, username, password, job_id)  # return None

    logger.info(f"{results=}")
    logger.info("END")

    return results


def supply_job_id():
    is_running_outside_verdi_worker_context = not Path("_job.json").exists()
    if is_running_outside_verdi_worker_context:
        logger.info("Running outside of job context. Generating random job ID")
        job_id = uuid.uuid4()
    else:
        with open("_job.json", "r+") as job:
            logger.info("job_path: {}".format(job))
            local_job_json = json.load(job)
            logger.info(f"{local_job_json=!s}")
        job_id = local_job_json["job_info"]["job_payload"]["payload_task_id"]

    return job_id


def supply_es_conn(args):
    provider = CMR_COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection] if hasattr(args, "collection") else args.provider
    if provider == "LPCLOUD":
        es_conn = get_hls_catalog_connection(logging.getLogger(__name__))
    elif provider in ("ASF", "ASF-SLC"):
        es_conn = get_slc_catalog_connection(logging.getLogger(__name__))
    elif provider == "ASF-RTC":
        es_conn = RTCProductCatalog(logging.getLogger(__name__))
    elif provider == "ASF-CSLC":
        raise NotImplementedError()
    else:
        raise AssertionError(f"Unsupported {provider=}")

    return es_conn


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
                           "choices": ["LPCLOUD", "ASF", "ASF-SLC", "ASF-RTC", "ASF-CSLC"],
                           "default": "LPCLOUD",
                           "help": "Specify a provider for collection search. Default is LPCLOUD."}}

    collection = {
        "positionals": ["-c", "--collection-shortname"],
        "kwargs": {
            "dest": "collection",
            "choices": [
                "HLSL30",
                "HLSS30",
                "SENTINEL-1A_SLC",
                "SENTINEL-1B_SLC",
                "OPERA_L2_RTC-S1_V1",
                "OPERA_L2_CSLC-S1_V1"
            ],
            "required": True,
            "help": "The collection shortname for which you want to retrieve data."
        }
    }

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
    max_revision = {"positionals": ["--max-revision"],
                  "kwargs": {"dest": "max_revision",
                             "type": int,
                             "default": 1000,
                             "help": "The maximum number of revision-id to process. If the granule's revision-id is higher than this, it is ignored."}}

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

    proc_mode = {"positionals": ["--processing-mode"],
               "kwargs": {"dest": "proc_mode",
                          "default": "forward",
                          "choices": ["forward", "reprocessing", "historical"],
                          "help": "Processing mode changes SLC data processing behavior"}}

    include_regions = {"positionals": ["--include-regions"],
                    "kwargs": {"dest": "include_regions",
                               "help": "Only process granules whose bounding bbox intersects with the region specified. Comma-separated list. Only applies in Historical processing mode."}}

    exclude_regions = {"positionals": ["--exclude-regions"],
                    "kwargs": {"dest": "exclude_regions",
                               "help": "Only process granules whose bounding bbox do not intersect with these regions. Comma-separated list. Only applies in Historical processing mode."}}

    step_hours = {"positionals": ["--step-hours"],
                           "kwargs": {"dest": "step_hours",
                            "default": 1,
                            "help": "Number of hours to step for each survey iteration"}}

    out_csv = {"positionals": ["--out-csv"],
                           "kwargs": {"dest": "out_csv",
                            "default": "cmr_survey.csv",
                            "help": "Specify name of the output CSV file"}}

    transfer_protocol = {"positionals": ["-x", "--transfer-protocol"],
               "kwargs": {"dest": "transfer_protocol",
                          "choices": ["s3", "https", "auto"],
                          "default": "auto",
                          "help": "The protocol used for retrieving data, HTTPS or S3 or AUTO, default of auto"}}


    parser_arg_list = [verbose, file]
    _add_arguments(parser, parser_arg_list)

    survey_parser = subparsers.add_parser("survey")
    survey_parser_arg_list = [verbose, endpoint, provider, collection, start_date, end_date, bbox, minutes, max_revision,
                              smoke_run, native_id, use_temporal, temporal_start_date, step_hours, out_csv]
    _add_arguments(survey_parser, survey_parser_arg_list)

    full_parser = subparsers.add_parser("full")
    full_parser_arg_list = [verbose, endpoint, collection, start_date, end_date, bbox, minutes,
                            dry_run, smoke_run, no_schedule_download, release_version, job_queue, chunk_size, max_revision,
                            batch_ids, use_temporal, temporal_start_date, native_id, transfer_protocol,
                            include_regions, exclude_regions, proc_mode]
    _add_arguments(full_parser, full_parser_arg_list)

    query_parser = subparsers.add_parser("query")
    query_parser_arg_list = [verbose, endpoint, collection, start_date, end_date, bbox, minutes,
                             dry_run, smoke_run, no_schedule_download, release_version, job_queue, chunk_size, max_revision,
                             native_id, use_temporal, temporal_start_date, transfer_protocol,
                             include_regions, exclude_regions, proc_mode]
    _add_arguments(query_parser, query_parser_arg_list)

    download_parser = subparsers.add_parser("download")
    download_parser_arg_list = [verbose, file, endpoint, dry_run, smoke_run, provider, batch_ids,
                                start_date, end_date, use_temporal, temporal_start_date, transfer_protocol]
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
    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])

    loglevel = "DEBUG" if args.verbose else "INFO"
    logging.basicConfig(level=loglevel)
    logger = logging.getLogger(__name__)
    logger.info("Log level set to " + loglevel)

    logger_hysds_commons = logging.getLogger("hysds_commons")
    logger_hysds_commons.addFilter(NoJobUtilsFilter())

    logger_elasticsearch = logging.getLogger("elasticsearch")
    logger_elasticsearch.addFilter(NoBaseFilter())

    asyncio.run(run(sys.argv))
