#!/usr/bin/env python3

import argparse
from datetime import datetime

from data_subscriber.cmr import (Collection,
                                 Endpoint,
                                 Provider,
                                 CMR_TIME_FORMAT)

def create_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)

    verbose = {"positionals": ["-v", "--verbose"],
               "kwargs": {"dest": "verbose",
                          "action": "store_true",
                          "help": "Verbose mode, enables debug level logging. If provided, "
                                  "takes precedence over the --quiet option."}}

    quiet ={"positionals": ["-q", "--quiet"],
            "kwargs": {"dest": "quiet",
                       "action": "store_true",
                       "help": "Quiet mode, only warning and error level messages will be logged."}}

    endpoint = {"positionals": ["--endpoint"],
                "kwargs": {"dest": "endpoint",
                           "choices": [endpoint.value for endpoint in Endpoint],
                           "default": Endpoint.OPS.value,
                           "help": "Specify the DAAC endpoint to use."}}

    provider = {"positionals": ["-p", "--provider"],
                "kwargs": {"dest": "provider",
                           "choices": [provider.value for provider in Provider],
                           "help": "Specify a provider for collection search."}}

    collection = {
        "positionals": ["-c", "--collection-shortname"],
        "kwargs": {
            "dest": "collection",
            "choices": [collection.value for collection in Collection],
            "required": True,
            "help": "The collection shortname to retrieve data for."
        }
    }

    start_date = {"positionals": ["-s", "--start-date"],
                  "kwargs": {"dest": "start_date",
                             "default": None,
                             "help": "The ISO date time after which data should "
                                     "be retrieved. For Example, "
                                     "--start-date 2021-01-14T00:00:00Z"}}

    end_date = {"positionals": ["-e", "--end-date"],
                "kwargs": {"dest": "end_date",
                           "default": None,
                           "help": "The ISO date time before which data should "
                                   "be retrieved. For Example, --end-date "
                                   "2021-01-14T00:00:00Z"}}

    bbox = {"positionals": ["-b", "--bounds"],
            "kwargs": {"dest": "bbox",
                       "default": "-180,-90,180,90",
                       "help": "The bounding rectangle to filter result in. "
                               "Format is W Longitude, S Latitude, "
                               "E Longitude, N Latitude (without spaces). "
                               "Due to an issue with parsing arguments, "
                               "to use this command, please use the "
                               "-b=\"-180,-90,180,90\" syntax when calling from "
                               "the command line."}}

    minutes = {"positionals": ["-m", "--minutes"],
               "kwargs": {"dest": "minutes",
                          "type": int,
                          "default": 60,
                          "help": "How far back in time, in minutes, should the "
                                  "script look for data. If running this "
                                  "script as a cron, this value should be equal "
                                  "to or greater than how often your cron runs."}}

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
                                       "help": "Toggle for query only operation "
                                               "(no downloads)."}}

    release_version = {"positionals": ["--release-version"],
                       "kwargs": {"dest": "release_version",
                                  "help": "The release version of the download job-spec."}}

    job_queue = {"positionals": ["--job-queue"],
                 "kwargs": {"dest": "job_queue",
                            "help": "The queue to use for the scheduled download job."}}

    chunk_size = {"positionals": ["--chunk-size"],
                  "kwargs": {"dest": "chunk_size",
                             "type": int,
                             "help": "chunk-size = 1 means 1 tile per job. "
                                     "chunk-size > 1 means multiple (N) tiles "
                                     "per job"}}

    max_revision = {"positionals": ["--max-revision"],
                  "kwargs": {"dest": "max_revision",
                             "type": int,
                             "default": 1000,
                             "help": "The maximum number of revision-id to process. "
                                     "If the granule's revision-id is higher "
                                     "than this, it is ignored."}}

    batch_ids = {"positionals": ["--batch-ids"],
                 "kwargs": {"dest": "batch_ids",
                            "nargs": "*",
                            "help": "A list of target tile IDs pending download."}}

    use_temporal = {"positionals": ["--use-temporal"],
                    "kwargs": {"dest": "use_temporal",
                               "action": "store_true",
                               "help": "Toggle for using temporal range rather "
                                       "than revision date (range) in the query."}}

    temporal_start_date = {"positionals": ["--temporal-start-date"],
                           "kwargs": {"dest": "temporal_start_date",
                                      "default": None,
                                      "help": "The ISO date time after which data "
                                              "should be retrieved. Only valid when "
                                              "--use-temporal is false/omitted. "
                                              "For Example, --temporal-start-date 2021-01-14T00:00:00Z"}}

    native_id = {"positionals": ["--native-id"],
                 "kwargs": {"dest": "native_id",
                            "help": "The native ID of a single product granule to "
                                    "be queried, overriding other query arguments "
                                    "if present. The native ID value supports the "
                                    "'*' and '?' wildcards."}}

    k = {"positionals": ["--k"],
                  "kwargs": {"dest": "k",
                             "type": int,
                             "help": "k is used only in DISP-S1 processing."}}

    coverage_percent = {"positionals": ["--coverage-percent"],
         "kwargs": {"dest": "coverage_target",
                    "type": int,
                    "help": "For DSWx-S1 processing."}}

    coverage_num = {"positionals": ["--coverage-num"],
                                     "kwargs": {"dest": "coverage_target_num",
                                                "type": int,
                                                "help": "For DSWx-S1 processing."}}

    grace_mins = {"positionals": ["--grace-mins"],
                        "kwargs": {"dest": "grace_mins",
                                   "type": int,
                                   "help": "CURRENTLY NOT USED. Used when querying for CSLC input files."}}

    m = {"positionals": ["--m"],
         "kwargs": {"dest": "m",
                    "type": int,
                    "help": "m is used only in DISP-S1 processing."}}

    proc_mode = {"positionals": ["--processing-mode"],
               "kwargs": {"dest": "proc_mode",
                          "default": "forward",
                          "choices": ["forward", "reprocessing", "historical"],
                          "help": "Processing mode changes SLC data processing behavior"}}

    include_regions = {"positionals": ["--include-regions"],
                    "kwargs": {"dest": "include_regions",
                               "help": "Only process granules whose bounding bbox "
                                       "intersects with the region specified. "
                                       "Comma-separated list. Only applies in "
                                       "Historical processing mode."}}

    exclude_regions = {"positionals": ["--exclude-regions"],
                    "kwargs": {"dest": "exclude_regions",
                               "help": "Only process granules whose bounding bbox "
                                       "do not intersect with these regions. "
                                       "Comma-separated list. Only applies in "
                                       "Historical processing mode."}}

    frame_id = {"positionals": ["--frame-id"],
                       "kwargs": {"dest": "frame_id",
                                  "help": "Only applies to DISP-S1 processing. "
                                          "CSLC frame id to process."}}

    step_hours = {"positionals": ["--step-hours"],
                           "kwargs": {"dest": "step_hours",
                            "default": 1,
                            "help": "Number of hours to step for each survey iteration."}}

    out_csv = {"positionals": ["--out-csv"],
                           "kwargs": {"dest": "out_csv",
                            "default": "cmr_survey.csv",
                            "help": "Specify name of the output CSV file."}}

    transfer_protocol = {"positionals": ["-x", "--transfer-protocol"],
               "kwargs": {"dest": "transfer_protocol",
                          "choices": ["s3", "https", "auto"],
                          "default": "auto",
                          "type": str.lower,
                          "help": "The protocol used for retrieving data, "
                                  "HTTPS or S3 or AUTO."}}

    parser_arg_list = [verbose, quiet]
    _add_arguments(parser, parser_arg_list)

    survey_parser = subparsers.add_parser("survey",
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    survey_parser_arg_list = [verbose, quiet, endpoint, provider, collection,
                              start_date, end_date, bbox, minutes, max_revision,
                              smoke_run, native_id, frame_id, use_temporal,
                              temporal_start_date, step_hours, out_csv]
    _add_arguments(survey_parser, survey_parser_arg_list)

    full_parser = subparsers.add_parser("full",
                                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    full_parser_arg_list = [verbose, quiet, endpoint, collection, start_date, end_date,
                            bbox, minutes, k, m, grace_mins,
                            dry_run, smoke_run, no_schedule_download,
                            release_version, job_queue, chunk_size, max_revision,
                            batch_ids, use_temporal, temporal_start_date, native_id,
                            transfer_protocol, frame_id, include_regions,
                            exclude_regions, proc_mode]
    _add_arguments(full_parser, full_parser_arg_list)
    _add_arguments(full_parser.add_mutually_exclusive_group(required=False), [coverage_percent, coverage_num])

    query_parser = subparsers.add_parser("query",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    query_parser_arg_list = [verbose, quiet, endpoint, provider, collection, start_date, end_date,
                             bbox, minutes, k, m, grace_mins,
                             dry_run, smoke_run, no_schedule_download,
                             release_version, job_queue, chunk_size, max_revision,
                             native_id, use_temporal, temporal_start_date, transfer_protocol,
                             frame_id, include_regions, exclude_regions, proc_mode]
    _add_arguments(query_parser, query_parser_arg_list)
    _add_arguments(query_parser.add_mutually_exclusive_group(required=False), [coverage_percent, coverage_num])


    download_parser = subparsers.add_parser("download",
                                            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    download_parser_arg_list = [verbose, quiet, endpoint, dry_run, smoke_run, provider,
                                batch_ids, start_date, end_date, use_temporal, proc_mode,
                                temporal_start_date, transfer_protocol, release_version]
    _add_arguments(download_parser, download_parser_arg_list)
    _add_arguments(download_parser.add_mutually_exclusive_group(required=False), [coverage_percent, coverage_num])

    return parser


def _add_arguments(parser, arg_list):
    for argument in arg_list:
        parser.add_argument(*argument["positionals"], **argument["kwargs"])


def validate_args(args):
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
        f"Error parsing bounds: {bbox}. "
        f"Format is <W Longitude>,<S Latitude>,<E Longitude>,<N Latitude> without spaces"
    )

    if len(bounds) != 4:
        raise value_error

    for b in bounds:
        try:
            float(b)
        except ValueError:
            raise value_error


def _validate_date(date, prefix="start"):
    try:
        datetime.strptime(date, CMR_TIME_FORMAT)
    except ValueError:
        raise ValueError(
            f"Error parsing {prefix} date: {date}. "
            f"Format must be like 2021-01-14T00:00:00Z")


def _validate_minutes(minutes):
    try:
        int(minutes)
    except ValueError:
        raise ValueError(f"Error parsing minutes: {minutes}. "
                         f"Number must be an integer.")
