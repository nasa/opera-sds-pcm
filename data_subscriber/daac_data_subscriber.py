#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import netrc
import re
import sys
import uuid
from collections import defaultdict, namedtuple
from datetime import datetime
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse

from more_itertools import first
from smart_open import open

import data_subscriber
from commons.logger import NoJobUtilsFilter, NoBaseFilter
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import CMR_COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.download import run_download
from data_subscriber.hls.hls_catalog_connection import get_hls_catalog_connection
from data_subscriber.query import update_url_index, run_query
from data_subscriber.rtc import evaluator
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.rtc.rtc_job_submitter import submit_dswx_s1_job_submissions_tasks
from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection
from data_subscriber.survey import run_survey
from rtc_utils import rtc_product_file_revision_regex
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id, is_running_outside_verdi_worker_context


@exec_wrapper
def main():
    asyncio.run(run(sys.argv))


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

    edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
    username, _, password = netrc.netrc().authenticators(edl)
    token = supply_token(edl, username, password)

    results = {}
    if args.subparser_name == "survey":
        await run_survey(args, token, cmr, settings)
    if args.subparser_name == "query" or args.subparser_name == "full":
        results["query"] = await run_query(args, token, es_conn, cmr, job_id, settings)
    if args.subparser_name == "download" or args.subparser_name == "full":
        netloc = urlparse(f"https://{edl}").netloc

        if args.provider == "ASF-RTC":
            results["download"] = await run_rtc_download(args, token, es_conn, netloc, username, password, job_id)
        else:
            results["download"] = await run_download(args, token, es_conn, netloc, username, password, job_id)  # return None

    logger.info(f"{results=}")
    logger.info("END")

    return results


async def run_rtc_download(args, token, es_conn, netloc, username, password, job_id):
    provider = args.provider  # "ASF-RTC"
    settings = SettingsConf().cfg

    if not is_running_outside_verdi_worker_context():
        job_context = JobContext("_context.json").ctx
        product_metadata = job_context["product_metadata"]
        logger.info(f"{product_metadata=}")

    logger.info("evaluating available burst sets")
    affected_mgrs_set_id_acquisition_ts_cycle_indexes = args.batch_ids
    logger.info(f"{affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")
    fully_covered_mgrs_sets, target_covered_mgrs_sets, incomplete_mgrs_sets = await evaluator.main(
        mgrs_set_id_acquisition_ts_cycle_indexes=affected_mgrs_set_id_acquisition_ts_cycle_indexes,
        coverage_target=settings["DSWX_S1_COVERAGE_TARGET"]
    )

    processable_mgrs_sets = {**incomplete_mgrs_sets, **fully_covered_mgrs_sets}

    # convert to "batch_id" mapping
    batch_id_to_products_map = defaultdict(set)
    for mgrs_set_id, product_burst_sets in processable_mgrs_sets.items():
        for product_burstset in product_burst_sets:
            rtc_granule_id_to_product_docs_map = first(product_burstset)
            first_product_doc_list = first(rtc_granule_id_to_product_docs_map.values())
            first_product_doc = first(first_product_doc_list)
            acquisition_cycle = first_product_doc["acquisition_cycle"]
            batch_id = "{}${}".format(mgrs_set_id, acquisition_cycle)
            batch_id_to_products_map[batch_id] = product_burstset

    succeeded = []
    failed = []

    # create args for downloading products
    Namespace = namedtuple(
        "Namespace",
        ["provider", "transfer_protocol", "batch_ids", "dry_run", "smoke_run"],
        defaults=[provider, args.transfer_protocol, None, args.dry_run, args.smoke_run]
    )

    uploaded_batch_id_to_products_map = {}
    uploaded_batch_id_to_s3paths_map = {}
    for batch_id, product_burstset in batch_id_to_products_map.items():
        args_for_downloader = Namespace(provider=provider, batch_ids=[batch_id])
        downloader = data_subscriber.download.DaacDownload.get_download_object(args=args_for_downloader)

        run_download_kwargs = {
            "token": token,
            "es_conn": es_conn,
            "netloc": netloc,
            "username": username,
            "password": password,
            "job_id": job_id
        }

        product_to_product_filepaths_map: dict[str, set[Path]] = await downloader.run_download(
            args=args_for_downloader, **run_download_kwargs, rm_downloads_dir=False)

        logger.info(f"Uploading MGRS burst set files to S3")
        burst_id_to_files_to_upload = defaultdict(set)
        for product_id, fp_set in product_to_product_filepaths_map.items():
            for fp in fp_set:
                match_product_id = re.match(rtc_product_file_revision_regex, product_id)
                burst_id = match_product_id.group("burst_id")
                burst_id_to_files_to_upload[burst_id].add(fp)

        s3paths: list[str] = []
        for burst_id, filepaths in burst_id_to_files_to_upload.items():
            s3paths.extend(
                concurrent_s3_client_try_upload_file(
                    bucket=settings["DATASET_BUCKET"],
                    key_prefix=f"tmp/dswx_s1/{batch_id}/{burst_id}",
                    files=filepaths
                )
            )

        uploaded_batch_id_to_products_map[batch_id] = product_burstset
        uploaded_batch_id_to_s3paths_map[batch_id] = s3paths

        logger.info(f"Submitting MGRS burst set download job {batch_id=}, num_bursts={len(product_burstset)}")
        # create args for job-submissions
        args_for_job_submitter = namedtuple(
            "Namespace",
            ["chunk_size", "release_version"],
            defaults=[1, args.release_version]
        )()
        if args.dry_run:
            logger.info(f"{args.dry_run=}. Skipping job submission. Producing mock job ID")
            results = [uuid.uuid4()]
        else:
            job_submission_tasks = submit_dswx_s1_job_submissions_tasks(uploaded_batch_id_to_s3paths_map, args_for_job_submitter)
            results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)

        suceeded_batch = [job_id for job_id in results if isinstance(job_id, str)]
        failed_batch = [e for e in results if isinstance(e, Exception)]
        if suceeded_batch:
            for products_map in uploaded_batch_id_to_products_map[batch_id]:
                for products in products_map.values():
                    for product in products:
                        if not product.get("mgrs_set_id_jobs_dict"):
                            product["mgrs_set_id_jobs_dict"] = {}
                        if not product.get("mgrs_set_id_jobs_submitted_for"):
                            product["mgrs_set_id_jobs_submitted_for"] = []

                        if not product.get("ati_jobs_dict"):
                            product["ati_jobs_dict"] = {}
                        if not product.get("ati_jobs_submitted_for"):
                            product["ati_jobs_submitted_for"] = []

                        if not product.get("dswx_s1_jobs_ids"):
                            product["dswx_s1_jobs_ids"] = []

                        # use doc obj to pass params to elasticsearch client
                        product["mgrs_set_id_jobs_dict"][batch_id.split("$")[0]] = first(suceeded_batch)
                        product["mgrs_set_id_jobs_submitted_for"].append(batch_id.split("$")[0])

                        product["ati_jobs_dict"][batch_id] = first(suceeded_batch)
                        product["ati_jobs_submitted_for"].append(batch_id)

                        product["dswx_s1_jobs_ids"].append(first(suceeded_batch))

            if args.dry_run:
                logger.info(f"{args.dry_run=}. Skipping marking jobs as downloaded. Producing mock job ID")
                pass
            else:
                from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
                es_conn: RTCProductCatalog
                es_conn.mark_products_as_job_submitted({batch_id: uploaded_batch_id_to_products_map[batch_id]})

            succeeded.extend(suceeded_batch)
            failed.extend(failed_batch)

            # manual cleanup since we needed to preserve downloads for manual s3 uploads
            for fp in chain.from_iterable(burst_id_to_files_to_upload.values()):
                fp.unlink(missing_ok=True)
            logger.info("Removed downloads from disk")

    return {
        "success": succeeded,
        "fail": failed
    }


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
                                start_date, end_date, use_temporal, temporal_start_date, transfer_protocol, release_version]
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

    main()
