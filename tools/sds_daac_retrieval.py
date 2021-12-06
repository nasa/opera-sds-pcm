#!/usr/bin/env python

"""

Tool to query the DAAC and retrieve products.

author mcayanan

"""

import os
import requests
import argparse
import traceback
import sys
import csv
import json
import getpass
import multiprocessing as mp

from pcm_commons.daac_retrieval.cmr import get_download_urls, get_cmr_token, delete_token, get_catalog_or_product_urls
from pcm_commons.daac_retrieval.upload_utils import upload_to_s3, upload_signal_to_s3, upload_failed_signal_to_s3


from util.conf_util import YamlConf
from util.common_util import convert_datetime
from util.common_util import get_source_includes
from util.common_util import get_latest_product_sort_list

from commons.logger import logger
from commons.logger import LogLevels

from commons.es_connection import get_grq_es
from commons.constants import product_metadata as pm

from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY

from urllib.parse import urlparse

from smart_open import open


DATE_FORMAT = "%Y-%m-%d"

DAAC_GRANULE_SEARCH_ENDPOINT = "search/granules.umm_json"

COLLECTION_CONCEPT_ID = "collection_concept_id"

ancillary_es = get_grq_es(logger)  # getting GRQ's es connection

IGNORE_FILES = [
    "context.json",
    "dataset.json",
    "archive.xml",
    "iso.xml",
    "cmr.json",
    "md5",
    "met.orig"
]


class SessionWithHeaderRedirection(requests.Session):
    """
    Borrowed from https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != AUTH_HOST and \
                    original_parsed.hostname != AUTH_HOST:
                del headers['Authorization']
        return


def __get_source_includes():
    source_includes = get_source_includes()
    source_includes.append(pm.DAAC_CATALOG_ID)
    source_includes.append(pm.DAAC_CATALOG_URL)
    source_includes.append(pm.DAAC_PRODUCT_FILE_URLS)
    return source_includes


def get_products_for_day(product_type, date, crid=None, size=10,
                         range_begin_date_time_key="metadata.{}".format(pm.RANGE_BEGINNING_DATE_TIME)):
    end_date = date + timedelta(hours=23, minutes=59,
                                seconds=59, microseconds=999999)

    range_beginning_date_time_key = range_begin_date_time_key
    conditions = None
    if crid:
        conditions = {
            pm.COMPOSITE_RELEASE_ID: crid
        }
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            range_beginning_date_time_key: {
                                "gte": convert_datetime(date)}
                        }
                    },
                    {
                        "range": {
                            range_beginning_date_time_key: {
                                "lte": convert_datetime(end_date)}
                        }
                    },
                ],
            }
        }
    }
    if conditions:
        query["query"]["bool"]["must"] = ancillary_es.construct_bool_query(
            conditions)

    sort_clause = {
        "sort": [{
            range_beginning_date_time_key: {
                "order": "asc"
            }
        }]
    }
    query.update(sort_clause)

    logger.info("Query: {}".format(json.dumps(query)))

    paged_result = ancillary_es.es.search(body=query, index="grq_*_{}".format(product_type.lower()),
                                          _source_includes=__get_source_includes(), scroll="30m", size=size)
    logger.info("Paged Result: {}".format(json.dumps(paged_result, indent=2)))
    return paged_result


def get_latest_records_for_day(product_type, date, crid=None,
                               range_begin_date_time_key="metadata.{}".format(pm.RANGE_BEGINNING_DATE_TIME)):
    conditions = None
    if crid:
        conditions = {
            "metadata.{}.keyword".format(pm.COMPOSITE_RELEASE_ID): crid
        }
    end_date = date + timedelta(hours=23, minutes=59,
                                seconds=59, microseconds=999999)

    records_list = list()
    aggregate_field = "metadata.{}.keyword".format(pm.PCM_RETRIEVAL_ID)

    records = ancillary_es.perform_aggregate_range_starts_within_bounds_query(
        beginning_date_time=convert_datetime(date),
        ending_date_time=convert_datetime(end_date),
        met_field_beginning_date_time=range_begin_date_time_key,
        aggregate_field=aggregate_field,
        conditions=conditions,
        sort_list=get_latest_product_sort_list(),
        source_includes=__get_source_includes(),
        index="grq_*_{}".format(product_type.lower()))
    logger.info("Found {} records for product {}".format(
        len(records), product_type))
    records_list.extend(records)

    return records_list


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(description="Tool to retrieve products from the DAAC and copy into a given S3 "
                                                 "bucket.")
    parser.add_argument("--product_types", type=str, required=False,
                        help="Specify one or more product types to search for products "
                             "(i.e. --product_types NEN_L_RRST,L0A_L_RRST)")
    parser.add_argument("--start_date", required=False,
                        help="Specify the starting date, in YYYY-MM-DD format, "
                             "to begin searching for products. "
                             "If this is omitted, default is the current date.")
    parser.add_argument("--end_date", required=False,
                        help="Specify the end date, in YYYY-MM-DD format, "
                             "to stop searching for products. If this is omitted, "
                             "default is the same as the start_date value.")
    parser.add_argument("--crid",
                        help="Optionally specify a CRID when searching for products.")
    parser.add_argument("--processes", required=False, default=1,
                        help="Specify number of processes to run in parallel. Defaults to 1")
    parser.add_argument("--username", required=True, help="Specify the username that will be used to provide "
                                                          "credentials when downloading the urls.")
    parser.add_argument("--password", required=False,
                        help="Specify the password associated with the username. If it is not specified here, "
                             "the program will prompt you for one.")
    parser.add_argument("--s3_bucket_name", required=True,
                        help="S3 bucket name where the files will be stored.")
    parser.add_argument("--s3_staging_area", required=False,
                        help="Optionally specify a staging area in the S3 bucket where the contents will be located. "
                             "If default, files will get stored at the top level of the bucket.")
    parser.add_argument("--chunk_size", required=False, default=102400,
                        help="Optionally specify the number of bytes to stream up to S3 at a time. "
                             "Default is 102400 bytes.")
    parser.add_argument("--verbose_level",
                        type=lambda verbose_level: LogLevels[verbose_level].value,
                        choices=LogLevels.list(),
                        help="Specify a verbosity level. Default is {}".format(LogLevels.INFO))
    parser.add_argument("--daac_environment", required=False, choices=["OPS", "UAT"], default="OPS",
                        help="Specify the DAAC environment to retrieve the products. "
                             "See the daac_environments.yaml file to see the URL endpoints that are associated "
                             "with the different environments. Default is set to OPS.")
    parser.add_argument("--daac_config", required=False,
                        help="Optionally specify a YAML file containing information about the various DAAC "
                             "environments. Default is to use the daac_envirnonments.yaml file.")
    parser.add_argument("--report_file", help="Optionally specify a report file that will contain benchmarking "
                                              "information such as how long it took to copy back the file to S3.")
    parser.add_argument("--ignore_files", required=False,
                        help="Specify file extensions to ignore when retrieving products. "
                             "If specifying multiple file extensions, surround with quotes. Default is to ignore "
                             "files that end with the following: {}".format(IGNORE_FILES))
    parser.add_argument("--failed_processing_file", required=False,
                        help="Specify a json file that will capture a list of download urls that failed. "
                             "Default is to create a file named 'failed_processing_<current_timestamp>.json'"
                             " in the current working directory.")
    parser.add_argument("--reprocess_failed_urls_file", required=False,
                        help="Pass a json file instead to tell the tool to retrieve just the data found in this file.")
    parser.add_argument("--force", required=False, action="store_true", default=False,
                        help="Specify this flag to force the tool to overwrite the existing file in s3.")
    parser.add_argument("--all_versions", required=False, action="store_true",
                        help="Specify this flag to find all versions of a product. Default is to just return the "
                             "latest version.")
    parser.add_argument("--dry_run", required=False, action="store_true",
                        help="Specify this flag to perform a dry run. No files will get copied when this flag is set.")
    parser.add_argument("--skip_signal_file", action="store_true", default=False,
                        help="Specify if signal file creation should be skipped. No signal file will get copied when this flag is set.")
    parser.add_argument("--signal_file_ext", required=False, default='signal',
                        help="Specify extention for the signal file, like 'signal', 'qac' etc. Defaults to 'ext'")
    return parser


def process_records(records, session, token, pool_handler, ignore_files, s3_bucket_name, s3_staging_area, chunk_size,
                    scroll_id=None, force_upload=False, dry_run=False, skip_signal_file=False, signal_file_ext='signal'):
    stats = list()
    signal_stats = list()
    failed_downloads = {}
    failed_signal_file_uploads = {}
    scroll_ids = set()

    while len(records) > 0:
        catalog_urls, product_urls = get_catalog_or_product_urls(
            records, ignore_files)
        download_urls = get_download_urls(
            catalog_urls, token, pool_handler, ignore_files)
        if product_urls:
            download_urls.update(product_urls)
        if dry_run is False:
            results = upload_to_s3(download_urls, session, token, pool_handler, s3_bucket_name, s3_staging_area,
                                   chunk_size, force_upload)
        else:
            results = list()

        for result in results:
            if "failed_download" in result:
                failed_downloads.update(result["failed_download"])
            else:
                stats.append(result)

        if dry_run is False and skip_signal_file is False:
            signal_results = upload_signal_to_s3(
                download_urls, pool_handler, s3_bucket_name, s3_staging_area, failed_downloads, force_upload, signal_file_ext)
        else:
            signal_results = list()

        for result in signal_results:
            if "failed_signal_file_upload" in result:
                failed_signal_file_uploads.update(
                    result["failed_signal_file_upload"])
            else:
                signal_stats.append(result)
        logger.info("upload_signal_to_s3 Completed")

        if scroll_id:
            scroll_ids.add(scroll_id)
            paged_result = ancillary_es.es.scroll(
                scroll_id=scroll_id, scroll="30m")
            scroll_id = paged_result["_scroll_id"]
            records = paged_result["hits"]["hits"]
        else:
            records = []

    for scroll_id in scroll_ids:
        ancillary_es.es.clear_scroll(scroll_id=scroll_id)
    return stats, failed_downloads, signal_stats, failed_signal_file_uploads


def __get_begin_date_time_key(product_type):
    key = "starttime"
    if product_type == pm.NEN_L_RRST:
        key = "metadata.{}".format(pm.FILE_CREATION_DATE_TIME)
    return key


def main():
    """
    Main entry point
    """
    crid = None

    args = get_parser().parse_args()

    # Parse DAAC environment YAML
    daac_config_file = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), "daac_environments.yaml")
    if args.daac_config:
        daac_config_file = os.path.abspath(args.daac_config)

    daac_environs = YamlConf(daac_config_file).cfg

    daac_environment = args.daac_environment
    endpoints = daac_environs["DAAC_ENVIRONMENTS"][daac_environment]

    # Get the base url associated with the given DAAC environment
    daac_base_url = endpoints["BASE_URL"]

    # This sets the AUTH_HOST associated with the given DAAC environment
    parsed_url = urlparse(endpoints["EARTHDATA_LOGIN"])
    global AUTH_HOST
    AUTH_HOST = parsed_url.netloc

    # Files to ignore
    ignore_files = IGNORE_FILES
    if args.ignore_files:
        ignore_files = [item.strip() for item in args.ignore_files.split(',')]

    product_types = None
    if args.product_types:
        product_types = [item.strip() for item in args.product_types.split(',')]

    reprocess_failed_urls = None
    if args.reprocess_failed_urls_file:
        with open(args.reprocess_failed_urls_file) as json_file:
            reprocess_failed_urls = json.load(json_file)

    if args.start_date:
        start_date = convert_datetime(args.start_date, strformat=DATE_FORMAT)
    else:
        start_date = datetime.utcnow()
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if args.end_date:
        end_date = convert_datetime(args.end_date, strformat=DATE_FORMAT)
    else:
        end_date = start_date

    if args.reprocess_failed_urls_file and args.product_types:
        raise RuntimeError(
            "Cannot specify both --reprocess_failed_urls_file and --product_types.")

    username = args.username
    if args.password:
        password = args.password
    else:
        password = getpass.getpass(
            "Enter your EarthData login password: ", stream=sys.stderr)

    if args.crid:
        crid = args.crid

    if args.verbose_level:
        LogLevels.set_level(args.verbose_level)

    s3_staging_area = None
    if args.s3_staging_area:
        s3_staging_area = args.s3_staging_area

    s3_bucket_name = args.s3_bucket_name

    failed_processing_file = os.path.join(
        os.getcwd(), "failed_processing_{}.json".format(convert_datetime(datetime.now(), "%Y%m%dT%H%M%S")))
    if args.failed_processing_file:
        failed_processing_file = os.path.abspath(args.failed_processing_file)

    force_upload = False
    if args.force:
        force_upload = args.force

    all_versions = False
    if args.all_versions:
        all_versions = True

    dry_run = False
    if args.dry_run is True:
        dry_run = True

    skip_signal_file = False
    if args.skip_signal_file is True:
        skip_signal_file = True

    signal_file_ext = 'signal'
    if args.signal_file_ext:
        signal_file_ext = args.signal_file_ext

    processes = int(args.processes)
    result_size = 10
    # Set the paged result size based on the number of processes set if it's greater than 10
    # If number of processes is greater than 100, cap the result size at 100 for now as we
    # do not want to hose ES
    if processes > 10 and processes < 100:
        result_size = processes
    elif processes > 100:
        result_size = 100

    pool_handler = mp.Pool(processes)
    report_file = None
    if args.report_file:
        report_file = os.path.abspath(args.report_file)
    token = get_cmr_token(username, password, daac_base_url, "NISAR")
    session = SessionWithHeaderRedirection(username, password)
    try:
        stats = list()
        failed_downloads = {}
        signal_stats = list()
        failed_signal_uploads = {}
        failed_results = {}

        if reprocess_failed_urls:
            if "datasets" in reprocess_failed_urls:
                download_urls = reprocess_failed_urls["datasets"]
                upload_stats = upload_to_s3(download_urls, session, token, pool_handler, s3_bucket_name,
                                            s3_staging_area, args.chunk_size, force_upload=force_upload)

                for result in upload_stats:
                    if "failed_download" in result:
                        failed_downloads.update(result["failed_download"])
                    else:
                        stats.append(result)

                if dry_run is False and skip_signal_file is False:
                    signal_results = upload_signal_to_s3(
                        download_urls, pool_handler, s3_bucket_name, s3_staging_area, failed_downloads, force_upload, signal_file_ext)
                else:
                    signal_results = list()

                for result in signal_results:
                    if "failed_signal_file_upload" in result:
                        failed_signal_uploads.update(
                            result["failed_signal_file_upload"])
                    else:
                        signal_stats.append(result)
                logger.info("upload_signal_to_s3 Completed")

            signal_results = None
            if "signals" in reprocess_failed_urls:
                upload_signals = reprocess_failed_urls["signals"]
                signal_results = upload_failed_signal_to_s3(upload_signals, pool_handler, s3_bucket_name, s3_staging_area, force_upload, signal_file_ext)
                for result in signal_results:
                    if "failed_signal_file_upload" in result:
                        failed_signal_uploads.update(
                            result["failed_signal_file_upload"])
                    else:
                        signal_stats.append(result)
                logger.info("upload_signal_to_s3 Completed")

        else:
            for current_date in rrule(DAILY, dtstart=start_date, until=end_date):
                for product_type in product_types:
                    range_begin_date_time_key = __get_begin_date_time_key(
                        product_type)
                    sid = None
                    if product_type == pm.NEN_L_RRST or all_versions is True:
                        paged_result = get_products_for_day(
                            product_type, current_date, crid, size=result_size,
                            range_begin_date_time_key=range_begin_date_time_key)
                        sid = paged_result["_scroll_id"]
                        records = paged_result.get("hits", {}).get("hits", [])
                    else:
                        records = get_latest_records_for_day(product_type, current_date, crid,
                                                             range_begin_date_time_key=range_begin_date_time_key)
                    if len(records) != 0:
                        upload_stats, failures, signal_upload_stats, signal_failures = process_records(
                            records, session, token, pool_handler, ignore_files, s3_bucket_name, s3_staging_area, args.chunk_size,
                            scroll_id=sid, force_upload=force_upload, dry_run=dry_run, skip_signal_file=skip_signal_file, signal_file_ext=signal_file_ext)
                        for us in upload_stats:
                            if us:
                                stats.append(us)

                        for us in signal_upload_stats:
                            if us:
                                signal_stats.append(us)

                        failed_downloads.update(failures)
                        failed_signal_uploads.update(signal_failures)
                        print("signal_failures : {}".format(signal_failures))
                    else:
                        logger.info("No products found for day {}".format(convert_datetime(current_date,
                                                                                           strformat=DATE_FORMAT)))
        print("failed_signal_uploads : {}".format(failed_signal_uploads))

        if report_file and len(stats) != 0:
            logger.info("Writing report file to {}".format(report_file))
            with open(report_file, "w") as writer:
                csv_writer = csv.writer(writer)
                count = 0
                for entry in stats:
                    if count == 0:
                        header = entry.keys()
                        csv_writer.writerow(header)
                        count += 1
                    csv_writer.writerow(entry.values())

                if len(signal_stats) != 0:
                    for entry in signal_stats:
                        csv_writer.writerow(entry.values())
            logger.info(
                "Successfully created report file: {}".format(report_file))

        if failed_downloads:
            failed_results["datasets"] = failed_downloads
        if failed_signal_uploads:
            failed_results["signals"] = list(set(failed_signal_uploads.keys()))

        if len(failed_downloads) > 0 or len(failed_signal_uploads) > 0:
            logger.info("Error processing some files. Creating failed processing file: {}".format(
                failed_processing_file))
            with open(failed_processing_file, "w") as output_file:
                json.dump(failed_results, output_file, indent=2)
            logger.info("Successfully created file: {}. Pass in this file via the --reprocess_failed_urls_file flag "
                        "the next time your run this tool.".format(failed_processing_file))
    except Exception:
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        delete_token(token, daac_base_url)
        logger.info("Successfully deleted token: {}".format(token))


if __name__ == "__main__":
    main()
