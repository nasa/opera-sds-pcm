#!/usr/bin/env python3

# Forked from github.com:podaac/data-subscriber.git


import argparse
import asyncio
import itertools
import socket
import json
import logging
import netrc
import os
import re
import shutil
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial
from http.cookiejar import CookieJar
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Iterable
from urllib import request
from urllib.parse import urlparse

import boto3
import requests
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import map_reduce, chunked
from smart_open import open

from data_subscriber.hls.hls_catalog_connection import get_hls_catalog_connection
from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection


class SessionWithHeaderRedirection(requests.Session):
    """
    Borrowed from https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    def __init__(self, username, password, auth_host):
        super().__init__()
        self.auth = (username, password)
        self.auth_host = auth_host

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers['Authorization']


async def run(argv: list[str]):
    parser = create_parser()
    args = parser.parse_args(argv[1:])
    try:
        validate(args)
    except ValueError as v:
        raise v

    HLS_CONN = get_hls_catalog_connection(logging.getLogger(__name__))
    if args.file:
        with open(args.file, "r") as f:
            update_url_index(HLS_CONN, f.readlines(), None, None)
        exit(0)

    IP_ADDR = socket.gethostbyname(socket.gethostname())
    EDL = "urs.earthdata.nasa.gov"
    CMR = "cmr.earthdata.nasa.gov"
    TOKEN_URL = f"https://{CMR}/legacy-services/rest/tokens"
    NETLOC = urlparse("https://urs.earthdata.nasa.gov").netloc

    LOGLEVEL = 'DEBUG' if args.verbose else 'INFO'
    logging.basicConfig(level=LOGLEVEL)
    logging.info("Log level set to " + LOGLEVEL)

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

    username, password = setup_earthdata_login_auth(EDL)

    with token_ctx(TOKEN_URL, IP_ADDR, EDL) as token:
        logging.info(f"{args.subparser_name=}")
        if not (
                args.subparser_name == "query"
                or args.subparser_name == "download"
                or args.subparser_name == "full"
        ):
            raise Exception(f"Unsupported operation. {args.subparser_name=}")

        results = {}
        if args.subparser_name == "query" or args.subparser_name == "full":
            results["query"] = await run_query(args, token, HLS_CONN, CMR, job_id)
        if args.subparser_name == "download" or args.subparser_name == "full":
            results["download"] = run_download(args, token, HLS_CONN, NETLOC, username, password, job_id)
    logging.info(f"{results=}")
    logging.info("END")
    return results


async def run_query(args, token, HLS_CONN, CMR, job_id):
    HLS_SPATIAL_CONN = get_hls_spatial_catalog_connection(logging.getLogger(__name__))
    query_dt = datetime.now()
    granules = query_cmr(args, token, CMR)

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    download_urls: list[str] = []
    for granule in granules:
        update_url_index(HLS_CONN, granule.get("filtered_urls"), granule.get("granule_id"), job_id, query_dt)
        update_granule_index(HLS_SPATIAL_CONN, granule)
        download_urls.extend(granule.get("filtered_urls"))

    if args.subparser_name == "full":
        logging.info(f"{args.subparser_name=}. Skipping download job submission.")
        return

    if args.no_schedule_download:
        logging.info(f"{args.no_schedule_download=}. Skipping download job submission.")
        return

    if not args.chunk_size:
        logging.info(f"{args.chunk_size=}. Skipping download job submission.")
        return

    tile_id_to_urls_map: dict[str, set[str]] = map_reduce(
        iterable=download_urls,
        keyfunc=url_to_tile_id,
        valuefunc=lambda url: url,
        reducefunc=set
    )

    logging.info(f"{tile_id_to_urls_map=}")
    job_submission_tasks = []
    loop = asyncio.get_event_loop()
    logging.info(f"{args.chunk_size=}")
    for tile_chunk in chunked(tile_id_to_urls_map.items(), n=args.chunk_size):
        chunk_id = str(uuid.uuid4())
        logging.info(f"{chunk_id=}")

        chunk_tile_ids = []
        chunk_urls = []
        for tile_id, urls in tile_chunk:
            chunk_tile_ids.append(tile_id)
            chunk_urls.extend(urls)

        logging.info(f"{chunk_tile_ids=}")
        logging.info(f"{chunk_urls=}")

        job_submission_tasks.append(
            loop.run_in_executor(
                executor=None,
                func=partial(
                    submit_download_job,
                    release_version=args.release_version,
                    params=[
                        {
                            "name": "isl_bucket_name",
                            "value": f"--isl-bucket={args.isl_bucket}",
                            "from": "value"
                        },
                        {
                            "name": "tile_ids",
                            "value": "--tile-ids " + " ".join(chunk_tile_ids) if chunk_tile_ids else "",
                            "from": "value"
                        },
                        {
                            "name": "smoke_run",
                            "value": "--smoke-run" if args.smoke_run else "",
                            "from": "value"
                        },
                        {
                            "name": "dry_run",
                            "value": "--dry-run" if args.dry_run else "",
                            "from": "value"
                        },

                    ],
                    job_queue=args.job_queue
                )
            )
        )

    results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
    logging.info(f"{len(results)=}")
    logging.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logging.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logging.info(f"{failed=}")
    return {
        "success": succeeded,
        "fail": failed
    }


def run_download(args, token, HLS_CONN, NETLOC, username, password, job_id):
    all_pending_downloads: Iterable[dict] = HLS_CONN.get_all_undownloaded()

    downloads = all_pending_downloads
    if args.tile_ids:
        logging.info(f"Filtering pending downloads by {args.tile_ids=}")
        downloads = list(filter(lambda d: to_tile_id(d) in args.tile_ids, all_pending_downloads))
        logging.info(f"{downloads=}")

    if not downloads:
        logging.info(f"No undownloaded files found in index.")
        return

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 tile(s).")
        args.tile_ids = args.tile_ids[:1]

    session = SessionWithHeaderRedirection(username, password, NETLOC)

    if args.transfer_protocol == "https":
        download_urls = [to_https_url(download) for download in downloads]
        logging.info(f"{download_urls=}")
        upload_url_list_from_https(session, HLS_CONN, download_urls, args, token, job_id)
    else:
        download_urls = [to_s3_url(download) for download in downloads]
        logging.info(f"{download_urls=}")
        upload_url_list_from_s3(session, HLS_CONN, download_urls, args, job_id)

    logging.info(f"Total files updated: {len(downloads)}")


def submit_download_job(*, release_version=None, params: list[dict[str, str]], job_queue: str) -> str:
    return submit_mozart_job_minimal(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": f"job-hls_download:{release_version}",
        },
        job_queue=job_queue
    )


def submit_mozart_job_minimal(*, hysdsio: dict, job_queue: str) -> str:
    return submit_mozart_job(
        hysdsio=hysdsio,
        product={},
        rule={
            "rule_name": "trigger-hls_download",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name="job-WF-hls_download",
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component=None
    )


def to_tile_id(dl_doc: dict[str, Any]):
    return url_to_tile_id(to_url(dl_doc))


def url_to_tile_id(url: str):
    input_filename = Path(url).name
    tile_id: str = re.findall(r"T\w{5}", input_filename)[0]
    return tile_id


def to_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    elif dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


def to_https_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


def to_s3_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name", required=True)
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Verbose mode.")
    parser.add_argument("-f", "--file", dest="file",
                        help="Path to file with newline-separated URIs to ingest into data product ES index (to be downloaded later).")

    full_parser = subparsers.add_parser("full")
    full_parser.add_argument("-p", "--provider", dest="provider", default='LPCLOUD',
                             help="Specify a provider for collection search. Default is LPCLOUD.")
    full_parser.add_argument("-c", "--collection-shortname", dest="collection", required=True,
                             help="The collection shortname for which you want to retrieve data.")
    full_parser.add_argument("-s", "--start-date", dest="startDate", default=False,
                             help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z")
    full_parser.add_argument("-e", "--end-date", dest="endDate", default=False,
                             help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z")
    full_parser.add_argument("-b", "--bounds", dest="bbox", default="-180,-90,180,90",
                             help="The bounding rectangle to filter result in. Format is W Longitude,S Latitude,E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from the command line. Default: \"-180,-90,180,90\".")
    full_parser.add_argument("-m", "--minutes", dest="minutes", type=int, default=60,
                             help="How far back in time, in minutes, should the script look for data. If running this script as a cron, this value should be equal to or greater than how often your cron runs (default: 60 minutes).")
    full_parser.add_argument("-i", "--isl-bucket", dest="isl_bucket", required=True,
                             help="The incoming storage location s3 bucket where data products will be downloaded.")
    full_parser.add_argument("-x", "--transfer-protocol", dest="transfer_protocol", default='s3',
                             help="The protocol used for retrieving data, HTTPS or default of S3")

    full_parser.add_argument("--dry-run", dest="dry_run", action="store_true",
                             help="Toggle for skipping physical downloads.")
    full_parser.add_argument("--smoke-run", dest="smoke_run", action="store_true",
                             help="Toggle for processing a single tile.")

    full_parser.add_argument("--no-schedule-download", dest="no_schedule_download", action="store_true",
                             help="Toggle for query only operation (no downloads).")
    full_parser.add_argument("--release-version", dest="release_version",
                             help="The release version of the download job-spec.")
    full_parser.add_argument("--job-queue", dest="job_queue",
                             help="The queue to use for the scheduled download job.")
    full_parser.add_argument("--chunk-size", dest="chunk_size", type=int,
                             help="chunk-size = 1 means 1 tile per job. chunk-size > 1 means multiple (N) tiles per job")

    full_parser.add_argument("--tile-ids", nargs="*", dest="tile_ids",
                             help="A list of target tile IDs pending download.")

    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("-c", "--collection-shortname", dest="collection", required=True,
                              help="The collection shortname for which you want to retrieve data.")
    query_parser.add_argument("-s", "--start-date", dest="startDate", default=False,
                              help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z")
    query_parser.add_argument("-e", "--end-date", dest="endDate", default=False,
                              help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z")
    query_parser.add_argument("-b", "--bounds", dest="bbox", default="-180,-90,180,90",
                              help="The bounding rectangle to filter result in. Format is W Longitude,S Latitude,E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from the command line. Default: \"-180,-90,180,90\".")
    query_parser.add_argument("-m", "--minutes", dest="minutes", type=int, default=60,
                              help="How far back in time, in minutes, should the script look for data. If running this script as a cron, this value should be equal to or greater than how often your cron runs (default: 60 minutes).")
    query_parser.add_argument("-p", "--provider", dest="provider", default='LPCLOUD',
                              help="Specify a provider for collection search. Default is LPCLOUD.")
    query_parser.add_argument("--no-schedule-download", dest="no_schedule_download", action="store_true",
                              help="Toggle for query only operation (no downloads).")
    query_parser.add_argument("--release-version", dest="release_version",
                              help="The release version of the download job-spec.")
    query_parser.add_argument("--job-queue", dest="job_queue",
                              help="The queue to use for the scheduled download job.")
    query_parser.add_argument("-i", "--isl-bucket", dest="isl_bucket", required=True,
                              help="The incoming storage location s3 bucket where data products will be downloaded.")
    query_parser.add_argument("--chunk-size", dest="chunk_size", type=int,
                              help="chunk-size = 1 means 1 tile per job. chunk-size > 1 means multiple (N) tiles per job")
    query_parser.add_argument("--dry-run", dest="dry_run", action="store_true",
                              help="Toggle for skipping physical downloads.")
    query_parser.add_argument("--smoke-run", dest="smoke_run", action="store_true",
                              help="Toggle for processing a single tile.")

    download_parser = subparsers.add_parser("download")
    download_parser.add_argument("-i", "--isl-bucket", dest="isl_bucket", required=True,
                                 help="The incoming storage location s3 bucket where data products will be downloaded.")
    download_parser.add_argument("-x", "--transfer-protocol", dest="transfer_protocol", default='s3',
                                 help="The protocol used for retrieving data, HTTPS or default of S3")
    download_parser.add_argument("--tile-ids", nargs="*", dest="tile_ids",
                                 help="A list of target tile IDs pending download.")
    download_parser.add_argument("--dry-run", dest="dry_run", action="store_true",
                                 help="Toggle for skipping physical downloads.")
    download_parser.add_argument("--smoke-run", dest="smoke_run", action="store_true",
                                 help="Toggle for processing a single tile.")

    return parser


def validate(args):
    if hasattr(args, "bbox") and args.bbox:
        validate_bounds(args.bbox)

    if hasattr(args, "startDate") and args.startDate:
        validate_date(args.startDate, "start")

    if hasattr(args, "endDate") and args.endDate:
        validate_date(args.endDate, "end")

    if hasattr(args, "minutes") and args.minutes:
        validate_minutes(args.minutes)


def validate_bounds(bbox):
    bounds = bbox.split(',')
    value_error = ValueError(
        f"Error parsing bounds: {bbox}. Format is <W Longitude>,<S Latitude>,<E Longitude>,<N Latitude> without spaces ")

    if len(bounds) != 4:
        raise value_error

    for b in bounds:
        try:
            float(b)
        except ValueError:
            raise value_error


def validate_date(date, type='start'):
    try:
        datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ValueError(
            f"Error parsing {type} date: {date}. Format must be like 2021-01-14T00:00:00Z")


def validate_minutes(minutes):
    try:
        int(minutes)
    except ValueError:
        raise ValueError(f"Error parsing minutes: {minutes}. Number must be an integer.")


def setup_earthdata_login_auth(endpoint):
    # ## Authentication setup
    #
    # This function will allow Python scripts to log into any Earthdata Login
    # application programmatically.  To avoid being prompted for
    # credentials every time you run and also allow clients such as curl to log in,
    # you can add the following to a `.netrc` (`_netrc` on Windows) file in
    # your home directory:
    #
    # ```
    # machine urs.earthdata.nasa.gov
    #     login <your username>
    #     password <your password>
    # ```
    #
    # Make sure that this file is only readable by the current user
    # or you will receive an error stating
    # "netrc access too permissive."
    #
    # `$ chmod 0600 ~/.netrc`
    #
    # You'll need to authenticate using the netrc method when running from
    # command line with [`papermill`](https://papermill.readthedocs.io/en/latest/).
    # You can log in manually by executing the cell below when running in the
    # notebook client in your browser.*

    """
    Set up the request library so that it authenticates against the given
    Earthdata Login endpoint and is able to track cookies between requests.
    This looks in the .netrc file first and if no credentials are found,
    it prompts for them.

    Valid endpoints include:
        urs.earthdata.nasa.gov - Earthdata Login production
    """
    username = password = ""
    try:
        username, _, password = netrc.netrc().authenticators(endpoint)
    except FileNotFoundError as e:
        logging.error("There's no .netrc file")
        raise e
    except TypeError as e:
        logging.error("The endpoint isn't in the netrc file")
        raise e

    manager = request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, endpoint, username, password)
    auth = request.HTTPBasicAuthHandler(manager)

    jar = CookieJar()
    processor = request.HTTPCookieProcessor(jar)
    opener = request.build_opener(auth, processor)
    opener.addheaders = [('User-agent', 'daac-subscriber')]
    request.install_opener(opener)

    return username, password


def get_token(url: str, client_id: str, user_ip: str, endpoint: str) -> str:
    username, _, password = netrc.netrc().authenticators(endpoint)
    xml = f"<?xml version='1.0' encoding='utf-8'?><token><username>{username}</username><password>{password}</password><client_id>{client_id}</client_id><user_ip_address>{user_ip}</user_ip_address></token>"
    headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
    resp = requests.post(url, headers=headers, data=xml)
    response_content = json.loads(resp.content)
    token = response_content['token']['id']

    return token


def query_cmr(args, token, CMR) -> list:
    PAGE_SIZE = 2000
    now = datetime.utcnow()
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus_minutes_date = (now - timedelta(minutes=args.minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

    start_date = args.startDate if args.startDate else now_minus_minutes_date
    end_date = args.endDate if args.endDate else now_date

    request_url = f"https://{CMR}/search/granules.umm_json"
    params = {
        'scroll': "false",
        'page_size': PAGE_SIZE,
        'sort_key': "-start_date",
        'provider': args.provider,
        'ShortName': args.collection,
        'updated_since': start_date,
        'token': token,
        'bounding_box': args.bbox,
    }

    time_range_is_defined = args.startDate or args.endDate
    if time_range_is_defined:
        temporal_range = get_temporal_range(start_date, end_date, now_date)
        logging.info("Temporal Range: " + temporal_range)
        params['temporal'] = temporal_range

    product_granules, search_after = request_search(request_url, params)

    while search_after:
        granules, search_after = request_search(request_url, params, search_after=search_after)
        product_granules.extend(granules)

    logging.info(f"Found {str(len(product_granules))} total granules")
    for granule in product_granules:
        granule['filtered_urls'] = filter_on_extension(granule, args)

    return product_granules


def get_temporal_range(start, end, now):
    start = start if start is not False else None
    end = end if end is not False else None

    if start is not None and end is not None:
        return "{},{}".format(start, end)
    if start is not None and end is None:
        return "{},{}".format(start, now)
    if start is None and end is not None:
        return "1900-01-01T00:00:00Z,{}".format(end)

    raise ValueError("One of start-date or end-date must be specified.")


def request_search(request_url, params, search_after=None):
    response = requests.get(request_url, params=params, headers={'CMR-Search-After': search_after}) \
        if search_after else requests.get(request_url, params=params)
    results = response.json()
    items = results.get('items')
    next_search_after = response.headers.get('CMR-Search-After')

    if items and 'umm' in items[0]:
        return [{"granule_id": item.get("umm").get("GranuleUR"),
                 "provider": item.get("meta").get("provider-id"),
                 "production_datetime": item.get("umm").get("DataGranule").get("ProductionDateTime"),
                 "short_name": item.get("umm").get("Platforms")[0].get("ShortName"),
                 "bounding_box": [{"lat": point.get("Latitude"), "lon": point.get("Longitude")}
                                  for point
                                  in item.get("umm").get("SpatialExtent").get("HorizontalSpatialDomain")
                                      .get("Geometry").get("GPolygons")[0].get("Boundary").get("Points")],
                 "related_urls": [url_item.get("URL") for url_item in item.get("umm").get("RelatedUrls")]}
                for item in items], next_search_after
    else:
        return [], None


def filter_on_extension(granule, args):
    EXTENSION_LIST_MAP = {"L30": ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"],
                          "S30": ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"],
                          "TIF": ["tif"]}
    filter_extension = "TIF"

    for extension in EXTENSION_LIST_MAP:
        if extension.upper() in args.collection.upper():
            filter_extension = extension.upper()
            break

    return [f
            for f in granule.get("related_urls")
            for extension in EXTENSION_LIST_MAP.get(filter_extension)
            if extension in f]


def convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def update_url_index(ES_CONN, urls, granule_id, job_id, query_dt):
    for url in urls:
        ES_CONN.process_url(url, granule_id, job_id, query_dt)


def update_granule_index(ES_SPATIAL_CONN, granule):
    ES_SPATIAL_CONN.process_granule(granule)


def upload_url_list_from_https(session, ES_CONN, downloads, args, token, job_id):
    num_successes = num_failures = num_skipped = 0
    filtered_downloads = [f for f in downloads if "https://" in f]

    for url in filtered_downloads:
        try:
            if ES_CONN.product_is_downloaded(url):
                logging.info(f"SKIPPING: {url}")
                num_skipped = num_skipped + 1
            else:
                if args.dry_run:
                    logging.info(f"{args.dry_run=}. Skipping downloads.")
                else:
                    result = https_transfer(url, args.isl_bucket, session, token)
                    if "failed_download" in result:
                        raise Exception(result["failed_download"])
                    else:
                        logging.debug(str(result))

                ES_CONN.mark_product_as_downloaded(url, job_id)
                logging.info(f"{str(datetime.now())} SUCCESS: {url}")
                num_successes = num_successes + 1
        except Exception as e:
            logging.error(f"{str(datetime.now())} FAILURE: {url}")
            num_failures = num_failures + 1
            logging.error(e)

    logging.info(f"Files downloaded: {str(num_successes)}")
    logging.info(f"Duplicate files skipped: {str(num_skipped)}")
    logging.info(f"Files failed to download: {str(num_failures)}")


def https_transfer(url, bucket_name, session, token, staging_area="", chunk_size=25600):
    file_name = os.path.basename(url)
    bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name

    key = os.path.join(staging_area, file_name)
    upload_start_time = datetime.utcnow()
    headers = {"Echo-Token": token}

    try:
        with session.get(url, headers=headers, stream=True) as r:
            if r.status_code != 200:
                r.raise_for_status()
            logging.info("Uploading {} to Bucket={}, Key={}".format(file_name, bucket_name, key))
            with open("s3://{}/{}".format(bucket, key), "wb") as out:
                pool = ThreadPool(processes=10)
                pool.map(upload_chunk,
                         [{'chunk': chunk, 'out': out} for chunk in r.iter_content(chunk_size=chunk_size)])
                pool.close()
                pool.join()
        upload_end_time = datetime.utcnow()
        upload_duration = upload_end_time - upload_start_time
        upload_stats = {
            "file_name": file_name,
            "file_size (in bytes)": r.headers.get('Content-Length'),
            "upload_duration (in seconds)": upload_duration.total_seconds(),
            "upload_start_time": convert_datetime(upload_start_time),
            "upload_end_time": convert_datetime(upload_end_time)
        }
        return upload_stats
    except (ConnectionResetError, requests.exceptions.HTTPError) as e:
        return {"failed_download": e}


def upload_chunk(chunk_dict):
    logging.debug("Uploading {} byte(s)".format(len(chunk_dict['chunk'])))
    chunk_dict['out'].write(chunk_dict['chunk'])


def upload_url_list_from_s3(session, ES_CONN, downloads, args, job_id):
    aws_creds = get_aws_creds(session)
    s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                       aws_secret_access_key=aws_creds['secretAccessKey'],
                       aws_session_token=aws_creds['sessionToken'],
                       region_name='us-west-2').client("s3")

    tmp_dir = "/tmp/data_subscriber"
    os.makedirs(tmp_dir, exist_ok=True)

    num_successes = num_failures = num_skipped = 0
    filtered_downloads = [f for f in downloads if "s3://" in f]

    for url in filtered_downloads:
        try:
            if ES_CONN.product_is_downloaded(url):
                logging.info(f"SKIPPING: {url}")
                num_skipped = num_skipped + 1
            else:
                if args.dry_run:
                    logging.info(f"{args.dry_run=}. Skipping downloads.")
                else:
                    result = s3_transfer(url, args.isl_bucket, s3, tmp_dir)
                    if "failed_download" in result:
                        raise Exception(result["failed_download"])
                    else:
                        logging.debug(str(result))

                ES_CONN.mark_product_as_downloaded(url, job_id)
                logging.info(f"{str(datetime.now())} SUCCESS: {url}")
                num_successes = num_successes + 1
        except Exception as e:
            logging.error(f"{str(datetime.now())} FAILURE: {url}")
            num_failures = num_failures + 1
            logging.error(e)

    logging.info(f"Files downloaded: {str(num_successes)}")
    logging.info(f"Duplicate files skipped: {str(num_skipped)}")
    logging.info(f"Files failed to download: {str(num_failures)}")

    shutil.rmtree(tmp_dir)


def get_aws_creds(session):
    with session.get("https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials") as r:
        if r.status_code != 200:
            r.raise_for_status()

        return r.json()


def s3_transfer(url, bucket_name, s3, tmp_dir, staging_area=""):
    file_name = os.path.basename(url)

    source = url[len("s3://"):].partition('/')
    source_bucket = source[0]
    source_key = source[2]

    target_bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name
    target_key = os.path.join(staging_area, file_name)

    try:
        s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

        target_s3 = boto3.resource("s3")
        target_s3.Bucket(target_bucket).upload_file(f"{tmp_dir}/{target_key}", target_key)

        return {"successful_download": target_key}
    except Exception as e:
        return {"failed_download": e}


@contextmanager
def token_ctx(token_url, ip_addr, edl):
    token = get_token(token_url, 'daac-subscriber', ip_addr, edl)
    try:
        yield token
    finally:
        delete_token(token_url, token)


def delete_token(url: str, token: str) -> None:
    try:
        headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
        url = '{}/{}'.format(url, token)
        resp = requests.request('DELETE', url, headers=headers)
        if resp.status_code == 204:
            logging.info("CMR token successfully deleted")
        else:
            logging.warning("CMR token deleting failed.")
    except Exception as e:
        logging.warning("Error deleting the token")


if __name__ == '__main__':
    asyncio.run(run(sys.argv))
