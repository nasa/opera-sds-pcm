#!/usr/bin/env python3

# Forked from github.com:podaac/data-subscriber.git


import argparse
import asyncio
import itertools
import json
import logging
import netrc
import re
import shutil
import sys
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path, PurePath
from typing import Any, Iterable
from urllib.parse import urlparse

import boto3
import dateutil.parser
import requests
import validators
from cachetools.func import ttl_cache
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import map_reduce, chunked
from requests.auth import HTTPBasicAuth
from smart_open import open

import extractor.extract
import product2dataset.product2dataset
from data_subscriber.hls.hls_catalog_connection import get_hls_catalog_connection
from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection
from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection
from tools import stage_orbit_file
from util.conf_util import SettingsConf

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


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

        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers["Authorization"]


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
    provider_esconn_map = {"LPCLOUD": get_hls_catalog_connection(logging.getLogger(__name__)),
                           "ASF": get_slc_catalog_connection(logging.getLogger(__name__))}
    es_conn = provider_esconn_map.get(args.provider)

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

    transfer_protocol = {"positionals": ["-x", "--transfer-protocol"],
                         "kwargs": {"dest": "transfer_protocol",
                                    "choices": ["s3", "https"],
                                    "default": "s3",
                                    "help": "The protocol used for retrieving data, HTTPS or default of S3"}}

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
                            "help": "The native ID of a single product granule to be queried, overriding other query arguments if present."}}

    parser_arg_list = [verbose, file, provider]
    _add_arguments(parser, parser_arg_list)

    full_parser = subparsers.add_parser("full")
    full_parser_arg_list = [verbose, endpoint, provider, collection, start_date, end_date, bbox, minutes,
                            transfer_protocol, dry_run, smoke_run, no_schedule_download, release_version, job_queue,
                            chunk_size, batch_ids, use_temporal, temporal_start_date, native_id]
    _add_arguments(full_parser, full_parser_arg_list)

    query_parser = subparsers.add_parser("query")
    query_parser_arg_list = [verbose, endpoint, provider, collection, start_date, end_date, bbox, minutes,
                             dry_run, smoke_run, no_schedule_download, release_version, job_queue, chunk_size,
                             native_id, use_temporal, temporal_start_date]
    _add_arguments(query_parser, query_parser_arg_list)

    download_parser = subparsers.add_parser("download")
    download_parser_arg_list = [verbose, file, endpoint, provider, transfer_protocol, dry_run, smoke_run,
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


def update_url_index(
        es_conn,
        urls: list[str],
        granule_id: str,
        job_id: str,
        query_dt: datetime,
        temporal_extent_beginning_dt: datetime,
        revision_date_dt: datetime
):
    for url in urls:
        es_conn.process_url(url, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt)


def update_granule_index(es_spatial_conn, granule):
    es_spatial_conn.process_granule(granule)


def supply_token(edl: str, username: str, password: str) -> str:
    """
    :param edl: Earthdata login endpoint
    :type edl: str
    """
    token_list = _get_tokens(edl, username, password)

    _revoke_expired_tokens(token_list, edl, username, password)

    if not token_list:
        token = _create_token(edl, username, password)
    else:
        token = token_list[0]["access_token"]

    return token


def _get_tokens(edl: str, username: str, password: str) -> list[dict]:
    token_list_url = f"https://{edl}/api/users/tokens"

    list_response = requests.get(token_list_url, auth=HTTPBasicAuth(username, password))
    list_response.raise_for_status()

    return list_response.json()


def _revoke_expired_tokens(token_list: list[dict], edl: str, username: str, password: str) -> None:
    for token_dict in token_list:
        now = datetime.utcnow().date()
        expiration_date = datetime.strptime(token_dict["expiration_date"], "%m/%d/%Y").date()

        if expiration_date <= now:
            _delete_token(edl, username, password, token_dict["access_token"])
            del token_dict


def _create_token(edl: str, username: str, password: str) -> str:
    token_create_url = f"https://{edl}/api/users/token"

    create_response = requests.post(token_create_url, auth=HTTPBasicAuth(username, password))
    create_response.raise_for_status()

    response_content = create_response.json()

    if "error" in response_content.keys():
        raise Exception(response_content["error"])

    token = response_content["access_token"]

    return token


def _delete_token(edl: str, username: str, password: str, token: str) -> None:
    url = f"https://{edl}/api/users/revoke_token"
    try:
        resp = requests.post(url, auth=HTTPBasicAuth(username, password),
                             params={"token": token})
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"Error deleting the token: {e}")

    logging.info("CMR token successfully deleted")


async def run_query(args, token, es_conn, cmr, job_id, settings):
    HLS_SPATIAL_CONN = get_hls_spatial_catalog_connection(logging.getLogger(__name__))

    query_dt = datetime.now()
    now = datetime.utcnow()
    query_timerange: DateTimeRange = get_query_timerange(args, now)

    granules = query_cmr(args, token, cmr, settings, query_timerange, now)

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    download_urls: list[str] = []

    for granule in granules:
        update_url_index(es_conn, granule.get("filtered_urls"), granule.get("granule_id"), job_id, query_dt,
                         temporal_extent_beginning_dt=dateutil.parser.isoparse(
                             granule["temporal_extent_beginning_datetime"]),
                         revision_date_dt=dateutil.parser.isoparse(granule["revision_date"]))

        if args.provider == "LPCLOUD":
            update_granule_index(HLS_SPATIAL_CONN, granule)

        if granule.get("filtered_urls"):
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

    # group URLs by this mapping func. E.g. group URLs by granule_id
    keyfunc = _hls_url_to_granule_id if args.provider == "LPCLOUD" else _url_to_orbit_number
    batch_id_to_urls_map: dict[str, set[str]] = map_reduce(
        iterable=download_urls,
        keyfunc=keyfunc,
        valuefunc=lambda url: url,
        reducefunc=set
    )

    logging.info(f"{batch_id_to_urls_map=}")
    job_submission_tasks = []
    loop = asyncio.get_event_loop()
    logging.info(f"{args.chunk_size=}")
    for batch_chunk in chunked(batch_id_to_urls_map.items(), n=args.chunk_size):
        chunk_id = str(uuid.uuid4())
        logging.info(f"{chunk_id=}")

        chunk_batch_ids = []
        chunk_urls = []
        for batch_id, urls in batch_chunk:
            chunk_batch_ids.append(batch_id)
            chunk_urls.extend(urls)

        logging.info(f"{chunk_batch_ids=}")
        logging.info(f"{chunk_urls=}")

        job_submission_tasks.append(
            loop.run_in_executor(
                executor=None,
                func=partial(
                    submit_download_job,
                    release_version=args.release_version,
                    provider=args.provider,
                    params=[
                        {
                            "name": "batch_ids",
                            "value": "--batch-ids " + " ".join(chunk_batch_ids) if chunk_batch_ids else "",
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
                        {
                            "name": "endpoint",
                            "value": f"--endpoint={args.endpoint}",
                            "from": "value"
                        },
                        {
                            "name": "start_datetime",
                            "value": f"--start-date={query_timerange.start_date}",
                            "from": "value"
                        },
                        {
                            "name": "end_datetime",
                            "value": f"--end-date={query_timerange.end_date}",
                            "from": "value"
                        },
                        {
                            "name": "use_temporal",
                            "value": "--use-temporal" if args.use_temporal else "",
                            "from": "value"
                        }

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


def get_query_timerange(args, now: datetime):
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus_minutes_date = (now - timedelta(minutes=args.minutes)).strftime(
        "%Y-%m-%dT%H:%M:%SZ") if not args.native_id else "1900-01-01T00:00:00Z"
    start_date = args.start_date if args.start_date else now_minus_minutes_date
    end_date = args.end_date if args.end_date else now_date

    query_timerange = DateTimeRange(start_date, end_date)
    logging.info(f"{query_timerange=}")
    return query_timerange


def get_download_timerange(args):
    start_date = args.start_date if args.start_date else "1900-01-01T00:00:00Z"
    end_date = args.end_date if args.end_date else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    download_timerange = DateTimeRange(start_date, end_date)
    logging.info(f"{download_timerange=}")
    return download_timerange


def query_cmr(args, token, cmr, settings, timerange: DateTimeRange, now: datetime) -> list:
    PAGE_SIZE = 2000

    request_url = f"https://{cmr}/search/granules.umm_json"
    params = {
        "page_size": PAGE_SIZE,
        "sort_key": "-start_date",
        "provider": args.provider,
        "ShortName": args.collection,
        "token": token,
        "bounding_box": args.bbox,
    }

    if args.native_id:
        params["native-id"] = args.native_id

    # derive and apply param "temporal"
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    temporal_range = _get_temporal_range(timerange.start_date, timerange.end_date, now_date)
    logging.info("Temporal Range: " + temporal_range)

    if args.use_temporal:
        params["temporal"] = temporal_range
    else:
        params["revision_date"] = temporal_range

        # if a temporal start-date is provided, set temporal
        if args.temporal_start_date:
            logging.info(f"{args.temporal_start_date=}")
            params["temporal"] = dateutil.parser.isoparse(args.temporal_start_date).strftime("%Y-%m-%dT%H:%M:%SZ")

    logging.info(f"{request_url=} {params=}")
    product_granules, search_after = _request_search(args, request_url, params)

    while search_after:
        granules, search_after = _request_search(args, request_url, params, search_after=search_after)
        product_granules.extend(granules)

    if args.collection in settings["SHORTNAME_FILTERS"]:
        product_granules = [granule
                            for granule in product_granules
                            if _match_identifier(settings, args, granule)]

        logging.info(f"Found {str(len(product_granules))} total granules")

    for granule in product_granules:
        granule["filtered_urls"] = _filter_granules(granule, args)

    return product_granules


def _get_temporal_range(start: str, end: str, now: str):
    start = start if start is not False else None
    end = end if end is not False else None

    if start is not None and end is not None:
        return "{},{}".format(start, end)
    if start is not None and end is None:
        return "{},{}".format(start, now)
    if start is None and end is not None:
        return "1900-01-01T00:00:00Z,{}".format(end)
    else:
        return "1900-01-01T00:00:00Z,{}".format(now)


def _request_search(args, request_url, params, search_after=None):
    response = requests.get(request_url, params=params, headers={"CMR-Search-After": search_after}) \
        if search_after else requests.get(request_url, params=params)

    results = response.json()
    items = results.get("items")
    next_search_after = response.headers.get("CMR-Search-After")

    collection_identifier_map = {"HLSL30": "LANDSAT_PRODUCT_ID",
                                 "HLSS30": "PRODUCT_URI"}

    if items and "umm" in items[0]:
        return [{"granule_id": item.get("umm").get("GranuleUR"),
                 "provider": item.get("meta").get("provider-id"),
                 "production_datetime": item.get("umm").get("DataGranule").get("ProductionDateTime"),
                 "temporal_extent_beginning_datetime": item["umm"]["TemporalExtent"]["RangeDateTime"][
                     "BeginningDateTime"],
                 "revision_date": item["meta"]["revision-date"],
                 "short_name": item.get("umm").get("Platforms")[0].get("ShortName"),
                 "bounding_box": [{"lat": point.get("Latitude"), "lon": point.get("Longitude")}
                                  for point
                                  in item.get("umm").get("SpatialExtent").get("HorizontalSpatialDomain")
                                      .get("Geometry").get("GPolygons")[0].get("Boundary").get("Points")],
                 "related_urls": [url_item.get("URL") for url_item in item.get("umm").get("RelatedUrls")],
                 "identifier": next(attr.get("Values")[0]
                                    for attr in item.get("umm").get("AdditionalAttributes")
                                    if attr.get("Name") == collection_identifier_map[
                                        args.collection]) if args.collection in collection_identifier_map else None}
                for item in items], next_search_after
    else:
        return [], None


def _filter_granules(granule, args):
    collection_map = {"HLSL30": ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"],
                      "HLSS30": ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"],
                      "SENTINEL-1A_SLC": ["IW"],
                      "SENTINEL-1B_SLC": ["IW"],
                      "DEFAULT": ["tif"]}
    filter_extension = "DEFAULT"

    for collection in collection_map:
        if collection in args.collection:
            filter_extension = collection
            break

    return [f
            for f in granule.get("related_urls")
            for extension in collection_map.get(filter_extension)
            if extension in f]


def _match_identifier(settings, args, granule) -> bool:
    for filter in settings["SHORTNAME_FILTERS"][args.collection]:
        if re.match(filter, granule["identifier"]):
            return True

    return False


def submit_download_job(*, release_version=None, provider="LPCLOUD", params: list[dict[str, str]],
                        job_queue: str) -> str:
    provider_map = {"LPCLOUD": "hls", "ASF": "slc"}
    job_spec_str = f"job-{provider_map[provider]}_download:{release_version}"

    return _submit_mozart_job_minimal(hysdsio={"id": str(uuid.uuid4()),
                                               "params": params,
                                               "job-specification": job_spec_str},
                                      job_queue=job_queue,
                                      provider_str=provider_map[provider])


def _submit_mozart_job_minimal(*, hysdsio: dict, job_queue: str, provider_str: str) -> str:
    return submit_mozart_job(
        hysdsio=hysdsio,
        product={},
        rule={
            "rule_name": f"trigger-{provider_str}_download",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name=f"job-WF-{provider_str}_download",
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component=None
    )


def _url_to_orbit_number(url: str):
    orbit_re = r"_\d{6}_"  # Orbit number

    input_filename = Path(url).name
    orbit_number: str = re.findall(orbit_re, input_filename)[0]
    return orbit_number[1:-1] # Strips leading and trailing underscores


def _hls_url_to_granule_id(url: str):
    # remove both suffixes to get granule ID (e.g. removes .Fmask.tif)
    granule_id = PurePath(url).with_suffix("").with_suffix("").name
    return granule_id


def _url_to_tile_id(url: str):
    tile_re = r"T\w{5}"

    input_filename = Path(url).name
    tile_id: str = re.findall(tile_re, input_filename)[0]
    return tile_id


def run_download(args, token, es_conn, netloc, username, password, job_id):
    download_timerange = get_download_timerange(args)
    all_pending_downloads: Iterable[dict] = es_conn.get_all_between(
        dateutil.parser.isoparse(download_timerange.start_date),
        dateutil.parser.isoparse(download_timerange.end_date),
        args.use_temporal
    )

    downloads = all_pending_downloads
    if args.batch_ids:
        logging.info(f"Filtering pending downloads by {args.batch_ids=}")
        id_func = _to_granule_id if args.provider == "LPCLOUD" else _to_orbit_number
        downloads = list(filter(lambda d: id_func(d) in args.batch_ids, all_pending_downloads))
        logging.info(f"{len(downloads)=}")
        logging.debug(f"{downloads=}")

    if not downloads:
        logging.info(f"No undownloaded files found in index.")
        return

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 tile(s).")
        args.batch_ids = args.batch_ids[:1]

    session = SessionWithHeaderRedirection(username, password, netloc)

    if args.provider == "ASF":
        download_urls = [_to_https_url(download) for download in downloads if _has_url(download)]
        logging.debug(f"{download_urls=}")
        download_from_asf(es_conn=es_conn, download_urls=download_urls, args=args, token=token, job_id=job_id)
    elif args.transfer_protocol == "https":
        download_urls = [_to_https_url(download) for download in downloads if _has_url(download)]
        logging.debug(f"{download_urls=}")

        granule_id_to_download_urls_map = group_download_urls_by_granule_id(download_urls)
        download_granules(session, es_conn, granule_id_to_download_urls_map, args, token, job_id)
    else:
        download_urls = [_to_s3_url(download) for download in downloads if _has_url(download)]
        logging.debug(f"{download_urls=}")

        granule_id_to_download_urls_map = group_download_urls_by_granule_id(download_urls)
        download_granules(session, es_conn, granule_id_to_download_urls_map, args, None, job_id)

    logging.info(f"Total files updated: {len(download_urls)}")


def _to_orbit_number(dl_doc: dict[str, Any]):
    return _url_to_orbit_number(_to_url(dl_doc))


def group_download_urls_by_granule_id(download_urls):
    granule_id_to_download_urls_map = defaultdict(list)
    for download_url in download_urls:
        # remove both suffixes to get granule ID (e.g. removes .Fmask.tif)
        granule_id = PurePath(download_url).with_suffix("").with_suffix("").name
        granule_id_to_download_urls_map[granule_id].append(download_url)
    return granule_id_to_download_urls_map


def _to_granule_id(dl_doc: dict[str, Any]):
    return _hls_url_to_granule_id(_to_url(dl_doc))

def _to_tile_id(dl_doc: dict[str, Any]):
    return _url_to_tile_id(_to_url(dl_doc))


def _to_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    elif dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


def _has_url(dl_dict: dict[str, Any]):
    if dl_dict.get("https_url"):
        return True
    if dl_dict.get("s3_url"):
        return True

    logging.error(f"Couldn't find any URL in {dl_dict=}")
    return False


def _to_https_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


def download_from_asf(
        es_conn,
        download_urls: list[str],
        args,
        token,
        job_id
):
    settings_cfg = SettingsConf().cfg  # has metadata extractor config
    logging.info("Creating directories to process products")

    # house all file downloads
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    if args.dry_run:
        logging.info(f"{args.dry_run=}. Skipping downloads.")

    for product_url in download_urls:
        logging.info(f"Processing {product_url=}")
        product_id = PurePath(product_url).name

        product_download_dir = downloads_dir / product_id
        product_download_dir.mkdir(exist_ok=True)

        # download product
        if args.dry_run:
            logging.debug(f"{args.dry_run=}. Skipping download.")
            continue
        product = product_filepath = download_asf_product(product_url, token, product_download_dir)
        logging.info(f"{product_filepath=}")

        logging.info(f"Marking as downloaded. {product_url=}")
        es_conn.mark_product_as_downloaded(product_url, job_id)

        logging.info(f"product_url_downloaded={product_url}")

        logging.info("downloading associated orbit file")
        dataset_dir = extract_one_to_one(product, settings_cfg, working_dir=Path.cwd())
        stage_orbit_file_args = stage_orbit_file.get_parser().parse_args([
            f"--output-directory={str(dataset_dir)}",
            str(product_filepath)
        ])
        stage_orbit_file.main(stage_orbit_file_args)
        logging.info("added orbit file to dataset")

    logging.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)


def download_granules(
        session: requests.Session,
        es_conn,
        granule_id_to_product_urls_map: dict[str, list[str]],
        args,
        token,
        job_id
):
    cfg = SettingsConf().cfg  # has metadata extractor config
    logging.info("Creating directories to process granules")
    # house all file downloads
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    if args.dry_run:
        logging.info(f"{args.dry_run=}. Skipping downloads.")

    if args.smoke_run:
        granule_id_to_product_urls_map = dict(itertools.islice(granule_id_to_product_urls_map.items(), 1))

    for granule_id, product_urls in granule_id_to_product_urls_map.items():
        logging.info(f"Processing {granule_id=}")

        granule_download_dir = downloads_dir / granule_id
        granule_download_dir.mkdir(exist_ok=True)

        # download products in granule
        products = []
        product_urls_downloaded = []
        for product_url in product_urls:
            if args.dry_run:
                logging.debug(f"{args.dry_run=}. Skipping download.")
                break
            product_filepath = download_product(product_url, session, token, args, granule_download_dir)
            products.append(product_filepath)
            product_urls_downloaded.append(product_url)
        logging.info(f"{products=}")

        logging.info(f"Marking as downloaded. {granule_id=}")
        for product_url in product_urls_downloaded:
            es_conn.mark_product_as_downloaded(product_url, job_id)

        logging.info(f"{len(product_urls_downloaded)=}, {product_urls_downloaded=}")

        extract_many_to_one(products, granule_id, cfg)

        logging.info(f"Removing directory {granule_download_dir}")
        shutil.rmtree(granule_download_dir)

    logging.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)


def download_product(product_url, session: requests.Session, token: str, args, target_dirpath: Path):
    if args.transfer_protocol.lower() == "https":
        product_filepath = download_product_using_https(
            product_url,
            session,
            token,
            target_dirpath=target_dirpath.resolve()
        )
    elif args.transfer_protocol.lower() == "s3":
        product_filepath = download_product_using_s3(
            product_url,
            session,
            target_dirpath=target_dirpath.resolve(),
            args=args
        )
    else:
        raise Exception(args.transfer_protocol)
    return product_filepath


def download_asf_product(product_url, token: str, target_dirpath: Path):
    logging.info(f"Requesting from {product_url}")

    asf_response = _handle_url_redirect(product_url, token)
    asf_response.raise_for_status()

    product_filename = PurePath(product_url).name
    product_download_path = target_dirpath / product_filename
    with open(product_download_path, "wb") as file:
        file.write(asf_response.content)
    return product_download_path.resolve()


def extract_many_to_one(products: list[Path], group_dataset_id, settings_cfg: dict):
    """Creates a dataset for each of the given products, merging them into 1 final dataset.

    :param products: the products to create datasets for.
    :param group_dataset_id: a unique identifier for the group of products.
    :param settings_cfg: the settings.yaml config as a dict.
    """
    # house all datasets / extracted metadata
    extracts_dir = Path("extracts")
    extracts_dir.mkdir(exist_ok=True)

    # create individual dataset dir for each product in the granule
    # (this also extracts the metadata to *.met.json files)
    product_extracts_dir = extracts_dir / group_dataset_id
    product_extracts_dir.mkdir(exist_ok=True)
    dataset_dirs = [
        extract_one_to_one(product, settings_cfg, working_dir=product_extracts_dir)
        for product in products
    ]
    logging.info(f"{dataset_dirs=}")

    # generate merge metadata from single-product datasets
    shared_met_entries_dict = {}  # this is updated, when merging, with metadata common to multiple input files
    total_product_file_sizes, merged_met_dict = \
        product2dataset.product2dataset.merge_dataset_met_json(
            str(product_extracts_dir.resolve()),
            extra_met=shared_met_entries_dict  # copy some common metadata from each product.
        )
    logging.debug(f"{merged_met_dict=}")

    logging.info("Creating target dataset directory")
    target_dataset_dir = Path(group_dataset_id)
    target_dataset_dir.mkdir(exist_ok=True)
    for product in products:
        shutil.copy(product, target_dataset_dir.resolve())
    logging.info("Copied input products to dataset directory")

    logging.info("update merged *.met.json with additional, top-level metadata")
    merged_met_dict.update(shared_met_entries_dict)
    merged_met_dict["FileSize"] = total_product_file_sizes
    merged_met_dict["FileName"] = group_dataset_id
    merged_met_dict["id"] = group_dataset_id
    logging.debug(f"{merged_met_dict=}")

    # write out merged *.met.json
    merged_met_json_filepath = target_dataset_dir.resolve() / f"{target_dataset_dir.name}.met.json"
    with open(merged_met_json_filepath, mode="w") as output_file:
        json.dump(merged_met_dict, output_file)
    logging.info(f"Wrote {merged_met_json_filepath=!s}")

    # write out basic *.dataset.json file (value + created_timestamp)
    dataset_json_dict = extractor.extract.create_dataset_json(
        product_metadata={"dataset_version": merged_met_dict["dataset_version"]},
        ds_met={},
        alt_ds_met={}
    )
    granule_dataset_json_filepath = target_dataset_dir.resolve() / f"{group_dataset_id}.dataset.json"
    with open(granule_dataset_json_filepath, mode="w") as output_file:
        json.dump(dataset_json_dict, output_file)
    logging.info(f"Wrote {granule_dataset_json_filepath=!s}")

    shutil.rmtree(extracts_dir)


def extract_one_to_one(product: Path, settings_cfg: dict, working_dir: Path) -> PurePath:
    """Creates a dataset for the given product.

    :param product: the product to create datasets for.
    :param settings_cfg: the settings.yaml config as a dict.
    :param working_dir: the working directory for the extract process. Serves as the output directory for the extraction.
    """
    # create dataset dir for product
    # (this also extracts the metadata to *.met.json file)
    logging.info("Creating dataset directory")
    dataset_dir = extractor.extract.extract(
        product=str(product),
        product_types=settings_cfg["PRODUCT_TYPES"],
        workspace=str(working_dir.resolve())
    )
    logging.info(f"{dataset_dir=}")
    return PurePath(dataset_dir)


def download_product_using_https(url, session: requests.Session, token, target_dirpath: Path, chunk_size=25600) -> Path:
    headers = {"Echo-Token": token}
    with session.get(url, headers=headers) as r:
        r.raise_for_status()

        file_name = PurePath(url).name
        product_download_path = target_dirpath / file_name
        with open(product_download_path, "wb") as output_file:
            output_file.write(r.content)
        return product_download_path.resolve()


def download_product_using_s3(url, session: requests.Session, target_dirpath: Path, args) -> Path:
    aws_creds = _get_aws_creds(session)
    logging.debug(f"{_get_aws_creds.cache_info()=}")

    s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                       aws_secret_access_key=aws_creds['secretAccessKey'],
                       aws_session_token=aws_creds['sessionToken'],
                       region_name='us-west-2').client("s3")
    product_download_path = _s3_download(url, s3, str(target_dirpath))
    return product_download_path.resolve()


def _https_transfer(url, bucket_name, token, staging_area=""):
    file_name = PurePath(url).name
    bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name
    key = Path(staging_area, file_name).name

    upload_start_time = datetime.utcnow()

    try:
        logging.info(f"Requesting from {url}")
        r = _handle_url_redirect(url, token)
        if r.status_code != 200:
            r.raise_for_status()

        with open("https.tmp", "wb") as file:
            file.write(r.content)

        logging.info(f"Uploading {file_name} to {bucket=}, {key=}")
        with open("https.tmp", "rb") as file:
            s3 = boto3.client("s3")
            s3.upload_fileobj(file, bucket, key)

        upload_end_time = datetime.utcnow()
        upload_duration = upload_end_time - upload_start_time
        upload_stats = {"file_name": file_name,
                        "file_size (in bytes)": r.headers.get("Content-Length"),
                        "upload_duration (in seconds)": upload_duration.total_seconds(),
                        "upload_start_time": _convert_datetime(upload_start_time),
                        "upload_end_time": _convert_datetime(upload_end_time)}
        logging.debug(f"{upload_stats=}")

        return upload_stats
    except (Exception, ConnectionResetError, requests.exceptions.HTTPError) as e:
        logging.error(e)
        return {"failed_download": e}


def _handle_url_redirect(url, token):
    if not validators.url(url):
        raise Exception(f"Malformed URL: {url}")

    r = requests.get(url, allow_redirects=False)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return requests.get(r.headers["Location"], headers=headers, allow_redirects=True)


def _convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def _to_s3_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


@ttl_cache(ttl=3300)  # 3300s == 55m. Refresh credentials before expiry. Note: validity period is 60 minutes
def _get_aws_creds(session):
    logging.info("entry")

    with session.get("https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials") as r:
        r.raise_for_status()

        return r.json()


def _s3_transfer(url, bucket_name, s3, tmp_dir, staging_area=""):
    try:
        _s3_download(url, s3, tmp_dir, staging_area)
        target_key = _s3_upload(url, bucket_name, tmp_dir, staging_area)

        return {"successful_download": target_key}
    except Exception as e:
        return {"failed_download": e}


def _s3_download(url, s3, tmp_dir, staging_area=""):
    file_name = PurePath(url).name
    target_key = str(Path(staging_area, file_name))

    source = url[len("s3://"):].partition("/")
    source_bucket = source[0]
    source_key = source[2]

    s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

    return Path(f"{tmp_dir}/{target_key}")


def _s3_upload(url, bucket_name, tmp_dir, staging_area=""):
    file_name = PurePath(url).name
    target_key = str(Path(staging_area, file_name))
    target_bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name

    target_s3 = boto3.resource("s3")
    target_s3.Bucket(target_bucket).upload_file(f"{tmp_dir}/{target_key}", target_key)

    return target_key


if __name__ == "__main__":
    asyncio.run(run(sys.argv))
