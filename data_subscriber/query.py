import asyncio
import logging
import re
import uuid
from collections import namedtuple
from datetime import datetime, timedelta
from functools import partial

import dateutil.parser
import requests
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import map_reduce, chunked

from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection
from data_subscriber.slc_spatial.slc_spatial_catalog_connection import get_slc_spatial_catalog_connection
from data_subscriber.url import _hls_url_to_granule_id, _slc_url_to_chunk_id
from geo.geo_util import does_bbox_intersect_north_america

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])
PRODUCT_PROVIDER_MAP = {"HLSL30": "LPCLOUD",
                        "HLSS30": "LPCLOUD",
                        "SENTINEL-1A_SLC": "ASF",
                        "SENTINEL-1B_SLC": "ASF"}


async def run_query(args, token, es_conn, cmr, job_id, settings):
    query_dt = datetime.now()
    now = datetime.utcnow()
    query_timerange: DateTimeRange = get_query_timerange(args, now)

    granules = query_cmr(args, token, cmr, settings, query_timerange, now)

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    download_urls: list[str] = []

    for granule in granules:
        additional_fields = {}

        additional_fields["processing_mode"] = args.proc_mode

        # If processing mode is historical,
        # throw out any granules that do not intersect with North America
        if args.proc_mode == "historical" and not does_bbox_intersect_north_america(granule["bounding_box"]):
            logging.info(f"Processing mode is historical and the following granule does not intersect with \
North America. Skipping processing. %s" % granule.get("granule_id"))
            continue

        if PRODUCT_PROVIDER_MAP[args.collection] == "ASF":
            if does_bbox_intersect_north_america(granule["bounding_box"]):
                additional_fields["intersects_north_america"] = True

        update_url_index(
            es_conn,
            granule.get("filtered_urls"),
            granule.get("granule_id"),
            job_id,
            query_dt,
            temporal_extent_beginning_dt=dateutil.parser.isoparse(granule["temporal_extent_beginning_datetime"]),
            revision_date_dt=dateutil.parser.isoparse(granule["revision_date"]),
            **additional_fields
        )

        if PRODUCT_PROVIDER_MAP[args.collection] == "LPCLOUD":
            spatial_catalog_conn = get_hls_spatial_catalog_connection(logging.getLogger(__name__))
            update_granule_index(spatial_catalog_conn, granule)
        elif PRODUCT_PROVIDER_MAP[args.collection] == "ASF":
            spatial_catalog_conn = get_slc_spatial_catalog_connection(logging.getLogger(__name__))
            update_granule_index(spatial_catalog_conn, granule)

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
    keyfunc = _hls_url_to_granule_id if PRODUCT_PROVIDER_MAP[args.collection] == "LPCLOUD" else _slc_url_to_chunk_id
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
                    provider=PRODUCT_PROVIDER_MAP[args.collection],
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
                        },
                        {
                            "name": "transfer_protocol",
                            "value": f"--transfer-protocol={args.transfer_protocol}",
                            "from": "value"
                        },
                        {
                            "name": "proc_mode",
                            "value": f"--processing-mode={args.proc_mode}",
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


def query_cmr(args, token, cmr, settings, timerange: DateTimeRange, now: datetime) -> list:
    page_size = 2000
    request_url = f"https://{cmr}/search/granules.umm_json"
    bounding_box = args.bbox

    if args.collection == "SENTINEL-1A_SLC" or args.collection == "SENTINEL-1B_SLC":
        bound_list = bounding_box.split(",")

        # Excludes Antarctica
        if float(bound_list[1]) < -60:
            bound_list[1] = "-60"
            bounding_box = ",".join(bound_list)

    params = {
        "page_size": page_size,
        "sort_key": "-start_date",
        "provider": PRODUCT_PROVIDER_MAP[args.collection],
        "ShortName": args.collection,
        "token": token,
        "bounding_box": bounding_box
    }

    if args.native_id:
        params["native-id"] = args.native_id

        if any(wildcard in args.native_id for wildcard in ['*', '?']):
            params["options[native-id][pattern]"] = 'true'

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
                 "bounding_box": [
                     {"lat": point.get("Latitude"), "lon": point.get("Longitude")}
                     for point
                     in item.get("umm")
                         .get("SpatialExtent")
                         .get("HorizontalSpatialDomain")
                         .get("Geometry")
                         .get("GPolygons")[0]
                         .get("Boundary")
                         .get("Points")
                 ],
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


def update_url_index(
        es_conn,
        urls: list[str],
        granule_id: str,
        job_id: str,
        query_dt: datetime,
        temporal_extent_beginning_dt: datetime,
        revision_date_dt: datetime,
        *args,
        **kwargs
):
    for url in urls:
        es_conn.process_url(url, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args,
                            **kwargs)


def update_granule_index(es_spatial_conn, granule, *args, **kwargs):
    es_spatial_conn.process_granule(granule, *args, **kwargs)
