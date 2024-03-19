import asyncio
import logging
import uuid
import os
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
import hashlib

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import map_reduce, chunked

from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection
from data_subscriber.slc_spatial.slc_spatial_catalog_connection import get_slc_spatial_catalog_connection
from data_subscriber.url import form_batch_id, _slc_url_to_chunk_id
from data_subscriber.cmr import query_cmr, PRODUCT_PROVIDER_MAP
from geo.geo_util import does_bbox_intersect_north_america, does_bbox_intersect_region, _NORTH_AMERICA
from util.conf_util import SettingsConf
from util.pge_util import download_object_from_s3

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

async def run_query(args, token, es_conn, cmr, job_id, settings):
    query_dt = datetime.now()
    now = datetime.utcnow()
    query_timerange: DateTimeRange = get_query_timerange(args, now)

    granules = query_cmr(args, token, cmr, settings, query_timerange, now)

    if args.smoke_run:
        logging.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    # group URLs by this mapping func. E.g. group URLs by granule_id
    keyfunc = form_batch_id if PRODUCT_PROVIDER_MAP[args.collection] == "LPCLOUD" else _slc_url_to_chunk_id
    batch_id_to_urls_map = defaultdict(set)

    # If we are processing ASF collection, we're gonna need the north america geojson
    if PRODUCT_PROVIDER_MAP[args.collection] == "ASF":
        localize_geojsons([_NORTH_AMERICA])

    # If processing mode is historical, apply include/exclude-region filtering
    if args.proc_mode == "historical":
        logging.info(f"Processing mode is historical so applying include and exclude regions...")

        # Fetch all necessary geojson files from S3
        localize_include_exclude(args)

        granules = filter_granules_by_regions(granules, args.include_regions, args.exclude_regions)

    for granule in granules:

        granule_id = granule.get("granule_id")
        revision_id = granule.get("revision_id")

        additional_fields = {}
        additional_fields["revision_id"] = revision_id
        additional_fields["processing_mode"] = args.proc_mode

        if PRODUCT_PROVIDER_MAP[args.collection] == "ASF":
            if does_bbox_intersect_north_america(granule["bounding_box"]):
                additional_fields["intersects_north_america"] = True

        update_url_index(
            es_conn,
            granule.get("filtered_urls"),
            granule_id,
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
            for filter_url in granule.get("filtered_urls"):
                batch_id_to_urls_map[keyfunc(granule_id, revision_id)].add(filter_url)

    if args.subparser_name == "full":
        logging.info(f"{args.subparser_name=}. Skipping download job submission.")
        return

    if args.no_schedule_download:
        logging.info(f"{args.no_schedule_download=}. Skipping download job submission.")
        return

    if not args.chunk_size:
        logging.info(f"{args.chunk_size=}. Skipping download job submission.")
        return

    '''batch_id_to_urls_map: dict[str, set[str]] = map_reduce(
        iterable=granules,
        keyfunc=keyfunc,
        valuefunc=lambda url: url,
        reducefunc=set
    )'''

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

        # If we are processing ASF collection, we will compute payload hash using the granule_id without the revision_id
        # NOTE: This will only work properly if the chunk size is 1 which should always be the case for ASF
        payload_hash = None
        if PRODUCT_PROVIDER_MAP[args.collection] == "ASF":
            granule_to_hash = ''
            for batch_id in chunk_batch_ids:
                granule_id, revision_id = es_conn.granule_and_revision(batch_id)
                granule_to_hash += granule_id

            payload_hash = hashlib.md5(granule_to_hash.encode()).hexdigest()

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
                    job_queue=args.job_queue,
                    payload_hash = payload_hash
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


def get_query_timerange(args, now: datetime, silent=False):
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus_minutes_date = (now - timedelta(minutes=args.minutes)).strftime(
        "%Y-%m-%dT%H:%M:%SZ") if not args.native_id else "1900-01-01T00:00:00Z"
    start_date = args.start_date if args.start_date else now_minus_minutes_date
    end_date = args.end_date if args.end_date else now_date

    query_timerange = DateTimeRange(start_date, end_date)
    if not silent:
        logging.info(f"{query_timerange=}")
    return query_timerange

def submit_download_job(*, release_version=None, provider="LPCLOUD", params: list[dict[str, str]],
                        job_queue: str, payload_hash = None) -> str:
    provider_map = {"LPCLOUD": "hls", "ASF": "slc"}
    job_spec_str = f"job-{provider_map[provider]}_download:{release_version}"

    return _submit_mozart_job_minimal(hysdsio={"id": str(uuid.uuid4()),
                                               "params": params,
                                               "job-specification": job_spec_str},
                                      job_queue=job_queue,
                                      provider_str=provider_map[provider],
                                      payload_hash=payload_hash)


def _submit_mozart_job_minimal(*, hysdsio: dict, job_queue: str, provider_str: str, payload_hash = None) -> str:
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
        payload_hash=payload_hash,
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
    # group pairs of URLs (http and s3) by filename
    filename_to_urls_map = defaultdict(list)
    for url in urls:
        filename = Path(url).name
        filename_to_urls_map[filename].append(url)

    for filename, filename_urls in filename_to_urls_map.items():
        es_conn.process_url(filename_urls, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args, **kwargs)


def update_granule_index(es_spatial_conn, granule, *args, **kwargs):
    es_spatial_conn.process_granule(granule, *args, **kwargs)

def localize_include_exclude(args):

    geojsons = []

    if args.include_regions is not None:
        geojsons.extend(args.include_regions.split(","))

    if args.exclude_regions is not None:
        geojsons.extend(args.exclude_regions.split(","))

    localize_geojsons(geojsons)

def localize_geojsons(geojsons):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]

    try:
        for geojson in geojsons:
            key = geojson.strip() + ".geojson"
            # output_filepath = os.path.join(working_dir, key)
            download_object_from_s3(bucket, key, key, filetype="geojson")
    except Exception as e:
        raise Exception("Exception while fetching geojson file: %s. " % key + str(e))

def does_granule_intersect_regions(granule, intersect_regions):
    regions = intersect_regions.split(',')
    for region in regions:
        region = region.strip()
        if does_bbox_intersect_region(granule["bounding_box"], region):
            return True, region

    return False, None

def filter_granules_by_regions(granules, include_regions, exclude_regions):
    '''Filters granules based on include and exclude regions lists'''
    filtered = []

    for granule in granules:

        # Skip this granule if it's not in the include list
        if include_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, include_regions)
            if result is False:
                logging.info(
                    f"The following granule does not intersect with any include regions. Skipping processing %s"
                    % granule.get("granule_id"))
                continue

        # Skip this granule if it's in the exclude list
        if exclude_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, exclude_regions)
            if result is True:
                logging.info(f"The following granule intersects with the exclude region %s. Skipping processing %s"
                             % (region, granule.get("granule_id")))
                continue

        # If both filters don't apply, add this granule to the list
        filtered.append(granule)

    return filtered
