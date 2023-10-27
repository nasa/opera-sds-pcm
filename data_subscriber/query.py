import asyncio
import logging
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked

from data_subscriber.cmr import query_cmr, COLLECTION_TO_PRODUCT_TYPE_MAP
from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection
from data_subscriber.slc_spatial.slc_spatial_catalog_connection import get_slc_spatial_catalog_connection
from data_subscriber.url import form_batch_id, _slc_url_to_chunk_id
from geo.geo_util import does_bbox_intersect_north_america

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


async def run_query(args, token, es_conn, cmr, job_id, settings):
    query_dt = datetime.now()
    now = datetime.utcnow()
    query_timerange: DateTimeRange = get_query_timerange(args, now)

    granules = query_cmr(args, token, cmr, settings, query_timerange, now)

    if args.smoke_run:
        logger.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    # group URLs by this mapping func. E.g. group URLs by granule_id
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] in ("HLS", "RTC", "CSLC"):
        keyfunc = form_batch_id
    elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
        keyfunc = _slc_url_to_chunk_id
    else:
        raise AssertionError(f"Can't use {args.collection=} to select grouping function.")

    batch_id_to_urls_map = defaultdict(set)

    for granule in granules:
        granule_id = granule.get("granule_id")
        revision_id = granule.get("revision_id")

        additional_fields = {}
        additional_fields["revision_id"] = revision_id
        additional_fields["processing_mode"] = args.proc_mode

        # If processing mode is historical,
        # throw out any granules that do not intersect with North America
        if args.proc_mode == "historical" and not does_bbox_intersect_north_america(granule["bounding_box"]):
            logger.info(f"Processing mode is historical and the following granule does not intersect with \
North America. Skipping processing. %s" % granule.get("granule_id"))
            continue

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
            if does_bbox_intersect_north_america(granule["bounding_box"]):
                additional_fields["intersects_north_america"] = True
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            pass
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
            raise NotImplementedError()
        else:
            pass

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

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "HLS":
            spatial_catalog_conn = get_hls_spatial_catalog_connection(logging.getLogger(__name__))
            update_granule_index(spatial_catalog_conn, granule)
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
            spatial_catalog_conn = get_slc_spatial_catalog_connection(logging.getLogger(__name__))
            update_granule_index(spatial_catalog_conn, granule)
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            pass
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
            raise NotImplementedError()
        else:
            pass

        if granule.get("filtered_urls"):
            for filter_url in granule.get("filtered_urls"):
                batch_id_to_urls_map[keyfunc(granule_id, revision_id)].add(filter_url)

    if args.subparser_name == "full":
        logger.info(f"{args.subparser_name=}. Skipping download job submission. Download will be performed directly.")
        return

    if args.no_schedule_download:
        logger.info(f"{args.no_schedule_download=}. Forcefully skipping download job submission.")
        return

    if not args.chunk_size:
        logger.info(f"{args.chunk_size=}. Insufficient chunk size. Skipping download job submission.")
        return

    logger.info(f"{batch_id_to_urls_map=}")
    job_submission_tasks = []
    logger.info(f"{args.chunk_size=}")
    for batch_chunk in chunked(batch_id_to_urls_map.items(), n=args.chunk_size):
        chunk_id = str(uuid.uuid4())
        logger.info(f"{chunk_id=}")

        chunk_batch_ids = []
        chunk_urls = []
        for batch_id, urls in batch_chunk:
            chunk_batch_ids.append(batch_id)
            chunk_urls.extend(urls)

        logger.info(f"{chunk_batch_ids=}")
        logger.info(f"{chunk_urls=}")

        job_submission_tasks.append(
            asyncio.get_event_loop().run_in_executor(
                executor=None,
                func=partial(
                    submit_download_job,
                    release_version=args.release_version,
                    product_type=COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection],
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
    logger.info(f"{len(results)=}")
    logger.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logger.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logger.info(f"{failed=}")

    return {
        "success": succeeded,
        "fail": failed
    }


def get_query_timerange(args, now: datetime, silent=False):
    now_minus_minutes_dt = (now - timedelta(minutes=args.minutes)) if not args.native_id else dateutil.parser.isoparse("1900-01-01T00:00:00Z")

    start_date = args.start_date if args.start_date else now_minus_minutes_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = args.end_date if args.end_date else now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_timerange = DateTimeRange(start_date, end_date)
    if not silent:
        logger.info(f"{query_timerange=}")
    return query_timerange


def submit_download_job(*, release_version=None, product_type: Literal["HLS", "SLC", "RTC", "CSLC"], params: list[dict[str, str]], job_queue: str) -> str:
    job_spec_str = f"job-{product_type.lower()}_download:{release_version}"

    return _submit_mozart_job_minimal(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec_str
        },
        job_queue=job_queue,
        provider_str=product_type.lower()
    )


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
    # group pairs of URLs (http and s3) by filename
    filename_to_urls_map = defaultdict(list)
    for url in urls:
        filename = Path(url).name
        filename_to_urls_map[filename].append(url)

    for filename, filename_urls in filename_to_urls_map.items():
        es_conn.process_url(filename_urls, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args, **kwargs)


def update_granule_index(es_spatial_conn, granule, *args, **kwargs):
    es_spatial_conn.process_granule(granule, *args, **kwargs)
