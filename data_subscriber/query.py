import asyncio
import logging
import math
import re
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal
import boto3
import json

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, first
from data_subscriber.cmr import COLLECTION_TO_PRODUCT_TYPE_MAP, async_query_cmr, CMR_COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.url import form_batch_id, form_batch_id_cslc, _slc_url_to_chunk_id
from data_subscriber.geojson_utils import localize_geojsons, localize_include_exclude, filter_granules_by_regions
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from geo.geo_util import does_bbox_intersect_north_america, does_bbox_intersect_region, _NORTH_AMERICA
from util.conf_util import SettingsConf

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

async def run_query(args, token, es_conn: HLSProductCatalog, cmr, job_id, settings):
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "HLS":
        from data_subscriber.hls.hls_query import HlsCmrQuery
        cmr_query = HlsCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
        from data_subscriber.slc.slc_query import SlcCmrQuery
        cmr_query = SlcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        from data_subscriber.rtc.rtc_query import RtcCmrQuery
        cmr_query = RtcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
        from data_subscriber.cslc.cslc_query import CslcCmrQuery
        cmr_query = CslcCmrQuery(args, token, es_conn, cmr, job_id, settings)

    result = await cmr_query.run_query(args, token, es_conn, cmr, job_id, settings)
    return result

class CmrQuery:
    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        self.args = args
        self.token = token
        self.es_conn = es_conn
        self.cmr = cmr
        self.job_id = job_id
        self.settings = settings
        self.proc_mode = args.proc_mode

    async def run_query(self, args, token, es_conn: HLSProductCatalog, cmr, job_id, settings):
        query_dt = datetime.now()
        now = datetime.utcnow()
        query_timerange: DateTimeRange = get_query_timerange(args, now)

        logger.info("CMR query STARTED")
        granules = await self.query_cmr(args, token, cmr, settings, query_timerange, now)
        logger.info("CMR query FINISHED")

        # Evaluate granules for additional catalog record and extend list if found: granules is MODIFIED in place
        # Can only happen for RTC and CSLC files
        self.extend_additional_records(granules)

        if args.smoke_run:
            logger.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
            granules = granules[:1]

        # If processing mode is historical, apply include/exclude-region filtering
        if self.proc_mode == "historical":
            logging.info(f"Processing mode is historical so applying include and exclude regions...")

            # Fetch all necessary geojson files from S3
            localize_include_exclude(args)
            granules[:] = filter_granules_by_regions(granules, args.include_regions, args.exclude_regions)

        # TODO: This function only applies to CSLC
        download_granules = self.determine_download_granules(granules)

        logger.info("catalogue-ing STARTED")
        self.catalog_granules(granules, query_dt)
        logger.info("catalogue-ing FINISHED")

        #TODO: This function only applies to RTC
        batch_id_to_products_map = await self.refresh_index()

        if args.subparser_name == "full":
            logger.info(
                f"{args.subparser_name=}. Skipping download job submission. Download will be performed directly.")
            if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
                args.provider = CMR_COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                args.batch_ids = self.affected_mgrs_set_id_acquisition_ts_cycle_indexes
            return
        if args.no_schedule_download:
            logger.info(f"{args.no_schedule_download=}. Forcefully skipping download job submission.")
            return
        if not args.chunk_size:
            logger.info(f"{args.chunk_size=}. Insufficient chunk size. Skipping download job submission.")
            return

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            job_submission_tasks = submit_rtc_download_job_submissions_tasks(batch_id_to_products_map.keys(), args)
        else:
            job_submission_tasks = download_job_submission_handler(args, download_granules, query_timerange, settings)

        results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
        logger.info(f"{len(results)=}")
        logger.info(f"{results=}")

        succeeded = [job_id for job_id in results if isinstance(job_id, str)]
        failed = [e for e in results if isinstance(e, Exception)]

        logger.info(f"{succeeded=}")
        logger.info(f"{failed=}")

        return {
            "success": succeeded,
            "fail": failed
        }

    async def query_cmr(self, args, token, cmr, settings, timerange, now: datetime):
        granules = await async_query_cmr(args, token, cmr, settings, timerange, now)
        return granules

    def prepare_additional_fields(self, granule, args, granule_id):

        additional_fields = {}
        additional_fields["revision_id"] = granule.get("revision_id")
        additional_fields["processing_mode"] = args.proc_mode

        return additional_fields

    def extend_additional_records(self, granules):
        pass

    def determine_download_granules(self, granules):
        return granules

    def catalog_granules(self, granules, query_dt):
        for granule in granules:
            granule_id = granule.get("granule_id")

            additional_fields = self.prepare_additional_fields(granule, self.args, granule_id)

            update_url_index(
                self.es_conn,
                granule.get("filtered_urls"),
                granule_id,
                self.job_id,
                query_dt,
                temporal_extent_beginning_dt=dateutil.parser.isoparse(granule["temporal_extent_beginning_datetime"]),
                revision_date_dt=dateutil.parser.isoparse(granule["revision_date"]),
                **additional_fields
            )

            self.update_granule_index(granule)

    def update_granule_index(self, granule):
        pass

    async def refresh_index(self):
        pass

def download_job_submission_handler(args, granules, query_timerange, settings):
    batch_id_to_urls_map = defaultdict(set)
    for granule in granules:
        granule_id = granule.get("granule_id")
        revision_id = granule.get("revision_id")

        if granule.get("filtered_urls"):
            # group URLs by this mapping func. E.g. group URLs by granule_id
            if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "HLS":
                url_grouping_func = form_batch_id
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
                url_grouping_func = _slc_url_to_chunk_id
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
                pass
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
                # CSLC will use the download_batch_id directly
                pass
            else:
                raise AssertionError(f"Can't use {args.collection=} to select grouping function.")

            for filter_url in granule.get("filtered_urls"):
                if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
                    batch_id_to_urls_map[granule["download_batch_id"]].add(filter_url)
                else:
                    batch_id_to_urls_map[url_grouping_func(granule_id, revision_id)].add(filter_url)
    logger.info(f"{batch_id_to_urls_map=}")
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        raise NotImplementedError()
    else:
        job_submission_tasks = submit_download_job_submissions_tasks(batch_id_to_urls_map, query_timerange, args, settings)
    return job_submission_tasks

def get_query_timerange(args, now: datetime, silent=False):
    now_minus_minutes_dt = (
                now - timedelta(minutes=args.minutes)) if not args.native_id else dateutil.parser.isoparse(
        "1900-01-01T00:00:00Z")

    start_date = args.start_date if args.start_date else now_minus_minutes_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = args.end_date if args.end_date else now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_timerange = DateTimeRange(start_date, end_date)
    if not silent:
        logger.info(f"{query_timerange=}")
    return query_timerange

def submit_download_job_submissions_tasks(batch_id_to_urls_map, query_timerange, args, settings):
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
                    release_version=settings["RELEASE_VERSION"],
                    product_type=COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection],
                    params=create_download_job_params(args, query_timerange, chunk_batch_ids),
                    job_queue=args.job_queue
                )
            )
        )
    return job_submission_tasks


def create_download_job_params(args, query_timerange, chunk_batch_ids):
    return [
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
    ]


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

def process_frame_burst_db():
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]

    try:
        for geojson in geojsons:
            key = geojson.strip() + ".geojson"
            # output_filepath = os.path.join(working_dir, key)
            download_from_s3(bucket, key, key)
    except Exception as e:
        raise Exception("Exception while fetching geojson file: %s. " % key + str(e))