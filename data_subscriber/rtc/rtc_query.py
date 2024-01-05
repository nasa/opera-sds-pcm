import asyncio
import logging
import re
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, first, last

from data_subscriber.cmr import COLLECTION_TO_PRODUCT_TYPE_MAP, async_query_cmr, CMR_COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.geojson_utils import localize_include_exclude, filter_granules_by_regions
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client, evaluator
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from data_subscriber.url import form_batch_id, _slc_url_to_chunk_id
from data_subscriber.query import CmrQuery
from geo.geo_util import does_bbox_intersect_region
from rtc_utils import rtc_granule_regex

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


class RtcCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

    async def run_query(self, args, token, es_conn: HLSProductCatalog, cmr, job_id, settings):

        query_dt = datetime.now()
        now = datetime.utcnow()
        query_timerange: DateTimeRange = get_query_timerange(args, now)

        logger.info("CMR query STARTED")
        granules = await async_query_cmr(args, token, cmr, settings, query_timerange, now)
        logger.info("CMR query FINISHED")

        if args.smoke_run:
            logger.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
            granules = granules[:1]

        # If processing mode is historical, apply include/exclude-region filtering
        if args.proc_mode == "historical":
            logger.info(f"Processing mode is historical so applying include and exclude regions...")

            # Fetch all necessary geojson files from S3
            localize_include_exclude(args)
            granules[:] = filter_granules_by_regions(granules, args.include_regions, args.exclude_regions)

        logger.info("catalogue-ing STARTED")

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            affected_mgrs_set_id_acquisition_ts_cycle_indexes = set()
            granules[:] = filter_granules_rtc(granules, args)

            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            if args.native_id:
                match_native_id = re.match(rtc_granule_regex, args.native_id)
                burst_id = mbc_client.product_burst_id_to_mapping_burst_id(match_native_id.group("burst_id"))

                native_id_mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))

        for granule in granules:
            granule_id = granule.get("granule_id")
            revision_id = granule.get("revision_id")

            additional_fields = {}
            additional_fields["revision_id"] = revision_id
            additional_fields["processing_mode"] = args.proc_mode

            additional_fields["instrument"] = "S1A" if "S1A" in granule_id else "S1B"

            match_product_id = re.match(rtc_granule_regex, granule_id)
            acquisition_dts = match_product_id.group("acquisition_ts")
            burst_id = match_product_id.group("burst_id")

            mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
            additional_fields["mgrs_set_ids"] = mgrs_burst_set_ids

            # RTC: Calculating the Collection Cycle Index (Part 1):
            #  required constants
            MISSION_EPOCH_S1A = dateutil.parser.isoparse("20190101T000000Z")  # set approximate mission start date
            MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
            MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
            ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=12).total_seconds()

            # RTC: Calculating the Collection Cycle Index (Part 2):
            #  RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
            #  The cycle restarts periodically with some miniscule drift over time and the life of the mission.
            burst_identification_number = int(burst_id.split(sep="-")[1])
            instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B
            seconds_after_mission_epoch = (
                        dateutil.parser.isoparse(acquisition_dts) - instrument_epoch).total_seconds()
            acquisition_index = (
                                        seconds_after_mission_epoch - (ACQUISITION_CYCLE_DURATION_SECS * (
                                            burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
                                ) / ACQUISITION_CYCLE_DURATION_SECS
            acquisition_cycle = round(acquisition_index)
            additional_fields["acquisition_cycle"] = acquisition_cycle

            mgrs_set_id_acquisition_ts_cycle_indexes = update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(
                acquisition_cycle, additional_fields, mgrs_burst_set_ids)
            if args.native_id:  # native-id supplied. don't affect adjacent burst sets, tossing out irrelevant burst sets
                matching_native_id_mgrs_burst_set_ids = list(set(native_id_mgrs_burst_set_ids) & set(mgrs_burst_set_ids))
                update_affected_mgrs_set_ids(acquisition_cycle, affected_mgrs_set_id_acquisition_ts_cycle_indexes, matching_native_id_mgrs_burst_set_ids)
            else:
                update_affected_mgrs_set_ids(acquisition_cycle, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids)

            es_conn: RTCProductCatalog
            es_conn.update_granule_index(
                granule=granule,
                job_id=job_id,
                query_dt=query_dt,
                mgrs_set_id_acquisition_ts_cycle_indexes=mgrs_set_id_acquisition_ts_cycle_indexes,
                **additional_fields
            )

        logger.info("catalogue-ing FINISHED")

        succeeded = []
        failed = []
        logger.info("performing index refresh")
        es_conn.refresh()
        logger.info("performed index refresh")

        logger.info("evaluating available burst sets")
        logger.info(f"{affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")
        if args.native_id:  # limit query to the 1 or 2 affected sets in backlog
            logger.info("Supplied native-id. Limiting evaluation")
            evaluator_results = evaluator.main(
                mgrs_set_id_acquisition_ts_cycle_indexes=affected_mgrs_set_id_acquisition_ts_cycle_indexes,
                coverage_target=settings["DSWX_S1_COVERAGE_TARGET"]
            )
        else:  # evaluate ALL sets in backlog
            logger.info("Performing full evaluation")
            evaluator_results = evaluator.main(
                coverage_target=settings["DSWX_S1_COVERAGE_TARGET"]
            )

        processable_mgrs_set_ids = {
            mgrs_set_id
            for mgrs_set_id, evaluation_result in evaluator_results["mgrs_sets"].items()
            if evaluation_result["coverage"] != -1
        }

        # convert to "batch_id" mapping
        batch_id_to_products_map = defaultdict(partial(defaultdict, list))
        for mgrs_set_id, evaluation_result in evaluator_results["mgrs_sets"].items():
            for product_burstset in evaluation_result["product_sets"]:
                for rtc_granule_id_to_product_docs_map in product_burstset:
                    for product_doc_list in rtc_granule_id_to_product_docs_map.values():
                        for product_doc in product_doc_list:
                            # doc needs to be part of a processable mgrs_set_id
                            if product_doc["mgrs_set_id"] in processable_mgrs_set_ids:
                                _, mgrs_set_id_aquisition_ts_cycle_index = product_doc["id"].split("$", 1)
                                batch_id = mgrs_set_id_aquisition_ts_cycle_index
                                # doc needs to be associated with the batch. so filter the other doc that isn't part of this batch
                                if product_doc["mgrs_set_id_acquisition_ts_cycle_index"] == batch_id:
                                    batch_id_to_products_map[batch_id][product_doc["id"]].append(product_doc)
                if args.smoke_run:
                    logger.info(f"{args.smoke_run=}. Not processing more sets of burst_sets.")
                    break

        if args.subparser_name == "full":
            logger.info(
                f"{args.subparser_name=}. Skipping download job submission. Download will be performed directly.")
            if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
                args.provider = CMR_COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                args.batch_ids = affected_mgrs_set_id_acquisition_ts_cycle_indexes
            return
        if args.no_schedule_download:
            logger.info(f"{args.no_schedule_download=}. Forcefully skipping download job submission.")
            return
        if not args.chunk_size:
            logger.info(f"{args.chunk_size=}. Insufficient chunk size. Skipping download job submission.")
            return

        results = []
        for batch_id, products_map in batch_id_to_products_map.items():
            job_submission_tasks = submit_rtc_download_job_submissions_tasks({batch_id: products_map}, args)
            results_batch = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
            results.extend(results_batch)

            suceeded_batch = [job_id for _, job_id in results_batch if isinstance(job_id, str)]
            failed_batch = [e for _, e in results_batch if isinstance(e, Exception)]
            if suceeded_batch:
                for product_id, products in batch_id_to_products_map[batch_id].items():
                    for product in products:
                        # use doc obj to pass params to elasticsearch client
                        product["download_job_ids"] = first(suceeded_batch)

                if args.dry_run:
                    logger.info(f"{args.dry_run=}. Skipping marking jobs as downloaded. Producing mock job ID")
                    pass
                else:
                    es_conn: RTCProductCatalog
                    es_conn.mark_products_as_download_job_submitted({batch_id: batch_id_to_products_map[batch_id]})

            succeeded.extend(suceeded_batch)
            failed.extend(failed_batch)

        logger.info(f"{len(results)=}")
        logger.info(f"{results=}")

        logger.info(f"{succeeded=}")
        logger.info(f"{failed=}")

        return {
            "success": succeeded,
            "fail": failed
        }


def update_affected_mgrs_set_ids(acquisition_cycle, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids):
    # construct filters for evaluation
    # ati = Acquisition Time Index
    if len(mgrs_burst_set_ids) == 1:
        current_ati = "{}${}".format(first(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati)
    elif len(mgrs_burst_set_ids) == 2:
        current_ati_a = "{}${}".format(first(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        current_ati_b = "{}${}".format(last(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_a)
        affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_b)


def update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(acquisition_cycle, additional_fields, mgrs_burst_set_ids):
    # construct filters for evaluation
    # ati = Acquisition Time Index
    if len(mgrs_burst_set_ids) == 1:
        current_ati = "{}${}".format(first(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        mgrs_set_id_acquisition_ts_cycle_indexes = [current_ati]
    elif len(mgrs_burst_set_ids) == 2:
        current_ati_a = "{}${}".format(first(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        current_ati_b = "{}${}".format(last(sorted(mgrs_burst_set_ids)), acquisition_cycle)
        mgrs_set_id_acquisition_ts_cycle_indexes = [current_ati_a, current_ati_b]

    return mgrs_set_id_acquisition_ts_cycle_indexes


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
    now_minus_minutes_dt = (now - timedelta(minutes=args.minutes)) if not args.native_id else dateutil.parser.isoparse("1900-01-01T00:00:00Z")

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


def update_granule_index(es_spatial_conn, granule, *args, **kwargs):
    es_spatial_conn.process_granule(granule, *args, **kwargs)


def does_granule_intersect_regions(granule, intersect_regions):
    regions = intersect_regions.split(',')
    for region in regions:
        region = region.strip()
        if does_bbox_intersect_region(granule["bounding_box"], region):
            return True, region

    return False, None


def filter_granules_rtc(granules, args):
    filtered_granules = []
    for granule in granules:
        granule_id = granule.get("granule_id")

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            match_product_id = re.match(rtc_granule_regex, granule_id)
            burst_id = match_product_id.group("burst_id")

            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            mgrs_sets = mbc_client.burst_id_to_mgrs_set_ids(mgrs,
                                                            mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
            if not mgrs_sets:
                logger.debug(f"{burst_id=} not associated with land or land/water data. skipping.")
                continue

        filtered_granules.append(granule)
    return filtered_granules
