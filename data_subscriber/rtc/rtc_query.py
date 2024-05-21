import asyncio
import itertools
from itertools import chain
import logging
import re
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import dateutil.parser
from more_itertools import first, last

from data_subscriber.cmr import async_query_cmr, COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.geojson_utils import localize_include_exclude, filter_granules_by_regions
from data_subscriber.query import CmrQuery
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client, evaluator
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from data_subscriber.url import determine_acquisition_cycle
from geo.geo_util import does_bbox_intersect_region
from rtc_utils import rtc_granule_regex

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

#  required constants
MISSION_EPOCH_S1A = dateutil.parser.isoparse("20190101T000000Z")  # set approximate mission start date
MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=12).total_seconds()


class RtcCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

    async def run_query(self, args, token, es_conn: RTCProductCatalog, cmr, job_id, settings):

        query_dt = datetime.now()
        now = datetime.utcnow()
        query_timerange: DateTimeRange = get_query_timerange(args, now)

        logger.info("CMR query STARTED")
        granules = await async_query_cmr(args, token, cmr, settings, query_timerange, now)
        logger.info("CMR query FINISHED")

        # If processing mode is historical, apply include/exclude-region filtering
        if args.proc_mode == "historical":
            logger.info(f"Processing mode is historical so applying include and exclude regions...")

            # Fetch all necessary geojson files from S3
            localize_include_exclude(args)
            granules[:] = filter_granules_by_regions(granules, args.include_regions, args.exclude_regions)

        logger.info("catalogue-ing STARTED")

        affected_mgrs_set_id_acquisition_ts_cycle_indexes = set()
        granules[:] = filter_granules_rtc(granules, args)
        logger.info(f"Filtered to {len(granules)} granules")

        mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
        if args.native_id:
            match_native_id = re.match(rtc_granule_regex, args.native_id)
            burst_id = mbc_client.product_burst_id_to_mapping_burst_id(match_native_id.group("burst_id"))

            native_id_mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))

        num_granules = len(granules)
        for i, granule in enumerate(granules):
            logger.debug(f"Processing granule {i+1} of {num_granules}")

            granule_id = granule.get("granule_id")
            revision_id = granule.get("revision_id")

            additional_fields = {}
            additional_fields["revision_id"] = revision_id
            additional_fields["processing_mode"] = args.proc_mode

            additional_fields["instrument"] = "S1A" if "S1A" in granule_id else "S1B"

            match_product_id = re.match(rtc_granule_regex, granule_id)
            acquisition_dts = match_product_id.group("acquisition_ts") #e.g. 20210705T183117Z
            burst_id = match_product_id.group("burst_id") # e.g. T074-157286-IW3

            # Returns up to two mgrs_set_ids. e.g. MS_74_76
            mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
            additional_fields["mgrs_set_ids"] = mgrs_burst_set_ids

            acquisition_cycle = determine_acquisition_cycle(burst_id, acquisition_dts, granule_id)
            additional_fields["acquisition_cycle"] = acquisition_cycle

            mgrs_set_id_acquisition_ts_cycle_indexes = update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(
                acquisition_cycle, additional_fields, mgrs_burst_set_ids)
            if args.native_id:  # native-id supplied. don't affect adjacent burst sets, tossing out irrelevant burst sets
                matching_native_id_mgrs_burst_set_ids = list(set(native_id_mgrs_burst_set_ids) & set(mgrs_burst_set_ids))
                update_affected_mgrs_set_ids(acquisition_cycle, affected_mgrs_set_id_acquisition_ts_cycle_indexes, matching_native_id_mgrs_burst_set_ids)
            else:
                update_affected_mgrs_set_ids(acquisition_cycle, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids)

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
            min_num_bursts = args.coverage_target_num
            if not min_num_bursts:
                min_num_bursts = settings["DSWX_S1_MINIMUM_NUMBER_OF_BURSTS_REQUIRED"]
            coverage_target = args.coverage_target
            if coverage_target is None:
                coverage_target = settings["DSWX_S1_COVERAGE_TARGET"]
            grace_mins = args.grace_mins
            if grace_mins is None:
                grace_mins = settings["DSWX_S1_COLLECTION_GRACE_PERIOD_MINUTES"]
            evaluator_results = evaluator.main(
                coverage_target=coverage_target,
                required_min_age_minutes_for_partial_burstsets=grace_mins,
                mgrs_set_id_acquisition_ts_cycle_indexes=affected_mgrs_set_id_acquisition_ts_cycle_indexes,
                min_num_bursts=min_num_bursts
            )
        else:  # evaluate ALL sets in backlog
            logger.info("Performing full evaluation")
            min_num_bursts = args.coverage_target_num
            if not min_num_bursts:
                min_num_bursts = settings["DSWX_S1_MINIMUM_NUMBER_OF_BURSTS_REQUIRED"]
            coverage_target = args.coverage_target
            if coverage_target is None:
                coverage_target = settings["DSWX_S1_COVERAGE_TARGET"]
            evaluator_results = evaluator.main(coverage_target=coverage_target, min_num_bursts=min_num_bursts)

        processable_mgrs_set_ids = {
            mgrs_set_id
            for mgrs_set_id, product_sets_and_coverage_dicts in evaluator_results["mgrs_sets"].items()
            for product_sets_and_coverage_dict in product_sets_and_coverage_dicts
            if product_sets_and_coverage_dict["coverage_group"] != -1
        }

        # convert to "batch_id" mapping
        batch_id_to_products_map = defaultdict(partial(defaultdict, list))
        for product_set_and_coverage_dict in itertools.chain.from_iterable(evaluator_results["mgrs_sets"].values()):
            for rtc_granule_id_to_product_docs_map in product_set_and_coverage_dict["product_set"]:
                for product_doc in chain.from_iterable(rtc_granule_id_to_product_docs_map.values()):
                    # doc needs to be part of a processable mgrs_set_id
                    if product_doc["mgrs_set_id"] in processable_mgrs_set_ids:
                        _, mgrs_set_id_aquisition_ts_cycle_index = product_doc["id"].split("$", 1)
                        batch_id = mgrs_set_id_aquisition_ts_cycle_index
                        # doc needs to be associated with the batch. so filter the other doc that isn't part of this batch
                        if product_doc["mgrs_set_id_acquisition_ts_cycle_index"] == batch_id:
                            batch_id_to_products_map[batch_id][product_doc["id"]].append(product_doc)
        if args.smoke_run:
            logger.info(f"{args.smoke_run=}. Filtering to single batch")
            batch_id_to_products_map = dict(sorted(batch_id_to_products_map.items())[:1])
        logger.info(f"num_batches={len(batch_id_to_products_map)}")

        if args.subparser_name == "full":
            logger.info(f"{args.subparser_name=}. Skipping download job submission. Download will be performed directly.")
            args.provider = COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
            args.batch_ids = affected_mgrs_set_id_acquisition_ts_cycle_indexes
            return
        if args.no_schedule_download:
            logger.info(f"{args.no_schedule_download=}. Forcefully skipping download job submission.")
            return
        if not args.chunk_size:
            logger.info(f"{args.chunk_size=}. Insufficient chunk size. Skipping download job submission.")
            return

        results = []
        logger.info(f"Submitting batches for RTC download job: {list(batch_id_to_products_map)}")
        for batch_id, products_map in batch_id_to_products_map.items():
            job_submission_tasks = submit_rtc_download_job_submissions_tasks({batch_id: products_map}, args, settings)
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
                    es_conn.mark_products_as_download_job_submitted({batch_id: batch_id_to_products_map[batch_id]})

            succeeded.extend(suceeded_batch)
            failed.extend(failed_batch)

        logger.info(f"{len(results)=}")
        logger.info(f"{results=}")

        logger.info(f"{len(succeeded)=}")
        logger.info(f"{succeeded=}")
        logger.info(f"{len(failed)=}")
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


def get_query_timerange(args, now: datetime, silent=False):
    now_minus_minutes_dt = (now - timedelta(minutes=args.minutes)) if not args.native_id else dateutil.parser.isoparse("1900-01-01T00:00:00Z")

    start_date = args.start_date if args.start_date else now_minus_minutes_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = args.end_date if args.end_date else now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_timerange = DateTimeRange(start_date, end_date)
    if not silent:
        logger.info(f"{query_timerange=}")
    return query_timerange


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
    logger.info("Applying land/water filter on CMR granules")

    filtered_granules = []
    for granule in granules:
        granule_id = granule.get("granule_id")

        match_product_id = re.match(rtc_granule_regex, granule_id)
        burst_id = match_product_id.group("burst_id")

        mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
        mgrs_sets = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
        if not mgrs_sets:
            logger.debug(f"{burst_id=} not associated with land or land/water data. skipping.")
            continue

        filtered_granules.append(granule)
    return filtered_granules
