import asyncio
import json
import re
import sys
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from os.path import basename
from typing import Union

from dateutil import parser
from more_itertools import one, first

from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import (localize_dist_burst_db, process_dist_burst_db, compute_dist_s1_triggering,
                                           extend_rtc_for_dist_records, build_rtc_native_ids, basic_decorate_granule,
                                           rtc_granule_dict_add, get_unique_rtc_id_for_dist,
                                           parse_k_parameter)
from data_subscriber.es_conn_util import get_document_timestamp_min_max
from data_subscriber.query import BaseQuery, DateTimeRange
from data_subscriber.rtc import mgrs_bursts_collection_db_client
from data_subscriber.rtc_for_dist.baseline_granule_retriever import BaselineGranuleRetriever
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency, CMR_RTC_CACHE_INDEX
from data_subscriber.rtc_for_dist.rtc_batch_evaluator import RtcBatchEvaluator
from dist_s1 import forward_state_config_dao
from dist_s1.dataset_util import create_dataset, create_ds_dataset_json, write_ds_dataset_json, write_ds_met_json
from dist_s1.state_config_service import state_configs_by_batch_id
from rtc_utils import rtc_product_file_regex
from tools.populate_cmr_rtc_cache import populate_cmr_rtc_cache, parse_rtc_granule_metadata

EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"
MAX_CMR_RTC_CACHE_GAP_DAYS = 3


class RtcForDistCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.k_offsets_counts = k_offsets_counts = parse_k_parameter(args.k_offsets_counts)
        self.logger.info(f"Using k_offsets_counts {k_offsets_counts}")

        if dist_s1_burst_db_file:
            dist_products, bursts_to_products, product_to_bursts, _ = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            dist_products, bursts_to_products, product_to_bursts, _ = localize_dist_burst_db()

        self.dist_products = dist_products
        self.bursts_to_products = bursts_to_products
        self.product_to_bursts = product_to_bursts
        """Map from product ID (in the format f"{tile_id}_{agn}") to the burst IDs associated with it."""

#        self.grace_mins = args.grace_mins if args.grace_mins else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        self.grace_mins = args.grace_mins if args.grace_mins is not None else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        self.logger.info(f"grace_mins={self.grace_mins}")

        dist_dependency = DistDependency(self.logger, dist_products, bursts_to_products, product_to_bursts, settings)
        self.dist_dependency = dist_dependency

        self.batch_id_to_current_granules = {}
        '''This map is set by determine_download_granules and consumed by download_job_submission_handler
        We're taking this indirect approach instead of just passing this through to work w the current class structure'''
        self.download_batch_id_to_k_granules = {}
        self.settings = settings
        self.force_product_id = None
        self.window_delta_days = window_delta_days = args.window_delta if args.window_delta else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_WINDOW_DELTA_DAYS"]
        self.forced_product_id_to_current_granules = {}
        self.download_batch_id_to_job_submittable = {}

        self.baseline_granule_retriever = BaselineGranuleRetriever(
            logger=self.logger,
            args=args,
            k_offsets_counts=k_offsets_counts,
            product_to_bursts=product_to_bursts,
            window_delta_days=window_delta_days,
            token=token,
            cmr=cmr,
            settings=settings,
            bursts_to_products=bursts_to_products,
        )

        self.rtc_batch_evaluator = RtcBatchEvaluator(
            logger=self.logger,
            download_batch_id_to_k_granules=self.download_batch_id_to_k_granules,
            batch_id_to_current_granules=self.batch_id_to_current_granules,
            download_batch_id_to_job_submittable=self.download_batch_id_to_job_submittable,
            args=args,
            dist_dependency=dist_dependency,
            es_conn=es_conn,
            settings=settings,
        )

        # state objects / result of evaluation
        self.usable_batch_id_to_current_urls_map = {}
        self._unusable_batch_id_to_current_urls_map = {}
        self.submittable_batch_id_to_current_urls_map = {}
        self._unsubmittable_batch_id_to_current_urls_map = {}

    def validate_args(self):
        if not self.args.k_offsets_counts:
            self.logger.error("k_offsets_counts not provided in args. This should not be possible because there must be a default value. Cannot retrieve baseline granules.")

        if self.args.proc_mode == "reprocessing":
            if not self.args.product_id_time:
                raise AssertionError("--product-id-time must be provided in DIST-S1 reprocessing mode.")

    def unique_latest_granules(self, granules):
        """ Remove duplicate granules defined by having the same burst_id and acquisition_ts, keep just the latest one

        On rare occasion, duplicate granules may share acquisition ts within 1 second of each other.
        :param granules: basic-decorated granules
        """
        granules_dict = {}
        for granule in granules:
            burst_id = granule["burst_id"]
            # normalize acquisition_ts to minute precision
            acq_dt = granule["acquisition_ts"]
            acq_minute = acq_dt.replace(second=0, microsecond=0)

            if type(granule["production_ts"]) == str:
                prod_dt = parser.isoparse(granule["production_ts"])
            else:
                prod_dt = granule["production_ts"]

            key = (burst_id, acq_minute)

            if key not in granules_dict:
                granules_dict[key] = granule
            else:
                self.logger.info(
                    f"Found duplicate burst_id {key}: "
                    f"{granule['granule_id']} vs {granules_dict[key]['granule_id']}. "
                    "Keeping latest production one."
                )

                if type(granules_dict[key]["production_ts"]) == str:
                    existing_prod_dt = parser.isoparse(granules_dict[key]["production_ts"])
                else:
                    existing_prod_dt = granules_dict[key]["production_ts"]

                if prod_dt > existing_prod_dt:
                    granules_dict[key] = granule

        return list(granules_dict.values())


    def query_cmr(self, timerange, now: datetime):
        self.logger.info(f"{self.args.proc_mode=}")
        self.logger.info(f"{self.args.product_id_time=}")
        if self.args.proc_mode == "forward" or (self.args.proc_mode == "historical" and not self.args.product_id_time):

            if "tile_filter" in self.args and self.args.tile_filter:
                self.logger.info(f"{self.args.tile_filter=}")
                rtc_native_id_patterns = rtc_native_id_patterns_from_tiles(self.args.tile_filter)
                self.args.native_id_patterns = rtc_native_id_patterns  # NOTE: informal arg being added here
                granules = asyncio.run(async_query_cmr(self.args, self.token, self.cmr, self.settings, timerange, now))
            else:
                # "Normal" query for granules
                granules = super().query_cmr(timerange, now)

            ''' In forward mode, fill in any gap in the cmr_rtc_cache between the start time of this query and the last revision time found in the cache.
            1. Get the last revision time found in the cache
            2. Query CMR for all granules between the start time of this query and the last revision time found in the cache
            3. Insert them into cmr_rtc_cache
            '''

            # Get the last revision time found in the cache. Reformat time from 2025-06-30T21:19:48+00:00 to look like 2025-07-01T01:00:00Z
            try:
                last_revision_time = get_document_timestamp_min_max(self.es_conn.es_util, CMR_RTC_CACHE_INDEX, "revision_timestamp")[1]
                last_revision_time = last_revision_time[:-6] + "Z"
            except Exception as e:
                self.logger.error(f"Error getting the last revision time found in cmr_rtc_cache: {e}")
                raise AssertionError(f"Error getting the last revision time found in cmr_rtc_cache: {e}. \
You should update the cmr_rtc_cache using tools/populate_cmr_rtc_cache.py first.")

            # The time cutoff of CMR is a bit fuzzy. We'll err on the side of including more granules.
            # String comparison is fine because the times are formatted as 2025-07-01T01:00:00Z
            if timerange.start_date < last_revision_time:
                self.logger.warning(f"{last_revision_time=} is greater than the start time of this query {timerange.start_date}. \
This is unusual. Still inserting the granules into the cmr_rtc_cache.")
                granules_for_cache = granules

            # The date difference is too large, greater than 3 days. In this case we'll throw an error
            elif datetime.strptime(timerange.start_date, "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(last_revision_time, "%Y-%m-%dT%H:%M:%SZ") > timedelta(days=MAX_CMR_RTC_CACHE_GAP_DAYS):
                raise AssertionError(f"The date difference between the start time of this query {timerange.start_date} \
and the last revision time found in the cache {last_revision_time} is too large, greater than {MAX_CMR_RTC_CACHE_GAP_DAYS} days. \
You should update the cmr_rtc_cache using tools/populate_cmr_rtc_cache.py first.")

            else:
                # Query CMR for all granules between the start time of this query and the last revision time found in the cache
                delta_timerange = DateTimeRange(last_revision_time, timerange.start_date)
                self.logger.info(f"Querying CMR for all granules between {last_revision_time=} and {timerange.start_date=} to fill in the gap in the cmr_rtc_cache")

                delta_granules = super().query_cmr(delta_timerange, now)
                self.logger.info(f"Found {len(delta_granules)} granules to fill in the gap in the cmr_rtc_cache")
                granules_for_cache = granules + delta_granules

            if self.args.use_temporal is False:
                decorated_granules = []
                for granule in granules_for_cache:
                    decorated_granule = parse_rtc_granule_metadata(granule["granule_id"])
                    decorated_granules.append(decorated_granule)

                # Insert them into cmr_rtc_cache
                populate_cmr_rtc_cache(decorated_granules, self.es_conn.es_util)
            else:
                self.logger.warning(f"Not inserting granules into cmr_rtc_cache because use_temporal is True")

        elif self.args.proc_mode == "reprocessing" or (self.args.proc_mode == "historical" and self.args.product_id_time):
            granules = []

            #TODO: We can switch over to this code if we want to trigger reprocessing by RTC granule_id
            '''burst_id, acquisition_dts = parse_r2_product_file_name(self.args.native_id, "L2_RTC_S1")
            product_ids = self.bursts_to_products[burst_id]
            if len(product_ids) == 0:
                raise AssertionError(f"Cannot find burst_id {burst_id} in burst database. Cannot process this product.")
            self.logger.info(f"Reprocessing burst_id {burst_id} with product_ids {product_ids}")'''

            #TODO: We probably want something more graceful than the product_id_time looking like 31SGR_3,20231217T053132Z
            # TODO: The fact that this is a loop makes sense if we ever decide to trigger by native_id instead of product_id_time
            for pit in self.args.product_id_time:
                product_id = pit.split(",")[0]
                acquisition_dts = pit.split(",")[1]

                acquisition_time = datetime.strptime(acquisition_dts, "%Y%m%dT%H%M%SZ")
                start_time = (acquisition_time - timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
                end_time = (acquisition_time + timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
                query_timerange = DateTimeRange(start_time, end_time)

                self.force_product_id = product_id #TODO: This needs to change if we change this code back to using granule_id instead of product_id
                new_args = deepcopy(self.args)
                new_args.use_temporal = True
                count, new_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts)
                if count == 0:
                    raise AssertionError(f"No burst_ids found for {product_id=}. Cannot process this product.")
                self.logger.info(new_args)
                gs = asyncio.run(
                    async_query_cmr(new_args, self.token, self.cmr, self.settings, query_timerange))
                for g in gs:
                    g["product_id"] = product_id # force product_id because one granule can belong to multiple products
                granules.extend(gs)

        # Remove granules whose burst_id is not in the burst database
        filtered_granules = []
        for granule in granules:
            basic_decorate_granule(granule)
            burst_id = granule["burst_id"]
            if burst_id in self.bursts_to_products:
                filtered_granules.append(granule)

        # If there are multiple granules with the same burst_id and acquisition_ts, we only want to keep the latest one
        filtered_granules = self.unique_latest_granules(filtered_granules)

        return filtered_granules

    def extend_additional_records(self, granules, no_duplicate=False, force_product_id=None):
        extend_rtc_for_dist_records(self.bursts_to_products, granules, no_duplicate, force_product_id)

    def prepare_additional_fields(self, granule, args, granule_id):
        """This is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "tile_id", "product_id", "acquisition_group", "acquisition_ts", "acquisition_cycle", "unique_id", "batch_id", "download_batch_id"]:
            additional_fields[f] = granule[f]

        return additional_fields

    def determine_download_granules(self, granules):
        #if len(granules) == 0:
        #    return granules

        self.logger.debug(f"{len(granules)} granules, before extending")
        self.extend_additional_records(granules, force_product_id=self.force_product_id)
        self.logger.debug(f"{len(granules)} granules, after extending")

        # Create a dict of granule_id to granule for both the new granules and unsubmitted granules
        granules_dict = {}
        rtc_granule_dict_add(granules_dict, granules)

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        if self.args.proc_mode != "historical" or not self.args.product_id_time:
            self.refresh_index()
            unsubmitted = self.es_conn.get_unsubmitted_granules()
            self.logger.info("len(unsubmitted)=%d", len(unsubmitted))
            self.logger.info(f"Determining download granules from {len(granules) + len(unsubmitted)} granule records")
            rtc_granule_dict_add(granules_dict, unsubmitted)

        #TODO: Right now we just have black or white of complete or incomplete bursts. Later we may want to do either percentage or count threshold.
        candidate_dist_s1_input_infos, _, __, ___ = compute_dist_s1_triggering(self.product_to_bursts, granules_dict, self.grace_mins, datetime.now(), complete_bursts_only=False)

        granules_to_download = []  # treat "current" granules as the ones to "download"
        batch_id_to_current_granules = defaultdict(list)
        if "tile_filter" in self.args and self.args.tile_filter:
                self.logger.info(f"{self.args.tile_filter=}")
        for batch_id, dist_s1_input_info in candidate_dist_s1_input_infos.items():  # batch ID for current granules
            # apply tile filter
            batch_id_tile_id = batch_id.split("_")[0].removeprefix("p")
            if "tile_filter" in self.args and self.args.tile_filter and batch_id_tile_id not in self.args.tile_filter:
                self.logger.info(f"Tile ID {batch_id_tile_id} not in tile filter. Skipping.")
                continue

            for rtc_granule in dist_s1_input_info.rtc_granules:
                unique_rtc_id = get_unique_rtc_id_for_dist(rtc_granule)
                batch_id_to_current_granules[batch_id].append(granules_dict[(unique_rtc_id, batch_id)])  # current granules
                granules_to_download.append(granules_dict[(unique_rtc_id, batch_id)])
        self.batch_id_to_current_granules.update(batch_id_to_current_granules)

        self.logger.info(f"The following {len(self.batch_id_to_current_granules)} products and will be submitted for download: {self.batch_id_to_current_granules.keys()}")

        if self.args.proc_mode == "forward" and not self.args.product_id_time:
            grace_mins = self.grace_mins
            for batch_id, batch_granules in self.batch_id_to_current_granules.items():
                # batch_id is currently in format: "36TYL_0_S1A_368"
                batch_id_split = batch_id.split("_")
                tile_id, agn, satellite, aci = batch_id_split

                rtc_granule_ids = [g["granule_id"] for g in batch_granules]
                product_id = f"{tile_id}_{agn}"
                expected_burst_count = len(self.product_to_bursts[product_id]) if product_id in self.product_to_bursts else 0

                self.logger.info(f"Upserting forward state-config: {batch_id} with {len(rtc_granule_ids)} RTCs, {expected_burst_count=}")
                forward_state_config_dao.upsert_state_config(
                    batch_id=batch_id,
                    rtc_granule_ids=rtc_granule_ids,
                    expected_burst_count=expected_burst_count,
                    grace_period_minutes=grace_mins,
                    recreate_dataset_dir_on_update=False,
                    k_offsets_counts=self.args.k_offsets_counts,
                    download_batch_id=batch_id,
                )
            return granules_to_download

        if self.args.proc_mode == "historical" and not self.args.product_id_time:
        # if self.args.proc_mode == "forward" or self.args.product_id_time:
            # DRAFT STATE-CONFIG LOCALLY
            product_id_time_to_state_config_ds_met_json = {}
            product_id_time_to_batch_id = {}
            tile_to_product_id_times = defaultdict(set)
            def acq_time_from_product_id_time(p):
                _, acquisition_dts = p.split(",")
                return acquisition_dts
            for batch_id, batch_granules in self.batch_id_to_current_granules.items():
                burst_id, acquisition_dts = parse_r2_product_file_name(batch_granules[0]["granule_id"], "L2_RTC_S1")
                products = self.bursts_to_products[burst_id]
                self.logger.error(f"{len(products)=}")

                # collect all product-id-times associated with this burst/batch (local)
                batch_id_tile_id = batch_id.split("_")[0].removeprefix("p")
                product_id_times = set()
                for product in products:
                    product_tile_id = product.split("_")[0]
                    if product_tile_id != batch_id_tile_id:
                        continue
                    product_id_time = f"{product},{acquisition_dts}"
                    product_id_times.add(product_id_time)
                product_id_times = sorted(product_id_times, key=acq_time_from_product_id_time)

                # collect all product-id-times in this historical timerange (global)
                for product_id_time in product_id_times:
                    product_id_time_to_batch_id[product_id_time] = batch_id

                # group all product-id-times by tile
                for product_id_time in product_id_times:
                    tile_id = product_id_time.split(",")[0].split("_")[0]
                    tile_to_product_id_times[tile_id].add(product_id_time)

            tile_to_product_id_times = dict(tile_to_product_id_times)
            for k in tile_to_product_id_times:
                tile_to_product_id_times[k] = sorted(tile_to_product_id_times[k], key=acq_time_from_product_id_time)

            for tile_id, product_id_times in tile_to_product_id_times.items():
                # draft state-config jsons
                if len(product_id_times) == 1:
                    product_id_times_pairwise = zip(product_id_times, [None])
                else:
                    product_id_times_pairwise = zip(product_id_times, product_id_times[1:] + [None])

                first_batch_id = product_id_time_to_batch_id[first(product_id_times)]

                for product_id_time, next_product_id_time in product_id_times_pairwise:
                    batch_id = product_id_time_to_batch_id[product_id_time]
                    dataset_id = f"DIST_S1_state-config_{batch_id}"
                    product, acquisition_dts = product_id_time.split(",")
                    tile_id = product.split("_")[0]
                    product_id_time_to_state_config_ds_met_json[product_id_time] = {
                        "id": dataset_id,
                        "batch_id": batch_id,
                        "status": "queued",
                        "product_id_time": product_id_time,
                        "next_product_id_time": next_product_id_time or "NULL",
                        "product_id": product,
                        "tile_id": tile_id,
                        "acquisition_ts": acquisition_dts,
                        "is_first_in_chain": batch_id == first_batch_id,
                    }

            self.logger.info(f"{product_id_time_to_state_config_ds_met_json=}")
            self.logger.info(f"{product_id_time_to_batch_id=}")
            self.logger.info(f"{tile_to_product_id_times=}")

            # write out all state-configs to "queue" them
            for _, ds_met_json in product_id_time_to_state_config_ds_met_json.items():
                # create state-config
                dataset_id = ds_met_json["id"]
                batch_id = ds_met_json["batch_id"]

                ds_dataset_json = create_ds_dataset_json(version="1.0")
                ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
                ds_met_json_path = write_ds_met_json(ds_met_json, dataset_id)
                dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-STATE-CONFIG")

            # sort within groups chronologically to establish the chain
            tile_to_product_id_times = dict(tile_to_product_id_times)
            for t in tile_to_product_id_times:
                tile_to_product_id_times[t] = sorted(tile_to_product_id_times[t], key=acq_time_from_product_id_time)

            # gather the first batch in each chain, to allow processing to continue
            first_time_batch_id_to_current_granules = {}
            for _, pits in tile_to_product_id_times.items():
                first_batch_id = product_id_time_to_batch_id[first(pits)]
                first_time_batch_id_to_current_granules[first_batch_id] = self.batch_id_to_current_granules[first_batch_id]

            self.logger.info("HISTORICAL MODE (SUBMISSION). Only processing first-time products.")
            self.batch_id_to_current_granules = first_time_batch_id_to_current_granules

            self.logger.info("Exiting early to start historical mode processing chains.")
            sys.exit(0)

        batch_id_to_current_granules_count = {}
        self.logger.error(f"{len(self.batch_id_to_current_granules)=}")
        for k in self.batch_id_to_current_granules:
            batch_id_to_current_granules_count[k] = len(self.batch_id_to_current_granules[k])
        self.logger.info(f"{batch_id_to_current_granules_count=}")

        # TODO chrisjrd: unused. remove.
        # # batch_id looks like this: 36TYL_0_S1A_368; download_batch_id looks like this: p36TYL_0_S1A_a368
        # batch_id_to_download_batch_id_map = {}
        # download_batch_id_to_batch_id_map = {}
        # for batch_id, batch_granules in self.batch_id_to_current_granules.items():
        #     download_batch_id = batch_granules[0]["download_batch_id"]
        #     batch_id_to_download_batch_id_map[batch_id] = download_batch_id
        #     download_batch_id_to_batch_id_map[batch_id] = batch_id

        download_batch_id_to_k_granules = self.retrieve_baseline_granules_for_affected_batches(self.batch_id_to_current_granules)
        self.download_batch_id_to_k_granules.update(download_batch_id_to_k_granules)

        for download_batch_id, baseline_granules in download_batch_id_to_k_granules.items():
            if not len(baseline_granules):
                product_id = f'{download_batch_id[0].removeprefix("p")}_{download_batch_id[1]}'
                self.logger.info(f"No baseline granules found for {product_id=} {download_batch_id=}.")
                self.download_batch_id_to_job_submittable[download_batch_id] = False  # TODO chrisjrd: mark True / remove after new SAS delivery. as of 2026-02-05
            else:
                self.download_batch_id_to_job_submittable[download_batch_id] = True

        if self.args.proc_mode == "historical" and not download_batch_id_to_k_granules.values():
            for batch_id, batch_granules in batch_id_to_current_granules.items():
                self.write_state_config_skippable(batch_id)

                self.logger.info("Exiting.")
                sys.exit(0)  # simply exit: this is expected to only run for a single batch_id

        return granules_to_download

    def retrieve_baseline_granules_for_affected_batches(self, batch_id_to_current_granules: dict):
        return self.baseline_granule_retriever.retrieve_baseline_granules_for_affected_batches(batch_id_to_current_granules)

    def download_job_submission_handler(self, total_granules, query_timerange, **kwargs):
        return self.evaluate(total_granules, query_timerange)

    def evaluate(self, total_granules, query_timerange):
        return self.rtc_batch_evaluator.evaluate(total_granules, query_timerange)

    def _restrict_batch_urls_by_common_bursts(self, batch_to_current, batch_to_baseline):
        self.logger.info(f'Restricting URLs to common bursts')

        def url_to_burst_id(url):
            match = re.match(rtc_product_file_regex, basename(url))

            if not match:
                raise ValueError(f'Could not determine burst ID from {url=}')

            burst_id = match.groupdict()['burst_id']
            return burst_id

        for batch_id in batch_to_current:
            self.logger.debug(f'{batch_id=}')

            batch_current_urls = batch_to_current[batch_id]
            batch_baseline_urls = batch_to_baseline[batch_id]

            self.logger.debug(f'{batch_current_urls=}')
            self.logger.debug(f'{batch_baseline_urls=}')

            current_burst_set = set([url_to_burst_id(url) for url in batch_current_urls])
            baseline_burst_set = set([url_to_burst_id(url) for url in batch_baseline_urls])

            self.logger.info(f'{current_burst_set=}')
            self.logger.info(f'{baseline_burst_set=}')

            if current_burst_set != baseline_burst_set:
                common_burst_set = current_burst_set & baseline_burst_set
                self.logger.warning(f'Detected a mismatch in bursts between the current and baseline RTC sets. '
                                    f'The common bursts are: {common_burst_set}')
                self.logger.info('Checking current set for extra bursts')

                extra_bursts = current_burst_set - baseline_burst_set

                if extra_bursts:
                    self.logger.warning(f'Found {len(extra_bursts)} bursts to remove '
                                        f'from the current set: {extra_bursts}')

                    batch_to_current[batch_id] = [url for url in batch_current_urls
                                                  if url_to_burst_id(url) in common_burst_set]

                    self.logger.info(f'Reduced baseline URL set for {batch_id=} from {len(batch_current_urls)} to '
                                     f'{len(batch_to_current[batch_id])}')

                self.logger.info('Checking current set for extra bursts')

                extra_bursts = baseline_burst_set - current_burst_set

                if extra_bursts:
                    self.logger.warning(f'Found {len(extra_bursts)} bursts to remove '
                                        f'from the baseline set: {extra_bursts}')

                    batch_to_baseline[batch_id] = [url for url in batch_baseline_urls
                                                   if url_to_burst_id(url) in common_burst_set]

                    self.logger.info(f'Reduced baseline URL set for {batch_id=} from {len(batch_baseline_urls)} to '
                                     f'{len(batch_to_baseline[batch_id])}')
            else:
                self.logger.info(f'Baseline and current sets for {batch_id=} contain no extra bursts')

        return batch_to_current, batch_to_baseline

    def write_state_config_skippable(self, batch_id):
        self.logger.info(f"Historical mode detected. Scheduling for publication a state-config marked as complete and skipped.")
        # NOTE: this will override any existing doc

        state_config_batch_id = batch_id.removeprefix("p").replace("_a", "_")

        existing_state_config = one(state_configs_by_batch_id(batch_id=state_config_batch_id))["_source"]["metadata"]
        ds_met_json = existing_state_config
        ds_met_json.update({
            "status": "complete",  # DEV: marking as complete to enable "skipping" this product-id-time in the chain
            "is_complete": True,
            "was_skipped": True,  # NOTE: added to distinguish from normal completion
        })

        # create state-config
        dataset_id = ds_met_json["id"]
        batch_id = ds_met_json["batch_id"]

        ds_dataset_json = create_ds_dataset_json(version="1.0")
        ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
        ds_met_json_path = write_ds_met_json(ds_met_json, dataset_id)
        dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-STATE-CONFIG")

    def populate_product_metadata(self, product_metadata, previous_tile_product_file_paths):
        # Append the S3 prefix to the previous_tile_product_file_paths
        # from:
        # "OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1_GEN-DIST-STATUS.tif"
        # to:
        # "s3://self.settings["DATASET_BUCKET"]/products/DIST_S1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1_GEN-DIST-STATUS.tif

        #s3_rs_bucket = self.settings["DATASET_BUCKET"]
        #s3_rs_prefix = "s3://" + s3_rs_bucket + "/products/DIST_S1/"
        #if previous_tile_product_file_paths:
        #    previous_tile_product_file_paths = [s3_rs_prefix + f for f in previous_tile_product_file_paths]

        self.logger.info(f"Previous tile product file paths: {previous_tile_product_file_paths}")
        product_metadata["previous_tile_product_file_paths"] = previous_tile_product_file_paths

    def _create_download_job_params(self, query_timerange, chunk_batch_ids, product_metadata, for_pending_job=False):
        params = super().create_download_job_params(query_timerange, chunk_batch_ids)
        params.append({
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": json.dumps(product_metadata) if for_pending_job else product_metadata # Pending jobs goes into ES as a string
        })
        return params

    def update_url_index(
            self,
            es_conn,
            urls: list[str],
            granule: dict,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            bulk=None,
            *args,
            **kwargs
    ):
        # We store the entire filtered_urls in the ES index from the granule dict in RTCForDistProductCatalog.form_document()
        es_conn.process_url([], granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, bulk=bulk, *args, **kwargs)


def rtc_native_id_patterns_from_tiles(tiles: Union[list[str], set[str]]) -> set:
    """Given a list of tiles, return the CMR native-id[] query param required to query by native IDs."""
    tiles = tiles if type(tiles) is set else set(tiles)
    def contains_matching_tiles(mgrs_tiles_parsed):
        return not not mgrs_tiles_parsed.intersection(tiles)
    df = mgrs_bursts_collection_db_client.cached_load_mgrs_burst_db(filter_land=True)
    df = df[df["mgrs_tiles_parsed"].apply(contains_matching_tiles)]
    return mgrs_bursts_collection_db_client.get_reduced_rtc_native_id_patterns(df)
