import asyncio
import functools
import json
import operator
import re
import sys
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from itertools import chain
from os.path import basename
from typing import Union

import dateutil
from dateutil import parser
from more_itertools import one, first

from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.cslc_utils import save_blocked_download_job, parse_r2_product_file_name
from data_subscriber.dist_s1_utils import (localize_dist_burst_db, process_dist_burst_db, compute_dist_s1_triggering,
                                           extend_rtc_for_dist_records, build_rtc_native_ids, rtc_granules_by_acq_index,
                                           basic_decorate_granule, rtc_granule_dict_add, get_unique_rtc_id_for_dist,
                                           parse_k_parameter, PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD, get_rtc_burst_prefix)
from data_subscriber.es_conn_util import get_document_timestamp_min_max
from data_subscriber.query import BaseQuery, DateTimeRange
from data_subscriber.rtc import mgrs_bursts_collection_db_client
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency, CMR_RTC_CACHE_INDEX
from dist_s1.dataset_util import create_dataset, create_ds_dataset_json, write_ds_dataset_json, write_ds_met_json
from dist_s1.state_config_service import state_configs_by_batch_id
from rtc_utils import rtc_granule_regex, dedupe_rtc, rtc_product_file_regex
from tools.populate_cmr_rtc_cache import populate_cmr_rtc_cache, parse_rtc_granule_metadata
from util.job_submitter import try_submit_mozart_job

EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"
MAX_CMR_RTC_CACHE_GAP_DAYS = 3


class RtcForDistCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, _ = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, _ = localize_dist_burst_db()

#        self.grace_mins = args.grace_mins if args.grace_mins else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        self.grace_mins = args.grace_mins if args.grace_mins is not None else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        self.logger.info(f"grace_mins={self.grace_mins}")

        self.dist_dependency = DistDependency(self.logger, self.dist_products, self.bursts_to_products, self.product_to_bursts, settings)

        self.batch_id_to_current_granules = {}
        '''This map is set by determine_download_granules and consumed by download_job_submission_handler
        We're taking this indirect approach instead of just passing this through to work w the current class structure'''
        self.download_batch_id_to_k_granules = {}
        self.settings = settings
        self.force_product_id = None
        self.window_delta_days = args.window_delta if args.window_delta else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_WINDOW_DELTA_DAYS"]
        self.forced_product_id_to_current_granules = {}
        self.download_batch_id_to_job_submittable = {}

    def validate_args(self):
        if self.args.proc_mode == "reprocessing":
            if not self.args.product_id_time:
                raise AssertionError("--product-id-time must be provided in DIST-S1 reprocessing mode.")

    def unique_latest_granules(self, granules):
        ''' Remove duplicate granules defined by having the same burst_id and acquisition_ts, keep just the latest one

        On rare occassion, duplicate granules may share acquisition ts within 1 second of each other.
        '''
        granules_dict = {}
        for granule in granules:
            burst_id = granule["burst_id"]
            # normalize acquisition_ts to minute precision
            acq_dt = granule["acquisition_ts"]
            acq_minute = acq_dt.replace(second=0, microsecond=0)

            prod_dt = parser.isoparse(granule["production_datetime"])

            key = (burst_id, acq_minute)

            if key not in granules_dict:
                granules_dict[key] = granule
            else:
                self.logger.info(
                    f"Found duplicate burst_id {key}: "
                    f"{granule['granule_id']} vs {granules_dict[key]['granule_id']}. "
                    "Keeping latest production one."
                )
                existing_prod_dt = parser.isoparse(granules_dict[key]["production_datetime"])

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

        granules_to_download = []
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
        self.batch_id_to_current_granules = batch_id_to_current_granules

        self.logger.info(f"The following {len(self.batch_id_to_current_granules)} products and will be submitted for download: {self.batch_id_to_current_granules.keys()}")

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

        # batch_id looks like this: 36TYL_0_S1A_368; download_batch_id looks like this: p36TYL_0_S1A_a368
        batch_id_to_download_batch_id_map = {}
        download_batch_id_to_batch_id_map = {}
        for batch_id, batch_granules in self.batch_id_to_current_granules.items():
            download_batch_id = batch_granules[0]["download_batch_id"]
            batch_id_to_download_batch_id_map[batch_id] = download_batch_id
            download_batch_id_to_batch_id_map[batch_id] = batch_id

        for batch_id, batch_granules in self.batch_id_to_current_granules.items():
            self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(batch_granules))

            download_batch_id = batch_granules[0]["download_batch_id"]
            self.logger.debug(f"download_batch_id={download_batch_id}")

            batch_id_to_download_batch_id_map[batch_id] = download_batch_id
            download_batch_id_to_batch_id_map[batch_id] = batch_id

            product_id = "_".join(batch_id.split("_")[0:2])

            try:
                if not self.args.k_offsets_counts:
                    self.logger.error("k_offsets_counts not provided in args. This should not be possible because there must be a default value. Cannot retrieve baseline granules.")

                k_offsets_counts = parse_k_parameter(self.args.k_offsets_counts)
                self.logger.info(f"Using k_offsets_counts {k_offsets_counts}")

                baseline_granules = self.retrieve_baseline_granules(product_id, batch_granules, self.args, k_offsets_counts, verbose=False)
            except Exception as e:
                self.logger.exception(f"Error retrieving baseline granules for {download_batch_id}. Cannot submit this job.", exc_info=e)
                continue

            if not len(baseline_granules):
                self.logger.info(f"No baseline granules found for {product_id=} {download_batch_id=}.")
                self.download_batch_id_to_job_submittable[download_batch_id] = False  # TODO chrisjrd: mark True / remove after new SAS delivery. as of 2026-02-05

                if self.args.proc_mode == "historical":
                    self.write_state_config_skippable(batch_id)

                    self.logger.info("Exiting.")
                    sys.exit(0)  # simply exit: this is expected to only run for a single batch_id
            else:
                self.download_batch_id_to_job_submittable[download_batch_id] = True

            self.download_batch_id_to_k_granules[download_batch_id] = baseline_granules

        return granules_to_download

    def retrieve_baseline_granules(self, product_id, downloads, args, k_offsets_and_counts, verbose = True):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the
        current product.
        k_offsets_and_counts is a list of tuples of (offset, count) where offset is the number of days to go back
        and count is the number of granules for that tuple set'''

        if len(downloads) == 0:
            return []

        # All download granules should be within a few minutes of each other in acquisition time so we just pick one
        modified_cmr_query_args = deepcopy(args)
        modified_cmr_query_args.use_temporal = True
        _, modified_cmr_query_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts)
        expected_burst_count = len(list(self.product_to_bursts[product_id]))
        self.logger.info(f"{product_id=}, expected_burst_count={expected_burst_count}")
        self.logger.debug(f"{list(self.product_to_bursts[product_id])=}")

        # TODO: Not sure if we'll need this or not; only need if we want to match the burst_id pattern exactly
        # Create a set of burst_ids for the current product to compare with the frames over k- cycles
        # burst_id_set = set()
        # for download in downloads:
        #     burst_id_set.add(download["burst_id"])

        k_granules = []
        k_offset_count_granules_map = {}

        acquisition_time = downloads[0]["acquisition_ts"]
        self.logger.info(f"{acquisition_time=}")
        for k_offset, k_count in k_offsets_and_counts:  # e.g. ( (365,4) , (730,3) , (1095,3) )
            k_offset_count_granules_map[(k_offset, k_count)] = []
            num_granules_satisfied = 0
            while num_granules_satisfied < k_count:
                end_date_shift = timedelta(days= k_offset, hours=1)
                end_dt = acquisition_time - end_date_shift
                end_date = end_dt.strftime(CMR_TIME_FORMAT)

                start_dt = end_dt - timedelta(days=self.window_delta_days)
                start_date = start_dt.strftime(CMR_TIME_FORMAT)

                self.logger.info(f"Retrieving K-1 granules [{start_date=} {end_date=}) using {self.window_delta_days=} for {product_id=}")

                # Sanity check: If the end date object is earlier than the earliest possible year, then error out. We've exhausted data space.
                if end_dt < datetime.strptime(EARLIEST_POSSIBLE_RTC_DATE, CMR_TIME_FORMAT):
                    self.logger.warning(f"We are searching earlier than {EARLIEST_POSSIBLE_RTC_DATE}. There is no more data here. {end_dt=}")
                    break

                self.logger.debug(f"{modified_cmr_query_args=}")

                # Step 1 of 3: This will return dict of acquisition_cycle -> set of granules for only ones that match the burst pattern
                granules = asyncio.run(async_query_cmr(modified_cmr_query_args, self.token, self.cmr, self.settings, DateTimeRange(start_date, end_date), verbose=verbose))
                self.logger.info(f"CMR results: {len(granules)=}")
                for granule in granules:
                    basic_decorate_granule(granule)
                    granule["product_id"] = product_id # force product_id because all baseline granules should have the same product_id as the current granules
                self.extend_additional_records(granules, no_duplicate=True, force_product_id=product_id)
                granules = self.unique_latest_granules(granules)

                if not granules:
                    self.logger.info("No granules to search through. Moving on from this k-offset-count.")
                    break

                # Step 2 of 3 ...Sort and pick the first k-1 frames
                granules_map = rtc_granules_by_acq_index(granules)
                self.logger.info(f"{product_id=} satisfies. {k_offset=} {k_count=} {len(granules)=}")
                acq_day_indices = sorted(granules_map.keys(), reverse=True)
                possible_k_granules = []
                for acq_day_index in acq_day_indices:
                    granules = granules_map[acq_day_index]
                    possible_k_granules.extend(granules)

                    num_granules_satisfied += 1
                    if num_granules_satisfied == k_count:
                        break

                # Step 3 of 3: Only copy over k_count per burst_id from possible_k_granules to k_granules
                burst_id_to_granules_map = defaultdict(list)
                for granule in possible_k_granules:
                    match_product_id = re.match(rtc_granule_regex, granule["granule_id"])
                    burst_id = match_product_id.group("burst_id")
                    if len(burst_id_to_granules_map[burst_id]) >= k_count:
                        continue  # skip any extra baseline granules per burst_id, capping the number to k_count, per k_offset

                    burst_id_to_granules_map[burst_id].append(granule)
                burst_id_to_granules_map = dict(burst_id_to_granules_map)

                possible_k_granules = functools.reduce(operator.add, burst_id_to_granules_map.values(), [])

                # dedupe for this lookback window
                len_pre_dedupe = len(possible_k_granules)
                possible_k_granules = dedupe_rtc(possible_k_granules)
                len_post_dedupe = len(possible_k_granules)
                if len_pre_dedupe != len_post_dedupe:
                    self.logger.info(f"Duplicates found during dedupe ({len_post_dedupe - len_pre_dedupe}). {len_pre_dedupe=}, {len_post_dedupe=}")

                k_granules.extend(possible_k_granules)
                k_offset_count_granules_map[(k_offset,k_count)].extend(possible_k_granules)

            self.logger.info(f"{k_offset=} {k_count=} {num_granules_satisfied=}")
            if num_granules_satisfied < k_count:
                self.logger.info(f"{k_offset=} {k_count=} not satisfied ({num_granules_satisfied=}).")

        k_offset_count_len_map = {}
        for k in k_offset_count_granules_map:
            _, k_count = k
            k_offset_count_len_map[k] = len(k_offset_count_granules_map[k])
            if expected_burst_count * k_count > k_offset_count_len_map[k]:
                self.logger.info(f"Incomplete baseline. {expected_burst_count * k_count=} vs {k_offset_count_len_map[k]=}")
        self.logger.info(f"{k_offset_count_len_map=}")

        k_granules = list({g["granule_id"]: g for g in k_granules}.values())  # EDGE CASE: remove duplicates
        self.logger.info(f"{len(k_granules)=}")
        return k_granules

    def download_job_submission_handler(self, total_granules, query_timerange):

        def add_filtered_urls(granule, filtered_urls: list, polarization_preference: Union[set, None] =None):
            self.logger.debug(f'add_filtered_urls:: {polarization_preference=} {granule["granule_id"]}')
            if granule.get("filtered_urls"):
                # ignore single polarizations. declared polarization
                if len(granule.get("polarization", [])) == 1:
                    return

                # ignore single polarizations. effective polarization
                polarizations = set()
                for filter_url in granule.get("filtered_urls"):
                    if filter_url.endswith("VV.tif"):
                        polarizations.add("VV")
                    if filter_url.endswith("VH.tif"):
                        polarizations.add("VH")
                    if filter_url.endswith("HH.tif"):
                        polarizations.add("HH")
                    if filter_url.endswith("HV.tif"):
                        polarizations.add("HV")
                if len(polarizations) == 1:
                    return

                polarizations = []
                for filter_url in granule.get("filtered_urls"):
                    if filter_url.endswith("VV.tif"):
                        polarizations.append(frozenset({"VV", "VH"}))
                    if filter_url.endswith("HH.tif"):
                        polarizations.append(frozenset({"HH", "HV"}))

                most_common_polarization = Counter(polarizations).most_common(1)
                self.logger.debug(f'most_common_polarization={most_common_polarization[0][0]}')

                if polarization_preference and most_common_polarization:
                    if polarization_preference != most_common_polarization[0][0]:
                        self.logger.info(f"Polarization switch detected. {polarization_preference=} {most_common_polarization[0][0]}")

                # if a preference is preferred (i.e. for CURRENT granules), filter by that
                if polarization_preference:
                    if polarization_preference == {"VV", "VH"}:
                        self.logger.debug('Filtering to pol pref VV/VH')
                        for filter_url in granule.get("filtered_urls"):
                            # NOTE: If we want to enable https downloads in the download worker, we need to change this
                            if not filter_url.startswith("s3://"):
                                continue

                            if any(filter_url.endswith(s) for s in ["VV.tif", "VH.tif"]):
                                filtered_urls.append(filter_url)
                        return frozenset({"VV", "VH"})
                    elif polarization_preference == {"HH", "HV"}:
                        self.logger.debug('Filtering to pol pref HH/HV')
                        for filter_url in granule.get("filtered_urls"):
                            # NOTE: If we want to enable https downloads in the download worker, we need to change this
                            if not filter_url.startswith("s3://"):
                                continue

                            if any(filter_url.endswith(s) for s in ["HH.tif", "HV.tif"]):
                                filtered_urls.append(filter_url)
                        return frozenset({"HH", "HV"})

                if most_common_polarization and most_common_polarization[0][0] == {"VV", "VH"}:
                    self.logger.debug('Filtering to common pol VV/VH')
                    for filter_url in granule.get("filtered_urls"):
                        # NOTE: If we want to enable https downloads in the download worker, we need to change this
                        if not filter_url.startswith("s3://"):
                            continue

                        if any(filter_url.endswith(s) for s in ["VV.tif", "VH.tif"]):
                            filtered_urls.append(filter_url)
                    return frozenset({"VV", "VH"})
                elif most_common_polarization and most_common_polarization[0][0] == {"HH", "HV"}:
                    self.logger.debug('Filtering to common pol HH/HV')
                    for filter_url in granule.get("filtered_urls"):
                        # NOTE: If we want to enable https downloads in the download worker, we need to change this
                        if not filter_url.startswith("s3://"):
                            continue

                        if any(filter_url.endswith(s) for s in ["HH.tif", "HV.tif"]):
                            filtered_urls.append(filter_url)
                    return frozenset({"HH", "HV"})
                else:
                    self.logger.error(f"Unexpected polarization {most_common_polarization=}. Falling back to regular filtering.")
                    for filter_url in granule.get("filtered_urls"):
                        # Get rid of .h and mask.tif files that aren't used
                        # NOTE: If we want to enable https downloads in the download worker, we need to change this
                        if "s3://" in filter_url and (filter_url[-6:] in ["VV.tif", "VH.tif", "HH.tif", "HV.tif"]):
                            filtered_urls.append(filter_url)

        # batch_id_to_granules = defaultdict(list)
        # for granule in total_granules:
        #     batch_id_to_granules[granule["download_batch_id"]].append(granule)

        # group current + baseline granules
        rtc_prefix_to_granules_map = defaultdict(set)
        self.logger.info("grouping current granules")
        for granule in total_granules:
            rtc_prefix_to_granules_map[get_rtc_burst_prefix(granule["granule_id"])].add(granule["granule_id"])
        self.logger.info("grouping baseline granules")
        for granule in chain.from_iterable(self.download_batch_id_to_k_granules.values()):
            rtc_prefix_to_granules_map[get_rtc_burst_prefix(granule["granule_id"])].add(granule["granule_id"])
        rtc_prefix_to_rsorted_granules_map = {}
        for k, granules in rtc_prefix_to_granules_map.items():
            rtc_prefix_to_rsorted_granules_map[k] = sorted(granules, key=get_unique_rtc_id_for_dist, reverse=True)
        self.logger.info(f"{rtc_prefix_to_rsorted_granules_map=}")

        # determine the polarization used in the (current) granules
        download_batch_id_to_current_granules = defaultdict(list)
        self.logger.debug(f"{list(self.batch_id_to_current_granules.keys())[:1]=}")
        for batch_id, current_granules in self.batch_id_to_current_granules.items():
            for g in current_granules:
                download_batch_id_to_current_granules[g["download_batch_id"]].append(g)
        batch_id_to_polarizations = RtcForDistCmrQuery.create_batch_id_to_polarizations_map(download_batch_id_to_current_granules)
        self.logger.info(f"{batch_id_to_polarizations=}")

        product_id_to_polarization_map = {}
        """The product ID of "the current granules". This is shared in common with the baseline granules."""
        for batch_id, current_granules in self.batch_id_to_current_granules.items():
            if current_granules:
                g = current_granules[0]
                product_id = g["product_id"]
                product_id_to_polarization_map[product_id] = RtcForDistCmrQuery.polarizations_for_granules(current_granules)
        self.logger.info(f"{product_id_to_polarization_map=}")

        batch_id_to_current_urls_map = defaultdict(list)
        self.logger.info(f"{len(total_granules)=}")

        current_set_polarizations = [
            frozenset({"VV", "VH"}) if filter_url.endswith('VV.tif') else frozenset({"HH", "HV"})
            for granule in total_granules for filter_url in granule.get("filtered_urls")
        ]

        burst_to_pol = {}

        for granule in total_granules:
            # prefer to filter granules based on this "base" polarization
            # pol_pref = RtcForDistCmrQuery.supply_cbs_polarizations(batch_id_to_polarizations, granule["download_batch_id"])
            g_polarizations = batch_id_to_polarizations.get(granule["download_batch_id"])  # e.g. { {"VV", "VH"} }
            if not g_polarizations:
                self.logger.warning(f'No polarization detected for {granule["download_batch_id"]}. Skipping.')
                continue
            elif len(g_polarizations) == 1:
                pol_pref = one(g_polarizations)
                if len(one(g_polarizations)) == 1:
                    self.logger.info(f'Single polarization {set(pol_pref)} detected in current granules for {granule["download_batch_id"]}. A download job will not be submitted.')
            elif len(g_polarizations) > 1:
                pol_pref = g_polarizations
                self.logger.info(f'Multiple polarizations {set(pol_pref)} detected in current granules for {granule["download_batch_id"]}. A download job will not be submitted.')
            else:
                continue
            burst_id = granule['granule_id'].split('_')[3]
            # burst_to_pol[burst_id] = pol_pref
            burst_to_pol[burst_id] = add_filtered_urls(granule, batch_id_to_current_urls_map[granule["download_batch_id"]], polarization_preference=pol_pref)

        batch_id_to_baseline_urls = defaultdict(list)
        for download_batch_id, granules in self.download_batch_id_to_k_granules.items():
            self.logger.info(f"Processing baseline granules. {download_batch_id=} {len(granules)=}")
            if not granules:
                self.logger.info(f"No granules to filter baseline URLs from. {download_batch_id=}. Skipping.")
            for granule in granules:
                # prefer to filter granules based on this "base" polarization
                #self.logger.info(download_batch_id)
                #self.logger.info(granule["download_batch_id"])
                pol_pref = first(product_id_to_polarization_map.get(granule["product_id"]))
                burst_id = granule['granule_id'].split('_')[3]
                pol_pref = burst_to_pol.get(burst_id, pol_pref)
                #print(download_batch_id, granule["download_batch_id"])
                add_filtered_urls(granule, batch_id_to_baseline_urls[download_batch_id], polarization_preference=pol_pref)
        #print(batch_id_to_baseline_urls)

        #self.logger.debug(f"{batch_id_to_urls_map=}")

        batch_id_to_current_urls_map, batch_id_to_baseline_urls = self._restrict_batch_urls_by_common_bursts(
            batch_id_to_current_urls_map,
            batch_id_to_baseline_urls
        )

        job_submission_tasks = []
        product_metadata = {}
        for batch_id, current_urls in batch_id_to_current_urls_map.items():
            chunk_batch_ids = [batch_id]
            self.logger.info(f"Submitting download job for {batch_id=}")
            self.logger.debug(f"{current_urls=}")

            if not self.download_batch_id_to_job_submittable.get(batch_id):
                self.logger.info(f"{batch_id=} is marked as not submittable (baseline bursts missing). Skipping job submission.")
                continue

            # If the length of urls is 0, we can't submit this. Skip.
            if len(current_urls) == 0:
                self.logger.error(f"No urls found for {batch_id}. Cannot submit download job.")
                if self.args.proc_mode == "historical":
                    self.write_state_config_skippable(batch_id)
                continue
            product_metadata["current_s3_paths"] = sorted(current_urls)

            if batch_id not in batch_id_to_baseline_urls:
                self.logger.warning(f"Cannot find baseline URLs for {batch_id}. Cannot submit download job.")
                continue
            product_metadata["baseline_s3_paths"] = sorted(batch_id_to_baseline_urls[batch_id])


            # If the previous run for this tile has not been processed, submit as a pending job
            # previous_tile_product_file_paths can be None or a list of file paths

            # From  "https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0_VH.tif" ...
            # To: OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0
            one_rtc_granule = current_urls[0].split("/")[-1][:-7]
            burst_id, acquisition_dts = parse_r2_product_file_name(one_rtc_granule, "L2_RTC_S1")
            acquisition_ts = dateutil.parser.isoparse(acquisition_dts[:-1])

            should_wait, previous_tile_product_file_paths, previous_tile_job_id = self.dist_dependency.should_wait_previous_run(batch_id, acquisition_ts)

            self.populate_product_metadata(product_metadata, previous_tile_product_file_paths)

            add_attributes = {"previous_tile_job_id": previous_tile_job_id, "download_batch_id": batch_id, "acquisition_ts": acquisition_ts}

            product_type = "rtc_for_dist"
            job_name = f"job-WF-{product_type}_download-{chunk_batch_ids[0]}"
            if should_wait:
                self.logger.info(
                    f"We will wait for the previous run for the job {previous_tile_job_id} to complete before submitting the download job.")
                params = self._create_download_job_params(query_timerange, chunk_batch_ids, product_metadata, for_pending_job=True)
                save_blocked_download_job(self.es_conn.es_util, PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD, self.settings["RELEASE_VERSION"],
                                                           product_type, params, self.args.job_queue, job_name, add_attributes)
                continue

            params = self._create_download_job_params(query_timerange, chunk_batch_ids, product_metadata)
            download_job_id = try_submit_mozart_job(product = {},
                                                    params=params,
                                                    job_queue=self.args.job_queue,
                                                    rule_name=f"trigger-{product_type}_download",
                                                    job_spec=f"job-{product_type}_download:{self.settings['RELEASE_VERSION']}",
                                                    job_type=f"{product_type}_download",
                                                    job_name=job_name)

            # Record download job id in ES
            self.es_conn.mark_download_job_id(batch_id, download_job_id)

            job_submission_tasks.append(download_job_id)

        return job_submission_tasks

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


    @staticmethod
    def create_batch_id_to_polarizations_map(batch_to_granules_map):
        return {
            batch_id: RtcForDistCmrQuery.polarizations_for_granules(granules)
            for batch_id, granules in batch_to_granules_map.items()
        }

    @staticmethod
    def supply_cbs_polarizations(batch_id_to_polarizations, batch_id):
        """Determine polarization for the current burst set (CBS). None if indeterminate."""
        batch_polarization = batch_id_to_polarizations.get(batch_id, None)
        if batch_polarization is None:
            polarization_preference = None
        else:
            if len(batch_polarization) == 1:
                polarization_preference = next(iter(batch_polarization))
            else:  # multiple polarizations / indeterminate
                polarization_preference = None
        return polarization_preference

    @staticmethod
    def polarizations_for_granules(granules) -> set[frozenset]:
        """
        Given a list of granules, return a (unique) set of all polarizations detected.
        In most cases, this will look like singleton set like { {"VV", "VH"} }
        But it is theoretically possible to have a longer set like { {"VV", "VH"}, {"HH, "HV"}, {"VV"}, {"HH"}, ... }
        Note: granules may have single polarizations, or multiple.
        """
        return {frozenset(g["polarization"]) for g in granules if g.get("polarization")}

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
            *args,
            **kwargs
    ):
        # We store the entire filtered_urls in the ES index from the granule dict in RTCForDistProductCatalog.form_document()
        es_conn.process_url([], granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args, **kwargs)


def rtc_native_id_patterns_from_tiles(tiles: Union[list[str], set[str]]) -> set:
    """Given a list of tiles, return the CMR native-id[] query param required to query by native IDs."""
    tiles = tiles if type(tiles) is set else set(tiles)
    def contains_matching_tiles(mgrs_tiles_parsed):
        return not not mgrs_tiles_parsed.intersection(tiles)
    df = mgrs_bursts_collection_db_client.cached_load_mgrs_burst_db(filter_land=True)
    df = df[df["mgrs_tiles_parsed"].apply(contains_matching_tiles)]
    return mgrs_bursts_collection_db_client.get_reduced_rtc_native_id_patterns(df)
