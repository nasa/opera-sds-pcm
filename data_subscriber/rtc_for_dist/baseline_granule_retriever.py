import asyncio
import functools
import logging
import operator
import re
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta

from dateutil import parser

from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr, Collection, Provider, PGEProduct
from data_subscriber.dist_s1_utils import (extend_rtc_for_dist_records, build_rtc_native_ids, rtc_granules_by_acq_index,
                                           basic_decorate_granule)
from data_subscriber.query import DateTimeRange
from rtc_utils import rtc_granule_regex, dedupe_rtc

logger = logging.getLogger(__name__)

EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"
MAX_CMR_RTC_CACHE_GAP_DAYS = 3


def unique_latest_granules(granules):
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

        prod_dt = granule["production_ts"]
        if type(granule["production_ts"]) == str:
            prod_dt = parser.isoparse(prod_dt)

        key = (burst_id, acq_minute)

        if key not in granules_dict:
            granules_dict[key] = granule
        else:
            logger.info(
                f"Found duplicate burst_id {key}: "
                f"{granule['granule_id']} vs {granules_dict[key]['granule_id']}. "
                "Keeping latest production one."
            )

            existing_prod_dt = granules_dict[key]["production_ts"]
            if type(existing_prod_dt) == str:
                existing_prod_dt = parser.isoparse(granules_dict[key]["production_ts"])


            if prod_dt > existing_prod_dt:
                granules_dict[key] = granule

    return list(granules_dict.values())

@dataclass
class Args:
    use_temporal: bool = True

    # required by CMR client
    bbox: str = "-180,-90,180,90"
    collection: str = Collection.RTC_S1_V1
    provider: str = Provider.ASF
    product: str = PGEProduct.DIST_1
    # native_id_patterns: str  # only used in historical, optionally
    max_revision: int = 1000


class BaselineGranuleRetriever:
    def __init__(
            self,
            logger=None,
            args: Args = Args(),
            k_offsets_counts=None,
            product_to_bursts=None,
            window_delta_days=None,
            token=None,
            cmr=None,
            settings=None,
            bursts_to_products=None,
            query_func_factory=None
    ):
        self.logger = logger
        self.args = args
        self.k_offsets_counts = k_offsets_counts
        self.product_to_bursts = product_to_bursts
        self.window_delta_days = window_delta_days
        self.token = token
        self.cmr = cmr
        self.settings = settings
        self.bursts_to_products = bursts_to_products
        self.query_func_factory = query_func_factory

    def retrieve_baseline_granules_for_affected_batches(self, batch_id_to_current_granules: dict):
        download_batch_id_to_k_granules = {}
        for batch_id, batch_granules in batch_id_to_current_granules.items():
            download_batch_id = batch_granules[0]["download_batch_id"]

            try:
                baseline_granules = self.retrieve_baseline_granules_for_affected_batch(batch_id, batch_granules, self.args, self.k_offsets_counts)
            except Exception as e:
                self.logger.exception(f"Error retrieving baseline granules for {download_batch_id}. Cannot submit this job.", exc_info=e)
                continue

            download_batch_id_to_k_granules[download_batch_id] = baseline_granules
        return download_batch_id_to_k_granules

    def retrieve_baseline_granules_for_affected_batch(self, batch_id, batch_granules, args, k_offsets_counts):
        self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(batch_granules))

        download_batch_id = batch_granules[0]["download_batch_id"]
        self.logger.debug(f"download_batch_id={download_batch_id}")

        product_id = "_".join(batch_id.split("_")[0:2])

        baseline_granules = self.retrieve_baseline_granules(product_id, batch_granules, args, k_offsets_counts, verbose=False)
        return baseline_granules

    def retrieve_baseline_granules(self, product_id, downloads, args, k_offsets_and_counts, verbose=True):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the
        current product.
        k_offsets_and_counts is a list of tuples of (offset, count) where offset is the number of days to go back
        and count is the number of granules for that tuple set'''

        if len(downloads) == 0:
            return []

        # All download granules have been acquired within a few minutes of each other in acquisition time so we just pick one
        modified_cmr_query_args = deepcopy(args)
        modified_cmr_query_args.use_temporal = True
        _, modified_cmr_query_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts)
        self.logger.debug(f"{modified_cmr_query_args=}")

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

                # Step 1 of 3: This will return dict of acquisition_cycle -> set of granules for only ones that match the burst pattern
                if self.query_func_factory is None:
                    self.logger.error('Cannot dynamically determine granule query method. Falling back to CMR')
                    granules = asyncio.run(async_query_cmr(modified_cmr_query_args, self.token, self.cmr, self.settings,
                                                           DateTimeRange(start_date, end_date), verbose=verbose))
                else:
                    query_func = self.query_func_factory(use_async=True, secondary=True, args=modified_cmr_query_args)
                    granules = query_func(DateTimeRange(start_date, end_date), None)
                self.logger.info(f"Query results: {len(granules)=}")
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

    def extend_additional_records(self, granules, no_duplicate=False, force_product_id=None):
        extend_rtc_for_dist_records(self.bursts_to_products, granules, no_duplicate, force_product_id)


    @staticmethod
    def unique_latest_granules(granules):
        ''' Remove duplicate granules defined by having the same burst_id and acquisition_ts, keep just the latest one

        On rare occassion, duplicate granules may share acquisition ts within 1 second of each other.
        '''
        return unique_latest_granules(granules)