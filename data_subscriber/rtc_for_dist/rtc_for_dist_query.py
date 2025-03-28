from collections import defaultdict
from datetime import datetime, timedelta
from copy import deepcopy
import asyncio

from util.job_submitter import try_submit_mozart_job

from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.url import determine_acquisition_cycle, rtc_for_dist_unique_id
from data_subscriber.query import CmrQuery, get_query_timerange, DateTimeRange
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import (localize_dist_burst_db, process_dist_burst_db, compute_dist_s1_triggering,
                                           dist_s1_download_batch_id, build_rtc_native_ids, rtc_granules_by_acq_index)

DIST_K_MULT_FACTOR = 2 # TODO: This should be a setting in probably settings.yaml.
K_GRANULES = 2 # Should be either parameter into query job or settings.yaml
EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = localize_dist_burst_db()

        #TODO: Grace minutes? Read from settings.yaml

        #TODO: Set up es_conn and data structures for Baseline Set granules

        '''This data structure is set by determine_download_granules and consumed by download_job_submission_handler
        We're taking this indirect approach instead of just passing this through to work w the current class structure'''
        self.batch_id_to_k_granules = {}

    def validate_args(self):
        pass

    def query_cmr(self, timerange, now: datetime):
        granules = super().query_cmr(timerange, now)

        # Remove granules whose burst_id is not in the burst database
        filtered_granules = []
        for granule in granules:
            burst_id, acquisition_dts = parse_r2_product_file_name(granule["granule_id"], "L2_RTC_S1")
            if burst_id in self.bursts_to_products:
                granule["burst_id"] = burst_id
                granule["acquisition_ts"] = acquisition_dts
                granule["acquisition_cycle"] = determine_acquisition_cycle(granule["burst_id"],
                                                                           granule["acquisition_ts"], granule["granule_id"])
                filtered_granules.append(granule)

        # If there are multiple granules with the same burst_id and acquisition_ts, we only want to keep the latest one
        granules_dict = {}
        for granule in filtered_granules:
            key = (granule["burst_id"], granule["acquisition_ts"])
            if key not in granules_dict:
                granules_dict[key] = granule
            else:
                if granule["acquisition_ts"] > granules_dict[key]["acquisition_ts"]:
                    granules_dict[key] = granule
        filtered_granules = list(granules_dict.values())

        self.extend_additional_records(filtered_granules)
        return filtered_granules

    def extend_additional_records(self, granules):

        extended_granules = []

        def decorate_granule(granule):
            granule["tile_id"] = granule["product_id"].split("_")[0]
            granule["acquisition_group"] = granule["product_id"].split("_")[1]
            granule["download_batch_id"] = dist_s1_download_batch_id(granule)
            granule["unique_id"] = rtc_for_dist_unique_id(granule["download_batch_id"], granule["burst_id"])

        for granule in granules:
            rtc_granule_id = granule["granule_id"]
            product_ids = list(self.bursts_to_products[granule["burst_id"]])

            if len(product_ids) == 0:
                self.logger.error(f"This shouldn't happen. Skipping {rtc_granule_id} as it does not belong to any DIST-S1 product.")
                continue

            granule["product_id"] = product_ids[0]
            decorate_granule(granule)

            if len(product_ids) > 1:
                for product_id in product_ids[1:]:
                    new_granule = deepcopy(granule)
                    new_granule["product_id"] = product_id
                    decorate_granule(new_granule)
                    extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):
        """This is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "tile_id", "product_id", "acquisition_group", "acquisition_ts", "acquisition_cycle", "unique_id", "download_batch_id"]:
            additional_fields[f] = granule[f]

        return additional_fields

    def determine_download_granules(self, granules):
        if len(granules) == 0:
            return granules

        download_granules = []

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        self.refresh_index()
        # TODO: time format is bit diff from CSLC. This one has Z at the end.
        #unsubmitted = self.es_conn.get_unsubmitted_granules()

        self.logger.debug("len(granules)=%d", len(granules))
        #self.logger.debug("len(unsubmitted)=%d", len(unsubmitted))

        #TODO: After merging new granules with unsubmitted granules, make sure to remove any duplicates and pick the latest

        # Create a dict of granule_id to granule
        granules_dict = {granule["granule_id"]: granule for granule in granules}
        granule_ids = list(granules_dict.keys())
        products_triggered, _, _ = compute_dist_s1_triggering(
            self.bursts_to_products, self.product_to_bursts, granule_ids)

        by_download_batch_id = defaultdict(lambda: defaultdict(dict))

        for product_id, product in products_triggered.items():
            for rtc_granule in product.rtc_granules:
                by_download_batch_id[product_id][rtc_granule] = granules_dict[rtc_granule]
                download_granules.append(granules_dict[rtc_granule])

        self.logger.info("Received the following RTC granules from CMR: ")
        for batch_id, download_batch in by_download_batch_id.items():
            #if batch_id == "32UPD_4_302":
            #    for k in download_batch.keys():
            #        print(k)
            self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(download_batch))
            self.batch_id_to_k_granules[batch_id] = self.retrieve_baseline_granules(list(download_batch.values()), self.args, K_GRANULES - 1, verbose=True)

        return download_granules

    def retrieve_baseline_granules(self, downloads, args, k_minus_one, verbose = True):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the
        current product. Return all the granules that satisfy that'''
        k_granules = []
        k_satified = 0

        if len(downloads) == 0:
            return k_granules

        '''All download granules should have the same product id
        All download granules should be within a few minutes of each other in acquisition time so we just pick one'''
        product_id = downloads[0]["product_id"]
        acquisition_time = downloads[0]["acquisition_ts"]
        acquisition_time = datetime.strptime(acquisition_time, "%Y%m%dT%H%M%SZ")
        new_args = deepcopy(args)
        new_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts)

        # TODO: Not sure if we'll need this or not
        # Create a set of burst_ids for the current frame to compare with the frames over k- cycles
        burst_id_set = set()
        for download in downloads:
            burst_id_set.add(download["burst_id"])

        # Move start and end date of new_args back and expand 5 days at both ends to capture all k granules
        shift_day_grouping = 12 * (k_minus_one * DIST_K_MULT_FACTOR) # Number of days by which to shift each iteration

        counter = 1
        while k_satified < k_minus_one:
            start_date_shift = timedelta(days= counter * shift_day_grouping, hours=1)
            end_date_shift = timedelta(days= (counter-1) * shift_day_grouping, hours=1)
            start_date = (acquisition_time - start_date_shift).strftime(CMR_TIME_FORMAT)
            end_date_object = (acquisition_time - end_date_shift)
            end_date = end_date_object.strftime(CMR_TIME_FORMAT)
            query_timerange = DateTimeRange(start_date, end_date)

            # Sanity check: If the end date object is earlier than the earliest possible year, then error out. We've exhaust data space.
            if end_date_object < datetime.strptime(EARLIEST_POSSIBLE_RTC_DATE, CMR_TIME_FORMAT):
                raise AssertionError(f"We are searching earlier than {EARLIEST_POSSIBLE_RTC_DATE}. There is no more data here. {end_date_object=}")

            self.logger.info(f"Retrieving K-1 granules {start_date=} {end_date=} for {product_id=}")
            self.logger.info(new_args)

            # Step 1 of 2: This will return dict of acquisition_cycle -> set of granules for only onse that match the burst pattern
            granules = asyncio.run(async_query_cmr(new_args, self.token, self.cmr, self.settings, query_timerange, datetime.now(), verbose=verbose))
            granules_map = rtc_granules_by_acq_index(granules)

            # Step 2 of 2 ...Sort that by acquisition_cycle in decreasing order and then pick the first k-1 frames
            acq_day_indices = sorted(granules_map.keys(), reverse=True)
            for acq_day_index in acq_day_indices:

                ''' This step is a bit tricky.
                1. We want exactly one frame worth of granules do don't create additional granules if the burst belongs to two frames.
                2. We already know what frame these new granules belong to because that's what we queried for. 
                    We need to force using that because 1/9 times one burst will belong to two frames.'''
                granules = granules_map[acq_day_index]
                k_granules.extend(granules)
                k_satified += 1
                self.logger.info(f"{product_id=} {acq_day_index=} satsifies. {k_satified=} {k_minus_one=}")
                if k_satified == k_minus_one:
                    break

            counter += 1

        return k_granules

    def download_job_submission_handler(self, granules, query_timerange):

        batch_id_to_urls_map = defaultdict(set)

        for granule in granules:
            if granule.get("filtered_urls"):
                for filter_url in granule.get("filtered_urls"):
                        batch_id_to_urls_map[granule["download_batch_id"]].add(filter_url)

        self.logger.debug(f"{batch_id_to_urls_map=}")

        job_submission_tasks = []

        for batch_chunk in self.get_download_chunks(batch_id_to_urls_map):
            chunk_batch_ids = []
            chunk_urls = []
            for batch_id, urls in batch_chunk:
                chunk_batch_ids.append(batch_id)
                chunk_urls.extend(urls)

            self.logger.debug(f"{chunk_batch_ids=}")
            self.logger.debug(f"{chunk_urls=}")

            product_type = "rtc_for_dist"

            download_job_id = try_submit_mozart_job(product = {},
                                                    params=self._create_download_job_params(query_timerange, chunk_batch_ids, chunk_urls),
                                                    job_queue=self.args.job_queue,
                                                    rule_name=f"trigger-{product_type}_download",
                                                    job_spec=f"job-{product_type}_download:{self.settings['RELEASE_VERSION']}",
                                                    job_type=f"{product_type}_download",
                                                    job_name=f"job-WF-{product_type}_download-{chunk_batch_ids[0]}")

            # Record download job id in ES
            for batch_id, urls in batch_chunk:
                self.es_conn.mark_download_job_id(batch_id, download_job_id)

            job_submission_tasks.append(download_job_id)

        return job_submission_tasks

    def _create_download_job_params(self, query_timerange, chunk_batch_ids, product_metadata):
        params = super().create_download_job_params(query_timerange, chunk_batch_ids)
        params.append({
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": product_metadata
        })
        return params
    def get_download_chunks(self, batch_id_to_urls_map):

        chunk_map = defaultdict(list)
        if len(list(batch_id_to_urls_map)) == 0:
            return chunk_map.values()

        for batch_chunk in batch_id_to_urls_map.items():
            chunk_map[batch_chunk[0]].append(batch_chunk)  # We don't actually care about the URLs, we only care about the batch_id

        return chunk_map.values()


