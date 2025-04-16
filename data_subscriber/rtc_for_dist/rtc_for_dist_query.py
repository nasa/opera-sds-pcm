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
                                           dist_s1_download_batch_id, build_rtc_native_ids, rtc_granules_by_acq_index,
                                           basic_decorate_granule)

DIST_K_MULT_FACTOR = 2 # TODO: This should be a setting in probably settings.yaml.
K_GRANULES = 10 # Should be either parameter into query job or settings.yaml
EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = localize_dist_burst_db()

        #TODO: Grace minutes? Read from settings.yaml

        '''These two maps are set by determine_download_granules and consumed by download_job_submission_handler
        We're taking this indirect approach instead of just passing this through to work w the current class structure'''
        self.batch_id_to_granules = {}
        self.batch_id_to_k_granules = {}
        self.force_product_id = None

    def validate_args(self):
        pass

    def query_cmr(self, timerange, now: datetime):
        if self.args.proc_mode == "forward":
            granules = super().query_cmr(timerange, now)
        elif self.args.proc_mode == "reprocessing":
            granules = []

            #TODO: We can switch over to this code if we want to trigger reprocessing by RTC granule_id
            '''burst_id, acquisition_dts = parse_r2_product_file_name(self.args.native_id, "L2_RTC_S1")
            product_ids = self.bursts_to_products[burst_id]
            if len(product_ids) == 0:
                raise AssertionError(f"Cannot find burst_id {burst_id} in burst database. Cannot process this product.")
            self.logger.info(f"Reprocessing burst_id {burst_id} with product_ids {product_ids}")'''

            #TODO: We probably want something more graceful than the native_id looking like 31SGR_3,20231217T053132Z
            product_ids = [self.args.native_id.split(",")[0]]
            acquisition_dts = self.args.native_id.split(",")[1]

            acquisition_time = datetime.strptime(acquisition_dts, "%Y%m%dT%H%M%SZ")
            start_time = (acquisition_time - timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
            end_time = (acquisition_time + timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
            query_timerange = DateTimeRange(start_time, end_time)
            for product_id in product_ids:
                self.force_product_id = product_id #TODO: This needs to change if we change this code back to using granule_id instead of product_id
                new_args = deepcopy(self.args)
                new_args.use_temporal = True
                count, new_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts)
                if count == 0:
                    raise AssertionError(f"No burst_ids found for {product_id=}. Cannot process this product.")
                self.logger.info(new_args)
                gs = asyncio.run(
                    async_query_cmr(new_args, self.token, self.cmr, self.settings, query_timerange, datetime.now()))
                for g in gs:
                    g["product_id"] = product_id # force product_id because one granule can belong to multiple products
                granules.extend(gs)

        elif self.args.proc_mode == "historical":
            self.logger.error("Historical processing mode is not supported for RTC for DIST products.")
            granules = []

        # Remove granules whose burst_id is not in the burst database
        filtered_granules = []
        for granule in granules:
            basic_decorate_granule(granule)
            burst_id = granule["burst_id"]
            if burst_id in self.bursts_to_products:
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

        return filtered_granules

    def extend_additional_records(self, granules, no_duplicate=False, force_product_id=None):

        extended_granules = []

        def decorate_granule(granule):
            granule["tile_id"] = granule["product_id"].split("_")[0]
            granule["acquisition_group"] = granule["product_id"].split("_")[1]
            granule["batch_id"] = granule["product_id"] + "_" + str(granule["acquisition_cycle"])
            granule["download_batch_id"] = dist_s1_download_batch_id(granule)
            granule["unique_id"] = rtc_for_dist_unique_id(granule["download_batch_id"], granule["burst_id"])

        for granule in granules:
            rtc_granule_id = granule["granule_id"]
            product_ids = list(self.bursts_to_products[granule["burst_id"]])

            if len(product_ids) == 0:
                self.logger.error(f"This shouldn't happen. Skipping {rtc_granule_id} as it does not belong to any DIST-S1 product.")
                continue

            granule["product_id"] = force_product_id if force_product_id else product_ids[0]
            decorate_granule(granule)

            if len(product_ids) > 1 and no_duplicate == False:
                for product_id in product_ids[1:]:
                    new_granule = deepcopy(granule)
                    new_granule["product_id"] = force_product_id if force_product_id else product_id
                    decorate_granule(new_granule)
                    extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):
        """This is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "tile_id", "product_id", "acquisition_group", "acquisition_ts", "acquisition_cycle", "unique_id", "batch_id", "download_batch_id"]:
            additional_fields[f] = granule[f]

        return additional_fields

    def determine_download_granules(self, granules):
        if len(granules) == 0:
            return granules

        self.logger.debug(f"{len(granules)} granules, before extending")
        self.extend_additional_records(granules, force_product_id=self.force_product_id)
        self.logger.debug(f"{len(granules)} granules, after extending")

        download_granules = []

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        self.refresh_index()
        unsubmitted = self.es_conn.get_unsubmitted_granules()
        self.logger.info("len(unsubmitted)=%d", len(unsubmitted))
        '''for granule in unsubmitted:
            print(granule)
            basic_decorate_granule(granule)'''

        self.logger.info(f"Determining download granules from {len(granules) + len(unsubmitted)} granules")

        # Create a dict of granule_id to granule for both the new granules and unsubmitted granules
        granules_dict = {(granule["granule_id"], granule["batch_id"]): granule for granule in granules}
        for granule in unsubmitted:
            granules_dict[(granule["granule_id"], granule["batch_id"])] = granule

        print("len(granules_dict)", len(granules_dict))
        print("granules_dict keys: ", granules_dict.keys())
        granule_ids = list(set([k[0] for k in granules_dict.keys()])) # Only use a unique set of granule_ids
        #TODO: Right now we just have black or white of complete or incomplete bursts. Later we may want to do either percentage or count threshold.
        products_triggered, granules_triggered, _, _ = compute_dist_s1_triggering(
            self.bursts_to_products, self.product_to_bursts, granule_ids, complete_bursts_only = True)
        self.logger.info(f"Following {len(products_triggered.keys())} products triggered and will be submitted for download: {products_triggered.keys()}")

        by_download_batch_id = defaultdict(lambda: defaultdict(dict))

        for batch_id, product in products_triggered.items():
            for rtc_granule in product.rtc_granules:
                by_download_batch_id[batch_id][rtc_granule] = granules_dict[(rtc_granule, batch_id)]
                download_granules.append(granules_dict[(rtc_granule, batch_id)])

        # batch_id looks like this: 32UPD_4_302; download_batch_id looks like this: p32UPD_4_a302
        for batch_id, download_batch in by_download_batch_id.items():
            #if batch_id == "32UPD_4_302":
            #    for k in download_batch.keys():
            #        print(k)
            product_id = "_".join(batch_id.split("_")[0:2])
            self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(download_batch))
            all_granules = list(download_batch.values())
            download_batch_id = all_granules[0]["download_batch_id"]
            self.batch_id_to_granules[download_batch_id] = all_granules # Used when submitting download job
            self.logger.debug(f"download_batch_id={download_batch_id}")

            try:
                self.batch_id_to_k_granules[download_batch_id] = self.retrieve_baseline_granules(product_id, all_granules, self.args, K_GRANULES - 1, verbose=False)
            except Exception as e:
                self.logger.warning(f"Error retrieving baseline granules for {download_batch_id}: {e}. Cannot submit this job.")
                continue

        return download_granules

    def retrieve_baseline_granules(self, product_id, downloads, args, k_minus_one, verbose = True):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the
        current product. Return all the granules that satisfy that'''
        k_granules = []
        k_satified = 0

        if len(downloads) == 0:
            return k_granules

        '''All download granules should be within a few minutes of each other in acquisition time so we just pick one'''
        acquisition_time = downloads[0]["acquisition_ts"]
        new_args = deepcopy(args)
        new_args.use_temporal = True
        _, new_args.native_id = build_rtc_native_ids(product_id, self.product_to_bursts) # First return value is the number of native_ids

        # TODO: Not sure if we'll need this or not; only need if we want to match the burst_id pattern exactly
        # Create a set of burst_ids for the current product to compare with the frames over k- cycles
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
            self.logger.debug(new_args)

            # Step 1 of 2: This will return dict of acquisition_cycle -> set of granules for only onse that match the burst pattern
            granules = asyncio.run(async_query_cmr(new_args, self.token, self.cmr, self.settings, query_timerange, datetime.now(), verbose=verbose))
            for granule in granules:
                basic_decorate_granule(granule)
                granule["product_id"] = product_id # force product_id because all baseline granules should have the same product_id as the current granules
            self.extend_additional_records(granules, no_duplicate=True, force_product_id=product_id)
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

    def download_job_submission_handler(self, total_granules, query_timerange):

        def add_filtered_urls(granule, filtered_urls: list):
            if granule.get("filtered_urls"):
                for filter_url in granule.get("filtered_urls"):
                    if "s3://" in filter_url and ("VV.tif" in filter_url or "VH.tif" in filter_url):
                        filtered_urls.append(filter_url)

        batch_id_to_urls_map = defaultdict(list)
        batch_id_to_baseline_urls = defaultdict(list)
        product_metadata = {}

        for batch_id, granules in self.batch_id_to_granules.items():
            for granule in granules:
                #self.logger.info(granule["download_batch_id"])
                add_filtered_urls(granule, batch_id_to_urls_map[batch_id])

        for download_batch_id, granules in self.batch_id_to_k_granules.items():
            for granule in granules:
                #print(download_batch_id, granule["download_batch_id"])
                add_filtered_urls(granule, batch_id_to_baseline_urls[download_batch_id])
        #print(batch_id_to_baseline_urls)

        #self.logger.debug(f"{batch_id_to_urls_map=}")

        job_submission_tasks = []

        for batch_id, urls in batch_id_to_urls_map.items():
            chunk_batch_ids = [batch_id]
            self.logger.info(f"Submitting download job for {batch_id=}")
            self.logger.debug(f"{urls=}")
            product_metadata["current_s3_paths"] = urls

            if batch_id not in batch_id_to_baseline_urls:
                self.logger.warning(f"Cannot find baseline URLs for {batch_id}. Cannot submit download job.")
                continue
            product_metadata["baseline_s3_paths"] = batch_id_to_baseline_urls[batch_id]

            product_type = "rtc_for_dist"
            download_job_id = try_submit_mozart_job(product = {},
                                                    params=self._create_download_job_params(query_timerange, chunk_batch_ids, product_metadata),
                                                    job_queue=self.args.job_queue,
                                                    rule_name=f"trigger-{product_type}_download",
                                                    job_spec=f"job-{product_type}_download:{self.settings['RELEASE_VERSION']}",
                                                    job_type=f"{product_type}_download",
                                                    job_name=f"job-WF-{product_type}_download-{chunk_batch_ids[0]}")

            # Record download job id in ES
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


