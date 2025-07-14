from collections import defaultdict
from datetime import datetime, timedelta
from copy import deepcopy
import asyncio

from util.job_submitter import try_submit_mozart_job

from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.url import determine_acquisition_cycle, rtc_for_dist_unique_id
from data_subscriber.query import CmrQuery, get_query_timerange, DateTimeRange
from data_subscriber.dist_s1_utils import (localize_dist_burst_db, process_dist_burst_db, compute_dist_s1_triggering,
                                           extend_rtc_for_dist_records, build_rtc_native_ids, rtc_granules_by_acq_index,
                                           basic_decorate_granule, add_unique_rtc_granules, get_unique_rtc_id_for_dist,
                                           parse_k_parameter)
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency

DIST_K_MULT_FACTOR = 2 # TODO: This should be a setting in probably settings.yaml; must be an integer
EARLIEST_POSSIBLE_RTC_DATE = "2016-01-01T00:00:00Z"

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = localize_dist_burst_db()

        self.grace_mins = args.grace_mins if args.grace_mins else settings["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        self.logger.info(f"grace_mins={self.grace_mins}")

        self.dist_dependency = DistDependency(self.logger, self.dist_products, self.bursts_to_products, self.product_to_bursts)

        '''This map is set by determine_download_granules and consumed by download_job_submission_handler
        We're taking this indirect approach instead of just passing this through to work w the current class structure'''
        self.batch_id_to_k_granules = {}

        self.force_product_id = None

    def validate_args(self):
        if self.args.proc_mode == "reprocessing":
            if not self.args.product_id_time:
                raise AssertionError("--product-id-time must be provided in DIST-S1 reprocessing mode.")

    def unique_latest_granules(self, granules):
        ''' Remove duplicate granules defined by having the same burst_id and acquisition_ts, keep just the latest one'''
        granules_dict = {}
        for granule in granules:
            key = (granule["burst_id"], granule["acquisition_ts"])
            if key not in granules_dict:
                granules_dict[key] = granule
            else:
                self.logger.info(f"Found duplicate granules {granule['granule_id']}, {granules_dict[key]['granule_id']} with the same burst_id and acquisition_ts. Keeping only the latest production one.")
                if granule["acquisition_ts"] > granules_dict[key]["acquisition_ts"]:
                    granules_dict[key] = granule
        return list(granules_dict.values())

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

            #TODO: We probably want something more graceful than the product_id_time looking like 31SGR_3,20231217T053132Z
            product_ids = [self.args.product_id_time.split(",")[0]]
            acquisition_dts = self.args.product_id_time.split(",")[1]

            acquisition_time = datetime.strptime(acquisition_dts, "%Y%m%dT%H%M%SZ")
            start_time = (acquisition_time - timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
            end_time = (acquisition_time + timedelta(minutes=10)).strftime(CMR_TIME_FORMAT)
            query_timerange = DateTimeRange(start_time, end_time)

            # TODO: The fact that this is a loop makes sense if we ever decide to trigger by native_id instead of product_id_time
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

        download_granules = []

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        self.refresh_index()
        unsubmitted = self.es_conn.get_unsubmitted_granules()
        self.logger.info("len(unsubmitted)=%d", len(unsubmitted))
        '''for granule in unsubmitted:
            print(granule)'''

        self.logger.info(f"Determining download granules from {len(granules) + len(unsubmitted)} granule records")

        # Create a dict of granule_id to granule for both the new granules and unsubmitted granules
        granules_dict = {}
        add_unique_rtc_granules(granules_dict, granules)
        add_unique_rtc_granules(granules_dict, unsubmitted)

        #print("len(granules_dict)", len(granules_dict))
        #print("granules_dict keys: ", granules_dict.keys())
        granule_ids = list(set([g["granule_id"] for g in granules_dict.values()])) # Only use a unique set of granule_ids
        #TODO: Right now we just have black or white of complete or incomplete bursts. Later we may want to do either percentage or count threshold.
        products_triggered, _, _, _ = compute_dist_s1_triggering(self.product_to_bursts, granules_dict, True, self.grace_mins, datetime.now())
        self.logger.info(f"Following {len(products_triggered.keys())} products triggered and will be submitted for download: {products_triggered.keys()}")

        by_download_batch_id = defaultdict(list)

        for batch_id, product in products_triggered.items():
            for rtc_granule in product.rtc_granules:
                unique_rtc_id = get_unique_rtc_id_for_dist(rtc_granule)
                by_download_batch_id[batch_id].append(granules_dict[(unique_rtc_id, batch_id)])
                download_granules.append(granules_dict[(unique_rtc_id, batch_id)])

        # batch_id looks like this: 32UPD_4_302; download_batch_id looks like this: p32UPD_4_a302
        for batch_id, batch_granules in by_download_batch_id.items():
            #if batch_id == "32UPD_4_302":
            #    for k in download_batch.keys():
            #        print(k)
            product_id = "_".join(batch_id.split("_")[0:2])
            self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(batch_granules))
            download_batch_id = batch_granules[0]["download_batch_id"]
            self.logger.debug(f"download_batch_id={download_batch_id}")

            try:
                if self.args.k_offsets_counts:
                    k_offsets_counts = self.args.k_offsets_counts
                    self.logger.info(f"Using k_offsets_counts {k_offsets_counts}")
                else:
                    self.logger.error("k_offsets_counts not provided in args. This should not be possible because \
there must be a default value. Cannot retrieve baseline granules.")

                k_offsets_counts = parse_k_parameter(k_offsets_counts)
                self.logger.info(f"Parsed k_offsets_counts: {k_offsets_counts}")

                self.batch_id_to_k_granules[download_batch_id] =(
                    self.retrieve_baseline_granules(product_id, batch_granules, self.args, k_offsets_counts, verbose=False))
            except Exception as e:
                self.logger.warning(f"Error retrieving baseline granules for {download_batch_id}: {e}. Cannot submit this job.")
                continue

        return download_granules

    def retrieve_baseline_granules(self, product_id, downloads, args, k_offsets_and_counts, verbose = True):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the
        current product.
        k_offsets_and_counts is a list of tuples of (offset, count) where offset is the number of days to go back
        and count is the number of granules for that tuple set'''
        k_granules = []

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

        for k_offset, k_count in k_offsets_and_counts:
            k_satisfied = 0

            # Move start and end date of new_args back and expand 5 days at both ends to capture all k granules
            shift_day_grouping = 12 * (k_count * DIST_K_MULT_FACTOR) # Number of days by which to shift each iteration

            counter = 1
            while k_satisfied < k_count:
                start_date_shift = timedelta(days= k_offset + counter * shift_day_grouping, hours=1)
                end_date_shift = timedelta(days= k_offset + (counter-1) * shift_day_grouping, hours=1)
                start_date = (acquisition_time - start_date_shift).strftime(CMR_TIME_FORMAT)
                end_date_object = (acquisition_time - end_date_shift)
                end_date = end_date_object.strftime(CMR_TIME_FORMAT)
                query_timerange = DateTimeRange(start_date, end_date)

                # Sanity check: If the end date object is earlier than the earliest possible year, then error out. We've exhausted data space.
                if end_date_object < datetime.strptime(EARLIEST_POSSIBLE_RTC_DATE, CMR_TIME_FORMAT):
                    self.logger.warning(f"We are searching earlier than {EARLIEST_POSSIBLE_RTC_DATE}. There is no more data here. {end_date_object=}")
                    break

                self.logger.info(f"Retrieving K-1 granules {start_date=} {end_date=} for {product_id=}")
                self.logger.debug(new_args)

                # Step 1 of 2: This will return dict of acquisition_cycle -> set of granules for only onse that match the burst pattern
                granules = asyncio.run(async_query_cmr(new_args, self.token, self.cmr, self.settings, query_timerange, datetime.now(), verbose=verbose))
                for granule in granules:
                    basic_decorate_granule(granule)
                    granule["product_id"] = product_id # force product_id because all baseline granules should have the same product_id as the current granules
                self.extend_additional_records(granules, no_duplicate=True, force_product_id=product_id)
                granules = self.unique_latest_granules(granules)
                granules_map = rtc_granules_by_acq_index(granules)

                # Step 2 of 2 ...Sort that by acquisition_cycle in decreasing order and then pick the first k-1 frames
                acq_day_indices = sorted(granules_map.keys(), reverse=True)
                for acq_day_index in acq_day_indices:
                    granules = granules_map[acq_day_index]
                    k_granules.extend(granules)
                    k_satisfied += 1
                    self.logger.info(f"{product_id=} {acq_day_index=} satisfies. {k_satisfied=} {k_offset=} {k_count=} {len(granules)=}")
                    if k_satisfied == k_count:
                        break

                counter += 1

        return k_granules

    def download_job_submission_handler(self, total_granules, query_timerange):

        def add_filtered_urls(granule, filtered_urls: list):
            if granule.get("filtered_urls"):
                for filter_url in granule.get("filtered_urls"):
                    # Get rid of .h and mask.tif files that aren't used
                    # NOTE: If we want to enable https downloads in the download worker, we need to change this
                    if "s3://" in filter_url and (filter_url[-6:] in ["VV.tif", "VH.tif", "HH.tif", "HV.tif"]):
                        filtered_urls.append(filter_url)

        batch_id_to_urls_map = defaultdict(list)
        batch_id_to_baseline_urls = defaultdict(list)
        product_metadata = {}

        for granule in total_granules:
            #self.logger.info(granule["download_batch_id"])
            add_filtered_urls(granule, batch_id_to_urls_map[granule["download_batch_id"]])

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

            # If the length of urls is 0, we can't submit this. Skip.
            if len(urls) == 0:
                self.logger.error(f"No urls found for {batch_id}. Cannot submit download job.")
                continue
            product_metadata["current_s3_paths"] = urls

            if batch_id not in batch_id_to_baseline_urls:
                self.logger.warning(f"Cannot find baseline URLs for {batch_id}. Cannot submit download job.")
                continue
            product_metadata["baseline_s3_paths"] = batch_id_to_baseline_urls[batch_id]

            product_type = "rtc_for_dist"

            # If the previous run for this tile has not been processed, submit as a pending job
            # previous_tile_product_file_paths can be None or a list of file paths
            should_wait, previous_tile_product_file_paths, previous_tile_job_id = self.dist_dependency.should_wait_previous_run(batch_id)
            if should_wait:
                self.logger.info(
                    f"We will wait for the previous run for the job {previous_tile_job_id} to complete before submitting the download job.")
                # save_blocked_download_job(self.es_conn.es_util, self.settings["RELEASE_VERSION"],
                #                                           product_type, params, self.args.job_queue, job_name,
                #                                            frame_id, acq_indices[0], self.args.k, self.args.m, chunk_batch_ids)
                continue

            product_metadata["previous_tile_product_file_paths"] = previous_tile_product_file_paths
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



