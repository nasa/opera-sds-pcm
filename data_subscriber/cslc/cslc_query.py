#!/usr/bin/env python3

import copy
import logging
from collections import defaultdict
import asyncio
from datetime import datetime, timedelta

from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT
from data_subscriber.cslc_utils import (localize_disp_frame_burst_hist,  build_cslc_native_ids,  parse_cslc_native_id,
                                        process_disp_frame_burst_hist, download_batch_id_forward_reproc, split_download_batch_id,
                                        parse_cslc_file_name, CSLCDependency,
                                        localize_disp_blackout_dates, process_disp_blackout_dates, DispS1BlackoutDates)
from data_subscriber.query import CmrQuery, DateTimeRange
from data_subscriber.url import cslc_unique_id
from data_subscriber.cslc.cslc_catalog import KCSLCProductCatalog

K_MULT_FACTOR = 3 #TODO: This should be a setting in probably settings.yaml.
EARLIEST_POSSIBLE_CSLC_DATE = "2016-01-01T00:00:00Z"

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings, disp_frame_burst_hist_file = None, blackout_dates_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_frame_burst_hist_file is None:
            self.disp_burst_map_hist, self.burst_to_frames, self.datetime_to_frames = localize_disp_frame_burst_hist()
        else:
            self.disp_burst_map_hist, self.burst_to_frames, self.datetime_to_frames = \
                process_disp_frame_burst_hist(disp_frame_burst_hist_file)

        if blackout_dates_file is None:
            self.frame_blackout_dates = localize_disp_blackout_dates()
        else:
            self.frame_blackout_dates = process_disp_blackout_dates(blackout_dates_file)
        self.blackout_dates_obj = DispS1BlackoutDates(self.frame_blackout_dates, self.disp_burst_map_hist)

        if args.grace_mins:
            self.grace_mins = args.grace_mins
        else:
            self.grace_mins = settings["DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES"]

        # This maps batch_id to list of batch_ids that should be used to trigger the DISP-S1 download job.
        # For example,
        self.download_batch_ids = defaultdict(set)

        self.k_batch_ids = defaultdict(set)  # We store this within this class object and use it when we catalog all granules
        self.k_retrieved_granules = []
        self.k_es_conn = KCSLCProductCatalog(logging.getLogger(__name__))

    def validate_args(self):

        if self.proc_mode == "historical":
            if self.args.frame_id is None:
                raise AssertionError("Historical mode requires frame id to be specified.")
            if self.args.start_date is None or self.args.end_date is None:
                raise AssertionError("Historical mode requires start and end date to be specified.")
            if self.args.native_id is not None:
                raise AssertionError("Historical mode does not support native_id.")

        if self.proc_mode == "reprocessing":
            if self.args.native_id is None and self.args.start_date is None and self.args.end_date is None:
                raise AssertionError("Reprocessing mode requires either a native_id or a date range to be specified.")

        if self.args.k is None:
            raise AssertionError("k parameter must be specified.")
        if self.args.k < 1:
            raise AssertionError("k parameter must be greater than 0.")

        if self.args.m is None:
            raise AssertionError("m parameter must be specified.")
        if self.args.m < 1:
            raise AssertionError("m parameter must be greater than 0.")

    def extend_additional_records(self, granules, no_duplicate=False, force_frame_id = None):
        """Add frame_id, burst_id, and acquisition_cycle to all granules.
        In forward  and re-processing modes, extend the granules with potentially additional records
        if a burst belongs to two frames."""

        extended_granules = []
        for granule in granules:
            granule_id = granule["granule_id"]

            burst_id, acquisition_dts, acquisition_cycles, frame_ids = (
                parse_cslc_native_id(granule_id, self.burst_to_frames, self.disp_burst_map_hist))

            granule["acquisition_ts"] = acquisition_dts

            granule["burst_id"] = burst_id
            granule["frame_id"] = frame_ids[0] if force_frame_id is None else force_frame_id
            granule["acquisition_cycle"] = acquisition_cycles[granule["frame_id"]]
            granule["download_batch_id"] = download_batch_id_forward_reproc(granule)
            granule["unique_id"] = cslc_unique_id(granule["download_batch_id"], granule["burst_id"])

            if self.proc_mode not in ["forward"] or no_duplicate:
                continue

            # If this burst belongs to two frames, make a deep copy of the granule and append to the list
            if len(frame_ids) == 2:
                new_granule = copy.deepcopy(granule)
                new_granule["frame_id"] = self.burst_to_frames[burst_id][1]
                granule["acquisition_cycle"] = acquisition_cycles[granule["frame_id"]]
                new_granule["download_batch_id"] = download_batch_id_forward_reproc(new_granule)
                new_granule["unique_id"] = cslc_unique_id(new_granule["download_batch_id"], new_granule["burst_id"])
                extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):
        """For CSLC this is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        download_batch_id = download_batch_id_forward_reproc(granule)

        # Additional fields are lost after writing to ES so better to keep this in the granule
        granule["download_batch_id"] = download_batch_id

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "frame_id", "acquisition_ts", "acquisition_cycle", "unique_id"]:
            additional_fields[f] = granule[f]
        additional_fields["download_batch_id"] = download_batch_id
        additional_fields["k"] = args.k
        additional_fields["m"] = args.m

        return additional_fields

    def determine_download_granules(self, granules):
        """In forward processing mode combine these new granules with existing unsubmitted granules to determine
        which granules to download. And also retrieve k granules.
        In reprocessing, just retrieve the k granules."""

        # In historical what we received from query was exactly what we needed.
        # This was all computed by run_disp_s1_historical_processing.py upstream
        if self.proc_mode == "historical":
            return granules

        # In reprocessing, get rid of any batch ids that don't have full burst set and then retrieve k-granules
        if self.proc_mode == "reprocessing":
            if len(granules) == 0:
                return granules

            reproc_granules = []
            # Group all granules by download_batch_id
            by_download_batch_id = defaultdict(lambda: defaultdict(dict))
            for granule in granules:
                by_download_batch_id[granule["download_batch_id"]][granule["unique_id"]] = granule

            for batch_id, download_batch in by_download_batch_id.items():
                frame_id, _ = split_download_batch_id(batch_id)
                max_bursts = len(self.disp_burst_map_hist[frame_id].burst_ids)
                if len(download_batch) == max_bursts:
                    reproc_granules.extend(list(download_batch.values()))
                else:
                    logger.info(f"Skipping download for {batch_id} because only {len(download_batch)} of {max_bursts} granules are present")

            if len(reproc_granules) == 0:
                return reproc_granules

            if self.args.k > 1:
                batch_id = reproc_granules[0]["download_batch_id"]
                k_granules = self.retrieve_k_granules(reproc_granules, self.args, self.args.k - 1, True, silent=True)
                self.catalog_granules(k_granules, datetime.now(), self.k_es_conn)
                logger.info(f"Length of K-granules: {len(k_granules)=}")
                for k_g in k_granules:
                    self.download_batch_ids[k_g["download_batch_id"]].add(batch_id)
                    self.k_batch_ids[batch_id].add(k_g["download_batch_id"])
                self.k_retrieved_granules.extend(k_granules)  # This is used for scenario testing
            return reproc_granules

        # From this point on is forward processing which is the most complex

        current_time = datetime.now()

        # This list is what is ultimately returned by this function
        download_granules = []

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        self.refresh_index()
        unsubmitted = self.es_conn.get_unsubmitted_granules()

        logger.info(f"{len(granules)=}")
        logger.info(f"{len(unsubmitted)=}")

        # Group all granules by download_batch_id
        # If the same download_batch_id is in both granules and unsubmitted, we will use the one in granules because it's newer
        # unique_id is mapped into id in ES
        by_download_batch_id = defaultdict(lambda: defaultdict(dict))
        for granule in granules:
            by_download_batch_id[granule["download_batch_id"]][granule["unique_id"]] = granule

        logger.info("Received the following cslc granules from CMR")
        for batch_id, download_batch in by_download_batch_id.items():
            logger.info(f"{batch_id=} {len(download_batch)=}")

        # THIS RULE ALSO NO LONGER APPLIES. Without Rule 2 there is no Rule 3
        # Rule 3: If granules have been downloaded already but with less than 100% and we have new granules for that batch, download all granules for that batch
        # If the download_batch_id of the granules we received had already been submitted,
        # we need to submit them again with the new granules. We add both the new granules and the previously-submitted granules
        # immediately to the download_granules list because we know for sure that we want to download them without additional reasoning.
        '''for batch_id, download_batch in by_download_batch_id.items():
            submitted = self.es_conn.get_submitted_granules(batch_id)
            frame_id, acquisition_cycle = split_download_batch_id(batch_id)
            max_bursts = len(self.disp_burst_map_hist[frame_id].burst_ids)
            if len(submitted) > 0 and len(submitted) < max_bursts:
                for download in download_batch.values():
                    download_granules.append(download)
                for granule in submitted:
                    download_granules.append(granule)
                self.download_batch_ids[batch_id].add(batch_id)'''

        for granule in unsubmitted:
            logger.info(f"Merging in unsubmitted granule {granule['unique_id']}: {granule['granule_id']} for triggering consideration")
            download_batch = by_download_batch_id[granule["download_batch_id"]]
            if granule["unique_id"] not in download_batch:
                download_batch[granule["unique_id"]] = granule

        # Print them all here so that it's nicely all in one place
        logger.info("After merging unsubmitted granules with the new ones returned from CMR")
        for batch_id, download_batch in by_download_batch_id.items():
            logger.info(f"{batch_id=} {len(download_batch)=}")

        # Combine unsubmitted and new granules and determine which granules meet the criteria for download
        # Rule 1: If all granules for a given download_batch_id are present, download all granules for that batch
        # No LONGER APPLIES and been commented out Rule 2: If it's been xxx hrs since last granule discovery (by OPERA) download all granules for that batch
        for batch_id, download_batch in by_download_batch_id.items():
            frame_id, acquisition_cycle = split_download_batch_id(batch_id)
            max_bursts = len(self.disp_burst_map_hist[frame_id].burst_ids)
            new_downloads = False

            if len(download_batch) == max_bursts: # Rule 1
                logger.info(f"Download all granules for {batch_id} because all {max_bursts} granules are present")
                self.download_batch_ids[batch_id].add(batch_id) # This batch needs to be submitted as part of the download job for sure
                new_downloads = True
            else:
                logger.info(f"Skipping download for {batch_id} because only {len(download_batch)} of {max_bursts} granules are present")
            '''As per email from Heresh at ADT on 7-25-2024, we will not use rule 2. We will always only process full-frames
            Keeping this code around in case we change our mind on that.
            else:
                # Rule 2
                min_creation_time = current_time
                for download in download_batch.values():
                    if "creation_timestamp" in download:
                        # creation_time looks like this: 2024-01-31T20:45:25.723945
                        creation_time = datetime.strptime(download["creation_timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
                        if creation_time < min_creation_time:
                            min_creation_time = creation_time

                mins_since_first_ingest = (current_time - min_creation_time).total_seconds() / 60.0
                if mins_since_first_ingest > self.grace_mins:
                    logger.info(f"Download all granules for {batch_id} because it's been {mins_since_first_ingest} minutes \
since the first CSLC file for the batch was ingested which is greater than the grace period of {self.grace_mins} minutes")
                    new_downloads = True
                    #print(batch_id, download_batch)'''

            if new_downloads:

                # And also add these downloads into the return download list
                for download in download_batch.values():
                    download_granules.append(download)
                    #print("**********************************************************", download["download_batch_id"])

                # Retrieve K- granules and M- compressed CSLCs for this batch
                if self.args.k > 1:
                    logger.info("Retrieving K frames worth of data from CMR")
                    k_granules = self.retrieve_k_granules(list(download_batch.values()), self.args, self.args.k-1)
                    self.catalog_granules(k_granules, current_time, self.k_es_conn)
                    self.k_retrieved_granules.extend(k_granules) # This is used for scenario testing
                    logger.info(f"Length of K-granules: {len(k_granules)=}")
                    #print(f"{granules=}")

                    # All the k batches need to be submitted as part of the download job for this batch
                    # Mark for all k_granules to cover all k batch_ids
                    for k_g in k_granules:
                        self.download_batch_ids[k_g["download_batch_id"]].add(batch_id)
                        self.k_batch_ids[batch_id].add(k_g["download_batch_id"])

            if (len(download_batch) > max_bursts):
                logger.error(f"{len(download_batch)=} {max_bursts=}")
                logger.error(f"{download_batch=}")
                raise AssertionError("Something seriously went wrong matching up CSLC input granules!")

        logger.info(f"{len(download_granules)=}")

        return download_granules

    def retrieve_k_granules(self, downloads, args, k_minus_one, VV_only = True, silent=False):
        '''# Go back as many 12-day windows as needed to find k- granules that have at least the same bursts as the current frame
        Return all the granules that satisfy that'''
        k_granules = []
        k_satified = 0
        new_args = copy.deepcopy(args)

        if len(downloads) == 0:
            return k_granules

        '''All download granules should have the same frame_id
        All download granules should be within a few minutes of each other in acquisition time so we just pick one'''
        frame_id = downloads[0]["frame_id"]
        acquisition_time = downloads[0]["acquisition_ts"]

        # Create a set of burst_ids for the current frame to compare with the frames over k- cycles
        burst_id_set = set()
        for download in downloads:
            burst_id_set.add(download["burst_id"])

        # Move start and end date of new_args back and expand 5 days at both ends to capture all k granules
        shift_day_grouping = 12 * (k_minus_one * K_MULT_FACTOR) # Number of days by which to shift each iteration

        counter = 1
        while k_satified < k_minus_one:
            start_date_shift = timedelta(days=6 + counter * shift_day_grouping)
            end_date_shift = timedelta(days= 6 + (counter-1) * shift_day_grouping)
            start_date = (acquisition_time - start_date_shift).strftime(CMR_TIME_FORMAT)
            end_date_object = (acquisition_time - end_date_shift)
            end_date = end_date_object.strftime(CMR_TIME_FORMAT)
            query_timerange = DateTimeRange(start_date, end_date)

            # Sanity check: If the end date object is earlier year 2016 then error out. We've exhaust data space.
            if end_date_object < datetime.strptime(EARLIEST_POSSIBLE_CSLC_DATE, CMR_TIME_FORMAT):
                raise AssertionError(f"We are searching earlier than {EARLIEST_POSSIBLE_CSLC_DATE}. There is no more data here. {end_date_object=}")

            logger.info(f"Retrieving K-1 granules {start_date=} {end_date=} for {frame_id=}")

            # Step 1 of 2: This will return dict of acquisition_cycle -> set of granules for only onse that match the burst pattern
            cslc_dependency = CSLCDependency(args.k, args.m, self.disp_burst_map_hist, args, self.token, self.cmr, self.settings, VV_only)
            _, granules_map = cslc_dependency.get_k_granules_from_cmr(query_timerange, frame_id, silent=silent)

            # Step 2 of 2 ...Sort that by acquisition_cycle in decreasing order and then pick the first k-1 frames
            acq_day_indices = sorted(granules_map.keys(), reverse=True)
            print("+++++++++++++++++++++++", acq_day_indices)
            for acq_day_index in acq_day_indices:

                ''' This step is a bit tricky.
                1. We want exactly one frame worth of granules do don't create additional granules if the burst belongs to two frames.
                2. We already know what frame these new granules belong to because that's what we queried for. 
                    We need to force using that because 1/9 times one burst will belong to two frames.'''
                granules = granules_map[acq_day_index]
                self.extend_additional_records(granules ,no_duplicate=True, force_frame_id=frame_id)
                granules = self.eliminate_duplicate_granules(granules)

                k_granules.extend(granules)
                k_satified += 1
                logger.info(f"{acq_day_index=} satsifies. {k_satified=} {k_minus_one=}")
                if k_satified == k_minus_one:
                    break

            counter += 1

        return k_granules

    def query_cmr_by_native_id (self, args, token, cmr, settings, now, native_id):

        local_args = copy.deepcopy(args)

        # expand the native_id to include all bursts in the frame to which this granule belongs.
        # And then restrict by the acquisition date. Go back 12 days * (k -1) to cover the acquisition date range
        local_args.use_temporal = True
        burst_id, acquisition_time, _, frame_ids = parse_cslc_native_id(native_id, self.burst_to_frames, self.disp_burst_map_hist)

        if len(frame_ids) == 0:
            logger.warning(f"{native_id=} is not found in the DISP-S1 Burst ID Database JSON. Nothing to process")
            return []

        # TODO: Also check if we should be processing this native_id for this acquisition cycle.
        # Not sure if parse_cslc_native_id() should also perform that check or not

        frame_id = min(frame_ids)  # In case of this burst belonging to two frames, pick the lower frame id
        start_date = (acquisition_time - timedelta(minutes=15)).strftime(CMR_TIME_FORMAT)
        end_date = (acquisition_time + timedelta(minutes=15)).strftime(CMR_TIME_FORMAT)
        timerange = DateTimeRange(start_date, end_date)
        logger.info(
            f"Querying CMR for all CSLC files that belong to the frame {frame_id}, derived from the native_id {native_id}")

        l, native_id_pattern = build_cslc_native_ids(frame_id, self.disp_burst_map_hist)
        local_args.native_id = native_id_pattern  # native_id is overwritten here. It's local deepcopy so doesn't matter.
        granules = asyncio.run(async_query_cmr(local_args, token, cmr, settings, timerange, now))

        # Remove granules that don't belong to the frame
        for g in granules:
            _, _, _, f_ids_local = parse_cslc_native_id(g["granule_id"], self.burst_to_frames, self.disp_burst_map_hist)
            if frame_id not in f_ids_local:
                granules.remove(g)

        self.extend_additional_records(granules, no_duplicate=True, force_frame_id=frame_id)

        return granules

    def query_cmr_by_frame_and_dates(self, args, token, cmr, settings, now, timerange, silent=False):

        frame_id = int(self.args.frame_id)
        if frame_id not in self.disp_burst_map_hist:
            raise Exception(f"Frame number {frame_id} not found in the historical database. \
        OPERA does not process this frame for DISP-S1.")

        new_args = copy.deepcopy(args)
        all_granules = []
        count, native_id = build_cslc_native_ids(frame_id, self.disp_burst_map_hist)
        if count == 0:
            return all_granules
        new_args.native_id = native_id
        new_granules = asyncio.run(async_query_cmr(new_args, token, cmr, settings, timerange, now, silent))
        self.extend_additional_records(new_granules, no_duplicate=True, force_frame_id=frame_id)
        all_granules.extend(new_granules)

        return all_granules

    def query_cmr(self, args, token, cmr, settings, timerange, now):

        # If we are in historical mode, we will query one frame worth at a time
        if self.proc_mode == "historical":
            all_granules = self.query_cmr_by_frame_and_dates(args, token, cmr, settings, now, timerange)

            # Get rid of any granules that aren't in the historical database sensing_datetime_days_index
            frame_id = int(self.args.frame_id)
            all_granules = [granule for granule in all_granules
                            if granule["acquisition_cycle"] in self.disp_burst_map_hist[frame_id].sensing_datetime_days_index]

        # TODO: How do we handle partial frames when querying by date? Make them all whole or only process the full frames?
        # Reprocessing can be done by specifying either a native_id or a date range
        # native_id search takes precedence over date range if both are specified
        elif self.proc_mode == "reprocessing":

            if args.native_id is not None:
                all_granules = self.query_cmr_by_native_id(args, token, cmr, settings, now, args.native_id)

            # Reprocessing by date range is a two-step process:
            # 1) Query CMR for all CSLC files in the date range specified and create list of granules with unique frame_ids
            # 2) Process each granule as if they were passed in as native_id
            elif args.start_date is not None and args.end_date is not None:
                unique_granules = {}
                frame_id_map = defaultdict(str)

                # First get all CSLC files in the range specified and create a unique set of frame_ids that we need to query for.
                if self.args.frame_id is not None:
                    granules = self.query_cmr_by_frame_and_dates(args, token, cmr, settings, now, timerange)
                    if len(granules) == 0:
                        return []
                    frame_id_map[self.args.frame_id] = granules[0]["granule_id"]
                else:
                    granules = asyncio.run(async_query_cmr(args, token, cmr, settings, timerange, now))
                    for granule in granules:
                        _, _, _, frame_ids = parse_cslc_native_id(granule["granule_id"], self.burst_to_frames, self.disp_burst_map_hist)
                        for frame_id in frame_ids:
                            frame_id_map[frame_id] = granule["granule_id"]

                # We could perform two queries so create a unique set of granules.
                for frame_id, native_id in frame_id_map.items():
                    new_granules = self.query_cmr_by_native_id(args, token, cmr, settings, now, native_id)
                    for granule in new_granules:
                        unique_granules[granule["granule_id"]] = granule

                all_granules = list(unique_granules.values())
            else:
                raise Exception("Reprocessing mode requires either a native_id or a date range to be specified.")

        else: # Forward processing
            if self.args.frame_id is not None:
                all_granules = self.query_cmr_by_frame_and_dates(args, token, cmr, settings, now, timerange)
            else:
                all_granules = asyncio.run(async_query_cmr(args, token, cmr, settings, timerange, now))
                all_granules = self.eliminate_none_frames(all_granules)
                self.extend_additional_records(all_granules)

        # Get rid of any granules that don't have the VV polarization
        # TODO: at some point we will change the code so that we can process HH polarization too
        temp_all_graules = copy.deepcopy(all_granules)
        all_granules = []
        for granule in temp_all_graules:
            if "_VV_" not in granule["granule_id"]:
                logger.info(f"Skipping granule {granule['granule_id']} because it's not in VV polarization")
            else:
                all_granules.append(granule)

        non_blackout_granules = []
        for granule in all_granules:
            frame_id = granule["frame_id"]
            is_black_out, dates = self.blackout_dates_obj.is_in_blackout(frame_id, granule["acquisition_ts"])
            if is_black_out:
                blackout_start = dates[0].strftime(CMR_TIME_FORMAT)
                blackout_end = dates[1].strftime(CMR_TIME_FORMAT)
                logger.info(f"Skipping granule {granule['granule_id']} because {frame_id=} falls on a blackout date {blackout_start=} {blackout_end=}")
            else:
                #logger.info(f"Adding granule {granule['granule_id']} to the list of granules to download")
                non_blackout_granules.append(granule)

        return non_blackout_granules

    def create_download_job_params(self, query_timerange, chunk_batch_ids):
        '''Same as base class except inject batch_ids for k granules'''

        chunk_batch_ids.extend(list(self.k_batch_ids[chunk_batch_ids[0]]))
        return super().create_download_job_params(query_timerange, chunk_batch_ids)

    def eliminate_duplicate_granules(self, granules):
        """For CSLC granules revision_id is always one. Instead, we correlate the granules by the unique_id
         which is a function of download_batch_id and burst_id

         You must run extend_additional_records before calling this function because it requires granules to have
         many properties that are added by that function."""

        granule_dict = {}
        for granule in granules:
            granule["download_batch_id"] = download_batch_id_forward_reproc(granule)
            granule["unique_id"] = cslc_unique_id(granule["download_batch_id"], granule["burst_id"])
            unique_id = granule["unique_id"]

            if unique_id in granule_dict:
                if granule["granule_id"] > granule_dict[unique_id]["granule_id"]:
                    granule_dict[unique_id] = granule
            else:
                granule_dict[unique_id] = granule
        granules = list(granule_dict.values())

        return granules

    def eliminate_none_frames(self, granules):
        '''Get rid of frames that don't show up in the historical database json.'''

        new_granules = []
        for granule in granules:
            burst_id, _ = parse_cslc_file_name(granule["granule_id"])
            if burst_id in self.burst_to_frames:
                new_granules.append(granule)

        return new_granules

    def get_download_chunks(self, batch_id_to_urls_map):
        '''For CSLC chunks we must group them by the batch_id that were determined at the time of triggering'''

        chunk_map = defaultdict(list)
        if len(list(batch_id_to_urls_map)) == 0:
            return chunk_map.values()

        frame_id, _ = split_download_batch_id(list(batch_id_to_urls_map)[0])

        for batch_chunk in batch_id_to_urls_map.items():

            # Chunking is done differently between historical and forward/reprocessing
            if self.proc_mode == "historical":
                chunk_map[frame_id].append(batch_chunk)
            else:
                chunk_map[batch_chunk[0]].append(
                    batch_chunk)  # We don't actually care about the URLs, we only care about the batch_id

        if self.proc_mode == "historical":
            if (len(chunk_map[frame_id]) != self.args.k):
                logger.error([chunk for chunk, data in chunk_map[frame_id]])
                err_str = f"Number of download batches {len(chunk_map[frame_id])} for frame {frame_id} does not equal K {self.args.k}."
                raise AssertionError(err_str)

        return chunk_map.values()

    def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")
