#!/usr/bin/env python3

import copy
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT
from data_subscriber.cslc_utils import (localize_disp_frame_burst_json,
                                        localize_disp_frame_burst_hist,
                                        build_cslc_native_ids,
                                        parse_cslc_native_id,
                                        process_disp_frame_burst_json,
                                        process_disp_frame_burst_hist,
                                        download_batch_id_forward_reproc,
                                        download_batch_id_hist,
                                        split_download_batch_id)
from data_subscriber.query import CmrQuery, DateTimeRange

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings, disp_frame_burst_file = None, disp_frame_burst_hist_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_frame_burst_file is None:
            self.disp_burst_map, self.burst_to_frame, metadata, version = localize_disp_frame_burst_json()
        else:
            self.disp_burst_map, self.burst_to_frame, metadata, version = process_disp_frame_burst_json(disp_frame_burst_file)

        if disp_frame_burst_hist_file is None:
            self.disp_burst_map_hist = localize_disp_frame_burst_hist()
        else:
            self.disp_burst_map_hist = process_disp_frame_burst_hist(disp_frame_burst_hist_file)

        if args.grace_mins:
            self.grace_mins = args.grace_mins
        else:
            self.grace_mins = settings["DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES"]

    def extend_additional_records(self, granules, no_duplicate=False, force_frame_id = None):
        """Add frame_id, burst_id, and acquisition_cycle to all granules.
        In forward  and re-processing modes, extend the granules with potentially additional records
        if a burst belongs to two frames."""

        extended_granules = []
        for granule in granules:
            granule_id = granule["granule_id"]

            burst_id, acquisition_dts, acquisition_cycle, frame_ids = parse_cslc_native_id(granule_id, self.burst_to_frame)

            granule["acquisition_ts"] = acquisition_dts
            granule["acquisition_cycle"] = acquisition_cycle
            granule["burst_id"] = burst_id
            granule["frame_id"] = frame_ids[0] if force_frame_id is None else force_frame_id
            granule["download_batch_id"] = download_batch_id_forward_reproc(granule)
            granule["unique_id"] = granule["download_batch_id"] + "_" + granule["burst_id"]

            assert len(frame_ids) <= 2  # A burst can belong to at most two frames. If it doesn't, we have a problem.

            if self.proc_mode not in ["forward"] or no_duplicate:
                continue

            # If this burst belongs to two frames, make a deep copy of the granule and append to the list
            if len(frame_ids) == 2:
                new_granule = copy.deepcopy(granule)
                new_granule["frame_id"] = self.burst_to_frame[burst_id][1]
                new_granule["download_batch_id"] = download_batch_id_forward_reproc(new_granule)
                new_granule["unique_id"] = new_granule["download_batch_id"] + "_" + new_granule["burst_id"]
                extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):
        """For CSLC this is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        if self.proc_mode == "historical":
            download_batch_id = download_batch_id_hist(args, granule)
        else: # forward or reprocessing
            download_batch_id = download_batch_id_forward_reproc(granule)

        # Additional fields are lost after writing to ES so better to keep this in the granule
        granule["download_batch_id"] = download_batch_id

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "frame_id", "acquisition_ts", "acquisition_cycle", "unique_id"]:
            additional_fields[f] = granule[f]
        additional_fields["download_batch_id"] = download_batch_id

        return additional_fields

    async def determine_download_granules(self, granules):
        """Combine these new granules with existing unsubmitted granules to determine which granules to download.
        This only applies to forward processing mode. Valid only in forward processing mode."""

        if self.proc_mode != "forward":
            return granules

        current_time = datetime.now()

        # This list is what is ultimately returned by this function
        download_granules = []

        # Get unsubmitted granules, which are forward-processing ES records without download_job_id fields
        await self.refresh_index()
        unsubmitted = self.es_conn.get_unsubmitted_granules()

        logger.info(f"{len(granules)=}")
        logger.info(f"{len(unsubmitted)=}")

        # Group all granules by download_batch_id
        # If the same download_batch_id is in both granules and unsubmitted, we will use the one in granules because it's newer
        # unique_id is mapped into id in ES
        by_download_batch_id = defaultdict(lambda: defaultdict(dict))
        for granule in granules:
            by_download_batch_id[granule["download_batch_id"]][granule["unique_id"]] = granule

        # Rule 3: If granules have been downloaded already but with less than 100% and we have new granules for that batch, download all granules for that batch
        # If the download_batch_id of the granules we received had already been submitted,
        # we need to submit them again with the new granules. We add both the new granules and the previously-submitted granules
        # immediately to the download_granules list because we know for sure that we want to download them without additional reasoning.
        for batch_id, download_batch in by_download_batch_id.items():
            submitted = self.es_conn.get_submitted_granules(batch_id)
            if len(submitted) > 0:
                asdf
                for download in download_batch.values():
                    download_granules.append(download)
                for granule in submitted:
                    download_granules.append(granule)

        for granule in unsubmitted:
            download_batch = by_download_batch_id[granule["download_batch_id"]]
            if granule["unique_id"] not in download_batch:
                download_batch[granule["unique_id"]] = granule

        # Combine unsubmitted and new granules and determine which granules meet the criteria for download
        # Rule 1: If all granules for a given download_batch_id are present, download all granules for that batch
        # Rule 2: If it's been xxx hrs since last granule discovery (by OPERA) download all granules for that batch
        for batch_id, download_batch in by_download_batch_id.items():
            logger.info(f"{batch_id=} {len(download_batch)=}")
            frame_id, acquisition_cycle = split_download_batch_id(batch_id)
            max_bursts = len(self.disp_burst_map[frame_id].burst_ids)
            new_downloads = False

            if len(download_batch) == max_bursts: # Rule 1
                logger.info(f"Download all granules for {batch_id} because all granules are present")
                new_downloads = True
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
                    #print(batch_id, download_batch)

            if new_downloads:
                for download in download_batch.values():
                    download_granules.append(download)
                    #print("**********************************************************", download["download_batch_id"])

                # Retrieve K- granules and M- compressed CSLCs for this batch
                # Go back K- 12-day windows and find the same frame
                args = self.args
                if args.k > 1:
                    logger.info(f"Retrieving K-1 granules")

                    # Add native-id condition in args
                    l, native_id = build_cslc_native_ids(frame_id, self.disp_burst_map)
                    args.native_id = native_id
                    logger.info(f"{args.native_id=}")

                    #TODO: We can only use past frames which contain the exact same bursts as the current frame
                    # If not, we will need to go back another cycle until, as long as we have to, we find one that does

                    # Move start and end date of args back and expand 5 days at both ends to capture all k granules
                    start_date = (datetime.strptime(args.start_date, CMR_TIME_FORMAT) - timedelta(
                        days=12 * (args.k - 1) + 5)).strftime(CMR_TIME_FORMAT)
                    end_date = (datetime.strptime(args.end_date, CMR_TIME_FORMAT) - timedelta(
                        days=12 - 5)).strftime(CMR_TIME_FORMAT)
                    query_timerange = DateTimeRange(start_date, end_date)
                    logger.info(f"{query_timerange=}")
                    granules = await self.query_cmr(args, self.token, self.cmr, self.settings, query_timerange, datetime.utcnow())

                    # This step is a bit tricky.
                    # 1) We want exactly one frame worth of granules do don't create additional granules if the burst belongs to two frames
                    # 2) We already know what frame these new granules belong to because that's what we queried for. We need to
                    #    force using that because 1/9 times one burst will belong to two frames
                    self.extend_additional_records(granules, no_duplicate=True, force_frame_id=frame_id)

                    granules = self.eliminate_duplicate_granules(granules)
                    self.catalog_granules(granules, current_time)
                    logger.info(f"{len(granules)=}")
                    #print(f"{granules=}")
                    download_granules.extend(granules)

            if (len(download_batch) > max_bursts):
                logger.error(f"{len(download_batch)=} {max_bursts=}")
                logger.error(f"{download_batch=}")
                raise AssertionError("Something seriously went wrong matching up CSLC input granules!")

        logger.info(f"{len(download_granules)=}")

        return download_granules

    async def query_cmr(self, args, token, cmr, settings, timerange, now):

        # If we are in historical mode, we will query one frame worth at a time
        if self.proc_mode == "historical":

            if args.frame_range is None:
                raise AssertionError("Historical mode requires frame range to be specified.")

            granules = []
            frame_start, frame_end = self.args.frame_range.split(",")
            for frame in range(int(frame_start), int(frame_end) + 1):
                count, native_id = build_cslc_native_ids(frame, self.disp_burst_map_hist)
                if count == 0:
                    continue
                args.native_id = native_id # Note that the native_id is overwritten here. It doesn't get used after this point so this should be ok.
                new_granules = await async_query_cmr(args, token, cmr, settings, timerange, now)
                self.extend_additional_records(new_granules, no_duplicate=True, force_frame_id=frame)
                granules.extend(new_granules)

        # If we are in reprocessing mode, we will expand the native_id to
        # include all bursts in the frame to which this granule belongs. And then restrict by the acquisition date
        # We need to go back 12 days * (k -1) to cover the acquisition date range
        elif self.proc_mode == "reprocessing":
            burst_id, acquisition_dts, acquisition_cycle, frame_ids = parse_cslc_native_id(args.native_id, self.burst_to_frame)
            frame_id = min(frame_ids) # In case of this burst belonging to two frames, pick the lower frame id
            acquisition_time = datetime.strptime(acquisition_dts, "%Y%m%dT%H%M%SZ") # 20231006T183321Z
            start_date = (acquisition_time - (args.k - 1) * timedelta(days=12) - timedelta(days=1)).strftime(CMR_TIME_FORMAT)
            end_date = (acquisition_time + timedelta(days=1)).strftime(CMR_TIME_FORMAT)
            timerange = DateTimeRange(start_date, end_date)
            args.use_temporal = True
            logger.info(f"Querying CMR for frame {frame_id}")
            l, native_id = build_cslc_native_ids(frame_id, self.disp_burst_map)
            args.native_id = native_id  # Note that the native_id is overwritten here. It doesn't get used after this point so this should be ok.
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)

            # Remove granules that don't belong to the frame
            for g in granules:
                _, _, _, f_ids_local = parse_cslc_native_id(g["granule_id"], self.burst_to_frame)
                if frame_id not in f_ids_local:
                    granules.remove(g)

            self.extend_additional_records(granules, no_duplicate=True, force_frame_id=frame_id)

        else:
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)
            self.extend_additional_records(granules)

        return granules

    def eliminate_duplicate_granules(self, granules):
        """For CSLC granules revision_id is always one. Instead, we correlate the granules by the unique_id
        which is a function of download_batch_id and burst_id"""
        granule_dict = {}
        for granule in granules:
            unique_id = granule["unique_id"]
            if unique_id in granule_dict:
                if granule["granule_id"] > granule_dict[unique_id]["granule_id"]:
                    granule_dict[unique_id] = granule
            else:
                granule_dict[unique_id] = granule
        granules = list(granule_dict.values())

        return granules

    def get_download_chunks(self, batch_id_to_urls_map):
        '''For CSLC chunks we must group them by frame id'''
        chunk_map = defaultdict(list)
        for batch_chunk in batch_id_to_urls_map.items():
            frame_id, _ = split_download_batch_id(batch_chunk[0])
            chunk_map[frame_id].append(batch_chunk)
            if (len(chunk_map[frame_id]) > self.args.k):
                raise AssertionError("Number of download batches is greater than K. This should not be possible!")
            #print("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[")
            #print(frame_id, batch_chunk[0])
        return chunk_map.values()

    async def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")
