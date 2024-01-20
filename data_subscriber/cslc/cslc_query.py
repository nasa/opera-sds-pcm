import logging
import re
import copy
from datetime import datetime, timedelta
from data_subscriber.cmr import async_query_cmr
from data_subscriber.cslc_utils import localize_disp_frame_burst_json, build_cslc_native_ids, \
    process_disp_frame_burst_json, download_batch_id_forward_reproc, download_batch_id_hist, split_download_batch_id
from data_subscriber.query import CmrQuery, DateTimeRange
from data_subscriber.rtc.rtc_query import MISSION_EPOCH_S1A, MISSION_EPOCH_S1B, determine_acquisition_cycle
from util import datasets_json_util
from collections import defaultdict

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings, disp_frame_burst_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_frame_burst_file is None:
            self.disp_burst_map, self.burst_to_frame, metadata, version = localize_disp_frame_burst_json()
        else:
            self.disp_burst_map, self.burst_to_frame, metadata, version = process_disp_frame_burst_json(disp_frame_burst_file)

    def extend_additional_records(self, granules, no_duplicate=False, force_frame_id = None):
        """Add frame_id, burst_id, and acquisition_cycle to all granules.
        In forward  and re-processing modes, extend the granules with potentially additional records
        if a burst belongs to two frames."""

        dataset_json = datasets_json_util.DatasetsJson()
        cslc_granule_regex = dataset_json.get("L2_CSLC_S1")["match_pattern"]

        extended_granules = []
        for granule in granules:
            granule_id = granule["granule_id"]
            match_product_id = re.match(cslc_granule_regex, granule_id)
            burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3
            acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z

            # Determine acquisition cycle
            instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B
            acquisition_cycle, _ = determine_acquisition_cycle(burst_id, acquisition_dts, instrument_epoch)
            granule["acquisition_ts"] = acquisition_dts
            granule["acquisition_cycle"] = acquisition_cycle
            granule["burst_id"] = burst_id

            frame_ids = self.burst_to_frame[burst_id]
            granule["frame_id"] = self.burst_to_frame[burst_id][0] if force_frame_id is None else force_frame_id
            granule["download_batch_id"] = download_batch_id_forward_reproc(granule)
            granule["unique_id"] = granule["download_batch_id"] + "_" + granule["burst_id"]

            assert len(frame_ids) <= 2  # A burst can belong to at most two frames. If it doesn't, we have a problem.

            if self.proc_mode not in ["forward", "reprocessing"] or no_duplicate:
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

        #TODO: There's no need to use different methods for historical. We can use the median date and then derive acquisition cycle from that.
        if self.proc_mode == "historical":
            download_batch_id = download_batch_id_hist(args)
        else: # forward or reprocessing
            download_batch_id = download_batch_id_forward_reproc(granule)

        # Additional fields are lost after writing to ES so better to keep this in the granule
        granule["download_batch_id"] = download_batch_id

        # download_batch_id also needs to be added to the additional_fields so that it'll be written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        additional_fields["burst_id"] = granule["burst_id"]
        additional_fields["frame_id"] = granule["frame_id"]
        additional_fields["acquisition_ts"] = granule["acquisition_ts"]
        additional_fields["acquisition_cycle"] = granule["acquisition_cycle"]
        additional_fields["unique_id"] = granule["unique_id"]
        additional_fields["download_batch_id"] = download_batch_id

        return additional_fields

    async def determine_download_granules(self, granules):
        """Combine these new granules with existing unsubmitted granules to determine which granules to download.
        This only applies to forward processing mode. Valid only in forward processing mode."""

        if self.proc_mode != "forward":
            return granules

        # Get unsubmitted granules, which are ES records without download_job_id fields
        unsubmitted = self.es_conn.get_unsubmitted_granules()

        logger.info(f"{len(granules)=}")
        logger.info(f"{len(unsubmitted)=}")

        # Group all granules by download_batch_id
        # If the same download_batch_id is in both granules and unsubmitted, we will use the one in granules because it's newer
        # unique_id is mapped into id in ES
        by_download_batch_id = defaultdict(lambda: defaultdict(dict))
        for granule in granules:
            by_download_batch_id[granule["download_batch_id"]][granule["unique_id"]] = granule
        for granule in unsubmitted:
            download_batch = by_download_batch_id[granule["download_batch_id"]]
            if granule["id"] not in download_batch:
                download_batch[granule["id"]] = granule

        # Combine unsubmitted and new granules and determine which granules meet the criteria for download
        # Rule #1: If all granules for a given download_batch_id are present, download all granules for that batch
        # TODO Rule #2: If it's been xxx hrs since last granule discovery (by OPERA) and xx% are available, download all granules for that batch
        # TODO Rule #3: If granules have been downloaded already but with less than 100% and we have new granules for that batch, download all granules for that batch
        download_granules = []
        for batch_id, download_batch in by_download_batch_id.items():
            logger.info(f"{batch_id=} {len(download_batch)=}")
            frame_id, acquisition_cycle = split_download_batch_id(batch_id)
            max_bursts = len(self.disp_burst_map[frame_id].burst_ids)

            # Rule #1: If all granules for a given download_batch_id are present, download all granules for that batch
            if len(download_batch) == max_bursts:
                logger.info(f"Download all granules for {batch_id}")
                for download in download_batch.values():
                    download_granules.append(download)

                # Retrieve K- granules and M- compressed CSLCs for this batch
                # Go back K- 12-day windows and find the same frame
                logger.info(f"Retrieving K-1 granules")
                for i in range(self.args.k - 1):
                    args = self.args

                    # Add native-id condition in args
                    native_id = build_cslc_native_ids(frame_id, self.disp_burst_map)
                    args.native_id = native_id
                    logger.info(f"{args.native_id=}")

                    #TODO: We can only use past frames which contain the exact same bursts as the current frame
                    # If not, we will need to go back another cycle until, as long as we have to, we find one that does

                    # Move start and end date of args back by 12 * (i + 1) days, and then expand 10 days to cast a wide net
                    start_date = (datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M:%SZ") - timedelta(
                        days=12 * (i + 1) + 5)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    end_date = (datetime.strptime(args.end_date, "%Y-%m-%dT%H:%M:%SZ") - timedelta(
                        days=12 * (i + 1) - 5)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    query_timerange = DateTimeRange(start_date, end_date)
                    logger.info(f"{query_timerange=}")
                    granules = await self.query_cmr(args, self.token, self.cmr, self.settings, query_timerange, datetime.utcnow())

                    # This step is a bit tricky.
                    # 1) We want exactly one frame worth of granules do don't create additional granules if the burst belongs to two frames
                    # 2) We already know what frame these new granules belong to because that's what we queried for. We need to
                    #    force using that because 1/9 times one burst will belong to two frames
                    self.extend_additional_records(granules, no_duplicate=True, force_frame_id=frame_id)

                    granules = self.eliminate_duplicate_granules(granules)
                    self.catalog_granules(granules, datetime.now())
                    print(f"{len(granules)=}")
                    #print(f"{granules=}")
                    download_granules.extend(granules)

            if (len(download_batch) > max_bursts):
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
                native_id = build_cslc_native_ids(frame, self.disp_burst_map)
                args.native_id = native_id # Note that the native_id is overwritten here. It doesn't get used after this point so this should be ok.
                granules.extend(await async_query_cmr(args, token, cmr, settings, timerange, now))

        else:
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)

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


    async def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")