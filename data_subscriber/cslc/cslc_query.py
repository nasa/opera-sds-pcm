import logging
import re
import copy
from data_subscriber.cmr import async_query_cmr
from data_subscriber.cslc_utils import localize_disp_frame_burst_json, build_cslc_native_ids, \
    process_disp_frame_burst_json, download_batch_id_forward_reproc, download_batch_id_hist
from data_subscriber.query import CmrQuery
from data_subscriber.rtc.rtc_query import MISSION_EPOCH_S1A, MISSION_EPOCH_S1B, determine_acquisition_cycle
from util import datasets_json_util

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings, disp_frame_burst_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_frame_burst_file is None:
            self.disp_burst_map, self.burst_to_frame, metadata, version = localize_disp_frame_burst_json()
        else:
            self.disp_burst_map, self.burst_to_frame, metadata, version = process_disp_frame_burst_json(disp_frame_burst_file)

    def extend_additional_records(self, granules):
        """Extend the granules with potentially additional records if a burst belongs to two frames.
        This only applies to forward  and re-processing modes.
        Also adds frame_id, burst_id, and acquisition_cycle to metadata."""

        if self.proc_mode not in ["forward", "reprocessing"]:
            return

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
            granule["acquisition_cycle"] = acquisition_cycle
            granule["burst_id"] = burst_id

            frame_ids = self.burst_to_frame[burst_id]
            granule["frame_id"] = self.burst_to_frame[burst_id][0]

            assert len(frame_ids) <= 2  # A burst can belong to at most two frames. If it doesn't, we have a problem.

            # If this burst belongs to two frames, make a deep copy of the granule and append to the list
            if len(frame_ids) == 2:
                new_granule = copy.deepcopy(granule)
                new_granule["frame_id"] = self.burst_to_frame[burst_id][1]
                extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):
        """For CSLC this is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        if self.proc_mode == "historical":
            download_batch_id = download_batch_id_hist(args)
        else: # forward or reprocessing
            download_batch_id = download_batch_id_forward_reproc(granule)

        # Additional fields are lost after writing to ES so better to keep this in the granule
        granule["download_batch_id"] = download_batch_id

        # download_batch_id also needs to be added to the additional_fields so that it'll be written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        additional_fields["download_batch_id"] = download_batch_id
        return additional_fields

    def determine_download_granules(self, granules):
        """Combine these new granules with existing unsubmitted granules to determine which granules to download.
        This only applies to forward processing mode."""

        # TODO: This is quick HACK to test a basic functionality
        return granules

        if self.proc_mode != "forward":
            return granules

        # Get unsubmitted granules and group by download_batch_id
        unsubmitted = self.es_conn.get_unsubmitted_granules()

        # TODO: For each unsubmitted see if any were ever submmitted. If so, we will submit again with the new addtional granules.


    async def query_cmr(self, args, token, cmr, settings, timerange, now):

        # If we are in historical mode, we will query one frame worth at a time
        if self.proc_mode == "historical":

            if args.frame_range is None:
                raise AssertionError("Historical and reprocessing modes require frame range to be specified.")

            granules = []

            frame_start, frame_end = self.args.frame_range.split(",")
            for frame in range(int(frame_start), int(frame_end) + 1):
                native_id = build_cslc_native_ids(frame, self.disp_burst_map)
                args.native_id = native_id # Note that the native_id is overwritten here. It doesn't get used after this point so this should be ok.
                granules.extend(await async_query_cmr(args, token, cmr, settings, timerange, now))

        else:
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)

        return granules

    async def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")