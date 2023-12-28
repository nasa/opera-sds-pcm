import logging
import re
import copy
from data_subscriber.cmr import async_query_cmr
from data_subscriber.cslc_utils import localize_disp_frame_burst_json, build_cslc_native_ids, process_disp_frame_burst_json
from data_subscriber.query import CmrQuery
from data_subscriber.rtc.rtc_query import MISSION_EPOCH_S1A, MISSION_EPOCH_S1B, determine_acquisition_cycle

logger = logging.getLogger(__name__)

cslc_granule_regex = (
    r'(?P<id>'
    r'(?P<project>OPERA)_'
    r'(?P<level>L2)_'
    r'(?P<product_type>CSLC)-'
    r'(?P<source>S1)_'
    r'(?P<burst_id>\w{4}-\w{6}-\w{3})_'
    r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    r'(?P<creation_ts>(?P<cre_year>\d{4})(?P<cre_month>\d{2})(?P<cre_day>\d{2})T(?P<cre_hour>\d{2})(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_'
    r'(?P<sensor>S1A|S1B)_'
    r'(?P<phase>VV|HH)_'
    r'(?P<product_version>v\d+[.]\d+)'
    r')'
)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings, disp_frame_burst_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_frame_burst_file is None:
            self.disp_burst_map, self.burst_to_frame, metadata, version = localize_disp_frame_burst_json()
        else:
            self.disp_burst_map, self.burst_to_frame, metadata, version = process_disp_frame_burst_json(disp_frame_burst_file)

    def extend_additional_records(self, granules):
        """Extend the granules with potentially additional records if a burst belongs to two frames.
        This only applies to forward processing mode.
        Also adds frame_id, burst_id, and acquisition_cycle to metadata."""

        if self.proc_mode != "forward":
            return

        extended_granules = []
        for granule in granules:
            granule_id = granule["granule_id"]
            match_product_id = re.match(cslc_granule_regex, granule_id)
            burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3
            acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z

            # Determine acquisition cycle
            instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B
            acquisition_cycle = determine_acquisition_cycle(burst_id, acquisition_dts, instrument_epoch)
            granule["acquisition_cycle"] = acquisition_cycle
            granule["burst_id"] = burst_id

            frame_ids = self.burst_to_frame[burst_id]
            granule["frame_id"] = self.burst_to_frame[burst_id][0]

            # If this burst belongs to two frames, make a deep copy of the granule and append to the list
            if len(frame_ids) == 2:
                new_granule = copy.deepcopy(granule)
                new_granule["frame_id"] = self.burst_to_frame[burst_id][0]
                extended_granules.append(new_granule)

        granules.extend(extended_granules)

    def prepare_additional_fields(self, granule, args, granule_id):

        additional_fields = super().prepare_additional_fields(granule, args, granule_id)

        # Use underscore instead of other special characters and lower case so that it can be used in ES TERM search
        download_batch_id = args.start_date + "_" + args.end_date
        if args.frame_range is not None:
            download_batch_id = download_batch_id + "_" + args.frame_range.split(",")[0]
        download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

        if download_batch_id is not None:
            additional_fields["download_batch_id"] = download_batch_id

        return additional_fields

    def determine_download_granules(self, granules):
        """Combine these new granules with existing unsubmitted granules to determine which granules to download.
        This only applies to forward processing mode."""

        if self.proc_mode != "forward":
            return granules

        # Get unsubmitted granules and group by download_batch_id
        unsubmitted = self.es_conn.get_unsubmitted_granules()

        # TODO: For each unsubmitted see if any were ever submmitted. If so, we will submit again with the new addtional granules.


    async def query_cmr(self, args, token, cmr, settings, timerange, now):

        # If we are in historical mode, we will query one frame worth at a time
        if self.proc_mode in ["historical", "reprocessing"]:

            if args.frame_range is None:
                raise AssertionError("Historical and reprocessing modes require frame range to be specified.")

            granules = []

            frame_start, frame_end = self.args.frame_range.split(",")
            for frame in range(int(frame_start), int(frame_end) + 1):
                native_ids = build_cslc_native_ids(frame, self.disp_burst_map)
                args.native_ids = native_ids # Note that the native_id is overwritten here. It doesn't get used after this point so this should be ok.
                granules.extend(await async_query_cmr(args, token, cmr, settings, timerange, now))

        else:
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)

        return granules

    async def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")