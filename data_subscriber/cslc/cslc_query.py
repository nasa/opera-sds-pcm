import logging
from data_subscriber.cmr import async_query_cmr
from data_subscriber.cslc_utils import localize_disp_frame_burst_json, build_cslc_native_ids
from data_subscriber.query import CmrQuery

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        self.disp_burst_map, metadata, version = localize_disp_frame_burst_json()

    async def query_cmr(self, args, token, cmr, settings, timerange, now):

        # Use underscore instead of other special characters and lower case so that it can be used in ES TERM search
        download_batch_id = args.start_date + "_" + args.end_date
        if args.frame_range is not None:
            download_batch_id = download_batch_id + "_" + args.frame_range.split(",")[0]
        download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

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

            return granules, download_batch_id

        else:
            granules = await async_query_cmr(args, token, cmr, settings, timerange, now)
            return granules, download_batch_id
