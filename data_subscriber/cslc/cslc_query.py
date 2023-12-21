import logging
from data_subscriber.cmr import async_query_cmr
from data_subscriber.cslc_utils import localize_disp_frame_burst_json, expand_clsc_frames
from data_subscriber.query import CmrQuery

logger = logging.getLogger(__name__)

class CslcCmrQuery(CmrQuery):

    async def query_cmr(self, args, token, cmr, settings, timerange, now):

        # If we are querying CSLC data we need to modify parameters going into cmr query
        # TODO: put this in a loop and query one frame at a time
        if args.frame_range is not None:
            disp_burst_map, metadata, version = localize_disp_frame_burst_json()

            # TODO: If we process more than one frame in a single query, we need to restructure this.
            # Use underscore instead of other special characters and lower case so that it can be used in ES term search
            download_batch_id = args.start_date + "_" + args.end_date + "_" + args.frame_range.split(",")[0]
            download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

            if expand_clsc_frames(args, disp_burst_map) == False:
                logging.info("No valid frames were found.")
                return None

        granules = await async_query_cmr(args, token, cmr, settings, timerange, now)

        return granules, download_batch_id
