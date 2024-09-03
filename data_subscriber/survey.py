import asyncio
import logging
from datetime import datetime, timedelta

import backoff

from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT
from data_subscriber.query import get_query_timerange, DateTimeRange
from data_subscriber.cslc.cslc_query import CslcCmrQuery
import cslc_utils

_date_format_str = CMR_TIME_FORMAT
_date_format_str_cmr = _date_format_str[:-1] + ".%fZ"


@backoff.on_exception(backoff.expo, Exception, max_value=13)
def _query_cmr_backoff(args, token, cmr, settings, query_timerange, now, cslc_query = None, silent=True):
    if cslc_query is not None:
        result = cslc_query.query_cmr_by_frame_and_dates(args, token, cmr, settings, query_timerange, now, silent)
    else:
        result = asyncio.run(async_query_cmr(args, token, cmr, settings, query_timerange, now, silent))
    return result


def run_survey(args, token, cmr, settings):

    start_dt = datetime.strptime(args.start_date, _date_format_str)
    end_dt = datetime.strptime(args.end_date, _date_format_str)

    out_csv = open(args.out_csv, 'w')
    out_csv.write("# DateTime Range:" + start_dt.strftime(_date_format_str) + " to " + end_dt.strftime(
        _date_format_str) + '\n')

    raw_csv = open(args.out_csv+".raw.csv", 'w')
    raw_csv.write("# Granule ID, Revision Time, Temporal Time, Revision-Temporal Delta Hours, revision-id \n")

    total_granules = 0

    all_granules = {}
    all_deltas = []

    cslc_query = None
    if args.frame_id is not None:
        logging.info("Querying for DISP-S1 frame_id only: " + str(args.frame_id))
        cslc_query = CslcCmrQuery(args, token, None, cmr, None, settings,
                                          cslc_utils.DISP_FRAME_BURST_MAP_HIST)

    while start_dt < end_dt:

        now = datetime.utcnow()
        step_time = timedelta(hours=float(args.step_hours))
        incre_time = step_time - timedelta(seconds=1)

        start_str = start_dt.strftime(_date_format_str)
        end_str = (start_dt + incre_time).strftime(_date_format_str)
        args.start_date = start_str
        args.end_date = end_str

        query_timerange: DateTimeRange = get_query_timerange(args, now, silent=True)

        granules = _query_cmr_backoff(args, token, cmr, settings, query_timerange, now, cslc_query, silent=True)

        count = 0
        for granule in granules:
            g_id = granule['granule_id']
            g_rd = granule['revision_date']
            g_td = granule['temporal_extent_beginning_datetime']
            r_id = str(granule['revision_id'])
            g_rd_dt = datetime.strptime(g_rd, _date_format_str_cmr)
            g_td_dt = datetime.strptime(g_td, _date_format_str)
            update_temporal_delta = g_rd_dt - g_td_dt
            update_temporal_delta_hrs = update_temporal_delta.total_seconds() / 3600
            logging.debug(f"{g_id}, {g_rd}, {g_td}, delta: {update_temporal_delta_hrs} hrs")
            if (g_id in all_granules):
                (og_rd, og_td, _, _) = all_granules[g_id]
                logging.warning(f"{g_id} had already been found {og_rd=} {og_td=}")
            else:
                raw_csv.write(g_id+", "+g_rd+", "+g_td+", "+"%10.2f" % update_temporal_delta_hrs+", "+r_id+"\n")
                all_granules[g_id] = (g_rd, g_td, update_temporal_delta_hrs, r_id)
                all_deltas.append(update_temporal_delta_hrs)
                count += 1

        total_granules += count

        out_csv.write(start_str)
        out_csv.write(',')
        out_csv.write(end_str)
        out_csv.write(',')
        out_csv.write(str(count))
        out_csv.write('\n')

        logging.info(f"{start_str},{end_str},{str(count)}")

        start_dt = start_dt + step_time


    total_g_str = "Total granules found: " + str(total_granules)
    print(f"{len(all_granules)=}")
    logging.info(total_g_str)
    out_csv.write(total_g_str)
    out_csv.close()

    raw_csv.close()

    logging.info(f"Output CSV written out to files: {args.out_csv}, {args.out_csv}.raw.csv" )

    hist_title = f"Histogram of Revision vs Temporal Time for all granules"
    logging.info(hist_title)
    import matplotlib.pyplot as plt
    _ = plt.hist(all_deltas, bins=50)
    #print(hist_e)
    #print(hist_v)
    plt.title(hist_title)
    logging.info("Saving histogram figure as " + args.out_csv+".svg")
    plt.savefig(args.out_csv+".svg", format="svg", dpi=1200)
    plt.show()

