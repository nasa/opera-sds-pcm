from datetime import datetime, timedelta
import logging
from data_subscriber.query import get_query_timerange, query_cmr

_date_format_str = "%Y-%m-%dT%H:%M:%SZ"

def run_survey(args, token, cmr, settings):

    start_dt = datetime.strptime(args.start_date, _date_format_str)
    end_dt = datetime.strptime(args.end_date, _date_format_str)

    out_csv = open(args.out_csv, 'w')
    out_csv.write("# DateTime Range:" + start_dt.strftime("%Y-%m-%dT%H:%M:%SZ") + " to " + end_dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ") + '\n')

    total_granules = 0

    while start_dt < end_dt:

        now = datetime.utcnow()
        step_time = timedelta(hours=args.step_hours)

        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = (start_dt + step_time).strftime("%Y-%m-%dT%H:%M:%SZ")
        args.start_date = start_str
        args.end_date = end_str

        query_timerange: DateTimeRange = get_query_timerange(args, now, silent=True)

        granules = query_cmr(args, token, cmr, settings, query_timerange, now, silent=True)

        count = len(granules)
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
    logging.info(total_g_str)
    out_csv.write(total_g_str)
    out_csv.close()

    logging.info("Output CSV written out to file: " + str(args.out_csv))