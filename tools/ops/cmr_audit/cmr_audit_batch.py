'''
Script to invoke cmr_audit-*.py N days at a time.
'''

import argparse
import time
from datetime import datetime, timedelta
import sys
import logging
import subprocess
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# wait time in between cmr_audit.py invocations
SLEEP_TIME_SECS = 60


def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument(
        "--start-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--input_product",
        help=f'The input product: HLS or SLC',
        default='SLC'
    ),
    argparser.add_argument(
        "--interval-days",
        help=f'The maximum number of days to query at a time',
        type=int,
        default=10
    )

    return argparser


if __name__ == "__main__":

    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])
    logging.info(args)

    # loop over N days at a time
    start_datetime = datetime.strptime(args.start_datetime, "%Y-%m-%dT%H:%M:%SZ")
    end_datetime = datetime.strptime(args.end_datetime, "%Y-%m-%dT%H:%M:%SZ")

    while start_datetime < end_datetime:
        stop_datetime = start_datetime + timedelta(days=args.interval_days)
        if stop_datetime > end_datetime:
            stop_datetime = end_datetime

        # start_datime ==> stop_datetime
        logging.info(f"Processing from: {start_datetime} to: {stop_datetime}")
        if args.input_product == 'HLS':
            script = 'cmr_audit_hls.py'
        elif args.input_product == 'SLC':
            script = 'cmr_audit_slc.py'
        else:
            logging.error(f"Invalid input product: {args.input_product}")
            exit(1)

        subprocess.run(['python', script,
                        "--start-datetime",
                        start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "--end-datetime",
                        stop_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")])

        start_datetime = stop_datetime
        time.sleep(SLEEP_TIME_SECS)





