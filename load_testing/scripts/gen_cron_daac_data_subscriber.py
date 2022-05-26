#!/usr/bin/env python3

import sys
from datetime import datetime, timedelta
import time
import argparse

#sys.path.insert(1, '../../data_subscriber')
# We should reuse the parser creation code from daac_data_subcriber module properly but just copying-pasting for now
#from daac_data_subscriber import create_parser

'''Example Usage:

./gen_cron_daac_data_subscriber.py -c HLSL30 -s opera-dev-isl-fwd-pyoon -jc 10 -sd 2022-01-01T00:00:00Z -mult=10

'''

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Verbose mode.")

    parser.add_argument("-c", "--collection", dest="collection", required=True,
                             help="The collection for which you want to retrieve data.")
    parser.add_argument("-sd", "--start-date", dest="start_date", default=False,
                             help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z")
    parser.add_argument("-b", "--bounds", dest="bbox", 
                             help="The bounding rectangle to filter result in. Format is W Longitude,S Latitude,E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from the command line. Default: \"-180,-90,180,90\".")
    parser.add_argument("-jp", "--job-period", dest="job_period", type=int, default=60,
                             help="How often, in minutes, should the cron job run (default: 60 minutes).")
    parser.add_argument("-jc", "--job-count", dest="job_count", type=int, required=True,
                             help="How many cron jobs to run.")
    parser.add_argument("-mult", "--multiplier", dest="multiplier", type=float, default=1.0,
                             help="Floating point multiplier factor for the time window. Each line item in the crontab runs every hour. If the multiplier is 1, each cronjob will cover 1 hour worth of data. If it's 0.5, it will cover 30 mins-worth. If it's 2 it will cover 2-hr worth. There is no overlap in data being fetched; the time-covered for the data will span NUM_HOURS * MULTIPLIER (default: 1.0).")
    parser.add_argument("-p", "--provider", dest="provider", default='LPCLOUD',
                             help="Specify a provider for collection search. Default is LPCLOUD.")
    parser.add_argument("-s", "--s3bucket", dest="s3_bucket", required=True,
                             help="The s3 bucket where data products will be downloaded.")
    parser.add_argument("-x", "--transfer-protocol", dest="transfer_protocol", default='s3',
                             help="The protocol used for retrieving data, HTTPS or default of S3")

    return parser


#TODO:
# Carry over other optional parameters into the daac_data_subscriber code

_python_and_subs = '/export/home/hysdsops/mozart/bin/python /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py'

parser = create_parser()
args = parser.parse_args()

date_format_str = "%Y-%m-%dT%H:%M:%SZ"
increment_mins = args.job_period * args.multiplier

start_dt = datetime.strptime(args.start_date, date_format_str)
job_dt = datetime.now() + timedelta(minutes=1)
for i in range(0, args.job_count):
    start = start_dt + timedelta(minutes=i*increment_mins)
    start_str = start.strftime(date_format_str)
    stop = start_dt + timedelta(minutes=(i+1)*increment_mins)
    stop_str = stop.strftime(date_format_str)
    cron = f"{job_dt.minute} {job_dt.hour} {job_dt.day} {job_dt.month} *"
    print(f" {cron} {_python_and_subs} -sd {start_str} -ed {stop_str} -c {args.collection} -s {args.s3_bucket}")

    job_dt = job_dt + timedelta(minutes = args.job_period)


