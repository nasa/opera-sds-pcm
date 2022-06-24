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
    parser.add_argument("-m", "--mode", dest="mode", type=str, required=True,
                             help="'full', 'query', or 'download' See daac_data_subscriber help for more info.")
    parser.add_argument("-s", "--start-date", dest="start_date", default=False,
                             help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z")
    parser.add_argument("-jp", "--job-period", dest="job_period", type=int, default=60,
                             help="How often, in minutes, should the cron job run (default: 60 minutes).")
    parser.add_argument("-jc", "--job-count", dest="job_count", type=int, required=True,
                             help="How many cron jobs to run.")
    parser.add_argument("-mult", "--multiplier", dest="multiplier", type=float, default=1.0,
                             help="Floating point multiplier factor for the time window. Each line item in the crontab runs every hour. If the multiplier is 1, each cronjob will cover 1 hour worth of data. If it's 0.5, it will cover 30 mins-worth. If it's 2 it will cover 2-hr worth. There is no overlap in data being fetched; the time-covered for the data will span NUM_HOURS * MULTIPLIER (default: 1.0).")
    parser.add_argument("-pass", "--pass-through", dest="pass_through", type=str, required=False,
                             help="Options to pass through to the daac_data_subscriber script directly")
    parser.add_argument("-o", "--output-file", dest="output_file", type=str, required=True,
                             help="Location of output file to send stdout to")      

    return parser


#TODO:
# Carry over other optional parameters into the daac_data_subscriber code

_python_and_subs = '/export/home/hysdsops/mozart/bin/python /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py'

parser = create_parser()
args = parser.parse_args()

date_format_str = "%Y-%m-%dT%H:%M:%SZ"
increment_mins = args.job_period * args.multiplier

start_dt = datetime.strptime(args.start_date, date_format_str)
job_dt = datetime.now() + timedelta(minutes=2) # Put two minute delay in the cron start so that you have some time between generation and running. Could make this a parameter.
output_file_str = args.output_file
for i in range(0, args.job_count): 
    start = start_dt + timedelta(minutes=i*increment_mins)
    start_str = start.strftime(date_format_str)
    stop = start_dt + timedelta(minutes=(i+1)*increment_mins)
    stop_str = stop.strftime(date_format_str)
    cron = f"{job_dt.minute} {job_dt.hour} {job_dt.day} {job_dt.month} *"
    print(f" {cron} (time {_python_and_subs} {args.mode} -s {start_str} -e {stop_str} {args.pass_through}) >> {output_file_str} 2>&1")

    job_dt = job_dt + timedelta(minutes = args.job_period)

