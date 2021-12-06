#!/usr/bin/env python
from __future__ import print_function
import argparse
import os
import time
from datetime import timedelta, datetime
import logging
import math

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
signal_file_bucket = "opera-dev-isl-fwd-mkarim"
event_misfire_delay_threshold_second = 60  # int(os.environ["DELAY_THRESHOLD"])
MAX_TRY = 12

'''
Function to get Metric Data from Cloud Watch Metrics
'''


def get_metric_data(event_misfire_metric_name):
    import boto3

    now = datetime.now()
    now_plus_10 = now + timedelta(minutes=10)
    now_minus_10 = now + timedelta(minutes=-10)

    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'string',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/Lambda',
                        'MetricName': 'NumberOfMissedFiles',
                        'Dimensions': [
                            {
                                'Name': 'LAMBDA_NAME',
                                'Value': 'event-misfire_lambda'
                            },
                            {
                                'Name': 'E_MISFIRE_METRIC_ALARM_NAME',
                                'Value': event_misfire_metric_name
                            },
                        ]
                    },
                    "Unit": "Count",
                    "Stat": "Average",
                    "Period": 3600
                },
                'ReturnData': True
                # 'Period': 123

            },
        ],
        StartTime=now_minus_10,
        EndTime=now_plus_10,
    )
    return response


'''
Function to get Metric Alarm Data from Cloud Watch Metrics
'''


def get_metric_alarm_data(event_misfire_metric_name):
    import boto3

    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.describe_alarms_for_metric(
        MetricName='NumberOfMissedFiles',
        Namespace='AWS/Lambda',
        Dimensions=[
            {
                'Name': 'LAMBDA_NAME',
                'Value': 'event-misfire_lambda'
            },
            {
                'Name': 'E_MISFIRE_METRIC_ALARM_NAME',
                'Value': event_misfire_metric_name
            }
        ]
    )

    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metric_name", help="Event Misfire Metric Name")
    parser.add_argument("res_file", help="result file")
    args = parser.parse_args()

    passed = False
    missed_file_count = 0
    isAlarm = False

    count = 0
    while count < MAX_TRY:
        try:
            metric_data = get_metric_data(args.metric_name)['MetricDataResults'][0]
            print(metric_data)
            missed_file_count = math.ceil(metric_data["Values"][0])
            if missed_file_count > 0:
                break
        except Exception as e:
            logger.error(str(e))

        time.sleep(60)
        count = count + 1

    if missed_file_count > 0:
        count = 0
        while count < MAX_TRY:
            try:
                metric_alarm_state = get_metric_alarm_data(args.metric_name)['MetricAlarms'][0]['StateValue']
                print("metric_alarm_state : {}".format(metric_alarm_state))

                if metric_alarm_state.upper() == "ALARM":
                    passed = True
                    break
            except Exception as e:
                logger.error(str(e))

            time.sleep(60)
            count = count + 1

    with open(args.res_file, "w") as f:
        if passed:
            msg = "SUCCESS: Found expected Missed File Alarm : {}".format(missed_file_count)
        else:
            msg = "ERROR: Failed to find expected Missed File Alarm : {}".format(missed_file_count)
        logger.info(msg)
        f.write(msg)
