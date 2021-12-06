#!/usr/bin/env python
"""
Script that implements backoff when attempting to update the ASGs

author: mcayanan
"""

import boto3
import logging
import os
import json
import argparse
import backoff

# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'):
            record.id = '--'
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

BACKOFF_CONF = {}  # back-off configuration

VALID_KEYS = [
    "MixedInstancesPolicy"
]


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def update_capacity(asg_name, desired_capacity):
    try:
        client = boto3.client("autoscaling")
        response = client.update_auto_scaling_group(AutoScalingGroupName=asg_name,
                                                    DesiredCapacity=desired_capacity)
        logger.info("Successfully updated {} to {}: {}".format(asg_name, desired_capacity, json.dumps(response)))
    except Exception as e:
        msg = "ERROR: Error occurred while trying to update {}: {}".format(asg_name, str(e))
        logger.error(msg)
        raise RuntimeError(msg)


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def process_input_json(asg_name, input_json):
    try:
        client = boto3.client("autoscaling")
        logger.info("Updating {} with the following: {}".format(asg_name, json.dumps(input_json, indent=2)))
        response = client.update_auto_scaling_group(AutoScalingGroupName=asg_name,
                                                    MixedInstancesPolicy=input_json["MixedInstancesPolicy"])
        logger.info("Successfully updated {}: {}".format(asg_name, json.dumps(response, indent=2)))
    except Exception as e:
        msg = "ERROR: Error occurred while trying to update {}: {}".format(asg_name, str(e))
        logger.error(msg)
        raise RuntimeError(msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("asg", help="Auto Scaling Group Name")
    parser.add_argument("--desired-capacity", type=int, help="Desired Capacity")
    parser.add_argument("--cli-input-json", help="CLI input json")
    parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
    parser.add_argument("--max_time", type=int, default=15, help="maximum total time")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    if args.desired_capacity:
        update_capacity(args.asg, args.desired_capacity)

    if args.cli_input_json:
        with open(args.cli_input_json, "r") as f:
            json_data = json.load(f)
            for key in json_data.keys():
                if key not in VALID_KEYS:
                    raise RuntimeError("{} is not yet supported in this script".format(key))
            process_input_json(args.asg, json_data)
