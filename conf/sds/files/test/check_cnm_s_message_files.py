#!/usr/bin/env python
"""Verification and validation of end-to-end integration test."""

import os
import json
import backoff
import boto3
import argparse
import re
import logging

#from swot_pcm.commons.logger import logger

log_format = (
    "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"  # set logger
)
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())



BACKOFF_CONF = {}  # back-off configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


def __get_dataset(key, datasets_cfg):
    dataset = None
    for ds in datasets_cfg["datasets"]:
        dataset_regex = re.compile(ds["match_pattern"])
        file_name = os.path.basename(key)
        dataset_name = file_name.split(".tar")[0]
        match = dataset_regex.search("/{}".format(dataset_name))
        if match:
            group_dict = match.groupdict()
            ipath = ds["ipath"].format(**group_dict)
            dataset = ipath.split("hysds::data/", 1)[1]
            break

    return dataset


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def check_scnm_s_messages(bucket, prefix, expected_datasets, datasets_cfg, output_file):
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucket, Prefix=prefix)
    discovered_datasets = dict()
    all_found = True
    unmatched_msg = ""
    if 'Contents' in response:
        total_products = 0
        for content in response['Contents']:
            logger.info("Reading contents of Bucket={}, Key={}".format(
                bucket, content['Key']))
            resp = s3.get_object(Bucket=bucket, Key=content['Key'])
            data = resp['Body'].read().decode('utf-8')
            scnm_s = json.loads(data)
            logger.info("Found {} product(s) in Bucket={}, Key={}".format(
                len(scnm_s['product']['files']), bucket, content['Key']))
            if len(scnm_s['product']['files']) == 0:
                raise RuntimeError("SCNM-S message is empty: Bucket={}, Key={}".format(bucket, content["Key"]))
            else:
                for prod_file in scnm_s['product']['files']:
                    uri = prod_file.get("uri")
                    uri = uri.split(".tar", 1)[0]
                    dataset_type = __get_dataset(uri, datasets_cfg)
                    if dataset_type is None:
                        raise RuntimeError("Could not determine type for {}".format(uri))
                    if dataset_type in discovered_datasets:
                        count = discovered_datasets[dataset_type]
                        discovered_datasets[dataset_type] = count + 1
                    else:
                        discovered_datasets[dataset_type] = 1

            total_products += len(scnm_s['product']['files'])

        logger.info("Number of products found in all SCNM-S messages in "
                    "Bucket={}, Prefix={}: {}".format(bucket, prefix,
                                                      total_products))
        logger.info("Discovered Datasets: {}".format(json.dumps(discovered_datasets, indent=2)))
        # Once all discovered datasets have been tallied up, compare against the expected count
        for expected_dataset in expected_datasets:
            ds = expected_dataset["dataset"]
            if ds.endswith("state-config"):
                continue
            expected_count = expected_dataset["count"]
            discovered_count = discovered_datasets[ds]
            msg = "{} count: expected={}, discovered={}".format(ds, expected_count, discovered_count)
            logger.info(msg)
            if expected_count != discovered_count:
                all_found = False
                unmatched_msg += "\n{}".format(msg)
        if all_found:
            logger.info("SUCCESS: Found all expected products in the SCNM-S messages.")
            pass
        else:
            raise RuntimeError(unmatched_msg)
    else:
        logger.info("No objects found in Bucket={}, Prefix={}: {}".format(
            bucket, prefix, response))
        raise RuntimeError
    with open(output_file, 'w') as f:
        if all_found is True:
            f.write("SUCCESS: All products found in the SCNM-S messages")
        else:
            f.write("ERROR: Not all products were found in the SCNM-S "
                    "messages:\n{}".format(unmatched_msg))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("osl_bucket", help="OSL bucket name.")
    parser.add_argument("prefix", help="The staging prefix where SCNM-S messages are located.")
    parser.add_argument("dataset_file", help="dataset json file containing the expected counts")
    parser.add_argument("data_segment", help="dataset segment(s)")
    parser.add_argument("res_file", help="result file")
    parser.add_argument("--datasets_cfg", help="The datasets configuration file", required=True)
    parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
    parser.add_argument("--max_time", type=int, default=1200, help="maximum total time")
    parser.add_argument("--overrides", required=False,
                        help="JSON file intended to override the original expected datasets "
                             "in the original datasets_e2e.json.")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    datasets_file = os.path.join(BASE_DIR, args.dataset_file)
    logger.info("datasets_file : {}".format(datasets_file))
    with open(datasets_file) as f:
        data = json.load(f)
    print(json.dumps(data, indent=4))
    segments = args.data_segment.split(",")

    if args.overrides:
        overrides_file = os.path.join(BASE_DIR, args.overrides)
        with open(overrides_file) as f:
            overrides = json.load(f)

        for segment in segments:
            if segment in overrides["datasets"]:
                logger.info("Overriding expected {} with {}".format(segment,
                                                                    json.dumps(overrides["datasets"][segment])))
                data["datasets"][segment] = overrides["datasets"][segment]

    expected_datasets = list()
    for segment in segments:
        for dataset in data["datasets"][segment]:
            expected_datasets.append(dataset)

    with open(args.datasets_cfg, "r") as f:
        ds_cfg = json.load(f)

    check_scnm_s_messages(args.osl_bucket, args.prefix, expected_datasets, ds_cfg, args.res_file)
