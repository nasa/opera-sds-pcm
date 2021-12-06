"""
This script will create an Accountability Report as a PCM job, upload it to the OSL, then
ingest it into the Rolling Storage.

"""

# !/usr/bin/env python
import os
import boto3

from util.ctx_util import JobContext
from util.common_util import convert_datetime
from util.conf_util import SettingsConf
from util.checksum_util import create_dataset_checksums
from util.exec_util import exec_wrapper

from report.accountability_report_cli import create_report, write_oad_report

from commons.logger import logger

from extractor import extract

DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@exec_wrapper
def create_accountability_report():
    ctx = JobContext("_context.json").ctx
    report_name = ctx.get("report_name")
    start_time = ctx.get("start_time")
    end_time = ctx.get("end_time")
    report_format = ctx.get("report_format")
    osl_bucket_name = ctx.get("osl_bucket_name")
    osl_staging_area = ctx.get("osl_staging_area")

    logger.info("Generating Accountability Report: report_name={}, start_time={}, end_time={}".format(report_name,
                                                                                                      start_time,
                                                                                                      end_time))
    response, metadata = create_report(report_format, start_time, end_time, "", report_name, "")
    report_file = write_oad_report(metadata, response.text, report_format)
    logger.info("Successfully generated report: {}".format(report_file))

    start_dt = convert_datetime(start_time, strformat=DATE_TIME_FORMAT)

    s3_object_name = "{}/{}/{}/{}".format(str(start_dt.year), str(start_dt.month).zfill(2), str(start_dt.day).zfill(2),
                                          os.path.basename(report_file))
    if osl_staging_area:
        s3_object_name = "{}/{}".format(osl_staging_area, s3_object_name)
    else:
        logger.info("No 'osl_staging_area' key specified.")

    s3_client = boto3.client("s3")
    logger.info("Uploading {} to s3://{}/{}".format(report_file, osl_bucket_name, s3_object_name))
    s3_client.upload_file(Filename=report_file, Bucket=osl_bucket_name, Key=s3_object_name)
    logger.info("Successfully uploaded {} to s3://{}/{}".format(report_file, osl_bucket_name, s3_object_name))

    logger.info("Creating dataset for {}".format(report_file))
    settings = SettingsConf().cfg
    try:
        output_dir = os.path.abspath("output")
        dataset_dir = extract.extract(report_file, settings["PRODUCT_TYPES"], output_dir)
        logger.info("Created dataset: {}".format(dataset_dir))
        create_dataset_checksums(
            os.path.join(dataset_dir, os.path.basename(report_file)), "md5"
        )
    except Exception as e:
        logger.error("Error occurred while trying to create a dataset for {}: {}".format(
            os.path.basename(report_file), str(e)))
        raise


if __name__ == "__main__":
    """
    Main program of job
    """
    create_accountability_report()
