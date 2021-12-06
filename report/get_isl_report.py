#!/usr/bin/env python

"""Creates an ISL report."""
import argparse
import sys
import traceback
import csv
import json
import time

from datetime import datetime
from collections import OrderedDict
from dateutil.rrule import rrule, DAILY

from commons.logger import logger, LogLevels
from commons.constants import product_metadata as pm

from elasticsearch.exceptions import NotFoundError

from util.common_util import convert_datetime

from hysds.es_util import get_grq_es, get_mozart_es

grq_es = get_grq_es()
mozart_es = get_mozart_es()

DATE_FORMAT = "%Y-%m-%d"


def get_job_stats(task_id, _id):
    logger.info("Querying jobs ES for task {}".format(task_id))

    query = {"query": {"term": {"uuid": task_id}}}
    results = mozart_es.es.search(body=query, index="job_status-current", size=1)

    if results["hits"]["total"]["value"] != 1:
        logger.warning("No job information could be found for {} with task id {}".format(_id, task_id))

    job_stats = OrderedDict()
    for result in results["hits"]["hits"]:
        job_met = result["_source"]["job"]
        job_stats = {
            "job_queued": job_met["job_info"]["time_queued"],
            "job_start": job_met["job_info"]["time_start"],
            "job_end": job_met["job_info"]["time_end"],
        }
        for product_staged in job_met["job_info"]["metrics"]["products_staged"]:
            if product_staged["id"] == _id:
                job_stats["ingest_start_time"] = product_staged["time_start"]
                job_stats["ingest_end_time"] = product_staged["time_end"]

    return job_stats


def process_isl_files(paged_result):
    sid = paged_result["_scroll_id"]
    record_length = len(paged_result["hits"]["hits"])
    entries = list()
    while record_length > 0:
        for record in paged_result.get("hits", {}).get("hits", []):
            metadata = record.get("_source").get("metadata")
            source = record.get("_source")
            dataset_id = metadata.get("id")
            logger.info("Processing dataset: {}".format(dataset_id))
            try:
                s3_event_time = metadata.get(pm.S3_EVENT_RECORD, {}).get(
                    "eventTime", None
                )
                sent_timestamp = (
                    metadata.get(pm.SQS_RECORD, {})
                    .get("attributes", {})
                    .get(pm.SENT_TIMESTAMP, None)
                )
                if sent_timestamp is not None:
                    sqs_event_time = time.strftime(
                        "%Y-%m-%dT%H:%M:%S", time.gmtime(int(sent_timestamp) / 1000)
                    )
                else:
                    sqs_event_time = None
                lambda_trigger_time = metadata.get(pm.LAMBDA_TRIGGER_TIME, None)
                if s3_event_time:
                    s3_event_time = convert_datetime(s3_event_time)
                else:
                    logger.warning("Could not find {}".format(pm.S3_EVENT_RECORD))
                    logger.warning("Could not find {}".format(pm.S3_EVENT_RECORD))
                if not lambda_trigger_time:
                    logger.warning("Could not find {}".format(pm.LAMBDA_TRIGGER_TIME))
                if not sent_timestamp:
                    logger.warning("Could not find {}".format(pm.SENT_TIMESTAMP))

                task_id = source["prov"]["wasGeneratedBy"]
                task_id = task_id.split(":")[1]
                job_stats = get_job_stats(task_id, record.get("_id"))
                latency_time = None
                if "ingest_end_time" in job_stats:
                    end_time = convert_datetime(job_stats["ingest_end_time"])
                    latency = end_time - s3_event_time
                    latency_time = latency.total_seconds()
                else:
                    logger.warning(
                        "Cannot determine latency. Missing ingest_end_time in the job information "
                        "for {}".format(dataset_id)
                    )

                entry = {
                    "file_name": metadata.get(pm.FILE_NAME),
                    "latency (in seconds)": latency_time,
                    "s3_event_time": convert_datetime(s3_event_time),
                    "sqs_event_time": sqs_event_time,
                    "lambda_trigger_time": lambda_trigger_time,
                }
                entry.update(job_stats)
                entries.append(entry)
            except Exception as e:
                logger.error("Error processing {}: {}".format(id, str(e)))
        paged_result = grq_es.es.scroll(scroll_id=sid, scroll="2m")
        sid = paged_result["_scroll_id"]
        record_length = len(paged_result["hits"]["hits"])

    try:
        grq_es.es.clear_scroll(scroll_id=sid)  # need to clear scroll id, ES7 only gives 500 at a time
    except NotFoundError:
        logger.warning('scroll_id not found, continuing...')
    except Exception as error:
        raise RuntimeError(error)

    return entries


def query_isl_files(ingest_date):
    logger.info("Querying for ISL files that were ingested on {}".format(
        convert_datetime(ingest_date, strformat=DATE_FORMAT)
    ))
    start_time = ingest_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = ingest_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    product_received_time = "metadata.{}".format(pm.PRODUCT_RECEIVED_TIME)

    query = {
        "query": {
            "bool": {
                "must": [{"match": {"metadata.tags": "ISL"}}],
                "filter": [
                    {
                        "range": {
                            product_received_time: {"gte": convert_datetime(start_time)}
                        }
                    },
                    {
                        "range": {
                            product_received_time: {"lte": convert_datetime(end_time)}
                        }
                    },
                ],
            }
        }
    }
    sort_clause = {
        "sort": [{
            product_received_time: {
                "order": "asc"
            }
        }]
    }
    query.update(sort_clause)
    print(json.dumps(query, indent=2))

    logger.info("Query: {}".format(json.dumps(query, indent=2)))
    paged_result = grq_es.es.search(body=query, index="grq", size=100, scroll="2m")
    logger.info("Paged Result: {}".format(json.dumps(paged_result, indent=2)))
    return paged_result


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(description="Generate an ISL report for a given time coverage")
    parser.add_argument("--start_date", required=False,
                        help="Specify the starting date, in YYYY-MM-DD format, to begin generating a report. If this "
                             "is omitted, default is the current date.")
    parser.add_argument("--end_date", required=False,
                        help="Specify the end date, in YYYY-MM-DD format, to stop generating a report. If this is "
                             "omitted, default is the same as the start_date value.")
    parser.add_argument("--output_file", required=True, help="Specify an output file for the ISL report.")
    parser.add_argument("--verbose_level", type=lambda verbose_level: LogLevels[verbose_level].value,
                        choices=LogLevels.list(), help="Specify a verbosity level. Default: {}".format(LogLevels.INFO))
    return parser


def main():
    """Main entry point"""

    args = get_parser().parse_args()
    output_file = args.output_file

    if args.start_date:
        start_date = convert_datetime(args.start_date, strformat=DATE_FORMAT)
    else:
        start_date = datetime.utcnow()
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if args.end_date:
        end_date = convert_datetime(args.end_date, strformat=DATE_FORMAT)
    else:
        end_date = start_date

    if args.verbose_level:
        LogLevels.set_level(args.verbose_level)

    report = {"isl_report": []}
    for current_date in rrule(DAILY, dtstart=start_date, until=end_date):
        paged_result = query_isl_files(current_date)
        entries = process_isl_files(paged_result)
        if entries:
            report["isl_report"].extend(entries)
        else:
            logger.info("No ISL files found for {}".format(convert_datetime(current_date, strformat=DATE_FORMAT)))

    # Convert to JSON
    if len(report["isl_report"]) != 0:
        logger.info("Writing ISL report to {}".format(output_file))
        with open(output_file, "w") as writer:
            csv_writer = csv.writer(writer)
            count = 0
            for entry in report["isl_report"]:
                if count == 0:
                    header = entry.keys()
                    csv_writer.writerow(header)
                    count += 1
                csv_writer.writerow(entry.values())
        logger.info("Successfully created ISL report: {}".format(output_file))
    else:
        logger.info(
            "Will not generate an ISL report. No ISL files found for start_date={}, end_date={}".format(
                convert_datetime(start_date, strformat=DATE_FORMAT),
                convert_datetime(end_date, strformat=DATE_FORMAT),
            )
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Error occurred while generating ISL report: {}".format(str(e)))
        logger.error(traceback.format_exc())
        sys.exit(1)
