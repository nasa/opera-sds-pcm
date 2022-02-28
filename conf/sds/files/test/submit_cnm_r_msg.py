#!/usr/bin/env python

"""
Python script to submit a hello world job
"""

from builtins import str
import logging
import json
import boto3
import hashlib
import argparse

from product_delivery.cnm.response_message_creator import ResponseMessageCreator
from product_delivery.cnm import utilities

from hysds.es_util import get_grq_es


# this python code needs to query ES with job_id
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hysds")

grq_es = get_grq_es()


def get_datasets(dataset, index="grq"):
    dataset_list = dataset.split(",")
    """Return dataset IDs found"""
    query = {
        "query": {"bool": {"should": [], "must_not": [{"term": {"metadata.restaged": "true"}}]}},
        "_source": [],
    }
    for i in dataset_list:
        query["query"]["bool"]["should"].append({"term": {"dataset.keyword": i}})

    logger.info("query: {}".format(json.dumps(query, indent=2)))
    try:
        result = grq_es.query(index=index, body=query)
        ingested_datasets = {}
        if len(result) > 0:
            for i in result:
                ingested_datasets[i["_id"]] = i["_source"]["metadata"]["ProductType"]
            return ingested_datasets
        else:
            raise RuntimeError("No {} datasets found".format(dataset))
    except Exception as e:
        logger.error(str(e))
        raise RuntimeError(e)


def create_cnm_r_msg(dataset_id, product_type):
    cnm_r = ResponseMessageCreator(
        identifier=dataset_id,
        collection=product_type,
        submission_time=utilities.get_current_datetime(),
        received_time=utilities.get_current_datetime(),
        status="SUCCESS",
        catalog_id="C1234208438-OPERA-TEST",
        catalog_url="http://some/cmr/url",
    )
    cnm_r.add_product(dataset_id)
    cnm_r.add_url("http://some/daac/url/{}".format(dataset_id), 1234, "data", "12345abcde")
    return cnm_r.dump()


def publish_cnm(cnm_r, source_arn):
    if "sns" in source_arn:
        client = boto3.client("sns")
        response = client.publish(TopicArn=source_arn, Message=json.dumps(cnm_r))
    elif "kinesis" in source_arn:
        client = boto3.client("kinesis")
        stream_name = source_arn.split("/")[-1]
        hash_value = hashlib.md5(json.dumps(cnm_r).encode("utf-8")).hexdigest()
        response = client.put_record(
            StreamName=stream_name,
            Data=json.dumps(cnm_r).encode("utf-8"),
            PartitionKey=hash_value,
        )
    elif "sqs" in source_arn:
        sqs = boto3.client("sqs")
        sqs_name = source_arn.split(":")[-1]
        response = sqs.get_queue_url(QueueName=sqs_name)
        if "QueueUrl" not in response:
            raise RuntimeError("Could not find SQS Queue Url for {}".format(sqs_name))

        # For non-dev environments, put the CNM-R in an SNS message
        logger.info(
            "Non-dev environment detected. Submitting CNM-R message inside an SNS message."
        )
        response = sqs.send_message(
            QueueUrl=response["QueueUrl"], MessageBody=json.dumps(cnm_r)
        )
    else:
        raise RuntimeError("Source ARN not supported: {}".format(source_arn))

    print("Successfully published CNM-R message: {}".format(response))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets", required=True, help="dataset types")
    parser.add_argument("source_arn", help="source ARN")
    parser.add_argument("output_file", help="output file")
    parser.add_argument(
        "--no_simulation",
        action="store_true",
        help="turn off simulation of CNM-R reception from DAAC",
    )
    args = parser.parse_args()
    try:
        datasets = args.datasets
        source_arn = args.source_arn
        output_file = args.output_file
        no_simulation = args.no_simulation
        dataset_ids = get_datasets(datasets)

        if no_simulation is False:
            for dataset_id, product_type in dataset_ids.items():
                cnm_r = create_cnm_r_msg(dataset_id, product_type)
                print("Successfully create CNM-R message: {}".format(json.dumps(cnm_r)))
                publish_cnm(cnm_r, source_arn)

        with open(output_file, "w") as f:
            json.dump(dataset_ids, f, sort_keys=True, indent=2)

    except Exception as e:
        raise RuntimeError("Failed to submit CNM-R test message: {}".format(e))
