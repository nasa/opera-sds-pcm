"""
This script will find state config datasets of a given type and do a force submission
to any that are past the expiration time.
"""

# !/usr/bin/env python
import os
import json
import boto3

from commons.es_connection import get_grq_es

from util.ctx_util import JobContext
from commons.constants import product_metadata as pm
from util.exec_util import exec_wrapper
from commons.logger import logger
from util.common_util import create_state_config_dataset
from util.common_util import convert_datetime
from util.common_util import create_info_message_files
from commons.constants import short_info_msg as short_msg

from datetime import datetime

BASE_PATH = os.path.dirname(__file__)

ancillary_es = get_grq_es(logger)  # getting GRQ's es connection


def notify_operator(sns_arn, subject, message):
    arn_fields = sns_arn.split(":")
    region = arn_fields[3]
    session = boto3.session.Session()
    sns = session.client("sns", region_name=region)
    response = sns.publish(TopicArn=sns_arn, Subject=subject, Message=message)
    logger.info(
        "Response from sending operator notification: {}".format(
            json.dumps(response, indent=2)
        )
    )


#def send_incomplete_network_pair_message(sns_arn, metadata):
#    sec_rslc = None
#    network_pair_rslcs = metadata.get(pm.NETWORK_PAIR_RSLCS)
#    for rslc in network_pair_rslcs:
#        if rslc:
#            sec_rslc = rslc
#    begin_time = metadata.get(pm.RADAR_START_DATE_TIME)
#    end_time = metadata.get(pm.RADAR_STOP_DATE_TIME)
#    subject = "Incomplete Network Pair"
#    message = (
#        "Missing Compatiable Neighbor RSLC file needed to create complete network pair for {} with "
#        "range times {} to {}.".format(sec_rslc, begin_time, end_time)
#    )
#    notify_operator(sns_arn, subject=subject, message=message)


def update_state_config(es_record):
    # NOTE: disabled the following original code since the expired-state-config
    # generation will force the subsequent PGE job; leaving it here uncommented as
    # we may need to add this back when we start integrating urgent response via
    # the TIURDROP; if not needed in UR implementation, this code should be remove
    """
    source = es_record.get("_source")
    metadata = source.get("metadata")
    metadata[pm.FORCE_SUBMIT] = True
    metadata[pm.SUBMITTED_BY_TIMER] = convert_datetime(datetime.utcnow())
    create_state_config_dataset(es_record.get("_id"), metadata, source.get(pm.START_TIME), source.get(pm.END_TIME))
    """

    # update state config that a timer has already ran for it
    ancillary_es.update_document(
        id=es_record.get("_id"),
        index=es_record.get("_index"),
        body={
            "doc_as_upsert": True,
            "doc": {
                "metadata": {pm.SUBMITTED_BY_TIMER: convert_datetime(datetime.utcnow())}
            },
        },
    )


def create_expired_state_config(es_record):
    source = es_record.get("_source")
    metadata = source.get("metadata")
    expired_state_config_id = es_record.get("_id").replace(
        "_state-config", "_expired-state-config"
    )
    create_state_config_dataset(
        expired_state_config_id,
        metadata,
        source.get(pm.START_TIME),
        source.get(pm.END_TIME),
    )


@exec_wrapper
def evaluate():
    ctx = JobContext("_context.json").ctx

    dataset_type = ctx["dataset_type"]
    sns_arn = ctx["notify_arn"]
    current_time = convert_datetime(datetime.utcnow())
    logger.info("Current timestamp: {}".format(current_time))
    conditions = {
        "metadata.{}".format(pm.IS_COMPLETE): False,
        "metadata.{}".format(pm.FORCE_SUBMIT): False,
    }
    query = {
        "query": {
            "bool": {
                "must": ancillary_es.construct_bool_query(conditions),
                "must_not": [
                    {"exists": {"field": "metadata.{}".format(pm.SUBMITTED_BY_TIMER)}}
                ],
            }
        }
    }

    query["query"]["bool"]["must"].append(
        {
            "bool": {
                "must": [
                    {
                        "range": {
                            "{}".format(pm.EXPIRATION_TIME): {
                                "lte": current_time
                            }
                        }
                    }
                ]
            }
        }
    )

    index = "grq_*_{}".format(dataset_type.lower())
    scroll_ids = set()
    logger.info(
        "Querying against the index {} using the following query: {}".format(
            index, json.dumps(query, indent=2)
        )
    )
    paged_result = ancillary_es.es.search(
        body=query, index=index, scroll="2m", ignore=404
    )
    records = paged_result.get("hits", {}).get("hits", [])
    if len(records) == 0:
        logger.info(
            "No records found in index {} with the following query: {}".format(
                index, json.dumps(query, indent=2)
            )
        )
        create_info_message_files(
            msg=short_msg.NO_EXPIRED_STATE_CONFIGS,
            msg_details="No expired state configs found for type {}".format(dataset_type)
        )

    while len(records) > 0:
        scroll_id = paged_result["_scroll_id"]
        for record in records:
            logger.info("Processing record {}".format(record.get("_id")))

            # check if expired-state-config exists; if so, skip
            expired_state_config_index = record.get("_index").replace(
                "-state-config", "-expired-state-config"
            )
            expired_state_config_id = record.get("_id").replace(
                "_state-config", "_expired-state-config"
            )
            expired_state_config = ancillary_es.get_by_id(
                index=expired_state_config_index,
                id=expired_state_config_id,
                ignore=[404]
            )
            if expired_state_config.get("found", False) is True:
                logger.info(
                    "Found expire-state-config record {} in {}. Skipping.".format(
                        expired_state_config_id, expired_state_config_index
                    )
                )
                continue

            # update state-config with submitted_by_timer timestamp
            update_state_config(record)

            # create expired-state-config
            create_expired_state_config(record)

            # extract metadata
            metadata = record.get("_source", {}).get("metadata", {})

            # Notify the operator if needed
            if dataset_type == pm.LDF_STATE_CONFIG:
                send_missing_nen_message(sns_arn, metadata)

        if scroll_id:
            scroll_ids.add(scroll_id)
            paged_result = ancillary_es.es.scroll(scroll_id=scroll_id, scroll="2m")
            records = paged_result["hits"]["hits"]
        else:
            records = []

    for scroll_id in scroll_ids:
        ancillary_es.es.clear_scroll(scroll_id=scroll_id)


if __name__ == "__main__":
    """
    Main program of job
    """
    evaluate()
