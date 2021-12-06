#!/usr/bin/env python
"""
Datatake Evaluator job
"""
import os
import json
from datetime import datetime
from util.exec_util import exec_wrapper
from util.ctx_util import JobContext
from util.conf_util import SettingsConf

from util.common_util import create_state_config_dataset
from util.common_util import create_expiration_time
from util.common_util import get_latest_product_sort_list, convert_datetime
from util.common_util import create_info_message_files

from commons.es_connection import get_grq_es
from commons.logger import logger
from commons.constants import product_metadata as pm
from commons.constants import constants
from commons.constants import short_info_msg as short_msg
from cop import cop_catalog as cop_catalog
from observation_accountability.catalog import (
    ES_INDEX as OBSERVATION_ACCOUNTABILITY_INDEX,
)

from collections import OrderedDict

ancillary_es = get_grq_es(logger)  # getting GRQ's es connection

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


def _get_l0a_ids(l0a_files):
    l0a_ids = []
    for l0a in l0a_files:
        l0a_ids.append(l0a.split(".")[0])
    return l0a_ids


def find_accountability_record(datatake_id):
    try:
        body = {
            "query": {"bool": {"must": [{"term": {"datatake_id": datatake_id}}]}}}

        result = ancillary_es.search(
            index=OBSERVATION_ACCOUNTABILITY_INDEX, body=body
        )
        results = result.get("hits", {}).get("hits", [])
        if len(results) < 1:
            return None, None
        for entry in results:
            entry_id = entry.get("_id", None)
            return entry_id, entry.get("_source", {})
    except Exception:
        logger.error("Failed to find accountablity records")
        return None, None


def update_accountability_entries(
    dataset_name, datatake_id, metadata, is_urgent_response
):
    to_create = []
    found_rrsts = _get_l0a_ids(metadata[pm.FOUND_L0A_RRST])
    if metadata[pm.IS_COMPLETE] is True:
        metadata["state"] = "job_complete"
    else:
        metadata["state"] = "job_incomplete"
    if is_urgent_response:
        # TODO: Key does not match with metadata[pm.OBSERVATION_ID]. Need to review
        '''
        observation_id = metadata[pm.OBSERVATION_ID]
        begin_time = metadata[pm.OBSERVATION_BEGIN_TIME]
        end_time = metadata[pm.OBSERVATION_END_TIME]
        '''
        observation_ids = metadata[pm.OBSERVATION_IDS]
        begin_time = metadata[pm.DATATAKE_BEGIN_TIME]
        end_time = metadata[pm.DATATAKE_END_TIME]
        entry_id, source = find_accountability_record(datatake_id)
        if entry_id is not None:
            if metadata[pm.IS_COMPLETE] is True:
                source["state-config_status"] = "complete"
            else:
                source["state-config_status"] = "incomplete"
            source["state-config_is_complete"] = metadata[pm.IS_COMPLETE]
            source["state-config_force_submit"] = metadata[pm.FORCE_SUBMIT]
            source["last_modified"] = convert_datetime(datetime.utcnow())
            source["L0A_L_RRST_ids"] = found_rrsts
            update_doc = {
                "doc_as_upsert": True,
                "doc": source,
            }
            try:
                ancillary_es.update_document(
                    id=entry_id, index=OBSERVATION_ACCOUNTABILITY_INDEX, body=update_doc
                )
                logger.info("updated document {}.".format(entry_id))
            except Exception:
                logger.error("Failed to update document {}.".format(entry_id))
        else:
            datatake_record = {
                "datatake_id": datatake_id,
                "datatake-state-config": dataset_name,
                "L0A_L_RRST_ids": found_rrsts,
                "observation_ids": observation_ids,
                "created_at": convert_datetime(datetime.utcnow()),
                "last_modified": convert_datetime(datetime.utcnow()),
                "datatake-state-config_id": dataset_name,
                "state-config_is_complete": metadata[pm.IS_COMPLETE],
                "state-config_force_submit": metadata[pm.FORCE_SUBMIT],
                "product_type": "urgent",
                "begin_time": begin_time,
                "end_time": end_time,
                "refrec_id": "",
            }
            if metadata[pm.IS_COMPLETE] is True:
                datatake_record["state-config_status"] = "complete"
            else:
                datatake_record["state-config_status"] = "incomplete"
            to_create.append(datatake_record)
    else:
        observation_ids = metadata[pm.OBSERVATION_IDS]
        begin_time = metadata[pm.DATATAKE_BEGIN_TIME]
        end_time = metadata[pm.DATATAKE_END_TIME]
        rrsts = _get_l0a_ids(found_rrsts)
        entry_id, source = find_accountability_record(datatake_id)
        if entry_id is not None:
            if metadata[pm.IS_COMPLETE] is True:
                source["state-config_status"] = "complete"
            else:
                source["state-config_status"] = "incomplete"
            source["state-config_is_complete"] = metadata[pm.IS_COMPLETE]
            source["state-config_force_submit"] = metadata[pm.FORCE_SUBMIT]
            source["datatake-state-config_id"] = dataset_name
            source["datatake-state-config"] = dataset_name
            source["L0A_L_RRST_ids"] = rrsts
            source["observation_ids"] = observation_ids
            source["processing_type"] = "nominal"
            source["begin_time"] = begin_time
            source["end_time"] = end_time
            source["last_modified"] = convert_datetime(datetime.utcnow())
            source["in_cop_catalog"] = False
            update_doc = {
                "doc_as_upsert": True,
                "doc": source,
            }
            try:
                ancillary_es.update_document(
                    id=entry_id, index=OBSERVATION_ACCOUNTABILITY_INDEX, body=update_doc
                )
                logger.info("updated document {}.".format(entry_id))
            except Exception:
                logger.error("Failed to update document {}.".format(entry_id))
        else:
            datatake_record = {
                "L0A_L_RRST_ids": rrsts,
                "observation_ids": observation_ids,
                "datatake_id": datatake_id,
                "created_at": convert_datetime(datetime.utcnow()),
                "last_modified": convert_datetime(datetime.utcnow()),
                "datatake-state-config": dataset_name,
                "datatake-state-config_id": dataset_name,
                "state-config_is_complete": metadata[pm.IS_COMPLETE],
                "state-config_force_submit": metadata[pm.FORCE_SUBMIT],
                "processing_type": "nominal",
                "begin_time": begin_time,
                "end_time": end_time,
                "refrec_id": "",
                "in_cop_catalog": False,
            }
            if metadata[pm.IS_COMPLETE] is True:
                datatake_record["state-config_status"] = "complete"
            else:
                datatake_record["state-config_status"] = "incomplete"
            to_create.append(datatake_record)
    if len(to_create) > 0:
        try:
            logger.info("Trying to save accountability records")
            for entry in to_create:
                _id = ""
                ancillary_es.index_document(
                    index=OBSERVATION_ACCOUNTABILITY_INDEX, id=_id, body=entry)
        except Exception:
            logger.error("Failed to save datatake accountability records")


def create_urgent_observation_state_config(
        datastore_refs, observation_id, obs_begin_time, obs_end_time
):
    found_rrsts = list()
    rrst_paths = list()

    for dr in datastore_refs:
        found_rrsts.append(os.path.basename(dr))
        rrst_paths.append(os.path.dirname(dr))

    found_rrsts.sort()
    rrst_paths.sort()

    state_config = dict()
    state_config[pm.FOUND_L0A_RRST] = found_rrsts
    state_config[pm.L0A_RRST_PRODUCT_PATHS] = rrst_paths
    state_config[pm.OBSERVATION_ID] = observation_id
    state_config[pm.OBSERVATION_BEGIN_TIME] = obs_begin_time
    state_config[pm.OBSERVATION_END_TIME] = obs_end_time
    state_config[pm.IS_URGENT] = True

    logger.info("Compiled State Config: {}".format(
        json.dumps(state_config, indent=2)))
    dataset_name = "{}_urgent-state-config".format(observation_id)
    logger.info("State Config Name: {}".format(dataset_name))
    create_state_config_dataset(
        dataset_name=dataset_name,
        metadata=state_config,
        start_time=obs_begin_time,
        end_time=obs_end_time,
    )
    update_accountability_entries(dataset_name, observation_id, state_config, True)


def create_datatake_state_config(
    found_rrsts,
    observation_ids,
    is_complete,
    datatake_id,
    datatake_begin_time,
    datatake_end_time,
    submitted_by_timer,
    is_urgent_response=False,
    urgent_response_index=False
):
    state_config = dict()
    state_config[pm.DATATAKE_ID] = datatake_id
    state_config[pm.FOUND_L0A_RRST] = list()
    state_config[pm.L0A_RRST_PRODUCT_PATHS] = list()
    state_config[pm.OBSERVATION_IDS] = observation_ids
    state_config[pm.DATATAKE_BEGIN_TIME] = datatake_begin_time
    state_config[pm.DATATAKE_END_TIME] = datatake_end_time
    state_config[pm.IS_COMPLETE] = is_complete
    state_config[pm.FORCE_SUBMIT] = False
    state_config[pm.IS_URGENT] = is_urgent_response
    state_config[pm.SUBMITTED_BY_TIMER] = submitted_by_timer

    # TODO: Need to set the default latency for the datatake evaluator timers

    settings = SettingsConf().cfg
    if urgent_response_index:
        latency = settings.get(constants.URGENT_RESPONSE_LATENCY, {}).get(constants.DATATAKE_EVALUATOR, 15)
    else:
        latency = settings.get(constants.NOMINAL_LATENCY, {}).get(constants.DATATAKE_EVALUATOR, 360)

    for rrst, product_path in found_rrsts.items():
        state_config[pm.FOUND_L0A_RRST].append(rrst)
        state_config[pm.L0A_RRST_PRODUCT_PATHS].append(product_path)

    logger.info("Compiled State Config: {}".format(
        json.dumps(state_config, indent=2)))

    dataset_name = get_state_config_id(datatake_id, urgent_response_type=urgent_response_index)

    logger.info("State Config Name: {}".format(dataset_name))

    entry_id, source = find_accountability_record(dataset_name)

    if entry_id is None:
        logger.info("Creating datatake_state_config for {} with urgent response : {}".format(dataset_name,
                                                                                             is_urgent_response))
        create_state_config_dataset(dataset_name=dataset_name, metadata=state_config,
                                    start_time=datatake_begin_time, end_time=datatake_end_time,
                                    expiration_time=create_expiration_time(int(latency)))

    update_accountability_entries(dataset_name, datatake_id, state_config, state_config[pm.IS_URGENT])


def get_datatake_records(datatake_ids, es_index=cop_catalog.ES_INDEX):
    """
    Gets observation records associated with the given datatakes.

    :param datatake_ids:
    :return: A mapping of datatake_ids to its observation records
    """
    datatake_records = dict()

    for datatake_id in datatake_ids:
        query = {
            "query": {
                "bool": {
                    "must": ancillary_es.construct_bool_query(
                        {cop_catalog.DATATAKE_ID: datatake_id}
                    )
                }
            }
        }
        results = ancillary_es.search(
            body=query,
            index=es_index,
            sort=["{}:asc".format(cop_catalog.CMD_LSAR_START_DATETIME_ISO)],
        )
        obs_records = results.get("hits", {}).get("hits", [])
        if len(obs_records) == 0:
            raise RuntimeError(
                "No observation records found in the COP Catalog that have the "
                "datatake_id={}".format(datatake_id)
            )
        datatake_records[datatake_id] = obs_records
    return datatake_records


def is_urgent_response(observations):
    is_urgent_response = False
    for observation in observations:
        if observation.get("_source", {}).get(cop_catalog.URGENT_RESPONSE) is True:
            is_urgent_response = True
            break

    return is_urgent_response


def get_l0a_records(begin_date_time, end_date_time, index, conditions=None):
    # Need to use this function as that would return the latest version of each product found
    l0a_records = ancillary_es.perform_aggregate_range_intersection_query(
        beginning_date_time=begin_date_time,
        ending_date_time=end_date_time,
        met_field_beginning_date_time="starttime",
        met_field_ending_date_time="endtime",
        aggregate_field="metadata.PCMRetrievalID.keyword",
        conditions=conditions,
        sort_list=get_latest_product_sort_list(),
        index=index,
    )
    return l0a_records


def sort_by_vcids(records):
    """
    Returns a dictionary of VCIDs to records

    :param records:
    :return:
    """
    vcid_map = dict()
    for record in records:
        vcid = record.get("_source", {}).get("metadata", {}).get(pm.VCID)
        if vcid in vcid_map:
            vcid_map[vcid].append(record)
        else:
            vcid_map[vcid] = [record]

    return vcid_map


def find_state_config(datatake_id, urgent_response_index=False):
    result = dict()

    # TODO: version is hardcoded. Need to review
    if urgent_response_index:
        es_index = "grq_1_datatake-urgent_response_state-config"
    else:
        es_index = "grq_1_datatake-state-config",

    existing_document = ancillary_es.get_by_id(
        id=get_state_config_id(datatake_id, urgent_response_type=urgent_response_index),
        index=es_index,
        ignore=[404],
    )

    if existing_document.get("found", False):
        result = existing_document.get("_source", {}).get("metadata", {})

    return result


def get_obs_records(input_begin_time, input_end_time, es_index=cop_catalog.ES_INDEX):
    # Query COP Catalog and find out which observations overlap with the input L0A file
    results = ancillary_es.perform_es_range_intersection_query(
        input_begin_time,
        input_end_time,
        cop_catalog.CMD_LSAR_START_DATETIME_ISO,
        cop_catalog.CMD_LSAR_END_DATETIME_ISO,
        sort=["{}:asc".format(cop_catalog.CMD_LSAR_START_DATETIME_ISO)],
        size=100,
        index=es_index,
    )

    obs_records = results.get("hits", {}).get("hits", [])
    return obs_records


def get_state_config_id(datatake_id, urgent_response_type):
    if urgent_response_type:
        return "{}_urgent_response_state-config".format(datatake_id)
    return "{}_state-config".format(datatake_id)


@exec_wrapper
def evaluate():
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.debug("job_context: {}".format(json.dumps(job_context, indent=2)))

    product_metadata = job_context.get("product_metadata")
    metadata = product_metadata.get("metadata")
    dataset_type = job_context.get("dataset_type")
    # Query COP Catalog and find out which observations overlap with the input L0A file
    input_begin_time = metadata.get(pm.RADAR_START_DATE_TIME, None)
    input_end_time = metadata.get(pm.RADAR_STOP_DATE_TIME, None)

    if input_begin_time is None or input_end_time is None:
        raise RuntimeError(
            "Missing {} and/or {} in the job context".format(
                pm.RADAR_START_DATE_TIME, pm.RADAR_STOP_DATE_TIME
            )
        )
    obs_records = get_obs_records(input_begin_time, input_end_time, es_index=cop_catalog.ES_INDEX)
    if len(obs_records) == 0:
        info_msg = "No observations found in the COP Catalog that are between {} and {}".format(
            input_begin_time, input_end_time
        )
        logger.info(info_msg)
        create_info_message_files(msg=short_msg.NO_OBSERVATIONS_FOUND, msg_details=info_msg)
        return

    # From the observation results, find out which data take L0A product belongs to
    # From the observations, find out which data takes
    datatake_ids = set()
    for obs_record in obs_records:
        datatake_ids.add(obs_record.get(
            "_source", {}).get(cop_catalog.DATATAKE_ID))

    datatake_records = get_datatake_records(datatake_ids)
    logger.info("datatake_records : {}".format(json.dumps(datatake_records, indent=2)))

    # For each data take, find out if we have full time coverage of the L0A products. Once we have all L0A hits
    # per VCID, we need to add their time ranges and see if it fully overlaps with the datatake time range.
    already_completed_state_configs = list()
    already_submitted_by_timer = list()
    for datatake_id, observations in datatake_records.items():
        # Using sets will ensure we capture unique L0As
        found_rrsts = OrderedDict()

        datatake_begin_time = (
            observations[0].get("_source", {}).get(
                cop_catalog.CMD_LSAR_START_DATETIME_ISO)
        )
        datatake_end_time = (
            observations[-1].get("_source", {}
                                 ).get(cop_catalog.CMD_LSAR_END_DATETIME_ISO)
        )

        is_urgent_response = False
        is_complete = True
        all_obs_records = list()
        observation_ids = list()

        # See if we have any urgent response overlap
        tiurdrop_obs_records = get_obs_records(input_begin_time, input_end_time, es_index=cop_catalog.TIURDROP_ES_INDEX)
        if len(tiurdrop_obs_records) == 0:
            logger.info(
                "No tiurdrop observations found in the TIURDROP Catalog that are between {} and {}".format(
                    input_begin_time, input_end_time
                )
            )
        else:
            logger.info("tiurdrop_obs_records Found between {} and {}: \n{}".format(input_begin_time, input_end_time, json.dumps(tiurdrop_obs_records, indent=2)))
            is_urgent_response = True

        logger.info("is_urgent_response : {}".format(is_urgent_response))
        for observation in observations:
            vetted_records = list()
            obs_begin_time = observation.get("_source", {}).get(
                cop_catalog.CMD_LSAR_START_DATETIME_ISO
            )
            obs_end_time = observation.get("_source", {}).get(
                cop_catalog.CMD_LSAR_END_DATETIME_ISO
            )

            # Disabling single polarity evaluation
            '''
            polarization_type = observation.get("_source", {}).get(
                cop_catalog.POLARIZATION_TYPE
            )'''
            # is_urgent = observation.get("_source", {}).get(cop_catalog.URGENT_RESPONSE)

            observation_id = observation.get(
                "_source", {}).get(cop_catalog.REFOBS_ID)
            observation_ids.append(observation_id)

            # Disabling single polarity evaluation

            '''
            # For single polarizations, find L0As with a matching VCID as the one that triggered this job.
                if polarization_type == PolarizationType.SINGLE.value:
                l0a_records = get_l0a_records(
                    obs_begin_time,
                    obs_end_time,
                    index="grq_*_{}".format(dataset_type.lower()),
                    conditions={
                        "metadata.{}.keyword".format(pm.VCID): metadata.get(pm.VCID)
                    },
                )

                best_fit_records = ancillary_es.select_best_fit(
                    obs_begin_time, obs_end_time, l0a_records
                )
                if len(best_fit_records) == 0:
                    is_complete = False
                    vetted_records.extend(l0a_records)
                else:
                    vetted_records.extend(best_fit_records)
            elif polarization_type == PolarizationType.DUAL.value:'''

            l0a_records = get_l0a_records(
                obs_begin_time,
                obs_end_time,
                index="grq_*_{}".format(dataset_type.lower()),
            )

            # Sort the records by VCID
            vcid_map = sort_by_vcids(l0a_records)
            if len(vcid_map.keys()) < 2:
                is_complete = False
            # Best fit function will return a result if it was able to determine that there was complete
            # coverage
            for vcid, recs in vcid_map.items():
                best_fit_records = ancillary_es.select_best_fit(
                    obs_begin_time, obs_end_time, recs
                )
                if len(best_fit_records) == 0:
                    is_complete = False
                    vetted_records.extend(l0a_records)
                else:
                    vetted_records.extend(best_fit_records)

            # Add to total records
            all_obs_records.extend(vetted_records)

        all_obs_records.sort(
            key=lambda x: x.get("_source").get(
                "metadata").get(pm.RADAR_START_DATE_TIME)
        )
        for dr in ancillary_es.get_datastore_refs_from_es_records(all_obs_records):
            found_rrsts[os.path.basename(dr)] = os.path.dirname(dr)

        """
        Create the state config for Nominal Datatake
        Index into nominal datatake-state-config index, set is_urgent_response based on TIURDROP overlap
        See if state config already exists for this datatake
        """
        existing_state_config = find_state_config(datatake_id, urgent_response_index=False)
        # If we find an existing state config, which is already complete, then do nothing
        if existing_state_config.get(pm.IS_COMPLETE, False) is True:
            logger.info(
                "Will not create a state config for {} as it is already declared as complete in ES".format(
                    datatake_id
                )
            )
            already_completed_state_configs.append(get_state_config_id(datatake_id, urgent_response_type=False))
        else:
            submitted_by_timer = existing_state_config.get(pm.SUBMITTED_BY_TIMER, None)
            logger.info("Creating datatake_state_config for nominal.")
            create_datatake_state_config(
                found_rrsts,
                observation_ids,
                is_complete,
                datatake_id,
                datatake_begin_time,
                datatake_end_time,
                submitted_by_timer,
                is_urgent_response=is_urgent_response,
                urgent_response_index=False
            )

        # Create the state config for Urgent Response Datatake(is_urgent_response=True) if TUIRDROP overlap found
        if is_urgent_response:
            existing_urgent_responsestate_config = find_state_config(datatake_id, urgent_response_index=True)
            if existing_urgent_responsestate_config.get(pm.IS_COMPLETE, False) is True:
                logger.info(
                    "Will not create a urgent response state config for {} as it is already declared "
                    "as complete in ES".format(datatake_id)
                )
                already_completed_state_configs.append(get_state_config_id(datatake_id, urgent_response_type=True))
            else:
                # For existing incomplete UR state-configs
                submitted_by_timer_urgent_response = existing_urgent_responsestate_config.get(pm.SUBMITTED_BY_TIMER,
                                                                                              None)
                if submitted_by_timer_urgent_response:
                    logger.info(
                        "Will not create a urgent response state config for {} as it is already "
                        "submitted by timer".format(datatake_id)
                    )
                    already_submitted_by_timer.append(get_state_config_id(datatake_id, urgent_response_type=True))
                else:
                    logger.info("Creating datatake_state_config for urgent response")
                    create_datatake_state_config(
                        found_rrsts,
                        observation_ids,
                        is_complete,
                        datatake_id,
                        datatake_begin_time,
                        datatake_end_time,
                        submitted_by_timer_urgent_response,
                        is_urgent_response=True,
                        urgent_response_index=True
                    )
    msgs = list()
    msg_details = ""
    if len(already_completed_state_configs) != 0:
        msgs.append(short_msg.STATE_CONFIGS_ALREADY_COMPLETE)
        msg_details += "\n\nThe following state configs will not be re-published as they are already declared " \
                       "as complete:\n\n"
        for id in already_completed_state_configs:
            msg_details += "{}\n".format(id)

    if len(already_submitted_by_timer) != 0:
        msgs.append(short_msg.ALREADY_SUBMITTED_BY_TIMER)
        msg_details += "\n\nThe following state configs will not be re-published as they were already " \
                       "submitted by the timer:\n\n"
        for id in already_submitted_by_timer:
            msg_details += "{}\n".format(id)

    if len(msgs) != 0:
        create_info_message_files(msg=msgs, msg_details=msg_details)


if __name__ == "__main__":
    evaluate()
