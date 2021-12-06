# !/usr/bin/env python
import json
import os
import sys
import re

from datetime import datetime
from cop import parser as cop_parser
from util.exec_util import exec_wrapper
from util.common_util import convert_datetime

from pcm_commons.query.ancillary_utility import AncillaryUtility

from cop.es_connection import get_cop_connection
from observation_accountability.es_connection import (
    get_observation_accountability_connection,
)
from observation_accountability import catalog as obs_acc_catalog
# from Datatake_Accountability.eval_datatake import find_accountability_record

from radar_mode import radar_mode_catalog
from radar_mode.polarization_type import PolarizationType
from cop import cop_catalog
from commons.logger import logger

ROOT_TAG = "op"
OBS = "obs"
OBSERVATIONS = "observations"
HEADER = "header"
CREATION_DATETIME = "creation_datetime"
LAST_LEAP_SECOND = "last_leap_second"

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"
COP_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S"

UR_OBS_SUPPORT_RE = r"UR_event_(\w+)_(\d{4})_(\d{2})_(\d{2})T(\d{2})_(\d{2})_(\d{2})_(\d{3})Z_(\w+)"


def __convert_datetime(doy_to_convert):
    """
    Function to convert the day of year datetime into ISO format.
    :param doy_to_convert: Date time in DOY format, YYYY-dddTHH:MM:SS.ssss
    :return: date time in ISO Format  YYYY-mm-ddTHH:MM:SS.ssssZ
    """
    try:
        record_datetime = datetime.strptime(doy_to_convert, "%Y-%jT%H:%M:%S.%f")
    except ValueError:
        record_datetime = datetime.strptime(doy_to_convert, "%Y-%jT%H:%M:%S")
    return record_datetime.strftime(ISO_DATETIME_PATTERN) + "Z"


def reformat_datetime_values(observations, header):
    """
    This finds all the datetime values and converts it to ISO format
    """
    reformatted_header = {}
    for key, value in header.items():
        if key.endswith("datetime") or key == LAST_LEAP_SECOND:
            # maintain original field
            reformatted_header[key] = value
            # add a reformatted field for iso
            reformatted_header[key + "_iso"] = __convert_datetime(value)
        else:
            reformatted_header[key] = value

    reformatted_observations = []
    for observation in observations:
        reformatted_obs = {}
        for key, value in observation.items():
            if key.endswith("datetime"):
                # maintain original field
                reformatted_obs[key] = value
                # add a reformatted field for iso
                reformatted_obs[key + "_iso"] = __convert_datetime(value)
            else:
                reformatted_obs[key] = value
        reformatted_observations.append(reformatted_obs)

    return reformatted_observations, reformatted_header


def update_urgent_response_data(observations):
    """
    NSDS-798
      - parse the OBS_SUPPORT field and check for existance of the "URGENT_RESPONSE_SDS_PROC" string
      - if the "URGENT_RESPONSE_SDS_PROC" exists, parse for the UR request ID,
        "UR_REQID_<USER-PROVIDED ID>_<YYYY><MM>_<DD>"
      - create 2 new fields in the COP catalog and populate it based on the results from steps above
        URGENT_RESPONSE: true or false
        UR_REQID: "" or "UR_REQID_<USER-PROVIDED ID><YYYY><MM>_<DD>"

    NSDS-1322
        Use implement TIURDROP ingestion and metadata extraction into a metadata database
    """

    urRegex = re.compile(UR_OBS_SUPPORT_RE)

    for observation in observations:
        ur_ids = []
        observation["urgent_response"] = False
        if "obs_support" in observation.keys():
            obs_support_value = observation["obs_support"]
            if (
                obs_support_value
                and "UR_EVENT" in obs_support_value.upper()
            ):
                observation["urgent_response"] = True
                observation["ur_event_ids"] = []
                try:
                    if "UR_event" in obs_support_value:
                        events = urRegex.findall(obs_support_value)
                        for e in events:
                            ur_id = "UR_event_{}_{}_{}_{}T{}_{}_{}_{}Z_{}".format(e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8])
                            ur_ids.append(ur_id)
                        if len(ur_ids) > 0:
                            observation["ur_event_ids"] = ur_ids
                        else:
                            logger.info("No UR Event Found")
                except Exception as err:
                    logger.info(
                        "ERROR Parsing OBS_SUPPORT data for UR_Event. Data : {}\t Error : {}".format(
                            obs_support_value, str(err)
                        )
                    )
            else:
                logger.info("UR_event NOT in {}".format(obs_support_value))
        else:
            logger.info("obs_support NOT in observation keys")

    return observations


def parse(cop_dataset):
    cop_file_name = os.path.join(
        os.path.abspath(cop_dataset), "{}.xml".format(cop_dataset)
    )
    logger.info("Parsing COP file: {}".format(cop_file_name))
    cop_doc = cop_parser.parse(cop_file_name)
    logger.info("Converting COP XML to JSON format")
    return cop_file_name, cop_parser.convert_to_json(cop_doc)


def find_accountability_record(observation_es, datatake_id):
    # make a query that retrieves all found observation_accountability records associated with a datatake_id.
    #
    try:
        body = {"query": {"bool": {"must": [{"term": {"datatake_id": datatake_id}}]}}}

        result = observation_es.search(
            index=obs_acc_catalog.ES_INDEX, body=body
        )
        results = result.get("hits", {}).get("hits", [])
        logger.info("results found: {}".format(json.dumps(results)))
        if len(results) < 1:
            return [{"_id": None, "_source": None}]
        results = []
        for entry in results:
            results.append({
                "_id": entry.get("_id", None),
                "_source": entry.get("_source", None)
            })

        return results
    except Exception:
        logger.error("Failed to find accountablity records")
        return [{"_id": None, "_source": None}]


def set_accountability_record(obs_connection, observation):
    try:
        results = find_accountability_record(
            obs_connection, observation[cop_catalog.DATATAKE_ID]
        )
        for result in results:
            obs_entry_id = result["_id"]
            obs_acc_entry = result["_source"]
            if obs_entry_id is None and obs_acc_entry is None:
                obs_acc_entry = {
                    "datatake_id": observation[cop_catalog.DATATAKE_ID],
                    "observation_ids": [observation["refobs_id"]],
                    "L0A_L_RRST_ids": [],
                    "created_at": convert_datetime(datetime.utcnow()),
                    "last_modified": convert_datetime(datetime.utcnow()),
                    "ref_start_datetime_iso": observation[
                        cop_catalog.REF_START_DATETIME_ISO
                    ],
                    "ref_end_datetime_iso": observation[cop_catalog.REF_END_DATETIME_ISO],
                    "refrec_id": "",
                }
                obs_connection.post(
                    [obs_acc_entry], header={}, index=obs_acc_catalog.ES_INDEX
                )
                logger.info(
                    "successfully created obs accountability entry: {}, {}".format(
                        observation[cop_catalog.DATATAKE_ID], observation["refobs_id"]
                    )
                )
            else:
                obs_acc_entry["observation_ids"].append(observation["refobs_id"])
                payload = {
                    "observation_ids": obs_acc_entry["observation_ids"],
                    "last_modified": convert_datetime(datetime.utcnow())
                }
                obs_connection.update_document(
                    index=obs_acc_catalog.ES_INDEX,
                    doc_type="_doc",
                    id=obs_entry_id,
                    body=payload,
                )
                logger.info(
                    "successfully updated obs accountability entry: {}, {}".format(
                        observation[cop_catalog.DATATAKE_ID], observation["refobs_id"]
                    )
                )
    except Exception as e:
        logger.error(
            "Failed to update/create observation accountability entry obs:{}, datatake:{}".format(
                observation["refobs_id"], observation[cop_catalog.DATATAKE_ID]
            )
        )
        logger.error(e)


@exec_wrapper
def main():
    dataset_name = sys.argv[1]
    cop_file_name, cop_data = parse(dataset_name)
    logger.info("COP JSON: {}".format(json.dumps(cop_data, indent=2)))

    es_index = cop_catalog.ES_INDEX
    if dataset_name.upper().startswith("TIURDROP"):
        es_index = cop_catalog.TIURDROP_ES_INDEX

    observations, header = reformat_datetime_values(
        cop_data[ROOT_TAG][OBSERVATIONS][OBS], cop_data[ROOT_TAG][HEADER]
    )
    catalog = get_cop_connection(logger)
    observation_catalog = get_observation_accountability_connection(logger)
    logger.info("Ingesting COP Observations into ElasticSearch")

    # update urgent resplonse values
    observations = update_urgent_response_data(observations)

    for observation in observations:
        if header:
            observation[HEADER] = header
        else:
            observation[HEADER] = {}

        query = {
            "query": {
                "bool": {
                    "must": AncillaryUtility.construct_bool_query(
                        {
                            radar_mode_catalog.RC_ID: observation[
                                cop_catalog.LSAR_CONFIG_ID
                            ]
                        }
                    )
                }
            }
        }
        results = catalog.es.search(body=query, index=radar_mode_catalog.ES_INDEX)
        if results["hits"]["total"]["value"] == 0:
            raise RuntimeError(
                "No record could be found in index {} that has {}={}".format(
                    radar_mode_catalog.ES_INDEX,
                    radar_mode_catalog.RC_ID,
                    observation[cop_catalog.LSAR_CONFIG_ID],
                )
            )

        radar_mode_record = results["hits"]["hits"][0]
        polarization_value = int(
            radar_mode_record["_source"][radar_mode_catalog.POLARIZATION]
        )
        if polarization_value == 0 or polarization_value == 1:
            observation[cop_catalog.POLARIZATION_TYPE] = PolarizationType.SINGLE.value
        else:
            observation[cop_catalog.POLARIZATION_TYPE] = PolarizationType.DUAL.value
        logger.info(
            "Setting {} to {} for observation {}".format(
                cop_catalog.POLARIZATION_TYPE,
                observation[cop_catalog.POLARIZATION_TYPE],
                observation[cop_catalog.REFOBS_ID],
            )
        )
        catalog.post_to_es(observation, es_index)
        # create/update observation accountability entry
        # first get datatake and search observation catalog and see if there is an entry there yet
        # if not, create a new entry, otherwise append the observation to that entry
        set_accountability_record(observation_catalog, observation)
    logger.info(
        "Successfully ingested observation data from the following COP file: {}".format(
            cop_file_name
        )
    )


if __name__ == "__main__":
    main()
