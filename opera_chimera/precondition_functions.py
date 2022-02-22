"""
Class that contains the precondition evaluation steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""
import copy
import inspect
import json
import re
import os
import traceback
from typing import Dict, List

import psutil

from cop import cop_catalog

from datetime import datetime, timedelta

from opera_chimera.accountability import OperaAccountability

from commons.logger import logger
from commons.es_connection import get_grq_es

from commons.constants import product_metadata

from chimera.precondition_functions import PreConditionFunctions
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

from rost import catalog as rost_catalog

from util.common_util import convert_datetime, to_datetime
from util.type_util import set_type
from util.stuf_util import get_stuf_info_from_xml

try:
    from tools.stage_dem import main as stage_dem
except Exception:
    pass


ancillary_es = get_grq_es(logger)

OE_TYPES = [
    "POE",
    "MOE",
    "NOE",
    "FOE",
]  # Orbit Ephemeris types in order from best to worst


class OperaPreConditionFunctions(PreConditionFunctions):
    def __init__(self, context, pge_config, settings, job_params):
        PreConditionFunctions.__init__(
            self, context, pge_config, settings, job_params)
        self.accountability = OperaAccountability(self._context)

    def set_product_time(self):
        """
        Set ProductionDateTime as PGE binary needs that to create/name the log file
        :return:
        """
        return {
            "{}".format(oc_const.PRODUCTION_DATETIME): convert_datetime(
                datetime.utcnow()
            )
        }

    def get_crid(self):
        crid = self._settings.get(oc_const.CRID)
        rc_params = {oc_const.COMPOSITE_RELEASE_ID: crid}
        return rc_params

    def __get_run_config_metadata(self, run_config_key, context):
        value = None
        for key in run_config_key.split("."):
            if value is not None:
                value = value[key]
            else:
                value = context[key]

        return value

    def __get_converted_data(self, data):
        """
        convert data to a format matched with ES data
        """

        if data and data.isnumeric():
            return int(data)
        elif data.upper() == "DESCENDING":
            return "D"
        elif data.upper() == "ASCENDING":
            return "A"

        return data

    def get_hardcoded_metadata(self):
        return self._pge_config.get(oc_const.GET_HARDCODED_METADATA, {})

    def get_product_counter(self, testmode=None):
        """
        To get the product counter

        :return:
        """
        counter = 1

        if testmode is None:

            if "value" in self._pge_config.get(oc_const.GET_PRODUCT_COUNTER, {}):
                return {
                    oc_const.PRODUCT_COUNTER: self._pge_config.get(
                        oc_const.GET_PRODUCT_COUNTER
                    ).get("value")
                }

            primary_output = self._pge_config.get(oc_const.PRIMARY_OUTPUT)
            index = "grq_*_{}".format(primary_output.lower())
            clauses = []
            pc_key = self._pge_config.get(oc_const.GET_PRODUCT_COUNTER, {})
            for term, job_params_key in pc_key.items():
                value = self.__get_converted_data(
                    self.__get_run_config_metadata(job_params_key, self._job_params)
                )
                if value:
                    clauses.append({"match": {term: value}})
                else:
                    raise RuntimeError(
                        "{} does not exist in the job_params.".format(job_params_key)
                    )

            query = {"query": {"bool": {"must": clauses}}}
            sort_clause = "metadata.{}:desc".format(product_metadata.PRODUCT_COUNTER)
            try:
                result = ancillary_es.search(
                    body=query, index=index, sort=sort_clause)
                hits = result.get("hits", {}).get("hits", [])
                logger.info("hits count : {}".format(len(hits)))
                if len(hits) > 0:
                    counter = int(
                        hits[0]
                        .get("_source")
                        .get("metadata", {})
                        .get(product_metadata.PRODUCT_COUNTER)
                    )
                    logger.debug("existing count : {}".format(counter))
                    counter = counter + 1
            except Exception:
                logger.warn(
                    "Exception caught in getting product counter: {}".format(
                        traceback.format_exc()
                    )
                )
                logger.warn("Setting product counter to 1")
        logger.info("Setting product counter: {}".format(str(counter).zfill(3)))
        return {oc_const.PRODUCT_COUNTER: counter}

    def get_products(self):
        """
        Returns the names of the products generated by the previous step and its
        metadata as a dict

        :return: dict containing the product s3 paths
        """
        logger.info("Evaluating precondition {}".format(oc_const.PRODUCT_PATHS))
        input_file_path_key = self._pge_config.get(oc_const.PRIMARY_INPUT)
        product_paths = []

        if self._context.get(oc_const.PRODUCT_PATHS):
            ppaths = self._context.get(oc_const.PRODUCT_PATHS, [])
            pmets = self._context.get(oc_const.PRODUCTS_METADATA, [])

            get_products_config = self._pge_config.get(oc_const.GET_PRODUCTS, {})
            is_state_config_trigger = get_products_config.get(oc_const.IS_STATE_CONFIG_TRIGGER, False)

            if is_state_config_trigger is True:
                file_names_met = get_products_config.get(oc_const.FILE_NAMES_KEY, None)
                if file_names_met is None:
                    raise RuntimeError("Missing {} in the PGE config for the {} precondtion".format(
                        oc_const.FILE_NAMES_KEY, oc_const.GET_PRODUCTS))
                file_names = pmets.get("metadata", {}).get(file_names_met)
                if not file_names:
                    raise RuntimeError("Missing '{}' from input metadata: {}".format(
                        file_names_met, json.dumps(pmets.get("metadata", {}), indent=2)))
                elif len(file_names) != len(ppaths):
                    raise RuntimeError("Length of '{}', {}, is not equal to length of product_paths {}".format(
                        file_names_met, len(file_names), len(ppaths)))
                for i in range(0, len(ppaths)):
                    file_ppath = os.path.join(ppaths[i], file_names[i])
                    logger.debug("{}: Adding product {}".format(oc_const.PRODUCT_PATHS, file_ppath))
                    product_paths.append(file_ppath)
            else:
                if isinstance(ppaths, list):
                    for (ppath, pmet) in zip(ppaths, pmets):
                        if "metadata" in pmet:
                            metadata = pmet.get("metadata")
                        else:
                            metadata = pmet
                        file_ppath = os.path.join(
                            ppath, metadata.get(product_metadata.FILE_NAME)
                        )
                        logger.debug(
                            "{}: Adding product {}".format(
                                oc_const.PRODUCT_PATHS, file_ppath
                            )
                        )
                        product_paths.append(file_ppath)
                else:
                    if "metadata" in pmets:
                        metadata = pmets.get("metadata")
                    else:
                        metadata = pmets
                    file_ppath = os.path.join(
                        ppaths, metadata.get(product_metadata.FILE_NAME)
                    )
                    logger.debug(
                        "{}: Adding product {}".format(
                            oc_const.PRODUCT_PATHS, file_ppath
                        )
                    )
                    product_paths.append(file_ppath)

        else:
            raise RuntimeError(
                "{} NOT FOUND in provided context file".format(oc_const.PRODUCT_PATHS)
            )

        logger.info(
            "Setting {} input products for key {} : {}".format(
                len(product_paths), input_file_path_key, ", ".join(product_paths)
            )
        )

        if len(product_paths) == 0:
            raise RuntimeError(
                "No products found to set as input in the context")

        return {input_file_path_key: product_paths}

    def get_product_metadata(self):
        """
        To get the metadata that was extracted from the products generated by the
        previous PGE run

        :return:
        """
        logger.info("Evaluating precondition {}".format(
            oc_const.GET_PRODUCT_METADATA))
        try:
            if self._context.get(oc_const.PRODUCTS_METADATA) is None:
                raise ValueError(
                    "No product metadata key found in the input context")
            keys = self._pge_config.get(
                oc_const.GET_PRODUCT_METADATA).get("keys", [])
            metadata = dict()
            metadata_obj = self._context.get(oc_const.PRODUCTS_METADATA)
            logger.debug("Found Product Metadata: {}".format(json.dumps(metadata_obj)))
            attribute_names = self._pge_config.get(oc_const.GET_PRODUCT_METADATA).get("attribute_names", {})
            if isinstance(metadata_obj, list):
                for product in metadata_obj:
                    metadata.update(self.__get_keys_from_dict(product.get("metadata"), keys, attribute_names))
            else:
                metadata.update(self.__get_keys_from_dict(metadata_obj.get("metadata"), keys, attribute_names))
            logger.info("Returning the following metadata to the job_params: {}".format(json.dumps(metadata)))
            logger.debug(json.dumps(metadata))
            return metadata

        except Exception as e:
            logger.error("Could not extract product metadata: {}".format(traceback))
            raise RuntimeError("Could not extract product metadata: {}".format(e))

    def get_metadata(self):
        """
        Returns a dict with only the key: value pair for keys in 'keys' from the
        input_context

        :return: dict or raises error if not found
        """
        logger.info("Evaluating precondition {}".format(oc_const.GET_METADATA))
        keys = self._pge_config.get(oc_const.GET_METADATA).get("keys")
        try:
            metadata = self.__get_keys_from_dict(self._context, keys)
            logger.info("Returning the following context metadata to the job_params: {}".format(json.dumps(metadata)))
            return metadata
        except Exception as e:
            logger.error(
                "Could not extract metadata from input "
                "context: {}".format(traceback.format_exc())
            )
            raise RuntimeError(
                "Could not extract metadata from input context: {}".format(e)
            )

    def __get_data_date_times(self):
        """
        Gets the range date time of the inputs. In the case of multiple inputs, it'll return
        the min range begin time and the max range end time.

        :return:
        """
        min_range_begin_date_time = None
        max_range_end_date_time = None
        metadata_obj = self._context.get(oc_const.PRODUCTS_METADATA)
        if isinstance(metadata_obj, list):
            product_mets = metadata_obj
        else:
            product_mets = [metadata_obj]

        for product_met in product_mets:
            met = product_met.get("metadata")
            try:
                rbt = met.get(product_metadata.RANGE_BEGINNING_DATE_TIME)
                rbt = convert_datetime(rbt)
            except Exception:
                raise RuntimeError(
                    "{} does not exist in the product_metadata of the context".format(
                        product_metadata.RANGE_BEGINNING_DATE_TIME
                    )
                )

            try:
                ret = met.get(product_metadata.RANGE_ENDING_DATE_TIME)
                ret = convert_datetime(ret)
            except Exception:
                raise RuntimeError(
                    "{} does not exist in the product_metadata of the context".format(
                        product_metadata.RANGE_ENDING_DATE_TIME
                    )
                )

            if min_range_begin_date_time is None:
                min_range_begin_date_time = rbt
            else:
                if rbt < min_range_begin_date_time:
                    min_range_begin_date_time = rbt

            if max_range_end_date_time is None:
                max_range_end_date_time = ret
            else:
                if ret > max_range_end_date_time:
                    max_range_end_date_time = ret

        return (
            convert_datetime(min_range_begin_date_time),
            convert_datetime(max_range_end_date_time),
        )

    def __get_attribute_name(self, key, attribute_names, substitution_map):
        attribute_name = key
        if attribute_names:
            if key in attribute_names:
                template = attribute_names[key]
                attribute_name = template.format(**substitution_map)
        return attribute_name

    def __check_missing(self, input_dict):
        for key in input_dict:
            if isinstance(input_dict.get(key), dict):
                pass

    def __get_keys_from_dict(self, input_dict, keys, attribute_names=None):
        """
        Returns a dict with the requested keys from the input dict
        :param input_dict:
        :param keys:
        :param attribute_names:
        :return:
        """
        new_dict = dict()
        if attribute_names is None:
            attribute_names = dict()
        for key in keys:
            if key in input_dict:
                attribute_name = attribute_names.get(key, key)
                new_dict.update({attribute_name: input_dict.get(key)})
        return new_dict

    def get_dyn_anc_over_time_range(self):
        run_config_parameters = dict()
        product_types = self._pge_config.get(
            oc_const.DYN_ANCILLARY_FILES).get("types")

        beginning_time = self._job_params.get(
            product_metadata.RANGE_BEGINNING_DATE_TIME
        )
        ending_time = self._job_params.get(product_metadata.RANGE_ENDING_DATE_TIME)

        for product_type in product_types:
            results = ancillary_es.perform_es_range_intersection_query(
                beginning_date_time=beginning_time,
                ending_date_time=ending_time,
                met_field_beginning_date_time="starttime",
                met_field_ending_date_time="endtime",
                index="grq_*_{}".format(product_type.lower()),
            )
            datastore_refs = ancillary_es.get_datastore_refs(results)

            if len(datastore_refs) == 0:
                raise Exception(
                    "Could not find any {} over time range {} to {} ".format(
                        product_type, beginning_time, ending_time
                    )
                )

            values = list()
            for datastore_ref in datastore_refs:
                values.append(str(datastore_ref))
            attribute_name = product_type
            run_config_parameters[attribute_name] = values
        return run_config_parameters

    def get_product_most_recent(self):
        """
        Get most recent by creation date

        :return:
        """
        run_config_parameters = dict()
        product_types = self._pge_config.get(
            oc_const.MOST_RECENT_FILES).get("types")
        for product_type in product_types:
            prod_type = eval("product_metadata.{}".format(product_type.upper()))
            query = {"query": {"match_all": {}}}
            ancillary = ancillary_es.get_latest_product_by_version(
                index="grq_*_{}".format(prod_type.lower()),
                es_query=query,
                version_metadata="metadata.{}".format(
                    product_metadata.FILE_CREATION_DATE_TIME
                ),
            )
            run_config_parameters[product_type] = ancillary
        return run_config_parameters

    def get_product_most_recent_version(self):
        """
        Get most recent by creation date

        :return:
        """
        run_config_parameters = dict()
        product_types = self._pge_config.get(oc_const.MOST_RECENT_VERSION_FILES).get(
            "types"
        )
        attribute_names = self._pge_config.get(
            oc_const.MOST_RECENT_VERSION_FILES, {}
        ).get(oc_const.ATTRIBUTE_NAMES_KEY, {})
        for product_type in product_types:
            query = {"query": {"match_all": {}}}
            ancillary = ancillary_es.get_latest_product_by_version(
                index="grq_*_{}".format(product_type.lower()), es_query=query
            )
            attribute_name = attribute_names.get(product_type, product_type)
            run_config_parameters[attribute_name] = ancillary
        return run_config_parameters

    def get_orbit_ephemeris(self):
        """
        Select one or more Orbit Ephemeris files that completely cover the given
        time range

        :return:
        """
        get_oe_params = self._pge_config.get(oc_const.GET_ORBIT_EPHEMERIS, {})
        if not get_oe_params:
            raise RuntimeError(
                "Missing {} area in the PGE config".format(oc_const.GET_ORBIT_EPHEMERIS)
            )

        oe_type = self._pge_config.get(oc_const.GET_ORBIT_EPHEMERIS, {}).get(
            "type", "best"
        )
        types_to_search = []
        if oe_type == "best":
            types_to_search = OE_TYPES
        else:
            oe_type = oe_type.upper()
            if oe_type not in OE_TYPES:
                raise ValueError(
                    "{} is not one of the valid Orbit Ephemeris types: {}".format(
                        oe_type, OE_TYPES
                    )
                )
            types_to_search.append(oe_type)

        try:
            beginning_time = self._job_params.get(
                get_oe_params.get("beginning_date_time")
            )
            ending_time = self._job_params.get(get_oe_params.get("ending_date_time"))
        except KeyError:
            raise RuntimeError(
                "Missing 'beginning_date_time' and/or 'ending_date_time' setting in the "
                "'{}' area of the PGE config".format(oc_const.GET_ORBIT_EPHEMERIS)
            )

        padding = int(get_oe_params.get("padding", 0))
        if padding != 0:
            logger.info(
                "Padding begin and end times by {} hour(s)".format(padding))

            bt = convert_datetime(beginning_time)
            bt = bt - timedelta(hours=padding)
            beginning_time = convert_datetime(bt)

            et = convert_datetime(ending_time)
            et = et + timedelta(hours=padding)
            ending_time = convert_datetime(et)

        logger.info(
            "Searching for the best Orbit Ephemeris file: "
            "type(s) = {}, beginning_time={}, ending_time={}".format(
                types_to_search, beginning_time, ending_time
            )
        )

        records = []
        for data_type in types_to_search:
            recs2check = ancillary_es.perform_aggregate_range_intersection_query(
                beginning_date_time=beginning_time,
                ending_date_time=ending_time,
                met_field_beginning_date_time="starttime",
                met_field_ending_date_time="endtime",
                sort_list=[
                    "metadata.{}:desc".format(product_metadata.CREATION_DATE_TIME)
                ],
                index="grq_*_{}".format(data_type.lower()),
            )

            if recs2check:
                logger.info("Records found for type {}".format(data_type))
                best_fit_records = ancillary_es.select_best_fit(
                    beginning_time, ending_time, recs2check
                )
                if best_fit_records:
                    ids = list()
                    for r in best_fit_records:
                        ids.append(r.get("_source", {}).get("id"))
                    logger.info("best fit records: {}".format(ids))
                    records = best_fit_records
                    break
                else:
                    ids = []
                    for rec in recs2check:
                        ids.append(rec.get("_source").get("id"))
                    message = (
                        "Record(s) for type {} do not completely cover the data "
                        "date time range {} to {}: {}".format(
                            type, beginning_time, ending_time, ids
                        )
                    )
                    if oe_type == "best":
                        logger.info(message)
                        # if no records match yet, use these non-zero results
                        #  to have something
                        if len(records) == 0:
                            records = list()
                    else:
                        logger.error(message)
                        raise RuntimeError(message)

        datastore_refs = ancillary_es.get_datastore_refs_from_es_records(
            records)
        if len(datastore_refs) == 0:
            raise ValueError(
                "Could not find any Orbit Ephemeris files of type(s) {} over {} to {} ".format(
                    types_to_search, beginning_time, ending_time
                )
            )
        else:
            logger.info(
                "Found the best available Orbit Ephemeris file(s): {}".format(
                    datastore_refs
                )
            )

        return {oc_const.ORBIT_EPHEMERIS_FILE: datastore_refs}

    def set_pge_job_name(self):
        """
        Sets the PGE job name when the job submission step is run.

        :return: a job name that will go into the job id during the job submission step.
        """
        logger.info("Evaluating {} preconditions".format(
            oc_const.SET_PGE_JOB_NAME))
        template = self._pge_config.get(
            oc_const.SET_PGE_JOB_NAME, {}).get("template")
        pge_job_name = template.format(**self._job_params)
        return {oc_const.PGE_JOB_NAME: pge_job_name}

    def set_l0b_mock_metadata(self):
        """
        Creates a mock metadata set specifically for L0B outputs when PGE runs are simulated.
        Specifically, it creates mock metadata for each observation since the PGE is designed
        to produce 1 output per observation.

        :return: A list of metadata dictionaries
        """
        logger.info("Evaluating {} preconditions".format(oc_const.SET_L0B_MOCK_METADATA))
        mock_metadata = {oc_const.MOCK_METADATA: {}}
        if self._settings.get(oc_const.PGE_SIM_MODE, True):
            config = {
                oc_const.SET_MOCK_METADATA: self._pge_config.get(oc_const.SET_L0B_MOCK_METADATA)
            }
            self._pge_config.update(config)
            init_mock_met = self.set_mock_metadata()
            l0b_l_rrsd_met = init_mock_met.get(oc_const.MOCK_METADATA).pop(product_metadata.L0B_L_RRSD)
            l0b_l_rrsd_met_list = list()
            mock_met_config = self._pge_config.get(oc_const.SET_L0B_MOCK_METADATA, {}).get(
                product_metadata.L0B_L_RRSD, {})
            if oc_const.OBSERVATIONS not in self._job_params:
                raise RuntimeError("'{}' missing from job_params: {}".format(oc_const.OBSERVATIONS,
                                                                             json.dumps(self._job_params, indent=2)))
            for observation in self._job_params.get(oc_const.OBSERVATIONS, {}):
                obs_id = observation.get(oc_const.PLANNED_OBSERVATION_ID)
                logger.info("Mocking metadata for observation: {}".format(obs_id))
                obs_met = copy.deepcopy(l0b_l_rrsd_met)
                for met_key, dyn_value in mock_met_config.get("observations", {}).items():
                    if isinstance(dyn_value, list):
                        obs_met[met_key] = list()
                        for dv in dyn_value:
                            v = observation.get(dv, None)
                            if v:
                                if isinstance(v, list):
                                    values = list()
                                    for i in v:
                                        values.append(set_type(i))
                                    obs_met[met_key].extend(values)
                                elif isinstance(v, dict):
                                    obs_met[met_key].append(v)
                                else:
                                    obs_met[met_key].append(set_type(v))
                            else:
                                raise RuntimeError(
                                    "Missing {} from observation dictionary {}".format(
                                        dv, obs_id
                                    )
                                )
                    else:
                        v = observation.get(dyn_value, None)
                        if v:
                            if isinstance(v, list):
                                values = list()
                                for i in v:
                                    values.append(set_type(i))
                                obs_met[met_key] = values
                            elif isinstance(v, dict):
                                obs_met[met_key] = v
                            else:
                                obs_met[met_key] = set_type(v)
                        else:
                            raise RuntimeError(
                                "Missing {} from observation dictionary {}".format(
                                    dyn_value, obs_id
                                )
                            )
                l0b_l_rrsd_met_list.append(obs_met)
            init_mock_met.get(oc_const.MOCK_METADATA).update({product_metadata.L0B_L_RRSD: l0b_l_rrsd_met_list})
            mock_metadata = init_mock_met

        return mock_metadata

    def set_mock_metadata(self):
        """
        Creates a mock metadata set when PGE runs are simulated.

        :return: metadata dictionary
        """
        logger.info("Evaluating {} preconditions".format(
            oc_const.SET_MOCK_METADATA))
        mock_metadata = {}
        if self._settings.get(oc_const.PGE_SIM_MODE, True):
            for output_type in self._pge_config.get(
                oc_const.SET_MOCK_METADATA, {}
            ).keys():
                mock_met_config = self._pge_config.get(
                    oc_const.SET_MOCK_METADATA, {}
                ).get(output_type, {})

                metadata = mock_met_config.get("static", {})
                for met_key, dyn_value in mock_met_config.get("dynamic", {}).items():
                    if isinstance(dyn_value, list):
                        metadata[met_key] = list()
                        for dv in dyn_value:
                            v = self._job_params.get(dv, None)
                            if v:
                                if isinstance(v, list):
                                    values = list()
                                    for i in v:
                                        values.append(set_type(i))
                                    metadata[met_key].extend(values)
                                elif isinstance(v, dict):
                                    metadata[met_key].append(v)
                                else:
                                    metadata[met_key].append(set_type(v))
                            else:
                                raise RuntimeError(
                                    "Missing {} from job params and/or runconfig".format(
                                        dv
                                    )
                                )
                    else:
                        v = self._job_params.get(dyn_value, None)
                        if v:
                            if isinstance(v, list):
                                values = list()
                                for i in v:
                                    values.append(set_type(i))
                                metadata[met_key] = values
                            elif isinstance(v, dict):
                                metadata[met_key] = v
                            else:
                                metadata[met_key] = set_type(v)
                        else:
                            raise RuntimeError(
                                "Missing {} in job params and/or runconfig".format(
                                    dyn_value
                                )
                            )

                # TODO: Figure out a way to generalize this
                if self._pge_config.get(oc_const.PGE_NAME) == oc_const.L0A:
                    regex = (
                        r"NISAR_S\d{3}_\w{2,3}_\w{3,4}_M\d{2}_P\d{5}_R\d{2}_C\d{2}_G\d{2}_(\d{4})_(\d{3})_"
                        r"(\d{2})_(\d{2})_(\d{2})_\d{9}\.vc\d{2}$"
                    )
                    match = re.search(regex, self._job_params.get("NEN_L_RRST")[0])
                    if not match:
                        raise RuntimeError(
                            "Failed to parse date from {}.".format(
                                self._job_params.get("NEN_L_RRST")[0]
                            )
                        )
                    begin_date_time = convert_datetime(
                        "{}-{}T{}:{}:{}".format(*match.groups()),
                        strformat="%Y-%jT%H:%M:%S",
                    )
                    match = re.search(
                        regex, self._job_params.get("NEN_L_RRST")[-1])
                    if not match:
                        raise RuntimeError(
                            "Failed to parse date from {}.".format(
                                self._job_params.get("NEN_L_RRST")[-1]
                            )
                        )
                    end_date_time = convert_datetime(
                        "{}-{}T{}:{}:{}".format(*match.groups()),
                        strformat="%Y-%jT%H:%M:%S",
                    )
                    metadata[product_metadata.DAPHNE_MIN_TIME_TAG] = convert_datetime(
                        begin_date_time
                    )
                    metadata[product_metadata.DAPHNE_MAX_TIME_TAG] = convert_datetime(
                        end_date_time
                    )

                # Convert the TrackFramePolygon to a json structure
                if product_metadata.BOUNDING_POLYGON in metadata:
                    if isinstance(metadata[product_metadata.BOUNDING_POLYGON], str):
                        metadata[product_metadata.BOUNDING_POLYGON] = \
                            json.loads(metadata[product_metadata.BOUNDING_POLYGON])

                # make VCID uppercase
                if product_metadata.VCID in metadata:
                    metadata[product_metadata.VCID] = metadata[
                        product_metadata.VCID
                    ].upper()

                mock_metadata[output_type] = metadata

        return {oc_const.MOCK_METADATA: mock_metadata}

    def set_l0b_base_names(self):
        """
        Sets the base name to be used when simulating PGE output products.

        :return: A base name.
        """
        base_names = {}
        if self._settings.get(oc_const.PGE_SIM_MODE, True):
            mock_met_copy = copy.deepcopy(
                self._job_params.get(oc_const.MOCK_METADATA, {})
            )

            for output_type in self._pge_config.get(oc_const.SET_L0B_BASE_NAMES, {}).keys():
                base_name_config = self._pge_config.get(oc_const.SET_L0B_BASE_NAMES, {}).get(
                    output_type, {}
                )
                dt_formats = base_name_config.get("date_time_formats", {})
                if output_type == product_metadata.L0B_L_RRSD:
                    base_names[output_type] = list()
                    rrsd_mock_met_list = mock_met_copy[output_type]
                    for rrsd_met in rrsd_mock_met_list:
                        print("rrsd_met : {}".format(json.dumps(rrsd_met, indent=2)))
                        for key in rrsd_met.keys():
                            if key in dt_formats.keys():
                                dt_value = convert_datetime(rrsd_met.get(key))
                                rrsd_met[key] = convert_datetime(dt_value,
                                                                 strformat=dt_formats.get(key))
                        template = base_name_config.get("template")
                        base_names[output_type].append(template.format(**rrsd_met))
                else:
                    for key in mock_met_copy[output_type].keys():
                        if key in dt_formats.keys():
                            dt_value = convert_datetime(mock_met_copy[output_type].get(key))
                            mock_met_copy[output_type][key] = convert_datetime(
                                dt_value, strformat=dt_formats.get(key)
                            )
                    template = base_name_config.get("template")
                    base_names[output_type] = template.format(
                        **mock_met_copy[output_type])

        return {oc_const.BASE_NAME: base_names}

    def set_base_name(self):
        """
        Sets the base name to be used when simulating PGE output products.

        :return: A base name.
        """
        base_names = {}
        if self._settings.get(oc_const.PGE_SIM_MODE, True):
            mock_met_copy = copy.deepcopy(
                self._job_params.get(oc_const.MOCK_METADATA, {})
            )

            for output_type in self._pge_config.get(oc_const.SET_BASE_NAME, {}).keys():
                base_name_config = self._pge_config.get(oc_const.SET_BASE_NAME, {}).get(
                    output_type, {}
                )

                dt_formats = base_name_config.get("date_time_formats", {})
                for key in mock_met_copy[output_type].keys():
                    if key in dt_formats.keys():
                        dt_value = convert_datetime(mock_met_copy[output_type].get(key))
                        mock_met_copy[output_type][key] = convert_datetime(
                            dt_value, strformat=dt_formats.get(key)
                        )

                template = base_name_config.get("template")
                base_names[output_type] = template.format(
                    **mock_met_copy[output_type])

        return {oc_const.BASE_NAME: base_names}

    def get_nominal_nens(self):
        """
        Currently, this is used during L0A PGE preconditions. According to the PGE team, the PGE will only accept
        nominal NEN files as input.

        This function will be used to filter out the bad NEN inputs by checking for Mode 0 in the file names.

        :return:
        """
        logger.info("Evaluating precondition 'get_nominal_nens'")
        input_file_path_key = self._pge_config.get(oc_const.PRIMARY_INPUT)

        pattern = (
            self._settings.get(oc_const.PRODUCT_TYPES, {})
            .get(product_metadata.NEN_L_RRST, {})
            .get(oc_const.PATTERN, None)
        )
        if not pattern:
            raise RuntimeError(
                "Cannot find {} product type definition in the settings.yaml".format(
                    product_metadata.NEN_L_RRST
                )
            )

        filtered_nens = list()
        logger.info(
            "NEN list prior to filtering: {}".format(
                self._job_params.get(input_file_path_key)
            )
        )
        for nen_input in self._job_params.get(input_file_path_key):
            match = pattern.search(nen_input)
            if match:
                keys = match.groupdict().keys()
                if product_metadata.MODE in keys:
                    mode = match.groupdict()[product_metadata.MODE]
                    if int(mode) == 0:
                        filtered_nens.append(nen_input)
                    else:
                        logger.info(
                            "Removing bad NEN file as it has a non-0 Mode of '{}': {}".format(
                                mode, os.path.basename(nen_input)
                            )
                        )
                else:
                    raise RuntimeError(
                        "Could not find {} in the metadata of the file name: {}".format(
                            product_metadata.MODE, nen_input
                        )
                    )
            else:
                raise RuntimeError(
                    "{} file does not match the expected {} product type pattern: {}".format(
                        os.path.basename(nen_input),
                        product_metadata.NEN_L_RRST,
                        pattern.pattern,
                    )
                )
        if len(filtered_nens) == 0:
            raise RuntimeError(
                "NEN list is now empty after filtering out the bad nens from the initial input list."
            )
        return {input_file_path_key: filtered_nens}

    def get_stuf_info(self):

        logger.info("Evaluating precondition 'get_stuf_info'")
        logger.info("self._job_params {}".format(self._job_params))
        beginning_time = self._job_params.get(product_metadata.RANGE_START_DATE_TIME)
        ending_time = self._job_params.get(product_metadata.RANGE_STOP_DATE_TIME)

        (
            orbit_num,
            cycle_num,
            relative_orbit_num,
            orbit_start_time,
            orbit_end_time,
            ctz,
            orbit_dir,
            eq_cross_time,
        ) = get_stuf_info_from_xml(beginning_time, ending_time)

        # parse stuf file and return
        CycleNumber = int(cycle_num)
        AbsoluteOrbitNumber = int(orbit_num)
        RelativeOrbitNumber = int(relative_orbit_num)
        OrbitDirection = orbit_dir
        LookDirection = "Left"

        rc_params = {
            oc_const.ABSOLUTE_ORBIT_NUMBER: AbsoluteOrbitNumber,
            oc_const.MISSION_CYCLE: CycleNumber,
            oc_const.RELATIVE_ORBIT_NUMBER: RelativeOrbitNumber,
            oc_const.ORBIT_DIRECTION: OrbitDirection,
            oc_const.LOOK_DIRECTION: LookDirection,
        }

        logger.info("get_stuf_info : rc_params : {}".format(rc_params))
        return rc_params

    def get_processing_type(self):
        processing_type = "PR"
        state_config_type = self._context.get(oc_const.DATASET_TYPE)
        if state_config_type == oc_const.DATATAKE_UR_STATE_CONFIG_DOC_TYPE or \
                state_config_type == oc_const.DATATAKE_UR_EXP_STATE_CONFIG_DOC_TYPE:
            processing_type = "UR"

        if self._context.get(oc_const.PRODUCTS_METADATA):
            pmets = self._context.get(oc_const.PRODUCTS_METADATA, [])
            is_urgent = pmets.get("metadata", {}).get(oc_const.IS_URGENT, False)

        rc_params = {oc_const.PROCESSINGTYPE: processing_type, oc_const.URGENT_RESPONSE_FIELD: is_urgent}
        logger.info("get_l0b_processing_type : rc_params : {}".format(rc_params))
        return rc_params

    def get_l0b_ancillary_files(self):
        logger.info("Evaluating precondition 'get_l0b_ancillary_files'")

        RadarConfigurationFile = None
        ChirpParameterFile = None
        WaveformConfigurationFile = None

        RadarConfigurationFiles = ancillary_es.get_latest_product_by_creation_time(
            index="grq_*_{}".format(oc_const.RADAR_CFG),
            sort_by="metadata.{}".format(product_metadata.PRODUCT_RECEIVED_TIME),
        )

        ChirpParameterFiles = ancillary_es.get_latest_product_by_creation_time(
            index="grq_*_{}".format(oc_const.CHIRP_PARAM),
            sort_by="metadata.{}".format(product_metadata.PRODUCT_RECEIVED_TIME),
        )

        WaveformConfigurationFiles = ancillary_es.get_latest_product_by_creation_time(
            index="grq_*_{}".format(oc_const.WAVEFORM.lower()),
            sort_by="metadata.{}".format(product_metadata.PRODUCT_RECEIVED_TIME),
        )

        if RadarConfigurationFiles and len(RadarConfigurationFiles["hits"]["hits"]) > 0:
            RadarConfigurationFile = ancillary_es.get_datastore_ref_from_es_record(
                RadarConfigurationFiles["hits"]["hits"][0]
            )[0]
        else:
            raise RuntimeError("Could not find any {} files in ES.".format(oc_const.RADAR_CFG))

        if ChirpParameterFiles and len(ChirpParameterFiles["hits"]["hits"]) > 0:
            ChirpParameterFile = ancillary_es.get_datastore_ref_from_es_record(
                ChirpParameterFiles["hits"]["hits"][0]
            )[0]
        else:
            raise RuntimeError("Could not find any {} files in ES.".format(oc_const.CHIRP_PARAM))

        if (
            WaveformConfigurationFiles
            and len(WaveformConfigurationFiles["hits"]["hits"]) > 0
        ):
            WaveformConfigurationFile = ancillary_es.get_datastore_ref_from_es_record(
                WaveformConfigurationFiles["hits"]["hits"][0]
            )[0]
        else:
            raise RuntimeError("Could not find any {} files in ES.".format(oc_const.WAVEFORM))

        rc_params = {
            oc_const.RADAR_CONFIGURATION_FILE: RadarConfigurationFile,
            oc_const.CHIRP_PARAMETER_FILE: ChirpParameterFile,
            oc_const.WAVE_CONFIGURATION_FILE: WaveformConfigurationFile,
        }

        logger.info("get_l0b_ancillary_files : rc_params : {}".format(rc_params))

        return rc_params

    def get_rost_data_from_cop_time(
        self, beginning_time, ending_time, met_field_beginning_time, met_field_ending_time
    ):

        total_number_rangelines = 0
        total_rangelines_to_skip = 0
        rost_results = ancillary_es.perform_es_range_intersection_query(
            beginning_date_time=beginning_time,
            ending_date_time=ending_time,
            padding=6,
            met_field_beginning_date_time=met_field_beginning_time,
            met_field_ending_date_time=met_field_ending_time,
            sort=["{}:asc".format(product_metadata.START_TIME_ISO)],
            index=rost_catalog.ES_INDEX,
        )
        if rost_results:
            rost_results = rost_results["hits"]["hits"]
            if len(rost_results) > 0:
                logger.info(
                    "\n{} ROST records found".format(len(rost_results)))
                prev_delta = None
                logger.info("observation time is {}".format(beginning_time))
                beginning_time = to_datetime(beginning_time) if type(beginning_time) is str else beginning_time
                for rec in rost_results:
                    rost_start_time = rec.get("_source").get(product_metadata.START_TIME_ISO)
                    format1 = re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2}Z')
                    format2 = re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2}.\d+Z')
                    if type(rost_start_time) is str:
                        if format1.match(rost_start_time+"Z") is not None:
                            rost_start_time = to_datetime(rost_start_time, "%Y-%m-%dT%H:%M:%SZ")
                        elif format2.match(rost_start_time+"Z") is not None:
                            rost_start_time = to_datetime(rost_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                        else:
                            rost_start_time = to_datetime(rost_start_time+"Z")
                    delta = abs(beginning_time - rost_start_time)
                    logger.info("delta: {}".format(delta))
                    if prev_delta is None:
                        nearest_match = True
                    elif delta < prev_delta:
                        nearest_match = True
                    else:
                        nearest_match = False
                    if nearest_match:
                        logger.info("Nearest match is {}, delta: {}".format(rost_start_time, delta))
                        logger.info("ROST rec : \n{}\n".format(json.dumps(rec, indent=2)))
                        total_number_rangelines = int(rec.get("_source").get("number_of_pulses"))
                        total_rangelines_to_skip = int(rec.get("_source").get("rangelines_to_skip_div_16"))
                    prev_delta = delta
            else:
                raise Exception(
                    "NO ROST record with range beginning_time={}".format(beginning_time)
                )
        else:
            raise Exception(
                "NO ROST record with range beginning_time={}".format(beginning_time)
            )
        return total_number_rangelines, total_rangelines_to_skip

    def get_observations_data(self):
        """
        This function gets observation data based on COP and ROST data within radar start and end time.
        This will be revisited after we receive samople COP and ROST data from PGE team
        and their relationship is better understood.
        """

        logger.info("Evaluating precondition 'get_observations'")

        observation_ids = self._job_params.get(product_metadata.OBSERVATION_IDS, None)

        if observation_ids is None:
            raise RuntimeError("Missing {} from job parameters".format(product_metadata.OBSERVATION_IDS))
        pmets = self._context.get(oc_const.PRODUCTS_METADATA, [])
        is_urgent = pmets.get("metadata", {}).get(oc_const.IS_URGENT, False)
        observations = list()
        for observation_id in observation_ids:
            logger.info("Getting observation info for: {}".format(observation_id))
            rec = ancillary_es.get_by_id(id=observation_id, index=cop_catalog.ES_INDEX)
            if rec.get("found", False) is True:
                observation_info = dict()
                logger.info("Observation record : \n{}\n".format(json.dumps(rec, indent=2)))
                observation_info[oc_const.PLANNED_OBSERVATION_ID] = rec.get("_id")
                observation_info[oc_const.IS_URGENT_OBSERVATION] = is_urgent
                observation_info[oc_const.CONFIGURATION_ID] = rec.get("_source").get(cop_catalog.LSAR_CONFIG_ID)
                lsar_start_datetime_iso = rec.get("_source").get(
                    cop_catalog.CMD_LSAR_START_DATETIME_ISO
                )
                lsar_end_datetime_iso = rec.get("_source").get(
                    cop_catalog.CMD_LSAR_END_DATETIME_ISO
                )
                try:
                    number_rangelines, rangelines_to_skip = self.get_rost_data_from_cop_time(
                        lsar_start_datetime_iso,
                        lsar_end_datetime_iso,
                        rost_catalog.START_TIME_ISO,
                        rost_catalog.END_TIME_ISO
                    )
                    observation_info[oc_const.TOTAL_NUMBER_RANGELINES] = number_rangelines
                    observation_info[oc_const.RANGELINES_TO_SKIP] = rangelines_to_skip

                except Exception as e:
                    raise RuntimeError(
                        "Number of range lines could not be found from the ROST records found with "
                        "cop start time={} and end time={}: {}".format(
                            lsar_start_datetime_iso, lsar_end_datetime_iso, str(e)
                        )
                    )
                observation_info[oc_const.START_TIME] = lsar_start_datetime_iso
                observation_info[oc_const.END_TIME] = lsar_end_datetime_iso
                # Need to hardcode this for now per ICS
                observation_info[oc_const.MISSION_CYCLE] = 1

                observations.append(observation_info)
            else:
                raise RuntimeError(
                    "NO COP record found with {} = {}".format(cop_catalog.REFOBS_ID, observation_id)
                )

        rc_params = {"Observations": observations}
        logger.info("get_observation_data : rc_params : {}".format(rc_params))
        return rc_params

    def get_dyn_anc_l1_l2(self):
        """
        For R2, the dyn ancillaries for L1/L2 PGEs to null. To accomodate that we have a dedicated precondition for now.
        :return:
        """
        dyn_anc = {
            oc_const.DEM_FILE: None,
            oc_const.ORBIT: None,
            oc_const.REFINED_POINTING: None,
            oc_const.EXT_CALIBRATION: None,
            oc_const.INT_CALIBRATION: None,
            oc_const.POL_CALIBRATION: None,
            oc_const.BOOK_CALIBRATION: None,
            oc_const.ANT_PATTERN: None,
            oc_const.WAVEFORM: None,
        }

        logger.info("Setting default dyn anc for GSLC : {}".format(dyn_anc))
        return dyn_anc

    def get_range_date_times(self):
        metadata = {}
        if self._pge_config.get(oc_const.PGE_NAME) == oc_const.L0B:
            if self._job_params.get(product_metadata.DATATAKE_START_DATE_TIME) and \
                    self._job_params.get(product_metadata.DATATAKE_STOP_DATE_TIME):
                metadata[product_metadata.RANGE_START_DATE_TIME] = \
                    self._job_params.get(product_metadata.DATATAKE_START_DATE_TIME)
                metadata[product_metadata.RANGE_STOP_DATE_TIME] = \
                    self._job_params.get(product_metadata.DATATAKE_STOP_DATE_TIME)
            elif self._job_params.get(product_metadata.OBSERVATION_BEGIN_TIME) and \
                    self._job_params.get(product_metadata.OBSERVATION_END_TIME):
                metadata[product_metadata.RANGE_START_DATE_TIME] = \
                    self._job_params.get(product_metadata.OBSERVATION_BEGIN_TIME)
                metadata[product_metadata.RANGE_STOP_DATE_TIME] = \
                    self._job_params.get(product_metadata.OBSERVATION_END_TIME)
            else:
                raise RuntimeError("Cannot find {}/{} or {}/{} in the job params".format(
                    product_metadata.DATATAKE_START_DATE_TIME, product_metadata.DATATAKE_STOP_DATE_TIME,
                    product_metadata.OBSERVATION_BEGIN_TIME, product_metadata.OBSERVATION_END_TIME))
        else:
            raise RuntimeError(
                "get_range_beginning_date_times precondition not implemented yet for PGE '{}'".format(
                    self._pge_config.get(oc_const.PGE_NAME)
                )
            )

        return metadata

    def get_file_size_limit(self):
        pge_name = self._pge_config.get(oc_const.PGE_NAME)
        file_size_limit = self._settings.get(pge_name, {}).get(oc_const.FILE_SIZE_LIMIT, "700M")
        rc_params = {oc_const.FILESIZELIMIT: file_size_limit}
        logger.info("get_file_size_limit : rc_params : {}".format(rc_params))
        return rc_params

    def get_number_of_threads(self):
        number_of_threads = psutil.cpu_count()
        rc_params = {oc_const.NUMBEROFTHREADS: number_of_threads}
        logger.info("get_number_of_threads : rc_params : {}".format(rc_params))
        return rc_params

    def get_number_of_threads_doubled(self):
        logger.info("Return value of psutil.cpu_count() = {}".format(psutil.cpu_count()))
        number_of_threads = (psutil.cpu_count() * 2) - 2 or 1

        rc_params = {oc_const.NUMBEROFTHREADS: number_of_threads}
        logger.info("get_number_of_threads : rc_params : {}".format(rc_params))
        return rc_params

    def get_track_frame_polygon(self):
        logger.info("Calling get_track_frame_polygon pre-condition function")
        # tf_poly = '{"type": "polygon", "coordinates": [[[2.109375, 24.84656534821976], [-7.3828125, 16.29905101458183], [-1.7578125, 9.795677582829743], [8.7890625, 18.646245142670608], [2.109375, 24.84656534821976]]]}'
        rc_params = {
            product_metadata.TRACK_FRAME_POLYGON: json.dumps(self._job_params.get(product_metadata.TRACK_FRAME_POLYGON))
        }
        logger.info("get_track_frame_polygon : rc_params : {}".format(rc_params))
        return rc_params

    def set_extra_pge_output_metadata(self):
        logger.info(
            "Calling {} pre-condition function".format(
                oc_const.SET_EXTRA_PGE_OUTPUT_METADATA
            )
        )
        extra_met = dict()
        for met_key, job_params_key in self._pge_config.get(
            oc_const.SET_EXTRA_PGE_OUTPUT_METADATA
        ).items():
            if job_params_key in self._job_params:
                extra_met[met_key] = self._job_params.get(job_params_key)
            else:
                raise RuntimeError(
                    "Cannot find {} in the job_params dictionary".format(job_params_key)
                )
        return {oc_const.EXTRA_PGE_OUTPUT_METADATA: extra_met}

    def get_gpu_enabled(self):
        logger.info(
            "Calling {} pre-condition function".format(oc_const.GPU_ENABLED))

        # read in SciFlo work unit json file and extract work directory
        work_unit_file = os.path.abspath("workunit.json")
        with open(work_unit_file) as f:
            work_unit = json.load(f)
        work_dir = os.path.dirname(work_unit["args"][0])

        # extract docker params for the PGE
        docker_params_file = os.path.join(work_dir, "_docker_params.json")
        with open(docker_params_file) as f:
            docker_params = json.load(f)
        container_image_name = self._context["job_specification"]["dependency_images"][
            0
        ]["container_image_name"]
        pge_docker_params = docker_params[container_image_name]

        # detect GPU
        gpu_enabled = (
            True if "gpus" in pge_docker_params.get("runtime_options", {}) else False
        )
        return {oc_const.GPU_ENABLED: gpu_enabled}

    def set_pcm_retrieval_id(self):
        logger.info(
            "Calling function {} function".format(oc_const.SET_PCM_RETRIEVAL_ID)
        )
        template = self._pge_config.get(oc_const.SET_PCM_RETRIEVAL_ID, {}).get(
            "template", None
        )

        if template:
            pcm_retrieval_id = template.format(**self._job_params)
            return {product_metadata.PCM_RETRIEVAL_ID: pcm_retrieval_id}
        else:
            raise RuntimeError(
                "Must define a 'template' field for the {} function".format(
                    oc_const.SET_PCM_RETRIEVAL_ID
                )
            )

    def get_gcov_job_params_from_context(self):
        """
        Returns a dict with only the key: value pair for keys in 'keys' from the
        input_context and do any PGE-specific value coercion for the GCOV PGE.

        :return: dict or raises error if not found
        """
        logger.info(
            "Evaluating precondition {}".format(
                oc_const.GET_GCOV_JOB_PARAMS_FROM_CONTEXT
            )
        )
        keys = self._pge_config.get(oc_const.GET_GCOV_JOB_PARAMS_FROM_CONTEXT).get(
            "keys"
        )
        try:
            metadata = self.__get_keys_from_dict(self._context, keys)
        except Exception as e:
            logger.error(
                "Could not extract metadata from input "
                "context: {}".format(traceback.format_exc())
            )
            raise RuntimeError(
                "Could not extract metadata from input context: {}".format(e)
            )

        # coerce output_posting
        logger.info(
            "Coercing output_posting. Current type: {}".format(
                type(metadata["output_posting"])
            )
        )
        metadata["output_posting"] = eval(metadata["output_posting"])
        logger.info("New type: {}".format(type(metadata["output_posting"])))

        # return full dot notation
        return {
            "processing.input_subset.fullcovariance": metadata["fullcovariance"],
            "processing.rtc.output_type": metadata["output_type"],
            "processing.rtc.algorithm_type": metadata["algorithm_type"],
            "processing.geocode.output_posting": metadata["output_posting"],
        }

    def get_bbox(self, dem_file=None):
        """
        Input : input_file_name
        Output : "min_lon minn_lat max_lon max_lot"
        This function finds the bbox from a dem file
        """
        from osgeo import osr, gdal

        if not dem_file:
            latlong = [-84.25936537447647, 34.297911285926524, -
                       82.11147803222626, 36.13956044986029]
            return latlong

        # get the existing coordinate system
        ds = gdal.Open(dem_file)
        old_cs = osr.SpatialReference()
        old_cs.ImportFromWkt(ds.GetProjectionRef())

        # create the new coordinate system
        wgs84_wkt = """
        GEOGCS["WGS 84",
            DATUM["WGS_1984",
                SPHEROID["WGS 84",6378137,298.257223563,
                    AUTHORITY["EPSG","7030"]],
                AUTHORITY["EPSG","6326"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.01745329251994328,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4326"]]"""
        new_cs = osr.SpatialReference()
        new_cs .ImportFromWkt(wgs84_wkt)

        # create a transform object to convert between coordinate systems
        transform = osr.CoordinateTransformation(old_cs, new_cs)

        # get the point to transform, pixel (0,0) in this case
        width = ds.RasterXSize
        height = ds.RasterYSize
        gt = ds.GetGeoTransform()
        minx = gt[0]
        miny = gt[3] + width * gt[4] + height * gt[5]
        maxx = gt[0] + width * gt[1] + height * gt[2]
        maxy = gt[3]

        # get the coordinates in lat long
        latlong1 = transform.TransformPoint(minx, miny)
        print(latlong1)
        print(len(latlong1))
        latlong2 = transform.TransformPoint(maxx, maxy)
        latlong = "{} {} {} {}".format(
            latlong1[0], latlong1[1], latlong2[0], latlong2[1])
        print(latlong)

        return latlong

    def get_dems(self):
        """
        Input : bbox : min_lon minn_lat max_lon max_lat
        Input : polygon : GeoJSON polygon
        This function downloads dems over the bbox or polygon
        """
        import argparse
        from shapely.geometry import shape
        from hysds.utils import get_disk_usage

        pge_metrics = {"download": [], "upload": []}

        work_unit_file = os.path.abspath("workunit.json")
        with open(work_unit_file) as f:
            work_unit = json.load(f)
        wd = os.path.dirname(work_unit['args'][0])

        print("get_dems : wd : {}".format(wd))
        get_dems_param = self._pge_config.get(
            oc_const.GET_DEMS, {})
        if not get_dems_param:
            raise RuntimeError("Missing {} area in the PGE config".format(
                oc_const.GET_DEMS))

        # get bbox param
        bbox = None
        if 'bbox' in get_dems_param:
            bbox_param = get_dems_param.get("bbox")
            if isinstance(bbox_param, list):
                bbox = bbox_param
            else:
                bbox = list(map(float, bbox_param.split()))

        # get polygon key param
        polygon_key = get_dems_param.get("polygon_key", None)

        # do checks
        if bbox is not None and polygon_key is not None:
            raise RuntimeError(
                "Cannot set both 'bbox' and 'polygon_key' "
                "parameters in {} area in the PGE config".format(oc_const.GET_DEMS)
            )
        if bbox is None and polygon_key is None:
            raise RuntimeError(
                "Set either the 'bbox' or 'polygon_key' "
                "parameter in {} area in the PGE config".format(oc_const.GET_DEMS)
            )

        # get bbox from bounding polygon
        if bbox is None:
            polygon = eval(f"product_metadata.{polygon_key}")
            bounding_polygon = self._job_params.get(polygon)
            if isinstance(bounding_polygon, str):
                bounding_polygon = json.loads(bounding_polygon)

            # extract bounding polygon bounds
            bbox = list(shape(bounding_polygon).bounds)

        print("bbox : {}".format(bbox))

        dem_file = os.path.join(wd, 'dem.vrt')

        args = argparse.Namespace()
        args.product = None
        args.filepath = wd
        args.outfile = dem_file
        args.margin = 5
        args.bbox = bbox

        loc_t1 = datetime.utcnow()
        try:
            stage_dem(args)
            print("get_dems : dem_file : {}".format(dem_file))
        except Exception as e:
            trace = traceback.format_exc()
            error = str(e)
            raise RuntimeError(
                "Failed to download dem_file: {}\n{}".format(error, trace)
            )

        loc_t2 = datetime.utcnow()
        loc_dur = (loc_t2 - loc_t1).total_seconds()
        path_disk_usage = get_disk_usage(dem_file)

        pge_metrics["download"].append(
            {
                "url": dem_file,
                "path": dem_file,
                "disk_usage": path_disk_usage,
                "time_start": loc_t1.isoformat() + "Z",
                "time_end": loc_t2.isoformat() + "Z",
                "duration": loc_dur,
                "transfer_rate": path_disk_usage / loc_dur,
            }
        )
        logger.info(json.dumps(pge_metrics, indent=2))

        with open(os.path.join(wd, "pge_metrics.json"), "x") as f:
            json.dump(pge_metrics, f, indent=2)

        rc_params = {
            oc_const.DEM_FILE: dem_file
        }
        logger.info("get_dems : rc_params : {}".format(rc_params))
        return rc_params

    def cast_string_to_int(self):
        logger.info("Calling {} pre-condition".format(oc_const.CAST_STRING_TO_INT))
        keys = self._pge_config.get(oc_const.CAST_STRING_TO_INT).get("keys")
        results = {}
        for key in keys:
            results[key] = int(self._job_params.get(key))
        logger.info("Casted Metadata Values: {}".format(json.dumps(results)))
        return results

    def get_pge_settings_values(self):
        logger.info("Calling {} pre-condition".format(oc_const.GET_PGE_SETTINGS_VALUES))
        pge_name = self._pge_config.get(oc_const.PGE_NAME)
        key_map = self._pge_config.get(oc_const.GET_PGE_SETTINGS_VALUES)
        pge_settings = self._settings.get(pge_name)
        results = {}
        for key, value in key_map.items():
            if key in pge_settings:
                results[value] = pge_settings.get(key)
            else:
                raise RuntimeError("Cannot find {} in the settings.yaml under the {} area.".format(key, pge_name))
        logger.info("Adding the following to the job params: {}".format(json.dumps(results)))
        return results

    def get_input_filepaths_from_state_config(self) -> Dict:
        """Returns a partial RunConfig containing the s3 paths of the published L2_HLS_DSWx products."""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]
        product_paths: List[str] = [product_path for band_or_qa, product_path in metadata.items() if band_or_qa != '@timestamp']

        # Used in conjunction with PGE Config YAML's $.localize_groups and its referenced properties in $.runconfig.
        # Compare key names of $.runconfig entries, referenced indirectly via $.localize_groups, with this dict.
        return {"L2_HLS": product_paths}
