"""
Class that contains the precondition evaluation steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""

import argparse
import copy
import inspect
import json
import os
import re
import traceback
from datetime import datetime
from pathlib import PurePath
from typing import Dict, List
from urllib.parse import urlparse

import boto3
import psutil
from chimera.precondition_functions import PreConditionFunctions

from commons.constants import product_metadata
from commons.es_connection import get_grq_es
from commons.logger import logger
from commons.logger import LogLevels
from hysds.utils import get_disk_usage
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from util import datasets_json_util
from util.common_util import convert_datetime, get_working_dir
from util.geo_util import bounding_box_from_slc_granule
from util.pge_util import (download_object_from_s3,
                           get_input_hls_dataset_tile_code,
                           write_pge_metrics)
from util.type_util import set_type
from tools.stage_dem import main as stage_dem
from tools.stage_ionosphere_file import VALID_IONOSPHERE_TYPES
from tools.stage_worldcover import main as stage_worldcover

ancillary_es = get_grq_es(logger)


class OperaPreConditionFunctions(PreConditionFunctions):
    def __init__(self, context, pge_config, settings, job_params):
        PreConditionFunctions.__init__(
            self, context, pge_config, settings, job_params)

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

    def get_cnm_version(self):
        # we may need to choose different CNM data version for different product types
        # for now, it is set as CNM_VERSION in settings.yaml
        cnm_version = self._settings.get(oc_const.CNM_VERSION)
        print("cnm_version: {}".format(cnm_version))
        return {"cnm_version": cnm_version}

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

    def get_rtc_s1_num_workers(self):
        """
        Determines the number of workers/cores to assign to an RTC-S1 as a
        fraction of the total available.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        available_cores = os.cpu_count()

        # Use 3/4th of the available cores
        num_workers = max(int(round((available_cores * 3) / 4)), 1)

        logger.info(f"Allocating {num_workers} core(s) out of {available_cores} available")

        rc_params = {
            "num_workers": str(num_workers)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_polarization(self):
        """
        Determines the polarization setting for the CSLC-S1 or RTC-S1 job based
        on the file name of the input SLC granule.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        slc_filename = metadata['FileName']

        slc_regex = "(S1A|S1B)_IW_SLC__1S(?P<pol>SH|SV|DH|DV).*"

        result = re.search(slc_regex, slc_filename)

        if not result:
            raise RuntimeError(
                f'Could not parse Polarization from SLC granule {slc_filename}'
            )

        pol = result.groupdict()['pol']

        logger.info(f'Parsed Polarization mode {pol} from SLC granule {slc_filename}')

        polarization_map = {
            'SH': 'co-pol',
            'SV': 'co-pol',
            'DH': 'dual-pol',
            'DV': 'dual-pol'
        }

        slc_polarization = polarization_map[pol]

        rc_params = {
            oc_const.POLARIZATION: slc_polarization
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_static_layers_enabled(self):
        """Gets the setting for the enable_static_layers flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_name = self._pge_config.get('pge_name')
        pge_shortname = pge_name[3:].upper()

        logger.info(f'Getting ENABLE_STATIC_LAYERS setting for PGE {pge_shortname}')

        enable_static_layers = self._settings.get(pge_shortname).get("ENABLE_STATIC_LAYERS")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]
        processing_mode = metadata[oc_const.PROCESSING_MODE_KEY]

        # Static layer generation should always be disabled for historical processing mode
        if processing_mode == oc_const.PROCESSING_MODE_HISTORICAL:
            logger.info(f"Processing mode for {pge_name} is set to {processing_mode}, "
                        f"static layer generation will be DISABLED.")
            enable_static_layers = False

        rc_params = {
            "product_type": (
                f"{pge_shortname}_STATIC" if enable_static_layers else f"{pge_shortname}"
            )
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_safe_file(self):
        """
        Obtains the input SAFE file for use with an CSLC-S1 or RTC-S1 job.
        This local path is then configured as the value of safe_file_path within the
        interim RunConfig.

        The SAFE file is manually localized here, so it will be available for
        use when obtaining the corresponding DEM.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        s3_product_path = f"{self._context['product_path']}/{metadata['FileName']}"
        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        logger.info(f"working_dir : {working_dir}")
        logger.info(f"s3_product_path : {s3_product_path}")
        logger.info(f"s3_bucket: {s3_bucket}")
        logger.info(f"output_filepath: {output_filepath}")

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="SAFE"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.SAFE_FILE_PATH: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_orbit_file(self):
        """
        Obtains the S3 location of the orbit file configured for use with a
        CSLC-S1 or RTC-S1 job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        s3_product_path = self._context['product_path']

        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket_name = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(s3_bucket_name)
        s3_objects = bucket.objects.filter(Prefix=s3_key)

        orbit_file_objects = list(
            filter(lambda s3_object: s3_object.key.endswith('.EOF'), s3_objects)
        )

        if len(orbit_file_objects) < 1:
            raise RuntimeError(
                f'Could not find an orbit file within the S3 location {s3_product_path}'
            )

        orbit_file_object = orbit_file_objects[0]

        s3_orbit_file_path = f"s3://{s3_bucket_name}/{orbit_file_object.key}"

        # Assign the s3 location of the orbit file to the chimera config,
        # it will be localized for us automatically
        rc_params = {
            oc_const.ORBIT_FILE_PATH: s3_orbit_file_path
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_burst_database(self):
        """
        Copies the static burst database file configured for use with an SLC-based
        job to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'opera_burst_database.sqlite3')

        s3_bucket = self._pge_config.get(oc_const.GET_SLC_S1_BURST_DATABASE, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_SLC_S1_BURST_DATABASE, {}).get(oc_const.S3_KEY)

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Burst Database"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.BURST_DATABASE_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_dem(self):
        """
        Stages a DEM file corresponding to the region covered by an input
        S1 SLC SAFE archive.

        The manifest.safe file is extracted from the archive and used to
        determine the lat/lon bounding box of the S1 swath. This bbox is then
        used with the stage_dem tool to obtain the appropriate DEM.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        # get the local file path of the input SAFE archive (should have already
        # been downloaded by the get_safe_file precondition function)
        safe_file_path = self._job_params.get(oc_const.SAFE_FILE_PATH)

        # get s3_bucket param
        s3_bucket = self._pge_config.get(oc_const.GET_SLC_S1_DEM, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_SLC_S1_DEM, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        logger.info(f"working_dir : {working_dir}")
        logger.info(f"safe_file_path: {safe_file_path}")
        logger.info(f"s3_bucket: {s3_bucket}")
        logger.info(f"s3_key: {s3_key}")
        logger.info(f"output_filepath: {output_filepath}")

        if not safe_file_path:
            raise RuntimeError(f'No value set for {oc_const.SAFE_FILE_PATH} in '
                               f'job parameters. Please ensure the get_safe_file '
                               f'precondition function has been run prior to this one.')

        bbox = bounding_box_from_slc_granule(safe_file_path)

        logger.info(f"Derived DEM bounding box: {bbox}")

        pge_name = self._pge_config.get('pge_name')
        pge_shortname = pge_name[3:].upper()

        logger.info(f'Getting ANCILLARY_MARGIN setting for PGE {pge_shortname}')

        margin = self._settings.get(pge_shortname).get("ANCILLARY_MARGIN")

        logger.info(f'Using margin value of {margin} with staged DEM')

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = s3_key
        args.outfile = output_filepath
        args.filepath = None
        args.margin = margin  # KM
        args.log_level = LogLevels.INFO.value
        args.bbox = bbox
        args.tile_code = None

        pge_metrics = self.get_opera_ancillary(ancillary_type='S1 DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_tec_file(self):
        """
        Stages an Ionosphere Correction (TEC) file for use with a CSLC-S1
        job. The name of the SLC archive to be processed is used to obtain
        the date of the corresponding TEC file to download. The stage_ionosphere_file.py
        script is then used to perform the download.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        s3_product_path = self._context['product_path']

        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket_name = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(s3_bucket_name)
        s3_objects = bucket.objects.filter(Prefix=s3_key)

        # Find the available Ionosphere files staged by the download job
        ionosphere_file_objects = []
        for ionosphere_file_type in VALID_IONOSPHERE_TYPES + ['RAP', 'FIN']:
            ionosphere_file_objects.extend(
                list(filter(lambda s3_object: ionosphere_file_type in s3_object.key, s3_objects))
            )
        logger.info(f"{ionosphere_file_objects}=")

        # May not of found any Ionosphere files during download phase, so check now
        if len(ionosphere_file_objects) < 1:
            raise RuntimeError(
                f'Could not find an Ionosphere file within the S3 location {s3_product_path}'
            )

        # There should only have been one file downloaded, but any should
        # work so just take the first
        ionosphere_file_object = ionosphere_file_objects[0]

        s3_ionosphere_file_path = f"s3://{s3_bucket_name}/{ionosphere_file_object.key}"

        # Assign the s3 location of the Ionosphere file to the chimera config,
        # it will be localized for us automatically
        rc_params = {
            oc_const.TEC_FILE: s3_ionosphere_file_path
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

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

    def get_product_version(self):
        """Assigns the product version specified in settings.yaml to PGE RunConfig"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        version_key = self._pge_config.get(oc_const.GET_PRODUCT_VERSION, {}).get(oc_const.VERSION_KEY)

        product_version = self._settings.get(version_key)

        if not product_version:
            raise RuntimeError(
                f"No value set for {version_key} in settings.yaml"
            )

        rc_params = {
            oc_const.PRODUCT_VERSION: product_version
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_data_validity_start_date(self):
        """Gets the setting for the data_validity_start_date flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_name = self._pge_config.get('pge_name')
        pge_shortname = pge_name[3:].upper()

        logger.info(f'Getting DATA_VALIDITY_START_DATE setting for PGE {pge_shortname}')

        data_validity_start_time = self._settings.get(pge_shortname).get("DATA_VALIDITY_START_DATE")

        rc_params = {
            "data_validity_start_date": data_validity_start_time
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_ancillary_inputs_coverage_flag(self):
        """Gets the setting for the check_ancillary_inputs_coverage flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        check_ancillary_inputs_coverage = self._settings.get("DSWX_HLS").get("CHECK_ANCILLARY_INPUTS_COVERAGE")

        rc_params = {
            "check_ancillary_inputs_coverage": check_ancillary_inputs_coverage
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_apply_ocean_masking_flag(self):
        """Gets the setting for the apply_ocean_masking flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        apply_ocean_masking = self._settings.get("DSWX_HLS").get("APPLY_OCEAN_MASKING")

        rc_params = {
            "apply_ocean_masking": apply_ocean_masking
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_hls_dem(self):
        """
        This function downloads dems over the bbox provided in the PGE yaml config,
        or derives the appropriate bbox based on the tile code of the product's
        metadata (if available).
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        # get s3_bucket param
        s3_bucket = self._pge_config.get(oc_const.GET_DSWX_HLS_DEM, {}).get(oc_const.S3_BUCKET)

        # get bbox param
        bbox = self._pge_config.get(oc_const.GET_DSWX_HLS_DEM, {}).get(oc_const.BBOX)

        if bbox:
            # Convert to list if we were given a space-delimited string
            if not isinstance(bbox, list):
                bbox = list(map(float, bbox.split()))

            logger.info(f"Got bbox from PGE config: {bbox}")

        # get MGRS tile code, if available from product metadata
        tile_code = get_input_hls_dataset_tile_code(self._context)

        if tile_code:
            logger.info(f'Derived MGRS tile code {tile_code} from product metadata')
        else:
            logger.warning('Could not determine a tile code from product metadata')

        # do checks
        if bbox is None and tile_code is None:
            raise RuntimeError(
                f"Can not determine a region to obtain DEM for.\n"
                f"The product metadata must specify an MGRS tile code, "
                f"or the 'bbox' parameter must be provided in the "
                f"'{oc_const.GET_DSWX_HLS_DEM}' area of the PGE config"
            )

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = ""
        args.outfile = output_filepath
        args.filepath = None
        args.margin = int(self._settings.get("DSWX_HLS", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged DEM')

        # Provide both the bounding box and tile code, stage_dem.py should
        # give preference to the tile code over the bbox if both are provided.
        args.bbox = bbox
        args.tile_code = tile_code

        pge_metrics = self.get_opera_ancillary(ancillary_type='DSWx DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_landcover(self):
        """
        Copies the static landcover file configured for use with a DSWx-HLS job
        to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'landcover.tif')

        s3_bucket = self._pge_config.get(oc_const.GET_LANDCOVER, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_LANDCOVER, {}).get(oc_const.S3_KEY)

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Landcover"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.LANDCOVER_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_worldcover(self):
        """
        This function downloads a Worldcover map over the bbox provided in the
        PGE yaml config, or derives the appropriate bbox based on the tile code
        of the product's metadata (if available).
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'worldcover.vrt')

        # get s3 bucket/key params
        s3_bucket = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.S3_BUCKET)
        worldcover_ver = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.WORLDCOVER_VER)
        worldcover_year = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.WORLDCOVER_YEAR)

        # get bbox param
        bbox = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.BBOX)

        if bbox:
            # Convert to list if we were given a space-delimited string
            if not isinstance(bbox, list):
                bbox = list(map(float, bbox.split()))

            logger.info(f"Got bbox from PGE config: {bbox}")

        # get MGRS tile code, if available from product metadata
        tile_code = get_input_hls_dataset_tile_code(self._context)

        if tile_code:
            logger.info(f'Derived MGRS tile code {tile_code} from product metadata')
        else:
            logger.warning('Could not determine a tile code from product metadata')

        if bbox is None and tile_code is None:
            raise RuntimeError(
                f"Can not determine a region to obtain a Worldcover map for.\n"
                f"The product metadata must specify an MGRS tile code, "
                f"or the '{oc_const.BBOX}' parameter must be provided in the "
                f"'{oc_const.GET_WORLDCOVER}' area of the PGE config"
            )

        # Set up arguments to stage_worldcover.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = ""
        args.worldcover_ver = worldcover_ver
        args.worldcover_year = worldcover_year
        args.outfile = output_filepath
        args.margin = int(self._settings.get("DSWX_HLS", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged Worldcover')

        # Provide both the bounding box and tile code, stage_worldcover.py should
        # give preference to the tile code over the bbox if both are provided.
        args.bbox = bbox
        args.tile_code = tile_code

        pge_metrics = self.get_opera_ancillary(ancillary_type='Worldcover',
                                               output_filepath=output_filepath,
                                               staging_func=stage_worldcover,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.WORLDCOVER_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_shoreline_shapefiles(self):
        """
        Copies the set of static shoreline shapefiles configured for use with a
        DSWx-HLS job to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        rc_params = {}

        s3_bucket = self._pge_config.get(oc_const.GET_SHORELINE_SHAPEFILES, {}).get(oc_const.S3_BUCKET)
        s3_keys = self._pge_config.get(oc_const.GET_SHORELINE_SHAPEFILES, {}).get(oc_const.S3_KEYS)

        for s3_key in s3_keys:
            output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

            pge_metrics = download_object_from_s3(
                s3_bucket, s3_key, output_filepath, filetype="Shoreline Shapefile"
            )

            write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

            # Set up the main shapefile which is configured in the PGE RunConfig
            if output_filepath.endswith(".shp"):
                rc_params = {
                    oc_const.SHORELINE_SHAPEFILE: output_filepath
                }

        # Make sure the .shp file was included in the set of files localized from S3
        if not rc_params:
            raise RuntimeError("No .shp file included with the localized Shoreline Shapefile dataset.")

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_opera_ancillary(self, ancillary_type, output_filepath, staging_func, staging_func_args):
        """
        Handles common operations for obtaining ancillary data used with OPERA
        PGE processing
        """
        pge_metrics = {"download": [], "upload": []}

        loc_t1 = datetime.utcnow()

        try:
            staging_func(staging_func_args)
            logger.info(f"Created {ancillary_type} file : {output_filepath}")
        except Exception as e:
            trace = traceback.format_exc()
            error = str(e)
            raise RuntimeError(
                f"Failed to download {ancillary_type} file, reason: {error}\n{trace}"
            )

        loc_t2 = datetime.utcnow()
        loc_dur = (loc_t2 - loc_t1).total_seconds()
        path_disk_usage = get_disk_usage(output_filepath)

        pge_metrics["download"].append(
            {
                "url": output_filepath,
                "path": output_filepath,
                "disk_usage": path_disk_usage,
                "time_start": loc_t1.isoformat() + "Z",
                "time_end": loc_t2.isoformat() + "Z",
                "duration": loc_dur,
                "transfer_rate": path_disk_usage / loc_dur,
            }
        )
        logger.info(json.dumps(pge_metrics, indent=2))

        return pge_metrics

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

    def get_dswx_hls_input_filepaths(self) -> Dict:
        """Returns a partial RunConfig containing the s3 paths of the published L2_HLS products."""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        work_dir = get_working_dir()
        with open(PurePath(work_dir) / "datasets.json") as fp:
            datasets_json_dict = json.load(fp)
        with open(PurePath(work_dir) / "_job.json") as fp:
            job_json_dict = json.load(fp)
            dataset_type = job_json_dict["params"]["dataset_type"]

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]
        product_paths: List[str] = []
        for file in metadata["Files"]:
            # Example publish location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
            publish_location = str(datasets_json_util.find_publish_location_s3(datasets_json_dict, dataset_type).parent) \
                .removeprefix("s3:/").removeprefix("/")  # handle prefix changed by PurePath
            product_path = f's3://{publish_location}/{self._context["input_dataset_id"]}/{file["FileName"]}'
            product_paths.append(product_path)


        # Used in conjunction with PGE Config YAML's $.localize_groups and its referenced properties in $.runconfig.
        # Compare key names of $.runconfig entries, referenced indirectly via $.localize_groups, with this dict.
        return {"L2_HLS": product_paths}

    def set_daac_product_type(self):
        """
        Sets the DAAC product type
        :return: a DAAC product type that will use to populate collection value in CNM-S msg
        """
        logger.info("Calling function {} function".format(oc_const.SET_DAAC_PRODUCT_TYPE))
        template = self._pge_config.get(oc_const.SET_DAAC_PRODUCT_TYPE, {}).get(
            "template", None
        )

        if template:
            daac_product_type = template.format(**self._job_params)
            print("daac_product_type: {}".format(daac_product_type))
            return {product_metadata.DAAC_PRODUCT_TYPE: daac_product_type}
        else:
            raise RuntimeError(
                "Must define a 'template' field for the {} function".format(
                    oc_const.SET_DAAC_PRODUCT_TYPE
                )
            )
