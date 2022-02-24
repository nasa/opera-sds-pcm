"""
Class that contains the post process steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""

from chimera.postprocess_functions import PostProcessFunctions
import ast
import os
import traceback
from datetime import datetime

from util.common_util import convert_datetime

from opera_chimera.accountability import OperaAccountability

from commons.logger import logger
from commons.es_connection import get_grq_es, get_mozart_es
from pass_accountability.es_connection import get_pass_accountability_connection
from observation_accountability.es_connection import get_observation_accountability_connection

from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

pass_es = get_pass_accountability_connection(logger)
observation_es = get_observation_accountability_connection(logger)


class OperaPostProcessFunctions(PostProcessFunctions):
    def __init__(self, context, pge_config, settings, job_result):
        ancillary_es = get_grq_es(logger)
        mozart_es = get_mozart_es(logger)
        PostProcessFunctions.__init__(
            self, context, pge_config, settings, job_result, mozart_es, ancillary_es
        )
        logger.info("job_result: {}".format(job_result))
        self.accountability = OperaAccountability(self._context)

    def _associate_products(self, output_types, products):
        results = {}
        if self.accountability.step == "L0B":
            # going through each output_type and checking if it was created
            for output_type in output_types:
                # finding the search tearm ex: L0B_L_RRSD or L0B_L_HST_DRT
                search_term = "_".join(output_type.split("_")[2:])
                # going through each product and seing if the search term is there (HST_DRT in product_id)
                for i in range(0, len(products)):
                    product_id = os.path.basename(products[i])
                    if search_term in product_id:
                        results[output_type] = product_id
                        products.remove(products[i])
                        break
            return results
        else:
            return {
                output_types[0]: products[0]
            }

    def update_pass_product_accountability(self):
        """
        Function to update newly created products in the pass accountability index of ES
        :returns: updated values that will be set in the updated documents
        """
        try:
            logger.info("update_pass_product_accountability index: {}".format(
                self.accountability.index))
            logger.info("Starting Update Pass Product Accountability")
            accountability_docs = None
            # execute query against the accountability index
            # get es doc id for accountability
            try:
                accountability_docs = self.accountability.get_entries()
            except Exception:
                logger.warn(
                    "Failed to fetch docs: {}".format(
                        traceback.format_exc()
                    )
                )

            output_type = list(self._pge_config.get(
                oc_const.OUTPUT_TYPES).keys())[0]
            # NECESSARY specific check to account for L0A and Time Extractor Differentiation
            if self._pge_config.get(oc_const.PGE_NAME) == "L0A":
                output_type += "_PP"
            # from self._pge_config get value of output_type + "_id" -> key
            output_type_key = output_type + "_id"
            # from self._pge_config get the value of output_type and add "_job_id" -> key
            ouput_type_job_key = output_type + "_job_id"
            product_id = None
            job_id = None
            if oc_const.OUTPUT_DATASETS in self._job_result:
                task_id = self._job_result.get(oc_const.TASK_ID_FIELD)
                job_id = self._job_result.get(oc_const.JOB_ID_FIELD)
                logger.info("job_id: {}".format(job_id))
                product_id = os.path.basename(
                    self._job_result.get(oc_const.OUTPUT_DATASETS)[0])
                # Again need to check if it's L0A job so I can make the necessary change to the product name for the
                # time extractor
                if self._pge_config.get(oc_const.PGE_NAME) == "L0A":
                    product_id = "_RRST_PP".join(product_id.split("_RRST"))
                # grab the task from mozart
                try:
                    job_id_exists = (job_id != "" and job_id is not None)
                    if not job_id_exists:
                        query = {
                            "query": {"bool": {"must": [{"term": {"_id": task_id}}]}}
                        }
                        result = self._mozart_es.search(
                            body=query, index=oc_const.TASK_INDEX)
                        hits = result.get("hits", {}).get("hits", [])
                        task_result_str = hits[0].get(
                            "_source").get("event").get("result")
                        if task_result_str is None:
                            logger.info("hits: {}".format(hits))
                            if len(hits) < 1:
                                raise Exception(
                                    "could not retrieve task with this task id: {}".format(
                                        task_id)
                                )
                        logger.info("task_result: {}".format(task_result_str))
                        task_result = ast.literal_eval(task_result_str)
                        job_id = task_result.get("payload_id")
                except Exception:
                    logger.warn(
                        "Exception caught when retrieving task doc: {}".format(
                            traceback.format_exc()
                        )
                    )
            else:
                products_staged, prev_context, message, job_id = self._get_job()
                # let's get the job status via the job_id
                # Append new {key: value} pairs to es doc and commit doc
                if len(products_staged) > 0:
                    product_id = products_staged[0]
                else:
                    raise Exception(
                        "No products found: {}".format(products_staged))
            input_dataset_type = self.accountability.input_dataset_type
            input_dataset_id = self.accountability.input_dataset_id
            logger.info("product_id: {}\njob_id: {}]\nprimary_input: {}\tinput_dataset_id: {}".format(
                product_id, job_id, input_dataset_type, input_dataset_id))
            if output_type_key == "ldf_id":
                raise Exception("trying to overwrite ldf_id for some reason")
            updated_values = {output_type_key: product_id}
            updated_values[oc_const.LAST_MOD] = datetime.now().isoformat()
            if job_id is not None:
                updated_values[ouput_type_job_key] = job_id
            updated_doc = {
                "doc_as_upsert": True,
                "doc": updated_values,
            }
            try:
                for doc in accountability_docs:
                    self.accountability._update_doc(
                        doc.get("_id"),
                        updated_doc
                    )
            except Exception:
                logger.warn(
                    "Exception caught when trying to update accountability doc: {}".format(
                        traceback.format_exc()
                    )
                )
            # returning updated_values as the pseudo_context
            return updated_doc["doc"]
        except Exception:
            logger.warn("failed to run pass postprocess function")
            return {}

    def update_observation_product_accountability(self):
        try:
            logger.info("update_observation_product_accountability index: {}".format(
                self.accountability.index))
            accountability_docs = None
            # execute query against the accountability index
            # get es doc id for accountability
            try:
                accountability_docs = self.accountability.get_entries()
            except Exception:
                logger.warn(
                    "Failed to fetch docs: {}".format(
                        traceback.format_exc()
                    )
                )
            # get the job id
            job_id = ""
            products_list = []
            if oc_const.OUTPUT_DATASETS in self._job_result:
                job_id = self._job_result.get(oc_const.JOB_ID_FIELD)
                products_list = self._job_result.get(oc_const.OUTPUT_DATASETS)
            else:
                products_list, prev_context, message, job_id = self._get_job()
                # let's get the job status via the job_id
                # Append new {key: value} pairs to es doc and commit doc
            output_types = list(self._pge_config.get(
                oc_const.OUTPUT_TYPES).keys())

            products = self._associate_products(output_types, products_list)
            logger.info("step is {}".format(self.accountability.step))

            if len(accountability_docs) == 0:
                logger.info(
                    "Could not find accountability_docs!!! creating new entries myself"
                )
                input_dataset_type = self.accountability.input_dataset_type
                input_dataset_id = self.accountability.input_dataset_id
                # setting up new entry
                metadata = self._context.get("product_metadata").get("metadata")
                l0a_rrsts = metadata.get("found_l0a_rrsts")
                observation_ids = metadata.get("observation_ids")
                dt_state_config_satus = metadata.get("state")
                begin_time = metadata.get("datatake_begin_time")
                end_time = metadata.get("datatake_end_time")
                processing_type = ""
                if self._context.get("processing_type") != "urgent":
                    processing_type = "nominal"
                else:
                    processing_type = "urgent"

                records = []
                if "_id" not in input_dataset_type:
                    input_dataset_type += "_id"
                new_entry = {
                    input_dataset_type: input_dataset_id,
                    "state-config_satus": dt_state_config_satus,
                    "L0A_L_RRST_ids": l0a_rrsts,
                    "observation_ids": observation_ids,
                    "begin_time": begin_time,
                    "end_time": end_time,
                    "created_at": convert_datetime(datetime.utcnow()),
                    oc_const.LAST_MOD: convert_datetime(datetime.utcnow()),
                    "refrec_id": "",
                    "processing_type": processing_type,
                    "input_dataset_not_found": True,
                }

                if input_dataset_type == "datatake-state-config":
                    new_entry["datatake_id"] = "_".join(
                        input_dataset_id.split("_")[0:-1])

                for prod_type in products:
                    output_type_key = prod_type
                    if "_id" not in output_type_key:
                        output_type_key += "_id"
                    product_id = products[prod_type]
                    new_entry[output_type_key] = product_id
                new_entry[self.accountability.step + "_job_id"] = job_id
                records.append(new_entry)
                try:
                    observation_es.post(records)
                except Exception:
                    logger.warn(
                        "failed to create new observation accountability entries")
                    logger.error("error: {}".format(traceback.format_exc()))
                return {
                    "records": records
                }
            else:
                updated_values = {
                    oc_const.LAST_MOD: datetime.now().isoformat(),
                    "state-config_satus": self._context.get("product_metadata").get("metadata").get("state")
                }

                for prod_type in products:
                    output_type_key = prod_type
                    if "_id" not in output_type_key:
                        output_type_key += "_id"
                    product_id = products[prod_type]
                    updated_values[output_type_key] = product_id

                updated_values[self.accountability.step + "_job_id"] = job_id

                updated_doc = {
                    "doc_as_upsert": True,
                    "doc": updated_values,
                }

                for doc in accountability_docs:
                    try:
                        self.accountability._update_doc(
                            doc.get("_id"),
                            updated_doc
                        )
                    except Exception:
                        logger.warn(
                            "Exception caught when trying to update accountability doc: {}".format(
                                traceback.format_exc()
                            )
                        )
                return updated_doc
            # returning updated_values as the pseudo_context
        except Exception:
            logger.warn("failed to run observation postprocess function")
            logger.error("error: {}".format(traceback.format_exc()))
            return {}

    def update_track_frame_product_accountability(self):
        try:
            job_id = ""
            products_list = []
            if oc_const.OUTPUT_DATASETS in self._job_result:
                job_id = self._job_result.get(oc_const.JOB_ID_FIELD)
                products_list = self._job_result.get(oc_const.OUTPUT_DATASETS)
            else:
                products_list, prev_context, message, job_id = self._get_job()
            records = self.accountability.set_products(products_list, job_id=job_id)
            self.accountability.set_status("job-completed")

            return {"updated_records": records}
        except Exception:
            logger.warn("failed to run pass postprocess function")
            return {}
