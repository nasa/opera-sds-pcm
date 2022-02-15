import traceback
import os
from datetime import datetime

from util.common_util import convert_datetime

from chimera.commons.accountability import Accountability

from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

from chimera.logger import logger

from commons.es_connection import get_grq_es
# from pass_accountability.es_connection import get_pass_accountability_connection
# from observation_accountability.es_connection import get_observation_accountability_connection

accountability_es = get_grq_es(logger)
# accountability_es = get_pass_accountability_connection(logger)
# obs_es = get_observation_accountability_connection(logger)

PGE_STEP_DICT = {
    "L0A": "L0A_L_RRST_PP",
    "Time_Extractor": "L0A_L_RRST",
    "L0B": "L0B_L_RRSD"
}

INDECES = {
    "": None,
    "pass": oc_const.PASS_ACCOUNTABILITY_INDEX,
    "observation": oc_const.OBSERVATION_ACCOUNTABILITY_INDEX,
    "track_frame": oc_const.TRACK_FRAME_ACCOUNTABILITY_INDEX
}


class OperaAccountability(Accountability):
    def __init__(self, context):
        Accountability.__init__(self, context)
        self.index = INDECES[context.get("es_index", "")]

    def _search(self, query):
        """
        searches for entries
        :param query: query to send to elasticsearch
        :type query: python dictionary in es format
        :returns: list of hits
        """

        hits = []
        try:
            result = {}
            if self.index in INDECES.values():
                result = accountability_es.search(
                    body=query, index=self.index
                )
            else:
                raise Exception("Index Not Found")
            hits = result.get("hits", {}).get("hits", [])
        except Exception:
            logger.warn("Failed to search this query: {}".format(query))
        return hits

    def _update_doc(self, id, body):
        """
        update document with given id with passed body
        :param id: document _id
        :type id: _id string
        :param body: dictionary to upsert to the document
        :type body: dictionary in the following format
        {
            "doc_as_upsert": True,
            "doc": <dictionary of updated values>,
        }
        :returns:
        """
        try:
            if self.index in INDECES.values():
                accountability_es.update_document(
                    id=id,
                    index=self.index,
                    body=body
                )
            else:
                raise Exception("Index Not Found")
        except Exception:
            logger.warn("Failed to update {} with {}".format(id, body))

    def get_entries(self):
        """
        retrieves entries as a list
        :returns: entries given a given context
        """
        conditions = [
            (self.input_dataset_type + ".keyword", self.input_dataset_id)]
        conditions = accountability_es.construct_bool_query(
            conditions)
        query = {"query": {"bool": {"must": conditions}}}

        logger.info("Query for grabbing entries: {}".format(query))
        accountability_docs = []
        try:
            hits = self._search(query)
            logger.info("hits with .keyword count : {}".format(len(hits)))
            accountability_docs = hits
        except Exception:
            logger.warn(
                "Could not retrieve associated accountability entries\t {}: {}".format(
                    self.input_dataset_type, self.input_dataset_id)
            )
        return accountability_docs

    def set_products(self, products, entries=None, job_id=None):
        if entries is None:
            entries = self.get_entries()
        for entry in entries:
            _id = entry.get("_id")
            source = entry.get("_source")
            logger.info("source: {}".format(source))
            logger.info("step: {}".format(self.step))
            records = []

            products = list(map(
                lambda prod: prod if "/data/work/" not in prod else os.path.basename(prod), products))
            logger.info("products: {}".format(products))
            source_copy = dict(source)

            output_type = PGE_STEP_DICT[self.step] + "_id"
            output_job_type = PGE_STEP_DICT[self.step] + "_job_id"

            first_product = products.pop(0)

            source[output_type] = first_product
            source[output_job_type] = job_id
            try:
                body = {
                    "doc_as_upsert": True,
                    "doc": source
                }
                self._update_doc(_id, body)
            except Exception:
                logger.error(
                    "Failed to update entry with first product {}".format(first_product)
                )
                return

            if len(products) > 0:
                records = []
                for product in products:
                    new_entry = dict(source_copy)
                    new_entry[output_type] = product
                    records.append(new_entry)
                try:
                    self.post(records)
                except Exception:
                    logger.error(
                        "Failed to create new entries with newly created products {}".format(
                            products
                        )
                    )
            records.append(source)
        return records

    def set_status(self, status):
        """
        Function to implement a custom status setting for a specific index storing accountability statuses for associated products
        :param status:
        :return:
        """
        # if self.step not in PGE_STEP_DICT:
        #     return
        accountability_docs = self.get_entries()
        if len(accountability_docs) >= 1:
            try:
                output_type_status = ""
                if self.step == "L0B":
                    output_type_status = self.step + "_status"
                else:
                    output_type = PGE_STEP_DICT[self.step]
                    output_type_status = output_type + "_status"
                updated_values = {
                    output_type_status: status,
                    "last_modified": convert_datetime(datetime.utcnow())
                }
                updated_doc = {
                    "doc_as_upsert": True,
                    "doc": updated_values,
                }
                for doc in accountability_docs:
                    self._update_doc(
                        doc.get("_id"),
                        updated_doc
                    )
            except Exception:
                logger.warn(
                    "Failed to update accountability docs: {}".format(
                        traceback.format_exc()
                    )
                )
        else:
            return
