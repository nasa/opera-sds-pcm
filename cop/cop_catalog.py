import os
import json
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

REFOBS_ID = "refobs_id"
ES_INDEX = "cop_catalog"
TIURDROP_ES_INDEX = "tiurdrop_catalog"
ES_TYPE = "observation"
HEADER = "header"
PRIORITY = "priority"
REF_START_DATETIME_ISO = "ref_start_datetime_iso"
REF_END_DATETIME_ISO = "ref_end_datetime_iso"
CMD_LSAR_START_DATETIME_ISO = "cmd_lsar_start_datetime_iso"
CMD_LSAR_END_DATETIME_ISO = "cmd_lsar_end_datetime_iso"
DATATAKE_ID = "datatake_id"
URGENT_RESPONSE = "urgent_response"
LSAR_CONFIG_ID = "lsar_config_id"
SSAR_CONFIG_ID = "ssar_config_id"
POLARIZATION_TYPE = "polarization_type"


class CopCatalog(ElasticsearchUtility):
    """
    Class to handle ingestion of COP contents into ElasticSearch

    https://github.com/hysds/hysds_commons/blob/develop/hysds_commons/elasticsearch_utils.py
    ElasticsearchUtility methods
        index_document
        get_by_id
        query
        search
        get_count
        delete_by_id
        update_document
    """

    def __add_mapping(self, index, mapping_type):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mappings_file = "{}_mappings.json".format(mapping_type)
        mappings_file = os.path.join(current_dir, "es_mapping", mappings_file)

        with open(mappings_file) as json_file:
            self.es.indices.put_mapping(index=index, body=json.load(json_file))

    def create_index(
        self, index=ES_INDEX, mapping_type=ES_TYPE, delete_old_index=False
    ):
        if delete_old_index is True:
            self.es.indices.delete(index=index, ignore=404)
            if self.logger:
                self.logger.info("Deleted old index: {}".format(index))

        self.es.indices.create(index=index)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(index))

        self.__add_mapping(index, mapping_type)  # Add mapping
        if self.logger:
            self.logger.info("Successfully add mapping to index {}".format(index))

    def delete_index(self, index=ES_INDEX):
        self.es.indices.delete(index=index, ignore=404)
        self.logger.info("Successfully deleted index: {}".format(index))

    def post(self, observations, header=None, index=ES_INDEX):
        """
        Post observations into ElasticSearch.

        :param observations: A list of COP observations to ingest.
        :param header: Header information associated with the observations.
        If present, they will get included with each observation document posted.
        :param index: Specify the index where the documents will get posted to.
        :return:
        """
        for observation in observations:
            if header:
                observation[HEADER] = header
            else:
                observation[HEADER] = {}

            if self.logger:
                self.logger.info("observation: {}".format(observation))
            self.post_to_es(observation, index)

    def post_to_es(self, document, index):
        """
        Posts the given document to ElasticSearch.

        :param document: The document to ingest.
        :return:
        """
        _id = "{}".format(document[REFOBS_ID])
        result = self.index_document(index=index, id=_id, body=document)
        if self.logger:
            self.logger.info("document indexed: {}".format(result))
