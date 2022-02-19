import os
import json
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

REFREC_ID = "refrec_id"
ES_INDEX = "jobs_accountability_catalog"
ES_TYPE = "job"
HEADER = "header"


class JobAccountabilityCatalog(ElasticsearchUtility):
    """
    Class to handle ingestion of All products past job-RRST_Accountability contents into ElasticSearch

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
        current_directory = os.path.dirname(os.path.abspath(__file__))
        mappings_file = "{}_mappings.json".format(mapping_type)
        mappings_file = os.path.join(current_directory, "es_mapping", mappings_file)

        with open(mappings_file) as json_file:
            self.es.indices.put_mapping(index=index, body=json.load(json_file))

    def create_index(self, index=ES_INDEX, mapping_type=ES_TYPE, delete_old_index=False):
        if delete_old_index is True:
            self.es.indices.delete(index=index, ignore=404)
            if self.logger:
                self.logger.info("Deleted old index: {}".format(index))

        self.es.indices.create(index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(index))

        self.__add_mapping(index, mapping_type)  # Add mapping
        if self.logger:
            self.logger.info("Successfully add mapping to index {}".format(index))

    def delete_index(self, index=ES_INDEX):
        self.es.indices.delete(index=index, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(index))

    def post(self, records, header=None, index=ES_INDEX):
        """
        Post records into ElasticSearch.

        :param records: A list of Pass Accountability records to ingest.
        :param header: Header information associated with the records.
        If present, they will get included with each record document posted.
        :param index: Specify the index where the documents will get posted to.
        :return:
        """
        for record in records:
            if header:
                record[HEADER] = header
            else:
                record[HEADER] = {}
            if self.logger:
                self.logger.info("record: {}".format(record))
            self.__post_to_es(record, index)

    def __post_to_es(self, document, index):
        """
        Posts the given document to ElasticSearch.

        :param document: The document to ingest.
        :return:
        """
        _id = document[REFREC_ID]
        result = self.index_document(index=index, id=_id, body=document)
        if self.logger:
            self.logger.info("document indexed: {}".format(result))
