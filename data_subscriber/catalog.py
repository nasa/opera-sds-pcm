import os
import json
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

REFREC_ID = "refrec_id"
ES_INDEX = "data_subscriber_product_catalog"
HEADER = "header"


class DataSubscriberProductCatalog(ElasticsearchUtility):
    """
    Class to track products downloaded by tools/daac_data_subscriber.py

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

    def create_index(self, index=ES_INDEX, delete_old_index=False):
        if delete_old_index is True:
            self.es.indices.delete(index=index, ignore=404)
            if self.logger:
                self.logger.info("Deleted old index: {}".format(index))

        self.es.indices.create(index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(index))

    def delete_index(self, index=ES_INDEX):
        self.es.indices.delete(index=index, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(index))

    def post(self, records, header=None, index=ES_INDEX):
        """
        Post records into ElasticSearch.

        :param records: A list of ROST records to ingest.
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
