from hysds_commons.elasticsearch_utils import ElasticsearchUtility

ES_INDEX = "data_subscriber_product_catalog"


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

    def create_index(self, index=ES_INDEX):
        self.es.indices.create(index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(index))

    def delete_index(self, index=ES_INDEX):
        self.es.indices.delete(index=index, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(index))

    def post(self, document, index=ES_INDEX):
        result = self.index_document(index=index, body=document)

        if self.logger:
            self.logger.debug(f"Document indexed: {result}")

    def mark_downloaded(self, id, index=ES_INDEX):
        result = self.update_document(id=id, body={"doc_as_upsert": True, "doc": {"downloaded": True}}, index=index)

        if self.logger:
            self.logger.debug(f"Document updated: {result}")

        #return result

    def query_existence(self, url, index=ES_INDEX):
        result =  self.search(body={"query": {"match": {"url": url}}}, index=index)

        if self.logger:
            self.logger.debug(f"Query result: {result}")

        return result
