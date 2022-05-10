from datetime import datetime

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

ES_INDEX = "hls_spatial_catalog"


class HLSSpatialProductCatalog(ElasticsearchUtility):
    """
    Class to track products downloaded by daac_data_subscriber.py

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

    def create_index(self):
        self.es.indices.create(body={"settings": {"index": {"sort.field": "granuleid", "sort.order": "asc"}},
                                     "mappings": {
                                         "properties": {
                                             "granule_id": {"type": "keyword"},
                                             "bounding_box": {"type": "object"},
                                             "short_name": {"type": "keyword"},
                                             "production_datetime": {"type": "date"},
                                             "index_datetime": {"type": "date"}}}},
                               index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(ES_INDEX))

    def delete_index(self):
        self.es.indices.delete(index=ES_INDEX, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(ES_INDEX))


    def _post(self, id, body):
        result = self.index_document(index=ES_INDEX, body=body, id=id)

        if self.logger:
            self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, id, index=ES_INDEX):
        try:
            result = self.get_by_id(index=index, id=id)
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None
            if self.logger:
                self.logger.debug(f"{id} does not exist in {index}")

        return result
