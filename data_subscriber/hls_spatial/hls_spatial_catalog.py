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
        self.es.indices.create(body={"settings": {},
                                     "mappings": {
                                         "properties": {
                                             "bounding_box": {"type": "geo_point"},
                                             "short_name": {"type": "keyword"},
                                             "product_id": {"type": "keyword"},
                                             "production_datetime": {"type": "date"},
                                             "creation_timestamp": {"type": "date"}}}},
                               index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(ES_INDEX))

    def delete_index(self):
        self.es.indices.delete(index=ES_INDEX, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(ES_INDEX))

    def process_granule(self, granule):
        result = self._query_existence(granule["granule_id"])

        if not result:
            doc = {
                "id": granule['granule_id'],
                "provider": granule["provider"],
                "production_datetime": granule["production_datetime"],
                "short_name": granule["short_name"],
                "product_id": granule["identifier"],
                "bounding_box": granule["bounding_box"],
                "creation_timestamp": datetime.now()
            }

            self._post(granule['granule_id'], doc)

    def _post(self, granule_id, body):
        result = self.index_document(index=ES_INDEX, body=body, id=granule_id)

        if self.logger:
            self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, granule_id, index=ES_INDEX):
        try:
            result = self.get_by_id(index=index, id=granule_id)
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None
            if self.logger:
                self.logger.debug(f"{granule_id} does not exist in {index}")

        return result
