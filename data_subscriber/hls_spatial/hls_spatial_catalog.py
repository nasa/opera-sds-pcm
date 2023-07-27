import logging
from datetime import datetime

from data_subscriber import es_conn_util

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

ES_INDEX = ["hls_spatial_catalog", "hls_spatial_catalog-*"]


def generate_es_index_name():
    return "hls_spatial_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m.%d.%H%M%S"))


class HLSSpatialProductCatalog:
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
    def __init__(self, /, logger=None):
        self.logger = logger or null_logger
        self.es = es_conn_util.get_es_connection(logger)

    def create_index(self):
        self.es.es.indices.put_index_template(
            name="hls_spatial_catalog_template",
            create=True,
            body={
                "index_patterns": ES_INDEX,
                "template": {
                    "settings": {},
                    "mappings": {
                        "properties": {
                            "bounding_box": {"type": "geo_point"},
                            "short_name": {"type": "keyword"},
                            "product_id": {"type": "keyword"},
                            "production_datetime": {"type": "date"},
                            "creation_timestamp": {"type": "date"}
                        }
                    }
                }
            }
        )

        self.logger.info("Successfully created index template: {}".format("hls_spatial_catalog_template"))

    def delete_index(self):
        self.logger.warning(f"Index deletion not supported for {ES_INDEX}")
        pass

    def process_granule(self, granule):
        results = self._query_existence(granule["granule_id"])

        if results:
            self.logger.warning(f'Granule {granule["granule_id"]} exists in DB. Returning.')
            return

        doc = {
            "id": granule["granule_id"],
            "provider": granule["provider"],
            "production_datetime": granule["production_datetime"],
            "short_name": granule["short_name"],
            "product_id": granule["identifier"],
            "bounding_box": granule["bounding_box"],
            "creation_timestamp": datetime.now()
        }
        self._post(granule["granule_id"], doc)

    def _post(self, granule_id, body):
        result = self.es.index_document(index=generate_es_index_name(), body=body, id=granule_id)

        self.logger.info(f"Document updated: {result}")

    def _get_index_name_for(self, _id, default=None):
        """Gets the index name for the most recent ES doc matching the given _id"""
        if default is None:
            raise

        results = self._query_existence(_id)
        self.logger.debug(f"{results=}")
        if not results:  # EDGECASE: index doesn't exist yet
            index = default
        else:  # reprocessed or revised product. assume reprocessed. update existing record
            if results:  # found results
                index = results[0]["_index"]  # get the ID of the most recent record
            else:
                index = default
        return index

    def _query_existence(self, _id):
        try:
            results = self.es.query(
                index=",".join(ES_INDEX),
                body={
                    "query": {"bool": {"must": [{"term": {"_id": _id}}]}},
                    "sort": [{"creation_timestamp": "desc"}],
                    "_source": {"includes": "false", "excludes": []}  # NOTE: returned object is different than when `"includes": []` is used
                }
            )
            self.logger.debug(f"Query results: {results}")

        except:
            self.logger.info(f"{_id} does not exist in {ES_INDEX}")
            results = None

        return results
