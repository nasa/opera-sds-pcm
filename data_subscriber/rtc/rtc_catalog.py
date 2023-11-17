import logging
from datetime import datetime

import elasticsearch.helpers

from ..hls.hls_catalog import HLSProductCatalog

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

ES_INDEX_PATTERNS = "rtc_catalog*"

class RTCProductCatalog(HLSProductCatalog):
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
        super().__init__(logger=logger)
        self.ES_INDEX_PATTERNS = ES_INDEX_PATTERNS

    def generate_es_index_name(self):
        return "rtc_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def filter_query_result(self, query_result):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        """For 'OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1' returns:
        OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0 and 1"""
        return es_id[:es_id.rfind("-")], es_id[es_id.rfind("-r")+2:]

    def get_download_granule_revision(self, batch_id: str):
        downloads = self.es.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"mgrs_set_id_acquisition_ts_cycle_indexes": batch_id}}
                        ]
                    }
                }
            }
        )
        return self.filter_query_result(downloads)

    def mark_products_as_job_submitted(self, batch_id_to_products_map: dict):
        operations = []
        for batch_id, products in batch_id_to_products_map.items():
            for product in products:
                for product_id, docs in product.items():
                    for doc in docs:
                        index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())
                        # self.es.update_document(
                        #     id=doc["id"],
                        #     body={
                        #         "doc_as_upsert": True,
                        #         "doc": {
                        #             "job_submitted": True
                        #         }
                        #     },
                        #     index=index
                        # )
                        operation = {
                            "_op_type": "update",
                            "_index": index,
                            "_type": "_doc",
                            "_id": doc["id"],
                            "doc": {"job_submitted": True},
                            "doc_as_upsert": True,
                        }
                        operations.append(operation)
        elasticsearch.helpers.bulk(self.es.es, operations)

        self.logger.info("performing index refresh")
        self.refresh()
        self.logger.info("performed index refresh")

