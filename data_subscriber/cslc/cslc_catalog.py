import logging
from datetime import datetime

from ..slc.slc_catalog import SLCProductCatalog

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

ES_INDEX_PATTERNS = "cslc_catalog*"
ES_INDEX_PATTERN_HIST = "cslc_catalog_hist*"

class CSLCProductCatalog(SLCProductCatalog):
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
        return "cslc_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def get_unsubmitted_granules(self):
        downloads = self.es.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must_not": [
                            {"match": {"submitted": True}}
                        ]
                    }
                }
            }
        )
        return self.filter_query_result(downloads)

    def get_download_granule_revision(self, id):
        downloads = self.es.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"download_batch_id": id}}
                        ]
                    }
                }
            }
        )
        return self.filter_query_result(downloads)

class CSLCHistProductCatalog(CSLCProductCatalog):
    def __init__(self, /, logger=None):
        super().__init__(logger=logger)
        self.ES_INDEX_PATTERNS = ES_INDEX_PATTERN_HIST