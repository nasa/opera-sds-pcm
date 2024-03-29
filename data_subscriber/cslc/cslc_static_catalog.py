import logging
from datetime import datetime
from data_subscriber.hls.hls_catalog import HLSProductCatalog

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

class CSLCStaticProductCatalog(HLSProductCatalog):
    """
    Class to track CSLC-S1 Static Layer products downloaded by daac_data_subscriber.py

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
        self.ES_INDEX_PATTERNS = "cslc_static_catalog*"

    def granule_and_revision(self, es_id):
        return es_id.split('-r')[0], es_id.split('-r')[1]

    def generate_es_index_name(self):
        return "cslc_static_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def filter_query_result(self, query_result):
        return [result['_source'] for result in (query_result or [])]
