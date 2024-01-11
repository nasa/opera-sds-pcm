import logging
from datetime import datetime
from ..hls.hls_catalog import HLSProductCatalog
from pathlib import Path

from data_subscriber import es_conn_util

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

class SLCProductCatalog(HLSProductCatalog):
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
        self.ES_INDEX_PATTERNS = "slc_catalog*"
    def granule_and_revision(self, es_id):
        '''For S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3.zip-r5 returns:
                S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3-SLC and 5 '''
        return es_id.split('.zip')[0]+'-SLC', es_id.split('-r')[1]

    def generate_es_index_name(self):
        return "slc_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def filter_query_result(self, query_result):
        return [result['_source'] for result in (query_result or [])]
