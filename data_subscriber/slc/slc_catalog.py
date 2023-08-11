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
        self.ES_INDEX_PATTERNS = ["slc_catalog", "slc_catalog-*"]

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        undownloaded = self._query_catalog(start_dt, end_dt, use_temporal)

        return [result['_source'] for result in (undownloaded or [])]

    def generate_es_index_name(self):
        return "slc_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        undownloaded = self._query_catalog(start_dt, end_dt, use_temporal)
        return [result['_source'] for result in (undownloaded or [])]
