import logging
from data_subscriber.query import CmrQuery
from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection

logger = logging.getLogger(__name__)


class HlsCmrQuery(CmrQuery):
        
    def update_granule_index(self, granule):
        spatial_catalog_conn = get_hls_spatial_catalog_connection(logger)
        spatial_catalog_conn.process_granule(granule)
