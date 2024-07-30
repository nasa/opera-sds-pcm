
import logging

from data_subscriber.query import CmrQuery
from data_subscriber.hls.hls_catalog import HLSSpatialProductCatalog


class HlsCmrQuery(CmrQuery):
    """Class used to query the Common Metadata Repository (CMR) for Harmonized Landsat and Sentinel-1 (HLS) products."""
    def update_granule_index(self, granule):
        spatial_catalog_conn = HLSSpatialProductCatalog(logging.getLogger(__name__))
        spatial_catalog_conn.process_granule(granule)
