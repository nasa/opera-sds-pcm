
from data_subscriber.query import BaseQuery
from data_subscriber.hls.hls_catalog import HLSSpatialProductCatalog


class HlsCmrQuery(BaseQuery):
    """Class used to query the Common Metadata Repository (CMR) for Harmonized Landsat and Sentinel-1 (HLS) products."""
    def update_granule_index(self, granule):
        spatial_catalog_conn = HLSSpatialProductCatalog(self.logger)
        spatial_catalog_conn.process_granule(granule)
