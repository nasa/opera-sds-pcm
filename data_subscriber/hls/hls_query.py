
from data_subscriber.query import BaseQuery
from data_subscriber.hls.hls_catalog import HLSSpatialProductCatalog


class HlsCmrQuery(BaseQuery):
    """Class used to query the Common Metadata Repository (CMR) for Harmonized Landsat and Sentinel-1 (HLS) products."""
    def update_granule_index(self, granule):
        spatial_catalog_conn = HLSSpatialProductCatalog(self.logger)
        spatial_catalog_conn.process_granule(granule)

    def determine_download_granules(self, granules):
        if not self.args.granule_dedupe or self.args.native_id:
            self.logger.info('Skipping granule dedupe check')
            return granules

        filtered_granules = []

        for granule in granules:
            if len(self.es_conn.get_cataloged_granule_by_granule_id(granule['granule_id'])) == 0:
                self.logger.info(f'Found new granule {granule["granule_id"]}')
                filtered_granules.append(granule)
            else:
                self.logger.info(f'Dropping granule {granule["granule_id"]} as it has already been cataloged')

        return filtered_granules
