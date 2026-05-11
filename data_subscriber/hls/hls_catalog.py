
from data_subscriber.catalog import ProductCatalog


class HLSProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Harmonized Landsat and Sentinel-1 (HLS) products."""
    NAME = "hls_catalog"
    ES_INDEX_PATTERNS = "hls_catalog*"

    def process_query_result(self, query_result : list[dict]):
        return [
            {
                "_id": catalog_entry["_id"],
                "granule_id": catalog_entry["_source"].get("granule_id"),
                "revision_id": catalog_entry["_source"].get("revision_id"),
                "s3_url": catalog_entry["_source"].get("s3_url"),
                "https_url": catalog_entry["_source"].get("https_url")
            }
            for catalog_entry in (query_result or [])
        ]

    def granule_and_revision(self, es_id: str):
        """
        For HLS.S30.T56MPU.2022152T000741.v2.0-r1 returns:
            HLS.S30.T56MPU.2022152T000741.v2.0 and 1
        """
        return es_id.split('-')[0], es_id.split('-r')[1]

    def get_cataloged_granule_by_granule_id(self, granule_id):
        query_result = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "granule_id": granule_id
                                }
                            }
                        ]
                    }
                }
            }
        )

        self.logger.info(f'Catalog search result: {query_result}')

        return query_result


class HLSSpatialProductCatalog(HLSProductCatalog):
    """Cataloging class for spatial regions of downloaded Harmonized Landsat and Sentinel-1 (HLS) products."""
    NAME = "hls_spatial_catalog"
    ES_INDEX_PATTERNS = "hls_spatial_catalog*"
