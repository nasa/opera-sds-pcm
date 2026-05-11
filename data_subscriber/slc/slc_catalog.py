
from data_subscriber.catalog import ProductCatalog


class SLCProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Single Look Complex (SLC) products."""
    NAME = "slc_catalog"
    ES_INDEX_PATTERNS = "slc_catalog*"

    def process_query_result(self, query_result: list[dict]):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        """
        For S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3.zip-r5 returns:
            S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3-SLC and 5
        """
        return es_id.split('.zip')[0]+'-SLC', es_id.split('-r')[1]

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


class SLCSpatialProductCatalog(SLCProductCatalog):
    """Cataloging class for spatial regions of downloaded Single Look Complex (SLC) products."""
    NAME = "slc_spatial_catalog"
    ES_INDEX_PATTERNS = "slc_spatial_catalog*"
