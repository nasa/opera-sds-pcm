
from data_subscriber.catalog import ProductCatalog


class HLSProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Harmonized Landsat and Sentinel-1 (HLS) products."""
    NAME = "hls_catalog"
    ES_INDEX_PATTERNS = "hls_catalog*"
    BATCH_ID_KEYWORD = 'granule_id'

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

    def mark_download_job_id(self, batch_id, job_id):
        docs = self._docs_for_batch.get(batch_id)

        if not docs:  # not cataloged by this process; fall back to marking by query
            return super().mark_download_job_id(batch_id, job_id)

        updated = self.mark_download_job_id_by_doc_ids(docs, job_id)
        self.logger.info(f"Document updated: {batch_id=} {job_id=} {updated=}")

    def get_query_for_download_job_marking(self, batch_id):
        granule_id, revision_id = self.granule_and_revision(batch_id)

        return {
            "bool": {
                "must": [
                    {"match": {f"{self.BATCH_ID_KEYWORD}": granule_id}},
                    {"match": {"revision_id": revision_id}},
                ]
            }
        }


class HLSSpatialProductCatalog(HLSProductCatalog):
    """Cataloging class for spatial regions of downloaded Harmonized Landsat and Sentinel-1 (HLS) products."""
    NAME = "hls_spatial_catalog"
    ES_INDEX_PATTERNS = "hls_spatial_catalog*"
