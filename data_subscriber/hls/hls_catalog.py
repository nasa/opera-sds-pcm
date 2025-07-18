
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
    
    def mark_download_job_id(self, granule_id, job_id):

        # This is bit of a hack. The granule_id coming in has revision information. But we don't store such value in the ES.
        # So this will update all revisions with the job_id. But it is ok because:
        # 1. Chances of having multiple revisions of the same granule on a running system is very low
        # 2. The download job id is funtionally moot. We don't use it for anything. It's for occassional debugging.
        # 3. The download job id is correctly updated by the download job upon execution.
        granule_id, _ = self.granule_and_revision(granule_id)
        self._mark_download_job_id(granule_id, job_id, query_key = "granule_id")


class HLSSpatialProductCatalog(HLSProductCatalog):
    """Cataloging class for spatial regions of downloaded Harmonized Landsat and Sentinel-1 (HLS) products."""
    NAME = "hls_spatial_catalog"
    ES_INDEX_PATTERNS = "hls_spatial_catalog*"
