
from data_subscriber.catalog import ProductCatalog


class SLCProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Single Look Complex (SLC) products."""
    NAME = "slc_catalog"
    ES_INDEX_PATTERNS = "slc_catalog*"
    BATCH_ID_KEYWORD = 'id'

    def process_query_result(self, query_result: list[dict]):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        """
        For S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3.zip-r5 returns:
            S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3-SLC and 5
        """
        return es_id.split('.zip')[0]+'-SLC', es_id.split('-r')[1]

    def mark_download_job_id(self, batch_id, job_id):
        docs = self._docs_for_batch.get(batch_id)

        if not docs:  # not cataloged by this process; fall back to marking by query
            return super().mark_download_job_id(batch_id, job_id)

        updated = self.mark_download_job_id_by_doc_ids(docs, job_id)
        self.logger.info(f"Document updated: {batch_id=} {job_id=} {updated=}")


class SLCSpatialProductCatalog(SLCProductCatalog):
    """Cataloging class for spatial regions of downloaded Single Look Complex (SLC) products."""
    NAME = "slc_spatial_catalog"
    ES_INDEX_PATTERNS = "slc_spatial_catalog*"
