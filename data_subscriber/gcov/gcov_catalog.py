from datetime import datetime

from data_subscriber.catalog import ProductCatalog


class NisarGcovProductCatalog(ProductCatalog):
    """Cataloging class for NISAR GCOV Products to support DSWx-NI triggering."""
    NAME = "nisar_gcov_catalog"
    ES_INDEX_PATTERNS = "nisar_gcov_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        return super().get_download_granule_revision(download_batch_id)

    def form_document(self, filename: str, granule: dict, job_id: str, query_dt: datetime,
                      temporal_extent_beginning_dt: datetime, revision_date_dt: datetime, revision_id):

        m = super().form_document(
            filename, granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, revision_id
        )

        # Add http_urls and s3_urls to the document
        m["filtered_urls"] = granule.get("filtered_urls", [])

        return m