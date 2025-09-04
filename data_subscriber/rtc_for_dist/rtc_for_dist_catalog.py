from datetime import datetime
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog

class RTCForDistProductCatalog(CSLCProductCatalog):
    """Cataloging class for cataloging RTC products queried from CMR for DIST-S1 production purposes."""
    NAME = "rtc_for_dist_catalog"
    ES_INDEX_PATTERNS = "rtc_for_dist_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        # TODO: Not sure why but we need this explicit call instead of relying on inheritance
        return super().get_download_granule_revision(download_batch_id)

    def form_document(self, filename: str, granule: dict, job_id: str, query_dt: datetime,
                      temporal_extent_beginning_dt: datetime, revision_date_dt: datetime, revision_id):

        m = super().form_document(
            filename, granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, revision_id
        )

        # Add http_urls and s3_urls to the document
        m["filtered_urls"] = granule.get("filtered_urls", [])

        return m