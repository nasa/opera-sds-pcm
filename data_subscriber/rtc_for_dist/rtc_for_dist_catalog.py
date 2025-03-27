from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog

class RTCForDistProductCatalog(CSLCProductCatalog):
    """Cataloging class for cataloging RTC products queried from CMR for DIST-S1 production purposes."""
    NAME = "rtc_for_dist_catalog"
    ES_INDEX_PATTERNS = "rtc_for_dist_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        # TODO: Not sure why but we need this explicit call instead of relying on inheritance
        return super().get_download_granule_revision(download_batch_id)