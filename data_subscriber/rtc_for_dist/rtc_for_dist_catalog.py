from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog

class RTCForDistProductCatalog(CSLCProductCatalog):
    """Cataloging class for cataloging RTC products queried from CMR for DIST-S1 production purposes."""
    NAME = "rtc_for_dist_catalog"
    ES_INDEX_PATTERNS = "rtc_for_dist_catalog*"