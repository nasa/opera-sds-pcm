from data_subscriber.rtc.rtc_catalog import RTCProductCatalog

class RTCForDistProductCatalog(RTCProductCatalog):
    """Cataloging class for cataloging RTC products queried from CMR for DIST-S1 production purposes."""
    NAME = "rtc_for_dist_catalog"
    ES_INDEX_PATTERNS = "rtc_for_dist_catalog*"