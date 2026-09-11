from enum import StrEnum


class Collection(StrEnum):
    DSWx_HLS = "DSWx_HLS"
    DSWx_S1 = "DSWx_S1"
    CSLC_S1 = "CSLC_S1"
    RTC_S1 = "RTC_S1"
    CSLC_S1_STATIC = "CSLC_S1_STATIC"
    RTC_S1_STATIC = "RTC_S1_STATIC"
    DISP_S1 = "DISP_S1"
    DISP_S1_STATIC = "DISP_S1_STATIC"
    DIST_S1 = "DIST_S1"
    TROPO = "TROPO"


CCID_MAP = {
    Collection.DSWx_HLS: 'C2617126679-POCLOUD',
    Collection.DSWx_S1: 'C2949811996-POCLOUD',
    Collection.CSLC_S1: 'C2777443834-ASF',
    Collection.RTC_S1: 'C2777436413-ASF',
    Collection.CSLC_S1_STATIC: 'C2795135668-ASF',
    Collection.RTC_S1_STATIC: 'C2795135174-ASF',
    Collection.DISP_S1: 'C3294057315-ASF',
    Collection.DISP_S1_STATIC: 'C3959290248-ASF',
    Collection.DIST_S1: 'C4090131664-ASF',
    Collection.TROPO: 'C3717139408-ASF',
}


GRQ_MAP = {
    Collection.DSWx_HLS: 'grq_*_l3_dswx_hls-*',
    Collection.DSWx_S1: 'grq_*_l3_dswx_s1-*',
    Collection.CSLC_S1: 'grq_*_l2_cslc_s1-*',
    Collection.RTC_S1: 'grq_*_l2_rtc_s1-*',
    Collection.CSLC_S1_STATIC: 'grq_*_l2_cslc_s1_static-*',
    Collection.RTC_S1_STATIC: 'grq_*_l2_rtc_s1_static-*',
    Collection.DISP_S1: 'grq_*_l3_disp_s1-*',
    Collection.DISP_S1_STATIC: 'grq_*_l3_disp_s1_static-*',
    Collection.DIST_S1: 'grq_*_l3_dist_s1-*',
    Collection.TROPO: 'grq_*_l4_tropo-*',
}
