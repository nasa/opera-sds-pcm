#!/usr/bin/env python3

import pytest
import conftest
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import localize_dist_burst_db
from data_subscriber.parser import create_parser
from data_subscriber.rtc_for_dist.rtc_for_dist_query import RtcForDistCmrQuery
from datetime import datetime
from data_subscriber.cmr import DateTimeRange

forward_arguments = ["query", "-c", "OPERA_L2_RTC-S1_V1", "--product=DIST_S1", "--processing-mode=forward",
                     "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-25T00:00:00Z", "--use-temporal"]

dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

def test_extend_additional_records():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    GRANULE_ID1 = "OPERA_L2_RTC-S1_T168-359595-IW3_20231217T053154Z_20231218T195230Z_S1A_30_v1.0"
    GRANULE_ID2 = "OPERA_L2_RTC-S1_T168-359595-IW2_20231217T053153Z_20231218T195230Z_S1A_30_v1.0"

    # TODO: Sort this out and renable it
    #m = {}
    #m["p31RGQ_3_a302"] = GRANULE_ID1
    #m["p32RKV_3_a302"] = GRANULE_ID1
    #m["p32RLV_3_a302"] = GRANULE_ID2
    #m["p32RKV_3_a302"] = GRANULE_ID2

    granules = []
    granules.append({"granule_id": GRANULE_ID1})
    granules.append({"granule_id": GRANULE_ID2})

    for granule in granules:
        burst_id, acquisition_dts = parse_r2_product_file_name(granule["granule_id"], "L2_RTC_S1")
        granule["burst_id"] = burst_id
        granule["acquisition_ts"] = acquisition_dts

    args = create_parser().parse_args(forward_arguments)
    cmr_query = RtcForDistCmrQuery(args, None, None, None, None, None)
    cmr_query.extend_additional_records(granules)

    assert len(granules) == 4

    # TODO: Sort this out and renable it
    #for granule in granules:
    #    assert m[granule["download_batch_id"]] == granule["granule_id"]

"""def test_reprocessing_by_native_id(caplog):
    ''' Tests reprocessing query commands and high-level processing when specifying a native_id'''
    reprocessing_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=reprocessing", "--k=4", "--m=4",
                              "--native-id=OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0", "--no-schedule-download"]
    reproc_args = create_parser().parse_args(reprocessing_arguments)
    c_query = cslc_query.CslcCmrQuery(reproc_args, None, None, None, None,
                                      {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60},BURST_MAP)
    c_query.query_cmr(None, datetime.utcnow())
    assert ("native_id=OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0 is not found in the DISP-S1 Burst ID Database JSON. Nothing to process"
            in caplog.text)


def test_reprocessing_by_dates():
    ''' Tests reprocessing query commands and high-level processing'''
    reprocessing_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=reprocessing",
                              "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-25T00:00:00Z",
                              "--k=4", "--no-schedule-download"]
    reproc_args = create_parser().parse_args(reprocessing_arguments)
    query_timerange = DateTimeRange(reproc_args.start_date, reproc_args.end_date)
    c_query = cslc_query.CslcCmrQuery(reproc_args, None, None, None, None,
                                      {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60},BURST_MAP)
    cr = c_query.query_cmr(query_timerange, datetime.utcnow())
    args = cr.cr_frame.f_locals["args"]
    assert args.collection == 'OPERA_L2_CSLC-S1_V1'
    assert args.start_date == '2021-01-24T23:00:00Z'
    assert args.end_date == '2021-01-25T00:00:00Z'
    assert args.proc_mode == 'reprocessing'"""