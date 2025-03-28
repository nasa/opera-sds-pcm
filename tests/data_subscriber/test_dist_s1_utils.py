#!/usr/bin/env python3

import pytest
import conftest
from data_subscriber.dist_s1_utils import localize_dist_burst_db, compute_dist_s1_triggering, build_rtc_native_ids

dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

def test_compute_dist_s1_triggering():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    granule_ids = ["OPERA_L2_RTC-S1_T168-359595-IW3_20231217T053154Z_20231218T195230Z_S1A_30_v1.0",
                   "OPERA_L2_RTC-S1_T168-359595-IW2_20231217T053153Z_20231218T195230Z_S1A_30_v1.0"]

    products_triggered, _, _ = compute_dist_s1_triggering(bursts_to_products, product_to_bursts, granule_ids, None)

    assert len(products_triggered) == 3

    assert len(products_triggered["31RGQ_3_302"].rtc_granules) == 1
    assert len(products_triggered["32RKV_3_302"].rtc_granules) == 2
    assert len(products_triggered["32RLV_3_302"].rtc_granules) == 1

def test_build_rtc_native_ids():
    '''Test building up rtc native ids'''
    l, native_id = build_rtc_native_ids("32RLV_3", product_to_bursts)
    #print("----------------------------------")
    assert l == 16
    assert native_id == \
           "OPERA_L2_RTC-S1_T168-359591-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359591-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359592-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359592-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359593-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359593-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359594-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359594-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359595-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359595-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359596-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359596-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359597-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359597-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359598-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359598-IW2*"