#!/usr/bin/env python3

import pytest
import conftest
from data_subscriber.dist_s1_utils import localize_dist_burst_db, compute_dist_s1_triggering, build_rtc_native_ids

dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

granule_ids = ["OPERA_L2_RTC-S1_T168-359595-IW3_20231217T053154Z_20231218T195230Z_S1A_30_v1.0",
                "OPERA_L2_RTC-S1_T168-359595-IW2_20231217T053153Z_20231218T195230Z_S1A_30_v1.0",
                "OPERA_L2_RTC-S1_T168-359595-IW1_20231217T053152Z_20231218T195230Z_S1A_30_v1.0",
                'OPERA_L2_RTC-S1_T168-359594-IW3_20231217T053151Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359593-IW3_20231217T053148Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359593-IW2_20231217T053147Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359593-IW1_20231217T053146Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359592-IW3_20231217T053145Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359592-IW2_20231217T053144Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359592-IW1_20231217T053143Z_20231218T195230Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359591-IW3_20231217T053143Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359591-IW2_20231217T053142Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359591-IW1_20231217T053141Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359590-IW3_20231217T053140Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359590-IW2_20231217T053139Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359590-IW1_20231217T053138Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359589-IW3_20231217T053137Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359589-IW2_20231217T053136Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359589-IW1_20231217T053135Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359588-IW3_20231217T053134Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359588-IW2_20231217T053133Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359588-IW1_20231217T053132Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359587-IW3_20231217T053132Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359587-IW2_20231217T053131Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359587-IW1_20231217T053130Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359586-IW3_20231217T053129Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359586-IW2_20231217T053128Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359586-IW1_20231217T053127Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359585-IW3_20231217T053126Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359585-IW2_20231217T053125Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359585-IW1_20231217T053124Z_20231220T055807Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359584-IW3_20231217T053123Z_20231220T055807Z_S1A_30_v1.0']

def test_compute_dist_s1_triggering_incomplete():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    products_triggered, granules_triggered, _, _ = (
        compute_dist_s1_triggering(bursts_to_products, product_to_bursts, granule_ids, False,None))

    assert len(products_triggered) == 12

    assert len(products_triggered["31RGQ_3_302"].rtc_granules) == 4
    assert len(products_triggered["32RKV_3_302"].rtc_granules) == 7
    assert len(products_triggered["32RLV_3_302"].rtc_granules) == 8

def test_compute_dist_s1_triggering_complete():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    products_triggered, granules_triggered, _, _ = (
        compute_dist_s1_triggering(bursts_to_products, product_to_bursts, granule_ids, True,None))

    assert len(products_triggered) == 4

    assert len(products_triggered["32SKA_3_302"].rtc_granules) == 16
    assert len(products_triggered["31SGR_3_302"].rtc_granules) == 7
    assert len(products_triggered["32SLA_3_302"].rtc_granules) == 16
    assert len(products_triggered["32SMA_3_302"].rtc_granules) == 8

def test_build_rtc_native_ids():
    '''Test building up rtc native ids'''
    l, native_id = build_rtc_native_ids("32RLV_3", product_to_bursts)
    #print("----------------------------------")
    assert l == 16
    assert native_id == \
           "OPERA_L2_RTC-S1_T168-359591-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359591-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359592-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359592-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359593-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359593-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359594-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359594-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359595-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359595-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359596-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359596-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359597-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359597-IW2*&native-id[]=OPERA_L2_RTC-S1_T168-359598-IW1*&native-id[]=OPERA_L2_RTC-S1_T168-359598-IW2*"