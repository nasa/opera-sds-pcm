#!/usr/bin/env python3

import pytest
import conftest
import pickle
from data_subscriber.dist_s1_utils import localize_dist_burst_db, compute_dist_s1_triggering, build_rtc_native_ids

dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

def test_burst_map_pickle():
    '''Test that we can pickle and unpickle burst map files'''

    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

    # Save the lengths of all the objects for comparison after unpickling
    dist_products_len = len(dist_products)
    bursts_to_products_len = len(bursts_to_products)
    product_to_bursts_len = len(product_to_bursts)
    all_tile_ids_len = len(all_tile_ids)

    # First, pickle dist_products, bursts_to_products, product_to_bursts, all_tile_ids into a single file
    with open("dist_products.pickle", "wb") as f:
        pickle.dump((dist_products, bursts_to_products, product_to_bursts, all_tile_ids), f)

    # Now, unpickle the file
    with open("dist_products.pickle", "rb") as f:
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = pickle.load(f)

    # Check that the lengths of the unpickled objects are the same as the original ones
    assert len(dist_products) == dist_products_len
    assert len(bursts_to_products) == bursts_to_products_len
    assert len(product_to_bursts) == product_to_bursts_len
    assert len(all_tile_ids) == all_tile_ids_len

@pytest.mark.skip(reason="compute_dist_s1_triggering is changed. This test needs to be updated.")
def test_compute_dist_s1_triggering_incomplete():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    products_triggered, granules_triggered, _, _ = (
        compute_dist_s1_triggering(bursts_to_products, product_to_bursts, granule_ids, False,None))

    assert len(products_triggered) == 12

    assert len(products_triggered["31RGQ_3_302"].rtc_granules) == 4
    assert len(products_triggered["32RKV_3_302"].rtc_granules) == 7
    assert len(products_triggered["32RLV_3_302"].rtc_granules) == 8

@pytest.mark.skip(reason="compute_dist_s1_triggering is changed. This test needs to be updated.")
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