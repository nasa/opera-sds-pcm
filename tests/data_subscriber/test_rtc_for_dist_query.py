#!/usr/bin/env python3
from collections import defaultdict
from email.policy import default

import pytest
import conftest
import sys

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()
from mock import MagicMock

from data_subscriber.url import determine_acquisition_cycle
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import localize_dist_burst_db, basic_decorate_granule
from data_subscriber.parser import create_parser
from data_subscriber.rtc_for_dist.rtc_for_dist_query import RtcForDistCmrQuery
from datetime import datetime, timedelta
from data_subscriber.cmr import DateTimeRange

forward_arguments = ["query", "-c", "OPERA_L2_RTC-S1_V1", "--product=DIST_S1", "--processing-mode=forward",
                     "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-25T00:00:00Z", "--use-temporal"]

dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

_granules_1 =  ['OPERA_L2_RTC-S1_T168-359433-IW1_20231217T052425Z_20231220T055805Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359432-IW3_20231217T052424Z_20231220T055805Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359432-IW2_20231217T052423Z_20231220T055805Z_S1A_30_v1.0',
                'OPERA_L2_RTC-S1_T168-359432-IW1_20231217T052422Z_20231220T055805Z_S1A_30_v1.0']

_granules_2 = ['OPERA_L2_RTC-S1_T168-359430-IW3_20231217T052419Z_20231220T055805Z_S1A_30_v1.0']

class MockESConnNoUnsubmittedGranules:
    def get_unsubmitted_granules(self):
        return []

class MockESConnUnsubmittedGranules:

    def __init__(self, rtc_for_dist_query, granules1, granules2, delta1, delta2):
        self.rtc_for_dist_query = rtc_for_dist_query
        self.granules1 = granules1
        self.granules2 = granules2
        self.delta1 = delta1
        self.delta2 = delta2

    def get_unsubmitted_granules(self):

        granules = []

        for granule in self.granules1:
            granules.append({"granule_id": granule, "creation_timestamp": (datetime.now() - timedelta(minutes=self.delta1)) .isoformat()})

        for granule in self.granules2:
            granules.append({"granule_id": granule, "creation_timestamp": (datetime.now() - timedelta(minutes=self.delta2)).isoformat()})

        for granule in granules:
            basic_decorate_granule(granule)

        self.rtc_for_dist_query.extend_additional_records(granules)

        return granules

def test_extend_additional_records():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    GRANULE_ID1 = "OPERA_L2_RTC-S1_T168-359595-IW3_20231217T053154Z_20231218T195230Z_S1A_30_v1.0"
    GRANULE_ID2 = "OPERA_L2_RTC-S1_T168-359595-IW2_20231217T053153Z_20231218T195230Z_S1A_30_v1.0"
    GRANULE_ID3 = "OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0"

    m = {}
    m["p31RGQ_3_a302"] = [GRANULE_ID1]
    m["p32RKV_3_a302"] = [GRANULE_ID1, GRANULE_ID2]
    m["p32RLV_3_a302"] = [GRANULE_ID2]
    m["p33VUF_5_a302"] = [GRANULE_ID3]
    m["p33VVF_4_a302"] = [GRANULE_ID3]
    m["p33VVE_4_a302"] = [GRANULE_ID3]
    m["p33VUE_5_a302"] = [GRANULE_ID3]

    granules = []
    granules.append({"granule_id": GRANULE_ID1})
    granules.append({"granule_id": GRANULE_ID2})
    granules.append({"granule_id": GRANULE_ID3})

    for granule in granules:
        basic_decorate_granule(granule)

    args = create_parser().parse_args(forward_arguments)
    cmr_query = RtcForDistCmrQuery(args, None, MockESConnNoUnsubmittedGranules(), None, None, {"DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES": 210})
    cmr_query.extend_additional_records(granules)

    assert len(granules) == 8

    for granule in granules:
        assert granule["granule_id"] in m[granule["download_batch_id"]]

def test_determine_download_granules(monkeypatch):
    """Given a list of granules, test that we are determining the download granules correctly"""

    granule_ids = ['OPERA_L2_RTC-S1_T168-359433-IW1_20231217T052425Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359432-IW3_20231217T052424Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359432-IW2_20231217T052423Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359432-IW1_20231217T052422Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359431-IW3_20231217T052421Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359431-IW2_20231217T052420Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359430-IW3_20231217T052419Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359431-IW1_20231217T052419Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359430-IW2_20231217T052418Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359430-IW1_20231217T052417Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359429-IW3_20231217T052416Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359429-IW1_20231217T052414Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359428-IW3_20231217T052413Z_20231220T055805Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359428-IW2_20231217T052412Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359428-IW1_20231217T052411Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359427-IW3_20231217T052410Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359427-IW2_20231217T052409Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359427-IW1_20231217T052408Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359426-IW3_20231217T052408Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359426-IW2_20231217T052407Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359426-IW1_20231217T052406Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359425-IW3_20231217T052405Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359425-IW2_20231217T052404Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359425-IW1_20231217T052403Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359424-IW3_20231217T052402Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359424-IW2_20231217T052401Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359424-IW1_20231217T052400Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359423-IW3_20231217T052359Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359423-IW2_20231217T052358Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359423-IW1_20231217T052357Z_20231220T055739Z_S1A_30_v1.0',
                    'OPERA_L2_RTC-S1_T168-359422-IW3_20231217T052356Z_20231220T055739Z_S1A_30_v1.0']

    granules = []
    for granule_id in granule_ids:
        granules.append({"granule_id": granule_id})
    for granule in granules:
        basic_decorate_granule(granule)

    args = create_parser().parse_args(forward_arguments)
    cmr_query = RtcForDistCmrQuery(args, None, MockESConnNoUnsubmittedGranules(), None, None, {"DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES": 210})

    mock_retrieve_baseline_granules = MagicMock(return_value=[])
    monkeypatch.setattr(
        cmr_query,
        cmr_query.retrieve_baseline_granules.__name__,
        mock_retrieve_baseline_granules
    )

    download_granules = cmr_query.determine_download_granules(granules)
    download_batch_id_to_granules = defaultdict(list)
    for granule in download_granules:
        download_batch_id_to_granules[granule["download_batch_id"]].append(granule)

    assert 'p33VUF_5_a302' in download_batch_id_to_granules
    assert 'p32VPL_5_a302' in download_batch_id_to_granules
    assert len(download_batch_id_to_granules["p33VUF_5_a302"]) == 11
    assert len(download_batch_id_to_granules["p32VPL_5_a302"]) == 7

    assert len(download_granules) == 18

def test_determine_download_granules_grace_period(monkeypatch):
    args = create_parser().parse_args(forward_arguments)
    cmr_query = RtcForDistCmrQuery(args, None, None, None, None,
                                   {"DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES": 210})

    mock_retrieve_baseline_granules = MagicMock(return_value=[])
    monkeypatch.setattr(
        cmr_query,
        cmr_query.retrieve_baseline_granules.__name__,
        mock_retrieve_baseline_granules
    )

    # Test 1: The first set of unsubmitted granules were generated within the grace period 200 min and
    # the second set were generated outside, 240 mins. So only the first set should trigger
    mock_es = MockESConnUnsubmittedGranules(cmr_query, _granules_1, _granules_2, 240, 200)
    cmr_query.es_conn = mock_es

    download_granules = cmr_query.determine_download_granules([])
    download_batch_id_to_granules = defaultdict(list)
    for granule in download_granules:
        download_batch_id_to_granules[granule["download_batch_id"]].append(granule)

    assert 'p33VVE_4_a302' in download_batch_id_to_granules
    assert len(download_batch_id_to_granules["p33VVE_4_a302"]) == 4

    assert 'p33VUF_5_a302' not in download_batch_id_to_granules

    # Test 2: Both sets were created outside of the grace period: 240 mins so they should all trigger
    mock_es = MockESConnUnsubmittedGranules(cmr_query, _granules_1, _granules_2, 240, 240)
    cmr_query.es_conn = mock_es

    download_granules = cmr_query.determine_download_granules([])
    download_batch_id_to_granules = defaultdict(list)
    for granule in download_granules:
        download_batch_id_to_granules[granule["download_batch_id"]].append(granule)

    assert 'p33VVE_4_a302' in download_batch_id_to_granules
    assert len(download_batch_id_to_granules["p33VVE_4_a302"]) == 4

    assert 'p33VUF_5_a302' in download_batch_id_to_granules
    assert len(download_batch_id_to_granules["p33VUF_5_a302"]) == 1


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
