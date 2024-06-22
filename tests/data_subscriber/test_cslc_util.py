#!/usr/bin/env python3

import pytest
import conftest

from data_subscriber import cslc_utils
from data_subscriber.parser import create_parser
import dateutil
from datetime import datetime
from data_subscriber.cmr import DateTimeRange, get_cmr_token
from util.conf_util import SettingsConf

hist_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=historical", "--start-date=2021-01-24T23:00:00Z",\
                  "--end-date=2021-01-24T23:00:00Z", "--frame-range=100,101"]

disp_burst_map_hist, burst_to_frames, datetime_to_frames = cslc_utils.process_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

#TODO: We may change the database json during production that could have different burst ids for the same frame
#TODO: So we may want to create different versions of this unit test, one for each version of the database json
def test_burst_map():
    assert len(disp_burst_map_hist.keys()) == 1433
    burst_set = set()
    for burst in ["T175-374393-IW1", "T175-374393-IW2", "T175-374393-IW3", "T175-374394-IW1", \
     "T175-374394-IW2", "T175-374394-IW3", "T175-374395-IW1", "T175-374395-IW2", \
     "T175-374395-IW3"]:
        burst_set.add(burst)
    assert disp_burst_map_hist[46800].burst_ids.difference(burst_set) == set()
    assert disp_burst_map_hist[46800].sensing_datetimes[0] == dateutil.parser.isoparse("2019-11-14T16:51:06")

    assert len(disp_burst_map_hist[46799].burst_ids) == 15
    assert len(disp_burst_map_hist[46799].sensing_datetimes) == 2

def test_split_download_batch_id():
    """Test that the download batch id is correctly split into frame and acquisition cycle"""
    frame_id, acquisition_cycle = cslc_utils.split_download_batch_id("f100_a200")
    assert frame_id == 100
    assert acquisition_cycle == 200

def test_arg_expansion():
    '''Test that the native_id field is expanded correctly for a given frame range'''
    l, native_id = cslc_utils.build_cslc_native_ids(46800, disp_burst_map_hist)
    #print("----------------------------------")
    assert l == 9
    assert native_id == \
           "OPERA_L2_CSLC-S1_T175-374393-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374393-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374393-IW3*&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW3*&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW3*"

def test_burst_to_frame_map():
    '''Test that the burst to frame map is correctly constructed
    Bursts belong to exactly 1 or 2 frames'''
    assert burst_to_frames["T004-006648-IW3"][0] == 831
    assert burst_to_frames["T004-006649-IW3"][0] == 831
    assert burst_to_frames["T004-006649-IW3"][1] == 832

#TODO: We may change the database json during production that could have different burst ids for the same frame
#TODO: So we may want to create different versions of this unit test, one for each version of the database json
def test_arg_expansion_hist():
    '''Test that the native_id field is expanded correctly for a given frame range'''
    l, native_id = cslc_utils.build_cslc_native_ids(46800, disp_burst_map_hist)
    #print("----------------------------------")
    assert l == 9
    assert native_id == \
           "OPERA_L2_CSLC-S1_T175-374393-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374393-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374393-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374394-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW1*&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW2*&native-id[]=OPERA_L2_CSLC-S1_T175-374395-IW3*"

def test_download_batch_id():
    """Test that the download batch id is correctly constructed for forward processing mode"""

    # Test forward mode
    granule = {'granule_id': 'OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0',  'acquisition_cycle': 145, 'burst_id': 'T027-056778-IW1', 'frame_id': 7098}
    download_batch_id = cslc_utils.download_batch_id_forward_reproc(granule)
    assert download_batch_id == "f7098_a145"

def test_parse_cslc_native_id():
    """Test that we get all the right info from parsing the native_id"""
    burst_id, acquisition_dts, acquisition_cycles, frame_ids = \
        cslc_utils.parse_cslc_native_id("OPERA_L2_CSLC-S1_T158-338083-IW1_20170403T130213Z_20240428T010605Z_S1A_VV_v1.1", burst_to_frames, disp_burst_map_hist)

    print(burst_id, acquisition_dts, acquisition_cycles, frame_ids)

    assert burst_id == "T158-338083-IW1"
    assert acquisition_dts == dateutil.parser.isoparse("20170403T130213")
    assert acquisition_cycles == {42261: 324}
    assert frame_ids == [42261]

def test_build_ccslc_m_index():
    """Test that the ccslc_m index is correctly constructed"""
    assert cslc_utils.build_ccslc_m_index("T027-056778-IW1", 445) == "t027_056778_iw1_445"

def test_determine_acquisition_cycle_cslc():
    """Test that the acquisition cycle is correctly determined"""
    acquisition_cycle = cslc_utils.determine_acquisition_cycle_cslc(dateutil.parser.isoparse("20170227T230524"), 831, disp_burst_map_hist)
    assert acquisition_cycle == 12

    acquisition_cycle = cslc_utils.determine_acquisition_cycle_cslc(dateutil.parser.isoparse("20170203T230547"), 832, disp_burst_map_hist)
    assert acquisition_cycle == 216

def test_determine_k_cycle():
    """Test that the k cycle is correctly determined"""

    args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward", "--use-temporal"])

    settings = SettingsConf().cfg

    #TODO: Figure out why this isn't working and then create unit test for acquisition date outside of the historical period
    #cmr, token, username, password, edl = get_cmr_token(args.endpoint, settings)
    cmr = None
    token = None

    k_cycle = cslc_utils.determine_k_cycle(dateutil.parser.isoparse("20170227T230524"), None, 831, disp_burst_map_hist, 10, args, token, cmr, settings)
    assert k_cycle == 2

    k_cycle = cslc_utils.determine_k_cycle(dateutil.parser.isoparse("20160702T230546"), None, 832, disp_burst_map_hist, 10, args, token, cmr, settings)
    assert k_cycle == 1

    k_cycle = cslc_utils.determine_k_cycle(dateutil.parser.isoparse("20161229T230549"), None, 832, disp_burst_map_hist, 10, args, token, cmr, settings)
    assert k_cycle == 0

def test_frame_geo_map():
    """Test that the frame geo simple map is correctly constructed"""
    frame_geo_map = cslc_utils.process_frame_geo_json()
    assert frame_geo_map[10859] == [[-101.239536, 20.325197], [-100.942045, 21.860135], [-98.526059, 21.55014], [-98.845633, 20.021978], [-101.239536, 20.325197]]

def test_frame_bounds():
    """Test that the frame bounds is correctly computed and formatted"""
    frame_geo_map = cslc_utils.process_frame_geo_json()
    bounds = cslc_utils.get_bounding_box_for_frame(10859, frame_geo_map)
    assert bounds == [-101.239536, 20.021978, -98.526059, 21.860135]