#!/usr/bin/env python3

import pytest
import conftest

from data_subscriber import cslc_utils
from data_subscriber.parser import create_parser
from data_subscriber.cslc import cslc_query
from datetime import datetime
from data_subscriber.query import DateTimeRange

hist_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=historical", "--start-date=2021-01-24T23:00:00Z",\
                  "--end-date=2021-01-24T23:00:00Z", "--frame-range=100,101"]

disp_burst_map, burst_to_frame, metadata, version = cslc_utils.process_disp_frame_burst_json(cslc_utils.DISP_FRAME_BURST_MAP_JSON)
disp_burst_map_hist = cslc_utils.process_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

def test_split_download_batch_id():
    """Test that the download batch id is correctly split into frame and acquisition cycle"""
    # Forward and reprocessing mode
    frame_id, acquisition_cycle = cslc_utils.split_download_batch_id("f100_a200")
    assert frame_id == 100
    assert acquisition_cycle == 200

    # Historical mode
    frame_id, acquisition_cycle = cslc_utils.split_download_batch_id("2023_10_01t00_00_00z_2023_10_25t00_00_00z_3601")
    assert frame_id == 3601
    assert acquisition_cycle == None

def test_arg_expansion():
    '''Test that the native_id field is expanded correctly for a given frame range'''
    l, native_id = cslc_utils.build_cslc_native_ids(100, disp_burst_map)
    #print("----------------------------------")
    assert l == 27
    assert native_id == \
           "OPERA_L2_CSLC-S1_T001-000793-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000793-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000793-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW3*\
&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW3*"

def test_burst_to_frame_map():
    '''Test that the burst to frame map is correctly constructed'''
    assert burst_to_frame["T001-000792-IW1"] == [99]
    assert burst_to_frame["T001-000793-IW1"] == [99, 100]

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

    # Test historical mode, forward works the same way
    hist_args = create_parser().parse_args(hist_arguments)
    download_batch_id = cslc_utils.download_batch_id_hist(hist_args, granule)
    assert download_batch_id == "2021_01_24t23_00_00z_2021_01_24t23_00_00z_7098"

def test_build_ccslc_m_index():
    """Test that the ccslc_m index is correctly constructed"""
    assert cslc_utils.build_ccslc_m_index("T027-056778-IW1", 445) == "t027_056778_iw1_445"

def test_determine_acquisition_cycle_cslc():
    """Test that the acquisition cycle is correctly determined"""
    acquisition_cycle = cslc_utils.determine_acquisition_cycle_cslc("T034-071111-IW1", "20240406T002953Z",
                                                                    "doesn't matter")
    assert acquisition_cycle == 460

    acquisition_cycle = cslc_utils.determine_acquisition_cycle_cslc("T001-000001-IW1", "20160703T000000Z",
                                                                    "doesn't matter")
    assert acquisition_cycle == 224

