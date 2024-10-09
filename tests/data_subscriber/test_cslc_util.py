#!/usr/bin/env python3

import pytest
import conftest
from pathlib import Path
from data_subscriber import cslc_utils
from data_subscriber.cslc_utils import CSLCDependency, DispS1BlackoutDates, process_disp_blackout_dates
from data_subscriber.parser import create_parser
import dateutil
from datetime import datetime, timedelta
from data_subscriber.cmr import DateTimeRange, get_cmr_token
from util.conf_util import SettingsConf

hist_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=historical", "--start-date=2021-01-24T23:00:00Z",\
                  "--end-date=2021-01-24T23:00:00Z", "--frame-range=100,101"]

disp_burst_map_hist, burst_to_frames, datetime_to_frames = cslc_utils.localize_disp_frame_burst_hist()

frame_blackout_dates = process_disp_blackout_dates(Path(__file__).parent / "sample_disp_s1_blackout.json")
blackout_dates_obj = DispS1BlackoutDates(frame_blackout_dates, disp_burst_map_hist, burst_to_frames)

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
    diff_time = disp_burst_map_hist[46800].sensing_datetimes[0] - dateutil.parser.isoparse("2019-11-14T16:51:00")
    assert diff_time.total_seconds() < 60

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

    assert burst_id == "T158-338083-IW1"
    assert acquisition_dts == dateutil.parser.isoparse("20170403T130213")
    assert acquisition_cycles == {42261: 276}
    assert frame_ids == [42261]

    #TODO: 09-05-2024 Uncomment after the database file has been updated
    '''burst_id, acquisition_dts, acquisition_cycles, frame_ids = \
        cslc_utils.parse_cslc_native_id(
            "OPERA_L2_CSLC-S1_T107-227769-IW3_20240902T003138Z_20240903T073341Z_S1A_VV_v1.1", burst_to_frames,
            disp_burst_map_hist)'''

def test_generate_arbitrary_cslc_native_id():
    """Test that the native_id is correctly generated"""

    acquisition_datetime = dateutil.parser.isoparse("20170403T130213")
    production_datetime = dateutil.parser.isoparse("20240403T130213")
    native_id = cslc_utils.generate_arbitrary_cslc_native_id(disp_burst_map_hist, 42261, 0, acquisition_datetime, production_datetime, "VV")
    assert native_id == "OPERA_L2_CSLC-S1_T158-338081-IW1_20170403T130213Z_20240403T130213Z_S1A_VV_v1.1"

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

    cmr = None
    token = None

    cslc_dependency = CSLCDependency(10, 1, disp_burst_map_hist, args, token, cmr, settings, blackout_dates_obj) # m doesn't matter here

    k_cycle = cslc_dependency.determine_k_cycle(dateutil.parser.isoparse("20170227T230524"), None, 831)
    assert k_cycle == 2

    k_cycle = cslc_dependency.determine_k_cycle(dateutil.parser.isoparse("20160702T230546"), None, 832)
    assert k_cycle == 1

    k_cycle = cslc_dependency.determine_k_cycle(dateutil.parser.isoparse("20161229T230549"), None, 832)
    assert k_cycle == 0

    k_cycle = cslc_dependency.determine_k_cycle(None, 192, 10859)
    assert k_cycle == 9

    # TODO: Figure out why this isn't working and then create unit test for acquisition date outside of the historical period
    '''cmr, token, username, password, edl = get_cmr_token(args.endpoint, settings)
    k_cycle = cslc_utils.determine_k_cycle(dateutil.parser.isoparse("2024..."), None, 832, disp_burst_map_hist, 10, args, token, cmr, settings)
    assert k_cycle == 0'''

def test_get_prev_day_indices():
    args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward", "--use-temporal"])
    settings = SettingsConf().cfg
    cmr = None
    token = None

    cslc_dependency = CSLCDependency(10, 1, disp_burst_map_hist, args, token, cmr, settings, blackout_dates_obj) # k and m don't matter here

    # This falls within the historical database json so doesn't need CMR call
    prev_day_indices = cslc_dependency.get_prev_day_indices(192, 10859)
    assert prev_day_indices == [0, 24, 48, 72, 96, 120, 144, 168]

    # TODO: Figure out why get_cmr_token doesn't work in this unit test context (works fine in production) and then enable this
    # cmr, token, username, password, edl = get_cmr_token(args.endpoint, settings)
    '''prev_day_indices = prev_day_indices.get_prev_day_indices(2688, 24733)
    assert prev_day_indices == [0, 12, 36, 60, 84, 132, 156, 180, 204, 228, 234, 252, 258, 282, 294, 366, 834, 858, 1122,
     1134, 1146, 1158, 1170, 1182, 1194, 1206, 1218, 1230, 1272, 1284, 1296, 1308, 1320, 1332, 1344, 1350, 1356, 1362, 
     1368, 1374, 1380, 1386, 1392, 1404, 1410, 1416, 1422, 1428, 1434, 1440, 1446, 1452, 1458, 1464, 1476, 1482, 1488, 
     1494, 1500, 1506, 1512, 1518, 1524, 1530, 1536, 1542, 1548, 1554, 1560, 1566, 1572, 1578, 1584, 1590, 1596, 1602, 
     1608, 1614, 1620, 1626, 1632, 1638, 1644, 1650, 1656, 1662, 1668, 1674, 1680, 1686, 1698, 1704, 1710, 1722, 1728, 
     1734, 1740, 1746, 1752, 1758, 1764, 1770, 1776, 1782, 1788, 1794, 1800, 1806, 1812, 1818, 1824, 1830, 1836, 1842, 
     1848, 1854, 1860, 1866, 1872, 1878, 1884, 1890, 1896, 1902, 1908, 1914, 1920, 1926, 1932, 1938, 1944, 1950, 1956, 
     1962, 1968, 1974, 1980, 1986, 1992, 1998, 2004, 2010, 2022, 2028, 2034, 2040, 2046, 2052, 2064, 2076, 2088, 2100, 
     2112, 2124, 2136, 2148, 2160, 2172, 2184, 2196, 2208, 2220, 2232, 2244, 2256, 2268, 2292, 2304, 2316, 2328, 2340, 
     2376, 2388, 2412, 2424, 2436, 2448, 2460, 2472, 2484, 2496, 2508, 2520, 2532, 2544, 2556, 2568, 2580, 2592, 2604, 
     2616, 2628, 2640, 2652, 2664, 2676, 2736, 2724, 2712, 2700]'''

def test_process_disp_blackout_dates_parsing():
    """Test that the blackout dates are correctly processed"""

    p = Path(__file__).parent / "sample_disp_s1_blackout.json"
    blackout_dates = cslc_utils.process_disp_blackout_dates(p)
    assert len(blackout_dates[831]) == 1
    assert blackout_dates[832][0] == (dateutil.parser.isoparse("2017-01-24T23:00:00"), dateutil.parser.isoparse("2017-08-24T23:00:00"))
    assert blackout_dates[832][1] == (dateutil.parser.isoparse("2022-01-24T23:00:00"), dateutil.parser.isoparse("2022-08-24T23:00:00"))
    assert 46543 not in blackout_dates
    assert 833 not in blackout_dates

def test_process_disp_blackout_dates_comparison():
    """Test that the blackout dates are correctly compared"""

    p = Path(__file__).parent / "sample_disp_s1_blackout.json"
    blackout_dates = cslc_utils.process_disp_blackout_dates(p)
    blackout_dates_obj = cslc_utils.DispS1BlackoutDates(blackout_dates, disp_burst_map_hist, burst_to_frames)

    burst_id, acquisition_dts, acquisition_cycles, frame_ids = \
        cslc_utils.parse_cslc_native_id(
            "OPERA_L2_CSLC-S1_T042-088913-IW1_20200227T140752Z_20240430T103028Z_S1A_VV_v1.1", burst_to_frames,
            disp_burst_map_hist)

    is_black_out, dates = blackout_dates_obj.is_in_blackout(frame_ids[0], acquisition_dts)
    assert is_black_out == False

    burst_id, acquisition_dts, acquisition_cycles, frame_ids = \
        cslc_utils.parse_cslc_native_id(
            "OPERA_L2_CSLC-S1_T042-088921-IW3_20200221T140735Z_20240430T094255Z_S1B_VV_v1.1", burst_to_frames,
            disp_burst_map_hist)

    is_black_out, dates = blackout_dates_obj.is_in_blackout(frame_ids[0], acquisition_dts)
    assert is_black_out == True
    assert dates == (dateutil.parser.isoparse("2019-11-29T14:06:51"), dateutil.parser.isoparse("2020-02-27T14:07:30"))

    is_black_out, dates = blackout_dates_obj.is_in_blackout(832, dateutil.parser.isoparse("2022-01-25T14:07:30"))
    assert is_black_out == True
    assert dates == (dateutil.parser.isoparse("2022-01-24T23:00:00"), dateutil.parser.isoparse("2022-08-24T23:00:00"))

def test_filter_cslc_blackout_polarizations():
    p = Path(__file__).parent / "sample_disp_s1_blackout.json"
    blackout_dates = cslc_utils.process_disp_blackout_dates(p)
    blackout_dates_obj = cslc_utils.DispS1BlackoutDates(blackout_dates, disp_burst_map_hist, burst_to_frames)

    granules = []
    for b in range(27):
        acquisition_datetime = dateutil.parser.isoparse("20170823T130213")
        production_datetime = dateutil.parser.isoparse("20240403T130213")

        native_id = cslc_utils.generate_arbitrary_cslc_native_id(disp_burst_map_hist, 832, b, acquisition_datetime,
                                                                 production_datetime, "VV")
        granules.append({"granule_id": native_id})

        native_id = cslc_utils.generate_arbitrary_cslc_native_id(disp_burst_map_hist, 833, b, acquisition_datetime,
                                                                 production_datetime, "VV")
        granules.append({"granule_id": native_id})

    granules = cslc_utils._filter_cslc_blackout_polarization(granules, "forward", blackout_dates_obj, no_duplicate=False, force_frame_id=None)

    assert len(granules) == 33

    granules = [{"granule_id": "OPERA_L2_CSLC-S1_T042-088939-IW1_20191129T140825Z_20240501T103901Z_S1B_VV_v1.1"}]
    filtered = cslc_utils._filter_cslc_blackout_polarization(granules, "forward", blackout_dates_obj, no_duplicate=True, force_frame_id=None)
    assert len(filtered) == 0

def test_get_dependent_ccslc_index():
    prev_day_indices = [0, 24, 48, 72]
    assert "t041_086868_iw1_72" == cslc_utils.get_dependent_ccslc_index(prev_day_indices, 0, 2, "t041_086868_iw1")
    assert "t041_086868_iw1_24" == cslc_utils.get_dependent_ccslc_index(prev_day_indices, 1, 2, "t041_086868_iw1")
    prev_day_indices = [0, 24, 48, 72, 96]
    assert "t041_086868_iw1_72" == cslc_utils.get_dependent_ccslc_index(prev_day_indices, 0, 2, "t041_086868_iw1")
    assert "t041_086868_iw1_24" == cslc_utils.get_dependent_ccslc_index(prev_day_indices, 1, 2, "t041_086868_iw1")

def test_frame_bounds():
    """Test that the frame bounds is correctly computed and formatted"""
    frame_geo_map = cslc_utils.localize_frame_geo_json()
    bounds = cslc_utils.get_bounding_box_for_frame(10859, frame_geo_map)
    assert bounds == [-101.239536, 20.021978, -98.526059, 21.860135]

    bounds = cslc_utils.get_bounding_box_for_frame(21517, frame_geo_map)
    assert bounds == [-179.171315, 49.613083, 176.926336, 51.639512]