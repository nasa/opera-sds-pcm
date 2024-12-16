import pytest
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from opv_util import generate_url_params
from opera_validator import (
    get_burst_id, 
    get_burst_sensing_datetime, 
    map_cslc_bursts_to_frames,
    validate_dswx_s1
)
from data_subscriber.cslc_utils import parse_cslc_native_id, localize_disp_frame_burst_hist
from opv_disp_s1 import get_frame_to_dayindex_to_granule, filter_for_trigger_frame

def test_get_burst_id():
    assert get_burst_id("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0") == "t020_041121_iw1"
    assert get_burst_id("OPERA_L2_CSLC-S1_T150-320187-IW3_20240718T232012Z_20240719T192611Z_S1A_VV_v1.1") == "t150_320187_iw3"

def test_get_burst_sensing_datetime():
    assert get_burst_sensing_datetime("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0") == "20231101T013115Z"
    assert get_burst_sensing_datetime("OPERA_L2_CSLC-S1_T150-320187-IW3_20240718T232012Z_20240719T192611Z_S1A_VV_v1.1") == "20240718T232012Z"

def test_generate_url_params():
    # Test case 1: Temporal
    start = "2024-08-23T00:00:00Z"
    end = "2024-08-24T00:00:00Z"
    endpoint = "OPS"
    provider = "ASF"
    short_name = "OPERA_L2_RTC-S1_V1"
    timestamp_type = "temporal"
    
    base_url, params = generate_url_params(start, end, endpoint, provider, short_name, 30, timestamp_type)
    
    assert base_url == "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    assert params['provider'] == provider
    assert params['ShortName[]'] == short_name
    assert params['temporal'] == f"{start},{end}"
    
    # Test case 2: Revision
    timestamp_type = "revision"
    
    base_url, params = generate_url_params(start, end, endpoint, provider, short_name, 30, timestamp_type)
    
    assert base_url == "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    assert params['provider'] == provider
    assert params['ShortName[]'] == short_name
    assert params['revision_date'] == f"{start},{end}"
    assert 'temporal' in params # similar to how ops fwd works, we check only for products with temporal time > 30 days ago 
    
    # Test case 3: UAT Endpoint
    endpoint = "UAT"
    
    base_url, params = generate_url_params(start, end, endpoint, provider, short_name, 30, timestamp_type)
    
    assert base_url == "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"
    assert params['provider'] == provider
    assert params['ShortName[]'] == short_name

def test_frame_to_dayindex_to_granule():
    frame_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()
    frames_to_validate = set(frame_to_bursts.keys())
    granule_ids = ['OPERA_L2_CSLC-S1_T062-131279-IW1_20241215T223549Z_20241216T172017Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071061-IW1_20160705T002652Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071061-IW2_20160705T002653Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071061-IW3_20160705T002654Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071062-IW1_20160705T002655Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071062-IW2_20160705T002656Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071062-IW3_20160705T002657Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071063-IW1_20160705T002658Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071063-IW2_20160705T002659Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071064-IW1_20160705T002700Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071063-IW3_20160705T002700Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071064-IW2_20160705T002701Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071064-IW3_20160705T002702Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071065-IW1_20160705T002703Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071065-IW2_20160705T002704Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071065-IW3_20160705T002705Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071066-IW1_20160705T002706Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071066-IW2_20160705T002707Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071066-IW3_20160705T002708Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071067-IW1_20160705T002709Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071067-IW2_20160705T002710Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071067-IW3_20160705T002711Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071068-IW1_20160705T002712Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071068-IW2_20160705T002712Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071068-IW3_20160705T002713Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071069-IW1_20160705T002714Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071069-IW2_20160705T002715Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071069-IW3_20160705T002716Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071070-IW1_20160705T002717Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071070-IW2_20160705T002718Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071070-IW3_20160705T002719Z_20240425T202502Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071071-IW1_20160705T002720Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071071-IW2_20160705T002721Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071071-IW3_20160705T002722Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071072-IW1_20160705T002723Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071072-IW2_20160705T002724Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071072-IW3_20160705T002724Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071073-IW1_20160705T002725Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071073-IW2_20160705T002726Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071073-IW3_20160705T002727Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071074-IW1_20160705T002728Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071074-IW2_20160705T002729Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071074-IW3_20160705T002730Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071075-IW1_20160705T002731Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071075-IW2_20160705T002732Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071075-IW3_20160705T002733Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071076-IW1_20160705T002734Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071076-IW3_20160705T002735Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071076-IW2_20160705T002735Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071077-IW1_20160705T002736Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071077-IW2_20160705T002737Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071077-IW3_20160705T002738Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071078-IW1_20160705T002739Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071078-IW2_20160705T002740Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071078-IW3_20160705T002741Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071079-IW1_20160705T002742Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071079-IW2_20160705T002743Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071079-IW3_20160705T002744Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071080-IW1_20160705T002745Z_20240611T005200Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071080-IW2_20160705T002746Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071080-IW3_20160705T002747Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071081-IW1_20160705T002747Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071081-IW2_20160705T002748Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071081-IW3_20160705T002749Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071082-IW1_20160705T002750Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071082-IW2_20160705T002751Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071082-IW3_20160705T002752Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071083-IW1_20160705T002753Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071083-IW2_20160705T002754Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071083-IW3_20160705T002755Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071084-IW1_20160705T002756Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071084-IW2_20160705T002757Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071085-IW1_20160705T002758Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071084-IW3_20160705T002758Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071085-IW2_20160705T002759Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071085-IW3_20160705T002800Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071086-IW1_20160705T002801Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071086-IW2_20160705T002802Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071086-IW3_20160705T002803Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071087-IW1_20160705T002804Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071087-IW2_20160705T002805Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071087-IW3_20160705T002806Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071088-IW1_20160705T002807Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071088-IW2_20160705T002808Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071088-IW3_20160705T002809Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071089-IW1_20160705T002809Z_20240425T202006Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071089-IW2_20160705T002810Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071089-IW3_20160705T002811Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071090-IW1_20160705T002812Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071090-IW2_20160705T002813Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071090-IW3_20160705T002814Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071091-IW1_20160705T002815Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071091-IW2_20160705T002816Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071091-IW3_20160705T002817Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071092-IW1_20160705T002818Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071092-IW2_20160705T002819Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071093-IW1_20160705T002820Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071092-IW3_20160705T002820Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071093-IW2_20160705T002821Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071093-IW3_20160705T002822Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071094-IW1_20160705T002823Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071094-IW2_20160705T002824Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071094-IW3_20160705T002825Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071095-IW1_20160705T002826Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071095-IW2_20160705T002827Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071095-IW3_20160705T002828Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071096-IW1_20160705T002829Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071096-IW2_20160705T002830Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071096-IW3_20160705T002831Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071097-IW1_20160705T002832Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071097-IW2_20160705T002832Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071097-IW3_20160705T002833Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071098-IW1_20160705T002834Z_20240425T202246Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071098-IW2_20160705T002835Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071098-IW3_20160705T002836Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071099-IW1_20160705T002837Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071099-IW2_20160705T002838Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071099-IW3_20160705T002839Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071100-IW1_20160705T002840Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071100-IW2_20160705T002841Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071100-IW3_20160705T002842Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071101-IW1_20160705T002843Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071101-IW2_20160705T002843Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071101-IW3_20160705T002844Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071102-IW1_20160705T002845Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071102-IW2_20160705T002846Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071102-IW3_20160705T002847Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071103-IW1_20160705T002848Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071103-IW2_20160705T002849Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071103-IW3_20160705T002850Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071104-IW1_20160705T002851Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071104-IW2_20160705T002852Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071104-IW3_20160705T002853Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071105-IW1_20160705T002854Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071105-IW3_20160705T002855Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071105-IW2_20160705T002855Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071106-IW1_20160705T002856Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071106-IW2_20160705T002857Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071106-IW3_20160705T002858Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071107-IW1_20160705T002859Z_20240425T202223Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071107-IW2_20160705T002900Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071107-IW3_20160705T002901Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071108-IW1_20160705T002902Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071108-IW2_20160705T002903Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071108-IW3_20160705T002904Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071109-IW1_20160705T002905Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071109-IW2_20160705T002906Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071110-IW1_20160705T002907Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071109-IW3_20160705T002907Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071110-IW2_20160705T002908Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071110-IW3_20160705T002909Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071111-IW1_20160705T002910Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071111-IW2_20160705T002911Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071111-IW3_20160705T002912Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071112-IW1_20160705T002913Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071112-IW2_20160705T002914Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071112-IW3_20160705T002915Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071113-IW1_20160705T002916Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071113-IW2_20160705T002917Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071113-IW3_20160705T002918Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071114-IW1_20160705T002918Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071114-IW2_20160705T002919Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071114-IW3_20160705T002920Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071115-IW1_20160705T002921Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071115-IW2_20160705T002922Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071115-IW3_20160705T002923Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071116-IW1_20160705T002924Z_20240611T005148Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071116-IW2_20160705T002925Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071116-IW3_20160705T002926Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071117-IW1_20160705T002927Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071117-IW2_20160705T002928Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071118-IW1_20160705T002929Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071117-IW3_20160705T002929Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071118-IW2_20160705T002930Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071118-IW3_20160705T002931Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071119-IW1_20160705T002932Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071119-IW2_20160705T002933Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071119-IW3_20160705T002934Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071120-IW1_20160705T002935Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071120-IW2_20160705T002936Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071120-IW3_20160705T002937Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071121-IW1_20160705T002938Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071121-IW2_20160705T002939Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071121-IW3_20160705T002940Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071122-IW1_20160705T002940Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071122-IW2_20160705T002941Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071122-IW3_20160705T002942Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071123-IW1_20160705T002943Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071123-IW2_20160705T002944Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071123-IW3_20160705T002945Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071124-IW1_20160705T002946Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071124-IW2_20160705T002947Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071124-IW3_20160705T002948Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071125-IW1_20160705T002949Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071125-IW2_20160705T002950Z_20240611T005013Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071125-IW3_20160705T002951Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071126-IW2_20160705T002952Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071126-IW1_20160705T002952Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071126-IW3_20160705T002953Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071127-IW1_20160705T002954Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071127-IW2_20160705T002955Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071127-IW3_20160705T002956Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071128-IW1_20160705T002957Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071128-IW2_20160705T002958Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071128-IW3_20160705T002959Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071129-IW1_20160705T003000Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071129-IW2_20160705T003001Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071129-IW3_20160705T003002Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071130-IW1_20160705T003003Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071130-IW2_20160705T003003Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071130-IW3_20160705T003004Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071131-IW1_20160705T003005Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071131-IW2_20160705T003006Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071131-IW3_20160705T003007Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071132-IW1_20160705T003008Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071132-IW2_20160705T003009Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071132-IW3_20160705T003010Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071133-IW1_20160705T003011Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071133-IW2_20160705T003012Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071133-IW3_20160705T003013Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071134-IW1_20160705T003014Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071134-IW3_20160705T003015Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071134-IW2_20160705T003015Z_20240611T005139Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071135-IW1_20160705T003016Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071135-IW2_20160705T003017Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071135-IW3_20160705T003018Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071136-IW1_20160705T003019Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071136-IW2_20160705T003020Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071136-IW3_20160705T003021Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071137-IW1_20160705T003022Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071137-IW2_20160705T003023Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071137-IW3_20160705T003024Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071138-IW1_20160705T003025Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071138-IW2_20160705T003026Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071139-IW1_20160705T003027Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071138-IW3_20160705T003027Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071139-IW2_20160705T003028Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071139-IW3_20160705T003029Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071140-IW1_20160705T003030Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071140-IW2_20160705T003031Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071140-IW3_20160705T003032Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071141-IW1_20160705T003033Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071141-IW2_20160705T003034Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071141-IW3_20160705T003035Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071142-IW1_20160705T003036Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071142-IW2_20160705T003037Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071142-IW3_20160705T003038Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071143-IW1_20160705T003038Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071143-IW2_20160705T003039Z_20240611T004822Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071143-IW3_20160705T003040Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071144-IW1_20160705T003041Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071144-IW2_20160705T003042Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071144-IW3_20160705T003043Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071145-IW1_20160705T003044Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071145-IW2_20160705T003045Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071145-IW3_20160705T003046Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071146-IW1_20160705T003047Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071146-IW2_20160705T003048Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071147-IW1_20160705T003049Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071146-IW3_20160705T003049Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071147-IW2_20160705T003050Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071147-IW3_20160705T003051Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071148-IW1_20160705T003052Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071148-IW2_20160705T003053Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071148-IW3_20160705T003054Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071149-IW1_20160705T003055Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071149-IW2_20160705T003056Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071149-IW3_20160705T003057Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071150-IW1_20160705T003058Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071150-IW2_20160705T003059Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071150-IW3_20160705T003100Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071151-IW1_20160705T003100Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071151-IW2_20160705T003101Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071151-IW3_20160705T003102Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071152-IW1_20160705T003103Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071152-IW2_20160705T003104Z_20240611T004809Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071152-IW3_20160705T003105Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071153-IW1_20160705T003106Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071153-IW2_20160705T003107Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071153-IW3_20160705T003108Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071154-IW1_20160705T003109Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071154-IW2_20160705T003110Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071154-IW3_20160705T003111Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071155-IW1_20160705T003111Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071155-IW2_20160705T003112Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071155-IW3_20160705T003113Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071156-IW1_20160705T003114Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071156-IW2_20160705T003115Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071156-IW3_20160705T003116Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071157-IW1_20160705T003117Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071157-IW2_20160705T003118Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071157-IW3_20160705T003119Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071158-IW1_20160705T003120Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071158-IW2_20160705T003121Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071158-IW3_20160705T003122Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071159-IW1_20160705T003123Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071159-IW2_20160705T003123Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071159-IW3_20160705T003124Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071160-IW1_20160705T003125Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071160-IW2_20160705T003126Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071160-IW3_20160705T003127Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071161-IW1_20160705T003128Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071161-IW2_20160705T003129Z_20240611T005143Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071161-IW3_20160705T003130Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071162-IW1_20160705T003131Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071162-IW2_20160705T003132Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071162-IW3_20160705T003133Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071163-IW1_20160705T003134Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071163-IW3_20160705T003135Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071163-IW2_20160705T003135Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071164-IW1_20160705T003136Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071164-IW2_20160705T003137Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071164-IW3_20160705T003138Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071165-IW1_20160705T003139Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071165-IW2_20160705T003140Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071165-IW3_20160705T003141Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071166-IW1_20160705T003142Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071166-IW2_20160705T003143Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071166-IW3_20160705T003144Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071167-IW1_20160705T003145Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071167-IW3_20160705T003146Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071167-IW2_20160705T003146Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071168-IW1_20160705T003147Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071168-IW2_20160705T003148Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071168-IW3_20160705T003149Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071169-IW1_20160705T003150Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071169-IW2_20160705T003151Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071169-IW3_20160705T003152Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071170-IW1_20160705T003153Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071170-IW2_20160705T003154Z_20240611T004808Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071170-IW3_20160705T003155Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071171-IW1_20160705T003156Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071171-IW2_20160705T003157Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071171-IW3_20160705T003158Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071172-IW1_20160705T003158Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071172-IW2_20160705T003159Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071172-IW3_20160705T003200Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071173-IW1_20160705T003201Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071173-IW2_20160705T003202Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071173-IW3_20160705T003203Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071174-IW1_20160705T003204Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071174-IW2_20160705T003205Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071174-IW3_20160705T003206Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071175-IW1_20160705T003207Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071175-IW2_20160705T003208Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071176-IW1_20160705T003209Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071175-IW3_20160705T003209Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071176-IW2_20160705T003210Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071176-IW3_20160705T003211Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071177-IW1_20160705T003212Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071177-IW2_20160705T003213Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071177-IW3_20160705T003214Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071178-IW1_20160705T003215Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071178-IW2_20160705T003216Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071178-IW3_20160705T003217Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071179-IW1_20160705T003218Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071179-IW2_20160705T003219Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071180-IW1_20160705T003220Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071179-IW3_20160705T003220Z_20240611T005152Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071180-IW2_20160705T003221Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071180-IW3_20160705T003222Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071181-IW1_20160705T003223Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071181-IW2_20160705T003224Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071181-IW3_20160705T003225Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071182-IW1_20160705T003226Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071182-IW2_20160705T003227Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071182-IW3_20160705T003228Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071183-IW1_20160705T003229Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071183-IW2_20160705T003230Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071184-IW1_20160705T003231Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071183-IW3_20160705T003231Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071184-IW2_20160705T003232Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071184-IW3_20160705T003233Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071185-IW1_20160705T003234Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071185-IW2_20160705T003235Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071185-IW3_20160705T003236Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071186-IW1_20160705T003237Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071186-IW2_20160705T003238Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071186-IW3_20160705T003239Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071187-IW1_20160705T003240Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071187-IW2_20160705T003241Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071187-IW3_20160705T003242Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071188-IW2_20160705T003243Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071188-IW1_20160705T003243Z_20240611T005147Z_S1A_VV_v1.1', 'OPERA_L2_CSLC-S1_T034-071188-IW3_20160705T003244Z_20240611T005147Z_S1A_VV_v1.1']
    frame_to_dayindex_to_granule = get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames,
                                                                    frame_to_bursts)

    '''print(frame_to_dayindex_to_granule)
    for frame_id in frame_to_dayindex_to_granule:
        print(frame_id)
        print(frame_to_dayindex_to_granule[frame_id])'''

    assert len(frame_to_dayindex_to_granule) == 16
    assert len(frame_to_dayindex_to_granule[8884].keys()) == 1
    assert len(frame_to_dayindex_to_granule[8884][0]) == 27

    # This will be removed by the next function
    assert len(frame_to_dayindex_to_granule[16410].keys()) == 1
    assert len(frame_to_dayindex_to_granule[16410][3000]) == 1

    filtered_dict = filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames)

    assert len(filtered_dict) == 15

@pytest.mark.skip
def test_map_cslc_bursts_to_frames(mocker):
    # Mock data for bursts to frames JSON
    bursts_to_frames_json = """
    {
        "data": {
            "t001_000001_iw1": {"frame_ids": [1]},
            "t001_000001_iw2": {"frame_ids": [1]},
            "t001_000003_iw1": {"frame_ids": [2]}
        }
    }
    """
    
    # Mock data for frames to bursts JSON
    frames_to_bursts_json = """
    {
        "data": {
            "1": {
                "burst_ids": [
                    "t001_000001_iw1",
                    "t001_000001_iw2",
                    "t001_000002_iw1",
                    "t001_000002_iw2"
                ]
            },
            "2": {
                "burst_ids": [
                    "t001_000003_iw1",
                    "t001_000003_iw2",
                    "t001_000003_iw3"
                ]
            }
        }
    }
    """

    # Mock the open function to return the mock JSON data
    mocker.patch('builtins.open', mocker.mock_open())
    
    # When the first file is opened, return the bursts to frames JSON
    mocker.patch('json.load', side_effect=[
        json.loads(bursts_to_frames_json), 
        json.loads(frames_to_bursts_json)
    ])
    
    # Define input burst IDs
    burst_ids = ["t001_000001_iw1", "t001_000001_iw2", "t001_000003_iw1"]
    
    # Call the function with the mocked data
    df = map_cslc_bursts_to_frames(burst_ids, "dummy_bursts_to_frames.json", "dummy_frames_to_bursts.json")
    
    # Assert that the DataFrame has the correct shape 
    assert df.shape == (2, 5)
    
    # Assert that the Frame ID for the first row is 1
    assert df.loc[0, 'Frame ID'] == 1
    
    # Assert that the All Possible Bursts column contains the correct bursts for frame 1
    assert set(df.loc[0, 'All Possible Bursts']) == {"t001_000001_iw1", "t001_000001_iw2", "t001_000002_iw1", "t001_000002_iw2"}
    
    # Assert that the Matching Bursts column contains the correct matching bursts for frame 1
    assert set(df.loc[0, 'Matching Bursts']) == {"t001_000001_iw1", "t001_000001_iw2"}
    
    # Assert that the Frame ID for the second row is 2
    assert df.loc[1, 'Frame ID'] == 2
    
    # Assert that the All Possible Bursts column contains the correct bursts for frame 2
    assert set(df.loc[1, 'All Possible Bursts']) == {"t001_000003_iw1", "t001_000003_iw2", "t001_000003_iw3"}
    
    # Assert that the Matching Bursts column contains the correct matching bursts for frame 2
    assert set(df.loc[1, 'Matching Bursts']) == {"t001_000003_iw1"}
