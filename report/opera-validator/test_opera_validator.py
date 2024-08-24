import pytest
import requests
import json
from opera_validator import get_burst_id, get_burst_sensing_datetime, map_bursts_to_frames

def test_get_burst_id():
    assert get_burst_id("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0") == "t020_041121_iw1"
    assert get_burst_id("OPERA_L2_CSLC-S1_T150-320187-IW3_20240718T232012Z_20240719T192611Z_S1A_VV_v1.1") == "t150_320187_iw3"

def test_get_burst_sensing_datetime():
    assert get_burst_sensing_datetime("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0") == "20231101T013115Z"
    assert get_burst_sensing_datetime("OPERA_L2_CSLC-S1_T150-320187-IW3_20240718T232012Z_20240719T192611Z_S1A_VV_v1.1") == "20240718T232012Z"

def test_map_bursts_to_frames(mocker):
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
    df = map_bursts_to_frames(burst_ids, "dummy_bursts_to_frames.json", "dummy_frames_to_bursts.json")
    
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