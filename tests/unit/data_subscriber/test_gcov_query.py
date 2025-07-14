import json
import os
import asyncio
from datetime import datetime

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

import data_subscriber.cmr as cmr
from data_subscriber.gcov.gcov_query import NisarGcovCmrQuery
from data_subscriber.query import DateTimeRange

"""
Local only unit tests for gcov_query. Mocks out interactions with CMR.
"""

@pytest.fixture
def example_cmr_response():
    """Load the example CMR response from file."""
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data", "example_cmr_query_response_gcov.json")) as f:
        return json.load(f)


@pytest.fixture
def mock_granules(example_cmr_response):
    """Create expected granule IDs from the example response for verification."""
    granules = cmr.response_jsons_to_cmr_granules(cmr.Collection.NISAR_GCOV_BETA_V1, [example_cmr_response])
    return granules


@pytest.fixture(scope="module")
def mgrs_test_db_path():
    """Path to the test MGRS Track Frame database."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data", "MGRS_collection_db_DSWx-NI_v0.1.sqlite")

@pytest.fixture
def mock_args():
    """Create a mock args object with the necessary attributes."""
    args = MagicMock()
    args.collection = "NISAR_L2_GCOV_BETA_V1"
    args.max_revision = 1000
    return args

@pytest.fixture
def query_params(mgrs_test_db_path, mock_args):
    """Create parameters for initializing the NisarGcovCmrQuery object."""
    return {
        'args': mock_args,
        'token': 'test-token',
        'es_conn': MagicMock(),
        'cmr': 'cmr-host',
        'job_id': 'test-job',
        'settings': {"SHORTNAME_FILTERS": []},
        'mgrs_track_frame_db_file': mgrs_test_db_path
    }


def test_query_cmr_mocked(example_cmr_response, query_params):
    """Test the query_cmr method of NisarGcovCmrQuery class."""

    # MOCK OUT CMR REQUESTS
    # Create a future for our mock response
    future_result = asyncio.Future()
    future_result.set_result([example_cmr_response]) 
    # Create an async mock for the async_cmr_posts function
    async_mock = AsyncMock(return_value=[example_cmr_response]) 
    # Replace the real function with our mock
    cmr.async_cmr_posts = async_mock
    
    # Initialize the query object
    query = NisarGcovCmrQuery(**query_params)
    
    # Execute the query_cmr method with arbitrary time range because of mock
    timerange = DateTimeRange(
        start_date="2023-01-01T00:00:00Z",
        end_date="2023-12-31T23:59:59Z"
    )
    now = datetime.now()
    
    granules = query.query_cmr(timerange, now)
 
    assert async_mock.called 
    assert len(granules) == 10

def test_get_frames_from_granules(mock_granules, query_params):
    """Test the get_frames_from_granules method of NisarGcovCmrQuery class."""
    query = NisarGcovCmrQuery(**query_params)
    frames = query._get_frames_from_granules(mock_granules)
    
    assert len(frames) == 10
    assert all(frames)
    assert frames == [11, 67, 67, 67, 68, 68, 69, 70, 70, 71]

def test_get_frames_from_granules_single(mock_granules, query_params):
    """Test the get_frames_from_granules method of NisarGcovCmrQuery class."""
    query = NisarGcovCmrQuery(**query_params)
    frames = query._get_frames_from_granules([mock_granules[0]])
    
    assert len(frames) == 1
    assert all(frames)
    assert frames == [11]

def test_get_mgrs_sets_from_granules_single(mock_granules, query_params):
    """Test the get_mgrs_sets_from_granules method of NisarGcovCmrQuery class."""
    query = NisarGcovCmrQuery(**query_params)
    mgrs_sets = query._get_mgrs_sets_from_granules([mock_granules[0]])
    assert len(mgrs_sets) == 519
    assert all(mgrs_sets.values())

def test_get_mgrs_sets_from_granules(mock_granules, query_params):
    """Test the get_mgrs_sets_from_granules method of NisarGcovCmrQuery class."""
    query = NisarGcovCmrQuery(**query_params)
    mgrs_sets = query._get_mgrs_sets_from_granules(mock_granules)
    
    assert len(mgrs_sets) == 1730
    assert all(mgrs_sets.values())
