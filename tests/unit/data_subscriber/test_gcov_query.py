import json
import os
import asyncio
from datetime import datetime

import pytest
from unittest.mock import MagicMock, AsyncMock

import data_subscriber.cmr as cmr
from data_subscriber import gcov_utils
from data_subscriber.gcov.gcov_query import NisarGcovCmrQuery
from data_subscriber.gcov.asf_gcov_download import AsfDaacGcovDownload
from data_subscriber.gcov_utils import DswxNiProductsToProcess
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
    granules = cmr.response_jsons_to_cmr_granules(cmr.Collection.NISAR_GCOV, [example_cmr_response])
    return granules


@pytest.fixture(scope="module")
def mgrs_test_db_path():
    """Path to the test MGRS Track Frame database."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data",
                        "DSWx-NI_MGRS_collection_DB_0.4.sqlite")

@pytest.fixture
def mock_args():
    """Create a mock args object with the necessary attributes."""
    args = MagicMock()
    args.collection = "NISAR_L2_GCOV_PROVISIONAL_V1"
    args.max_revision = 1000
    args.job_queue = "test-queue"
    args.query_replacement_file = None
    args.native_id = None
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
        'settings': {"SHORTNAME_FILTERS": [], "RELEASE_VERSION": "test-version"},
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

    gcov_utils.DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH = query_params['mgrs_track_frame_db_file']
    
    granules = query.query_cmr(timerange, now)
 
    assert async_mock.called 
    assert len(granules) == 10


def test_convert_query_result_to_gcov_granules(query_params):
    """Test the _convert_query_result_to_gcov_granules method."""
    query = NisarGcovCmrQuery(**query_params)
    
    # Mock granules with the expected structure
    mock_granules = [
        {
            "granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001",
            "related_urls": [
                "s3://test-bucket/test-path/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"
            ],
            "revision_date": "2023-06-19T00:08:17Z",
            "temporal_extent_beginning_datetime": "2023-06-19T00:08:17Z"
        }
    ]
    
    # Mock the MGRS database response
    query.mgrs_track_frame_db.frame_and_track_to_mgrs_sets = MagicMock(return_value={"MS_1_1": set([1, 2, 3])})
    
    gcov_granules = query._convert_query_result_to_gcov_granules(mock_granules)
    
    assert len(gcov_granules) == 1
    
    # Check the first granule
    granule = gcov_granules[0]
    assert granule.granule_id == "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"
    assert granule.track_number == 156
    assert granule.frame_number == 11
    assert granule.cycle_number == 15 
    assert granule.mgrs_set_id == "MS_1_1"
    assert granule.s3_download_url == "s3://test-bucket/test-path/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"

def test_convert_query_result_to_gcov_granules_mgrs_lookup_failure(query_params):
    """Test _convert_query_result_to_gcov_granules when MGRS lookup fails."""
    query = NisarGcovCmrQuery(**query_params)
    
    mock_granules = [
        {
            "granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001",
            "related_urls": [
                "s3://test-bucket/test-path/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000835_T00406_M_P_J_001.h5"
            ],
            "revision_date": "2023-06-19T00:08:17Z",
            "temporal_extent_beginning_datetime": "2023-06-19T00:08:17Z"
        }
    ]
    
    # Mock the MGRS database to raise an exception
    query.mgrs_track_frame_db.frame_and_track_to_mgrs_sets = MagicMock(side_effect=Exception("DB Error"))
    
    gcov_granules = query._convert_query_result_to_gcov_granules(mock_granules)
    
    assert gcov_granules == []


def test_determine_download_granules(query_params):
    """Test the determine_download_granules method."""
    query = NisarGcovCmrQuery(**query_params)
    
    # Mock the ES connection
    query.es_conn.get_gcov_products_from_catalog = MagicMock(return_value=[
        {"_source": {"s3_download_url": "s3://test-bucket/file1.h5"}},
        {"_source": {"s3_download_url": "s3://test-bucket/file2.h5"}},
        {"_source": {"s3_download_url": "s3://test-bucket/test-path/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"}}
    ])
    
    # Mock granules
    mock_granules = [
        {
            "granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001",
            "related_urls": [
                "s3://test-bucket/test-path/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"
            ],
            "revision_date": "2023-06-19T00:08:17Z",
            "temporal_extent_beginning_datetime": "2023-06-19T00:08:17Z"
        }
    ]
    
    query.mgrs_track_frame_db.frame_and_track_to_mgrs_sets = MagicMock(return_value={"MS_156_10": {10,11},"MS_156_11": {10,11,12},"MS_156_12": {11,12,13}})

    # Mock the catalog_granules method
    query._catalog_granules = MagicMock()
    
    result = query._convert_query_result_to_gcov_granules(mock_granules)

    assert result[0].mgrs_set_id == "MS_156_10"
    assert result[0].cycle_number == 15

    assert result[1].mgrs_set_id == "MS_156_11"
    assert result[1].cycle_number == 15

    assert result[2].mgrs_set_id == "MS_156_12"
    assert result[2].cycle_number == 15


def test_create_dswx_ni_job_params(query_params):
    """Test the create_dswx_ni_job_params method."""
    dl = AsfDaacGcovDownload(cmr.Provider.ASF_NISAR_GCOV,
                             mgrs_track_frame_db_file=query_params['mgrs_track_frame_db_file'])
    
    # Create a mock set to process
    set_to_process = DswxNiProductsToProcess(
        mgrs_set_id="MS_1_2",
        cycle_number=15,
        gcov_input_product_urls=["s3://test-bucket/file1.h5", "s3://test-bucket/file2.h5"],
        gcov_input_product_https_urls=["https://path/to/file1.h5", "https://path/to/file2.h5"]
    )
    
    params = dl.create_dswx_ni_job_params(set_to_process)
    
    assert len(params) == 4
    
    # Check mgrs_set_id parameter
    mgrs_param = next(p for p in params if p["name"] == "mgrs_set_id")
    assert mgrs_param["value"] == "MS_1_2"
    
    # Check cycle_number parameter
    cycle_param = next(p for p in params if p["name"] == "cycle_number")
    assert cycle_param["value"] == 15
    
    # Check gcov_input_product_urls parameter
    urls_param = next(p for p in params if p["name"] == "gcov_input_product_urls")
    assert len(urls_param["value"]) == 2
    
    # Check product_metadata parameter
    metadata_param = next(p for p in params if p["name"] == "product_metadata")
    assert metadata_param["value"]["dataset"] == "L3_DSWx_NI-MS_1_2-15"
