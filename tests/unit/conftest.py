
import sys
import types
from functools import cache
from os.path import abspath, dirname, join

import geopandas as gpd
import pytest
import requests
from geopandas import GeoDataFrame


# create mocks for HySDS modules which would otherwise prevent unit testing
# this leverages sys.modules[] which is used during repeated imports when
# resolving modules
class MockIndicesClient:
    def refresh(self):
        pass

class MockElasticsearch:
    def __init__(self, *args, **kwargs):
        self.indices = MockIndicesClient()

    def update_by_query(self, index, body=None, doc_type=None, params=None, headers=None):
        pass

class MockElasticsearchUtility:
    def __init__(self, *args, **kwargs):
        self.es = MockElasticsearch()

    def query(self, **kwargs):
        pass

    def index_document(self, **kwargs):
        pass

    def update_document(self, **kwargs):
        pass


def submit_mozart_job(*args, **kwargs):
    pass

@cache
def mock_load_mgrs_burst_db_raw(filter_land=True) -> GeoDataFrame:
    mtc_local_filepath = join(dirname(abspath(__file__)), "data_subscriber", "test_data", "MGRS_tile_collection.sqlite")

    vector_gdf = gpd.read_file(mtc_local_filepath, crs="EPSG:4326")

    if filter_land:
        vector_gdf = vector_gdf[vector_gdf["land_ocean_flag"].isin(["water/land", "land"])]

    return vector_gdf


mock_celeryconfig = types.ModuleType('celeryconfig')
sys.modules['celeryconfig'] = mock_celeryconfig
mock_celeryconfig.GRQ_AWS_ES = False
mock_celeryconfig.GRQ_ES_HOST = None
mock_celeryconfig.GRQ_ES_URL = None
mock_celeryconfig.AWS_REGION = None
mock_celeryconfig.ES_CLUSTER_MODE = "dummy"
mock_celeryconfig.JOBS_ES_URL = "http://127.0.0.1:9200"
mock_celeryconfig.METRICS_ES_URL = "http://127.0.0.1/"

mock_elasticsearch_utils = types.ModuleType('hysds_commons.elasticsearch_utils')
sys.modules['hysds_commons.elasticsearch_utils'] = mock_elasticsearch_utils
mock_elasticsearch_utils.ElasticsearchUtility = MockElasticsearchUtility

mock_job_utils = types.ModuleType('hysds_commons.job_utils')
sys.modules['hysds_commons.job_utils'] = mock_job_utils
mock_job_utils.submit_mozart_job = submit_mozart_job

mock_commons_es_connection = types.ModuleType('commons.es_connection')
sys.modules['commons.es_connection'] = mock_commons_es_connection
mock_commons_es_connection.get_grq_es = lambda *args, **kwargs: None

mock_mgrs_bursts_collection_db_client = types.ModuleType('data_subscriber.rtc.mgrs_bursts_collection_db_client')
sys.modules['data_subscriber.rtc.mgrs_bursts_collection_db_client'] = mock_mgrs_bursts_collection_db_client
mock_mgrs_bursts_collection_db_client.cached_load_mgrs_burst_db = mock_load_mgrs_burst_db_raw


@pytest.fixture(autouse=True)
def deny_network_requests(monkeypatch):
    monkeypatch.delattr(requests.sessions.Session, requests.sessions.Session.request.__name__)
