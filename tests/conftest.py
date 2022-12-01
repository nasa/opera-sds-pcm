import sys
import types

import pytest
import requests


# create mocks for HySDS modules which would otherwise prevent unit testing
# this leverages sys.modules[] which is used during repeated imports when
# resolving modules
class MockElasticsearchUtility:
    def __init__(self, *args, **kwargs):
        pass


def submit_mozart_job(*args, **kwargs):
    pass


mock_celeryconfig = types.ModuleType('celeryconfig')
sys.modules['celeryconfig'] = mock_celeryconfig
mock_celeryconfig.GRQ_AWS_ES = False
mock_celeryconfig.GRQ_ES_HOST = None
mock_celeryconfig.GRQ_ES_URL = None
mock_celeryconfig.AWS_REGION = None

mock_elasticsearch_utils = types.ModuleType('hysds_commons.elasticsearch_utils')
sys.modules['hysds_commons.elasticsearch_utils'] = mock_elasticsearch_utils
mock_elasticsearch_utils.ElasticsearchUtility = MockElasticsearchUtility

mock_job_utils = types.ModuleType('hysds_commons.job_utils')
sys.modules['hysds_commons.job_utils'] = mock_job_utils
mock_job_utils.submit_mozart_job = submit_mozart_job

mock_commons_es_connection = types.ModuleType('commons.es_connection')
sys.modules['commons.es_connection'] = mock_commons_es_connection
mock_commons_es_connection.get_grq_es = lambda *args, **kwargs: None


@pytest.fixture(autouse=True)
def deny_network_requests(monkeypatch):
    monkeypatch.delattr(requests.sessions.Session, requests.sessions.Session.request.__name__)
