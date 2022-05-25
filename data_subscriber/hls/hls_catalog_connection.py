from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch import RequestsHttpConnection
from hysds.celery import app

from data_subscriber.hls.hls_catalog import HLSProductCatalog

DATA_SUBSCRIBER_CONN = None


def get_hls_catalog_connection(logger):
    global DATA_SUBSCRIBER_CONN

    if DATA_SUBSCRIBER_CONN is None:
        aws_es = app.conf['GRQ_AWS_ES']
        es_host = app.conf['GRQ_ES_HOST']
        es_url = app.conf['GRQ_ES_URL']
        region = app.conf['AWS_REGION']

        if aws_es is True:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            DATA_SUBSCRIBER_CONN = HLSProductCatalog(
                es_url=es_url,
                logger=logger,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True,
            )
        else:
            DATA_SUBSCRIBER_CONN = HLSProductCatalog(es_url, logger)
    return DATA_SUBSCRIBER_CONN
