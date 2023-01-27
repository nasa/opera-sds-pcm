from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch import RequestsHttpConnection
from hysds.celery import app
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

CONN = None


def get_es_connection(logger):
    global CONN

    if CONN is None:
        aws_es = app.conf['GRQ_AWS_ES']
        es_host = app.conf['GRQ_ES_HOST']
        es_url = app.conf['GRQ_ES_URL']
        region = app.conf['AWS_REGION']

        if aws_es is True:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            CONN = ElasticsearchUtility(
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
            CONN = ElasticsearchUtility(es_url, logger)
    return CONN
