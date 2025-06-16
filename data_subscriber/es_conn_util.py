from hysds.celery import app
from pcm_commons.query.ancillary_utility import AncillaryUtility

from commons.constants import product_metadata
from commons.logger import logger as default_logger

CONN = None


def get_es_connection(logger):
    global CONN

    if CONN is None:
        aws_es = app.conf.get('GRQ_AWS_ES', False)
        logger.info(f"{aws_es=}")
        es_url = app.conf.get('GRQ_ES_URL', "http://localhost:9200")
        logger.info(f"{es_url=}")

        es_engine = app.conf.get('GRQ_ES_ENGINE', "elasticsearch")
        logger.info(f"{es_engine=}")

        if aws_es is True or "es.amazonaws.com" in es_url:
            region = app.conf.get('AWS_REGION', "us-west-2")
            if es_engine == "elasticsearch":
                from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
                from elasticsearch import RequestsHttpConnection

                es_host = app.conf.get('GRQ_ES_HOST', "localhost:9200")
                aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
                CONN = AncillaryUtility(
                    es_url=es_url,
                    logger=logger,
                    product_metadata=product_metadata,
                    http_auth=aws_auth,
                    connection_class=RequestsHttpConnection,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
            elif es_engine == "opensearch":
                import boto3
                from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

                credentials = boto3.Session().get_credentials()
                aws_auth = AWSV4SignerAuth(credentials, region)
                CONN = AncillaryUtility(
                    es_url=es_url,
                    engine=es_engine,
                    logger=logger or default_logger,
                    product_metadata=product_metadata,
                    http_auth=aws_auth,
                    retry_on_timeout = True,
                )
        else:
            if es_engine == "elasticsearch":
                CONN = AncillaryUtility(es_url, logger=logger, product_metadata=product_metadata)
                CONN.es.ping()  # TODO chrisjrd: remove before final
            elif es_engine == "opensearch":
                CONN = AncillaryUtility(
                    es_url,
                    engine=es_engine,
                    logger=logger or default_logger,
                    product_metadata=product_metadata,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
                CONN.es.ping()  # TODO chrisjrd: remove before final
    return CONN
