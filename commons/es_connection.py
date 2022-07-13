#!/usr/bin/env python

from elasticsearch import RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

from hysds.celery import app
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from pcm_commons.query.ancillary_utility import AncillaryUtility
from commons.constants import product_metadata

from .logger import logger as default_logger


GRQ_ES = None
MOZART_ES = None


def get_grq_es(logger=None) -> AncillaryUtility:
    global GRQ_ES

    if GRQ_ES is None:
        aws_es = app.conf.get('GRQ_AWS_ES', False)
        es_host = app.conf.get('GRQ_ES_HOST', "localhost:9200")
        es_url = app.conf.get('GRQ_ES_URL', "http://localhost:9200")
        region = app.conf.get('AWS_REGION', "us-west-2")

        if aws_es is True:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            GRQ_ES = AncillaryUtility(
                es_url=es_url,
                logger=logger or default_logger,
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
        else:
            GRQ_ES = AncillaryUtility(es_url, logger=logger, product_metadata=product_metadata)
    return GRQ_ES


def get_mozart_es(logger):
    global MOZART_ES
    if MOZART_ES is None:
        MOZART_ES = ElasticsearchUtility(app.conf.JOBS_ES_URL, logger=logger or default_logger)
    return MOZART_ES
