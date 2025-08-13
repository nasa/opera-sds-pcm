#!/usr/bin/env python
from typing import Union

from hysds.celery import app
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from pcm_commons.query.ancillary_utility import AncillaryUtility
from commons.constants import product_metadata

from .logger import logger as default_logger


GRQ_ES = None
MOZART_ES = None


def get_grq_es(logger=None) -> Union[ElasticsearchUtility, AncillaryUtility]:
    global GRQ_ES

    if GRQ_ES is None:
        aws_es = app.conf.get('GRQ_AWS_ES', False)
        es_url = app.conf.get('GRQ_ES_URL', "http://localhost:9200")

        es_engine = app.conf.get('GRQ_ES_ENGINE', "elasticsearch")

        if aws_es is True or "es.amazonaws.com" in es_url:
            region = app.conf.get('AWS_REGION', "us-west-2")
            if es_engine == "elasticsearch":
                from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
                from elasticsearch import RequestsHttpConnection

                es_host = app.conf.get('GRQ_ES_HOST', "localhost:9200")
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
            elif es_engine == "opensearch":
                import boto3
                from opensearchpy import AWSV4SignerAuth, RequestsHttpConnection

                credentials = boto3.Session().get_credentials()
                aws_auth = AWSV4SignerAuth(credentials, region)
                GRQ_ES = AncillaryUtility(
                    es_url=es_url,
                    engine=es_engine,
                    logger=logger or default_logger,
                    product_metadata=product_metadata,
                    http_auth=aws_auth,
                    retry_on_timeout = True,
                )
        else:
            if es_engine == "elasticsearch":
                GRQ_ES = AncillaryUtility(es_url, logger=logger, product_metadata=product_metadata)
            elif es_engine == "opensearch":
                GRQ_ES = AncillaryUtility(
                    es_url=es_url,
                    engine=es_engine,
                    logger=logger or default_logger,
                    product_metadata=product_metadata,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
    return GRQ_ES


def get_mozart_es(logger):
    global MOZART_ES
    if MOZART_ES is None:
        es_cluster_mode = app.conf['ES_CLUSTER_MODE']
        if es_cluster_mode:
            hosts = [app.conf.JOBS_ES_URL, app.conf.GRQ_ES_URL, app.conf.METRICS_ES_URL]
        else:
            hosts = [app.conf.JOBS_ES_URL]

        es_engine = app.conf.get('GRQ_ES_ENGINE', "elasticsearch")
        if es_engine == "elasticsearch":
            MOZART_ES = ElasticsearchUtility(hosts, logger=logger or default_logger)
        elif es_engine == "opensearch":
            MOZART_ES = AncillaryUtility(
                    es_url=hosts,
                    engine=es_engine,
                    logger=logger or default_logger,
                    product_metadata=product_metadata,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
    return MOZART_ES
