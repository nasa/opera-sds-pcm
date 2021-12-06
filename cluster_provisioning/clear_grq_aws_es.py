#!/usr/bin/env python

import backoff
import argparse

from hysds.celery import app
from hysds.es_util import get_grq_es


grq_es = get_grq_es()

BACKOFF_CONF = {}  # back-off configuration


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def delete_dataset_indices():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete
    grq_es.es.indices.delete(index='grq_*', ignore=[404])
    print("deleted all dataset indices: grq_*")


@backoff.on_exception(backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time)
def delete_catalog_indices():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete
    grq_es.es.indices.delete(index='*_catalog', ignore=[404])
    print("deleted all dataset indices: *_catalog")


def delete_template():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete_index_template
    grq_es.es.indices.delete_template(name='grq', ignore=[404])
    print("deleted _template: grq")


def delete_ingest_pipeline():
    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IngestClient
    pipeline_name = 'dataset_pipeline'
    grq_es.es.ingest.delete_pipeline(id=pipeline_name, ignore=[404])
    print("deleted ingest pipeline: %s" % pipeline_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max_value", type=int, default=64, help="maximum backoff time")
    parser.add_argument("--max_time", type=int, default=1800, help="maximum total time")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    if app.conf.get('GRQ_AWS_ES') is True:
        # only delete if using GRQ is connected to AWS Elasticsearch
        print("GRQ is connected to AWS Elasticsearch, cleaning indices templates and pipelines")
        delete_dataset_indices()
        delete_catalog_indices()
        delete_template()
        delete_ingest_pipeline()
    else:
        print("NOT using AWS Elasticsearch, no need to clear indices and templates")
