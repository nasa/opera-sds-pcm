import logging
from datetime import datetime

import backoff
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from data_subscriber import es_conn_util

logger = logging.getLogger(__name__)


def get_slc_datasets_without_ionosphere_data(creation_timestamp_start_dt: datetime, creation_timestamp_end_dt: datetime):
    es: Elasticsearch = es_conn_util.get_es_connection(logger).es

    body = get_body()
    # TODO chrisjrd: limit data returned by query?
    # body["_source"]["includes"] = "false"
    # body["_source"]["includes"] = ["metadata.intersects_north_america", "browse_urls"]

    body["query"]["bool"]["must"].append(get_range("creation_timestamp", creation_timestamp_start_dt.isoformat(), creation_timestamp_end_dt.isoformat()))
    body["query"]["bool"]["must"].append({"term": {"metadata.intersects_north_america": "true"}})

    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.job_id"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.s3_url"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.source_url"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileName"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileSize"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileLocation"}})

    search_results = list(helpers.scan(es, body, index="grq_*_l1_s1_slc*", scroll="5m", size=10_000))

    return search_results


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def try_update_slc_dataset_with_ionosphere_metadata(index, product_id, ionosphere_metadata):
    es: Elasticsearch = es_conn_util.get_es_connection(logger).es
    es.update(index, product_id, body={"doc": {**ionosphere_metadata}})


def get_body() -> dict:
    return {
        "query": {
            "bool": {
                "must": [{"match_all": {}}],
                "must_not": [],
                "should": []
            }
        },
        "from": 0,
        "size": 10_000,
        "sort": [],
        "aggs": {},
        "_source": {"includes": [], "excludes": []}
    }


def get_range(
        datetime_fieldname="creation_timestamp",
        start_dt_iso="1970-01-01",
        end_dt_iso="9999-01-01"
) -> dict:
    return {
        "range": {
            datetime_fieldname: {
                "gte": start_dt_iso,
                "lt": end_dt_iso
            }
        }
    }