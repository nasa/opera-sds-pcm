"""Helper functions for interacting with GRQ."""
import logging
from datetime import datetime
from typing import Literal

import backoff
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch

from data_subscriber import es_conn_util

logger = logging.getLogger(__name__)


def get_slc_datasets_without_ionosphere_data(creation_timestamp_start_dt: datetime, creation_timestamp_end_dt: datetime, es_engine: Literal["elasticsearch", "opensearch"]):

    body = get_body()
    body["sort"] = []
    body["query"]["bool"]["must"].append(get_range("creation_timestamp", creation_timestamp_start_dt.isoformat(), creation_timestamp_end_dt.isoformat()))
    body["query"]["bool"]["must"].append({"term": {"metadata.intersects_north_america": "true"}})
    body["query"]["bool"]["must"].append({"term": {"metadata.processing_mode": "forward"}})

    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.job_id"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.s3_url"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.source_url"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileName"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileSize"}})
    body["query"]["bool"]["must_not"].append({"exists": {"field": "metadata.ionosphere.FileLocation"}})

    if es_engine == "elasticsearch":
        es: Elasticsearch = es_conn_util.get_es_connection(logger).es
        from elasticsearch import helpers
        search_results = list(helpers.scan(es, body, index="grq_*_l1_s1_slc*", scroll="5m", size=10_000))
    elif es_engine == "opensearch":
        es: OpenSearch = es_conn_util.get_es_connection(logger).es
        from opensearchpy import helpers
        search_results = list(helpers.scan(es, body, index="grq_*_l1_s1_slc*", scroll="5m", size=10_000))
    else:
        raise AssertionError(f"Invalid {es_engine=}")

    return search_results


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def try_update_slc_dataset_with_ionosphere_metadata(index, product_id, ionosphere_metadata):
    es: Elasticsearch = es_conn_util.get_es_connection(logger).es
    es.update(index, product_id, body={"doc": {"metadata": ionosphere_metadata}})


def get_body(match_all=True) -> dict:
    """
    Returns a generic Elasticsearch query body for use with a raw elasticsearch-py client.
    By default, it includes a match_all query and will sort results by "creation_timestamp".
    $.size is set to 10_000.

    Clients should override $.query.bool.must[] and $.sort[] as needed.
    Clients may set $._source_includes = "false" to omit the document in the Elasticsearch response.
    """
    return {
        "query": {
            "bool": {
                "must": [{"match_all": {}}] if match_all else [],
                "must_not": [],
                "should": []
            }
        },
        "from": 0,
        "size": 10_000,
        "sort": [{
            "creation_timestamp": {"order": "asc"}
        }],
        "aggs": {},
        "_source": {"includes": [], "excludes": []}
    }


def get_range(
        datetime_fieldname="creation_timestamp",
        start_dt_iso="1970-01-01",
        end_dt_iso="9999-12-31T23:59:59.999"
) -> dict:
    """
    Returns a query range filter typically set in an Elasticsearch body's $.query.bool.must[] section.
    The default range is from 1970 to the year 10,000.
    The "from" datetime uses "gte" and the "to" datetime uses "lt".
    """
    return {
        "range": {
            datetime_fieldname: {
                "gte": start_dt_iso,
                "lt": end_dt_iso
            }
        }
    }
