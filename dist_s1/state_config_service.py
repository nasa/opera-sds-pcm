import opensearchpy

from opera_commons.es_connection import get_grq_es
from util.grq_client import get_body


def state_configs_by_batch_id(batch_id):
    grq_es = get_grq_es()
    body = get_body(match_all=False)
    del body["sort"]  # default sort not applicable for these specialized docs
    body["query"]["bool"]["must"].append({"term": {"metadata.batch_id": batch_id}})
    return _search(grq_es, body)

def _search(grq_es, body):
    try:
        results = grq_es.search(body=body, index="grq_1.0_dist_s1-state-config")
    except opensearchpy.exceptions.NotFoundError as e:
        # return []  # intentionally commented out and left in for context to reader
        raise e
    return results["hits"]["hits"]
