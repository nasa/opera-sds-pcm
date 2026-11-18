from copy import deepcopy
from datetime import datetime

CNM_NOT_SENT = {
    "query": {
        "bool": {
            "must_not": [
                {
                    "exists": {
                        "field": "daac_CNM_S_status"
                    }
                }
            ]
        }
    }
}

CNM_SEND_FAILED = {
    "query": {
        "bool": {
            "must": [
                {
                    "exists": {
                        "field": "daac_CNM_S_status"
                    }
                }
            ],
            "must_not": [
                {
                    "term": {
                        "daac_CNM_S_status.keyword": "SUCCESS"
                    }
                }
            ]
        }
    }
}

CNM_NO_RESPONSE = {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "daac_CNM_S_status.keyword": "SUCCESS"
                    }
                }
            ],
            "must_not": [
                {
                    "exists": {
                        "field": "daac_delivery_status"
                    }
                }
            ]
        }
    }
}

CNM_INGEST_FAILED = {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "daac_CNM_S_status.keyword": "SUCCESS"
                    }
                },
                {
                    "exists": {
                        "field": "daac_delivery_status"
                    }
                }
            ],
            "must_not": [
                {
                    "term": {
                        "daac_delivery_status.keyword": "SUCCESS"
                    }
                }
            ]
        }
    }
}

BLANK_QUERY = {
    "query": {
        "bool": {
            "must": []
        }
    }
}


def add_time_range(q, start: datetime, end: datetime):
    range_block = {
        "range": {
            "@timestamp": {
                "gte": int(start.timestamp() * 1000),
                "lte": int(end.timestamp() * 1000),
            }
        }
    }

    q = deepcopy(q)

    if 'must' not in q['query']['bool']:
        q['query']['bool']['must'] = [range_block]
    else:
        q['query']['bool']['must'].append(range_block)

    return q
