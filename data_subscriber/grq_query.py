import json
import re
from collections import namedtuple
from datetime import datetime

from opensearchpy import OpenSearch
from opensearchpy.helpers import scan

from data_subscriber.cmr import Collection, ProductType, COLLECTION_TO_PRODUCT_TYPE_MAP
from opera_commons.es_connection import get_grq_es
from opera_commons.logger import get_logger
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client
from rtc_utils import rtc_granule_regex



logger = get_logger()


DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

GLOBAL_BBOX = "-180,-90,180,90"
SUPPORTED_PRODUCT_TYPES = {
    ProductType.RTC,
    ProductType.CSLC
}


async def async_query_grq(args, index_pattern, settings, timerange: DateTimeRange, now: datetime, verbose=True) -> list:
    if index_pattern is None:
        raise ValueError("index_pattern cannot be None")

    logger.info(f'Querying GRQ index {index_pattern}')

    es_conn: OpenSearch = get_grq_es().es

    query = _build_grq_query(args, timerange)
    logger.info(f'GRQ query: {json.dumps(query)}')

    granules = [_grq_doc_to_granule(doc) for doc in scan(es_conn, query, index=index_pattern)]

    logger.info(f'Query complete. Found {len(granules):,} granule(s)')

    return granules


def _grq_doc_to_granule(doc: dict) -> dict:
    logger.info(json.dumps(doc))
    raise NotImplementedError()


def _datetime_to_es_query_timestamp(dt: datetime) -> int:
    # TODO: Need to handle TZ?
    return int((dt - datetime(1970, 1, 1)).total_seconds() * 1000)


def _build_grq_query(args, timerange: DateTimeRange) -> dict:
    bbox = args.bbox

    if bbox != GLOBAL_BBOX:
        # TODO: Can now implement since we're indexing RTC/CSLC polygons
        raise NotImplementedError('GRQ querying by bbox not yet implemented')

    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] not in SUPPORTED_PRODUCT_TYPES:
        raise ValueError(f'Collection {args.collection} is not supported for GRQ querying')

    # Assert that timerange looks like this: 2016-08-22T23:00:00Z
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.start_date)
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.end_date)

    start_date = datetime.strptime(timerange.start_date, "%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.strptime(timerange.end_date, "%Y-%m-%dT%H:%M:%SZ")

    query = {
        "query": {
            "bool": {}
        }
    }

    must = []
    should = []

    if args.use_temporal:
        must.append({
            "range": {
                "metadata.acquisition_ts": {
                    "gte": _datetime_to_es_query_timestamp(start_date),
                    "lte": _datetime_to_es_query_timestamp(end_date)
                }
            }
        })
    else:
        must.append({
            "range": {
                "creation_timestamp": {
                    "gte": _datetime_to_es_query_timestamp(start_date),
                    "lte": _datetime_to_es_query_timestamp(end_date)
                }
            }
        })

    if args.native_id is not None:
        # TODO: Validate?

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.RTC:
            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            # extract burst ID from the native-ID, and find the 1-2 relevant MGRS burst sets containing that burst ID.
            match_native_id = re.match(rtc_granule_regex, args.native_id)
            burst_id = mbc_client.product_burst_id_to_mapping_burst_id(match_native_id.group("burst_id"))
            native_ids = mbc_client.get_reduced_rtc_native_id_patterns(mgrs[mgrs["bursts"].str.contains(burst_id)])

            if not native_ids:
                raise ValueError(f"The supplied {args.native_id=} is not associated with "
                                 f"any land-based MGRS tile collection.")

            for nid in native_ids:
                should.append({
                    "wildcard": {
                        "id.keyword": nid
                    }
                })

            # The must clause from the implicit time range sets the minimum matching should clauses to 0, override
            #  that to at least one so that we actually filter by wildcards
            query["query"]["bool"]["minimum_should_match"] = 1
        else:
            if '*' in args.native_id or '?' in args.native_id:
                must.append({
                    "wildcard": {
                        "id.keyword": args.native_id
                    }
                })
            else:
                must.append({
                    "term": {
                        "id.keyword": args.native_id
                    }
                })

    if len(must) > 0:
        query["query"]["bool"]["must"] = must
    if len(should) > 0:
        query["query"]["bool"]["should"] = should

    return query
