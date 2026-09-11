import argparse
import json
from datetime import datetime
from functools import partial

import backoff
import requests
from opensearchpy.helpers import scan, bulk
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from opera_commons.es_connection import get_grq_es
from opera_commons.logger import get_logger
from granule import get_granule_for_collection, Granule
from granule.collection import Collection, CCID_MAP, GRQ_MAP


CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'


logger = get_logger()


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'collection',
        choices=[c.value for c in Collection],
        help='Collection to backfill'
    )

    def _datetime_arg(s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')

    parser.add_argument(
        '-s', '--start-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z"
    )

    parser.add_argument(
        '-e', '--end-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z"
    )

    parser.add_argument(
        '--use-revision',
        action='store_false',
        dest='use_temporal',
        help='Toggle for using revision date range rather than temporal range in the query.'
    )

    return parser


def _fatal_code(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.RequestException) and err.response is not None:
        return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]
    return False


def _backoff_logger(details):
    logger.warning(
        f"Backing off {details['target']} function for {details['wait']:0.1f} "
        f"seconds after {details['tries']} tries."
    )
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=_fatal_code,
                      on_backoff=_backoff_logger,
                      interval=15)
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
                      max_tries=2)
def _do_cmr_query(url, params, func=None, headers=None):
    if headers is None:
        headers = {}
    logger.info(f'Querying {url} with params {params} and headers {headers}')
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()

    response_items = response_json['items']

    if len(response_items) > 0:
        logger.info(f'Most recent granule retrieved: {response_items[-1]["umm"]["GranuleUR"]}')

    if func is not None:
        response_items = func(response_items)
        if not isinstance(response_items, list):
            raise TypeError(f'Expecting a list, got {type(response_items)}')

    return response_items, response.headers.get('CMR-Search-After', None)


def query_cmr(cmr_url, ccid, start, end, func=None, use_temporal=True):
    granules = []

    params = {
        'collection_concept_id': ccid,
        'page_size': 2000
    }

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        if use_temporal:
            params['temporal[]'] = f'{start_q_str},{end_q_str}'
        else:
            params['revision_date[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params, func=func)
    granules.extend(query_result)

    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, func=func, headers=headers)
        granules.extend(query_result)

    return granules


def _convert_and_dedupe(cmr_items, coll: Collection, dedupe_ids=None) -> list[Granule]:
    if dedupe_ids is None:
        dedupe_ids = []

    deduped_granules = []

    with logging_redirect_tqdm():
        for item in tqdm(cmr_items, desc='Parsing CMR items: ', leave=False):
            granule = get_granule_for_collection(coll, item)

            if granule.id not in dedupe_ids:
                deduped_granules.append(granule)

    return deduped_granules


def main(args):
    logger.info(f'Running backfill for {args.collection}')

    es_conn = get_grq_es(logger).es
    index_pattern = GRQ_MAP[args.collection]

    logger.info(f'Scanning ES {index_pattern} for existing doc IDs')

    scan_start = datetime.now()
    existing_doc_ids = [doc['_id'] for doc in scan(es_conn, index=index_pattern, query={'_source': False}, size=10_000)]

    logger.info(f'ES scan finished in {datetime.now() - scan_start}. Found {len(existing_doc_ids):,} doc IDs')

    query_start = datetime.now()
    ccid = CCID_MAP[args.collection]

    logger.info(f'Beginning CMR scan for {args.collection} [{ccid}]')
    granules = query_cmr(
        CMR_URL, ccid, args.start_date, args.end_date,
        use_temporal=args.use_temporal,
        func=partial(_convert_and_dedupe, coll=args.collection, dedupe_ids=existing_doc_ids)
    )

    logger.info(f'CMR scan finished in {datetime.now() - query_start}. Found {len(granules):,} granules')

    if len(granules) == 0:
        logger.info('Nothing to backfill')
        return

    operations = []

    for granule in tqdm(granules, desc='Creating bulk operations: '):
        doc_id, index, doc = granule.to_grq_doc()

        operations.append({
            '_op_type': 'create',
            '_index': index,
            '_id': doc_id,
            "doc": doc
        })

    del granules

    logger.info('Inserting docs into GRQ')

    with logging_redirect_tqdm():
        inserted_docs, errors = bulk(
            es_conn,
            tqdm(operations, desc='Docs inserted: '),
            raise_on_error=False
        )

    logger.info(f'Completed GRQ bulk insert: {inserted_docs:,} docs successfully inserted, {len(errors):,} errors')

    with open(f'backfill_results_{args.collection}.json', 'w') as outfile:
        json.dump({
            'inserted_docs': inserted_docs,
            'errors': errors
        }, outfile, indent=2)

    es_conn.indices.refresh(index=index_pattern)

    logger.info(f'Wrote ES bulk insert results to backfill_results_{args.collection}.json')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
