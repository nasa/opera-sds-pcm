import argparse
import json
from datetime import datetime
from functools import partial, cache

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
        '-b', '--bbox',
        default=None,
        type=float,
        nargs=4,
        metavar=('MIN_LON', 'MIN_LAT', 'MAX_LON', 'MAX_LAT'),
        help='Bounding box. 4 float coordinates: min_lon, min_lat, max_lon, max_lat. '
             '-90 <= lat <= 90; -180 <= lon <= 180.'
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
    response = requests.get(url, params=params, headers=headers, timeout=(10, 120))
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


# TODO: Remove eventually when no longer needed for DIST
def _get_token():
    import netrc
    from data_subscriber.aws_token import supply_token

    edl = 'urs.earthdata.nasa.gov'
    username, _, password = netrc.netrc().authenticators(edl)
    token = supply_token(edl, username, password)

    return token


def cmr_to_grq(cmr_url, ccid, start, end, es_conn, bbox=None, func=None, use_temporal=True):
    params = {
        'collection_concept_id': ccid,
        'page_size': 2000
    }

    if bbox is not None:
        params['bounding_box'] = ','.join(map(str, bbox))

    # TODO: Remove eventually when no longer needed for DIST
    if ccid == CCID_MAP[Collection.DIST_S1]:
        try:
            logger.info(f'Fetching EDL token for DIST access. In the future this code should be removed or disabled '
                        f'(if new collections are similarly privated)')
            token = _get_token()
            params['token'] = token
        except Exception as e:
            raise RuntimeError('Could not retrieve EDL token which is currently needed for DIST-S1') from e

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        if use_temporal:
            params['temporal[]'] = f'{start_q_str},{end_q_str}'
        else:
            params['revision_date[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params, func=func)
    inserted, errors, skipped = _create_and_insert_grq(query_result, es_conn)

    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, func=func, headers=headers)
        page_inserted, page_errors, page_skipped = _create_and_insert_grq(query_result, es_conn)
        inserted += page_inserted
        errors.extend(page_errors)
        skipped.extend(page_skipped)

    return inserted, errors, skipped


@cache
def _is_index_writable(index, es_conn):
    setting = (es_conn.indices.get(index=index).get(index='grq_v1.0_l3_dswx_s1-2026.04')
               .get('grq_v1.0_l3_dswx_s1-2026.04', {}).get('settings', {}).get('index', {})
               .get('blocks', {}).write('write', 'false'))

    return setting.lower() == 'false'


def _create_and_insert_grq(granules, es_conn):
    operations = []
    skipped = []

    for granule in tqdm(granules, desc='Creating bulk operations: '):
        doc_id, index, doc = granule.to_grq_doc()

        if not _is_index_writable(index, es_conn):
            skipped.append({
                'doc_id': doc_id,
                'index': index,
                'doc': doc,
                'reason': 'Index not writable'
            })

        op_doc = {
            '_op_type': 'create',
            '_index': index,
            '_id': doc_id,
        }
        op_doc.update(doc)

        operations.append(op_doc)

    del granules

    logger.info('Inserting docs into GRQ')

    with logging_redirect_tqdm():
        inserted_docs, errors = bulk(
            es_conn,
            tqdm(operations, desc='Docs inserted: '),
            raise_on_error=False
        )

    logger.info(f'Completed GRQ bulk insert: {inserted_docs:,} docs successfully inserted, {len(errors):,} errors')

    return inserted_docs, errors, skipped


def _convert_and_dedupe(cmr_items, coll: Collection, dedupe_ids=None) -> list[Granule]:
    if len(cmr_items) == 0:
        return []

    if dedupe_ids is None:
        dedupe_ids = set()

    deduped_granules = []
    n_deduped_granules = 0

    with logging_redirect_tqdm():
        for item in tqdm(cmr_items, desc='Parsing CMR items: ', leave=False):
            granule = get_granule_for_collection(coll, item)

            if granule.id not in dedupe_ids:
                deduped_granules.append(granule)
            else:
                n_deduped_granules += 1

    logger.info(f'Deduped {n_deduped_granules:,} granules')

    return deduped_granules


def main(args):
    logger.info(f'Running backfill for {args.collection}')

    es_conn = get_grq_es(logger).es
    index_pattern = GRQ_MAP[args.collection]

    logger.info(f'Scanning ES {index_pattern} for existing doc IDs')

    scan_start = datetime.now()
    existing_doc_ids = {doc['_id'] for doc in scan(es_conn, index=index_pattern, query={'_source': False}, size=10_000)}

    logger.info(f'ES scan finished in {datetime.now() - scan_start}. Found {len(existing_doc_ids):,} doc IDs')

    query_start = datetime.now()
    ccid = CCID_MAP[args.collection]

    logger.info(f'Beginning CMR -> GRQ copy for {args.collection} [{ccid}]')
    inserted, errors, skipped = cmr_to_grq(
        CMR_URL, ccid, args.start_date, args.end_date, es_conn,
        bbox=args.bbox,
        use_temporal=args.use_temporal,
        func=partial(_convert_and_dedupe, coll=args.collection, dedupe_ids=existing_doc_ids)
    )

    logger.info(f'CMR -> GRQ copy finished in {datetime.now() - query_start}: '
                f'{inserted:,} docs successfully inserted, {len(errors):,} errors, {len(skipped):,} skipped')

    with open(f'backfill_results_{args.collection}.json', 'w') as outfile:
        json.dump({
            'inserted_docs': inserted,
            'n_errors': len(errors),
            'n_skipped': len(skipped),
            'errors': errors,
            'skipped': skipped
        }, outfile, indent=2)

    es_conn.indices.refresh(index=index_pattern)

    logger.info(f'Wrote CMR -> GRQ results to backfill_results_{args.collection}.json')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
