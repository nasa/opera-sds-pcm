import argparse
import json
import logging
import os.path
import re
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime
from itertools import combinations

import backoff
import boto3
import rasterio
import requests
from rasterio.session import AWSSession
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)

logging.getLogger('botocore').setLevel(logging.WARNING)


DEBUG_BREAK_SURVEY_EARLY = False  # TODO: Remove this when dev wraps up
TRY_S3 = True
DEFAULT_S3_RETRY_INTERVAL = 250

PRIOR_PRODUCT_META_KEY = 'prior_product_name'
PRIOR_PRODUCT_META_KEY_ALT = 'prior_dist_s1_product'
PRIOR_PRODUCT_ADDITIONAL_ATTR_NAME = '__PLACEHOLDER__'  # TODO: Update when/if available

DEFAULT_GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'
DIST_S1_START_DATE = datetime(2026, 1, 1)

CCIDS = {
    'PROD': 'C4090131664-ASF',
    # 'UAT': 'C1275699124-ASF',  # OPERA_L3_DIST-ALERT-S1_PROVISIONAL_V0
    'UAT': 'C1275699127-ASF',  # OPERA_L3_DIST-ALERT-S1_V1
}

CMR_URLS = {
    'PROD': 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4',
    'UAT': 'https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json'
}

EDL_URLS = {
    'PROD': 'urs.earthdata.nasa.gov',
    'UAT': 'uat.urs.earthdata.nasa.gov'
}

TILE_ID_FIELD = 3
ACQ_TIME_FIELD = 4
PROD_TIME_FIELD = 5
SENSOR_FIELD = 6

SURVEY_DROPPED_PRODUCTS = []
SURVEY_LATEST_DEDUPED_PRODUCTS = {}
DUPLICATES = {}

DEFAULT_PAGE_SIZE = 2000


def _dist_id_to_unique_tuple(gid):
    if gid is None:
        return None

    id_fields = gid.split('_')

    return (
        id_fields[TILE_ID_FIELD],
        id_fields[ACQ_TIME_FIELD],
        id_fields[SENSOR_FIELD],
    )


def _get_token(venue):
    import netrc
    from data_subscriber.aws_token import supply_token

    edl = EDL_URLS[venue]
    username, _, password = netrc.netrc().authenticators(edl)
    token = supply_token(edl, username, password)

    return token


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


def _additional_attributes_umm_to_dict(aa_umm):
    aa_dict = {}

    for attr in aa_umm:
        name = attr['Name']
        values = attr['Values']

        if len(values) == 1:
            values = values[0]

        aa_dict[name] = values

    return aa_dict


def _cmr_items_to_dicts(items):
    item_dicts = []

    for item in items:
        item_id = item['umm']['GranuleUR']
        if 'AdditionalAttributes' in item['umm']:
            additional_attributes = _additional_attributes_umm_to_dict(item['umm']['AdditionalAttributes'])
        else:
            additional_attributes = {}

        urls = {
            'https': None,
            's3': None
        }

        for url_dict in item['umm']['RelatedUrls']:
            url_type = url_dict['Type']

            if not url_type.startswith('GET DATA'):
                continue

            url = url_dict['URL']

            if url.endswith('_GEN-DIST-STATUS.tif'):
                if url.startswith('https://'):
                    urls['https'] = url
                elif url.startswith('s3://'):
                    urls['s3'] = url

        if urls['https'] is None:
            logger.warning(f'Granule {item_id} has no https URL so will be ignored. This may cause a confirmation '
                           f'chaining error down the line')
            SURVEY_DROPPED_PRODUCTS.append(item_id)
            continue

        id_fields = item_id.split('_')

        item_dicts.append({
            'id': item_id,
            'tile': id_fields[TILE_ID_FIELD],
            'acquisition_time': datetime.strptime(id_fields[ACQ_TIME_FIELD], DEFAULT_GRANULE_TIME_FMT),
            'production_time': datetime.strptime(id_fields[PROD_TIME_FIELD], DEFAULT_GRANULE_TIME_FMT),
            'unique_tuple': _dist_id_to_unique_tuple(item_id),
            'urls': urls,
            'additional_attributes': additional_attributes,
            'pge_version': item['umm'].get('PGEVersionClass', {}).get('PGEVersion')
        })

    return item_dicts


@backoff.on_exception(backoff.constant, requests.exceptions.RequestException,
                      max_time=300, giveup=_fatal_code, on_backoff=_backoff_logger, interval=15)
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


def query_cmr(cmr_url, ccid, start, end, func=None, token=None, **extra_params):
    granules = []

    params = {
        'collection_concept_id': ccid,
        'page_size': DEFAULT_PAGE_SIZE
    }

    if token is not None:
        params['token'] = token

    params.update(extra_params)

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        params['temporal[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params, func=func)
    granules.extend(query_result)

    while search_after is not None:
        if DEBUG_BREAK_SURVEY_EARLY:
            break

        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, func=func, headers=headers)
        granules.extend(query_result)

    return granules


def _try_get_tiff_metadata(https_url, s3_url):
    with ExitStack() as stack:
        if s3_url is not None and TRY_S3:
            aws_session = AWSSession(boto3.Session())
            stack.enter_context(rasterio.Env(aws_session))
            url = s3_url
            logger.info(f'Attempting to open COG via S3 at url {url}')
        else:
            stack.enter_context(rasterio.Env(
                CPL_VSIL_CURL_ALLOWED_EXTENSIONS='TIF',
                GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
                # CPL_DEBUG='ON',
                # CPL_CURL_VERBOSE='ON',
                GDAL_HTTP_COOKIEFILE='/tmp/cookies.txt',
                GDAL_HTTP_COOKIEJAR='/tmp/cookies.txt',

            ))
            url = https_url

            # TODO: Temp
            if 'earthdatacloud.nasa.gov' in url:
                url = url.replace('earthdatacloud.nasa.gov', 'alaska.edu')
                logger.debug(f'Updated URL {https_url} -> {url}')

            logger.info(f'Attempting to open COG via HTTPS at url {url}')

        dataset = stack.enter_context(rasterio.open(url))

        return dataset.tags()


def get_cog_metadata(https_url, s3_url):
    global TRY_S3

    try:
        return _try_get_tiff_metadata(https_url, s3_url)
    except Exception as e:
        logger.error(f'Failed to open COG: {e}. This may be retried if S3 was attempted')

        if TRY_S3 and s3_url is not None:
            TRY_S3 = False
            return _try_get_tiff_metadata(https_url, s3_url)
        else:
            raise


def _group_by_tile(dist_product_dicts):
    grouped_dicts = {}

    for product in dist_product_dicts:
        tile_id = product['tile']
        grouped_dicts.setdefault(tile_id, []).append(product)

    logger.info(f'Grouped DIST-S1 products to {len(grouped_dicts):,} confirmation chains')

    for tile in grouped_dicts:
        grouped_dicts[tile].sort(key=lambda x: (x['acquisition_time'], x['production_time']))

    return grouped_dicts


def _find_production_order_errors(confirmation_chain):
    confirmation_chain = deepcopy(confirmation_chain)

    misordered_products = []

    prev_production_time = confirmation_chain.pop(0)['production_time']

    for product in confirmation_chain:
        cur_production_time = product['production_time']

        if cur_production_time < prev_production_time:
            misordered_products.append({
                'misordered_product_id': product['id'],
                'production_time': cur_production_time.strftime(DEFAULT_GRANULE_TIME_FMT),
                'prior_product_production_time': prev_production_time.strftime(DEFAULT_GRANULE_TIME_FMT)
            })

    return misordered_products


def _find_chain_errors(confirmation_chain, start_datetime, warn_on_first_null=False):
    confirmation_chain = deepcopy(confirmation_chain)

    discontinuities = []
    incorrect_products = []

    nominal_confirmation_chain = [_dist_id_to_unique_tuple(p['id']) for p in confirmation_chain]
    nominal_confirmation_chain.sort(key=lambda x: x[1])

    logger.debug(f'Nominal confirmation chain for tile {confirmation_chain[0]["tile"]}: {nominal_confirmation_chain}')

    first_product = confirmation_chain.pop(0)

    warn = (warn_on_first_null and (start_datetime is not None and start_datetime > DIST_S1_START_DATE)
            and first_product['previous_product_id'] is None)

    prev_product = first_product

    def _get_latest_or_none(uniq_t):
        if uniq_t is None:
            return None
        return SURVEY_LATEST_DEDUPED_PRODUCTS[uniq_t]

    for product in confirmation_chain:
        product_tuple = _dist_id_to_unique_tuple(product['id'])
        prev_product_tuple = _dist_id_to_unique_tuple(product['previous_product_id'])

        confirmation_chain_index = nominal_confirmation_chain.index(product_tuple)

        if confirmation_chain_index == 0:
            expected_prev_product_tuple = None
        else:
            expected_prev_product_tuple = nominal_confirmation_chain[confirmation_chain_index - 1]

        if product['previous_product_id'] is None:
            discontinuities.append({
                'discontinuous_product_id': product['id'],
                # 'expected_prev_product_id': SURVEY_LATEST_DEDUPED_PRODUCTS[expected_prev_product['unique_tuple']],
                'expected_prev_product_id': _get_latest_or_none(expected_prev_product_tuple),
            })

            if (product['unique_tuple'] == prev_product['unique_tuple'] and
                    product['id'] != prev_product['id']):
                DUPLICATES.setdefault(product['unique_tuple'], set())
                DUPLICATES[product['unique_tuple']].add(product['id'])
                DUPLICATES[product['unique_tuple']].add(prev_product['id'])
                discontinuities[-1]['duplicate_flag'] = True
        # elif _dist_id_to_unique_tuple(product['previous_product_id']) != expected_prev_product['unique_tuple']:
        elif prev_product_tuple != expected_prev_product_tuple:
            incorrect_products.append({
                'misordered_product_id': product['id'],
                # 'expected_prev_product_id': SURVEY_LATEST_DEDUPED_PRODUCTS[expected_prev_product['unique_tuple']],
                'expected_prev_product_id': _get_latest_or_none(expected_prev_product_tuple),
                'incorrect_previous_product_id': product['previous_product_id']
            })

            for a, b in combinations((product['id'], prev_product['id'], product['previous_product_id']), r=2):
                if a != b and _dist_id_to_unique_tuple(a) == _dist_id_to_unique_tuple(b):
                    t = _dist_id_to_unique_tuple(a)
                    DUPLICATES.setdefault(t, set())
                    DUPLICATES[t].add(a)
                    DUPLICATES[t].add(b)
                    incorrect_products[-1]['duplicate_flag'] = True

        prev_product = product

    return discontinuities, incorrect_products, warn


def _add_previous_product_id(dist_product_dict, pbar=None):
    if PRIOR_PRODUCT_ADDITIONAL_ATTR_NAME in dist_product_dict['additional_attributes']:
        previous_product_id = dist_product_dict['additional_attributes'][PRIOR_PRODUCT_ADDITIONAL_ATTR_NAME]
        # TODO: May have to convert string null to python null
    else:
        product_metadata = get_cog_metadata(
            https_url=dist_product_dict['urls']['https'],
            s3_url=dist_product_dict['urls']['s3']
        )

        if PRIOR_PRODUCT_META_KEY in product_metadata:
            previous_product_id = product_metadata[PRIOR_PRODUCT_META_KEY]
        elif PRIOR_PRODUCT_META_KEY_ALT in product_metadata:
            alt_prior_product_value = product_metadata[PRIOR_PRODUCT_META_KEY_ALT]

            if alt_prior_product_value == 'None':
                previous_product_id = None
            else:
                previous_product_id = os.path.basename(alt_prior_product_value)
        else:
            raise KeyError(f'Neither {PRIOR_PRODUCT_META_KEY} nor {PRIOR_PRODUCT_META_KEY_ALT} exists in '
                           f'metadata for {dist_product_dict["id"]}')

    dist_product_dict['previous_product_id'] = previous_product_id

    del dist_product_dict['additional_attributes']
    del dist_product_dict['urls']

    if pbar:
        pbar.update()


def parse_args():
    parser = argparse.ArgumentParser()

    product_selection_group = parser.add_argument_group(
        'Product selection options',
        'Options to narrow down products checked by filtering DIST results from CMR'
    )

    def _datetime_arg(s):
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')

    product_selection_group.add_argument(
        '-s', '--start-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z"
    )

    product_selection_group.add_argument(
        '-e', '--end-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z"
    )

    def _tile(v):
        v = v.upper()
        if not re.fullmatch(r'T?\d{2}[A-Z]{3}', v):
            raise ValueError(f'Invalid tile: {v}')
        return v.removeprefix('T')

    product_selection_group.add_argument(
        '-t', '--tiles',
        default=None,
        type=_tile,
        nargs='+',
        help='One or more MGRS tiles to restrict survey to'
    )

    product_selection_group.add_argument(
        '--production-start-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time for filtering by production time. "
             "For Example, --production-start-date 2021-01-14T00:00:00Z"
    )

    product_selection_group.add_argument(
        '--production-end-date',
        default=None,
        type=_datetime_arg,
        help="The ISO date time for filtering by production time. "
             "For Example, --production-end-date 2021-01-14T00:00:00Z"
    )

    product_selection_group.add_argument(
        '--pge-versions',
        nargs='+',
        default=None,
        help='PGE version number(s) to filter products to'
    )

    parser.add_argument(
        '-o', '--output',
        default=f'dist_s1_confirmation_report_{datetime.now().strftime("%Y%m%dT%H%M%S")}.json',
        help='Output filename. Defaults to dist_s1_confirmation_report_{timestamp}.json'
    )

    parser.add_argument(
        '--venue',
        choices=list(CMR_URLS.keys()),
        default='PROD',
        help='Venue to check: PROD or UAT. Default: PROD'
    )

    parser.add_argument(
        '--ignore-first-null',
        action='store_false',
        dest='warn_on_first_null',
        help='For a given confirmation chain, if the first product did not use a previous product as an input and the '
             'survey\'s start time is after the DIST-S1 start time, by default, the chain\'s tile will be flagged with '
             'a warning, as we\'ll have no way to determine if there should be a product before it in the chain. If '
             'this option is set, these warnings are inhibited.'
    )

    parser.add_argument(
        '--get-token',
        action='store_true',
        help='Retrieve an EDL token for the CMR queries. This should only be needed temporarily while the prod '
             'DIST-S1 collection is non-public'
    )

    return parser.parse_args()


def _apply_extra_survey_filters(survey, **filters):
    prod_start = filters.get('production_start_date')
    prod_end = filters.get('production_end_date')
    pge_versions = filters.get('pge_versions')

    filtered_survey = []

    for product in survey:
        if prod_start is not None and product['production_time'] < prod_start:
            continue
        if prod_end is not None and product['production_time'] > prod_end:
            continue
        if pge_versions is not None and product['pge_version'] not in pge_versions:
            continue

        filtered_survey.append(product)

    logger.info(f'Applied extra filters to surveyed products: {len(survey):,} -> {len(filtered_survey):,}')

    return filtered_survey


def main(venue, start, end, tiles, warn_on_first_null_after_start=True, get_token=False, **other_filtering_params):
    extra_survey_params = {}

    if tiles is not None:
        tile_params = [f'string,MGRS_TILE_ID,{tile}' for tile in tiles]
        extra_survey_params['attribute[]'] = tile_params
        if len(tile_params) > 1:
            extra_survey_params['options[attribute][or]'] = 'true'

    token = _get_token(venue) if get_token else None

    survey_results = query_cmr(
        cmr_url=CMR_URLS[venue],
        ccid=CCIDS[venue],
        start=start,
        end=end,
        func=_cmr_items_to_dicts,
        token=token,
        **extra_survey_params
    )

    logger.info(f'CMR survey returned {len(survey_results):,} DIST-S1 products')

    if other_filtering_params:
        survey_results = _apply_extra_survey_filters(survey_results, **other_filtering_params)

    for result in survey_results:
        unique_tuple = result['unique_tuple']
        SURVEY_LATEST_DEDUPED_PRODUCTS.setdefault(unique_tuple, []).append(result)

    for unique_tuple in SURVEY_LATEST_DEDUPED_PRODUCTS:
        SURVEY_LATEST_DEDUPED_PRODUCTS[unique_tuple].sort(key=lambda x: x['production_time'], reverse=True)
        SURVEY_LATEST_DEDUPED_PRODUCTS[unique_tuple] = SURVEY_LATEST_DEDUPED_PRODUCTS[unique_tuple][0]['id']

    grouped_products = _group_by_tile(survey_results)

    bad_tiles = set()
    warn_tiles = []

    production_products_misordered = []
    chaining_discontinuities = []
    chaining_bad_orders = []
    failed_metadata_retrieval = []
    skipped_chains = set()

    with logging_redirect_tqdm():
        with tqdm(total=len(survey_results),   desc='DIST product metadata ', leave=False) as pbar:
            for i, tile in enumerate(tqdm(grouped_products, desc='  Confirmation chains ', leave=False)):
                if i > 0 and i % DEFAULT_S3_RETRY_INTERVAL == 0:
                    global TRY_S3
                    TRY_S3 = True
                    logger.info('Reset S3 attempts')

                logger.info(f'Checking confirmation chain for tile {tile} for production misorderings')
                tile_prod_discontinuities = _find_production_order_errors(grouped_products[tile])
                if tile_prod_discontinuities:
                    logger.error(f'Found {len(tile_prod_discontinuities):,} production misorderings for tile {tile}')
                    bad_tiles.add(tile)
                    production_products_misordered.extend(tile_prod_discontinuities)
                else:
                    logger.info(f'Confirmation chain for tile {tile} was produced in order')

                logger.info(f'Gathering prev_product metadata for confirmation chain for tile {tile}')
                for product in grouped_products[tile]:
                    try:
                        _add_previous_product_id(product, pbar)
                    except Exception as e:
                        logger.critical(f'Failed to get metadata for product {product}: {e}')
                        failed_metadata_retrieval.append((product['id'], e))
                        skipped_chains.add(tile)
                        pbar.update()

        with open('debug.json', 'w') as f:
            json.dump(grouped_products, f, indent=2, default=repr)

        for tile in tqdm(grouped_products, desc='Confirmation chains ', leave=False):
            if tile in skipped_chains:
                logger.error(
                    f'Skipping confirmation chain checks for tile {tile} as we could not gather all product metadata')
                continue

            logger.info(f'Checking confirmation chain for tile {tile} for chaining errors and discontinuities')
            tile_chain_discontinuities, tile_chain_errors, warn = _find_chain_errors(
                grouped_products[tile],
                start,
                warn_on_first_null_after_start
            )

            if len(tile_chain_discontinuities) > 0:
                logger.error(f'Found {len(tile_chain_discontinuities):,} chaining discontinuities for tile {tile}')
                bad_tiles.add(tile)
                chaining_discontinuities.extend(tile_chain_discontinuities)
            if len(tile_chain_errors) > 0:
                logger.error(f'Found {len(tile_chain_errors):,} chaining errors for tile {tile}')
                bad_tiles.add(tile)
                chaining_bad_orders.extend(tile_chain_errors)

            if len(tile_chain_discontinuities) == 0 and len(tile_chain_errors) == 0:
                logger.info(f'No chaining errors or discontinuities found for tile {tile}')

            if warn:
                logger.warning(f'The first product in the confirmation chain for tile {tile} had no previous product '
                               f'and the survey started after the DIST-S1 record start date. This may indicate a '
                               f'chaining discontinuity on this tile')
                warn_tiles.append(tile)

    bad_tiles = list(bad_tiles)

    report = {}

    def _count_list(x):
        return {
            'count': len(x),
            'products': x,
        }

    if len(bad_tiles) == 0:
        if len(warn_tiles) == 0:
            logger.info('All confirmation chains surveyed have no confirmation or production errors and no warnings')
        else:
            logger.info(f'All confirmation chains surveyed have no confirmation or production errors, but there are '
                        f'warnings of potential discontinuities for {len(warn_tiles):,} tiles')

            report = {
                'tiles_with_warnings': warn_tiles,
            }
    else:
        logger.error(f'There are {len(bad_tiles):,} tiles with bad confirmation chains. Production order error '
                     f'count: {len(production_products_misordered):,}, chaining discontinuity count: '
                     f'{len(chaining_discontinuities):,}, chaining order error count: '
                     f'{len(chaining_bad_orders):,}')

        bad_tiles.sort()

        report = {
            'bad_tiles': {
                'count': len(bad_tiles),
                'percentage': (len(bad_tiles) / len(grouped_products)) * 100,
                'tiles': bad_tiles,
            }
        }

        if production_products_misordered:
            report['production_products_misordered'] = _count_list(production_products_misordered)
        if chaining_discontinuities:
            report['chaining_discontinuities'] = _count_list(chaining_discontinuities)
        if chaining_bad_orders:
            report['chaining_bad_orders'] = _count_list(chaining_bad_orders)

        if warn_tiles:
            logger.info(f'There are also warnings of potential discontinuities for {len(warn_tiles):,} tiles')
            warn_tiles.sort()
            report['tiles_with_warnings'] = warn_tiles

    if failed_metadata_retrieval:
        logger.error(f'Tool was unable to get metadata for {len(failed_metadata_retrieval):,} products, so their '
                     f'chains could not be checked')
        report['failed_metadata_retrieval'] = {p: str(e) for p, e in failed_metadata_retrieval}
        report['skipped_chains'] = len(sorted(skipped_chains))

    if SURVEY_DROPPED_PRODUCTS:
        logger.warning(f'{len(SURVEY_DROPPED_PRODUCTS):,} products had to be dropped from the CMR survey due to not '
                       f'having an HTTPS URL, this may have caused some false positives. Please review these closely.')
        SURVEY_DROPPED_PRODUCTS.sort()
        report['dropped_products'] = SURVEY_DROPPED_PRODUCTS

    if DUPLICATES:
        logger.warning(f'Identified {len(DUPLICATES):,} sets of duplicate products')
        duplicate_ids = []

        for unique_tuple in DUPLICATES:
            duplicate_set = list(DUPLICATES[unique_tuple])
            duplicate_set.sort(key=lambda x: x.split('_')[PROD_TIME_FIELD], reverse=True)
            duplicate_ids.extend(duplicate_set[1:])

        duplicate_ids.sort()
        logger.warning(f'Found {len(duplicate_ids):,} duplicate products for removal')

        report['duplicate_products'] = duplicate_ids

    return report, list(set(grouped_products.keys()) - set(bad_tiles))


if __name__ == '__main__':
    args = parse_args()

    extra_filtering_args = {}

    if args.production_start_date is not None:
        extra_filtering_args['production_start_date'] = args.production_start_date
    if args.production_end_date is not None:
        extra_filtering_args['production_end_date'] = args.production_end_date
    if args.pge_versions is not None:
        extra_filtering_args['pge_versions'] = args.pge_versions

    dist_report, good_tiles = main(
        args.venue, args.start_date, args.end_date, args.tiles,
        warn_on_first_null_after_start=args.warn_on_first_null,
        get_token=args.get_token,
        **extra_filtering_args
    )

    good_tiles.sort()

    output_filename = args.output
    if not output_filename.endswith('.json'):
        output_filename += '.json'

    good_tiles_filename = output_filename.replace('.json', '.good.json')

    if dist_report:
        with open(output_filename, 'w') as f:
            json.dump(dist_report, f, indent=2)
        logger.info(f'Report written to {output_filename}')
    else:
        logger.info('No issues to report')

    with open(good_tiles_filename, 'w') as f:
        json.dump(good_tiles, f, indent=2)
    logger.info(f'Good tile list written to {good_tiles_filename}')
