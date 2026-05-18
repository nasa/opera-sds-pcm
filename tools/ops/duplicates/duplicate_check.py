import argparse
import re
import json
import requests
from datetime import datetime
import backoff
import logging
from itertools import chain

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)

CMR_URLS = {
    'PROD': 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4',
    'UAT': 'https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json'
}

DEFAULT_GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'


# TODO: Fill out for other products
PRODUCTS = {
    'DSWX_HLS': {
        'CCID': {
            'PROD': 'C2617126679-POCLOUD',
            # 'UAT': ''
        },
        'PATTERN': re.compile(r'OPERA_L3_DSWx-HLS_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S2A|S2B|S2C|S2D|L8|L9)_30_v\d+[.]\d+'),
        'UNIQUE_GROUPS': ['tile_id', 'acquisition_ts', 'sensor'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'CSLC_S1': {
        'CCID': {
            'PROD': 'C2777443834-ASF',
            # 'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L2_CSLC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_'
                              r'(?P<acquisition_ts>\d{8}T\d{6}Z)_(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_'
                              r'(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_v\d+[.]\d+)'),
        'UNIQUE_GROUPS': ['burst_id', 'acquisition_ts', 'sensor', 'pol'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'RTC_S1': {
        'CCID': {
            'PROD': 'C2777436413-ASF',
            'UAT': 'C1259974840-ASF'
        },
        'PATTERN': re.compile(r'OPERA_L2_RTC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+'),
        'UNIQUE_GROUPS': ['burst_id', 'acquisition_ts', 'sensor'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'CSLC_S1_STATIC': {
        'CCID': {
            'PROD': 'C2795135668-ASF',
            'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L2_CSLC-S1-STATIC_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<validity_ts>\d{8})_'
                              r'(?P<sensor>S1[A-D])_v\d+[.]\d+)'),
        'UNIQUE_GROUPS': ['burst_id', 'validity_ts', 'sensor'],
        'AGG_TS_GROUP': 'validity_ts',
        'AGG_TS_FORMAT': '%Y%m%d',
    },
    'RTC_S1_STATIC': {
        'CCID': {
            'PROD': 'C2795135174-ASF',
            'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L2_RTC-S1-STATIC_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<validity_ts>\d{8})_'
                              r'(?P<sensor>S1[A-D])_30_v\d+[.]\d+)'),
        'UNIQUE_GROUPS': ['burst_id', 'validity_ts', 'sensor'],
        'AGG_TS_GROUP': 'validity_ts',
        'AGG_TS_FORMAT': '%Y%m%d',
    },
    'DSWX_S1': {
        'CCID': {
            'PROD': 'C2949811996-POCLOUD',
            # 'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L3_DSWx-S1_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+)'),
        'UNIQUE_GROUPS': ['tile_id', 'acquisition_ts', 'sensor'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'DISP_S1': {
        'CCID': {
            'PROD': 'C3294057315-ASF',
            'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L3_DISP-S1_IW_(?P<frame_id>F\d{5})_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_'
                              r'(?P<ref_datetime>\d{8}T\d{6}Z)_(?P<sec_datetime>\d{8}T\d{6}Z)_v\d+[.]\d+_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z))'),
        'UNIQUE_GROUPS': ['frame_id', 'pol', 'ref_datetime', 'sec_datetime'],
        'AGG_TS_GROUP': 'creation_ts',  # TODO: Would ref or sec be more appropriate here?
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    # 'DISP_S1_STATIC': {
    #     'CCID': {
    #         'PROD': '',
    #         'UAT': ''
    #     },
    #     'PATTERN': re.compile(r''),
    #     'UNIQUE_GROUPS': [],
    #     'AGG_TS_GROUP': '',
    #     'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
    # },
    # 'DSWX_NI': {
    #     'CCID': {
    #         'PROD': '',
    #         'UAT': ''
    #     },
    #     'PATTERN': re.compile(r''),
    #     'UNIQUE_GROUPS': [],
    #     'AGG_TS_GROUP': '',
    #     'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
    # },
    # 'DISP_NI': {
    #     'CCID': {
    #         'PROD': '',
    #         'UAT': ''
    #     },
    #     'PATTERN': re.compile(r''),
    #     'UNIQUE_GROUPS': [],
    #     'AGG_TS_GROUP': '',
    #     'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
    # },
    'TROPO': {
        'CCID': {
            'PROD': 'C3717139408-ASF',
            # 'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L4_TROPO-ZENITH_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<model>.+?)_v\d+[.]\d+)'),
        'UNIQUE_GROUPS': ['acquisition_ts', 'model'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'DIST_ALERT_HLS': {
        'CCID': {
            'PROD': 'C2746980408-LPCLOUD',
            # 'UAT': ''
        },
        'PATTERN': re.compile(r'(?P<id>OPERA_L3_DIST-ALERT-HLS_(?P<tile_id>T[^\W_]{5})_'
                              r'(?P<acquisition_ts>\d{8}T\d{6}Z)_(?P<creation_ts>\d{8}T\d{6}Z)_'
                              r'(?P<sensor>S2A|S2B|S2C|S2D|L8|L9)_30_v\d+([.]\d+)?)'),
        'UNIQUE_GROUPS': ['tile_id', 'acquisition_ts', 'sensor'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
    'DIST_ALERT_S1': {
        'CCID': {
            'PROD': 'C4090131664-ASF',
            'UAT': 'C1275699124-ASF'
        },
        'PATTERN': re.compile(r'OPERA_L3_DIST-ALERT-S1_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                              r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[ABCD]?)_30_v\d+[.]\d+'),
        'UNIQUE_GROUPS': ['tile_id', 'acquisition_ts', 'sensor'],
        'AGG_TS_GROUP': 'acquisition_ts',
        'AGG_TS_FORMAT': DEFAULT_GRANULE_TIME_FMT,
        'CREATE_TS_GROUP': 'creation_ts',
    },
}


def _fatal_code(err: requests.exceptions.RequestException) -> bool:
    return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]


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
@backoff.on_exception(backoff.expo, requests.exceptions.Timeout, max_tries=2)
def _do_cmr_query(url, params, headers=None):
    if headers is None:
        headers = {}
    logger.info(f'Querying {url} with params {params} and headers {headers}')
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    return ([i['umm']['GranuleUR'] for i in response_json.get('items', [])],
            response.headers.get('CMR-Search-After', None))


def get_granule_ids_from_cmr(cmr_url, ccid, start, end, temporal, test_pattern: re.Pattern = None):
    granules = []

    params = {
        'collection_concept_id': ccid,
        'page_size': 2000
    }

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        if temporal:
            params['temporal[]'] = f'{start_q_str},{end_q_str}'
        else:
            params['revision_date[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params)
    granules.extend(query_result)
    if len(granules) > 0:
        logger.info(f'Most recent granule retrieved: {granules[-1]}')
        if test_pattern is not None:
            if test_pattern.match(granules[0]) is None:
                raise ValueError(f'Pattern {test_pattern} does not match granule: {granules[0]}')
    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, headers)
        granules.extend(query_result)
        if len(granules) > 0:
            logger.info(f'Most recent granule retrieved: {granules[-1]}')

    return granules


def main(args):
    start_time = datetime.now()

    ccid = PRODUCTS[args.product]['CCID'][args.venue]
    cmr_url = CMR_URLS[args.venue]

    pattern = PRODUCTS[args.product]['PATTERN']
    unique_groups = PRODUCTS[args.product]['UNIQUE_GROUPS']
    aggregation_ts = PRODUCTS[args.product]['AGG_TS_GROUP']
    aggregation_ts_fmt = PRODUCTS[args.product].get('AGG_TS_FORMAT', DEFAULT_GRANULE_TIME_FMT)

    logger.info(f'Listing granule IDs for product {args.product}({ccid}) from {args.venue}')

    granule_ids = get_granule_ids_from_cmr(
        cmr_url, ccid, args.start_date, args.end_date, args.use_temporal, test_pattern=pattern
    )

    logger.info(f'Found {len(granule_ids)} granule IDs')

    if len(granule_ids) == 0:
        logger.info('No data found!')
        return

    granule_month_map = {}
    aqc_date_map = {}
    unique_granules = {}

    for granule_id in granule_ids:
        match = pattern.match(granule_id)

        if match is None:
            raise RuntimeError(f'Failed to parse granule ID {granule_id} with pattern {pattern.pattern}')

        group_dict = match.groupdict()

        granule_agg_time = datetime.strptime(group_dict[aggregation_ts], aggregation_ts_fmt)
        granule_agg_day = granule_agg_time.replace(hour=0, minute=0, second=0, microsecond=0)
        granule_agg_day = granule_agg_day.strftime('%Y-%m-%d')

        granule_agg_month = granule_agg_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        granule_agg_month = granule_agg_month.strftime('%Y-%m')

        if granule_agg_day not in aqc_date_map:
            aqc_date_map[granule_agg_day] = {
                'n_granules': 0,
                'n_duplicates': 0,
                'percent_duplicates': -1.0,
                'duplicates': {}
            }

        if granule_agg_month not in granule_month_map:
            granule_month_map[granule_agg_month] = {
                'n_granules': 0,
                'n_duplicates': 0,
                'percent_duplicates': -1.0,
                'duplicates': {}
            }

        aqc_date_map[granule_agg_day]['n_granules'] += 1
        granule_month_map[granule_agg_month]['n_granules'] += 1

        granule_unique_ids = tuple([group_dict[grp] for grp in unique_groups])

        if granule_unique_ids in unique_granules:
            granule_month_map[granule_agg_month]['n_duplicates'] += 1
            aqc_date_map[granule_agg_day]['n_duplicates'] += 1
            first_duplicate = unique_granules[granule_unique_ids]

            if first_duplicate[1] not in granule_month_map[granule_agg_month]['duplicates']:
                granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

            if first_duplicate[1] not in aqc_date_map[granule_agg_day]['duplicates']:
                aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]] = [first_duplicate[0]]

            granule_month_map[granule_agg_month]['duplicates'][first_duplicate[1]].append(granule_id)
            aqc_date_map[granule_agg_day]['duplicates'][first_duplicate[1]].append(granule_id)
        else:
            unique_granules[granule_unique_ids] = (granule_id, repr(granule_unique_ids))

    if 'CREATE_TS_GROUP' in PRODUCTS[args.product]:
        for month in granule_month_map:
            for duplicate in granule_month_map[month]['duplicates']:
                duplicate_granule_ids = granule_month_map[month]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[PRODUCTS[args.product]['CREATE_TS_GROUP']], reverse=True
                )

                granule_month_map[month]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

        for date in aqc_date_map:
            for duplicate in aqc_date_map[date]['duplicates']:
                duplicate_granule_ids = aqc_date_map[date]['duplicates'][duplicate]
                duplicate_granule_ids.sort(
                    key=lambda x: pattern.match(x).groupdict()[PRODUCTS[args.product]['CREATE_TS_GROUP']], reverse=True
                )

                aqc_date_map[date]['duplicates'][duplicate] = {
                    'latest_product': duplicate_granule_ids[0],
                    'duplicate_products': duplicate_granule_ids[1:],
                }

    granule_month_map = dict(sorted(granule_month_map.items()))
    aqc_date_map = dict(sorted(aqc_date_map.items()))

    for month in granule_month_map.keys():
        granule_month_map[month]['percent_duplicates'] = (granule_month_map[month]['n_duplicates'] /
                                                          granule_month_map[month]['n_granules']) * 100

    for date in aqc_date_map.keys():
        aqc_date_map[date]['percent_duplicates'] = (aqc_date_map[date]['n_duplicates'] /
                                                    aqc_date_map[date]['n_granules']) * 100

    n_duplicates = sum([month['n_duplicates'] for month in granule_month_map.values()])
    logger.info(f'Found {n_duplicates} duplicate granule IDs out of {len(granule_ids)} granules '
                f'({(n_duplicates / len(granule_ids)) * 100:.1f}%)')

    if 'CREATE_TS_GROUP' in PRODUCTS[args.product]:
        duplicate_counts = list(chain.from_iterable(list(map(lambda x: [len(dup['duplicate_products'])
                                                                        for dup in x['duplicates'].values()],
                                                             granule_month_map.values()))))
    else:
        duplicate_counts = list(chain.from_iterable(list(map(lambda x: [len(dup) for dup in x['duplicates'].values()],
                                                             granule_month_map.values()))))

    if len(duplicate_counts) > 0:
        logger.info(f'Minimum number of duplicates per granule ID: {min(duplicate_counts)}')
        logger.info(f'Maximum number of duplicates per granule ID: {max(duplicate_counts)}')
        logger.info(f'Average number of duplicates per granule ID: {sum(duplicate_counts) / len(duplicate_counts)}')

    final_report = {
        'summary': {
            'product': args.product,
            'venue': args.venue,
            'ccid': ccid,
            'n_granules': len(granule_ids),
            'n_duplicates': n_duplicates,
            'percent_duplicates': (n_duplicates / len(granule_ids)) * 100,
            'min_duplicates_per_granule': min(duplicate_counts) if n_duplicates > 0 else None,
            'max_duplicates_per_granule': max(duplicate_counts) if n_duplicates > 0 else None,
            'avg_duplicates_per_granule': sum(duplicate_counts) / len(duplicate_counts) if n_duplicates > 0 else None,
            'report_run_time': str(datetime.now() - start_time),
        },
    }

    if args.facet in {'months', 'both'}:
        final_report['months'] = granule_month_map
    if args.facet in {'dates', 'both'}:
        final_report['dates'] = aqc_date_map

    with open(args.output, 'w') as f:
        json.dump(final_report, f, indent=2)

    logger.info(f'Wrote JSON report to {args.output}')
    logger.info(f'Done in {datetime.now() - start_time}')


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'product',
        choices=list(PRODUCTS.keys()),
        help='Product to check'
    )

    parser.add_argument(
        '-o', '--output',
        default='duplicate_report.json',
        help='Name of output JSON file'
    )

    parser.add_argument(
        '--venue',
        choices=list(CMR_URLS.keys()),
        default='PROD',
        help='Venue to check: PROD or UAT. Default: PROD'
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

    parser.add_argument(
        '--facet',
        choices=['months', 'dates', 'both'],
        default='months',
        help=argparse.SUPPRESS
    )

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
