import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from random import sample

import backoff
import pandas as pd
import requests
from tabulate import tabulate

from opera_commons.logger import logger
from tools.dataspace_s1_download import query, build_query_filter, ISO_TIME
from util.backoff_util import fatal_code, backoff_logger
from util.dataspace_util import NoQueryResultsException

PLATFORM_CCID_MAP = {
    'A': 'C1214470488-ASF',
    'C': 'C3470873558-ASF'
}

CMR_SEARCH_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--start-time',
        required=False,
        default=None,
        type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
        help='Optional filter collection granules to those at or after this time. Must be in yyyy-mm-ddThh:mm:ssZ '
             'format'
    )

    parser.add_argument(
        '--end-time',
        required=False,
        default=None,
        type=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'),
        help='Optional filter collection granules to those at or before this time. Must be in yyyy-mm-ddThh:mm:ssZ '
             'format'
    )

    parser.add_argument(
        '--platforms',
        required=False,
        default=['A', 'C'],
        nargs='+',
        choices=['A', 'C'],
        help='List of S1 satellites to survey. Supports A & C (default)'
    )

    def __positive_integer(s):
        i = int(s)

        if i <= 0:
            raise ValueError(f'Expected positive integer but got {i}')

        return i

    parser.add_argument(
        '--sample',
        required=False,
        default=None,
        type=__positive_integer,
        help='Optional parameter to randomly select n SLC granules from the initial CMR response'
    )

    parser.add_argument(
        '-f', '--format',
        required=False,
        default='text',
        choices=['text', 'json', 'csv', 'quiet'],
        help='Optional parameter to set output format. Options: text (print to stdout), json (dump to file) and csv'
             ' (dump to file). Output filenames can be set with --output (omit extensions). Use "quiet" to just output'
             ' average latencies.'
    )

    parser.add_argument(
        '-o', '--output',
        required=False,
        default='slc_latencies',
        help='Filename (without extension) of output file for json/csv formats'
    )

    parser.add_argument(
        '--query-by-revision-date',
        action='store_false',
        dest='temporal_query',
        help='Query SLCs by time of last revision rather than by sensing time'
    )

    return parser


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=fatal_code,
                      on_backoff=backoff_logger,
                      interval=15)
def _do_cmr_query(url, params, headers=None):
    if headers is None:
        headers = {}

    logger.info(f'Querying {url} with params {params} and headers {headers}')
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    response_json = response.json()

    return response_json['items'], response.headers.get('CMR-Search-After', None)


def _dataspace_get_by_id(gid):
    # results = query(build_query_filter(f"Name eq '{gid}.SAFE'"))
    results = query(build_query_filter(f"contains(Name,'{gid}')"))

    if len(results) == 0:
        logger.error(f'No data found for gid {gid}')
        return None, gid
    if len(results) > 1:
        logger.warning(f'Multiple results found for gid {gid}, picking first')

    return results[0], gid


def query_cmr(args):
    granules = []

    for platform in args.platforms:
        start_q_str = args.start_time.strftime('%Y-%m-%dT%H:%M:%SZ') if args.start_time is not None else ''
        end_q_str = args.end_time.strftime('%Y-%m-%dT%H:%M:%SZ') if args.end_time is not None else ''

        params = {
            'collection_concept_id': PLATFORM_CCID_MAP[platform],
            'page_size': 2000,
            'attribute[]': 'string,BEAM_MODE,IW'
        }

        if args.temporal_query:
            params['temporal[]'] = f'{start_q_str},{end_q_str}'
        else:
            params['revision_date[]'] = f'{start_q_str},{end_q_str}'

        query_result, search_after = _do_cmr_query(CMR_SEARCH_URL, params)
        granules.extend(query_result)

        while search_after is not None:
            headers = {'CMR-Search-After': search_after}
            query_result, search_after = _do_cmr_query(CMR_SEARCH_URL, params, headers)
            granules.extend(query_result)

    logger.info(f'Found {len(granules):,} granules from CMR')

    if args.sample is not None and len(granules) > args.sample:
        logger.info(f'Sampling {args.sample} granules from the CMR response')
        granules = sample(granules, args.sample)

    return granules


def cmr_to_dates(cmr, args):
    return dict(
        sensing_date=datetime.strptime(
            cmr['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'],
            '%Y-%m-%dT%H:%M:%S.%fZ'
        ),
        published_date=datetime.strptime(
            [d for d in cmr['umm']['ProviderDates'] if d['Type'] == 'Insert'][0]['Date'],
            '%Y-%m-%dT%H:%M:%S.%fZ'
        )
    )


def dataspace_to_dates(dataspace, args):
    return dict(
        sensing_date=datetime.strptime(
            dataspace['ContentDate']['Start'],
            '%Y-%m-%dT%H:%M:%S.%fZ'
        ),
        published_date=datetime.strptime(
            dataspace['PublicationDate'],
            '%Y-%m-%dT%H:%M:%S.%fZ'
        )
    )


def main(args):
    cmr_granules = query_cmr(args)

    survey: dict = {g['umm']['GranuleUR'].removesuffix('-SLC'): dict(cmr=g, dataspace=None) for g in cmr_granules}

    if args.temporal_query:
        dataspace_query_filters = [
            f'ContentDate/End ge {datetime.strftime(args.start_time, ISO_TIME)}',
            f'ContentDate/Start le {datetime.strftime(args.end_time, ISO_TIME)}'
        ]
    else:
        dataspace_query_filters = [
            f'ModificationDate ge {datetime.strftime(args.start_time, ISO_TIME)}',
            f'ModificationDate le {datetime.strftime(args.end_time, ISO_TIME)}'
        ]

    query_results = query(build_query_filter(*dataspace_query_filters, platforms=tuple(args.platforms)))

    dataspace_granules = query_results

    debug_start_end_dates = [(datetime.strftime(args.start_time, ISO_TIME), datetime.strftime(args.end_time, ISO_TIME))]

    while len(query_results) == 11000:
        logger.warning('Potentially exceeded maximum result size of dataspace query. Splitting')

        new_start_time = max([
            datetime.strptime(
                g['ContentDate']['End'],
                '%Y-%m-%dT%H:%M:%S.%fZ'
            ) for g in dataspace_granules
        ]).strftime(ISO_TIME)

        logger.info(f'New start time: {new_start_time}')

        dataspace_query_filters = [
            f'ContentDate/End ge {new_start_time}',
            f'ContentDate/Start le {datetime.strftime(args.end_time, ISO_TIME)}'
        ]

        debug_start_end_dates.append((new_start_time, datetime.strftime(args.end_time, ISO_TIME)))

        query_results = query(build_query_filter(*dataspace_query_filters, platforms=tuple(args.platforms)))
        dataspace_granules.extend(query_results)

    logger.info(f'Found {len(dataspace_granules):,} granules in ESA Dataspace')

    missing_from_cmr = {}
    missing_from_dataspace = {}

    for result in dataspace_granules:
        granule_id = result['Name'].removesuffix('.SAFE')

        if granule_id in survey:
            survey[granule_id]['dataspace'] = result
        else:
            missing_from_cmr[granule_id] = dataspace_to_dates(result, args)

    missing_gids = [gid for gid, entry in survey.items() if entry['dataspace'] is None]

    if len(missing_gids) > 0:
        to_drop = []

        logger.error(f'There are {len(missing_gids):,} granules from CMR that Dataspace did not return')

        with ThreadPoolExecutor() as pool:
            futures = [pool.submit(_dataspace_get_by_id, missing_gid) for missing_gid in missing_gids]

            for f in futures:
                result, gid = f.result()
                if result is not None:
                    dataspace_granules.append(result)
                    survey[gid]['dataspace'] = result
                else:
                    to_drop.append(gid)

        if len(to_drop) > 0:
            logger.error(f'Need to drop {len(to_drop):,} granules from survey since they were unable to be found in '
                         f'both systems')
            for gid in to_drop:
                missing_from_dataspace[gid] = cmr_to_dates(survey.pop(gid)['cmr'], args)

    logger.info(f'Final survey size: {len(survey):,}')

    for gid in survey:
        survey[gid]['cmr'] = cmr_to_dates(survey[gid]['cmr'], args)
        survey[gid]['dataspace'] = dataspace_to_dates(survey[gid]['dataspace'], args)

        sensing_date = survey[gid]['dataspace']['sensing_date']  # Use ESA's since it's more precise
        cmr_publish_date = survey[gid]['cmr']['published_date']
        dataspace_publish_date = survey[gid]['dataspace']['published_date']

        survey[gid] = {
            'sensing_date': sensing_date,
            'cmr_publish_date': cmr_publish_date,
            'cmr_publish_latency': cmr_publish_date - sensing_date,
            'dataspace_publish_date': dataspace_publish_date,
            'dataspace_publish_latency': dataspace_publish_date - sensing_date,
        }

    if args.format == 'text':
        print(tabulate(
            [[k] + list(v.values()) for k, v in survey.items()],
            headers=['slc_id'] + list(list(survey.values())[0].keys())
        ))
    elif args.format == 'json':
        filename = f'{args.output}.json'

        with open(filename, 'w') as f:
            json.dump(
                survey,
                f,
                indent=2,
                default=lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%fZ") if isinstance(x, datetime) else str(x)
            )

        logger.info(f'Wrote survey to {filename}')
    elif args.format == 'csv':
        filename = f'{args.output}.csv'

        df = pd.DataFrame.from_dict(survey, orient='index')
        df.to_csv(filename, index_label='slc_id')

        logger.info(f'Wrote survey to {filename}')

    logger.info(f'Surveyed {len(survey):,} SLCs')
    logger.info(f'Average CMR latency: {sum([v["cmr_publish_latency"] for v in survey.values()], start=timedelta(seconds=0)) / len(survey)}')
    logger.info(f'Average Dataspace latency: {sum([v["dataspace_publish_latency"] for v in survey.values()], start=timedelta(seconds=0)) / len(survey)}')

    if len(missing_from_cmr) > 0:
        filename = f'{args.output}.missing_cmr.json'
        with open(filename, 'w') as f:
            json.dump(
                missing_from_cmr,
                f,
                indent=2,
                default=lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%fZ") if isinstance(x, datetime) else str(x)
            )
        logger.warning(f'{len(missing_from_cmr):,} granules found in dataspace query that were not in CMR. '
                       f'Dumped to {filename}. Note some of these may be due to CMR\'s ingestion latency.')

    if len(missing_from_dataspace) > 0:
        filename = f'{args.output}.missing_dataspace.json'
        with open(filename, 'w') as f:
            json.dump(
                missing_from_dataspace,
                f,
                indent=2,
                default=lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%fZ") if isinstance(x, datetime) else str(x)
            )
        logger.warning(f'{len(missing_from_dataspace):,} granules found in CMR query that were not in Dataspace. '
                       f'Dumped to {filename}.')


if __name__ == '__main__':
    main(get_parser().parse_args())

