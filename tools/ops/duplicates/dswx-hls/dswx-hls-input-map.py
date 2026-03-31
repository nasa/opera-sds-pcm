import argparse
import json
import logging
import re
import warnings
from copy import deepcopy
from datetime import datetime, timedelta
from math import ceil
from os.path import basename, join
from pathlib import Path

import backoff
import numpy as np
import requests
from matplotlib import pyplot as plt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'
CCID = 'C2617126679-POCLOUD'
CCID_HLSS = 'C2021957295-LPCLOUD'
CCID_HLSL = 'C2021957657-LPCLOUD'
DSWX_PATTERN = re.compile(r'OPERA_L3_DSWx-HLS_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                          r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S2A|S2B|S2C|S2D|L8|L9)_30_v\d+[.]\d+')
HLS_PATTERN = re.compile(r'(?P<id>(?P<product_shortname>HLS[.](?P<source>[SL])30)[.](?P<tile_id>T[^\W_]{5})[.]'
                         r'(?P<acquisition_ts>\d{7}T\d{6})[.](?P<collection_version>v\d+[.]\d+))')
HLS_SUFFIX = re.compile(r'[.](B[A-Za-z0-9]{2}|Fmask)[.]tif$')

DSWX_GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'
HLS_GRANULE_TIME_FMT = '%Y%jT%H%M%S'
CMR_TIME_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
REPORT_TIME_FMT = '%Y-%m-%dT%H:%M:%S'
# FACET_DATE_FMT = '%Y-%j'

L9_EARLIEST_DSWX_ACQUISITION_DATE = datetime(2025, 10, 1, 0, 4, 7, 135000)


def _format_facet_date(d: datetime) -> str:
    return f'{d.strftime("%Y-%m-%d")} / {d.strftime("%Y-%j")}'


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


def query_cmr(cmr_url, ccid, start, end, func=None):
    granules = []

    params = {
        'collection_concept_id': ccid,
        'page_size': 2000
    }

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        params['temporal[]'] = f'{start_q_str},{end_q_str}'
        # if temporal:
        #     params['temporal[]'] = f'{start_q_str},{end_q_str}'
        # else:
        #     params['revision_date[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params, func=func)
    granules.extend(query_result)

    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, func=func, headers=headers)
        granules.extend(query_result)

    return granules


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-o', '--output',
        default='dswx_hls_report.json',
        help='Name of output JSON file'
    )

    parser.add_argument(
        '-d', '--histogram-dir',
        default=None,
        help='Optional directory to save histogram plots to'
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
        '--full-report',
        action='store_true',
        help='Include 1-to-1 HLS to OPERA mappings in the report'
    )

    parser.add_argument(
        '--plot-days',
        action='store_true',
        dest='skip_agg',
        help=argparse.SUPPRESS
    )

    return parser


def _plot_and_save_counts(counts, directory, filename, title):
    days = [day.split('/')[0].strip() for day in sorted(counts.keys())]

    data = {
        'hls_granules': tuple([counts[d]['hls_granules'] for d in sorted(counts.keys())]),
        'matched_dswx_hls_granules': tuple([counts[d]['matched_dswx_hls_granules'] for d in sorted(counts.keys())]),
        'hls_to_many_dswx': tuple([counts[d]['hls_to_many_dswx'] for d in sorted(counts.keys())]),
        'hls_to_no_dswx': tuple([counts[d]['hls_to_no_dswx'] for d in sorted(counts.keys())]),
    }

    x = np.arange(len(days))
    width = 1 / 5
    multiplier = 0

    fig, ax = plt.subplots(layout='constrained', figsize=(5 + len(days), 8))

    for (measure, count), color in zip(data.items(), [
        'tab:green', 'tab:blue', 'tab:orange', 'tab:red'
    ]):
        offset = width * multiplier
        rects = ax.bar(x + offset, count, width, label=measure, color=color)
        ax.bar_label(rects, padding=3, rotation=90, fontsize=12, fmt='{:,.0f}')
        multiplier += 1

    ax.set_xlabel('Acquisition date (at 00:00:00Z)', fontsize=12)
    ax.set_ylabel('Granule Count', fontsize=12)

    ax.set_xticks(x + width, days, rotation=90)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=UserWarning)
        ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

    ax.set_title(title, fontsize=14)

    ymax = max([max(data[d]) for d in data.keys()])
    if ymax > 0:
        ymax = ceil(ymax * 1.2)  # Scale a bit to fit the labels
    else:
        ymax = 1

    ax.set_ylim(bottom=0, top=ymax)

    ax.legend([
        'HLS Granules',
        'OPERA DSWx-HLS Granules',
        'HLS granules mapped to more than one DSWx-HLS granule',
        'HLS granules mapped to no DSWx-HLS granule',
    ], fontsize=12)

    plt.savefig(join(directory, filename))
    logger.info(f'Wrote plot {filename}')


def plot_and_save(date_counts, directory, skip_agg=False):
    Path(directory).mkdir(exist_ok=True, parents=True)

    if not skip_agg:
        month_map = {}

        for date in date_counts:
            month_str = datetime.strptime(date.split('/')[0].strip(), "%Y-%m-%d").strftime('%Y-%m')

            if month_str not in month_map:
                month_map[month_str] = {}

            month_map[month_str][date] = date_counts[date]

        for month in month_map:
            month_counts = month_map[month]
            _plot_and_save_counts(month_counts, directory, f'{month}.png', f'Counts for {month}')
    else:
        days = [day.split('/')[0].strip() for day in sorted(date_counts.keys())]

        if days[0] != days[-1]:
            filename = f'dswx_hls_accountability_{days[0]}_to_{days[-1]}.png'
            title = f'DSWx-HLS Accountability for {days[0]} to {days[-1]}'
        else:
            filename = f'dswx_hls_accountability_{days[0]}.png'
            title = f'DSWx-HLS Accountability for {days[0]}'

        _plot_and_save_counts(date_counts, directory, filename, title)


def main(args):
    start = datetime.now()

    logger.info('Listing DSWx-HLS granules over configured time range')
    dswx_granules = query_cmr(
        CMR_URL,
        CCID,
        args.start_date,
        args.end_date,
        lambda x: [
            (
                i['umm']['GranuleUR'],
                _format_facet_date(datetime.strptime(i['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'],
                                   CMR_TIME_FMT)),
                i['umm']['InputGranules']
            ) for i in x
        ]  # list[cmr] -> list[(gUR, cmr temporal time, inputs)]
    )

    logger.info(f'Found {len(dswx_granules):,} granules')

    hls_to_dswx = {}

    for granule, date, inputs in dswx_granules:
        filtered_inputs = set()

        for i in inputs:
            stripped = re.sub(HLS_SUFFIX, '', basename(i))
            if HLS_PATTERN.match(stripped) is not None:
                filtered_inputs.add((stripped, date))

        filtered_inputs = list(filtered_inputs)

        if len(filtered_inputs) == 0:
            raise ValueError(f'Could not get inputs for granule {granule}')
        elif len(filtered_inputs) > 1:
            logger.warning(f'Found {len(filtered_inputs)} inputs for granule {granule}: {filtered_inputs}')

        for i in filtered_inputs:
            hls_to_dswx.setdefault(i, []).append(granule)

    n_dswx_hls_inputs = len(hls_to_dswx)

    logger.info(f'Mapped OPERA DSWx-HLS products to {n_dswx_hls_inputs:,} HLS inputs')

    logger.info(f'Listing HLS-S granules over configured time range')
    hls_s_granules = query_cmr(
        CMR_URL,
        CCID_HLSS,
        args.start_date,
        args.end_date,
        lambda x: [
            (
                i['umm']['GranuleUR'],
                _format_facet_date(datetime.strptime(i['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'],
                                   CMR_TIME_FMT)),
            ) for i in x
        ]
    )
    logger.info(f'Found {len(hls_s_granules):,} HLS-S granules')

    logger.info(f'Listing HLS-L granules over configured time range')
    hls_l_granules = query_cmr(
        CMR_URL,
        CCID_HLSL,
        args.start_date,
        args.end_date,
        lambda x: [
            (
                i['umm']['GranuleUR'],
                datetime.strptime(i['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'], CMR_TIME_FMT),
                [p['ShortName'] for p in i['umm']['Platforms']]
            ) for i in x
        ]
    )
    n_hls_l_inputs = len(hls_l_granules)
    logger.info(f'Found {n_hls_l_inputs:,} HLS-L granules')

    # Filter out HLS granules from Landsat-9 acquired before OPERA began producing DSWx-HLS with L9 data

    hls_l_granules = [
        (g[0], _format_facet_date(g[1]))
        for g in hls_l_granules
        if ('LANDSAT-9' not in g[2] or g[1] >= L9_EARLIEST_DSWX_ACQUISITION_DATE)
    ]

    logger.info(f'Filtered HLS-L granules from {n_hls_l_inputs:,} to {len(hls_l_granules):,} for a combined '
                f'{len(hls_s_granules) + len(hls_l_granules):,} HLS granules')

    for hls_granule, date in hls_s_granules + hls_l_granules:
        if (hls_granule, date) not in hls_to_dswx:
            hls_to_dswx[(hls_granule, date)] = []

    logger.info(f'Found {len(hls_to_dswx) - n_dswx_hls_inputs:,} HLS granules not mapped to an OPERA DSWx-HLS product')

    date_map = {}
    hls_mappings = {}

    logger.info(f'Grouping HLS granules by date')

    for hls_granule, date in hls_to_dswx:
        if date not in date_map:
            date_map[date] = {}

        date_map[date][hls_granule] = hls_to_dswx[(hls_granule, date)]
        hls_mappings[hls_granule] = hls_to_dswx[(hls_granule, date)]

    date_counts = {}
    month_counts = {}

    for date in date_map:
        date_counts[date] = {
            'hls_granules': len(date_map[date]),
            'matched_dswx_hls_granules': sum([len(v) for v in date_map[date].values()]),
            'hls_to_many_dswx': len([v for v in date_map[date].values() if len(v) > 1]),
            'hls_to_no_dswx': len([v for v in date_map[date].values() if len(v) == 0]),
        }

        month = datetime.strptime(date.split('/')[0].strip(), '%Y-%m-%d').replace(day=1).strftime('%Y-%m')

        if month not in month_counts:
            month_counts[month] = deepcopy(date_counts[date])
        else:
            month_counts[month]['hls_granules'] += date_counts[date]['hls_granules']
            month_counts[month]['matched_dswx_hls_granules'] += date_counts[date]['matched_dswx_hls_granules']
            month_counts[month]['hls_to_many_dswx'] += date_counts[date]['hls_to_many_dswx']
            month_counts[month]['hls_to_no_dswx'] += date_counts[date]['hls_to_no_dswx']

    overall_counts = {
        'hls_granules': sum([v['hls_granules'] for v in date_counts.values()]),
        'matched_dswx_hls_granules': sum([v['matched_dswx_hls_granules'] for v in date_counts.values()]),
        'hls_to_many_dswx': sum([v['hls_to_many_dswx'] for v in date_counts.values()]),
        'hls_to_no_dswx': sum([v['hls_to_no_dswx'] for v in date_counts.values()]),
    }

    missing_dswx = [hls for hls, opera in hls_mappings.items() if len(opera) == 0]
    hls_mappings = {hls: opera for hls, opera in hls_mappings.items() if len(opera) > 0}

    duplicates = []

    for product_list in hls_mappings.values():
        product_list.sort(key=lambda x: DSWX_PATTERN.match(x).groupdict()['creation_ts'], reverse=True)
        duplicates.extend(product_list[1:])

    if not args.full_report:
        for date in date_map:
            date_map[date] = {k: v for k, v in date_map[date].items() if len(v) != 1}

    report = {
        'summary': {
            'query_start_date': args.start_date.strftime(REPORT_TIME_FMT),
            'query_end_date': args.end_date.strftime(REPORT_TIME_FMT),
            'report_run_time': str(datetime.now() - start),
            'overall_counts': overall_counts,
        },
        'counts_by_date': date_counts,
        'counts_by_month': month_counts,
        'hls_missing_dswx': missing_dswx,
        'dswx_duplicates': duplicates,
        'hls_to_dswx_mappings_by_date': date_map
    }

    with open(args.output, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f'Wrote report to {args.output}')

    if args.histogram_dir is not None and len(date_counts) > 0:
        if args.skip_agg:
            # Fill in missing dates for the plot
            plot_start_date = args.start_date
            plot_end_date = args.end_date

            if plot_end_date > plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0):
                plot_end_date = plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                plot_end_date = plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

            plot_date = plot_start_date

            while plot_date <= plot_end_date:
                plot_date_str = plot_date.strftime("%Y-%m-%d / %Y-%j")

                if plot_date_str not in date_counts:
                    date_counts[plot_date_str] = {
                        'hls_granules': 0,
                        'matched_dswx_hls_granules': 0,
                        'hls_to_many_dswx': 0,
                        'hls_to_no_dswx': 0,
                    }

                plot_date += timedelta(days=1)

        logger.info(f'Writing monthly histograms to {args.histogram_dir}')
        plot_and_save(date_counts, args.histogram_dir, args.skip_agg)

    logger.info('Done')


if __name__ == '__main__':
    main(get_parser().parse_args())
