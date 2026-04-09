import argparse
import json
import logging
import os.path
import subprocess
import sys
import warnings
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import boto3
import matplotlib.pyplot as plt
import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)
s3 = boto3.client('s3')

try:
    from datetime import UTC

    def now(utc=False):
        if utc:
            return datetime.now(UTC)
        else:
            return datetime.now()
except ImportError:
    def now(utc=False):
        if utc:
            return datetime.utcnow()
        else:
            return datetime.now()


def _get_start_end_dates(args: argparse.Namespace):
    if args.start_date is not None or args.end_date is not None:
        if args.start_date is None:
            args.start_date = datetime(1900, 1, 1)
        if args.end_date is None:
            args.end_date = datetime(3000, 1, 1)
    else:
        args.end_date = now(True).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        args.start_date = args.end_date - timedelta(days=args.days_back)

    return args.start_date.strftime('%Y-%m-%dT%H:%M:%SZ'), args.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'products',
        nargs='+',
        choices=['DSWX_HLS', 'CSLC_S1', 'RTC_S1', 'DSWX_S1', 'DISP_S1', 'TROPO', ],
        help='Products to check'
    )

    parser.add_argument(
        '--venue',
        choices=['PROD', 'UAT'],
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

    def _pos_int(s):
        i = int(s)
        assert i > 0
        return i

    parser.add_argument(
        '-d', '--days-back',
        type=_pos_int,
        default=None,
        help="The number of days back to check. For Example, --days-back 5"
    )

    parser.add_argument(
        '--report-dir',
        required=True,
        type=Path,
        help="The directory to write reports to."
    )

    parser.add_argument(
        '--plot-dir',
        required=True,
        type=Path,
        help="The directory to write plots to."
    )

    parser.add_argument(
        '--s3-report-path',
        required=True,
        help='S3 root URL to store reports of duplicate products into'
    )

    parser.add_argument(
        '--s3-plot-path',
        required=True,
        help='S3 root URL to store plots of duplicate products into'
    )

    parser.add_argument(
        '--opensearch',
        default=None,
        help='Base URL of opensearch cluster'
    )

    parser.add_argument(
        '--duplicate-index',
        default='duplicates',
        help='Opensearch index name to send duplicate data to'
    )

    parser.add_argument(
        '--accountability-index',
        default='accountability',
        help='Opensearch index name to send accountability data to'
    )

    parser.add_argument(
        '--duplicate-plot-length',
        default=10,
        type=_pos_int,
        dest='plot_length',
        help='The maximum number of dates in the duplicate plots produced'
    )

    return parser


def plot_data_and_save(data, plot_dir, s3_dir):
    plot_dir.mkdir(exist_ok=True, parents=True)

    s3_url = urlparse(s3_dir)
    s3_bucket = s3_url.netloc
    s3_path = Path(s3_url.path.lstrip('/'))

    start_date = datetime.strptime(data['start_date'], '%Y-%m-%dT%H:%M:%SZ').replace(hour=0, minute=0,
                                                                                     second=0, microsecond=0)
    end_date = datetime.strptime(data['end_date'], '%Y-%m-%dT%H:%M:%SZ')

    # If end date is at midnight UTC, it's very likely we'll have no granules in that date, so let's drop it from the
    # plot.
    if end_date > end_date.replace(hour=0, minute=0, second=0, microsecond=0):
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    report_acquisition_dates = []

    date = start_date
    while date <= end_date:
        report_acquisition_dates.append(date.strftime('%Y-%m-%d'))
        date += timedelta(days=1)

    report_acquisition_dates = set(report_acquisition_dates)
    product_set = list(data['date_maps'].keys())

    for product in product_set:
        product_dates = set(data['date_maps'][product].keys())
        days = list(product_dates | report_acquisition_dates)
        days.sort()

        x = np.arange(len(days))

        product_data = {
            'total_products': tuple([data['date_maps'][product].get(date, {}).get('products', 0) for date in days]),
            'duplicate_products': tuple(
                [data['date_maps'][product].get(date, {}).get('duplicates', 0) for date in days]
            ),
            'duplicate_percent': tuple(
                [data['date_maps'][product].get(date, {}).get('percent_duplicates', 0) for date in days]
            )
        }

        width = 1 / 3
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        for measure, color in zip(['total_products', 'duplicate_products'],
                                  ['tab:blue', 'tab:orange']):
            count = product_data[measure]
            offset = width * multiplier
            rects = ax.bar(x + offset, count, width, label=measure, color=color)

            if measure == 'total_products':
                ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)
            else:
                labels = [f'{c:,}\n({perc:0.2f}%)' for c, perc in zip(count, product_data['duplicate_percent'])]
                ax.bar_label(rects, labels, padding=3, fontsize=12, rotation=90)

            multiplier += 1

        ax.set_xlabel('Acquisition date (at 00:00:00Z)', fontsize=12)
        ax.set_ylabel('Granule Count', fontsize=12)

        ax.set_xticks(x + (width / 2), days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(f'Product counts for {product} from {days[0]} to {days[-1]}', fontsize=14)

        ymax = max(product_data['total_products'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total Product Count', 'Duplicate Product Count'], fontsize=12)

        plot_path = plot_dir / f'{product}_counts.png'
        s3_key = str(s3_path / f'{product}_counts.png').lstrip('/')

        plt.savefig(plot_path)
        logger.info(f'Wrote duplicate product counts plot to {str(plot_path)}')

        s3.upload_file(plot_path, s3_bucket, s3_key)
        logger.info(f'Uploaded total products plot to s3://{s3_bucket}/{s3_key}')


def plot_timeseries_data_and_save(data, plot_dir, s3_dir):
    plot_dir.mkdir(exist_ok=True, parents=True)

    s3_url = urlparse(s3_dir)
    s3_bucket = s3_url.netloc
    s3_path = Path(s3_url.path.lstrip('/'))

    data.sort(key=lambda x: x['date'])

    product_set = []

    for record in data:
        product_set.extend(record['product_counts'].keys())

    product_set = list(set(product_set))
    product_set.sort()

    days = [r['date'] for r in data]

    x = np.arange(len(days))

    for product in product_set:
        product_data = {
            'total_products': tuple([r['product_counts'].get(product, {}).get('total_products', 0) for r in data]),
            'duplicate_products': tuple([r['product_counts'].get(product, {}).get('duplicates', 0) for r in data]),
            'duplicate_percent': tuple([r['product_counts'].get(product, {}).get('percent_duplicates', 0) for r in data])
        }

        width = 1 / 3
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        for measure, color in zip(['total_products', 'duplicate_products'],
                                  ['tab:blue', 'tab:orange']):
            count = product_data[measure]
            offset = width * multiplier
            rects = ax.bar(x + offset, count, width, label=measure, color=color)

            if measure == 'total_products':
                ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)
            else:
                labels = [f'{c:,}\n({perc:0.2f}%)' for c, perc in zip(count, product_data['duplicate_percent'])]
                ax.bar_label(rects, labels, padding=3, fontsize=12, rotation=90)

            multiplier += 1

        ax.set_xlabel('Acquisition date (at 00:00:00Z)', fontsize=12)
        ax.set_ylabel('Granule Count', fontsize=12)

        ax.set_xticks(x + (width / 2), days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(f'Product counts timeseries for {product} from {days[0]} to {days[-1]}', fontsize=14)

        ymax = max(product_data['total_products'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)  # Scale a bit to fit the labels
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total Product Count', 'Duplicate Product Count'], fontsize=12)

        plot_path = plot_dir / f'{product}_counts_timeseries.png'
        s3_key = str(s3_path / f'{product}_counts_timeseries.png').lstrip('/')

        plt.savefig(plot_path)
        logger.info(f'Wrote duplicate product counts plot to {str(plot_path)}')

        s3.upload_file(plot_path, s3_bucket, s3_key)
        logger.info(f'Uploaded total products plot to s3://{s3_bucket}/{s3_key}')


def record_dswx_hls_accountability(args, start_date, end_date):
    report_path = str(args.report_dir / 'DSWX_HLS_accountability.json')
    report_date = now().strftime('%Y-%m-%d')

    with open(report_path) as f:
        report_data = json.load(f)

    date_counts = report_data['counts_by_date']
    days = [day.split('/')[0].strip() for day in sorted(date_counts.keys())]

    # Fix list of days as sometimes no data on a given day can lead to omissions in the report
    plot_start_date = datetime.strptime(report_data['summary']['query_start_date'], "%Y-%m-%dT%H:%M:%S")
    plot_end_date = datetime.strptime(report_data['summary']['query_end_date'], "%Y-%m-%dT%H:%M:%S")

    if plot_end_date > plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0):
        plot_end_date = plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        plot_end_date = plot_end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    plot_date = plot_start_date

    while plot_date <= plot_end_date:
        plot_date_str = plot_date.strftime("%Y-%m-%d")
        days.append(plot_date_str)
        plot_date += timedelta(days=1)

    days = list(set(days))
    days.sort()

    if len(days) == 0:
        logger.info('No accountability data for DSWx-HLS')
        duplicate_count = 0
        report_url = None
    else:
        expected_filename = f'dswx_hls_accountability_{days[0]}_to_{days[-1]}.png' if len(days) > 1 \
            else f'dswx_hls_accountability_{days[0]}.png'

        expected_path = str(args.plot_dir / 'DSWX_HLS_accountability' / expected_filename)

        if os.path.exists(expected_path):
            s3_url = urlparse(args.s3_plot_path)

            s3_bucket = s3_url.netloc
            s3_path = Path(s3_url.path.lstrip('/'))

            s3_key = str(s3_path / 'DSWX_HLS_accountability.png').lstrip('/')

            s3.upload_file(expected_path, s3_bucket, s3_key)
            logger.info(f'Uploaded DSWx-HLS accountability plot to s3://{s3_bucket}/{s3_key}')

            try:
                os.unlink(expected_path)
            except:
                ...
        else:
            logger.error(f'Expected plot for DSWX_HLS accountability ({expected_path}) does not exist')

        with TemporaryDirectory() as temp_dir:
            s3_url = urlparse(args.s3_report_path)

            s3_bucket = s3_url.netloc
            s3_path = Path(s3_url.path.lstrip('/'))

            report_filename = f'OPERA_DSWx_HLS_accountability_{days[0]}_to_{days[-1]}.txt'
            report_path = os.path.join(temp_dir, report_filename)
            report_key = str(s3_path / 'DSWX_HLS' / f'{report_date}' / report_filename).lstrip('/')

            with open(report_path, 'w') as f:
                for missing in sorted(report_data['hls_missing_dswx']):
                    f.write(f'{missing}\n')

            s3.upload_file(report_path, s3_bucket, report_key)

        duplicate_count = len(report_data['hls_missing_dswx'])
        report_url = f's3://{s3_bucket}/{report_key}'

    es_doc = {
        '@timestamp': report_date,
        'report_time': report_date,
        'report_id': f'{report_date}-DSWX_HLS',
        'start_date': start_date,
        'end_date': end_date,
        'product': 'DSWX_HLS',
        'report_url': report_url,
        'missing_granules': duplicate_count
    }

    if args.opensearch is not None:
        resp = requests.post(
            f'{args.opensearch.rstrip("/")}/{args.accountability_index}/_doc/{report_date}-DSWX_HLS',
            headers={'Content-Type': 'application/json'},
            data=json.dumps(es_doc)
        )

        resp.raise_for_status()
        logger.info(f'Inserted accountability doc for product DSWX_HLS into Opensearch: {resp.json()}')


def main(args):
    start = datetime.now()
    procs = []

    start_date, end_date = _get_start_end_dates(args)
    report_date = now().strftime('%Y-%m-%d')

    accountability_script_map = {
        'DSWX_HLS': [
            sys.executable, 'dswx-hls/dswx-hls-input-map.py', '-o',
            str(args.report_dir / 'DSWX_HLS_accountability.json'), '-d', str(args.plot_dir / 'DSWX_HLS_accountability'),
            '--start-date', start_date, '--end-date', end_date, '--plot-days'
        ] if args.venue == 'PROD' else None
    }

    for product in args.products:
        report_path = args.report_dir / f'{product}.json'
        report_path.parent.mkdir(parents=True, exist_ok=True)

        report_path = str(report_path)

        logger.info(f'Invoking duplicate search script for product {product}')
        procs.append(subprocess.Popen(
            [sys.executable, 'duplicate_check.py', product, '-o', report_path, '--venue', args.venue,
             '--start-date', start_date, '--end-date', end_date, '--facet', 'dates'],
            stdout=subprocess.PIPE
        ))

        if accountability_script_map.get(product, None) is not None:
            procs.append(subprocess.Popen(accountability_script_map[product], stdout=subprocess.PIPE))

    for proc in procs:
        ret = proc.wait()

        if ret != 0:
            logger.critical('One or more scripts failed. Quitting')
            exit(1)

    logger.info('All scripts complete')

    s3_url = urlparse(args.s3_report_path)
    s3_paths = (s3_url.netloc, Path(s3_url.path))

    if args.opensearch is not None:
        opensearch_url = f'{args.opensearch.rstrip("/")}/{args.duplicate_index}/_doc'
    else:
        opensearch_url = None

    plot_data = {
        'date': report_date,
        'start_date': start_date,
        'end_date': end_date,
        'venue': args.venue,
        'product_counts': {},
        'date_maps': {}
    }

    for product in args.products:
        report_path = args.report_dir / f'{product}.json'
        report_path = str(report_path)

        if os.path.exists(report_path):
            with open(report_path, 'r') as f:
                report: dict = json.load(f)

            logger.info(f'Loaded report for product {product}: {report_path}')
        else:
            report: dict = {'months': {}, 'summary': {'n_granules': 0}, 'dates': {}}
            logger.info(f'No report was produced for product {product}, likely because there were no products in the '
                        f'time window. Initializing an empty report')

        duplicates = []
        date_map = {}

        for date in report['dates']:
            report_acq_date = report['dates'][date]
            for duplicate in report_acq_date['duplicates']:
                duplicates.extend(report_acq_date['duplicates'][duplicate]['duplicate_products'])

            date_map[date] = {
                'products': report_acq_date['n_granules'],
                'duplicates': report_acq_date['n_duplicates'],
                'percent_duplicates': report_acq_date['percent_duplicates'],
            }

        duplicates.sort()

        if len(duplicates) > 0:
            s3_bucket, root_s3_path = s3_paths

            s3_key = (
                    root_s3_path / f'{product}' / f'{report_date}' /
                    f'OPERA_DUPLICATES_{product}_{start_date}_to_{end_date}_checked_{report_date}.txt'
            )
            s3_key = str(s3_key).lstrip('/')

            with TemporaryDirectory() as temp_dir:
                with open(os.path.join(temp_dir, 'duplicates.txt'), 'w') as f:
                    for duplicate in duplicates:
                        f.write(f'{duplicate}\n')

                s3.upload_file(os.path.join(temp_dir, 'duplicates.txt'), s3_bucket, s3_key)
                logger.info(f'Uploaded duplicates report for {product} '
                            f'(len={len(duplicates)}) to s3://{s3_bucket}/{s3_key}')

            es_doc = {
                '@timestamp': report_date,
                'id': report_date,
                'start_date': start_date,
                'end_date': end_date,
                'product': product,
                'report_url': f's3://{s3_bucket}/{s3_key}',
                'duplicate_count': len(duplicates)
            }
        else:
            es_doc = {
                '@timestamp': report_date,
                'report_time': report_date,
                'report_id': f'{report_date}-{product}',
                'start_date': start_date,
                'end_date': end_date,
                'product': product,
                'report_url': None,
                'duplicate_count': 0
            }

        if opensearch_url is not None:
            resp = requests.post(
                f'{opensearch_url}/{report_date}-{product}',
                headers={'Content-Type': 'application/json'},
                data=json.dumps(es_doc)
            )

            resp.raise_for_status()
            logger.info(f'Inserted doc for product {product} into Opensearch: {resp.json()}')

        plot_data['product_counts'][product] = {
            'total_products': report['summary']['n_granules'],
            'duplicates': len(duplicates),
            'percent_duplicates': (len(duplicates) / report['summary']['n_granules'] * 100) if
            report['summary']['n_granules'] > 0 else 0,
        }
        plot_data['date_maps'][product] = date_map

    s3_bucket, root_s3_path = s3_paths

    plot_data_key = str(root_s3_path / 'plot_data.json').lstrip('/')

    plot_data_exists = len(s3.list_objects_v2(Bucket=s3_bucket, Prefix=plot_data_key).get('Contents', [])) != 0

    if plot_data_exists:
        with TemporaryDirectory() as temp_dir:
            s3.download_file(s3_bucket, plot_data_key, os.path.join(temp_dir, 'plot_data.json'))
            with open(os.path.join(temp_dir, 'plot_data.json')) as f:
                existing_plot_data = json.load(f)

        logger.info('Read in existing plot data')

        timeseries_plot_data = existing_plot_data + [plot_data]
    else:
        timeseries_plot_data = [plot_data]

    for i in range(len(timeseries_plot_data) - 1):
        if timeseries_plot_data[i]['date'] == report_date:
            logger.warning('Date already exists in timeseries plot data')
            timeseries_plot_data.pop(i)

    timeseries_plot_data = timeseries_plot_data[:args.plot_length]
    timeseries_plot_data.sort(key=lambda x: x['date'])

    with TemporaryDirectory() as temp_dir:
        with open(os.path.join(temp_dir, 'plot_data.json'), 'w') as f:
            json.dump(timeseries_plot_data, f, indent=2)

        s3.upload_file(os.path.join(temp_dir, 'plot_data.json'), s3_bucket, plot_data_key)

        logger.info(f'Uploaded plot data to s3://{s3_bucket}/{plot_data_key}')

    plot_data_and_save(plot_data, args.plot_dir, args.s3_plot_path)
    plot_timeseries_data_and_save(timeseries_plot_data, args.plot_dir, args.s3_plot_path)

    record_dswx_hls_accountability(args, start_date, end_date)

    logger.info(f'Finished all tasks in {datetime.now() - start}')


if __name__ == '__main__':
    main(get_parser().parse_args())
