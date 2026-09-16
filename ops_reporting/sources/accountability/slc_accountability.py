import warnings
from math import ceil

import boto3
import json
import os
import tarfile
import sys
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Tuple

import numpy as np
from botocore.exceptions import ClientError
from matplotlib import pyplot as plt

from opera_commons.logger import logger
from util.conf_util import SettingsConf
from util.exec_util import run_as_subprocess, join_subprocess
from .accountability import Accountability
from ..source import Attachment


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
BAT_SCRIPT = ROOT_DIR / 'tools' / 'ops' / 'cmr_audit' / 'cmr_audit_burst_coverage.py'
CACHE_FILE = 'burst_accountability_cache.tar.gz'
GEOJSON_FILE = {
    'rtc-s1': 'global_land_no_antarctica.geojson',
    'cslc-s1': 'north_america_opera_expanded.geojson'
}

VENV_REQUIREMENTS = [
    'aiohttp',
    'python-dateutil',
    'more_itertools',
    'backoff',
    'requests',
    'boto3',
    'shapely'
]


class SLCAccountability(Accountability):
    def __init__(
            self,
            product: str,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            product,
            venue,
            window,
            **kwargs
        )

        self._executor = None
        self._future: Future = None

        self._settings = SettingsConf().cfg

        lts = self._settings.get('LTS_BUCKET')

        if not lts or lts == '{{ LTS_BUCKET }}':
            lts = os.environ.get('LTS_BUCKET')

        self._lts = lts
        self._s3 = boto3.client('s3')

        self._clear_cache = kwargs.get('clear_cache', False)
        self._product_result = None

    def __enter__(self):
        self._tmp_dir = TemporaryDirectory()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='DSWx-S1-Accountability-')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.cleanup()
        self._tmp_dir = None

        self._executor.shutdown(wait=True)
        self._executor = None

    def _run(self):
        if self._venue == 'UAT':
            self._errors.append(f'{self._venue} accountability script currently does not support the CMR UAT venue')
            return

        if not self._lts:
            self._errors.append(f'No LTS bucket defined - cannot cache!')

        self._future = self._executor.submit(self._slc_accountability_scripts)

    def _slc_accountability_scripts(self):
        logger.info('Initializing virtual environment')

        p = run_as_subprocess(
            [sys.executable, '-m', 'pip', 'install'] + VENV_REQUIREMENTS,
            self._tmp_dir.name
        )

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'venv dependencies install'

        cache_dir = os.path.join(self._tmp_dir.name, 'cache')
        os.makedirs(cache_dir, exist_ok=True)

        if self._lts:
            logger.info('Retrieving request cache')

            try:
                self._s3.head_object(
                    Bucket=self._lts,
                    Key=CACHE_FILE
                )
                self._s3.download_file(
                    self._lts, CACHE_FILE, os.path.join(self._tmp_dir.name, CACHE_FILE)
                )
                logger.info(f'Got cache from s3://{self._lts}/{CACHE_FILE}')
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    logger.warning(f'could not access cache: {e}')
                    self._errors.append(f'WARNING: could not access cache: {e}')
                else:
                    logger.info('No cache found, initializing a new one')

            if os.path.isfile(os.path.join(self._tmp_dir.name, CACHE_FILE)):
                with tarfile.open(os.path.join(self._tmp_dir.name, CACHE_FILE), 'r') as tar:
                    tar.extractall(path=cache_dir)

        logger.info(f'Running audit tool for {self._product} against {self._venue}')

        env = os.environ.copy()
        env['PYTHONPATH'] = str(ROOT_DIR)

        cmd = [
            sys.executable, str(BAT_SCRIPT), '--start-datetime', self._window_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            '--end-datetime', self._window_end.strftime('%Y-%m-%dT%H:%M:%SZ'), '--geojson',
            str(ROOT_DIR / f'geo/data/{GEOJSON_FILE[self._product]}'),
            '--do-rtc', 'true' if self._product == 'rtc-s1' else 'false',
            '--do-cslc', 'true' if self._product == 'cslc-s1' else 'false',
            '--output', 'burst_accountability.json',
            '--cache-dir', cache_dir,
            '--zero-on-missing'
        ]

        if self._venue == 'GRQ':
            cmd.extend(['--coverage-target', 'GRQ'])

        if self._clear_cache:
            cmd.extend(['--clear-cache-namespace', 'cmr_opera'])

        logger.info(cmd)

        p = run_as_subprocess(
            cmd,
            self._tmp_dir.name,
            env=env
        )

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'audit tool failed'

        if self._lts:
            logger.info('Pushing request cache')

            with tarfile.open(os.path.join(self._tmp_dir.name, CACHE_FILE), 'w:gz') as tar:
                for root, dirs, files in os.walk(cache_dir):
                    for file in files:
                        path = os.path.join(root, file)
                        tar.add(path, arcname=path.removeprefix(cache_dir))

            try:
                self._s3.upload_file(
                    os.path.join(self._tmp_dir.name, CACHE_FILE),
                    self._lts,
                    CACHE_FILE
                )
                logger.info('Successfully pushed audit tool cache')
            except Exception as e:
                logger.warning(f'failed to upload cache: {e}')

        return os.path.join(self._tmp_dir.name, 'burst_accountability.json'), None, None

    @staticmethod
    def _count_by_sensing_date(slc_list):
        counts_by_date = {}

        for slc in slc_list:
            date = datetime.strptime(
                slc['acquisition_time'].split('+')[0],
                '%Y-%m-%dT%H:%M:%S'
            ).strftime('%Y-%m-%d')

            if date not in counts_by_date:
                counts_by_date[date] = 0
            counts_by_date[date] += 1

        return {d: counts_by_date[d] for d in sorted(counts_by_date.keys())}

    @staticmethod
    def _slc_set_from_burst_list(burst_list):
        d = {b['slc_native_id']: b['acquisition_time'] for b in burst_list}
        return list(dict(slc_native_id=k, acquisition_time=v) for k, v in d.items())

    @staticmethod
    def combine_slc_lists(*slc_source: 'SLCAccountability') -> list[Attachment]:
        combined_burst_list = []

        for source in slc_source:
            combined_burst_list.extend(source._product_result['missing'])

        combined_burst_list = SLCAccountability._slc_set_from_burst_list(combined_burst_list)

        if len(combined_burst_list) == 0:
            return []

        native_ids_by_sensor = {}

        for burst in combined_burst_list:
            native_id = burst['slc_native_id']
            sensor = native_id[:3]

            if sensor not in native_ids_by_sensor:
                native_ids_by_sensor[sensor] = [native_id]
            else:
                native_ids_by_sensor[sensor].append(native_id)

        attachments = []

        for sensor in sorted(native_ids_by_sensor.keys()):
            buf = BytesIO()

            for native_id in native_ids_by_sensor[sensor]:
                buf.write(f'{native_id}\n'.encode())

            attachments.append(Attachment(
                buf.getvalue(),
                f'accountability_native_ids_s1slc_combined_{sensor.lower()}.txt',
                content_type='text/plain',
            ))

        return attachments

    def _create_plot(self):
        plots = {}

        missing_bursts = self._product_result['missing']
        found_bursts = self._product_result['found']

        missing_burst_dates = self._count_by_sensing_date(missing_bursts)
        total_burst_dates = self._count_by_sensing_date(missing_bursts + found_bursts)

        missing_slc_dates = self._count_by_sensing_date(self._slc_set_from_burst_list(missing_bursts))
        found_slc_dates = self._count_by_sensing_date(self._slc_set_from_burst_list(found_bursts))
        total_slc_dates = self._count_by_sensing_date(self._slc_set_from_burst_list(missing_bursts + found_bursts))

        window_start_date = self._window_start.replace(hour=0, minute=0, second=0, microsecond=0)
        window_end_date = self._window_end

        # If end date is at midnight UTC, it's very likely we'll have no granules in that date,
        # so let's drop it from the plot.
        if window_end_date > window_end_date.replace(hour=0, minute=0, second=0, microsecond=0):
            window_end_date = window_end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            window_end_date = window_end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

        window_start_date = window_start_date.strftime('%Y-%m-%d')
        window_end_date = window_end_date.strftime('%Y-%m-%d')

        reported_dates = set(
            list(missing_slc_dates.keys()) + list(found_slc_dates.keys()) + [window_start_date, window_end_date]
        )
        print(reported_dates)
        reported_dates = [datetime.strptime(date, '%Y-%m-%d') for date in sorted(reported_dates)]

        start_date = reported_dates[0]
        end_date = reported_dates[-1]

        date = start_date
        days = set()

        while date <= end_date:
            days.add(date.strftime('%Y-%m-%d'))
            date += timedelta(days=1)

        days = list(days)
        days.sort()

        x = np.arange(len(days))

        product_data = {
            'total_bursts': tuple([
                total_burst_dates.get(date, 0) for date in days
            ]),
            'missing_bursts': tuple([
                missing_burst_dates.get(date, 0) for date in days
            ]),
            'total_slcs': tuple([
                total_slc_dates.get(date, 0) for date in days
            ]),
            'missing_slcs': tuple([
                missing_slc_dates.get(date, 0) for date in days
            ]),
        }

        width = 1 / 3
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        for measure, color in zip(['total_bursts', 'missing_bursts'],
                                  ['tab:blue', 'tab:orange']):
            count = product_data[measure]
            offset = width * multiplier
            rects = ax.bar(x + offset, count, width, label=measure, color=color)

            ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)
            multiplier += 1

        ax.set_xlabel('Acquisition date', fontsize=12)
        ax.set_ylabel('Burst Count', fontsize=12)

        ax.set_xticks(x + (width / 2), days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(
            f'{self._product.upper().replace("_", "-")} Burst counts from {days[0]} to {days[-1]}',
            fontsize=14
        )

        ymax = max(product_data['total_bursts'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total Burst Count', 'Missing Burst Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf, format='png')

        logger.info('Generated burst plot')
        plots['burst'] = buf.getvalue()

        width = 1 / 3
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        for measure, color in zip(['total_slcs', 'missing_slcs'],
                                  ['tab:blue', 'tab:orange']):
            count = product_data[measure]
            offset = width * multiplier
            rects = ax.bar(x + offset, count, width, label=measure, color=color)

            ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)
            multiplier += 1

        ax.set_xlabel('Acquisition date', fontsize=12)
        ax.set_ylabel('SLC Count', fontsize=12)

        ax.set_xticks(x + (width / 2), days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(
            f'{self._product.upper().replace("_", "-")} input SLC counts from {days[0]} to {days[-1]}',
            fontsize=14
        )

        ymax = max(product_data['total_slcs'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total SLC Count', 'Missing SLC Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf, format='png')

        logger.info('Generated SLC plot')
        plots['slc'] = buf.getvalue()

        return plots

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        result_file, stderr, stage = self._future.result()

        with open(result_file) as f:
            result = json.load(f)

        if result is None:
            self._data = {}
            self._errors.append(
                f'{self._product.upper()} accountability scripts failed in stage {stage}: {stderr}'
            )
            return

        self._product_result = result['products'][self._product.upper().replace('_', '-')]

        self._data = {
            'expected_burst_count': self._product_result['expected_count'],
            'found_burst_count': self._product_result['found_count'],
            'missing_burst_count': self._product_result['missing_count'],
            'coverage_percent': self._product_result['coverage_percent'],
        }

        plots = self._create_plot()

        self._attachments.extend([
            Attachment(
                result_file,
                f'accountability_report_{self._product.lower()}.json',
                content_type='application/json',
            ),
            Attachment(
                plots['burst'],
                f'accountability_plot_{self._product.lower()}_bursts.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),
            Attachment(
                plots['slc'],
                f'accountability_plot_{self._product.lower()}_slcs.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),
        ])
