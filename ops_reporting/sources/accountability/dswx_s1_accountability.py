import json
import os
import shutil
import sqlite3
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, timedelta
from functools import cache
from glob import glob
from io import BytesIO
from itertools import chain
from math import ceil
from tempfile import TemporaryDirectory
from typing import Literal, Tuple

import numpy as np
from matplotlib import pyplot as plt

from opera_commons.logger import logger
from util.exec_util import run_as_subprocess, join_subprocess
from .accountability import Accountability
from ..source import Attachment

OPS_REPO = 'https://github.com/nasa/opera-sds-ops.git'
ACQUISITION_DATE_INDEX = 4


class _AccountabilityScriptResults:
    def __init__(
            self,
            coverage_file,
            reduced_mapping_file,
            tile_set_accountability_file,
            raw_accountability_file,
            db_file
    ):
        self._coverage_file = coverage_file
        self._reduced_mapping_file = reduced_mapping_file
        self._tile_set_accountability_file = tile_set_accountability_file
        self._raw_accountability_file = raw_accountability_file
        self._db_file = db_file

        self._coverage = None
        self._reduced_mapping = None
        self._tile_set_accountability = None
        self._raw_accountability = None

    @property
    def coverage(self):
        if self._coverage is None:
            with open(self._coverage_file) as fp:
                self._coverage = json.load(fp)
        return self._coverage

    @property
    def reduced_mapping(self):
        if self._reduced_mapping is None:
            with open(self._reduced_mapping_file) as fp:
                self._reduced_mapping = json.load(fp)
        return self._reduced_mapping

    @property
    def tile_set_accountability(self):
        if self._tile_set_accountability is None:
            with open(self._tile_set_accountability_file) as fp:
                self._tile_set_accountability = json.load(fp)
        return self._tile_set_accountability

    @property
    def raw_accountability(self):
        if self._raw_accountability is None:
            with open(self._raw_accountability_file) as fp:
                self._raw_accountability = json.load(fp)
        return self._raw_accountability

    @property
    def db_file(self):
        return self._db_file

    @property
    def coverage_file(self):
        return self._coverage_file

    @property
    def reduced_mapping_file(self):
        return self._reduced_mapping_file


class DSWxS1Accountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DSWx-S1',
            venue,
            window,
            **kwargs
        )

        self._executor = None
        self._future: Future = None

    def __enter__(self):
        self._tmp_dir = TemporaryDirectory()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='DSWx-S1-Accountability-')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.cleanup()
        self._tmp_dir = None

        self._executor.shutdown(wait=True)
        self._executor = None

    def _run(self):
        if self._venue != 'PROD':
            self._errors.append('DSWx-HLS accountability script currently only supports PROD CMR venue')
            return

        self._future = self._executor.submit(self._dswx_s1_scripts)

    def _dswx_s1_scripts(self):
        logger.info('Cloning OPS repo')

        p = run_as_subprocess(
            ['git', 'clone', OPS_REPO, 'ops_repo'],
            self._tmp_dir.name
        )

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'git clone'

        logger.info('Copying DSWx-S1 accountability scripts from repo')

        script_dir = os.path.join(self._tmp_dir.name, 'script')

        shutil.copytree(
            os.path.join(self._tmp_dir.name, 'ops_repo', 'accountability_tools', 'dswx_s1'),
            script_dir
        )

        logger.info('Cleaning up')

        shutil.rmtree(os.path.join(self._tmp_dir.name, 'ops_repo'))

        logger.info('Running initial product surveys')

        cmd = [
            sys.executable,
            '-u',
            os.path.join(script_dir, 'survey.py'),
        ]

        if self._window_start is not None:
            cmd.extend(['--start-date', self._window_start.strftime('%Y-%m-%dT%H:%M:%SZ')])
        if self._window_end is not None:
            cmd.extend(['--end-date', self._window_end.strftime('%Y-%m-%dT%H:%M:%SZ')])

        p = run_as_subprocess(cmd, script_dir)

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'product surveys'

        logger.info('Running initial RTC-DSWx accountability')

        cmd = [
            sys.executable,
            '-u',
            os.path.join(script_dir, 'accountability.py'),
        ]

        p = run_as_subprocess(cmd, script_dir)

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'accountability'

        logger.info('Mapping missing RTCs to tile sets')

        cmd = [
            sys.executable,
            '-u',
            os.path.join(script_dir, 'missing_rtcs_to_tile_sets.py'),
            '--no-tqdm'
        ]

        p = run_as_subprocess(cmd, script_dir)

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'tile set mapping'

        logger.info('Assigning cycle indices')

        cmd = [
            sys.executable,
            '-u',
            os.path.join(script_dir, 'add_cycle_indices.py'),
            '--no-tqdm'
        ]

        p = run_as_subprocess(cmd, script_dir)

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'cycle index mapping'

        logger.info('Checking final burst coverage')

        cmd = [
            sys.executable,
            '-u',
            os.path.join(script_dir, 'check_burst_coverage.py'),
            '--no-tqdm'
        ]

        p = run_as_subprocess(cmd, script_dir)

        status, _, stderr = join_subprocess(p)

        if status != 0:
            return None, stderr, 'burst coverage check'

        logger.info('Done')

        return (
            _AccountabilityScriptResults(
                os.path.join(script_dir, 'missing_mgrs_sets_by_coverage.json'),
                os.path.join(script_dir, 'missing_rtc_mgrs_set_mappings_with_sufficient_coverage_reduced.json'),
                os.path.join(script_dir, 'missing_mgrs_set_cycle_indices.json'),
                os.path.join(script_dir, 'missing_rtc_products.json'),
                glob(os.path.join(script_dir, '*.sqlite'))[0],
            ),
            None, None
        )

    @staticmethod
    def _count_rtcs_by_sensing_date(rtcs):
        counts_by_date = {}

        for rtc in rtcs:
            date = datetime.strptime(
                rtc.split('_')[ACQUISITION_DATE_INDEX], '%Y%m%dT%H%M%SZ'
            ).strftime('%Y-%m-%d')

            if date not in counts_by_date:
                counts_by_date[date] = 0
            counts_by_date[date] += 1

        return {d: counts_by_date[d] for d in sorted(counts_by_date.keys())}

    def _create_rtc_plots(
            self,
            result: _AccountabilityScriptResults,
            triggerable_unmapped_rtcs,
            potential_product_map
    ) -> dict[str, bytes]:
        plots = {}

        unmapped_date_map = self._count_rtcs_by_sensing_date(result.raw_accountability)
        triggerable_date_map = self._count_rtcs_by_sensing_date(triggerable_unmapped_rtcs)

        tile_set_date_map = {}
        potential_product_date_map = {}

        for tile_set in result.coverage['valid']['tile_sets']:
            tile_set_id = list(tile_set.keys())[0]
            tile_set = tile_set[tile_set_id]

            sensing_date = tile_set['sensing-date']

            if sensing_date not in tile_set_date_map:
                tile_set_date_map[sensing_date] = 0
            tile_set_date_map[sensing_date] += 1

            if sensing_date not in potential_product_date_map:
                potential_product_date_map[sensing_date] = 0
            potential_product_date_map[sensing_date] += potential_product_map[tile_set_id]

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
            list(unmapped_date_map.keys()) + list(triggerable_date_map.keys()) + [window_start_date, window_end_date]
        )
        reported_dates = [datetime.strptime(date, '%Y-%m-%d') for date in sorted(reported_dates)]

        start_date = reported_dates[0]
        end_date = reported_dates[-1]

        date = start_date

        days = set()

        while date <= end_date:
            days.add(date.strftime('%Y-%m-%d'))
            date = date + timedelta(days=1)

        days = list(days)
        days.sort()

        x = np.arange(len(days))

        product_data = {
            'unmapped_rtcs': tuple([
                unmapped_date_map.get(date, 0) for date in days
            ]),
            'triggerable_unmapped_rtcs': tuple([
                triggerable_date_map.get(date, 0) for date in days
            ]),
            'tile_sets': tuple(
                tile_set_date_map.get(date, 0) for date in days
            ),
            'potential_products': tuple(
                potential_product_date_map.get(date, 0) for date in days
            )
        }

        width = 1 / 3
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        for measure, color in zip(['unmapped_rtcs', 'triggerable_unmapped_rtcs'],
                                  ['tab:blue', 'tab:orange']):
            count = product_data[measure]
            offset = width * multiplier
            rects = ax.bar(x + offset, count, width, label=measure, color=color)

            ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)
            multiplier += 1

        ax.set_xlabel('Acquisition date', fontsize=12)
        ax.set_ylabel('Granule Count', fontsize=12)

        ax.set_xticks(x + (width / 2), days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(f'Unmapped RTC -> DSWx-S1 counts from {days[0]} to {days[-1]}', fontsize=14)

        ymax = max(product_data['unmapped_rtcs'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total Unmapped RTC Count', 'Duplicate Triggerable Unmapped RTC Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf)

        logger.info(f'Generated unmapped RTC plot')

        plots['rtcs'] = buf.getvalue()

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        rects = ax.bar(x, product_data['tile_sets'], label='tile_sets', color='tab:orange')
        ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)

        ax.set_xlabel('Acquisition date', fontsize=12)
        ax.set_ylabel('Tile Set Count', fontsize=12)

        ax.set_xticks(x, days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(f'{self._product.upper()} MGRS tiles sets with gaps from {days[0]} to {days[-1]}', fontsize=14)

        ymax = max(product_data['tile_sets'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Tile Set Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf)

        logger.info(f'Generated tile set plot')

        plots['tile_sets'] = buf.getvalue()

        fig, ax = plt.subplots(layout='constrained', figsize=(5 + 1 * len(days), 8))

        rects = ax.bar(x, product_data['potential_products'], label='potential_products', color='tab:orange')
        ax.bar_label(rects, padding=3, fmt='{:,.0f}', fontsize=12, rotation=90)

        ax.set_xlabel('Acquisition date', fontsize=12)
        ax.set_ylabel('Potential missing output product count', fontsize=12)

        ax.set_xticks(x, days, rotation=90)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=UserWarning)
            ax.set_yticklabels([f'{label:,.0f}' for label in ax.get_yticks()])

        ax.set_title(
            f'Possible missing {self._product.upper()} product counts from {days[0]} to {days[-1]}', fontsize=14
        )

        ymax = max(product_data['potential_products'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['DSWx-S1 Product Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf)

        logger.info(f'Generated potential missing product plot')

        plots['potential_products'] = buf.getvalue()

        return plots

    def _create_reduced_native_id_list(self, result: _AccountabilityScriptResults) -> bytes:
        buf = BytesIO()

        for native_id in result.reduced_mapping:
            buf.write(f'{native_id}\n'.encode())

        return buf.getvalue()

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        result: _AccountabilityScriptResults

        result, stderr, stage = self._future.result()

        if result is None:
            self._data = {}
            self._errors.append(
                f'DSWx-S1 accountability scripts failed in stage {stage}: {stderr}'
            )
            return

        total_missing_rtcs = len(result.raw_accountability)
        tile_sets_to_unmapped_rtc_count = {
            ts_id: len(result.tile_set_accountability[ts_id]) for ts_id in result.tile_set_accountability
        }
        triggerable_tile_sets = [
            list(ts.keys())[0] for ts in result.coverage['valid']['tile_sets']
        ]
        triggerable_unmapped_rtcs = list(chain.from_iterable(
            [result.tile_set_accountability[ts_id] for ts_id in triggerable_tile_sets]
        ))
        reduced_native_id_count = len(result.reduced_mapping)

        conn = sqlite3.connect(result.db_file)

        @cache
        def _tile_count_for_tile_set(mgrs_set_id):
            mgrs_set_id = mgrs_set_id.split('$')[0]

            query = """
                SELECT number_of_mgrs_tiles
                FROM mgrs_burst_db
                WHERE mgrs_set_id = ?
                """

            cursor = conn.cursor()
            cursor.execute(query, (mgrs_set_id,))

            row = cursor.fetchone()
            return int(row[0])

        potential_product_map = {}

        for product in triggerable_tile_sets:
            potential_product_map[product] = _tile_count_for_tile_set(product.split('$')[0])

        print(f'{total_missing_rtcs=}\n{tile_sets_to_unmapped_rtc_count=}\n{triggerable_tile_sets=}\n'
              f'{triggerable_unmapped_rtcs=}\n{reduced_native_id_count=}\n{potential_product_map=}')

        self._data = {
            'total_unmapped_rtcs': total_missing_rtcs,
            'total_triggerable_unmapped_rtcs': len(triggerable_unmapped_rtcs),
            'total_affected_tile_sets': len(triggerable_tile_sets),
            'total_reduced_native_id_count': reduced_native_id_count,
            'potential_output_product_count': sum(potential_product_map.values())
        }
        self._attachments.extend([
            Attachment(
                result.coverage_file,
                f'accountability_report_{self._product.lower()}.json',
                content_type='application/json',
            ),
            Attachment(
                result.reduced_mapping_file,
                f'accountability_report_{self._product.lower()}_reduced.json',
                content_type='application/json',
            ),
            Attachment(
                self._create_rtc_plots(result, triggerable_unmapped_rtcs, potential_product_map)['rtcs'],
                f'accountability_plot_{self._product.lower()}_unmapped_rtcs.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),
            Attachment(
                self._create_rtc_plots(result, triggerable_unmapped_rtcs, potential_product_map)['tile_sets'],
                f'accountability_plot_{self._product.lower()}_tile_sets.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),
            Attachment(
                self._create_rtc_plots(result, triggerable_unmapped_rtcs, potential_product_map)['potential_products'],
                f'accountability_plot_{self._product.lower()}_potential_products.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            )
        ])

        if self._data['total_reduced_native_id_count'] > 0:
            self._attachments.append(
                Attachment(
                    self._create_reduced_native_id_list(result),
                    f'accountability_native_ids_{self._product.lower()}.txt',
                    content_type='text/plain',
                )
            )
