import json
import os.path
import sys
import warnings
from datetime import datetime, timedelta
from importlib.util import find_spec
from io import BytesIO
from math import ceil
from tempfile import TemporaryDirectory
from typing import Tuple, Literal

import matplotlib.pyplot as plt
import numpy as np

from opera_commons.logger import logger
from tools.ops.duplicates.duplicate_check import PRODUCTS
from util.exec_util import run_as_subprocess, join_subprocess
from .source import Source, Attachment


class DuplicatesSource(Source):
    """Source class for duplicates detection"""

    _type = 'DUPLICATES'

    def __init__(
            self,
            source_id: str,
            venue: Literal["PROD", "UAT", "GRQ"],
            use_revision: bool = False,
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        if window is None or (window[0] is None or window[1] is None):
            raise ValueError(f'Duplicates detection requires a window to be specified')

        super().__init__(source_id, window, **kwargs)

        if source_id not in PRODUCTS:
            raise ValueError(f'Unsupported product type: {source_id}')

        if venue not in {"PROD", "UAT", "GRQ"}:
            raise ValueError("Venue must be one of PROD, UAT or GRQ")

        self._product = source_id
        self._venue = venue
        self._use_revision = use_revision

        self._p = None
        self._tmp_dir = None

    def __enter__(self):
        self._tmp_dir = TemporaryDirectory()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.cleanup()
        self._tmp_dir = None

    def run(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir has not been created, please run in with block')

        script_path = find_spec('tools.ops.duplicates.duplicate_check').origin

        cmd = [
            sys.executable,
            '-u',
            script_path,
            self._product,
            '--venue', self._venue,
            '--facet', 'dates'
        ]

        if self._window_start is not None:
            cmd.extend(['--start-date', self._window_start.strftime('%Y-%m-%dT%H:%M:%SZ')])
        if self._window_end is not None:
            cmd.extend(['--end-date', self._window_end.strftime('%Y-%m-%dT%H:%M:%SZ')])

        if self._venue == 'GRQ':
            try:
                from hysds.celery import app
                es_url = app.conf.get('GRQ_ES_URL')
                cmd.extend(['--grq-url', es_url])
            except Exception as e:
                raise RuntimeError("Unable to determine GRQ URL") from e

        if self._use_revision:
            cmd.append('--use-revision')

        self._p = run_as_subprocess(cmd, self._tmp_dir.name)

    def _make_plot(self, report_data) -> bytes:
        logger.info(f'Building duplicates plot for {self._product}')

        start_date = self._window_start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = self._window_end

        # If end date is at midnight UTC, it's very likely we'll have no granules in that date, so let's drop it from the
        # plot.
        if end_date > end_date.replace(hour=0, minute=0, second=0, microsecond=0):
            end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

        date_map = {}

        for date in report_data['dates']:
            report_acq_date = report_data['dates'][date]

            date_map[date] = {
                'products': report_acq_date['n_granules'],
                'duplicates': report_acq_date['n_duplicates'],
                'percent_duplicates': report_acq_date['percent_duplicates'],
            }

        report_acquisition_dates = []

        date = start_date
        while date <= end_date:
            report_acquisition_dates.append(date.strftime('%Y-%m-%d'))
            date += timedelta(days=1)

        report_acquisition_dates = set(report_acquisition_dates)

        product_dates = set(date_map.keys())
        days = list(product_dates | report_acquisition_dates)
        days.sort()

        x = np.arange(len(days))

        product_data = {
            'total_products': tuple([date_map.get(date, {}).get('products', 0) for date in days]),
            'duplicate_products': tuple(
                [date_map.get(date, {}).get('duplicates', 0) for date in days]
            ),
            'duplicate_percent': tuple(
                [date_map.get(date, {}).get('percent_duplicates', 0) for date in days]
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

        ax.set_title(f'Product counts for {self._product} from {days[0]} to {days[-1]}', fontsize=14)

        ymax = max(product_data['total_products'])
        if ymax > 0:
            ymax = ceil(ymax * 1.2)
        else:
            ymax = 1

        ax.set_ylim(bottom=0, top=ymax)

        ax.legend(['Total Product Count', 'Duplicate Product Count'], fontsize=12)

        buf = BytesIO()
        plt.savefig(buf)

        logger.info(f'Generated duplicates plot for {self._product}')

        return buf.getvalue()

    def _make_dupe_list(self, report_data) -> bytes:
        logger.info(f'Generating duplicate list for {self._product}')

        duplicates = []

        for date in report_data['dates']:
            report_acq_date = report_data['dates'][date]
            for duplicate in report_acq_date['duplicates']:
                duplicates.extend(report_acq_date['duplicates'][duplicate]['duplicate_products'])

        duplicates.sort()

        buf = BytesIO()

        for duplicate in duplicates:
            buf.write(f'{duplicate}\n'.encode())

        return buf.getvalue()

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        status, stdout, stderr = join_subprocess(self._p)

        if status != 0:
            self._data = {}
            self._errors.append(stderr)
            return

        with open(os.path.join(self._tmp_dir.name, 'duplicate_report.json')) as f:
            report_data = json.load(f)

        self._data = report_data['summary']

        self._attachments.extend([
            Attachment(
                os.path.join(self._tmp_dir.name, 'duplicate_report.json'),
                f'duplicates_report_{self._product.lower()}.json',
                content_type='application/json',
            ),
            Attachment(
                self._make_plot(report_data),
                f'duplicates_plot_{self._product.lower()}.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),

        ])

        if self._data['n_duplicates'] > 0:
            self._attachments.append(
                Attachment(
                    self._make_dupe_list(report_data),
                    f'duplicates_list_{self._product.lower()}.txt',
                    content_type='text/plain',
                )
            )
