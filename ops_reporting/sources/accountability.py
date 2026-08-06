from abc import ABC, abstractmethod
import json
import os.path
import sys
import warnings
from datetime import datetime, timedelta
from glob import glob
from importlib.util import find_spec
from io import BytesIO
from math import ceil
from tempfile import TemporaryDirectory
from typing import Tuple, Literal, Type

import matplotlib.pyplot as plt
import numpy as np

from opera_commons.logger import logger
from tools.ops.duplicates.duplicate_check import PRODUCTS
from util.exec_util import run_as_subprocess, join_subprocess
from .source import Source, Attachment


class Accountability(Source, ABC):
    _type = 'ACCOUNTABILITY'

    def __init__(
            self,
            source_id: str,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        if window is None or (window[0] is None or window[1] is None):
            raise ValueError(f'Accountability requires a window to be specified')

        super().__init__(source_id, window, **kwargs)

        self._product = source_id
        self._venue = venue

        self._tmp_dir = None
        self._p = None

    def __enter__(self):
        self._tmp_dir = TemporaryDirectory()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._tmp_dir.cleanup()
        self._tmp_dir = None

    def run(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir has not been created, please run in with block')

        self._run()

    @abstractmethod
    def _run(self):
        ...


class DSWxHLSAccountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DSWx-HLS',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        script_path = find_spec('tools.ops.duplicates.dswx-hls.dswx-hls-input-map').origin

        if self._venue != 'PROD':
            self._errors.append('DSWx-HLS accountability script currently only supports PROD CMR venue')
            return

        cmd = [
            sys.executable,
            '-u',
            script_path,
            '-d', os.path.join(self._tmp_dir.name, 'plot'),
            '--plot-days'
        ]

        if self._window_start is not None:
            cmd.extend(['--start-date', self._window_start.strftime('%Y-%m-%dT%H:%M:%SZ')])
        if self._window_end is not None:
            cmd.extend(['--end-date', self._window_end.strftime('%Y-%m-%dT%H:%M:%SZ')])

        self._p = run_as_subprocess(cmd, self._tmp_dir.name)

    @staticmethod
    def _make_missing_product_list(report_data) -> bytes:
        buf = BytesIO()

        for missing_hls in sorted(report_data['hls_missing_dswx']):
            buf.write(f'{missing_hls}\n'.encode())

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

        with open(os.path.join(self._tmp_dir.name, 'dswx_hls_report.json')) as f:
            report_data = json.load(f)

        self._data = report_data['summary']

        self._attachments.extend([
            Attachment(
                os.path.join(self._tmp_dir.name, 'dswx_hls_report.json'),
                f'accountability_report_{self._product.lower()}.json',
                content_type='application/json',
            ),
            Attachment(
                glob(os.path.join(self._tmp_dir.name, 'plot', '*.png'))[0],
                f'accountability_plot_{self._product.lower()}.png',
                content_type='image/png',
                content_disposition='INLINE',
                content_id=Attachment.get_random_id('img')
            ),

        ])

        if self._data.get('overall_counts', {}).get('hls_to_no_dswx', 0) > 0:
            self._attachments.append(
                Attachment(
                    self._make_missing_product_list(report_data),
                    f'dswx_hls_gaps_{self._product.lower()}.txt',
                    content_type='text/plain',
                )
            )


# TODO: Does this need to be split out for each product?
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

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


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

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


class DISPS1Accountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DISP-S1',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


class DISTS1Accountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DIST-S1',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


class DISPStaticAccountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DISP-S1-STATIC',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


class DSWxNIAccountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'DSWx-NI',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


class TropoAccountability(Accountability):
    def __init__(
            self,
            venue: Literal["PROD", "UAT", "GRQ"],
            window: Tuple[datetime, datetime] | None = None,
            **kwargs
    ):
        super().__init__(
            'TROPO',
            venue,
            window,
            **kwargs
        )

    def _run(self):
        ...

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']


_ACCOUNTABILITY_CLASSES = {
    'dswx-hls': DSWxHLSAccountability,
    'rtc-s1': SLCAccountability,
    'cslc-s1': SLCAccountability,
    'rtc-s1-static': SLCAccountability,
    'cslc-s1-static': SLCAccountability,
    'dswx-s1': DSWxS1Accountability,
    'disp-s1': DISPS1Accountability,
    'disp-s1-static': DISPStaticAccountability,
    'dist-s1': DISTS1Accountability,
    'dswx-ni': DSWxNIAccountability,
    'tropo': TropoAccountability,
}


def get_accountability_cls_for_product(
        product: str,
        venue: Literal["PROD", "UAT", "GRQ"],
        window: Tuple[datetime, datetime] | None = None,
        **kwargs
) -> Accountability:
    product = product.lower().replace('_', '-')

    if product not in _ACCOUNTABILITY_CLASSES:
        raise ValueError(f'Accountability class {product} not recognized. '
                         f'Supported: {list(_ACCOUNTABILITY_CLASSES.keys())} (case-insensitive; '
                         f'_ and - interchangeable)')

    cls = _ACCOUNTABILITY_CLASSES[product]

    if cls == SLCAccountability:
        return cls(product, venue, window, **kwargs)
    else:
        return cls(venue, window, **kwargs)


def get_accountability_products():
    return [
        p.upper().replace('-', '_').replace('DSWX', 'DSWx') for p in _ACCOUNTABILITY_CLASSES.keys()
    ]
