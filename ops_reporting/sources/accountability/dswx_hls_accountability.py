import json
import os
import sys
from datetime import datetime
from glob import glob
from importlib.util import find_spec
from io import BytesIO
from typing import Literal, Tuple

from util.exec_util import run_as_subprocess, join_subprocess
from .accountability import Accountability
from ..source import Attachment


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
