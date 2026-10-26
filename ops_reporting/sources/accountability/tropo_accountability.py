import json
import os
import sys
from datetime import datetime
from glob import glob
from importlib.util import find_spec
from io import BytesIO
from typing import Literal, Tuple

from opera_commons.logger import logger
from util.exec_util import run_as_subprocess, join_subprocess
from .accountability import Accountability
from ops_reporting.sources import Attachment


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
        script_path = find_spec('tools.ops.cmr_audit.cmr_audit_tropo').origin

        if self._venue != 'PROD':
            self._errors.append('TROPO accountability script currently only supports PROD CMR venue')
            return

        cmd = [
            sys.executable,
            '-u',
            script_path,
            '--start', self._window_start.strftime('%Y-%m-%d'),
            '--end', self._window_end.strftime('%Y-%m-%d'),
        ]

        self._p = run_as_subprocess(cmd, self._tmp_dir.name)

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        status, stdout, stderr = join_subprocess(self._p)

        if status != 0:
            self._data = {}
            self._errors.append(stderr.decode('utf-8'))
            logger.error(f'{self.source_id}: {stderr.decode("utf-8")}')
            return

        counts_file = os.path.join(self._tmp_dir.name, 'tropo_granule_counts.csv')
        dates_file = os.path.join(self._tmp_dir.name, 'tropo_missing_dates.csv')
