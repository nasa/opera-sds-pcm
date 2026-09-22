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
        if self._venue != 'PROD':
            self._errors.append('DIST-S1 accountability script currently only supports PROD CMR venue')
            return

    def _join(self):
        if self._tmp_dir is None:
            raise RuntimeError('Script temp dir appears to have been deleted, please stay within the with block '
                               'until join')

        # TODO: Implement and remove fixed values
        self._data = {}
        self._attachments = []
        self._errors = ['Not implemented']
