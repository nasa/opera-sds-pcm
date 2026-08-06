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
# from ops_reporting.sources.source import Source, Attachment
from ..source import Source


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
