from abc import ABC, abstractmethod
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Tuple, Literal

from ops_reporting.sources import Source


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
