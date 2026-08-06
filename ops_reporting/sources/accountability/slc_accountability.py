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
