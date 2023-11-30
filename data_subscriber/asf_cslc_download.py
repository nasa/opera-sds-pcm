import concurrent.futures
import logging
import os
from collections import defaultdict
from pathlib import PurePath, Path

import requests
import requests.utils
from more_itertools import partition

from data_subscriber.asf_download import DaacDownloadAsf
from data_subscriber.url import _has_url, _to_url, _to_https_url, _rtc_url_to_chunk_id

logger = logging.getLogger(__name__)


class AsfDaacCslcDownload(DaacDownloadAsf):
    pass

