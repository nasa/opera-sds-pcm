import asyncio
import itertools
import re
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from pathlib import Path

import dateutil.parser
from more_itertools import first, last

from data_subscriber.cmr import async_query_cmr, COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.geojson_utils import localize_include_exclude, filter_granules_by_regions
from data_subscriber.query import CmrQuery, get_query_timerange
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client, evaluator
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from data_subscriber.url import determine_acquisition_cycle
from geo.geo_util import does_bbox_intersect_region
from rtc_utils import rtc_granule_regex

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

    def run_query(self):
        pass