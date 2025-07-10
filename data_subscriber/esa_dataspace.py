
import re
from collections import namedtuple
from datetime import datetime, timedelta
from typing import Iterable

import dateutil.parser
from commons.logger import get_logger
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import Collection, ProductType, COLLECTION_TO_PRODUCT_TYPE_MAP
from more_itertools import first_true
# from util.dataspace_util import
from tools.dataspace_s1_download import query, build_query_filter

MAX_CHARS_PER_LINE = 250000
"""The maximum number of characters per line you can display in cloudwatch logs"""

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


async def async_query_dataspace(args, settings, timerange, now: datetime, verbose=True) -> list:
    logger = get_logger()
    bounding_box = args.bbox

    # Assert that timerange looks like this: 2016-08-22T23:00:00Z
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.start_date)
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.end_date)

    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.SLC:
        ...
    else:
        raise NotImplementedError(f"Collection {args.collection} is not supported for ESA queries")

