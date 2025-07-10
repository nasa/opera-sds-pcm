
import re
from collections import namedtuple
from datetime import datetime, timedelta
from os.path import splitext
from typing import Iterable

import dateutil.parser
from commons.logger import get_logger
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import Collection, ProductType, COLLECTION_TO_PRODUCT_TYPE_MAP
from more_itertools import first_true
from util.dataspace_util import DEFAULT_DOWNLOAD_ENDPOINT
from shapely.geometry import box
from tools.dataspace_s1_download import query, build_query_filter

MAX_CHARS_PER_LINE = 250000
"""The maximum number of characters per line you can display in cloudwatch logs"""

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

PLATFORM_MAP = {
    Collection.S1A_SLC: 'A',
    Collection.S1B_SLC: 'B',
    Collection.S1C_SLC: 'C',
    # Collection.S1D_SLC: 'D',
}


async def async_query_dataspace(args, settings, timerange, now: datetime, verbose=True) -> list:
    logger = get_logger()
    bounding_box = args.bbox

    # Assert that timerange looks like this: 2016-08-22T23:00:00Z
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.start_date)
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.end_date)

    if not COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.SLC:
        raise NotImplementedError(f"Collection {args.collection} is not supported for ESA queries")

    platform = PLATFORM_MAP[args.collection]

    filters = []

    if args.use_temportal:
        filters.extend([f'ContentDate/Start gt {timerange.start_date}',
                        f'ContentDate/Start lt {timerange.end_date}'])
    else:
        filters.extend([f'ModificationDate/Start gt {timerange.start_date}',
                        f'ModificationDate/Start lt {timerange.end_date}'])

        if args.temporal_start_date:
            assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", args.temporal_start_date)
            filters.append(f'ModificationDate/Start gt {args.temporal_start_date}')

    bound_list = [float(b) for b in bounding_box.split(',')]

    if bound_list[1] < -60:
        bound_list[1] = 60.0

    bbox_wkt = box(*bound_list).wkt

    filters.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}')")

    query_params = build_query_filter(
        *filters,
        platforms=(platform,),
        sort_reverse=True
    )

    logger.info('Querying Copernicus OData')

    # TODO: There seems to be a limit of 11k results per query. If we hit that, we'll need to split our temporal ranges
    granules = response_to_cmr_granules(query(query_params))
    search_results_count = len(granules)

    logger.info(f'Query complete. Found {search_results_count:,} granule(s)')

    # TODO: Filtering

    return granules


def response_to_cmr_granules(esa_granules):
    granules = []

    for item in esa_granules:
        granule_name = splitext(item['Name'])[0]

        granules.append({
            "granule_id": f'{granule_name}-SLC',
            "revision_id": 0,
            "provider": 'ESA',
            "production_datetime": item['ModificationDate'],  # TODO: CHECK
            "provider_date": item['PublicationDate'],  # TODO: CHECK
            "temporal_extent_beginning_datetime": item['ContentDate']['Start'],
            "revision_date": item['ModificationDate'],
            "short_name": f'SENTINEL-1{granule_name[2]}',
            "bounding_box": [
                {"lat": point[1], "lon": point[0]}
                for point
                in item["GeoFootprint"]
                .get("coordinates")[0]
            ],
            "related_urls": [
                f'{DEFAULT_DOWNLOAD_ENDPOINT}(${item["Id"]})/$zip',
                f'{DEFAULT_DOWNLOAD_ENDPOINT}(${item["Id"]})/$value',
            ],
            "identifier": None
        })

    return granules
