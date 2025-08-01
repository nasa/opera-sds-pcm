
import re
from collections import namedtuple
from datetime import datetime, timedelta
from os.path import splitext
from typing import Iterable

import dateutil.parser
from opera_commons.logger import get_logger
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import Collection, ProductType, COLLECTION_TO_PRODUCT_TYPE_MAP
from more_itertools import first_true
from util.dataspace_util import DEFAULT_DOWNLOAD_ENDPOINT
from shapely.geometry import box
from tools.dataspace_s1_download import query, build_query_filter, ISO_TIME

MAX_CHARS_PER_LINE = 250000
"""The maximum number of characters per line you can display in cloudwatch logs"""

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

PLATFORM_MAP = {
    Collection.S1A_SLC: 'A',
    Collection.S1B_SLC: 'B',
    Collection.S1C_SLC: 'C',
    # Collection.S1D_SLC: 'D',
}

MAX_DATASPACE_QUERY_RESPONSE_SIZE = 11000


ESA_SAFE_NAME_REGEX = re.compile(r'(?P<mission_id>S1A|S1B|S1C)_(?P<beam_mode>IW)_(?P<product_type>SLC)(?P<resolution>_)'
                                 r'_(?P<level>1)(?P<class>S)(?P<pol>SH|SV|DH|DV)_(?P<start_ts>(?P<start_year>\d{4})'
                                 r'(?P<start_month>\d{2})(?P<start_day>\d{2})T(?P<start_hour>\d{2})(?P<start_minute>'
                                 r'\d{2})(?P<start_second>\d{2}))_(?P<stop_ts>(?P<stop_year>\d{4})(?P<stop_month>\d{2})'
                                 r'(?P<stop_day>\d{2})T(?P<stop_hour>\d{2})(?P<stop_minute>\d{2})'
                                 r'(?P<stop_second>\d{2}))_(?P<orbit_num>\d{6})_(?P<data_take_id>[0-9A-F]{6})_'
                                 r'(?P<product_id>[0-9A-F]{4})[.](?P<format>SAFE)$')


async def async_query_dataspace(args, settings, timerange, now: datetime, verbose=True) -> list:
    logger = get_logger()

    query_params = _get_query_params(args, timerange)

    logger.info('Querying Copernicus OData')

    query_granules = query(query_params)
    granules = query_granules

    while len(query_granules) == MAX_DATASPACE_QUERY_RESPONSE_SIZE:
        logger.warning('Potentially exceeded maximum result size of dataspace query. Splitting')

        if args.use_temporal:
            new_end_time = min([
                datetime.strptime(
                    g['ContentDate']['Start'],
                    '%Y-%m-%dT%H:%M:%S.%fZ'
                ) for g in granules
            ]).strftime(ISO_TIME)
        else:
            new_end_time = min([
                datetime.strptime(
                    g['ModificationDate'],
                    '%Y-%m-%dT%H:%M:%S.%fZ'
                ) for g in granules
            ]).strftime(ISO_TIME)

        logger.info(f'New end time: {new_end_time}')

        new_timerange = DateTimeRange(timerange.start_date, new_end_time)
        new_query_params = _get_query_params(args, new_timerange)

        query_granules = query(new_query_params)
        granules.extend(query_granules)

    granules = response_to_cmr_granules(granules)
    search_results_count = len(granules)

    logger.info(f'Query complete. Found {search_results_count:,} granule(s)')

    # TODO: Filtering

    # TODO: Not sure if this is needed. The query doesn't give us file extensions & we already narrow down to IW
    #  but this field is used. Maybe I should just hardcode it below?
    for granule in granules:
        granule["filtered_urls"] = granule['related_urls']

    return granules


def _get_query_params(args, timerange):
    bounding_box = args.bbox

    # Assert that timerange looks like this: 2016-08-22T23:00:00Z
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.start_date)
    assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", timerange.end_date)

    if not COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.SLC:
        raise NotImplementedError(f"Collection {args.collection} is not supported for ESA queries")

    platform = PLATFORM_MAP[args.collection]

    filters = []

    if args.use_temporal:
        filters.extend([f'ContentDate/End ge {timerange.start_date}',
                        f'ContentDate/Start le {timerange.end_date}'])
    else:
        filters.extend([f'ModificationDate ge {timerange.start_date}',
                        f'ModificationDate le {timerange.end_date}'])

        if args.temporal_start_date:
            assert re.fullmatch("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", args.temporal_start_date)
            filters.append(f'ContentDate/End ge {args.temporal_start_date}')

    bound_list = [float(b) for b in bounding_box.split(',')]

    if bound_list[1] < -60:
        bound_list[1] = -60.0

    bbox_wkt = box(*bound_list).wkt

    filters.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}')")

    if args.native_id:
        native_id_arg = str(args.native_id).removesuffix('-SLC')

        if not native_id_arg.endswith('.SAFE'):
            native_id_arg += '.SAFE'

        if not ESA_SAFE_NAME_REGEX.fullmatch(native_id_arg):
            raise ValueError('Native ID has incorrect format. Must be the full S1-SLC SAFE file id, with or without'
                             'the .SAFE extension. No wildcards are supported.')

        filters.append(f"Name eq '{native_id_arg}'")

    query_params = build_query_filter(
        *filters,
        platforms=(platform,),
        sort_reverse=True
    )

    return query_params



def response_to_cmr_granules(esa_granules):
    granules = []

    for item in esa_granules:
        granule_name = splitext(item['Name'])[0]

        footprint_type = item["GeoFootprint"].get('type')

        if footprint_type == 'Polygon':
            bbox = [
                {"lat": point[1], "lon": point[0]}
                for point
                in item["GeoFootprint"]['coordinates'][0]
            ]
        elif footprint_type == 'MultiPolygon':
            bbox = [
                {"lat": point[1], "lon": point[0]}
                for point
                in item["GeoFootprint"]['coordinates'][0][0]
            ]

        granules.append({
            "granule_id": f'{granule_name}-SLC',
            "revision_id": 0,
            "provider": 'ESA',
            "production_datetime": item['ModificationDate'],  # TODO: CHECK
            "provider_date": item['PublicationDate'],  # TODO: CHECK
            "temporal_extent_beginning_datetime": item['ContentDate']['Start'],
            "revision_date": item['ModificationDate'],
            "short_name": f'SENTINEL-1{granule_name[2]}',
            "bounding_box": bbox,
            "related_urls": [
                f'{DEFAULT_DOWNLOAD_ENDPOINT}({item["Id"]})/$zip',
                f'{DEFAULT_DOWNLOAD_ENDPOINT}({item["Id"]})/$value',
            ],
            "identifier": None
        })

    return granules
