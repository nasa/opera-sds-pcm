import logging
import re
from datetime import datetime
from typing import Iterable

import dateutil.parser
from more_itertools import first_true

from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client
from rtc_utils import rtc_granule_regex
from tools.ops.cmr_audit import cmr_client
from tools.ops.cmr_audit.cmr_client import cmr_requests_get, async_cmr_posts

logger = logging.getLogger(__name__)

COLLECTION_TO_PROVIDER_MAP = {
    "HLSL30": "LPCLOUD",
    "HLSS30": "LPCLOUD",
    "SENTINEL-1A_SLC": "ASF",
    "SENTINEL-1B_SLC": "ASF",
    "OPERA_L2_RTC-S1_V1": "ASF",
    "OPERA_L2_CSLC-S1_V1": "ASF"
}

CMR_COLLECTION_TO_PROVIDER_TYPE_MAP = {
    "HLSL30": "LPCLOUD",
    "HLSS30": "LPCLOUD",
    "SENTINEL-1A_SLC": "ASF",
    "SENTINEL-1B_SLC": "ASF",
    "OPERA_L2_RTC-S1_V1": "ASF-RTC",
    "OPERA_L2_CSLC-S1_V1": "ASF-CSLC"
}

COLLECTION_TO_PRODUCT_TYPE_MAP = {
    "HLSL30": "HLS",
    "HLSS30": "HLS",
    "SENTINEL-1A_SLC": "SLC",
    "SENTINEL-1B_SLC": "SLC",
    "OPERA_L2_RTC-S1_V1": "RTC",
    "OPERA_L2_CSLC-S1_V1": "CSLC"
}


async def async_query_cmr(args, token, cmr, settings, timerange, now: datetime, silent=False) -> list:
    request_url = f"https://{cmr}/search/granules.umm_json"
    bounding_box = args.bbox

    if args.collection == "SENTINEL-1A_SLC" or args.collection == "SENTINEL-1B_SLC":
        bound_list = bounding_box.split(",")

        # Excludes Antarctica
        if float(bound_list[1]) < -60:
            bound_list[1] = "-60"
            bounding_box = ",".join(bound_list)

    params = {
        "sort_key": "-start_date",
        "provider": COLLECTION_TO_PROVIDER_MAP[args.collection],
        "ShortName[]": [args.collection],
        "token": token,
        "bounding_box": bounding_box
    }

    if args.native_id:
        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            match_native_id = re.match(rtc_granule_regex, args.native_id)
            burst_id = mbc_client.product_burst_id_to_mapping_burst_id(match_native_id.group("burst_id"))
            native_ids = mbc_client.get_reduced_rtc_native_id_patterns(mgrs[mgrs["bursts"].str.contains(burst_id)])
            if not native_ids:
                raise Exception(f"The supplied {args.native_id=} is not associated with any MGRS tile collection")
            params["options[native-id][pattern]"] = 'true'
            params["native-id[]"] = native_ids
        else:
            params["native-id[]"] = [args.native_id]

        if any(wildcard in args.native_id for wildcard in ['*', '?']):
            params["options[native-id][pattern]"] = 'true'

    # derive and apply param "temporal"
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    temporal_range = _get_temporal_range(timerange.start_date, timerange.end_date, now_date)
    if not silent:
        logger.info("Temporal Range: " + temporal_range)

    if args.use_temporal:
        params["temporal"] = temporal_range
    else:
        params["revision_date"] = temporal_range

        # if a temporal start-date is provided, set temporal
        if args.temporal_start_date:
            if not silent:
                logger.info(f"{args.temporal_start_date=}")
            params["temporal"] = dateutil.parser.isoparse(args.temporal_start_date).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not silent:
        logger.info(f"Querying CMR. {request_url=} {params=}")
    product_granules = await _async_request_search_cmr_granules(args, request_url, [params])
    logger.info(f"Found {len(product_granules)} granules")

    # Filter out granules with revision-id greater than max allowed
    least_revised_granules = []
    for granule in product_granules:
        if granule['revision_id'] <= args.max_revision:
            least_revised_granules.append(granule)
        else:
            logger.warning(
                f"Granule {granule['granule_id']} currently has revision-id of {granule['revision_id']} "
                f"which is greater than the max {args.max_revision}. "
                "Ignoring and not storing or processing this granule."
            )
    product_granules = least_revised_granules
    logger.info(f"Filtered to {len(product_granules)} granules")

    if args.collection in settings["SHORTNAME_FILTERS"]:
        product_granules = [granule for granule in product_granules if _match_identifier(settings, args, granule)]

    if not silent:
        logger.info(f"Filtered to {len(product_granules)} total granules")

    for granule in product_granules:
        granule["filtered_urls"] = _filter_granules(granule, args)

    return product_granules


def _get_temporal_range(start: str, end: str, now: str):
    start = start if start is not False else "1900-01-01T00:00:00Z"
    end = end if end is not False else now

    return "{},{}".format(start, end)


async def _async_request_search_cmr_granules(args, request_url, paramss: Iterable[dict]):
    response_jsons = await async_cmr_posts(request_url, cmr_client.paramss_to_request_body(paramss))
    return response_jsons_to_cmr_granules(args, response_jsons)


def _request_search_cmr_granules(args, request_url, params):
    response_jsons = cmr_requests_get(args, request_url, params)
    return response_jsons_to_cmr_granules(args, response_jsons)


def response_jsons_to_cmr_granules(args, response_jsons):
    items = [item
             for response_json in response_jsons
             for item in response_json.get("items")]

    collection_identifier_map = {
        "HLSL30": "LANDSAT_PRODUCT_ID",
        "HLSS30": "PRODUCT_URI"
    }

    granules = []
    for item in items:
        if item["umm"]["TemporalExtent"].get("RangeDateTime"):
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        else:
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["SingleDateTime"]

        granules.append({
            "granule_id": item["umm"].get("GranuleUR"),
            "revision_id": item.get("meta").get("revision-id"),
            "provider": item.get("meta").get("provider-id"),
            "production_datetime": item["umm"].get("DataGranule").get("ProductionDateTime"),
            "temporal_extent_beginning_datetime": temporal_extent_beginning_datetime,
            "revision_date": item["meta"]["revision-date"],
            "short_name": item["umm"].get("Platforms")[0].get("ShortName"),
            "bounding_box": [
                {"lat": point.get("Latitude"), "lon": point.get("Longitude")}
                for point
                in item["umm"]
                .get("SpatialExtent")
                .get("HorizontalSpatialDomain")
                .get("Geometry")
                .get("GPolygons")[0]
                .get("Boundary")
                .get("Points")
            ],
            "related_urls": [url_item.get("URL") for url_item in item["umm"].get("RelatedUrls")],
            "identifier": next(
                attr.get("Values")[0]
                for attr in item["umm"].get("AdditionalAttributes")
                if attr.get("Name") == collection_identifier_map[args.collection]
            ) if args.collection in collection_identifier_map else None
        })
    return granules


def _filter_granules(granule, args):
    collection_to_extensions_filter_map = {
        "HLSL30": ["B02.tif", "B03.tif", "B04.tif", "B05.tif", "B06.tif", "B07.tif", "Fmask.tif"],
        "HLSS30": ["B02.tif", "B03.tif", "B04.tif", "B8A.tif", "B11.tif", "B12.tif", "Fmask.tif"],
        "SENTINEL-1A_SLC": ["zip"],
        "SENTINEL-1B_SLC": ["zip"],
        "OPERA_L2_RTC-S1_V1": ["tif", "h5"],
        "OPERA_L2_CSLC-S1_V1": ["h5"],
        "DEFAULT": ["tif"]
    }

    filter_extension_key = first_true(collection_to_extensions_filter_map.keys(), pred=lambda x: x == args.collection, default="DEFAULT")

    return [
        url
        for url in granule.get("related_urls")
        for extension in collection_to_extensions_filter_map.get(filter_extension_key)
        if url.endswith(extension)
    ]


def _match_identifier(settings, args, granule) -> bool:
    for filter in settings["SHORTNAME_FILTERS"][args.collection]:
        if re.match(filter, granule["identifier"]):
            return True

    return False
