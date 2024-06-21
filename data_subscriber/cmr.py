#!/usr/bin/env python3

import logging
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Iterable
from collections import namedtuple
import netrc

import dateutil.parser
from more_itertools import first_true

from data_subscriber.aws_token import supply_token
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client
from rtc_utils import rtc_granule_regex
from tools.ops.cmr_audit import cmr_client
from tools.ops.cmr_audit.cmr_client import cmr_requests_get, async_cmr_posts

logger = logging.getLogger(__name__)
MAX_CHARS_PER_LINE = 250000 #This is the maximum number of characters per line you can display in cloudwatch logs

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

class Collection(str, Enum):
    HLSL30 = "HLSL30"
    HLSS30 = "HLSS30"
    S1A_SLC = "SENTINEL-1A_SLC"
    S1B_SLC = "SENTINEL-1B_SLC"
    RTC_S1_V1 = "OPERA_L2_RTC-S1_V1"
    CSLC_S1_V1 = "OPERA_L2_CSLC-S1_V1"
    CSLC_S1_STATIC_V1 = "OPERA_L2_CSLC-S1-STATIC_V1"

class Endpoint(str, Enum):
    OPS = "OPS"
    UAT = "UAT"

class Provider(str, Enum):
    LPCLOUD = "LPCLOUD"
    ASF = "ASF"
    ASF_SLC = "ASF-SLC"
    ASF_RTC = "ASF-RTC"
    ASF_CSLC = "ASF-CSLC"
    ASF_CSLC_STATIC = "ASF-CSLC-STATIC"

class ProductType(str, Enum):
    HLS = "HLS"
    SLC = "SLC"
    RTC = "RTC"
    CSLC = "CSLC"
    CSLC_STATIC = "CSLC_STATIC"

CMR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

COLLECTION_TO_PROVIDER_MAP = {
    Collection.HLSL30: Provider.LPCLOUD.value,
    Collection.HLSS30: Provider.LPCLOUD.value,
    Collection.S1A_SLC: Provider.ASF.value,
    Collection.S1B_SLC: Provider.ASF.value,
    Collection.RTC_S1_V1: Provider.ASF.value,
    Collection.CSLC_S1_V1: Provider.ASF.value,
    Collection.CSLC_S1_STATIC_V1: Provider.ASF.value
}

COLLECTION_TO_PROVIDER_TYPE_MAP = {
    Collection.HLSL30: Provider.LPCLOUD.value,
    Collection.HLSS30: Provider.LPCLOUD.value,
    Collection.S1A_SLC: Provider.ASF.value,
    Collection.S1B_SLC: Provider.ASF.value,
    Collection.RTC_S1_V1: Provider.ASF_RTC.value,
    Collection.CSLC_S1_V1: Provider.ASF_CSLC.value,
    Collection.CSLC_S1_STATIC_V1: Provider.ASF_CSLC_STATIC.value
}

COLLECTION_TO_PRODUCT_TYPE_MAP = {
    Collection.HLSL30: ProductType.HLS.value,
    Collection.HLSS30: ProductType.HLS.value,
    Collection.S1A_SLC: ProductType.SLC.value,
    Collection.S1B_SLC: ProductType.SLC.value,
    Collection.RTC_S1_V1: ProductType.RTC.value,
    Collection.CSLC_S1_V1: ProductType.CSLC.value,
    Collection.CSLC_S1_STATIC_V1: ProductType.CSLC_STATIC.value
}

COLLECTION_TO_EXTENSIONS_FILTER_MAP = {
    Collection.HLSL30: ["B02.tif", "B03.tif", "B04.tif", "B05.tif", "B06.tif", "B07.tif", "Fmask.tif"],
    Collection.HLSS30: ["B02.tif", "B03.tif", "B04.tif", "B8A.tif", "B11.tif", "B12.tif", "Fmask.tif"],
    Collection.S1A_SLC: ["zip"],
    Collection.S1B_SLC: ["zip"],
    Collection.RTC_S1_V1: ["tif", "h5"],
    Collection.CSLC_S1_V1: ["h5"],
    Collection.CSLC_S1_STATIC_V1: ["h5"],
    "DEFAULT": ["tif", "h5"]
}

def get_cmr_token(endpoint, settings):

    cmr = settings["DAAC_ENVIRONMENTS"][endpoint]["BASE_URL"]
    edl = settings["DAAC_ENVIRONMENTS"][endpoint]["EARTHDATA_LOGIN"]
    username, _, password = netrc.netrc().authenticators(edl)
    token = supply_token(edl, username, password)

    return cmr, token, username, password, edl

async def async_query_cmr(args, token, cmr, settings, timerange, now: datetime, silent=False) -> list:
    request_url = f"https://{cmr}/search/granules.umm_json"
    bounding_box = args.bbox

    if args.collection in (Collection.S1A_SLC, Collection.S1B_SLC):
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
        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.RTC:
            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            match_native_id = re.match(rtc_granule_regex, args.native_id)
            burst_id = mbc_client.product_burst_id_to_mapping_burst_id(match_native_id.group("burst_id"))
            native_ids = mbc_client.get_reduced_rtc_native_id_patterns(mgrs[mgrs["bursts"].str.contains(burst_id)])

            if not native_ids:
                raise Exception(
                    f"The supplied {args.native_id=} is not associated with any MGRS tile collection"
                )

            params["options[native-id][pattern]"] = 'true'
            params["native-id[]"] = native_ids
        else:
            params["native-id[]"] = [args.native_id]

        if any(wildcard in args.native_id for wildcard in ['*', '?']):
            params["options[native-id][pattern]"] = 'true'

    # derive and apply param "temporal"
    now_date = now.strftime(CMR_TIME_FORMAT)
    temporal_range = _get_temporal_range(timerange.start_date, timerange.end_date, now_date)

    force_temporal = False
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.RTC:
        if args.native_id:
            match_native_id = re.match(rtc_granule_regex, args.native_id)
            acquisition_dt = dateutil.parser.parse(match_native_id.group("acquisition_ts"))
            timerange_start_date = (acquisition_dt - timedelta(hours=1)).strftime(CMR_TIME_FORMAT)
            timerange_end_date = (acquisition_dt + timedelta(hours=1)).strftime(CMR_TIME_FORMAT)
            temporal_range = _get_temporal_range(timerange_start_date, timerange_end_date, now_date)
            force_temporal = True

    if not silent:
        logger.info(f"Time Range: {temporal_range}  use_temporal: {args.use_temporal}")

    if args.use_temporal or force_temporal is True:
        params["temporal"] = temporal_range
    else:
        params["revision_date"] = temporal_range

        # if a temporal start-date is provided, set temporal
        if args.temporal_start_date:
            if not silent:
                logger.info(f"{args.temporal_start_date=}")
            params["temporal"] = dateutil.parser.isoparse(args.temporal_start_date).strftime(CMR_TIME_FORMAT)

    if not silent:
        logger.info(f"Querying CMR. {request_url=} {params=}")

    product_granules = await _async_request_search_cmr_granules(args, request_url, [params])
    search_results_count = len(product_granules)
    logger.info(f"QUERY RESULTS: Found {search_results_count} granules")

    products_per_line = 1000 # Default but this would never be used because we calculate dynamically below. Just here incase code moves around and we want a reasonable default
    if search_results_count > 0:
        # Print out all the query results but limit the number of characters per line
        one_logout = f'{(product_granules[0]["granule_id"], "revision " + str(product_granules[0]["revision_id"]))}'
        chars_per_line = len(one_logout) + 6 # 6 is a fudge factor
        products_per_line = MAX_CHARS_PER_LINE // chars_per_line
        for i in range(0, search_results_count, products_per_line):
            end_range = i + products_per_line
            if end_range > search_results_count:
                end_range = search_results_count
            logger.info(f'QUERY RESULTS {i+1} to {end_range} of {search_results_count}: {[(granule["granule_id"], "revision " + str(granule["revision_id"])) for granule in product_granules[i:end_range]]}')

    # Filter out granules with revision-id greater than max allowed
    least_revised_granules = []

    for granule in product_granules:
        if granule['revision_id'] <= args.max_revision:
            least_revised_granules.append(granule)
        else:
            logger.warning(
                f"Granule {granule['granule_id']} currently has revision-id of "
                f"{granule['revision_id']} which is greater than the max "
                f"{args.max_revision}. Ignoring and not storing or processing "
                f"this granule."
            )

    product_granules = least_revised_granules
    if len(product_granules) != search_results_count:
        logger.info(f"Filtered to {len(product_granules)} granules after least revision check")

    if args.collection in settings["SHORTNAME_FILTERS"]:
        product_granules = [granule for granule in product_granules
                            if _match_identifier(settings, args, granule)]

    if len(product_granules) != search_results_count:
        logger.info(f"Filtered to {len(product_granules)} total granules after shortname filter check")
        for i in range(0, len(product_granules), products_per_line):
            end_range = i + products_per_line
            if end_range > len(product_granules):
                end_range = len(product_granules)
            logger.info(f'FILTERED RESULTS {i+1} to {end_range} of {len(product_granules)}: {[(granule["granule_id"], "revision " + str(granule["revision_id"])) for granule in product_granules[i:end_range]]}')

    for granule in product_granules:
        granule["filtered_urls"] = _filter_granules(granule, args)

    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == ProductType.SLC:
        for granule in product_granules:
            granule["filtered_urls"] = _filter_slc_granules(granule)

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
        Collection.HLSL30: "LANDSAT_PRODUCT_ID",
        Collection.HLSS30: "PRODUCT_URI"
    }

    granules = []
    for item in items:
        if item["umm"]["TemporalExtent"].get("RangeDateTime"):
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        else:
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["SingleDateTime"]

        # NOTE: ProviderDates.Insert provides a better timestamp than ProductionDateTime across products for calculating
        # retrieval time. Especially for SLC products.
        provider_datetime = None
        for provider_date in item["umm"].get("ProviderDates", []):
            if provider_date["Type"] == "Insert":
                provider_datetime = provider_date["Date"]
                break
        production_datetime = item["umm"].get("DataGranule").get("ProductionDateTime")
        granules.append({
            "granule_id": item["umm"].get("GranuleUR"),
            "revision_id": item.get("meta").get("revision-id"),
            "provider": item.get("meta").get("provider-id"),
            "production_datetime": production_datetime,
            "provider_date": provider_datetime,
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
    filter_extension_key = first_true(
        COLLECTION_TO_EXTENSIONS_FILTER_MAP.keys(),
        pred=lambda x: x == args.collection, default="DEFAULT"
    )

    return [
        url
        for url in granule.get("related_urls")
        for extension in COLLECTION_TO_EXTENSIONS_FILTER_MAP.get(filter_extension_key)
        if url.endswith(extension)
    ]


def _filter_slc_granules(granule):
    return [url for url in granule["related_urls"] if "IW" in url]


def _match_identifier(settings, args, granule) -> bool:
    for filter in settings["SHORTNAME_FILTERS"][args.collection]:
        if re.match(filter, granule["identifier"]):
            return True

    return False
