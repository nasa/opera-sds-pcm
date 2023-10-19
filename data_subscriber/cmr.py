import logging
import re
from datetime import datetime

import dateutil.parser
import requests
from more_itertools import first_true

logger = logging.getLogger(__name__)

# TODO chrisjrd: refactor. this is a duplicate. kind of. (value here maps to CMR provider query param)
PRODUCT_PROVIDER_MAP = {
    "HLSL30": "LPCLOUD",
    "HLSS30": "LPCLOUD",
    "SENTINEL-1A_SLC": "ASF",
    "SENTINEL-1B_SLC": "ASF",
    "OPERA_L2_RTC-S1_V1": "ASF",
    "OPERA_L2_CSLC-S1_V1": "ASF"
}


def query_cmr(args, token, cmr, settings, timerange, now: datetime, silent=False) -> list:
    request_url = f"https://{cmr}/search/granules.umm_json"
    bounding_box = args.bbox

    if args.collection == "SENTINEL-1A_SLC" or args.collection == "SENTINEL-1B_SLC":
        bound_list = bounding_box.split(",")

        # Excludes Antarctica
        if float(bound_list[1]) < -60:
            bound_list[1] = "-60"
            bounding_box = ",".join(bound_list)

    params = {
        "page_size": 1,  # TODO chrisjrd: set back to 2000 before commit
        "sort_key": "-start_date",
        "provider": PRODUCT_PROVIDER_MAP[args.collection],
        "ShortName[]": [args.collection],
        "token": token,
        "bounding_box": bounding_box
    }

    if args.native_id:
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
        logger.info(f"{request_url=} {params=}")
    product_granules, search_after = _request_search(args, request_url, params)

    # TODO chrisjrd: uncomment before commit
    # while search_after:
    #     granules, search_after = _request_search(args, request_url, params, search_after=search_after)
    #     product_granules.extend(granules)

    # Filter out granules with revision-id greater than max allowed
    least_revised_granules = []
    for granule in product_granules:
        if granule['revision_id'] <= args.max_revision:
            least_revised_granules.append(granule)
        else:
            logger.warning(f"Granule {granule['granule_id']} currently has revision-id of {granule['revision_id']}\
 which is greater than the max {args.max_revision}. Ignoring and not storing or processing this granule.")
    product_granules = least_revised_granules

    if args.collection in settings["SHORTNAME_FILTERS"]:
        product_granules = [granule for granule in product_granules if _match_identifier(settings, args, granule)]

        if not silent:
            logger.info(f"Found {len(product_granules)} total granules")

    for granule in product_granules:
        granule["filtered_urls"] = _filter_granules(granule, args)

    return product_granules


def _get_temporal_range(start: str, end: str, now: str):
    start = start if start is not False else "1900-01-01T00:00:00Z"
    end = end if end is not False else now

    return "{},{}".format(start, end)


def _request_search(args, request_url, params, search_after=None):
    response = requests.get(request_url, params=params, headers={"CMR-Search-After": search_after}) \
        if search_after else requests.get(request_url, params=params)

    results = response.json()
    items = results.get("items")
    next_search_after = response.headers.get("CMR-Search-After")

    collection_identifier_map = {
        "HLSL30": "LANDSAT_PRODUCT_ID",
        "HLSS30": "PRODUCT_URI"
    }

    results = []
    for item in items:
        if item["umm"]["TemporalExtent"].get("RangeDateTime"):
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        else:
            temporal_extent_beginning_datetime = item["umm"]["TemporalExtent"]["SingleDateTime"]

        results.append({
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
    return results, next_search_after


def _filter_granules(granule, args):
    collection_to_extensions_filter_map = {
        "HLSL30": ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"],
        "HLSS30": ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"],
        "SENTINEL-1A_SLC": ["IW"],
        "SENTINEL-1B_SLC": ["IW"],
        "OPERA_L2_RTC-S1_V1": ["tif", "h5"],
        "OPERA_L2_CSLC-S1_V1": ["h5"],
        "DEFAULT": ["tif"]
    }
    filter_extension = "DEFAULT"

    # TODO chrisjrd: previous code using substring comparison for args.collection. may point to subtle bug in existing system
    # for collection in collection_map:
    #     if collection in args.collection:
    #         filter_extension = collection
    #         break
    filter_extension = first_true(collection_to_extensions_filter_map.keys(), pred=lambda x: x == args.collection, default="DEFAULT")

    return [
        url
        for url in granule.get("related_urls")
        for extension in collection_to_extensions_filter_map.get(filter_extension)
        if url.endswith(extension)
    ]  # TODO chrisjrd: not using endswith may point to subtle bug in existing system


def _match_identifier(settings, args, granule) -> bool:
    for filter in settings["SHORTNAME_FILTERS"][args.collection]:
        if re.match(filter, granule["identifier"]):
            return True

    return False