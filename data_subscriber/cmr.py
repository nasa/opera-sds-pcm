import logging
import math
import os
import re
from datetime import datetime

import backoff
import dateutil.parser
import requests
from more_itertools import first_true
from requests.exceptions import HTTPError

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
        "sort_key": "-start_date",
        "provider": COLLECTION_TO_PROVIDER_MAP[args.collection],
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
    product_granules = _request_search_cmr_granules(args, request_url, params)

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


def giveup_cmr_requests(e):
    if isinstance(e, HTTPError):
        if e.response.status_code == 413 and e.response.reason == "Payload Too Large":  # give up. Fix bug
            return True
        if e.response.status_code == 400:  # Bad Requesst. give up. Fix bug
            return True
        if e.response.status_code == 504 and e.response.reason == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    return False


def _request_search_cmr_granules(args, request_url, params):
    response_jsons = _request_search(args, request_url, params)
    return response_jsons_to_cmr_granules(args, response_jsons)


def _request_search(args, request_url, params):
    page_size = 2000  # default is 10, max is 2000
    params["page_size"] = page_size

    logger.debug(f"_request_search_concurrent({request_url=}, {params=}")

    response_jsons = []
    max_pages = 1  # cap the number of pages (requests) to scroll through results.
    # update after first response

    current_page = 1
    headers = {
        'Client-Id': f'nasa.jpl.opera.sds.pcm.data_subscriber.{os.environ["USER"]}'
    }
    while current_page <= max_pages:
        response = try_request_get(request_url, params, headers, raise_for_status=True)
        response_json = response.json()
        response_jsons.append(response_json)

        if current_page == 1:
            logger.info(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
            max_pages = math.ceil(response_json["hits"] / page_size)
            logger.info(f"Updating max pages to {max_pages=}")
        logger.info(f'CMR number of granules (cmr-query-page {current_page} of {max_pages}): {len(response_json["items"])=:,}')

        cmr_search_after = response.headers.get("CMR-Search-After")
        logger.debug(f"{cmr_search_after=}")
        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

        if len(response_json["items"]) < page_size:
            logger.info("Reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if not current_page <= max_pages:
            logger.warning(
                "Reached max pages limit. "
                "Not all search results exhausted. "
                "Adjust limit or time ranges to process all hits, then re-run this script."
            )

    return response_jsons


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


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError,),
    max_tries=7,  # NOTE: increased number of attempts because of random API unreliability and slowness
    jitter=None,
    giveup=giveup_cmr_requests
)
def try_request_get(request_url, params, headers=None, raise_for_status=True):
    response = requests.get(request_url, params=params, headers=headers)
    if raise_for_status:
        response.raise_for_status()
    return response


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
