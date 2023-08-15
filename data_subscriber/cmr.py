import logging
import re
from datetime import datetime
import dateutil.parser
import requests

PRODUCT_PROVIDER_MAP = {"HLSL30": "LPCLOUD",
                        "HLSS30": "LPCLOUD",
                        "SENTINEL-1A_SLC": "ASF",
                        "SENTINEL-1B_SLC": "ASF"}

def query_cmr(args, token, cmr, settings, timerange, now: datetime, silent=False) -> list:
    page_size = 2000
    request_url = f"https://{cmr}/search/granules.umm_json"
    bounding_box = args.bbox

    if args.collection == "SENTINEL-1A_SLC" or args.collection == "SENTINEL-1B_SLC":
        bound_list = bounding_box.split(",")

        # Excludes Antarctica
        if float(bound_list[1]) < -60:
            bound_list[1] = "-60"
            bounding_box = ",".join(bound_list)

    params = {
        "page_size": page_size,
        "sort_key": "-start_date",
        "provider": PRODUCT_PROVIDER_MAP[args.collection],
        "ShortName": args.collection,
        "token": token,
        "bounding_box": bounding_box
    }

    if args.native_id:
        params["native-id"] = args.native_id

        if any(wildcard in args.native_id for wildcard in ['*', '?']):
            params["options[native-id][pattern]"] = 'true'

    # derive and apply param "temporal"
    now_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    temporal_range = _get_temporal_range(timerange.start_date, timerange.end_date, now_date)
    if not silent:
        logging.info("Temporal Range: " + temporal_range)

    if args.use_temporal:
        params["temporal"] = temporal_range
    else:
        params["revision_date"] = temporal_range

        # if a temporal start-date is provided, set temporal
        if args.temporal_start_date:
            if not silent:
                logging.info(f"{args.temporal_start_date=}")
            params["temporal"] = dateutil.parser.isoparse(args.temporal_start_date).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not silent:
        logging.info(f"{request_url=} {params=}")
    product_granules, search_after = _request_search(args, request_url, params)

    while search_after:
        granules, search_after = _request_search(args, request_url, params, search_after=search_after)
        product_granules.extend(granules)

    # Filter out granules with revision-id greater than max allowed
    new_list = []
    for granule in product_granules:
        if granule['revision_id'] <= args.max_revision:
            new_list.append(granule)
        else:
            logging.warning(f"Granule {granule['granule_id']} currently has revision-id of {granule['revision_id']}\
 which is greater than the max {args.max_revision}. Ignoring and not storing or processing this granule.")
    product_granules = new_list

    if args.collection in settings["SHORTNAME_FILTERS"]:
        product_granules = [granule
                            for granule in product_granules
                            if _match_identifier(settings, args, granule)]

        if not silent:
            logging.info(f"Found {str(len(product_granules))} total granules")

    for granule in product_granules:
        granule["filtered_urls"] = _filter_granules(granule, args)

    return product_granules


def _get_temporal_range(start: str, end: str, now: str):
    start = start if start is not False else None
    end = end if end is not False else None

    if start is not None and end is not None:
        return "{},{}".format(start, end)
    if start is not None and end is None:
        return "{},{}".format(start, now)
    if start is None and end is not None:
        return "1900-01-01T00:00:00Z,{}".format(end)
    else:
        return "1900-01-01T00:00:00Z,{}".format(now)


def _request_search(args, request_url, params, search_after=None):
    response = requests.get(request_url, params=params, headers={"CMR-Search-After": search_after}) \
        if search_after else requests.get(request_url, params=params)

    results = response.json()
    items = results.get("items")
    next_search_after = response.headers.get("CMR-Search-After")

    collection_identifier_map = {"HLSL30": "LANDSAT_PRODUCT_ID",
                                 "HLSS30": "PRODUCT_URI"}

    if items and "umm" in items[0]:
        return [{"granule_id": item.get("umm").get("GranuleUR"),
                 "revision_id": item.get("meta").get("revision-id"),
                 "provider": item.get("meta").get("provider-id"),
                 "production_datetime": item.get("umm").get("DataGranule").get("ProductionDateTime"),
                 "temporal_extent_beginning_datetime": item["umm"]["TemporalExtent"]["RangeDateTime"][
                     "BeginningDateTime"],
                 "revision_date": item["meta"]["revision-date"],
                 "short_name": item.get("umm").get("Platforms")[0].get("ShortName"),
                 "bounding_box": [
                     {"lat": point.get("Latitude"), "lon": point.get("Longitude")}
                     for point
                     in item.get("umm")
                         .get("SpatialExtent")
                         .get("HorizontalSpatialDomain")
                         .get("Geometry")
                         .get("GPolygons")[0]
                         .get("Boundary")
                         .get("Points")
                 ],
                 "related_urls": [url_item.get("URL") for url_item in item.get("umm").get("RelatedUrls")],
                 "identifier": next(attr.get("Values")[0]
                                    for attr in item.get("umm").get("AdditionalAttributes")
                                    if attr.get("Name") == collection_identifier_map[
                                        args.collection]) if args.collection in collection_identifier_map else None}
                for item in items], next_search_after
    else:
        return [], None


def _filter_granules(granule, args):
    collection_map = {"HLSL30": ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"],
                      "HLSS30": ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"],
                      "SENTINEL-1A_SLC": ["IW"],
                      "SENTINEL-1B_SLC": ["IW"],
                      "DEFAULT": ["tif"]}
    filter_extension = "DEFAULT"

    for collection in collection_map:
        if collection in args.collection:
            filter_extension = collection
            break

    return [f
            for f in granule.get("related_urls")
            for extension in collection_map.get(filter_extension)
            if extension in f]


def _match_identifier(settings, args, granule) -> bool:
    for filter in settings["SHORTNAME_FILTERS"][args.collection]:
        if re.match(filter, granule["identifier"]):
            return True

    return False