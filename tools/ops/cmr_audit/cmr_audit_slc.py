import argparse
import asyncio
import concurrent.futures
from datetime import datetime, date, timezone
import functools
import logging
import logging.handlers
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from typing import Union, Iterable

import aiohttp
import dateutil.parser
import more_itertools
from dotenv import dotenv_values
from more_itertools import always_iterable

from geo.geo_util import does_bbox_intersect_north_america
from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules, get_cmr_audit_granules

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.getLogger("geo.geo_util").setLevel(level=logging.WARNING)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logger = logging.getLogger(__name__)

config = {
    **dotenv_values("../../.env"),
    **os.environ
}


def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument(
        "--start-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--output", "-o",
        help=f'Output filepath.'
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json", "db"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    argparser.add_argument('--log-level', default='INFO', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))

    return argparser


def init_logging(log_level=logging.INFO):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format, datefmt="%Y-%m-%d %H:%M:%S", force=True)

    rfh1 = logging.handlers.RotatingFileHandler('cmr_audit_slc.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler('cmr_audit_slc-error.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)


#######################################################################
# CMR AUDIT FUNCTIONS
#######################################################################

async def async_get_cmr_granules_slc_s1a(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules("SENTINEL-1A_SLC",
                                  temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                  platform_short_name="SENTINEL-1A")


async def async_get_cmr_granules_slc_s1b(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules("SENTINEL-1B_SLC",
                                  temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                  platform_short_name="SENTINEL-1B")


async def async_get_cmr_cslc(cslc_native_id_patterns: set, temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr(cslc_native_id_patterns, collection_short_name="OPERA_L2_CSLC-S1_V1",
                               temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end, chunk_size=100)


async def async_get_cmr_rtc(rtc_native_id_patterns: set, temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr(rtc_native_id_patterns, collection_short_name="OPERA_L2_RTC-S1_V1",
                               temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end, chunk_size=100)


async def async_get_cmr(
        native_id_patterns: set,
        collection_short_name: Union[str, Iterable[str]],
        temporal_date_start: str, temporal_date_end: str,
        chunk_size=1000):  # 1000 ~= 55,100 length
    """
    Issue CMR query requests.
    :param native_id_patterns: the native ID patterns to use in the query. Corresponds to query param `&native-id[]`. Allows use of wildcards "*" and "?", but is descouraged.
    :param collection_short_name: CMR collection short name. Typically found in PCM's settings.yaml
    :param temporal_date_start: temporal start date. Corresponds to query param `&temporal[]=<start>,<end>`
    :param temporal_date_end: temporal end date. Corresponds to query param `&temporal[]=<start>,<end>`
    :param chunk_size: split queries across N native-id patterns per request. CMR request bodies have an implicit size limit of 55,100 length. Must be a value in the interval [1,1000].
    """
    logger.debug(f"entry({len(native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    native_id_patterns = more_itertools.always_iterable(native_id_patterns)
    native_id_pattern_batches = list(more_itertools.chunked(native_id_patterns, chunk_size))

    request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    sem = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for i, native_id_pattern_batch in enumerate(native_id_pattern_batches, start=1):
            # native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(native_id_pattern_batch)

            request_body = (
                "provider=ASF"
                f'{"&short_name[]=" + "&short_name[]=".join(always_iterable(collection_short_name))}'
                "&platform[]=Sentinel-1A"
                "&platform[]=Sentinel-1B"
                "&bounding_box=-180,-60,180,90"
                # "&options[native-id][pattern]=true"
                # f"{native_id_patterns_query_params}"
                f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            )
            logger.debug(f"Creating request task {i} of {len(native_id_pattern_batches)}")
            post_cmr_tasks.append(get_cmr_audit_granules(request_url, request_body, session, sem))
            break
        logger.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        logger.debug("Batching tasks")
        cmr_granules = set()
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, len(post_cmr_tasks)))  # CMR recommends 2-5 threads.
        for i, task_chunk in enumerate(task_chunks, start=1):
            logger.info(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                cmr_granules.update(post_cmr_tasks_result[0])
        return cmr_granules

slc_regex = (
    r'(?P<mission_id>S1A|S1B)_'
    r'(?P<beam_mode>IW)_'
    r'(?P<product_type>SLC)'
    r'(?P<resolution>_)_'
    r'(?P<level>1)'
    r'(?P<class>S)'
    r'(?P<pol>SH|SV|DH|DV)_'
    r'(?P<start_ts>(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})T(?P<start_hour>\d{2})(?P<start_minute>\d{2})(?P<start_second>\d{2}))_'
    r'(?P<stop_ts>(?P<stop_year>\d{4})(?P<stop_month>\d{2})(?P<stop_day>\d{2})T(?P<stop_hour>\d{2})(?P<stop_minute>\d{2})(?P<stop_second>\d{2}))_'
    r'(?P<orbit_num>\d{6})_'
    r'(?P<data_take_id>[0-9A-F]{6})_'
    r'(?P<product_id>[0-9A-F]{4})-'
    r'SLC$'
)

def slc_granule_ids_to_cslc_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    cslc_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(slc_regex, granule)
        cslc_acquisition_dt_str = m.group("start_ts")

        #                          OPERA_L2_CSLC-S1_*_20231124T124529Z_*_S1*
        cslc_native_id_pattern = f'OPERA_L2_CSLC-S1_*_{cslc_acquisition_dt_str}Z_*_S1*v1.1'
        cslc_native_id_patterns.add(cslc_native_id_pattern)

        # bi-directional mapping of HLS-DSWx inputs and outputs
        if hasattr(input_to_outputs_map[granule], "add"):
            input_to_outputs_map[granule].add(cslc_native_id_pattern)  # strip wildcard char
        else:  # hasattr(input_to_outputs_map[granule], "append"):
            input_to_outputs_map[granule].append(cslc_native_id_pattern)  # strip wildcard char

        output_to_inputs_map[cslc_native_id_pattern].add(granule)

    return cslc_native_id_patterns


def slc_granule_ids_to_rtc_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    rtc_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(slc_regex, granule)
        rtc_acquisition_dt_str = m.group("start_ts")

        rtc_native_id_pattern = f'OPERA_L2_RTC-S1_*_{rtc_acquisition_dt_str}Z_*Z_S1?_30_v'
        rtc_native_id_patterns.add(rtc_native_id_pattern + "*")

        # bidirectional mapping of HLS-DSWx inputs and outputs
        if hasattr(input_to_outputs_map[granule], "add"):
            input_to_outputs_map[granule].add(rtc_native_id_pattern)  # strip wildcard char
        else:  # hasattr(input_to_outputs_map[granule], "append"):
            input_to_outputs_map[granule].append(rtc_native_id_pattern)  # strip wildcard char

        output_to_inputs_map[rtc_native_id_pattern].add(granule)

    return rtc_native_id_patterns


def cmr_products_native_id_pattern_diff(cmr_products, cmr_native_id_patterns):
    product_type_and_acquisition_time_to_products_map = defaultdict(set)
    for cmr_product in cmr_products:
        product_type = "RTC" if "RTC" in cmr_product else "CSLC"
        if product_type == "CSLC":
            acquisition_time = cmr_product[33:48]
        else:  # product_type == "RTC"
            acquisition_time = cmr_product[32:47]
        product_type_and_acquisition_time_to_products_map[(product_type, acquisition_time)].add(cmr_product)

    product_type_acquisition_time_to_native_id_pattern_map = defaultdict(set)
    for native_id_pattern in cmr_native_id_patterns:
        product_type = "RTC" if "RTC" in native_id_pattern else "CSLC"
        if product_type == "CSLC":
            acquisition_time = native_id_pattern[19:34]
        else:  # product_type == "RTC"
            acquisition_time = native_id_pattern[18:33]
        product_type_acquisition_time_to_native_id_pattern_map[(product_type, acquisition_time)].add(native_id_pattern)

    cmr_product_refs = product_type_and_acquisition_time_to_products_map.keys()
    expected_product_refs = product_type_acquisition_time_to_native_id_pattern_map.keys()
    missing_product_refs = expected_product_refs - cmr_product_refs
    missing_product_native_id_patterns = [product_type_acquisition_time_to_native_id_pattern_map[type_time] for type_time in missing_product_refs]
    if not missing_product_native_id_patterns:
        missing_product_native_id_patterns = set()
    else:
        missing_product_native_id_patterns = functools.reduce(set.union, missing_product_native_id_patterns)
    return missing_product_native_id_patterns


#######################################################################
# CMR AUDIT
#######################################################################

async def run(argv: list[str]):
    logger.info(f'{argv=}')
    args = create_parser().parse_args(argv[1:])

    logger.info("Querying CMR for list of expected SLC granules")
    cmr_start_dt_str = args.start_datetime
    cmr_start_dt = dateutil.parser.isoparse(cmr_start_dt_str)
    cmr_end_dt_str = args.end_datetime
    cmr_end_dt = dateutil.parser.isoparse(cmr_end_dt_str)

    cmr_granules_slc_s1a, cmr_granules_slc_s1a_details = await async_get_cmr_granules_slc_s1a(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)
    cmr_granules_slc_s1b, cmr_granules_slc_s1b_details = await async_get_cmr_granules_slc_s1b(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    cmr_granules_slc = cmr_granules_slc_s1a.union(cmr_granules_slc_s1b)
    cmr_granules_slc_details = {}; cmr_granules_slc_details.update(cmr_granules_slc_s1a_details); cmr_granules_slc_details.update(cmr_granules_slc_s1b_details)

    logger.info("Filtering North America granules")
    cmr_granules_slc_na = set()
    cmr_granules_slc_details_na = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        does_bbox_intersect_north_america([])  # DEV: optimization to force initial cache load of geojson file
        future_to_granule_map = {}
        for granule_id, granule_details in cmr_granules_slc_details.items():
            bounding_box = [
                {"lat": point["Latitude"], "lon": point["Longitude"]}
                for point in granule_details["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]
            ]
            future_to_granule_map[executor.submit(does_bbox_intersect_north_america, bbox=bounding_box)] = (granule_id, granule_details)
        for future in concurrent.futures.as_completed(future_to_granule_map):
            is_granule_in_na = future.result()
            if is_granule_in_na:
                granule_id, granule_details = future_to_granule_map[future]
                cmr_granules_slc_na.add(granule_id)
                cmr_granules_slc_details_na[granule_id] = granule_details

    logger.info(f"Expected CSLC input (granules): {len(cmr_granules_slc_na)=:,}")
    logger.info(f"Expected RTC input (granules): {len(cmr_granules_slc)=:,}")

    cslc_native_id_patterns = slc_granule_ids_to_cslc_native_id_patterns(
        cmr_granules_slc_na,
        input_slc_to_outputs_cslc_map := defaultdict(set),
        output_cslc_to_inputs_slc_map := defaultdict(set)
    )

    rtc_native_id_patterns = slc_granule_ids_to_rtc_native_id_patterns(
        cmr_granules_slc,
        input_slc_to_outputs_rtc_map := defaultdict(set),
        output_rtc_to_inputs_slc_map := defaultdict(set)
    )

    logger.info("Querying CMR for list of expected CSLC granules")
    cmr_cslc_products = await async_get_cmr_cslc(cslc_native_id_patterns, temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    logger.info("Querying CMR for list of expected RTC granules")
    cmr_rtc_products = await async_get_cmr_rtc(rtc_native_id_patterns, temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    missing_cslc_native_id_patterns = cmr_products_native_id_pattern_diff(cmr_products=cmr_cslc_products, cmr_native_id_patterns=cslc_native_id_patterns)
    missing_rtc_native_id_patterns = cmr_products_native_id_pattern_diff(cmr_products=cmr_rtc_products, cmr_native_id_patterns=rtc_native_id_patterns)

    #######################################################################
    # CMR_AUDIT SUMMARY
    #######################################################################
    # logger.debug(f"{pstr(missing_rtc_native_id_patterns)=!s}")

    missing_cmr_granules_slc_cslc = [output_cslc_to_inputs_slc_map[native_id_pattern] for native_id_pattern in missing_cslc_native_id_patterns]
    missing_cmr_granules_slc_cslc = set(functools.reduce(set.union, missing_cmr_granules_slc_cslc)) if missing_cmr_granules_slc_cslc else set()

    missing_cmr_granules_slc_rtc = [output_rtc_to_inputs_slc_map[native_id_pattern] for native_id_pattern in missing_rtc_native_id_patterns]
    missing_cmr_granules_slc_rtc = set(functools.reduce(set.union, missing_cmr_granules_slc_rtc)) if missing_cmr_granules_slc_rtc else set()

    # logger.debug(f"{pstr(missing_slc)=!s}")
    logger.info(f"Expected input (granules): {len(cmr_granules_slc)=:,}")
    logger.info(f"Fully published (granules) (CSLC): {len(cmr_cslc_products)=:,}")
    logger.info(f"Fully published (granules) (RTC): {len(cmr_rtc_products)=:,}")
    logger.info(f"Missing processed CSLC (granules): {len(missing_cmr_granules_slc_cslc)=:,}")
    logger.info(f"Missing processed RTC (granules): {len(missing_cmr_granules_slc_rtc)=:,}")

    start_dt_str = cmr_start_dt.strftime("%Y%m%d-%H%M%S")
    end_dt_str = cmr_start_dt.strftime("%Y%m%d-%H%M%S")
    current_dt_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    outfilename = f"{start_dt_str}Z_{end_dt_str}Z_{current_dt_str}Z"

    if args.format == "db":
        def date_from_slc(granule_id):
            return date.fromtimestamp(datetime.strptime(granule_id[17:32], "%Y%m%dT%H%M%S").timestamp())
        def date_from_cslc(granule_id):
            return date.fromtimestamp(datetime.strptime(granule_id.split("_")[4], "%Y%m%dT%H%M%SZ").timestamp())
        def date_from_rtc(granule_id):
            return date.fromtimestamp(datetime.strptime(granule_id.split("_")[4], "%Y%m%dT%H%M%SZ").timestamp())

        def dt_from_slc(granule_id):
            return datetime.fromtimestamp(datetime.strptime(granule_id[17:32], "%Y%m%dT%H%M%S").timestamp())
        def dt_from_cslc(granule_id):
            return datetime.fromtimestamp(datetime.strptime(granule_id.split("_")[4], "%Y%m%dT%H%M%SZ").timestamp())
        def dt_from_rtc(granule_id):
            return datetime.fromtimestamp(datetime.strptime(granule_id.split("_")[4], "%Y%m%dT%H%M%SZ").timestamp())

        # group input products (SLC-RTC) by acquisition dt
        slc_products_by_acq_dt_rtc = defaultdict(set)
        for slc in input_slc_to_outputs_rtc_map:
            dt_slc = dt_from_slc(slc)
            slc_products_by_acq_dt_rtc[dt_slc].add(slc)
        # group input products (SLC-RTC) by acquisition date
        slc_products_by_acq_date_rtc = defaultdict(set)
        for slc in input_slc_to_outputs_rtc_map:
            date_slc = date_from_slc(slc)
            slc_products_by_acq_date_rtc[date_slc].add(slc)

        # group input products (SLC-CSLC) by acquisition dt
        slc_products_by_acq_dt_cslc = defaultdict(set)
        for slc in input_slc_to_outputs_cslc_map:
            dt_slc = dt_from_slc(slc)
            slc_products_by_acq_dt_cslc[dt_slc].add(slc)
        # group input products (SLC-CSLC) by acquisition date
        slc_products_by_acq_date_cslc = defaultdict(set)
        for slc in input_slc_to_outputs_cslc_map:
            date_slc = date_from_slc(slc)
            slc_products_by_acq_date_cslc[date_slc].add(slc)

        # group output products (RTC) by acquisition dt
        rtc_products_by_acq_dt = defaultdict(set)
        for rtc in cmr_rtc_products:
            dt_rtc = dt_from_rtc(rtc)
            rtc_products_by_acq_dt[dt_rtc].update({rtc})
        # group output products (RTC) by acquisition date
        rtc_products_by_acq_date = defaultdict(set)
        for rtc in cmr_rtc_products:
            date_rtc = date_from_rtc(rtc)
            rtc_products_by_acq_date[date_rtc].update({rtc})

        # group output products (CSLC) by acquisition dt
        cslc_products_by_acq_dt = defaultdict(set)
        for cslc in cmr_cslc_products:
            dt_cslc = dt_from_cslc(cslc)
            cslc_products_by_acq_dt[dt_cslc].update({cslc})
        # group output products (CSLC) by acquisition date
        cslc_products_by_acq_date = defaultdict(set)
        for cslc in cmr_cslc_products:
            date_cslc = date_from_cslc(cslc)
            cslc_products_by_acq_date[date_cslc].update({cslc})

        # duplicate detection
        rtc_duplicates_by_acq_dt = {
            dt: products
            for dt, products in rtc_products_by_acq_dt.items()
            if len(products) >= 2
        }
        cslc_duplicates_by_acq_dt = {
            dt: products
            for dt, products in cslc_products_by_acq_dt.items()
            if len(products) >= 2
        }

        missing_rtc_dts = slc_products_by_acq_dt_rtc.keys() - rtc_products_by_acq_dt.keys()
        print(f"{missing_rtc_dts=}")
        missing_cslc_dts = slc_products_by_acq_dt_cslc.keys() - cslc_products_by_acq_dt.keys()
        print(f"{missing_cslc_dts=}")

        # group metrics by output type and acquisition dt
        product_accountability_map = {
            # "CSLC": {
            #     "2025-01-01": {
            #         "expected_inputs": 123456,
            #         "produced_outputs": 123456,
            #     }
            # },
            # "RTC": {
            #     "2025-01-01": {
            #         "expected_inputs": 123456,
            #         "produced_outputs": 123456,
            #     }
            # },
        }

        cslc_accountability_map = {"CSLC": {}}
        for acquisition_date in cslc_products_by_acq_date:
            cslc_accountability_map["CSLC"][acquisition_date] = {}

            num_inputs = len(slc_products_by_acq_date_cslc[acquisition_date])
            cslc_accountability_map["CSLC"][acquisition_date]["expected_inputs"] = num_inputs

            num_outputs = len(cslc_products_by_acq_date[acquisition_date])
            cslc_accountability_map["CSLC"][acquisition_date]["produced_outputs"] = num_outputs
        product_accountability_map.update(cslc_accountability_map)

        rtc_accountability_map = {"RTC": {}}
        for acquisition_date in rtc_products_by_acq_date:
            rtc_accountability_map["RTC"][acquisition_date] = {}

            num_inputs = len(slc_products_by_acq_date_rtc[acquisition_date])
            rtc_accountability_map["RTC"][acquisition_date]["expected_inputs"] = num_inputs

            num_outputs = len(rtc_products_by_acq_date[acquisition_date])
            rtc_accountability_map["RTC"][acquisition_date]["produced_outputs"] = num_outputs
        product_accountability_map.update(rtc_accountability_map)

        # group missing inputs by output type and acquisition dt
        output_product_types_to_products_map = {
            # "CSLC": {
            #     "2025-01-01": {"SLC-A", "SLC-B", ...}
            # },
            # "RTC": {
            #     "2025-01-01": {"SLC-A", "SLC-B", ...}
            # },
        }

        cslc_output_date_to_missing_input_products_map = {"CSLC": {}}
        output_product_types_to_products_map.update(cslc_output_date_to_missing_input_products_map)
        for slc in missing_cmr_granules_slc_cslc:
            date_slc = date_from_slc(slc)
            if not output_product_types_to_products_map["CSLC"].get(date_slc):
                output_product_types_to_products_map["CSLC"][date_slc] = set()
            output_product_types_to_products_map["CSLC"][date_slc].add(slc)

        rtc_output_date_to_missing_input_products_map = {"RTC": {}}
        output_product_types_to_products_map.update(rtc_output_date_to_missing_input_products_map)
        for slc in missing_cmr_granules_slc_rtc:
            date_slc = date_from_slc(slc)
            if not output_product_types_to_products_map["RTC"].get(date_slc):
                output_product_types_to_products_map["RTC"][date_slc] = set()
            output_product_types_to_products_map["RTC"][date_slc].add(slc)

        # create DB model (accountability)
        docs = []
        for product_type in product_accountability_map:
            acquisition_date: date
            for acquisition_date in product_accountability_map[product_type]:
                doc = {
                    "acquisition_date": acquisition_date.isoformat(),
                    "product_type": product_type,
                    "num_inputs": product_accountability_map[product_type][acquisition_date]["expected_inputs"],
                    "num_outputs": product_accountability_map[product_type][acquisition_date]["produced_outputs"],
                    "num_missing": len(output_product_types_to_products_map[product_type].get(acquisition_date, []))
                }
                docs.append(doc)


        from pymongo import MongoClient
        def db_init():
            client = MongoClient(host="localhost")
            # client = MongoClient(host="mongo")  # TODO chrisjrd: needed for docker-compose (name of service)
            db = client["new_db"]  # switch db

            # NOTE: In EC2 against a Mongo DB Docker container, this doesn't create the DB.
            #  This works locally on macOS against a Mongo DB Docker container, however.
            try:
                assert "new_db" in client.list_database_names()
            except:
                pass
            return db

        def create_collection(db, collection_name):
            # try to create the collection upfront in the new db in Mongo DB
            #  may not work in some version-client combinations, so ignore errors:
            #  Mongo DB will likely create the collection on document insert instead
            try:
                db.create_collection(collection_name)
                assert collection_name in db.list_collection_names()
            except Exception:
                logger.warning("Failed to create collection. It may already exist. This warning may be safely ignored.")
            jobs_collection = db[collection_name]
            return jobs_collection

        # write out to DB
        db = db_init()

        create_collection(db, "accountability")
        jobs_collection = db["accountability"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            jobs_collection.update_one(
                filter={
                    "acquisition_date": doc["acquisition_date"],
                    "product_type": doc["product_type"]
                },
                update={"$set": doc},
                upsert=True
            )

        # create db model (missing products)
        docs = []
        for product_type in output_product_types_to_products_map:
            acquisition_date: date
            for acquisition_date in product_accountability_map[product_type]:
                missing_products = output_product_types_to_products_map[product_type].get(acquisition_date, [])
                for missing_product in missing_products:
                    doc = {
                        "acquisition_date": acquisition_date.isoformat(),
                        "product_type": product_type,
                        "product": missing_product
                    }
                    docs.append(doc)

        create_collection(db, "missing_products")
        missing_collection = db["missing_products"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            missing_collection.update_one(
                filter={
                    "acquisition_date": doc["acquisition_date"],
                    "product_type": doc["product_type"]
                },
                update={"$set": doc},
                upsert=True
            )

        # create db model (duplicates)
        docs = []
        for duplicates in cslc_duplicates_by_acq_dt:
            for duplicate in duplicates:
                docs.append({"product": duplicate})
        for duplicates in rtc_duplicates_by_acq_dt:
            for duplicate in duplicates:
                docs.append({"product": duplicate})

        create_collection(db, "duplicate_products")
        duplicates_collection = db["duplicate_products"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            duplicates_collection.update_one(
                filter={"product": doc["product"]},
                update={"$set": doc},
                upsert=True
            )

    elif args.format == "txt":
        output_file_missing_cmr_granules = args.output if args.output else f"missing_granules_SLC-CSLC_{outfilename}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_slc_cslc))
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

        output_file_missing_cmr_granules = args.output if args.output else f"missing_granules_SLC-RTC_{outfilename}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_slc_rtc))
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")
    elif args.format == "json":
        output_file_missing_cmr_granules = args.output if args.output else f"missing_granules_SLC-CSLC_{outfilename}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_slc_cslc))
            fp.write(json_str)
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

        output_file_missing_cmr_granules = args.output if args.output else f"missing_granules_SLC-RTC_{outfilename}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_slc_rtc))
            fp.write(json_str)
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")
    else:
        raise Exception()


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    log_level = args.log_level
    init_logging()

    asyncio.run(run(sys.argv))
