import argparse
import asyncio
import concurrent.futures
import functools
import logging
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from io import StringIO
from pprint import pprint
from typing import Union, Iterable

import aiohttp
import more_itertools
from dotenv import dotenv_values
from more_itertools import always_iterable

from geo.geo_util import does_bbox_intersect_north_america
from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules
from tools.ops.cmr_audit.cmr_client import async_cmr_post

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.getLogger("geo.geo_util").setLevel(level=logging.WARNING)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logger = logging.getLogger()

config = {
    **dotenv_values("../../.env"),
    **os.environ
}


def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument(
        "--start-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR.'
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR.'
    )
    argparser.add_argument(
        "--output", "-o",
        help=f'ISO formatted datetime string. Must be compatible with CMR.'
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    return argparser


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
    return await async_get_cmr(cslc_native_id_patterns, collection_short_name="OPERA_CSLC_S1", collection_concept_id="C1257337155-ASF",
                               temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end)


async def async_get_cmr_rtc(rtc_native_id_patterns: set, temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr(rtc_native_id_patterns, collection_short_name="OPERA_RTC_S1", collection_concept_id="C1257337044-ASF",
                               temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end)


async def async_get_cmr(
        native_id_patterns: set,
        collection_short_name: Union[str, Iterable[str]],
        collection_concept_id: str,
        temporal_date_start: str, temporal_date_end: str):
    logger.debug(f"entry({len(native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    native_id_patterns = more_itertools.always_iterable(native_id_patterns)
    native_id_pattern_batches = list(more_itertools.chunked(native_id_patterns, 1000))  # 1000 == 55,100 length

    request_url = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"
    logger.warning(f"PRE-PRODUCTION: Using CMR UAT environment. {request_url=}")  # TODO chrisjrd: eventually update URL and remove for ops

    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for i, rtc_native_id_pattern_batch in enumerate(native_id_pattern_batches, start=1):
            native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(rtc_native_id_pattern_batch)

            request_body = (
                "provider=ASF"
                f'{"&short_name[]=" + "&short_name[]=".join(always_iterable(collection_short_name))}'
                f"&collection_concept_id={collection_concept_id}"
                "&platform[]=Sentinel-1A"
                "&platform[]=Sentinel-1B"
                "&options[native-id][pattern]=true"
                f"{native_id_patterns_query_params}"
                f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            )
            logger.debug(f"Creating request task {i} of {len(native_id_pattern_batches)}")
            post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))
        logger.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        logger.debug("Batching tasks")
        cmr_granules = set()
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, 30))
        for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
            logger.info(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                cmr_granules.update(post_cmr_tasks_result[0])
        return cmr_granules


def slc_granule_ids_to_cslc_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    rtc_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(
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
            r'SLC$',
            granule
        )
        cslc_acquisition_dt_str = m.group("start_ts")

        rtc_native_id_pattern = f'OPERA_L2_CSLC-S1?_IW_*_{cslc_acquisition_dt_str}Z_v*_*'
        rtc_native_id_patterns.add(rtc_native_id_pattern)

        # bi-directional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(rtc_native_id_pattern)  # strip wildcard char
        output_to_inputs_map[rtc_native_id_pattern].add(granule)

    return rtc_native_id_patterns


def slc_granule_ids_to_rtc_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    rtc_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(
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
            r'SLC$',
            granule
        )
        rtc_acquisition_dt_str = m.group("start_ts")

        rtc_native_id_pattern = f'OPERA_L2_RTC-S1_*_{rtc_acquisition_dt_str}Z_*Z_S1?_30_v*'
        rtc_native_id_patterns.add(rtc_native_id_pattern)

        # bidirectional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(rtc_native_id_pattern)  # strip wildcard char
        output_to_inputs_map[rtc_native_id_pattern].add(granule)

    return rtc_native_id_patterns


def cmr_products_native_id_pattern_diff(cmr_products, cmr_native_id_patterns):
    product_type_and_acquisition_time_to_products_map = defaultdict(set)
    for cmr_product in cmr_products:
        product_type = "RTC" if "RTC" in cmr_product else "CSLC"
        if product_type == "CSLC":
            acquisition_time = cmr_product[40:55]
        else:  # product_type == "RTC"
            acquisition_time = cmr_product[32:47]
        product_type_and_acquisition_time_to_products_map[(product_type, acquisition_time)].add(cmr_product)

    product_type_acquisition_time_to_native_id_pattern_map = defaultdict(set)
    for native_id_pattern in cmr_native_id_patterns:
        product_type = "RTC" if "RTC" in native_id_pattern else "CSLC"
        if product_type == "CSLC":
            acquisition_time = native_id_pattern[23:38]
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
    cmr_end_dt_str = args.end_datetime

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
    if not missing_cmr_granules_slc_cslc:
        missing_cmr_granules_slc_cslc = set()
    else:
        missing_cmr_granules_slc_cslc = set(functools.reduce(set.union, missing_cmr_granules_slc_cslc))

    missing_cmr_granules_slc_rtc = [output_rtc_to_inputs_slc_map[native_id_pattern] for native_id_pattern in missing_rtc_native_id_patterns]
    if not missing_cmr_granules_slc_rtc:
        missing_cmr_granules_slc_rtc = set()
    else:
        missing_cmr_granules_slc_rtc = set(functools.reduce(set.union, missing_cmr_granules_slc_rtc))

    # logger.debug(f"{pstr(missing_slc)=!s}")
    logger.info(f"Expected input (granules): {len(cmr_granules_slc)=:,}")
    logger.info(f"Fully published (granules) (CSLC): {len(cmr_cslc_products)=:,}")
    logger.info(f"Fully published (granules) (RTC): {len(cmr_rtc_products)=:,}")
    logger.info(f"Missing processed CSLC (granules): {len(missing_cmr_granules_slc_cslc)=:,}")
    logger.info(f"Missing processed RTC (granules): {len(missing_cmr_granules_slc_rtc)=:,}")

    if args.format == "txt":
        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - SLC to CSLC - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_slc_cslc))
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - SLC to RTC - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_slc_cslc))
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

    elif args.format == "json":
        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - SLC to CSLC - {cmr_start_dt_str} to {cmr_end_dt_str}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_slc_cslc))
            fp.write(json_str)
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - SLC to RTC - {cmr_start_dt_str} to {cmr_end_dt_str}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_slc_rtc))
            fp.write(json_str)
        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")
    else:
        raise Exception()


if __name__ == "__main__":
    asyncio.run(run(sys.argv))
