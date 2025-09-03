import argparse
import asyncio
import concurrent.futures
import functools
import logging
import logging.handlers
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from typing import Union, Iterable

import aiohttp
import more_itertools
from opera_commons.datetime_utils import parse_iso_datetime
from dotenv import dotenv_values
from more_itertools import always_iterable

from tools.ops.cmr_audit.cmr_audit_utils import str2bool
from geo.geo_util import does_bbox_intersect_north_america
from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules, get_cmr_audit_granules, init_logging

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.getLogger("geo.geo_util").setLevel(level=logging.WARNING)

config = {
    **dotenv_values("../../.env"),
    **os.environ
}

def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument(
        "--start-datetime",
        required=True,
        type = argparse_dt,
        help=f"ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00Z"
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        type = argparse_dt,
        help=f"ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00Z",
        default=datetime.now(timezone.utc)
    )
    argparser.add_argument(
        "--output", "-o",
        help=f'Output filepath.'
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    argparser.add_argument(
        "--do_cslc",
        type=str2bool,
        default=True,
        help=f'Flag to execute CSLC accountability. Defaults to "%(default)s".'
    )
    argparser.add_argument(
        "--do_rtc",
        type=str2bool,
        default=True,
        help=f'Flag to execute RTC accountability. Defaults to "%(default)s".'
    )
    argparser.add_argument('--log-level', default='INFO', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))

    return argparser

def argparse_dt(dt_str):
    dt = parse_iso_datetime(dt_str)
    if not dt.tzinfo:
        raise ValueError()
    return dt


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

        #                         OPERA_L2_CSLC-S1_*_20231124T124529Z_*_S1*
        rtc_native_id_pattern = f'OPERA_L2_CSLC-S1_*_{cslc_acquisition_dt_str}Z_*_S1*v1.1'
        rtc_native_id_patterns.add(rtc_native_id_pattern)

        # bi-directional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(rtc_native_id_pattern)  # strip wildcard char
        output_to_inputs_map[rtc_native_id_pattern].add(granule)

    return rtc_native_id_patterns


def slc_granule_ids_to_rtc_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    rtc_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(
            r'(?P<mission_id>S1A|S1B|S1C)_'
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

def get_out_filename(cmr_start_dt_str, cmr_end_dt_str, product, input="SLC"):

    now = datetime.now(timezone.utc)
    current_dt_str = now.strftime("%Y%m%d-%H%M%S")
    start_dt_str = cmr_start_dt_str.replace("-", "")
    start_dt_str = start_dt_str.replace("T", "-")
    start_dt_str = start_dt_str.replace(":", "")
    start_dt_str = start_dt_str.replace("Z", "")

    end_dt_str = cmr_end_dt_str.replace("-", "")
    end_dt_str = end_dt_str.replace("T", "-")
    end_dt_str = end_dt_str.replace(":", "")
    end_dt_str = end_dt_str.replace("Z", "")
    out_filename = f"{start_dt_str}Z_{end_dt_str}Z_{current_dt_str}Z"

    return f"missing_granules_{input}-{product}_{out_filename}"

def write_missing_products_to_file(out_filename, missing_cmr_granules):

    out_file_missing_cmr_granules = args.output if args.output else f"{out_filename}.{args.format}"
    logger.info(f"Writing granule list to file {out_file_missing_cmr_granules!r}")

    if args.format == "txt":

        with open(out_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules))
        if missing_cmr_granules:
            with open(out_file_missing_cmr_granules, mode='a') as fp:
                fp.write('\n')

    elif args.format == "json":

        with open(out_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300,
                                  max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules))
            fp.write(json_str)

    else:
        raise Exception(f"Unrecognized output format {args.format}")

    logger.info(f"Finished writing to file {out_file_missing_cmr_granules!r}")

#######################################################################
# CMR AUDIT
#######################################################################

async def run(start_datetime: datetime = None, end_datetime: datetime = None, do_cslc=False, do_rtc=False, **kwargs):

    logger.info("Querying CMR for list of expected SLC granules")
    cmr_start_dt_str = start_datetime.replace(tzinfo=None).isoformat()
    cmr_end_dt_str = end_datetime.replace(tzinfo=None).isoformat()

    cmr_granules_slc_s1a, cmr_granules_slc_s1a_details = await async_get_cmr_granules_slc_s1a(
        temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)
    cmr_granules_slc_s1b, cmr_granules_slc_s1b_details = await async_get_cmr_granules_slc_s1b(
        temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    cmr_granules_slc = cmr_granules_slc_s1a.union(cmr_granules_slc_s1b)
    cmr_granules_slc_details = {}
    cmr_granules_slc_details.update(cmr_granules_slc_s1a_details)
    cmr_granules_slc_details.update(cmr_granules_slc_s1b_details)

    logger.info(f"Expected input (granules): {len(cmr_granules_slc)=:,}")

    if do_cslc:
        logger.info("Filtering North America granules")
        cmr_granules_slc_na = set()
        cmr_granules_slc_details_na = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            does_bbox_intersect_north_america(
                [])  # DEV: optimization to force initial cache load of geojson file
            future_to_granule_map = {}
            for granule_id, granule_details in cmr_granules_slc_details.items():
                bounding_box = [
                    {"lat": point["Latitude"], "lon": point["Longitude"]}
                    for point in
                    granule_details["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"][
                        "GPolygons"][0]["Boundary"]["Points"]
                ]
                future_to_granule_map[
                    executor.submit(does_bbox_intersect_north_america, bbox=bounding_box)] = (
                    granule_id, granule_details)

            for future in concurrent.futures.as_completed(future_to_granule_map):
                is_granule_in_na = future.result()
                if is_granule_in_na:
                    granule_id, granule_details = future_to_granule_map[future]
                    cmr_granules_slc_na.add(granule_id)
                    cmr_granules_slc_details_na[granule_id] = granule_details

        logger.info(f"Expected CSLC input (granules): {len(cmr_granules_slc_na)=:,}")

        cslc_native_id_patterns = slc_granule_ids_to_cslc_native_id_patterns(
            cmr_granules_slc_na,
            input_slc_to_outputs_cslc_map := defaultdict(set),
            output_cslc_to_inputs_slc_map := defaultdict(set)
        )

        logger.info("Querying CMR for list of expected CSLC granules")
        cmr_cslc_products = await async_get_cmr_cslc(cslc_native_id_patterns,
                                                     temporal_date_start=cmr_start_dt_str,
                                                     temporal_date_end=cmr_end_dt_str)

        missing_cslc_native_id_patterns = cmr_products_native_id_pattern_diff(
            cmr_products=cmr_cslc_products, cmr_native_id_patterns=cslc_native_id_patterns)

        missing_cmr_granules_slc_cslc = [output_cslc_to_inputs_slc_map[native_id_pattern] for
                                         native_id_pattern in missing_cslc_native_id_patterns]
        missing_cmr_granules_slc_cslc = set(
            functools.reduce(set.union,
                             missing_cmr_granules_slc_cslc)) if missing_cmr_granules_slc_cslc else set()

        logger.info(f"Fully published (granules) (CSLC): {len(cmr_cslc_products)=:,}")
        logger.info(f"Missing processed CSLC (granules): {len(missing_cmr_granules_slc_cslc)=:,}")

        out_filename = get_out_filename(cmr_start_dt_str, cmr_end_dt_str, "CSLC")
        write_missing_products_to_file(out_filename, missing_cmr_granules_slc_cslc)

    if do_rtc:

        logger.info(f"Expected RTC input (granules): {len(cmr_granules_slc)=:,}")

        rtc_native_id_patterns = slc_granule_ids_to_rtc_native_id_patterns(
            cmr_granules_slc,
            input_slc_to_outputs_rtc_map := defaultdict(set),
            output_rtc_to_inputs_slc_map := defaultdict(set)
        )

        logger.info("Querying CMR for list of expected RTC granules")
        cmr_rtc_products = await async_get_cmr_rtc(rtc_native_id_patterns,
                                                   temporal_date_start=cmr_start_dt_str,
                                                   temporal_date_end=cmr_end_dt_str)

        missing_rtc_native_id_patterns = cmr_products_native_id_pattern_diff(
            cmr_products=cmr_rtc_products, cmr_native_id_patterns=rtc_native_id_patterns)

        missing_cmr_granules_slc_rtc = [output_rtc_to_inputs_slc_map[native_id_pattern] for
                                        native_id_pattern in missing_rtc_native_id_patterns]
        missing_cmr_granules_slc_rtc = set(
            functools.reduce(set.union,
                             missing_cmr_granules_slc_rtc)) if missing_cmr_granules_slc_rtc else set()

        logger.info(f"Fully published (granules) (RTC): {len(cmr_rtc_products)=:,}")
        logger.info(f"Missing processed RTC (granules): {len(missing_cmr_granules_slc_rtc)=:,}")

        out_filename = get_out_filename(cmr_start_dt_str, cmr_end_dt_str, "RTC")
        write_missing_products_to_file(out_filename, missing_cmr_granules_slc_rtc)


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging('cmr_audit_slc.log', 'cmr_audit_slc-error.log', level=args.log_level)
    logger = logging.getLogger(__name__)

    asyncio.run(run(**args.__dict__))
