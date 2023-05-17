import argparse
import asyncio
import datetime
import functools
import logging
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from io import StringIO
from pprint import pprint
from typing import Iterable, Union

import aiohttp
import dateutil.parser
import more_itertools
from dateutil.rrule import rrule, DAILY, HOURLY
from dotenv import dotenv_values
from more_itertools import always_iterable

from tools.cmr_audit.cmr_client import async_cmr_post

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG)
logging.getLogger()

config = {
    **dotenv_values("../../.env"),
    **os.environ
}


def pstr(o):
    sio = StringIO()
    pprint(o, stream=sio)
    sio.seek(0)
    return sio.read()


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

logging.info(f'{sys.argv=}')
args = argparser.parse_args(sys.argv[1:])


#######################################################################
# CMR AUDIT
#######################################################################

async def async_get_cmr_granules_slc_s1a(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules("SENTINEL-1A_SLC",
                                  temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                  platform_short_name="SENTINEL-1A")


async def async_get_cmr_granules_slc_s1b(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules("SENTINEL-1B_SLC",
                                  temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                  platform_short_name="SENTINEL-1B")


async def async_get_cmr_granules_hls_l30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSL30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name="LANDSAT-8")


async def async_get_cmr_granules_hls_s30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSS30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name=["Sentinel-2A", "Sentinel-2B"])


async def async_get_cmr_granules(collection_short_name, temporal_date_start: str, temporal_date_end: str,
                                 platform_short_name: Union[str, Iterable[str]]):
    logging.debug(f"entry({collection_short_name=}, {temporal_date_start=}, {temporal_date_end=}, {platform_short_name})")

    async with aiohttp.ClientSession() as session:
        request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

        # cycle through subset timeranges to split up requests
        post_cmr_tasks = []

        temporal_start_dt = dateutil.parser.isoparse(temporal_date_start)
        temporal_end_dt = dateutil.parser.isoparse(temporal_date_end)
        range_days = rrule(freq=DAILY, dtstart=temporal_start_dt, interval=1, until=temporal_end_dt)
        for day in range_days:
            logging.debug(f"{day=!s}")
            if day >= temporal_end_dt:
                logging.debug(f"Current day is beyond the global end datetime. Skipping")
                break

            # NOTE: keep freq+interval and duration in sync
            range_hours = rrule(freq=HOURLY, dtstart=day, interval=12, until=day + datetime.timedelta(days=1))
            duration = datetime.timedelta(hours=12)
            for hour in range_hours:
                logging.debug(f"{hour=!s}")

                if hour == temporal_end_dt:  # reached global end datetime
                    logging.debug("EDGECASE: hour matches global end datetime. Skipping")
                    break
                if hour >= day + datetime.timedelta(days=1):  # current hour goes into the next day. skip and let next day iteration handle.
                    logging.debug("EDGECASE: hour goes into next day (i.e. next outer loop iteration). Skipping")
                    break

                local_start_dt_str = hour.isoformat(timespec="milliseconds")
                if temporal_end_dt < hour + duration:  # if on last partial hour, use global end datetime
                    logging.debug("Clamping local end datetime for trailing partial duration")
                    local_end_dt = temporal_end_dt
                else:
                    local_end_dt = hour + duration
                local_end_dt_str = local_end_dt.isoformat(timespec="milliseconds")

                request_body = request_body_supplier(collection_short_name, temporal_date_start=local_start_dt_str, temporal_date_end=local_end_dt_str, platform_short_name=platform_short_name)
                logging.debug(f"Creating request task for {local_start_dt_str=}, {local_end_dt_str=}")
                post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))

                if local_end_dt == temporal_end_dt:  # processed last partial hour. prevent further iterations.
                    logging.debug("EDGECASE: processed last partial hour. Preempting")
                    break

        logging.debug(f"Number of query requests to make: {len(post_cmr_tasks)=}")

        logging.debug("Batching tasks")
        cmr_granules = set()
        cmr_granules_details = {}
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, 30))
        for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
            logging.debug(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False))
            for post_cmr_tasks_result in post_cmr_tasks_results:
                cmr_granules.update(post_cmr_tasks_result[0])
            # DEV: uncomment as needed
                cmr_granules_details.update(post_cmr_tasks_result[1])

        logging.info(f"{collection_short_name} {len(cmr_granules)=:,}")
        return cmr_granules, cmr_granules_details


def request_body_supplier(collection_short_name, temporal_date_start: str, temporal_date_end: str, platform_short_name: Union[str, Iterable[str]]):
    if collection_short_name == "HLSL30" or collection_short_name == "HLSS30":
        return (
            "provider=LPCLOUD"
            f"&ShortName[]={collection_short_name}"
            "&bounding_box=-180,-90,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            "&options[platform][exclude_collection]=true"
            f'{"&platform[]=" + "&platform[]=".join(always_iterable(platform_short_name))}'
            ""
        )
    if collection_short_name == "SENTINEL-1A_SLC" or collection_short_name == "SENTINEL-1B_SLC":
        return (
            "provider=ASF"
            f"&ShortName[]={collection_short_name}"
            "&bounding_box=-180,-90,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            f'{"&platform[]=" + "&platform[]=".join(always_iterable(platform_short_name))}'
            "&attribute[]=string,BEAM_MODE,IW"
        )
    raise Exception(f"Unsupported collection short name. {collection_short_name=}")



async def async_get_cmr_dswx(dswx_native_id_patterns: set):
    logging.debug(f"entry({len(dswx_native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    dswx_native_id_patterns = more_itertools.always_iterable(dswx_native_id_patterns)
    dswx_native_id_pattern_batches = more_itertools.chunked(dswx_native_id_patterns, 1000)  # 1000 == 55,100 length

    request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for dswx_native_id_pattern_batch in dswx_native_id_pattern_batches:
            dswx_native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(dswx_native_id_pattern_batch)

            request_body = (
                "provider=POCLOUD"
                "&ShortName[]=OPERA_L3_DSWX-HLS_PROVISIONAL_V1"
                "&options[native-id][pattern]=true"
                f"{dswx_native_id_patterns_query_params}"
            )
            logging.debug(f"Creating request task")
            post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))
        logging.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        logging.debug("Batching tasks")
        dswx_granules = set()
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, 30))
        for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
            logging.debug(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                dswx_granules.update(post_cmr_tasks_result[0])
        return dswx_granules


async def async_get_cmr_rtc(rtc_native_id_patterns: set):
    logging.debug(f"entry({len(rtc_native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    rtc_native_id_patterns = more_itertools.always_iterable(rtc_native_id_patterns)
    rtc_native_id_pattern_batches = more_itertools.chunked(rtc_native_id_patterns, 1000)  # 1000 == 55,100 length

    request_url = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"

    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for rtc_native_id_pattern_batch in rtc_native_id_pattern_batches:
            dswx_native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(rtc_native_id_pattern_batch)

            request_body = (
                "provider=ASF"
                "&ShortName[]=OPERA_RTC_S1""&ShortName[]=OPERA_CSLC_S1"  # TODO chrisjrd: separate?
                "&options[native-id][pattern]=true"
                f"{dswx_native_id_patterns_query_params}"
            )
            logging.debug(f"Creating request task")
            post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))
        logging.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        logging.debug("Batching tasks")
        rtc_granules = set()
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, 30))
        for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
            logging.debug(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                rtc_granules.update(post_cmr_tasks_result[0])
        return rtc_granules


def raise_(ex: Exception):
    raise ex


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

        # bi-directional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(rtc_native_id_pattern)  # strip wildcard char
        output_to_inputs_map[rtc_native_id_pattern].add(granule)

    return rtc_native_id_patterns


def hls_granule_ids_to_dswx_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    dswx_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(
            r'(?P<product_shortname>HLS[.]([LS])30)[.]'
            r'(?P<tile_id>T[^\W_]{5})[.]'
            r'(?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.]'
            r'(?P<collection_version>v\d+[.]\d+)$',
            granule
        )
        tile = m.group("tile_id")
        year = m.group("year")
        doy = m.group("day_of_year")
        time_of_day = m.group("acquisition_ts").split("T")[1]
        date = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)
        dswx_acquisition_dt_str = f"{date.strftime('%Y%m%d')}T{time_of_day}"

        dswx_native_id_pattern = f'OPERA_L3_DSWx-HLS_{tile}_{dswx_acquisition_dt_str}Z_*'
        dswx_native_id_patterns.add(dswx_native_id_pattern)

        # bi-directional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(dswx_native_id_pattern[:-1])  # strip wildcard char
        output_to_inputs_map[dswx_native_id_pattern[:-1]].add(granule)

    return dswx_native_id_patterns


def dswx_native_ids_to_prefixes(cmr_dswx_native_ids):
    dswx_regex_pattern = (
        r'(?P<project>OPERA)_'
        r'(?P<level>L3)_'
        r'(?P<product_type>DSWx)-(?P<source>HLS)_'
        r'(?P<tile_id>T[^\W_]{5})_'
        r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    )
    return {re.match(dswx_regex_pattern, prefix).group(0) for prefix in cmr_dswx_native_ids}


def to_dsxw_metadata_small(missing_cmr_granules, cmr_granules_details, input_hls_to_outputs_dswx_map):
    missing_cmr_granules_details_short = [
        {
            "id": i,
            "expected-dswx-id-prefix": next(iter(input_hls_to_outputs_dswx_map[i])),
            "revision-date": cmr_granules_details[i]["meta"]["revision-date"],
            # TODO chrisjrd: commented out for ad-hoc request. 5/10/2023
            # "provider-date": next(iter(
            #     cmr_granules_details[i]["umm"]["ProviderDates"]
            # ))["Date"],
            # "temporal-date": cmr_granules_details[i]["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"],
            # "hls-processing-time": next(iter(
            #     next(iter(
            #         list(filter(lambda a: a["Name"] == "HLS_PROCESSING_TIME",
            #                     cmr_granules_details[i]["umm"]["AdditionalAttributes"]))
            #     ))["Values"]
            # )),
            "sensing-time": next(iter(
                next(iter(
                    list(filter(lambda a: a["Name"] == "SENSING_TIME",
                                cmr_granules_details[i]["umm"]["AdditionalAttributes"]))
                ))["Values"]
            ))
        }
        for i in missing_cmr_granules
    ]

    return missing_cmr_granules_details_short


loop = asyncio.get_event_loop()

logging.info("Querying CMR for list of expected L30 and S30 granules (HLS)")
cmr_start_dt_str = args.start_datetime
cmr_end_dt_str = args.end_datetime

cmr_granules_slc_s1a, cmr_granules_slc_s1a_details = loop.run_until_complete(
    async_get_cmr_granules_slc_s1a(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))
cmr_granules_slc_s1b, cmr_granules_slc_s1b_details = loop.run_until_complete(
    async_get_cmr_granules_slc_s1b(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))

cmr_granules_slc = cmr_granules_slc_s1a.union(cmr_granules_slc_s1b)
cmr_granules_details = {}; cmr_granules_details.update(cmr_granules_slc_s1a_details); cmr_granules_details.update(cmr_granules_slc_s1b_details)
logging.info(f"Expected input (granules): {len(cmr_granules_slc)=:,}")

rtc_native_id_patterns = slc_granule_ids_to_rtc_native_id_patterns(
    cmr_granules_slc,
    input_slc_to_outputs_rtc_map := defaultdict(set),
    output_rtc_to_inputs_slc_map := defaultdict(set)
)

logging.info("Querying CMR for list of expected RTC granules")
cmr_rtc_products = loop.run_until_complete(async_get_cmr_rtc(rtc_native_id_patterns))

rtc_native_id_regexps = {x.replace("*", "(.+)").replace("?", "(.)") for x in rtc_native_id_patterns}

found_rtc_regexps = set()
for i, cmr_rtc_product in enumerate(cmr_rtc_products, start=1):
    logging.debug(f"Validating {i} of {len(cmr_rtc_products)}: {cmr_rtc_product=}")
    for rtc_native_id_regexp in rtc_native_id_regexps:
        if re.match(rtc_native_id_regexp, cmr_rtc_product) is not None:
            found_rtc_regexps.add(rtc_native_id_regexp)
            rtc_native_id_regexps.remove(rtc_native_id_regexp)  # OPTIMIZATION: remove already processed regex
            break
missing_rtc_regexp = rtc_native_id_regexps  # only missing regexes left
missing_rtc_native_id_patterns = {x.replace("(.+)", "*").replace("(.)", "?") for x in missing_rtc_regexp}

cmr_granules_l30, cmr_granules_l30_details = loop.run_until_complete(
    async_get_cmr_granules_hls_l30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))
cmr_granules_s30, cmr_granules_s30_details = loop.run_until_complete(
    async_get_cmr_granules_hls_s30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))

cmr_granules_hls = cmr_granules_l30.union(cmr_granules_s30)
cmr_granules_details = {}; cmr_granules_details.update(cmr_granules_l30_details); cmr_granules_details.update(cmr_granules_s30_details)
logging.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")

dswx_native_id_patterns = hls_granule_ids_to_dswx_native_id_patterns(
    cmr_granules_hls,
    input_hls_to_outputs_dswx_map := defaultdict(set),
    output_dswx_to_inputs_hls_map := defaultdict(set)
)

logging.info("Querying CMR for list of expected DSWx granules")
cmr_dswx_products = loop.run_until_complete(async_get_cmr_dswx(dswx_native_id_patterns))

cmr_dswx_prefix_expected = {prefix[:-1] for prefix in dswx_native_id_patterns}
cmr_dswx_prefix_actual = dswx_native_ids_to_prefixes(cmr_dswx_products)
missing_cmr_dswx_granules_prefixes = cmr_dswx_prefix_expected - cmr_dswx_prefix_actual

#######################################################################
# CMR_AUDIT SUMMARY
#######################################################################
# logging.debug(f"{pstr(missing_rtc_native_id_patterns)=!s}")

missing_cmr_granules_slc = set(functools.reduce(set.union, [output_rtc_to_inputs_slc_map[x] for x in missing_rtc_native_id_patterns]))

# logging.debug(f"{pstr(missing_slc)=!s}")
logging.info(f"Expected input (granules): {len(cmr_granules_slc)=:,}")
logging.info(f"Fully published (granules): {len(cmr_rtc_products)=:,}")
logging.info(f"Missing processed (granules): {len(missing_cmr_granules_slc)=:,}")

if args.format == "txt":
    output_file_missing_cmr_granules = args.output if args.output else f"missing granules - RTC - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
    logging.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
    with open(output_file_missing_cmr_granules, mode='w') as fp:
        fp.write('\n'.join(missing_cmr_granules_slc))
elif args.format == "json":
    output_file_missing_cmr_granules = args.output if args.output else f"missing granules - RTC - {cmr_start_dt_str} to {cmr_end_dt_str}.json"
    with open(output_file_missing_cmr_granules, mode='w') as fp:
        from compact_json import Formatter
        formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
        json_str = formatter.serialize(list(missing_cmr_granules_slc))
        fp.write(json_str)
else:
    raise Exception()

logging.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

# logging.debug(f"{pstr(missing_cmr_dswx_granules_prefixes)=!s}")

missing_cmr_granules = {next(iter(output_dswx_to_inputs_hls_map[prefix])) for prefix in missing_cmr_dswx_granules_prefixes}

# logging.debug(f"{pstr(missing_cmr_granules)=!s}")
logging.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")
logging.info(f"Fully published (granules): {len(cmr_dswx_products)=:,}")
logging.info(f"Missing processed (granules): {len(missing_cmr_granules)=:,}")

if args.format == "txt":
    output_file_missing_cmr_granules = args.output if args.output else f"missing granules - DSWx - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
    logging.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
    with open(output_file_missing_cmr_granules, mode='w') as fp:
        fp.write('\n'.join(missing_cmr_granules))
elif args.format == "json":
    output_file_missing_cmr_granules = args.output if args.output else f"missing granules - DSWx - {cmr_start_dt_str} to {cmr_end_dt_str}.json"
    with open(output_file_missing_cmr_granules, mode='w') as fp:
        from compact_json import Formatter
        formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
        json_str = formatter.serialize(list(missing_cmr_granules))
        fp.write(json_str)
else:
    raise Exception()

logging.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

# DEV: uncomment to export granules and metadata
# missing_cmr_granules_details_short = to_dsxw_metadata_small(missing_cmr_granules, cmr_granules_details, input_hls_to_outputs_dswx_map)
# with open(output_file_missing_cmr_granules.replace(".json", " - details.json"), mode='w') as fp:
#     from compact_json import Formatter
#     formatter = Formatter(indent_spaces=2, max_inline_length=300)
#     json_str = formatter.serialize(missing_cmr_granules_details_short)
#     fp.write(json_str)
