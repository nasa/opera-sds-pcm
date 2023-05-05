import argparse
import asyncio
import datetime
import logging
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from io import StringIO
from math import ceil
from pprint import pprint

import aiohttp
import backoff
import dateutil.parser
import more_itertools
from dateutil.rrule import rrule, DAILY, HOURLY
from dotenv import dotenv_values

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logging.getLogger()

config = {
    **dotenv_values("../.env"),
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
    help=f'ISO formatted datetime string. Must be compatible with CMR. Defaults to "%(default)s".'
)
argparser.add_argument(
    "--end-datetime",
    required=True,
    help=f'ISO formatted datetime string. Must be compatible with CMR. Defaults to "%(default)s".'
)
argparser.add_argument(
    "--output", "-o",
    help=f'ISO formatted datetime string. Must be compatible with CMR. Defaults to "%(default)s".'
)

logging.info(f'{sys.argv=}')
args = argparser.parse_args(sys.argv[1:])


#######################################################################
# CMR AUDIT
#######################################################################

async def async_get_cmr_granules(collection_short_name, temporal_date_start: str, temporal_date_end: str):
    logging.debug(f"entry({collection_short_name=}, {temporal_date_start=}, {temporal_date_end=})")

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
                logging.debug(f"Current day is beyond the global end datetime")
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

                request_body = (
                        "provider=LPCLOUD"
                        f"&ShortName[]={collection_short_name}"
                        "&bounding_box=-180,-90,180,90"
                        "&sort_key=-start_date"
                        # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
                        f"&temporal[]={urllib.parse.quote(local_start_dt_str, safe='/:')},{urllib.parse.quote(local_end_dt_str, safe='/:')}"
                        ""
                )
                post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))

                if local_end_dt == temporal_end_dt:  # processed last partial hour. prevent further iterations.
                    logging.debug("EDGECASE: processed last partial hour. Preempting")
                    break

        logging.debug(f"Number of query requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        cmr_granules = set()
        cmr_granules_details = {}
        for task_chunk in more_itertools.chunked(post_cmr_tasks, 5):  # CMR recommends 2-5 threads.
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(lambda it: isinstance(it, Exception), await asyncio.gather(*task_chunk, return_exceptions=False))
            post_cmr_tasks_results = next(post_cmr_tasks_results)
            cmr_granules = cmr_granules.union(post_cmr_tasks_results[0])
            cmr_granules_details.update(post_cmr_tasks_results[1])
        return cmr_granules, cmr_granules_details


async def async_get_cmr_dswx(dswx_native_id_patterns: set):
    logging.debug(f"entry({len(dswx_native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    dswx_native_id_patterns = more_itertools.always_iterable(dswx_native_id_patterns)
    dswx_native_id_pattern_batches = more_itertools.chunked(dswx_native_id_patterns, 1000)  # 100 == 5,700 char URL, 1000 == 55,100

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
            post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))
        logging.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        dswx_granules = set()
        for task_chunk in more_itertools.chunked(post_cmr_tasks, 5):  # CMR recommends 2-5 threads.
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(lambda it: isinstance(it, Exception), await asyncio.gather(*task_chunk, return_exceptions=False))
            post_cmr_tasks_results = next(post_cmr_tasks_results)
            dswx_granules = dswx_granules.union(post_cmr_tasks_results[0])
        return dswx_granules


async def async_cmr_post(url, data: str, session: aiohttp.ClientSession):
    page_size = 2000  # default is 10, max is 2000
    data += f"&page_size={page_size}"

    logging.debug(f"async_post_cmr({url=}..., {len(data)=:,}, {data[-250:]=}")

    cmr_granules = set()
    cmr_granules_detailed = {}
    # page_num, offset (0-based), page_size, sort_key
    # You can not page past the 1 millionth item.
    # Additionally granule queries which do not target a set of collections are limited to paging up to the 10000th item.
    max_pages = int(100_000/2000)  # cap the number of pages (requests) to scroll through results. CMR's hard limit is page_size * max_pages <= 100,000

    current_page = 1
    cmr_search_after = ""
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    while current_page <= max_pages:
        async with await fetch_post_url(session, url, data, headers) as response:
            response_json = await response.json()

        if current_page == 1:
            logging.info(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
        logging.debug(f'CMR number of granules (cmr-query-page {current_page}/{ceil(response_json["hits"]/page_size)}): {len(response_json["items"])=:,}')
        cmr_granules = cmr_granules.union({item["meta"]["native-id"] for item in response_json["items"]})
        cmr_granules_detailed.update({item["meta"]["native-id"]: item for item in response_json["items"]})

        cmr_search_after = response.headers.get("CMR-Search-After")
        logging.debug(f"{cmr_search_after=}")
        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

        if len(response_json["items"]) < page_size:
            logging.debug("reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if not current_page <= max_pages:
            logging.warning("Reached max pages limit. Not all search results exhausted. Adjust limit or time ranges to process all hits, then re-run this script.")

    return cmr_granules, cmr_granules_detailed


def giveup(e):
    if isinstance(e, aiohttp.ClientResponseError):
        if e.status == 413 and e.message == "Payload Too Large":  # give up. Fix bug
            return True
        if e.status == 400:  # Bad Requesst. give up. Fix bug
            return True
        if e.status == 504 and e.message == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    return False


@backoff.on_exception(
    backoff.expo,
    exception=(aiohttp.ClientResponseError, aiohttp.ClientOSError),  # ClientOSError happens when connection is closed by peer
    max_tries=3,
    jitter=None,
    giveup=giveup
)
async def fetch_post_url(session: aiohttp.ClientSession, url, data: str, headers):
    return await session.post(url, data=data, headers=headers, raise_for_status=True)


def raise_(ex: Exception):
    raise ex


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


def to_dsxw_metadata_small():
    missing_cmr_granules_details_short = [
        {
            "id": i,
            "revision-date": cmr_granules_details[i]["meta"]["revision-date"],
            "provider-date": next(iter(
                cmr_granules_details[i]["umm"]["ProviderDates"]
            ))["Date"],
            "temporal-date": cmr_granules_details[i]["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"],
            "hls-processing-time": next(iter(
                next(iter(
                    list(filter(lambda a: a["Name"] == "HLS_PROCESSING_TIME",
                                cmr_granules_details[i]["umm"]["AdditionalAttributes"]))
                ))["Values"]
            )),
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

cmr_granules_l30, cmr_granules_l30_details = loop.run_until_complete(async_get_cmr_granules(collection_short_name="HLSL30", temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))
cmr_granules_s30, cmr_granules_s30_details = loop.run_until_complete(async_get_cmr_granules(collection_short_name="HLSS30", temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str))
cmr_granules = cmr_granules_l30.union(cmr_granules_s30)
cmr_granules_details = {}; cmr_granules_details.update(cmr_granules_l30_details); cmr_granules_details.update(cmr_granules_s30_details)
logging.info(f"Expected input (granules): {len(cmr_granules)=:,}")

dswx_native_id_patterns = hls_granule_ids_to_dswx_native_id_patterns(
    cmr_granules,
    input_hls_to_outputs_dswx_map := defaultdict(set),
    output_dswx_to_inputs_hls_map := defaultdict(set)
)

logging.info("Querying CMR for list of expected DSWx granules")
cmr_dswx_products = loop.run_until_complete(async_get_cmr_dswx(dswx_native_id_patterns))

dswx_regex_pattern = (
    r'(?P<project>OPERA)_'
    r'(?P<level>L3)_'
    r'(?P<product_type>DSWx)-(?P<source>HLS)_'
    r'(?P<tile_id>T[^\W_]{5})_'
    r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
)

cmr_dswx_prefix_expected = {prefix[:-1] for prefix in dswx_native_id_patterns}
cmr_dswx_prefix_actual = {re.match(dswx_regex_pattern, prefix).group(0) for prefix in cmr_dswx_products}
missing_cmr_dswx_granules_prefixes = cmr_dswx_prefix_expected - cmr_dswx_prefix_actual

#######################################################################
# CMR_AUDIT SUMMARY
#######################################################################
logging.debug(f"{pstr(missing_cmr_dswx_granules_prefixes)=!s}")

missing_cmr_granules = {next(iter(output_dswx_to_inputs_hls_map[prefix])) for prefix in missing_cmr_dswx_granules_prefixes}

logging.info(f"{pstr(missing_cmr_granules)=!s}")
logging.info(f"Expected input (granules): {len(cmr_granules)=:,}")
logging.info(f"Fully published (granules): {len(cmr_dswx_products)=:,}")
logging.info(f"Missing processed (granules): {len(missing_cmr_granules)=:,}")
logging.info(f"Missing processed (granules): {len(missing_cmr_granules)=:,}")

missing_cmr_granules_details_full = [cmr_granules_details[i] for i in missing_cmr_granules]
missing_cmr_granules_details_short = to_dsxw_metadata_small()

output_file_missing_cmr_granules = args.output if args.output else f"missing granules - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
logging.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
with open(output_file_missing_cmr_granules, mode='w') as fp:
    fp.write('\n'.join(missing_cmr_granules))
    # DEV: uncomment to export granules and metadata
    # from compact_json import Formatter
    # formatter = Formatter(indent_spaces=2, max_inline_length=300)
    # json_str = formatter.serialize(missing_cmr_granules_details_short)
    # fp.write(json_str)
logging.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")