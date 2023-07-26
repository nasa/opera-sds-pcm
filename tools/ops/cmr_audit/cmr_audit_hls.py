import argparse
import asyncio
import datetime
import functools
import logging
import os
import re
import sys
from collections import defaultdict

import aiohttp
import more_itertools
from dotenv import dotenv_values

from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules
from tools.ops.cmr_audit.cmr_client import async_cmr_post

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG)
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

async def async_get_cmr_granules_hls_l30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSL30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name="LANDSAT-8")


async def async_get_cmr_granules_hls_s30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSS30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name=["Sentinel-2A", "Sentinel-2B"])



async def async_get_cmr_dswx(dswx_native_id_patterns: set):
    logger.debug(f"entry({len(dswx_native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    dswx_native_id_patterns = more_itertools.always_iterable(dswx_native_id_patterns)
    dswx_native_id_pattern_batches = list(more_itertools.chunked(dswx_native_id_patterns, 1000))  # 1000 == 55,100 length

    request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for i, dswx_native_id_pattern_batch in enumerate(dswx_native_id_pattern_batches, start=1):
            dswx_native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(dswx_native_id_pattern_batch)

            request_body = (
                "provider=POCLOUD"
                "&short_name[]=OPERA_L3_DSWX-HLS_PROVISIONAL_V1"
                "&options[native-id][pattern]=true"
                f"{dswx_native_id_patterns_query_params}"
            )
            logger.debug(f"Creating request task {i} of {len(dswx_native_id_pattern_batches)}")
            post_cmr_tasks.append(async_cmr_post(request_url, request_body, session))
        logger.debug(f"Number of requests to make: {len(post_cmr_tasks)=}")

        # issue requests in batches
        logger.debug("Batching tasks")
        dswx_granules = set()
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, 30))
        for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
            logger.info(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                dswx_granules.update(post_cmr_tasks_result[0])
        return dswx_granules


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

#######################################################################
# CMR AUDIT
#######################################################################

async def run(argv: list[str]):
    logger.info(f'{argv=}')
    args = create_parser().parse_args(argv[1:])

    logger.info("Querying CMR for list of expected L30 and S30 granules (HLS)")
    cmr_start_dt_str = args.start_datetime
    cmr_end_dt_str = args.end_datetime

    cmr_granules_l30, cmr_granules_l30_details = await async_get_cmr_granules_hls_l30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)
    cmr_granules_s30, cmr_granules_s30_details = await async_get_cmr_granules_hls_s30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    cmr_granules_hls = cmr_granules_l30.union(cmr_granules_s30)
    cmr_granules_details = {}; cmr_granules_details.update(cmr_granules_l30_details); cmr_granules_details.update(cmr_granules_s30_details)
    logger.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")

    dswx_native_id_patterns = hls_granule_ids_to_dswx_native_id_patterns(
        cmr_granules_hls,
        input_hls_to_outputs_dswx_map := defaultdict(set),
        output_dswx_to_inputs_hls_map := defaultdict(set)
    )

    logger.info("Querying CMR for list of expected DSWx granules")
    cmr_dswx_products = await async_get_cmr_dswx(dswx_native_id_patterns)

    cmr_dswx_prefix_expected = {prefix[:-1] for prefix in dswx_native_id_patterns}
    cmr_dswx_prefix_actual = dswx_native_ids_to_prefixes(cmr_dswx_products)
    missing_cmr_dswx_granules_prefixes = cmr_dswx_prefix_expected - cmr_dswx_prefix_actual

    #######################################################################
    # CMR_AUDIT SUMMARY
    #######################################################################
    # logger.debug(f"{pstr(missing_cmr_dswx_granules_prefixes)=!s}")

    missing_cmr_granules_hls = set(functools.reduce(set.union, [output_dswx_to_inputs_hls_map[prefix] for prefix in missing_cmr_dswx_granules_prefixes]))

    # logger.debug(f"{pstr(missing_cmr_granules)=!s}")
    logger.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")
    logger.info(f"Fully published (granules): {len(cmr_dswx_products)=:,}")
    logger.info(f"Missing processed (granules): {len(missing_cmr_granules_hls)=:,}")

    if args.format == "txt":
        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - DSWx - {cmr_start_dt_str} to {cmr_end_dt_str}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_hls))
    elif args.format == "json":
        output_file_missing_cmr_granules = args.output if args.output else f"missing granules - DSWx - {cmr_start_dt_str} to {cmr_end_dt_str}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_hls))
            fp.write(json_str)
    else:
        raise Exception()

    logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")

    # DEV: uncomment to export granules and metadata
    # missing_cmr_granules_details_short = to_dsxw_metadata_small(missing_cmr_granules, cmr_granules_details, input_hls_to_outputs_dswx_map)
    # with open(output_file_missing_cmr_granules.replace(".json", " - details.json"), mode='w') as fp:
    #     from compact_json import Formatter
    #     formatter = Formatter(indent_spaces=2, max_inline_length=300)
    #     json_str = formatter.serialize(missing_cmr_granules_details_short)
    #     fp.write(json_str)


if __name__ == "__main__":
    asyncio.run(run(sys.argv))
