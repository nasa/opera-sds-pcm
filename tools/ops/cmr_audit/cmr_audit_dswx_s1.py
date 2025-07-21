import argparse
import asyncio
import logging.handlers
import re
import sys
from collections import defaultdict, namedtuple
from datetime import datetime, timezone
from functools import reduce
from typing import Literal

import pandas as pd
from dateutil.parser import isoparse

import rtc_utils
from data_subscriber.cmr import async_query_cmr_v2
from data_subscriber.rtc import mgrs_bursts_collection_db_client
from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)

def init_logging(level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=level, format=log_format, force=True)

    rfh1 = logging.handlers.RotatingFileHandler("cmr_audit_dswx_s1.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler("cmr_audit_dswx_s1-error.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)

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
        help=f"Output filepath."
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    argparser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), help="(default: %(default)s)")

    return argparser

def argparse_dt(dt_str):
    dt = isoparse(dt_str)
    if not dt.tzinfo:
        raise ValueError()
    return dt

def main(start_datetime: datetime=None, end_datetime:datetime=None, **kwargs):
    start_date = start_datetime.isoformat().replace("+00:00", "Z")
    end_date = end_datetime.isoformat().replace("+00:00", "Z")
    # TODO chrisjrd: filter_land=True
    print("loading mgrs bursts collection database")
    mgrs = mgrs_bursts_collection_db_client.cached_load_mgrs_burst_db(filter_land=False)
    print("DONE")

    print("mapping mgrs_set_id to burst_id")
    burst_id_to_mgrs_set_ids_map = defaultdict(set)
    for i, row in mgrs.iterrows():
        for b in row.bursts_parsed:
            burst_id_to_mgrs_set_ids_map[b].add(row.mgrs_set_id)
    print("DONE")


    DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


    # async def example():
    #     rsps = await asyncio.gather(
    #         async_query_cmr_v2(timerange=DateTimeRange("2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"), provider="ASF", collection="OPERA_L2_RTC-S1_V1"),
    #         async_query_cmr_v2(timerange=DateTimeRange("2025-01-01T01:00:00Z", "2025-01-01T02:00:00Z"), provider="ASF", collection="OPERA_L2_RTC-S1_V1")
    #     )
    #     return [i for rsp in rsps for i in rsp]

    timerange = DateTimeRange(start_date, end_date)

    # cmr_products = asyncio.run(
    #     async_query_cmr_v2(timerange=timerange, provider="ASF", collection="OPERA_L2_RTC-S1_V1")
    # )

    cmr_granule_ids, cmr_products = asyncio.run(
        async_get_cmr_granules(
            collection_short_name="OPERA_L2_RTC-S1_V1",
            temporal_date_start=timerange.start_date,
            temporal_date_end=timerange.end_date,
            platform_short_name=None,
            concurrency=5
        )
    )
    cmr_products = cmr_products.values()

    # CONVERT INTO AUDIT MODEL
    rtc_audit_data = []
    for cmr_product in cmr_products:
        native_id = cmr_product["meta"]["native-id"]
        burst_id = native_id[16:31]
        burst_id_normalized = burst_id.lower().replace("-","_")
        mgrs_set_ids = burst_id_to_mgrs_set_ids_map[burst_id_normalized]
        if not mgrs_set_ids:
            print("RTC granule burst ID not in loaded MGRS bursts collection DB. Skipping.")
            continue  # Skip. not on land if the MGRS bursts collection DB filters only land
        acquisition_cycle = rtc_utils.determine_acquisition_cycle_for_rtc_granule(granule_id=native_id)
        audit_data = {
            "native_id": native_id,  # e.g. "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0"
            "revision_id": cmr_product["meta"]["revision-id"],
            "revision_date": cmr_product["meta"]["revision-date"],
            "burst_id": burst_id,
            "burst_id_normalized": burst_id_normalized,
            "bid_acq": native_id[16:48],  # e.g. "T168-359595-IW3_20250516T053145Z"
            "mgrs_set_ids": mgrs_set_ids,
            "mgrs_set_id_acquisition_ts_cycle_indexes": {
                mgrs_set_id_acquisition_ts_cycle_index := f"{mgrs_set_id}${acquisition_cycle}"
                for mgrs_set_id in mgrs_set_ids
            }
        }
        rtc_audit_data.append(audit_data)

    # map special index to CMR (input) products
    coverage = defaultdict(set)
    for audit_data in rtc_audit_data:
        mgrs_set_id_acquisition_ts_cycle_indexes = audit_data["mgrs_set_id_acquisition_ts_cycle_indexes"]
        for mgrs_set_id_acquisition_ts_cycle_index in mgrs_set_id_acquisition_ts_cycle_indexes:
            coverage[mgrs_set_id_acquisition_ts_cycle_index].add(audit_data["native_id"])


    ########################################################################################################

    # cmr_products = asyncio.run(
    #     async_query_cmr_v2(timerange=timerange, provider="POCLOUD", collection="OPERA_L3_DSWX-S1_V1")
    # )

    cmr_granule_ids, cmr_products = asyncio.run(
        async_get_cmr_granules(
            collection_short_name="OPERA_L3_DSWX-S1_V1",
            temporal_date_start=timerange.start_date,
            temporal_date_end=timerange.end_date,
            platform_short_name=None,
            concurrency=5
        )
    )
    cmr_products = cmr_products.values()

    # CONVERT INTO AUDIT MODEL
    dswx_s1_audit_data = {}
    for cmr_product in cmr_products:
        input_granules = {re.match(rtc_utils.rtc_granule_regex, g).group("id") for g in cmr_product["umm"]["InputGranules"]}
        input_granule_burst_ids_normalized = {g[16:31].lower().replace("-", "_") for g in input_granules}

        related_mgrs_set_ids = [
            burst_id_to_mgrs_set_ids_map[b]
            for b in input_granule_burst_ids_normalized
        ]
        mgrs_set_id = reduce(set.intersection, related_mgrs_set_ids)  # EDGE CASE: len(2)

        native_id = cmr_product["meta"]["native-id"]
        audit_data = {
            "native_id": native_id,  # e.g. "OPERA_L3_DSWx-S1_T55GCQ_20250512T193408Z_20250513T064736Z_S1A_30_v1.0"
            "revision_id": cmr_product["meta"]["revision-id"],
            "revision_date": cmr_product["meta"]["revision-date"],
            "input_granules": input_granules,
            "input_granule_burst_ids": {g[16:31] for g in input_granules},
            "input_granule_burst_ids_normalized": input_granule_burst_ids_normalized,
            "input_bid_acq": {g[16:48] for g in input_granules},  # e.g. {"T118-252625-IW2_20250512T193412Z", ...}
            "bid_acq": native_id[17:40],  # e.g. "T55GCQ_20250512T193408Z"
            "mgrs_set_id": mgrs_set_id
        }
        dswx_s1_audit_data[native_id] = audit_data

    # a = reduce(set.union, [at["mgrs_set_ids"] for at in rtc_audit_data])
    # b = reduce(set.union, [at["mgrs_set_id"] for at in dswx_s1_audit_data])
    # covered_mgrs_burst_set_ids = a & b
    # symmetric_difference = a ^ b

    rtc_output_audit_data = []
    for dswx_s1_audit_datum in dswx_s1_audit_data.values():
        for g in dswx_s1_audit_datum["input_granules"]:
            native_id = g
            burst_id = native_id[16:31]
            burst_id_normalized = burst_id.lower().replace("-", "_")
            mgrs_set_ids = burst_id_to_mgrs_set_ids_map[burst_id_normalized]
            if not mgrs_set_ids:
                print("RTC granule burst ID not in loaded MGRS bursts collection DB. Skipping.")
                continue  # Skip. not on land if the MGRS bursts collection DB filters only land
            acquisition_cycle = rtc_utils.determine_acquisition_cycle_for_rtc_granule(granule_id=native_id)
            audit_data = {
                "native_id": native_id,
                # e.g. "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0"
                # "revision_id": cmr_product["meta"]["revision-id"],
                # "revision_date": cmr_product["meta"]["revision-date"],
                "burst_id": burst_id,
                "burst_id_normalized": burst_id_normalized,
                "bid_acq": native_id[16:48],  # e.g. "T168-359595-IW3_20250516T053145Z"
                "mgrs_set_ids": mgrs_set_ids,
                "mgrs_set_id_acquisition_ts_cycle_indexes": {
                    mgrs_set_id_acquisition_ts_cycle_index := f"{mgrs_set_id}${acquisition_cycle}"
                    for mgrs_set_id in mgrs_set_ids
                }
            }
            rtc_output_audit_data.append(audit_data)

    input_rtc_id_to_audit_data = {}
    for audit_data in rtc_audit_data:
        native_id = audit_data["native_id"]
        input_rtc_id_to_audit_data[native_id] = audit_data

    output_rtc_id_to_audit_data = {}
    for audit_data in rtc_output_audit_data:
        native_id = audit_data["native_id"]
        output_rtc_id_to_audit_data[native_id] = audit_data

    # Build DataFrames from dictionaries
    if not input_rtc_id_to_audit_data:
        print("Warning: input_rtc_id_to_audit_data is empty.")
        a = pd.DataFrame()
    else:
        a = pd.DataFrame.from_dict(input_rtc_id_to_audit_data, orient="index")

    if not output_rtc_id_to_audit_data:
        print("Warning: output_rtc_id_to_audit_data is empty.")
        b = pd.DataFrame()
    else:
        b = pd.DataFrame.from_dict(output_rtc_id_to_audit_data, orient="index")

    # Check before computing unused_rtc
    if "native_id" in b.columns and "native_id" in a.columns:
        missing_rtc = set(a["native_id"]) ^ set(b["native_id"])
    elif "native_id" in a.columns:
        # b is missing 'native_id', so treat it as an empty set
        missing_rtc = set(a["native_id"])
        print("'native_id' column missing in output_rtc_id_to_audit_data")
    elif "native_id" in b.columns:
        # a is missing 'native_id', so all of b is unused
        missing_rtc = set()
        print("'native_id' column missing in input_rtc_id_to_audit_data")
    else:
        # Neither has 'native_id' — define unused_rtc as empty set
        missing_rtc = set()
        print("'native_id' column missing in both input and output audit data")

    if a.empty or "native_id" not in a.columns:
        logger.info(f"Expected input (granules) (RTC): 0 (RTC-S1 is empty or missing 'native_id' column)")
    else:
        logger.info(f"Expected input (granules) (RTC): {len(set(a['native_id']))=:,}")

    if b.empty or "native_id" not in b.columns:
        logger.info("Fully published (granules) (RTC): 0 (DSWX-S1 is empty or missing 'native_id' column)")
    else:
        logger.info(f"Fully published (granules) (RTC): {len(set(b['native_id']))=:,}")

    if not missing_rtc:
        logger.info(f"Missing processed RTC (granules): 0")
    else:
        logger.info(f"Missing processed RTC (granules): {len(missing_rtc)=:,}")

    now = datetime.now()
    current_dt_str = now.strftime("%Y%m%d-%H%M%S")
    start_dt_str = start_date.replace("-","")
    start_dt_str = start_dt_str.replace("T", "-")
    start_dt_str = start_dt_str.replace(":", "")

    end_dt_str = end_date.replace("-", "")
    end_dt_str = end_dt_str.replace("T", "-")
    end_dt_str = end_dt_str.replace(":", "")
    outfilename = f"missing_granules_RTC-DSWx_{start_dt_str}Z_{end_dt_str}Z_{current_dt_str}Z"

    if args.format == "txt":
        output_file_missing_cmr_granules = args.output if args.output else f"{outfilename}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_rtc))
        if missing_rtc:
            with open(output_file_missing_cmr_granules, mode='a') as fp:
                fp.write('\n')
    elif args.format == "json":
        output_file_missing_cmr_granules = args.output if args.output else f"{outfilename}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_rtc))
            fp.write(json_str)
    else:
        raise Exception()

if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging(level=args.log_level)
    logger = logging.getLogger(__name__)

    logger.debug(f"{__file__} invoked with {sys.argv=}")

    main(**args.__dict__)
