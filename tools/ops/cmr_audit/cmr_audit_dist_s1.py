import argparse
import asyncio
import logging.handlers
from typing import Optional
import pandas as pd
import re
import requests
import sys

from collections import defaultdict, namedtuple
from datetime import datetime, timezone
from dateutil.parser import isoparse
import xml.etree.ElementTree as ET

from data_subscriber.cmr import async_query_cmr_v2
from data_subscriber.rtc.mgrs_bursts_collection_db_client import cached_load_mgrs_burst_db
from tools.ops.cmr_audit.cmr_audit_utils import init_logging

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


def create_parser():
    argparser = argparse.ArgumentParser(description="Audit CMR RTC and DIST-S1 products.")
    argparser.add_argument(
        "--start-datetime",
        required=True,
        type=argparse_dt,
        help="ISO datetime string (UTC). e.g. 2023-08-02T04:00:00Z",
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        type=argparse_dt,
        help="ISO datetime string (UTC). e.g. 2023-08-02T04:00:00Z",
        default=datetime.now(timezone.utc),
    )
    argparser.add_argument(
        "--cmr-environment",
        default="PROD",
        choices=("PROD", "UAT"),
        help="CMR environment to audit (default: %(default)s)",
    )
    argparser.add_argument("--db-file", help="Path to the DIST-S1 burst database parquet file")
    argparser.add_argument("--output", "-o", help="Output filepath.")
    argparser.add_argument(
        "--format", default="txt", choices=["txt", "json"], help="Output format (default: %(default)s)"
    )
    argparser.add_argument("--full-output", action=argparse.BooleanOptionalAction, help="Outputs full metadata")
    argparser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level (default: %(default)s)",
    )
    return argparser


def argparse_dt(dt_str: str) -> datetime:
    dt = isoparse(dt_str)
    if not dt.tzinfo:
        raise argparse.ArgumentTypeError(f"Datetime must be timezone-aware: {dt_str}")
    return dt


def map_burst_to_mgrs() -> dict[str, set[str]]:
    logger.info("Mapping mgrs_set_id to burst_id")
    mgrs = cached_load_mgrs_burst_db(filter_land=False)
    burst_to_mgrs_map = defaultdict(set)
    for _, row in mgrs.iterrows():
        for b in row.bursts_parsed:
            burst_to_mgrs_map[b].add(row.mgrs_set_id)
    return burst_to_mgrs_map


def filter_valid_times(cmr_products: list[dict], start_datetime: datetime, end_datetime: datetime):
    """Generator filtering CMR products by embedded UTC timestamp."""
    for product in cmr_products:
        native_id = product["meta"]["native-id"]
        match = re.search(r"(?<=_)\d{8}T\d{6}Z(?=_)", native_id)
        if not match:
            logger.warning(f"Skipping invalid granule ID: {native_id}")
            continue

        time_coverage = datetime.strptime(match.group(), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        if start_datetime <= time_coverage <= end_datetime:
            yield product


def extract_iso_xml_url(product: dict) -> str:
    for related_url in product["umm"].get("RelatedUrls", []):
        url = related_url.get("URL", "")
        if url.startswith("https") and url.endswith("iso.xml"):
            # Temporarily replace earthdatacloud.nasa.gov portion with alaska.edu
            return url.replace("earthdatacloud.nasa.gov", "alaska.edu")
    raise RuntimeError(f"No iso.xml URL found for {product['meta']['native-id']}")


def obtain_iso_xml(url: str):
    resp = requests.get(url)
    resp.raise_for_status()
    return ET.fromstring(resp.content)


def extract_dist_input_granules(root) -> set[str]:
    """Extract comma-separated PostRtcOperaIds from ISO XML."""
    namespaces = {
        "eos": "http://earthdata.nasa.gov/schema/eos",
        "gco": "http://www.isotc211.org/2005/gco",
    }

    for attr in root.findall(".//eos:AdditionalAttribute", namespaces):
        name = attr.find(".//eos:name/gco:CharacterString", namespaces)
        if name is not None and name.text == "PostRtcOperaIds":
            value_elem = attr.find(".//eos:value/gco:CharacterString", namespaces)
            if value_elem is not None:
                return {x.strip() for x in value_elem.text.split(",")}
    return set()


def extract_burst_id_from_filename(filename: str) -> str:
    match = re.search(r"T\d{3}-\d{6}-IW\d", filename)
    if not match:
        raise RuntimeError(f"Unable to extract burst ID from {filename}")
    return match.group()


def extract_rtc_burst(native_id: str) -> Optional[str]:
    match = re.search(r"T\d{3}-\d{6}-IW\d", native_id)
    return match.group() if match else None


def extract_rtc_bid_acq(native_id: str) -> Optional[str]:
    match = re.search(r"T\d{3}-\d{6}-IW\d_\d{8}T\d{6}Z", native_id)
    return match.group() if match else None


def extract_dist_s1_bid_acq(native_id: str) -> Optional[str]:
    match = re.search(r"T([0-9A-Z]+)_\d{8}T\d{6}Z", native_id)
    return match.group() if match else None


def query_and_format_rtc(timerange: DateTimeRange) -> pd.DataFrame:
    cmr_rtc_products = asyncio.run(
        async_query_cmr_v2(timerange=timerange, provider="ASF", collection="OPERA_L2_RTC-S1_V1")
    )

    rtc_audit_data = []
    for rtc_product in cmr_rtc_products:
        native_id = rtc_product["meta"]["native-id"]
        burst_id = extract_rtc_burst(native_id)
        bid_acq = extract_rtc_bid_acq(native_id)

        if not burst_id or not bid_acq:
            continue

        # acq_cycle = determine_acquisition_cycle_for_rtc_granule(granule_id=native_id)
        audit_data = {
            "native_id": native_id,  # e.g. "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0"
            "revision_id": rtc_product["meta"]["revision-id"],
            "revision_date": rtc_product["meta"]["revision-date"],
            "burst_id": burst_id,  # e.g. "T168-359595-IW3"
            "bid_acq": bid_acq,  # e.g. "T168-359595-IW3_20250516T053145Z"
        }
        rtc_audit_data.append(audit_data)

    input_rtc_df = pd.DataFrame.from_dict({a["native_id"]: a for a in rtc_audit_data}, orient="index")

    return input_rtc_df


def query_and_format_dist_s1(start_datetime: datetime, end_datetime: datetime, cmr_env: str) -> pd.DataFrame:
    if cmr_env == "PROD":
        cmr_hostname = "cmr.earthdata.nasa.gov"
    elif cmr_env == "UAT":
        cmr_hostname = "cmr.uat.earthdata.nasa.gov"
    else:
        raise RuntimeError(f"CMR environment {cmr_env} is not supported")

    # UAT has missing metadata so we need to return ALL granules and locally filter for time of interest
    cmr_dist_products = asyncio.run(
        async_query_cmr_v2(
            timerange=None,
            provider="ASF",
            collection="OPERA_L3_DIST-ALERT-S1_PROVISIONAL_V0",
            cmr_hostname=cmr_hostname,
        )
    )

    # Filter for granules within timerange of interest
    filtered_dist_products = list(filter_valid_times(cmr_dist_products, start_datetime, end_datetime))

    dist_s1_audit_data = {}
    for dist_product in filtered_dist_products:
        native_id = dist_product["meta"]["native-id"]
        iso_xml = obtain_iso_xml(extract_iso_xml_url(dist_product))
        input_granules = extract_dist_input_granules(iso_xml)

        dist_s1_audit_data[native_id] = {
            "native_id": native_id,
            "input_granules": input_granules,
        }

    pairs = [
        (rtc_id, dist_record["native_id"])
        for dist_record in dist_s1_audit_data.values()
        for rtc_id in dist_record["input_granules"]
    ]
    output_rtc_df = pd.DataFrame(pairs, columns=["rtc_id", "parent_dist_native_id"]).set_index("rtc_id")

    return output_rtc_df


def main(start_datetime: datetime = None, end_datetime: datetime = None, **kwargs):
    timerange = DateTimeRange(
        start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"), end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    cmr_env = kwargs.get("cmr_environment")

    input_rtc_df = query_and_format_rtc(timerange)
    output_rtc_df = query_and_format_dist_s1(start_datetime, end_datetime, cmr_env)

    merged = input_rtc_df.merge(output_rtc_df, how="left", left_index=True, right_index=True, indicator=True)
    missing_rtc_df = merged[merged["_merge"] == "left_only"]
    missing_rtc_df = missing_rtc_df.drop(["parent_dist_native_id", "_merge"], axis=1)

    # Need to map burst_ids to set of tile ids + acq id using bursts_to_products
    if kwargs.get("full_output"):
        from data_subscriber.dist_s1_utils import localize_dist_burst_db, parse_local_burst_db_pickle

        if kwargs.get("db_file"):
            pickle_file_name = kwargs.get("db_file") + ".pickle"
            _, bursts_to_products, _, _ = parse_local_burst_db_pickle(kwargs.get("db_file"), pickle_file_name)
        else:
            _, bursts_to_products, _, _ = localize_dist_burst_db()

        missing_rtc_df["mgrs_tile_id_acq_group"] = missing_rtc_df["burst_id"].map(
            lambda b: list(bursts_to_products[b]) if b in bursts_to_products else []
        )

    logger.info(f"RTC granules in CMR: {len(input_rtc_df):,}")
    logger.info(f"RTC granules with corresponding DIST-S1 granules: {len(output_rtc_df):,}")
    logger.info(f"RTC granules missing corresponding DIST-S1 granule: {len(missing_rtc_df):,}")

    now = datetime.now(timezone.utc)
    outprefix = f"missing_granules_RTC-DIST_S1_{start_datetime:%Y%m%dT%H%M%SZ}_{end_datetime:%Y%m%dT%H%M%SZ}_{now:%Y%m%dT%H%M%SZ}"

    fmt = kwargs.get("format", "txt")
    output_path = kwargs.get("output") or f"{outprefix}.{fmt}"

    if fmt == "txt":
        if kwargs.get("full_output"):
            missing_rtc_df.sort_index().to_csv(output_path, index=False)
        else:
            missing_rtc_df.index.to_series().sort_values().to_csv(output_path, index=False, header=False)
    elif fmt == "json":
        if kwargs.get("full_output"):
            missing_rtc_df.to_json(output_path)
        else:
            from compact_json import Formatter

            formatter = Formatter(indent_spaces=2, max_inline_length=300)
            with open(output_path, "w") as f:
                f.write(formatter.serialize(sorted(missing_rtc_df.index)))
    else:
        raise ValueError(f"Unknown output format: {fmt}")

    logger.info(f"Wrote missing RTC granules to {output_path}")


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging("cmr_audit_dist_s1.log", "cmr_audit_dist_s1-error.log", level=args.log_level)
    logger = logging.getLogger(__name__)
    logger.debug(f"{__file__} invoked with {sys.argv=}")
    main(**vars(args))
