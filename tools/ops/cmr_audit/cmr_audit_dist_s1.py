import argparse
import asyncio
import logging
import logging.handlers
import re
import sys
import xml.etree.ElementTree as ET
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from dateutil.parser import isoparse

from data_subscriber.cmr import async_query_cmr_v2
from tools.ops.cmr_audit.cmr_audit_utils import init_logging

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


def argparse_dt(dt_str: str) -> datetime:
    dt = isoparse(dt_str)
    if not dt.tzinfo:
        raise argparse.ArgumentTypeError(f"Datetime must be timezone-aware: {dt_str}")
    return dt


def create_parser():
    argparser = argparse.ArgumentParser(description="Audit CMR RTC and DIST-S1 products.")
    argparser.add_argument(
        "--start-datetime",
        required=True,
        type=argparse_dt,
        help="ISO datetime string (UTC). e.g. 2023-08-02T04:00:00Z",
    )
    argparser.add_argument(
        "--end-datetime", required=True, type=argparse_dt, help="ISO datetime string (UTC). e.g. 2023-08-02T04:00:00Z"
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
    argparser.add_argument(
        "--rtc-output",
        action=argparse.BooleanOptionalAction,
        help="Organize output by RTC granule instead of DIST-S1 product id time",
    )
    argparser.add_argument(
        "--full-output", action=argparse.BooleanOptionalAction, help="Inclue additional metadata in output"
    )
    argparser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level (default: %(default)s)",
    )
    return argparser


def filter_valid_times(cmr_products: list[dict], start_datetime: datetime, end_datetime: datetime):
    """Yield CMR products whose native IDs fall within a UTC time range."""
    for product in cmr_products:
        native_id = product["meta"].get("native-id")
        if not native_id:
            continue

        match = re.search(r"(?<=_)\d{8}T\d{6}Z(?=_)", native_id)
        if not match:
            logger.warning(f"Skipping invalid granule ID: {native_id}")
            continue

        time_coverage = datetime.strptime(match.group(), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        if start_datetime <= time_coverage <= end_datetime:
            yield product


def extract_iso_xml_url(product: dict) -> str:
    """
    Return the HTTPS iso.xml URL for a given CMR product.
    Short term swaps domain due to upstream issues obtaining data from earthdatacloud domains.
    """
    for related_url in product["umm"].get("RelatedUrls", []):
        url = related_url.get("URL", "")
        if url.startswith("https") and url.endswith("iso.xml"):
            # Temporarily replace earthdatacloud.nasa.gov portion with alaska.edu
            return url.replace("earthdatacloud.nasa.gov", "alaska.edu")
    raise RuntimeError(f"No iso.xml URL found for {product['meta']['native-id']}")


def obtain_iso_xml(url: str):
    """Download and parse ISO XML from a given URL."""
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


def extract_rtc_burst(native_id: str) -> Optional[str]:
    """Extract burst ID from RTC granule native ID."""
    match = re.search(r"T\d{3}-\d{6}-IW\d", native_id)
    return match.group() if match else None


def extract_rtc_bid_acq(native_id: str) -> Optional[str]:
    """Extract burst-acquisition identifier from RTC granule native ID."""
    match = re.search(r"T\d{3}-\d{6}-IW\d_\d{8}T\d{6}Z", native_id)
    return match.group() if match else None


def query_and_format_rtc(timerange: DateTimeRange) -> pd.DataFrame:
    """Query CMR for RTC products and return as a DataFrame."""
    try:
        cmr_rtc_products = asyncio.run(
            async_query_cmr_v2(timerange=timerange, provider="ASF", collection="OPERA_L2_RTC-S1_V1")
        )
    except Exception as e:
        logger.exception("Failed to query RTC products from CMR")
        raise RuntimeError("RTC CMR query failed") from e

    if not cmr_rtc_products:
        raise RuntimeError("RTC CMR query returned no results")

    rtc_audit_data = []
    for rtc_product in cmr_rtc_products:
        native_id = rtc_product["meta"].get("native-id")
        if not native_id:
            logger.warning(f"Unable to extract native_id from {rtc_product['meta']}. Skipping.")
            continue

        burst_id = extract_rtc_burst(native_id)
        bid_acq = extract_rtc_bid_acq(native_id)

        if not (burst_id and bid_acq):
            continue

        rtc_audit_data.append(
            {
                "native_id": native_id,
                "revision_id": rtc_product["meta"].get("revision-id"),
                "revision_date": rtc_product["meta"].get("revision-date"),
                "burst_id": burst_id,
                "bid_acq": bid_acq,
            }
        )

        if not rtc_audit_data:
            raise RuntimeError("No valid RTC granules found after parsing CMR results")

    return pd.DataFrame(rtc_audit_data).set_index("native_id", drop=False)


def query_and_format_dist_s1(start_datetime: datetime, end_datetime: datetime, cmr_env: str) -> pd.DataFrame:
    if cmr_env == "PROD":
        cmr_hostname = "cmr.earthdata.nasa.gov"
    elif cmr_env == "UAT":
        cmr_hostname = "cmr.uat.earthdata.nasa.gov"
    else:
        raise RuntimeError(f"CMR environment {cmr_env} is not supported")

    # UAT has missing metadata so for now we need to return ALL granules and locally filter for time of interest
    try:
        cmr_dist_products = asyncio.run(
            async_query_cmr_v2(
                timerange=None,
                provider="ASF",
                collection="OPERA_L3_DIST-ALERT-S1_PROVISIONAL_V0",
                cmr_hostname=cmr_hostname,
            )
        )
    except Exception as e:
        logger.exception("Failed to query DIST-S1 products from CMR")
        raise RuntimeError("DIST-S1 CMR query failed") from e

    if not cmr_dist_products:
        logger.info("DIST-S1 CMR query returned no results")
        return pd.DataFrame([], columns=["rtc_id", "parent_dist_native_id"]).set_index("rtc_id")

    # Filter for granules within timerange of interest
    filtered_dist_products = list(filter_valid_times(cmr_dist_products, start_datetime, end_datetime))
    if not filtered_dist_products:
        raise RuntimeError("No DIST-S1 products within requested date range")

    dist_s1_audit_data = {}
    for dist_product in filtered_dist_products:
        native_id = dist_product["meta"].get("native-id")
        if not native_id:
            logger.warning(f"Unable to extract native_id from {dist_product['meta']}. Skipping.")
            continue

        try:
            iso_xml = obtain_iso_xml(extract_iso_xml_url(dist_product))
        except Exception as e:
            logger.error(f"Unable to obtain ISO XML for {native_id}: {e}")
            continue

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
    try:
        timerange = DateTimeRange(
            start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"), end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        cmr_env = kwargs.get("cmr_environment")
        rtc_organization = kwargs.get("rtc_output")
        full_output = kwargs.get("full_output")

        output_rtc_df = query_and_format_dist_s1(start_datetime, end_datetime, cmr_env)
        input_rtc_df = query_and_format_rtc(timerange)

    except RuntimeError as e:
        logger.error(f"Fatal: {e}")
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected fatal error during main()")
        sys.exit(1)

    merged = input_rtc_df.merge(output_rtc_df, how="left", left_index=True, right_index=True, indicator=True)
    missing_rtc_df = merged[merged["_merge"] == "left_only"]
    missing_rtc_df = missing_rtc_df.drop(["parent_dist_native_id", "_merge"], axis=1)

    # Need to map burst_ids to set of tile ids + acq id using bursts_to_products
    if not rtc_organization:
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

    missing_rtc_df = missing_rtc_df.sort_index()

    if not rtc_organization:
        # We want version indexed by DIST tile, grouping input RTC granules
        exploded = missing_rtc_df.explode("mgrs_tile_id_acq_group")
        missing_dist_df = (
            exploded.groupby("mgrs_tile_id_acq_group", dropna=True)["native_id"]
            .apply(list)
            .reset_index()
            .rename(columns={"native_id": "rtc_granules"})
        )
        missing_dist_df = missing_dist_df[["mgrs_tile_id_acq_group", "rtc_granules"]]
        missing_dist_df["rtc_granules"] = missing_dist_df["rtc_granules"].sort_values()

        def make_product_id_time(row):
            mgrs = row["mgrs_tile_id_acq_group"]
            granules = row["rtc_granules"]
            result = []
            for g in granules:
                match = re.search(r"\d{8}T\d{6}Z", g)
                if match:
                    timestamp = match.group(0)
                    result.append(f"{mgrs},{timestamp}")
            return result

        missing_dist_df["product_id_time"] = missing_dist_df.apply(make_product_id_time, axis=1)

        output_path = f"mgrs_tile_id_acq_group_{output_path}"

    if full_output:
        if rtc_organization:
            output = missing_rtc_df
        else:
            output = missing_dist_df
    else:
        if rtc_organization:
            output = missing_rtc_df.index.to_series().sort_values()
        else:
            output = (
                missing_dist_df["product_id_time"]
                .apply(lambda x: x if isinstance(x, list) else [x])
                .explode()
                .sort_values()
            )

    if fmt == "txt":
        delimiter = "|"
        inner_delim = ";"

        def normalize_value(v):
            if isinstance(v, list):
                return inner_delim.join(map(str, v))
            return str(v)

        with open(output_path, "w", encoding="utf-8") as f:
            if full_output:
                f.write(delimiter.join(map(str, output.columns)) + "\n")
                for _, row in output.iterrows():
                    normalized = [normalize_value(v) for v in row.tolist()]
                    f.write(delimiter.join(normalized) + "\n")
            else:
                for value in output:
                    f.write(str(value) + "\n")
    elif fmt == "json":
        output.to_json(output_path, orient="records", indent=2)
    else:
        raise ValueError(f"Unknown output format: {fmt}")

    logger.info(f"Wrote missing RTC granules to {output_path}")


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging("cmr_audit_dist_s1.log", "cmr_audit_dist_s1-error.log", level=args.log_level)
    logger.debug(f"{__file__} invoked with {sys.argv=}")
    main(**vars(args))
