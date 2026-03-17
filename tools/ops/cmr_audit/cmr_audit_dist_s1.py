import argparse
import asyncio
import logging
import logging.handlers
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from dateutil.parser import isoparse
from requests.exceptions import RequestException

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
    argparser.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Maximum number of concurrent iso.xml downloads (default: %(default)s)",
    )
    argparser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retry attempts for iso.xml downloads (default: %(default)s)",
    )
    return argparser


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


def obtain_iso_xml(url: str, max_retries: int = 3, base_delay: float = 1.0):
    """
    Download and parse ISO XML from a given URL with exponential backoff retry.

    Args:
        url: URL to fetch
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.0)

    Returns:
        Parsed XML ElementTree

    Raises:
        RequestException: If all retries fail
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except RequestException as e:
            last_exception = e

            # Don't retry on 4xx errors (except 429 Too Many Requests)
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                if 400 <= status_code < 500 and status_code != 429:
                    logger.warning(f"Non-retryable error {status_code} for {url}: {e}")
                    raise

            if attempt < max_retries:
                # Exponential backoff: base_delay * 2^attempt
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries + 1} failed for {url}: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} attempts failed for {url}")
                raise last_exception


async def obtain_iso_xml_async(url: str, max_retries: int = 3):
    """Download and parse ISO XML from a given URL (async version)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: obtain_iso_xml(url, max_retries=max_retries))


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


async def fetch_dist_product_inputs(dist_product: dict, semaphore: asyncio.Semaphore, max_retries: int = 3) -> Optional[dict]:
    """
    Fetch and parse iso.xml for a single DIST-S1 product.

    Returns dict with native_id and input_granules, or None if failed.
    """
    async with semaphore:
        native_id = dist_product["meta"].get("native-id")
        if not native_id:
            logger.warning(f"Unable to extract native_id from {dist_product['meta']}. Skipping.")
            return None

        try:
            iso_xml_url = extract_iso_xml_url(dist_product)
            iso_xml = await obtain_iso_xml_async(iso_xml_url, max_retries=max_retries)
            input_granules = extract_dist_input_granules(iso_xml)

            return {
                "native_id": native_id,
                "input_granules": input_granules,
            }
        except Exception as e:
            logger.error(f"Unable to obtain ISO XML for {native_id}: {e}")
            return None


async def query_and_format_dist_s1_async(timerange: DateTimeRange, cmr_env: str, max_concurrent: int = 10, max_retries: int = 3) -> pd.DataFrame:
    """Query CMR for DIST-S1 products and return as a DataFrame (async version with parallelization)."""
    if cmr_env == "PROD":
        cmr_hostname = "cmr.earthdata.nasa.gov"
    elif cmr_env == "UAT":
        cmr_hostname = "cmr.uat.earthdata.nasa.gov"
    else:
        raise RuntimeError(f"CMR environment {cmr_env} is not supported")

    try:
        cmr_dist_products = await async_query_cmr_v2(
            timerange=timerange,
            provider="ASF",
            collection="OPERA_L3_DIST-ALERT-S1_PROVISIONAL_V0",
            cmr_hostname=cmr_hostname,
        )
    except Exception as e:
        logger.exception("Failed to query DIST-S1 products from CMR")
        raise RuntimeError("DIST-S1 CMR query failed") from e

    if not cmr_dist_products:
        logger.info("DIST-S1 CMR query returned no results")
        return pd.DataFrame([], columns=["rtc_id", "parent_dist_native_id"]).set_index("rtc_id")

    logger.info(f"Obtaining iso.xml files for {len(cmr_dist_products)} DIST-S1 products (parallel, max_concurrent={max_concurrent}, max_retries={max_retries})")

    # Fetch all iso.xml files in parallel with concurrency limit
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [fetch_dist_product_inputs(dist_product, semaphore, max_retries) for dist_product in cmr_dist_products]
    results = await asyncio.gather(*tasks)

    # Filter out None results (failed fetches)
    dist_s1_audit_data = {r["native_id"]: r for r in results if r is not None}

    logger.info(f"Successfully obtained iso.xml for {len(dist_s1_audit_data)} / {len(cmr_dist_products)} products")

    pairs = [
        (rtc_id, dist_record["native_id"])
        for dist_record in dist_s1_audit_data.values()
        for rtc_id in dist_record["input_granules"]
    ]
    output_rtc_df = pd.DataFrame(pairs, columns=["rtc_id", "parent_dist_native_id"]).set_index("rtc_id")

    return output_rtc_df


def query_and_format_dist_s1(timerange: DateTimeRange, cmr_env: str, max_concurrent: int = 10, max_retries: int = 3) -> pd.DataFrame:
    """Query CMR for DIST-S1 products and return as a DataFrame (wrapper for async version)."""
    return asyncio.run(query_and_format_dist_s1_async(timerange, cmr_env, max_concurrent, max_retries))


def reduce_product_id_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse product_id_time entries into one representative "<mgrs_tile_id_acq_group>,<timestamp>" per cluster.

    Each product_id_time entry is a string "<mgrs_tile_id_acq_group>,<timestamp>" or a list of such strings.
    Timestamps within the same mgrs_tile_id_acq_group are clustered when they occur ≤3 minutes apart.
    The earliest timestamp in each cluster is selected.

    Returns
    -------
    pandas.Series
        One "<mgrs_tile_id_acq_group>,<timestamp>" string per mgrs_tile_id_acq_group/cluster.
    """
    product_id_time = df["product_id_time"].apply(lambda x: x if isinstance(x, list) else [x]).explode().sort_values()

    df = product_id_time.str.split(",", expand=True)
    df.columns = ["mgrs_tile_id_acq_group", "ts_str"]

    df["ts"] = pd.to_datetime(df["ts_str"], format="%Y%m%dT%H%M%SZ")
    df = df.sort_values(["mgrs_tile_id_acq_group", "ts"])

    # For each mgrs_tile_id_acq_group, mark cluster boundaries
    df["cluster"] = (
        df.groupby("mgrs_tile_id_acq_group")["ts"]  # group by mgrs_tile_id_acq_group
        .diff()  # time since previous value
        .gt(pd.Timedelta(minutes=3))  # is the gap > 3 minutes?
        .fillna(True)  # first row always starts cluster
        .cumsum()  # assign cluster IDs
    )

    # Select ONE row per cluster (earliest timestamp)
    representatives = df.groupby(["mgrs_tile_id_acq_group", "cluster"]).first().reset_index()

    # Reconstruct final output strings
    return representatives["mgrs_tile_id_acq_group"].str.cat(representatives["ts_str"], sep=",")


def main(start_datetime: datetime = None, end_datetime: datetime = None, **kwargs):
    try:
        timerange = DateTimeRange(
            start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"), end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        cmr_env = kwargs.get("cmr_environment")
        rtc_organization = kwargs.get("rtc_output")
        full_output = kwargs.get("full_output")
        max_concurrent = kwargs.get("max_concurrent", 10)
        max_retries = kwargs.get("max_retries", 3)

        output_rtc_df = query_and_format_dist_s1(timerange, cmr_env, max_concurrent, max_retries)
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
            output = reduce_product_id_times(missing_dist_df)

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
