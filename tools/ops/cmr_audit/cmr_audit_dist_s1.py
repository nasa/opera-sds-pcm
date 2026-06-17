"""
CMR Audit Tool for DIST-S1 Products

This tool identifies missing DIST-S1 products by comparing RTC products in CMR
with those that have been used as inputs for existing DIST-S1 products. It can
optionally trigger the input validation on potentially missing products.

Features:
- Queries CMR for DIST-S1 and RTC products within a specified time range
- Extracts metadata from ISO XML files via HTTPS or S3 URLs
- Maps RTC bursts to MGRS tiles using the DIST-S1 burst database
- Identifies tile+time combinations that are missing DIST-S1 products
- Filters out false positives (where a DIST-S1 product already exists)
- Can automatically run dist_s1_input_tool.py on the output for end-to-end validation

End-to-End Validation:
- Use --run-input-validation to automatically run dist_s1_input_tool.py on the audit results
- This step validates inputs of potentially missing products to find truly missing products

Basic Usage:
    python cmr_audit_dist_s1.py --start-datetime 2023-01-01T00:00:00Z --end-datetime 2023-01-02T00:00:00Z

UAT Usage:
    python cmr_audit_dist_s1.py --start-datetime 2023-01-01T00:00:00Z --end-datetime 2023-01-02T00:00:00Z --cmr-environment UAT

End-to-End Usage:
    python cmr_audit_dist_s1.py --start-datetime 2023-01-01T00:00:00Z --end-datetime 2023-01-02T00:00:00Z --run-input-validation
"""

import argparse
import asyncio
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError
from dateutil.parser import isoparse
from requests.exceptions import RequestException

# Avoid importing gcov_utils (No need for DIST-S1)
import types

mock_module = types.ModuleType("data_subscriber.gcov_utils")
mock_module.load_mgrs_track_frame_db = lambda *args, **kwargs: None

sys.modules["data_subscriber.gcov_utils"] = mock_module

from data_subscriber.cmr import async_query_cmr_v2
from tools.ops.cmr_audit.cmr_audit_utils import extract_fields, init_logging

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

DIST_S1_COLLECTION = "OPERA_L3_DIST-ALERT-S1_V1"

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
    argparser.add_argument(
        "--run-input-validation",
        action=argparse.BooleanOptionalAction,
        help="Run dist_s1_input_tool.py on the output to validate if missing products actually have sufficient inputs",
    )
    argparser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level (default: %(default)s)",
    )
    return argparser


def use_s3_urls() -> bool:
    """Determine whether to use S3 URLs based on the execution environment."""
    try:
        requests.get("http://169.254.169.254/latest/meta-data/", timeout=1)
        return True
    except requests.exceptions.RequestException:
        return False


def extract_iso_xml_url(product: dict, use_s3: bool) -> str:
    """
    Return the iso.xml URL for a given CMR product.

    Args:
        product: The CMR product dictionary containing metadata
        use_s3: Whether to use s3 locations for iso.xml

    Returns:
        URL string for accessing the iso.xml file
    """

    s3_url = None
    https_url = None

    for related_url in product["umm"].get("RelatedUrls", []):
        url = related_url.get("URL", "")

        # Check for S3 URL
        if url.startswith("s3://") and url.endswith("iso.xml"):
            s3_url = url
            if use_s3:
                logger.debug(f"Using S3 URL: {s3_url}")
                return s3_url

        # Check for HTTPS URL
        elif url.startswith("https") and url.endswith("iso.xml"):
            # Temporarily replace earthdatacloud.nasa.gov portion with alaska.edu for HTTPS
            https_url = url.replace("earthdatacloud.nasa.gov", "alaska.edu")
            if not use_s3:
                logger.debug(f"Using HTTPS URL: {https_url}")
                return https_url

    # Return the first URL found based on availability
    if s3_url:
        logger.debug(f"Using S3 URL: {s3_url}")
        return s3_url
    elif https_url:
        logger.debug(f"Using HTTPS URL: {https_url}")
        return https_url

    raise RuntimeError(f"No iso.xml URL found for {product['meta']['native-id']}")


def _get_s3_object(bucket: str, key: str, max_retries: int = 3, base_delay: float = 1.0):
    """
    Get an object from S3 with exponential backoff retry.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.0)

    Returns:
        Object content as bytes

    Raises:
        BotoCoreError, ClientError: If S3 access fails after all retries
    """
    s3_client = boto3.client("s3")
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except (BotoCoreError, ClientError) as e:
            last_exception = e

            if attempt < max_retries:
                # Exponential backoff: base_delay * 2^attempt
                delay = base_delay * (2**attempt)
                logger.warning(
                    f"S3 attempt {attempt + 1}/{max_retries + 1} failed for s3://{bucket}/{key}: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} S3 attempts failed for s3://{bucket}/{key}")
                raise last_exception


def _get_http_content(url: str, max_retries: int = 3, base_delay: float = 1.0):
    """
    Get content from an HTTP URL with exponential backoff retry.

    Args:
        url: HTTP(S) URL to fetch
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.0)

    Returns:
        HTTP response content as bytes

    Raises:
        RequestException: If all retries fail
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.content
        except RequestException as e:
            last_exception = e

            # Don't retry on 4xx errors (except 429 Too Many Requests)
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                if 400 <= status_code < 500 and status_code != 429:
                    logger.warning(f"Non-retryable error {status_code} for {url}: {e}")
                    raise

            if attempt < max_retries:
                # Exponential backoff: base_delay * 2^attempt
                delay = base_delay * (2**attempt)
                logger.warning(
                    f"HTTP attempt {attempt + 1}/{max_retries + 1} failed for {url}: {e}. Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} HTTP attempts failed for {url}")
                raise last_exception


def obtain_iso_xml(url: str, max_retries: int = 3, base_delay: float = 1.0):
    """
    Download and parse ISO XML from a given URL with exponential backoff retry.
    Handles both S3 and HTTP/HTTPS URLs.

    Args:
        url: URL to fetch (s3:// or https://)
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.0)

    Returns:
        Parsed XML ElementTree

    Raises:
        Exception: If the URL protocol is not supported or if all retries fail
    """
    try:
        # Check URL type and use appropriate handler
        if url.startswith("s3://"):
            # Parse S3 URL
            parsed_url = urlparse(url)
            bucket = parsed_url.netloc
            key = parsed_url.path.lstrip("/")

            logger.debug(f"Fetching S3 object: bucket={bucket}, key={key}")
            content = _get_s3_object(bucket, key, max_retries, base_delay)

        elif url.startswith(("http://", "https://")):
            logger.debug(f"Fetching HTTP(S) URL: {url}")
            content = _get_http_content(url, max_retries, base_delay)

        else:
            raise ValueError(f"Unsupported URL protocol: {url}")

        # Parse content as XML
        return ET.fromstring(content)

    except Exception as e:
        logger.error(f"Failed to obtain and parse iso.xml from {url}: {e}")
        raise


async def obtain_iso_xml_async(url: str, max_retries: int = 3):
    """
    Download and parse ISO XML from a given URL (async version).
    Handles both S3 and HTTP/HTTPS URLs.

    Args:
        url: URL to fetch (s3:// or https://)
        max_retries: Maximum number of retry attempts

    Returns:
        Parsed XML ElementTree

    Raises:
        Exception: If the URL protocol is not supported or if all retries fail
    """
    # Run the synchronous function in a thread pool executor
    # This is more efficient than spawning a new thread for each request
    loop = asyncio.get_running_loop()
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


def parse_dist_s1_native_id(native_id: str) -> tuple:
    """Parse a DIST-S1 native ID to extract tile ID and acquisition time."""
    # DIST-S1 native ID pattern - updated based on actual native ID format
    pattern = (
        r"OPERA_L3_DIST(?:-ALERT)?-S1_"
        r"(?P<tile_id>T\w+)_"
        r"(?P<acq_time>\d{8}T\d{6}Z)_"
        r"(?P<prod_time>\d{8}T\d{6}Z)_"
        r"S1[A-D]_\d+_v\d+\.\d+"
    )

    match = re.match(pattern, native_id)
    if not match:
        logger.debug(f"Failed to match pattern on native ID: {native_id}")
        return None, None

    tile_id = match.group("tile_id")
    acq_time_str = match.group("acq_time")

    return tile_id, acq_time_str


def normalize_tile_time_key(tile_id, timestamp):
    """
    Create a normalized key for comparing tile+time combinations.
    This ensures the format used in existing_tile_times matches the format in product_id_time.
    """
    # Ensure tile_id doesn't have a T prefix (for consistency)
    if tile_id.startswith("T"):
        tile_id = tile_id[1:]

    # Return the normalized format
    return f"{tile_id},{timestamp}"


def query_and_format_rtc(timerange: DateTimeRange) -> pd.DataFrame:
    """Query CMR for RTC products and return as a DataFrame."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            rtc_paths = asyncio.run(
                async_query_cmr_v2(timerange=timerange, provider="ASF", collection="OPERA_L2_RTC-S1_V1",
                                   output_dir=tmpdir)
            )
            rtc_records = extract_fields(rtc_paths, ["meta.native-id", "meta.revision-id", "meta.revision-date"])
    except Exception as e:
        logger.exception("Failed to query RTC products from CMR")
        raise RuntimeError("RTC CMR query failed") from e

    if not rtc_records:
        raise RuntimeError("RTC CMR query returned no results")

    rtc_audit_data = []
    for record in rtc_records:
        native_id = record.get("meta.native-id")
        if not native_id:
            logger.warning(f"Unable to extract native_id from record. Skipping.")
            continue

        burst_id = extract_rtc_burst(native_id)
        bid_acq = extract_rtc_bid_acq(native_id)

        if not (burst_id and bid_acq):
            continue

        rtc_audit_data.append(
            {
                "native_id": native_id,
                "revision_id": record.get("meta.revision-id"),
                "revision_date": record.get("meta.revision-date"),
                "burst_id": burst_id,
                "bid_acq": bid_acq,
            }
        )

    if not rtc_audit_data:
        raise RuntimeError("No valid RTC granules found after parsing CMR results")

    return pd.DataFrame(rtc_audit_data).set_index("native_id", drop=False)


async def fetch_dist_product_inputs(
    dist_product: dict, semaphore: asyncio.Semaphore, max_retries: int = 3, use_s3: bool = False
) -> Optional[dict]:
    """
    Fetch and parse iso.xml for a single DIST-S1 product.

    Args:
        dist_product: The CMR product dictionary
        semaphore: Semaphore for limiting concurrent requests
        max_retries: Maximum number of retry attempts

    Returns:
        dict with native_id and input_granules, or None if failed.
    """
    async with semaphore:
        native_id = dist_product["meta"].get("native-id")
        if not native_id:
            logger.warning(f"Unable to extract native_id from {dist_product['meta']}. Skipping.")
            return None

        try:
            iso_xml_url = extract_iso_xml_url(dist_product, use_s3)
            iso_xml = await obtain_iso_xml_async(iso_xml_url, max_retries=max_retries)
            input_granules = extract_dist_input_granules(iso_xml)

            return {
                "native_id": native_id,
                "input_granules": input_granules,
            }
        except Exception as e:
            logger.error(f"Unable to obtain ISO XML for {native_id}: {e}")
            return None


async def query_and_format_dist_s1_async(
    timerange: DateTimeRange, cmr_env: str, max_concurrent: int = 10, max_retries: int = 3
) -> tuple:
    """
    Query CMR for DIST-S1 products and return a DataFrame and a set of existing tile+time combinations.

    Returns:
        Tuple containing:
        - DataFrame mapping RTC IDs to parent DIST-S1 product IDs
        - Set of existing tile+time combinations as "tile_id,timestamp" strings
    """
    if cmr_env == "PROD":
        cmr_hostname = "cmr.earthdata.nasa.gov"
    elif cmr_env == "UAT":
        cmr_hostname = "cmr.uat.earthdata.nasa.gov"
    else:
        raise RuntimeError(f"CMR environment {cmr_env} is not supported")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            dist_paths = await async_query_cmr_v2(
                timerange=timerange,
                provider="ASF",
                collection=DIST_S1_COLLECTION,
                cmr_hostname=cmr_hostname,
                output_dir=tmpdir,
            )
            dist_records = extract_fields(dist_paths, ["meta.native-id", "umm.RelatedUrls"])
    except Exception as e:
        logger.exception("Failed to query DIST-S1 products from CMR")
        raise RuntimeError("DIST-S1 CMR query failed") from e

    if not dist_records:
        logger.info("DIST-S1 CMR query returned no results")
        return pd.DataFrame([], columns=["rtc_id", "parent_dist_native_id"]).set_index("rtc_id"), set()

    # Reconstruct minimal product dicts for downstream processing
    cmr_dist_products = [
        {"meta": {"native-id": r["meta.native-id"]}, "umm": {"RelatedUrls": r["umm.RelatedUrls"]}}
        for r in dist_records
    ]

    # Extract tile+time combinations from all DIST-S1 products
    existing_tile_times = set()
    successful_parses = 0
    failed_parses = 0

    logger.info(f"Extracting tile+time combinations from {len(cmr_dist_products)} DIST-S1 products")

    # Log a few sample native IDs for debugging
    if len(cmr_dist_products) > 0:
        logger.debug("Sample DIST-S1 native IDs:")
        for i, dist_product in enumerate(cmr_dist_products[:3]):  # Log up to 3 sample IDs
            native_id = dist_product["meta"].get("native-id")
            logger.debug(f"  Sample {i + 1}: {native_id}")

    for dist_product in cmr_dist_products:
        native_id = dist_product["meta"].get("native-id")
        if not native_id:
            logger.debug(f"Missing native-id in product metadata: {dist_product['meta']}")
            continue

        tile_id, acq_time = parse_dist_s1_native_id(native_id)
        if tile_id and acq_time:
            # Normalize the tile+time key format to match the format used in make_product_id_time
            tile_time_key = normalize_tile_time_key(tile_id, acq_time)
            existing_tile_times.add(tile_time_key)
            successful_parses += 1
            logger.debug(f"Extracted tile+time: {tile_time_key} from {native_id}")
        else:
            failed_parses += 1
            logger.warning(f"Failed to parse DIST-S1 native ID: {native_id}")

    if successful_parses == 0:
        logger.warning("WARNING: No DIST-S1 native IDs were successfully parsed!")
        logger.warning("This will prevent any filtering of false positives.")
        logger.warning("Please check the regex pattern against the actual native IDs.")
    else:
        logger.info(f"Found {len(existing_tile_times)} unique tile+time combinations with existing DIST-S1 products")
        logger.debug(f"Successfully parsed {successful_parses} native IDs, failed to parse {failed_parses} native IDs")

    use_s3 = use_s3_urls()

    logger.info(
        f"Obtaining iso.xml files for {len(cmr_dist_products)} DIST-S1 products (use_s3={use_s3}, parallel, max_concurrent={max_concurrent}, max_retries={max_retries})"
    )

    # Fetch all iso.xml files in parallel with concurrency limit
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        fetch_dist_product_inputs(dist_product, semaphore, max_retries, use_s3) for dist_product in cmr_dist_products
    ]
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

    return output_rtc_df, existing_tile_times


def query_and_format_dist_s1(
    timerange: DateTimeRange, cmr_env: str, max_concurrent: int = 10, max_retries: int = 3
) -> tuple:
    """
    Query CMR for DIST-S1 products and return a DataFrame and a set of existing tile+time combinations.

    Args:
        timerange: Time range to query within
        cmr_env: CMR environment to query (PROD or UAT)
        max_concurrent: Maximum number of concurrent downloads
        max_retries: Maximum number of retry attempts

    Returns:
        Tuple containing:
        - DataFrame mapping RTC IDs to parent DIST-S1 product IDs
        - Set of existing tile+time combinations as "tile_id,timestamp" strings
    """
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
        .gt(pd.Timedelta(minutes=10))  # is the gap > 3 minutes?
        .fillna(True)  # first row always starts cluster
        .cumsum()  # assign cluster IDs
    )

    # Select ONE row per cluster (earliest timestamp)
    representatives = df.groupby(["mgrs_tile_id_acq_group", "cluster"]).first().reset_index()

    # Reconstruct final output strings
    return representatives["mgrs_tile_id_acq_group"].str.cat(representatives["ts_str"], sep=",")


def run_dist_s1_input_tool(input_file_path: str) -> int:
    """
    Run the dist_s1_input_tool.py script on the audit output file.

    Args:
        input_file_path: Path to the audit output file to use as input

    Returns:
        Return code from the subprocess
    """

    tool_path = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "dist_s1_input_tool.py")
    )

    # Prepare the command
    cmd = [sys.executable, tool_path, "--input-file", input_file_path]

    # Log the command and header information
    cmd_str = " ".join(cmd)
    logger.info(f"Running dist_s1_input_tool.py: {cmd_str}")
    logger.info("---------- dist_s1_input_tool.py output START ----------")

    try:
        # Run the process with real-time output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # Line buffered
        )

        # Read and process output line by line
        for line in process.stdout:
            # Remove trailing newlines to avoid double spacing in log
            line = line.rstrip("\n")
            logger.info(line)

        # Get the return code
        process.wait()
        return_code = process.returncode

        # Log completion status
        logger.info("")  # Empty line before footer
        logger.info(f"# Process completed with return code: {return_code}")

    except Exception as e:
        logger.error(f"Error running dist_s1_input_tool.py: {e}")
        return 1

    logger.info("---------- dist_s1_input_tool.py output END ----------")

    return return_code


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

        # Get options for dist_s1_input_tool integration
        run_input_validation = kwargs.get("run_input_validation", False)

        # Get both the DataFrame and the set of existing tile+time combinations
        output_rtc_df, existing_tile_times = query_and_format_dist_s1(timerange, cmr_env, max_concurrent, max_retries)
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
    outprefix = (
        f"DIST_S1_potential_missing_products_{start_datetime:%Y%m%dT%H%M%SZ}_{end_datetime:%Y%m%dT%H%M%SZ}_{now:%Y%m%dT%H%M%SZ}"
    )

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
                    # Use the same normalization function for consistency
                    result.append(normalize_tile_time_key(mgrs, timestamp))
            return result

        missing_dist_df["product_id_time"] = missing_dist_df.apply(make_product_id_time, axis=1)

        # Filter out tile+time combinations that already have a DIST-S1 product
        if existing_tile_times:
            logger.info(
                f"Checking {len(missing_dist_df)} potential missing products against {len(existing_tile_times)} existing tile+time combinations"
            )
        else:
            logger.warning("WARNING: No existing tile+time combinations to filter against!")

            # Log a sample of what we're looking for
            if len(existing_tile_times) > 0:
                sample_existing = list(existing_tile_times)[:3]  # Take up to 3 examples
                logger.debug(f"Sample existing tile+time combinations: {sample_existing}")
                # Check the format of the existing tile+time combinations
                logger.debug(
                    f"Format of existing tile+time combinations: {type(next(iter(existing_tile_times))).__name__} - {next(iter(existing_tile_times))}"
                )

            if len(missing_dist_df) > 0:
                # Log a sample of product_id_time values to verify format
                sample_rows = missing_dist_df.head(3)
                for _, row in sample_rows.iterrows():
                    pid_times = row["product_id_time"]
                    logger.debug(
                        f"Sample potential missing product: mgrs={row['mgrs_tile_id_acq_group']}, product_id_time={pid_times}"
                    )

                    # Check the format of the product_id_time values
                    if pid_times:
                        first_pid = pid_times[0] if isinstance(pid_times, list) else pid_times
                        logger.debug(f"Format of product_id_time: {type(first_pid).__name__} - {first_pid}")

            original_count = len(missing_dist_df)
            filtered_rows = []

            # Instead of direct filtering, we'll check each row individually for debugging
            for idx, row in missing_dist_df.iterrows():
                pid_times = row["product_id_time"]
                pid_times_list = pid_times if isinstance(pid_times, list) else [pid_times]

                # Check if any product_id_time value matches an existing tile+time
                matches_found = False
                for pid_time in pid_times_list:
                    if pid_time in existing_tile_times:
                        matches_found = True
                        logger.debug(f"Match found: {pid_time} exists in both missing and existing sets")
                        break

                if not matches_found:
                    filtered_rows.append(idx)
                else:
                    logger.debug(
                        f"Filtering out row with mgrs={row['mgrs_tile_id_acq_group']} (found in existing products)"
                    )

            # Create a new DataFrame with only the non-matching rows
            missing_dist_df = missing_dist_df.loc[filtered_rows]

            filtered_count = original_count - len(missing_dist_df)
            logger.info(
                f"Filtered out {filtered_count} false positives (tile+time combinations that already have a DIST-S1 product)"
            )
            logger.info(f"Potential missing products: {len(missing_dist_df)}")

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

    # Run dist_s1_input_tool.py if requested
    if run_input_validation and not rtc_organization:
        if fmt != "txt":
            logger.warning(f"Input validation with dist_s1_input_tool.py only supports txt format, but got {fmt}")
            logger.warning("Skipping input validation")
        else:
            logger.info("Running input validation with dist_s1_input_tool.py")
            ret_code = run_dist_s1_input_tool(output_path)
            if ret_code == 0:
                logger.info("Input validation complete successfully")
            else:
                logger.error(f"Input validation completed with return code {ret_code}")


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging("cmr_audit_dist_s1.log", "cmr_audit_dist_s1-error.log", level=args.log_level)
    logger.debug(f"{__file__} invoked with {sys.argv=}")
    main(**vars(args))
