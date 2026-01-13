#!/usr/bin/env python3
"""
CLI tool for DIST-S1 lookback window CMR queries.

This tool queries CMR for RTC-S1 files within three lookback windows
centered at t0 - 1 year, t0 - 2 years, and t0 - 3 years.

Files are selected as the n closest files to the center of each window.
"""

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dateutil.parser import isoparse

from data_subscriber.cmr import DateTimeRange, async_query_cmr_v2

# Regex pattern for DIST-S1 native IDs
# Example: OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1
DIST_S1_NATIVE_ID_REGEX = (
    r"OPERA_L3_DIST(?:-ALERT)?-S1_"
    r"(?P<tile_id>T?\w+)_"
    r"(?P<acq_time>\d{8}T\d{6}Z)_"
    r"(?P<prod_time>\d{8}T\d{6}Z)_"
    r"S1_30_v[\d.]+.*"
)

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)


def parse_dist_s1_native_id(native_id: str) -> tuple:
    """
    Parse a DIST-S1 native ID to extract tile ID and acquisition time.

    Args:
        native_id: DIST-S1 native ID (e.g., "OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_...")

    Returns:
        Tuple of (tile_id, acquisition_time) or (None, None) if parsing fails

    Examples:
        >>> parse_dist_s1_native_id("OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1")
        ('T20QLE', datetime(2025, 9, 24, 22, 20, 19))
    """
    import re

    match = re.match(DIST_S1_NATIVE_ID_REGEX, native_id)
    if not match:
        return None, None

    tile_id = match.group("tile_id")
    acq_time_str = match.group("acq_time")

    # Parse acquisition time
    try:
        acq_time = datetime.strptime(acq_time_str, "%Y%m%dT%H%M%SZ")
        return tile_id, acq_time
    except ValueError:
        return None, None


def _mgrs_tile_to_bbox(tile_id: str, margin_km: float = 50.0) -> tuple:
    """
    Convert MGRS tile ID to a bounding box for CMR spatial filtering.

    This uses the mgrs library to get the tile corner and calculates a bbox with margin.
    The bbox doesn't need to be perfectly precise - it just needs to be large enough
    to capture all relevant granules for the tile while making CMR queries more efficient.

    Implemented here instead of using bounding_box_from_mgrs_tile from utils/geo_util.py
    to avoid extra GDAL dependency.

    Args:
        tile_id: MGRS tile ID (e.g., "T168" or "168")
        margin_km: Margin in kilometers (approximate, converted to degrees)

    Returns:
        Tuple of (lon_min, lat_min, lon_max, lat_max) or None if conversion fails
    """
    try:
        import mgrs

        # Remove 'T' prefix if present and strip leading zeros from zone
        if tile_id.startswith("T"):
            tile_id = tile_id[1:]

        # Strip leading zeros from zone number (e.g., "031SGR" -> "31SGR")
        # MGRS zones are 1-60, so we need to handle 01-09
        if len(tile_id) >= 2 and tile_id[0] == "0" and tile_id[1].isdigit():
            tile_id = tile_id[1:]

        mgrs_obj = mgrs.MGRS()

        # Get the lower-left corner in lat/lon
        # Note: mgrs.toLatLon() returns the SW corner of the grid square
        lat_ll, lon_ll = mgrs_obj.toLatLon(tile_id)

        # MGRS grid squares (2-letter codes like GJ, LE, etc.) are 100km x 100km
        # Use simple degree approximation: 1 degree ≈ 111km at equator
        # This is approximate but sufficient for CMR spatial filtering
        grid_square_size_km = 100.0
        grid_square_size_deg = grid_square_size_km / 111.0  # ~0.90 degrees
        margin_deg = margin_km / 111.0  # ~0.45 degrees for 50km margin

        # Calculate bounds with margin
        # The toLatLon gives us the SW corner, so we add the grid size to get NE corner
        lon_min = lon_ll - margin_deg
        lat_min = lat_ll - margin_deg
        lon_max = lon_ll + grid_square_size_deg + margin_deg
        lat_max = lat_ll + grid_square_size_deg + margin_deg

        # Clamp to valid lat/lon ranges
        lat_min = max(lat_min, -90.0)
        lat_max = min(lat_max, 90.0)

        # Handle longitude wrapping at antimeridian
        if lon_min < -180:
            lon_min += 360
        if lon_max > 180:
            lon_max -= 360

        return (lon_min, lat_min, lon_max, lat_max)

    except ImportError:
        logger.error("'mgrs' library not installed. Install with: pip install mgrs")
        return None
    except Exception as e:
        logger.error("Could not convert MGRS tile '%s' to bbox: %s", tile_id, e)
        return None


@dataclass
class RtcGranule:
    """Represents an RTC granule file with its acquisition time."""

    granule_id: str
    acquisition_time: datetime

    def __repr__(self) -> str:
        return f"RtcGranule({self.granule_id}, {self.acquisition_time.isoformat()})"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "granule_id": self.granule_id,
            "acquisition_time": self.acquisition_time.isoformat(),
        }


@dataclass
class LookbackWindow:
    window_start: datetime
    window_center: datetime
    window_end: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "window_start": self.window_start.isoformat(),
            "window_center": self.window_center.isoformat(),
            "window_end": self.window_end.isoformat(),
        }


# Type alias for granule lists
GranuleList = list[RtcGranule]


# ============================================================================
# Lookback Window Logic
# ============================================================================


def calculate_lookback_window(t0: datetime, years_back: int, window_size_days: int) -> LookbackWindow:
    """
    Calculate a lookback window centered at t0 - years_back years.

    Args:
        t0: Reference time
        years_back: Number of years to look back (1, 2, or 3)
        window_size_days: Half-width of the window in days (e.g., 60 means +/- 60 days)

    Returns:
        LookbackWindow with window_start, window_center, window_end
    """
    # Calculate the center of the window using timedelta to handle leap years properly
    days_back = years_back * 365
    window_center = t0 - timedelta(days=days_back)

    # Calculate window boundaries
    window_start = window_center - timedelta(days=window_size_days)
    window_end = window_center + timedelta(days=window_size_days)

    return LookbackWindow(window_start, window_center, window_end)


def select_files_in_window(
    available_files: GranuleList, lookback_window: LookbackWindow, max_files: int
) -> GranuleList:
    """
    Select files within a window, choosing those closest to the window center.

    Args:
        available_files: GranuleList of available files
        lookback_window: LookbackWindow object
        max_files: Maximum number of files to select

    Returns:
        GranuleList of selected files, sorted by proximity to center
    """
    # Filter files within the window
    files_in_window = [
        file
        for file in available_files
        if lookback_window.window_start <= file.acquisition_time <= lookback_window.window_end
    ]

    # Sort by distance from window center
    files_in_window.sort(key=lambda f: abs((f.acquisition_time - lookback_window.window_center).total_seconds()))

    # Return up to max_files
    return files_in_window[:max_files]


def select_dist_s1_input_files(
    t0: datetime, available_files: GranuleList, window_configs: list[tuple[int, int, int]]
) -> tuple[GranuleList, GranuleList, GranuleList]:
    """
    Select input files for DIST-S1 algorithm across three lookback windows.

    Args:
        t0: Reference time
        available_files: GranuleList of available files
        window_configs: List of (years_back, window_size_days, max_files) tuples
                       for w1, w2, w3 respectively

    Returns:
        Tuple of (w1_files, w2_files, w3_files) as GranuleLists
    """
    results = []

    for years_back, window_size_days, max_files in window_configs:
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        selected_files = select_files_in_window(available_files, lookback_window, max_files)

        if len(selected_files) == 0:
            logger.warning(
                "No files found in window w%d (center: %s, range: %s to %s)",
                years_back,
                lookback_window.window_center.isoformat(),
                lookback_window.window_start.isoformat(),
                lookback_window.window_end.isoformat(),
            )

        results.append(selected_files)

    return tuple(results)


# ============================================================================
# CMR Query Functions
# ============================================================================


def get_bbox_from_tile_id(tile_id: str, margin_km: float = 50.0) -> str:
    """
    Get bounding box string from MGRS tile ID for CMR querying.

    Args:
        tile_id: MGRS tile ID (e.g., "T168" or "168")
        margin_km: Margin in kilometers to add around the tile (default 50km for buffer)

    Returns:
        Bounding box string in format "west,south,east,north" or None if conversion fails
    """
    result = _mgrs_tile_to_bbox(tile_id, margin_km)
    if result is None:
        return None

    lon_min, lat_min, lon_max, lat_max = result

    # Format as "west,south,east,north"
    return f"{lon_min},{lat_min},{lon_max},{lat_max}"


async def query_rtc_granules_for_windows(
    tile_id: str,
    t0: datetime,
    window_configs: list[tuple[int, int, int]],
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
    auto_bbox: bool = True,
) -> GranuleList:
    """
    Query CMR for RTC granules within specific lookback windows.

    This function queries only the time ranges needed for the lookback windows,
    making it much more efficient than querying years of data.

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR" or "T168")
        t0: Reference time for lookback calculation
        window_configs: List of (years_back, window_size_days, max_files) tuples
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Bounding box in format "west,south,east,north" (optional, will auto-derive if not provided)
        auto_bbox: If True and bbox is None, automatically derive bbox from tile_id (default True)

    Returns:
        Combined list of RtcGranule objects from all windows
    """
    # Auto-derive bbox from tile_id if not provided
    if bbox is None and auto_bbox:
        bbox = get_bbox_from_tile_id(tile_id)
        if bbox:
            logger.info("Auto-derived bounding box from tile %s: %s", tile_id, bbox)

    all_granules = []

    # Query each window separately
    for years_back, window_size_days, max_files in window_configs:
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        # Create time range for this specific window
        timerange = DateTimeRange(
            start_date=lookback_window.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=lookback_window.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        logger.info("Querying window w%d: %s to %s", years_back, timerange.start_date, timerange.end_date)

        # Query CMR without token (RTC-S1 is public data)
        cmr_results = await async_query_cmr_v2(
            timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox
        )

        logger.info("  Found %d CMR results", len(cmr_results))

        # Convert CMR results to RtcGranule objects
        # Note: We rely on the bbox filtering from CMR, so we accept all RTC granules
        # returned by CMR within the DIST tile's bounding box
        matched_count = 0
        for i, result in enumerate(cmr_results):
            # Extract granule ID from UMM-JSON
            umm = result.get("umm", {})
            granule_id = umm.get("GranuleUR")
            if not granule_id:
                continue

            # Log first few granule IDs for debugging
            if i < 3:
                logger.debug("  Sample granule: %s", granule_id[:80])

            # Extract acquisition time from UMM-JSON
            acquisition_time = _extract_acquisition_time_from_umm(umm)
            if not acquisition_time:
                continue

            all_granules.append(RtcGranule(granule_id, acquisition_time))
            matched_count += 1

        logger.info("  Accepted %d RTC granules within bbox", matched_count)

    logger.info("Total granules after filtering: %d", len(all_granules))
    return all_granules


def _extract_acquisition_time_from_umm(umm: dict) -> Optional[datetime]:
    """
    Extract acquisition time from UMM-JSON metadata.

    Args:
        umm: UMM section of CMR response

    Returns:
        Acquisition time as naive datetime (UTC), or None if not found
    """
    # Try TemporalExtent for acquisition time
    temporal_extent = umm.get("TemporalExtent", {})

    # Check RangeDateTime first
    range_datetime = temporal_extent.get("RangeDateTime")
    if range_datetime:
        time_str = range_datetime.get("BeginningDateTime")
        if time_str:
            try:
                dt = isoparse(time_str)
                # Convert to naive UTC (remove timezone info)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except (ValueError, TypeError):
                pass

    # Fallback to SingleDateTime
    time_str = temporal_extent.get("SingleDateTime")
    if time_str:
        try:
            dt = isoparse(time_str)
            # Convert to naive UTC (remove timezone info)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            pass

    # Last resort: try ProductionDateTime
    data_granule = umm.get("DataGranule", {})
    time_str = data_granule.get("ProductionDateTime")
    if time_str:
        try:
            dt = isoparse(time_str)
            # Convert to naive UTC (remove timezone info)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            pass

    return None


# ============================================================================
# CLI Interface
# ============================================================================


def parse_max_files(value: str) -> tuple[int, int, int]:
    """
    Parse max_files argument in format "8,6,6".

    Args:
        value: Comma-separated string of three integers

    Returns:
        Tuple of three integers

    Raises:
        argparse.ArgumentTypeError: If format is invalid
    """
    try:
        parts = [int(x.strip()) for x in value.split(",")]
        if len(parts) != 3:
            raise ValueError("Must provide exactly 3 values")
        if any(x < 0 for x in parts):
            raise ValueError("Values must be non-negative")
        return tuple(parts)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid max_files format: {e}")


def parse_datetime(value: str) -> datetime:
    """
    Parse datetime string in ISO format.

    Args:
        value: ISO format datetime string (e.g., "2025-09-25T12:00:00Z")

    Returns:
        datetime object (naive UTC)

    Raises:
        argparse.ArgumentTypeError: If format is invalid
    """
    try:
        dt = isoparse(value)
        # Convert to naive UTC
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid datetime format: {e}")


async def process_single_query(
    tile_id: str,
    time: datetime,
    window_configs: list[tuple[int, int, int]],
    bbox: Optional[str],
    auto_bbox: bool,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> dict:
    """
    Process a single DIST-S1 lookback query.

    Args:
        tile_id: MGRS tile ID
        time: Reference time
        window_configs: Window configurations (years_back, window_size_days, max_files)
        bbox: Optional explicit bounding box
        auto_bbox: Whether to auto-derive bbox from tile
        semaphore: Optional semaphore for rate limiting concurrent requests

    Returns:
        Dictionary with query results
    """
    # Use semaphore for rate limiting if provided
    if semaphore:
        async with semaphore:
            return await _process_single_query_impl(
                tile_id, time, window_configs, bbox, auto_bbox
            )
    else:
        return await _process_single_query_impl(
            tile_id, time, window_configs, bbox, auto_bbox
        )


async def _process_single_query_impl(
    tile_id: str,
    time: datetime,
    window_configs: list[tuple[int, int, int]],
    bbox: Optional[str],
    auto_bbox: bool,
) -> dict:
    """Internal implementation of process_single_query."""
    # Query CMR for granules
    available_granules = await query_rtc_granules_for_windows(
        tile_id=tile_id,
        t0=time,
        window_configs=window_configs,
        bbox=bbox,
        auto_bbox=auto_bbox,
    )

    if len(available_granules) == 0:
        logger.warning("No granules found for tile %s in any lookback window", tile_id)
        return None

    # Select files from each window
    w1, w2, w3 = select_dist_s1_input_files(time, available_granules, window_configs)

    return {
        "tile_id": tile_id,
        "reference_time": time,
        "w1": w1,
        "w2": w2,
        "w3": w3,
    }


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Query CMR for DIST-S1 lookback window files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic query for tile T168 at specific time (bbox auto-derived from tile)
  %(prog)s T168 2025-09-25T12:00:00Z

  # Using DIST-S1 native ID (automatically extracts tile ID and time)
  %(prog)s --native-id OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1

  # Batch processing from input file (one native ID per line)
  %(prog)s --input-file dist_s1_granules.txt --output json

  # Custom window size (±30 days instead of default ±60)
  %(prog)s T031SGR 2024-02-29T12:00:00Z --window-size 30

  # Custom max files per window (w1=5, w2=4, w3=4)
  %(prog)s T102 2025-09-25T12:00:00Z --max-files 5,4,4

  # With explicit bounding box (overrides auto-derived bbox)
  %(prog)s T102 2025-09-25T12:00:00Z --bbox "-156,62,-155,62.5"

  # Disable auto-bbox (query globally, slower but more comprehensive)
  %(prog)s T168 2025-09-25T12:00:00Z --no-auto-bbox

  # JSON output
  %(prog)s T168 2025-09-25T12:00:00Z --output json

  # Granule IDs only
  %(prog)s T168 2025-09-25T12:00:00Z --output ids
        """,
    )

    parser.add_argument("tile_id", nargs="?", help="MGRS tile ID (e.g., T168, T031SGR)")

    parser.add_argument(
        "time", nargs="?", type=parse_datetime, help="Acquisition time in ISO format (e.g., 2025-09-25T12:00:00Z)"
    )

    parser.add_argument(
        "--native-id",
        type=str,
        metavar="NATIVE_ID",
        help="DIST-S1 native ID to parse for tile ID and time (e.g., OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_...)",
    )

    parser.add_argument(
        "--input-file",
        type=str,
        metavar="FILE",
        help="Path to file containing one DIST-S1 native ID per line (batch mode)",
    )

    parser.add_argument(
        "--window-size",
        type=int,
        default=60,
        metavar="DAYS",
        help="Half-width of lookback windows in days (default: 60, meaning ±60 days)",
    )

    parser.add_argument(
        "--max-files",
        type=parse_max_files,
        default=(8, 6, 6),
        metavar="W1,W2,W3",
        help="Max files per window as comma-separated list (default: 8,6,6)",
    )

    parser.add_argument(
        "--bbox",
        type=str,
        metavar="WEST,SOUTH,EAST,NORTH",
        help="Bounding box to filter results. If not provided, will be auto-derived from tile ID.",
    )

    parser.add_argument(
        "--no-auto-bbox",
        action="store_true",
        help="Disable automatic bbox derivation from tile ID (queries all data globally)",
    )

    parser.add_argument(
        "--output",
        choices=["text", "json", "ids"],
        default="text",
        help="Output format: text (human-readable), json (structured), ids (granule IDs only)",
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=3,
        metavar="N",
        help="Maximum number of concurrent queries in batch mode (default: 3). Note: each query makes 3 CMR requests (one per window).",
    )

    args = parser.parse_args()

    # Validate input mode
    input_modes = sum([bool(args.input_file), bool(args.native_id), bool(args.tile_id and args.time)])
    if input_modes == 0:
        parser.error("Must provide either --input-file, --native-id, or both tile_id and time arguments")
    if input_modes > 1:
        parser.error("Cannot combine --input-file with --native-id or tile_id/time arguments")

    # Build window configurations
    window_configs = [
        (1, args.window_size, args.max_files[0]),  # w1: 1 year back
        (2, args.window_size, args.max_files[1]),  # w2: 2 years back
        (3, args.window_size, args.max_files[2]),  # w3: 3 years back
    ]

    # Process input - either batch mode or single query
    if args.input_file:
        # Batch mode: read native IDs from file
        logger.info("=" * 80)
        logger.info("DIST-S1 Lookback Window Query (Batch Mode)")
        logger.info("=" * 80)
        logger.info("Input file: %s", args.input_file)
        logger.info("Window size: ±%d days", args.window_size)
        logger.info(
            "Max files per window: w1=%d, w2=%d, w3=%d", args.max_files[0], args.max_files[1], args.max_files[2]
        )
        logger.info("=" * 80)

        # Read native IDs from file
        try:
            with open(args.input_file, "r") as f:
                native_ids = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error("Input file not found: %s", args.input_file)
            sys.exit(1)
        except Exception as e:
            logger.error("Failed to read input file: %s", e)
            sys.exit(1)

        if not native_ids:
            logger.error("No native IDs found in input file")
            sys.exit(1)

        logger.info("Processing %d native IDs with max %d concurrent queries...", len(native_ids), args.max_concurrent)

        # Parse all native IDs first
        parsed_items = []
        for i, native_id in enumerate(native_ids, 1):
            tile_id, time = parse_dist_s1_native_id(native_id)
            if tile_id is None or time is None:
                logger.warning("[%d/%d] Failed to parse native ID, skipping: %s", i, len(native_ids), native_id)
                continue
            logger.info("[%d/%d] Parsed %s: tile_id=%s, time=%s", i, len(native_ids), native_id, tile_id, time.isoformat())
            parsed_items.append((native_id, tile_id, time))

        if not parsed_items:
            logger.error("No valid native IDs to process")
            sys.exit(1)

        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(args.max_concurrent)

        # Create tasks for concurrent processing
        async def process_with_metadata(native_id: str, tile_id: str, time: datetime):
            result = await process_single_query(
                tile_id=tile_id,
                time=time,
                window_configs=window_configs,
                bbox=args.bbox,
                auto_bbox=not args.no_auto_bbox,
                semaphore=semaphore,
            )
            if result:
                result["native_id"] = native_id
            return result

        # Process all items concurrently
        logger.info("Querying CMR concurrently...")
        tasks = [process_with_metadata(native_id, tile_id, time) for native_id, tile_id, time in parsed_items]
        results = await asyncio.gather(*tasks)

        # Filter out None results
        results = [r for r in results if r is not None]

        if not results:
            logger.error("No valid results obtained")
            sys.exit(1)

        logger.info("Successfully processed %d/%d queries", len(results), len(parsed_items))

    else:
        # Single query mode
        # Parse native ID if provided, otherwise use tile_id and time arguments
        if args.native_id:
            tile_id, time = parse_dist_s1_native_id(args.native_id)
            if tile_id is None or time is None:
                logger.error("Failed to parse DIST-S1 native ID: %s", args.native_id)
                logger.error(
                    "Expected format: OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1"
                )
                sys.exit(1)
            logger.info("Parsed native ID: tile_id=%s, time=%s", tile_id, time.isoformat())
        else:
            tile_id = args.tile_id
            time = args.time

        # Log query parameters
        logger.info("=" * 80)
        logger.info("DIST-S1 Lookback Window Query")
        logger.info("=" * 80)
        if args.native_id:
            logger.info("Native ID: %s", args.native_id)
        logger.info("Tile ID: %s", tile_id)
        logger.info("Reference time (t0): %s", time.isoformat())
        logger.info("Window size: ±%d days", args.window_size)
        logger.info(
            "Max files per window: w1=%d, w2=%d, w3=%d", args.max_files[0], args.max_files[1], args.max_files[2]
        )
        if args.bbox:
            logger.info("Bounding box (user-provided): %s", args.bbox)
        elif not args.no_auto_bbox:
            logger.info("Bounding box: Will auto-derive from tile ID")
        else:
            logger.info("Bounding box: Disabled (querying globally)")
        logger.info("=" * 80)

        result = await process_single_query(
            tile_id=tile_id,
            time=time,
            window_configs=window_configs,
            bbox=args.bbox,
            auto_bbox=not args.no_auto_bbox,
        )

        if not result:
            logger.error("No granules found for tile %s in any lookback window", tile_id)
            sys.exit(1)

        results = [result]

    # Extract w1, w2, w3 for output formatting (for single query mode)
    if len(results) == 1:
        w1 = results[0]["w1"]
        w2 = results[0]["w2"]
        w3 = results[0]["w3"]
        tile_id = results[0]["tile_id"]
        time = results[0]["reference_time"]

    # Output results based on format
    if args.output == "json":
        # JSON output
        if len(results) == 1:
            # Single query mode - original format
            output = {
                "query": {
                    "tile_id": tile_id,
                    "reference_time": time.isoformat(),
                    "window_size_days": args.window_size,
                    "max_files": list(args.max_files),
                    "bbox": args.bbox,
                },
                "windows": {
                    "w1": {
                        "years_back": 1,
                        "window": calculate_lookback_window(time, 1, args.window_size).to_dict(),
                        "granules": [g.to_dict() for g in w1],
                        "count": len(w1),
                    },
                    "w2": {
                        "years_back": 2,
                        "window": calculate_lookback_window(time, 2, args.window_size).to_dict(),
                        "granules": [g.to_dict() for g in w2],
                        "count": len(w2),
                    },
                    "w3": {
                        "years_back": 3,
                        "window": calculate_lookback_window(time, 3, args.window_size).to_dict(),
                        "granules": [g.to_dict() for g in w3],
                        "count": len(w3),
                    },
                },
                "total_granules": len(w1) + len(w2) + len(w3),
            }
        else:
            # Batch mode - array of results
            output = {
                "query": {
                    "window_size_days": args.window_size,
                    "max_files": list(args.max_files),
                    "bbox": args.bbox,
                },
                "results": [],
            }

            for result in results:
                tile_id = result["tile_id"]
                time = result["reference_time"]
                w1 = result["w1"]
                w2 = result["w2"]
                w3 = result["w3"]

                result_entry = {
                    "native_id": result.get("native_id"),
                    "tile_id": tile_id,
                    "reference_time": time.isoformat(),
                    "windows": {
                        "w1": {
                            "years_back": 1,
                            "window": calculate_lookback_window(time, 1, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in w1],
                            "count": len(w1),
                        },
                        "w2": {
                            "years_back": 2,
                            "window": calculate_lookback_window(time, 2, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in w2],
                            "count": len(w2),
                        },
                        "w3": {
                            "years_back": 3,
                            "window": calculate_lookback_window(time, 3, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in w3],
                            "count": len(w3),
                        },
                    },
                    "total_granules": len(w1) + len(w2) + len(w3),
                }
                output["results"].append(result_entry)

            # Add summary
            output["summary"] = {
                "total_queries": len(results),
                "total_granules_across_all_queries": sum(r["total_granules"] for r in output["results"]),
            }

        print(json.dumps(output, indent=2))

    elif args.output == "ids":
        # Output granule IDs only, one per line
        for result in results:
            w1 = result["w1"]
            w2 = result["w2"]
            w3 = result["w3"]
            for window_name, granules in [("w1", w1), ("w2", w2), ("w3", w3)]:
                for g in granules:
                    print(g.granule_id)

    else:
        # Text output (human-readable)
        if len(results) == 1:
            # Single query mode
            print("\n" + "=" * 80)
            print("DIST-S1 Lookback Window Selection Results")
            print("=" * 80 + "\n")

            total_files = 0
            for window_name, granules, years_back in [("Window 1", w1, 1), ("Window 2", w2, 2), ("Window 3", w3, 3)]:
                lookback_window = calculate_lookback_window(time, years_back, args.window_size)

                years_suffix = "s" if years_back > 1 else ""
                print(f"{window_name} (t0 - {years_back} year{years_suffix}):")
                print(f"  Center: {lookback_window.window_center.isoformat()}")
                print(
                    f"  Range:  {lookback_window.window_start.isoformat()} to {lookback_window.window_end.isoformat()}"
                )
                print(f"  Files found: {len(granules)}/{args.max_files[years_back-1]}")

                if len(granules) > 0:
                    print("  Granules:")
                    for g in granules:
                        days_from_center = (g.acquisition_time - lookback_window.window_center).days
                        sign = "+" if days_from_center >= 0 else ""
                        print(f"    {g.acquisition_time.isoformat()} ({sign}{days_from_center}d): {g.granule_id}")
                else:
                    print("  (No granules found)")

                print()
                total_files += len(granules)

            print("=" * 80)
            print(f"Total files selected: {total_files}")
            print("=" * 80 + "\n")

        else:
            # Batch mode
            print("\n" + "=" * 80)
            print("DIST-S1 Lookback Window Selection Results (Batch)")
            print("=" * 80 + "\n")

            grand_total = 0
            for i, result in enumerate(results, 1):
                tile_id = result["tile_id"]
                time = result["reference_time"]
                w1 = result["w1"]
                w2 = result["w2"]
                w3 = result["w3"]
                native_id = result.get("native_id")

                print(f"[{i}/{len(results)}] {native_id}")
                print(f"  Tile: {tile_id}, Time: {time.isoformat()}")

                result_total = 0
                for window_name, granules, years_back in [("w1", w1, 1), ("w2", w2, 2), ("w3", w3, 3)]:
                    print(f"  {window_name}: {len(granules)}/{args.max_files[years_back-1]} files")
                    result_total += len(granules)

                print(f"  Subtotal: {result_total} files")
                print()
                grand_total += result_total

            print("=" * 80)
            print(f"Processed {len(results)} queries, total files selected: {grand_total}")
            print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)
