#!/usr/bin/env python3
"""
CLI tool for DIST-S1 lookback window CMR queries.

This tool queries CMR for RTC-S1 files within three backward-looking lookback windows
ending at t0 - 1 year, t0 - 2 years, and t0 - 3 years. Each window looks backward
a specified number of days from its target date.

Files are selected as the n closest files to the END of each window (target date).
For each unique burst+subswath combination, lookback window selection is performed
independently, generating separate "baseline products".
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
from data_subscriber.dist_s1_utils import localize_dist_burst_db

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


def _mgrs_tile_to_bbox(tile_id: str, margin_km: float = 75.0) -> tuple:
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
    """Represents an RTC granule file with its acquisition time and polarization."""

    granule_id: str
    acquisition_time: datetime
    polarization: Optional[list] = None  # e.g., ["VV", "VH"] or ["HH", "HV"]

    def __repr__(self) -> str:
        pol_str = f", {self.polarization}" if self.polarization else ""
        return f"RtcGranule({self.granule_id}, {self.acquisition_time.isoformat()}{pol_str})"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "granule_id": self.granule_id,
            "acquisition_time": self.acquisition_time.isoformat(),
            "polarization": self.polarization,
        }

    def is_dual_pol(self) -> bool:
        """
        Check if this granule has dual polarization.

        Returns:
            True if granule has HH+HV or VV+VH polarization
        """
        if not self.polarization or len(self.polarization) != 2:
            return False

        pol_set = set(self.polarization)
        return pol_set == {"HH", "HV"} or pol_set == {"VV", "VH"}


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
# Helper Functions
# ============================================================================


def get_bursts_for_tile_from_db(tile_id: str) -> set:
    """
    Get all burst IDs that overlap a given MGRS tile from the lookup table.

    Args:
        tile_id: MGRS tile ID (e.g., "T102", "T031SGR")

    Returns:
        Set of burst IDs (e.g., {"T102-217642-IW1", "T102-217642-IW2", ...})
    """
    try:
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()
    except Exception as e:
        logger.warning("Could not load MGRS burst lookup table: %s", e)
        logger.warning("Falling back to bbox-based search")
        return None

    # Normalize tile ID (remove T prefix if present, then add it back)
    normalized_tile = tile_id if not tile_id.startswith("T") else tile_id[1:]

    # Check both with and without T prefix
    tile_variants = [normalized_tile, f"T{normalized_tile}"]

    all_bursts = set()
    for tile_variant in tile_variants:
        if tile_variant in dist_products:
            # Get all product IDs for this tile
            product_ids = dist_products[tile_variant]

            # For each product, get its bursts
            for product_id in product_ids:
                if product_id in product_to_bursts:
                    bursts = product_to_bursts[product_id]
                    all_bursts.update(bursts)

            logger.info("Found %d bursts for tile %s from lookup table", len(all_bursts), tile_variant)
            return all_bursts

    logger.warning("Tile %s not found in lookup table", tile_id)
    return None


def extract_burst_and_subswath_from_granule_id(granule_id: str) -> tuple:
    """
    Extract burst ID and subswath from an RTC granule ID.

    Args:
        granule_id: RTC granule ID (e.g., "OPERA_L2_RTC-S1_T168-359429-IW2_...")

    Returns:
        Tuple of (burst_id, subswath) or (None, None) if parsing fails

    Example:
        >>> extract_burst_and_subswath_from_granule_id("OPERA_L2_RTC-S1_T168-359429-IW2_20240925T120000Z_...")
        ('359429', 'IW2')
    """
    import re

    # Pattern: OPERA_L2_RTC-S1_{tile}-{burst}-{subswath}_{rest}
    # Example: OPERA_L2_RTC-S1_T168-359429-IW2_20240925T120000Z_...
    pattern = r"OPERA_L2_RTC-S1_T?\w+-(\d+)-(IW[123])_"

    match = re.search(pattern, granule_id)
    if match:
        burst_id = match.group(1)
        subswath = match.group(2)
        return burst_id, subswath

    return None, None


def extract_full_burst_id_from_granule_id(granule_id: str) -> str:
    """
    Extract the full burst identifier (tile-burst-subswath) from an RTC granule ID.

    Args:
        granule_id: RTC granule ID (e.g., "OPERA_L2_RTC-S1_T168-359429-IW2_...")

    Returns:
        Full burst ID (e.g., "T168-359429-IW2") or None if parsing fails

    Example:
        >>> extract_full_burst_id_from_granule_id("OPERA_L2_RTC-S1_T168-359429-IW2_20240925T120000Z_...")
        'T168-359429-IW2'
    """
    import re

    # Pattern: OPERA_L2_RTC-S1_{full_burst_id}_{rest}
    # Example: OPERA_L2_RTC-S1_T168-359429-IW2_20240925T120000Z_...
    pattern = r"OPERA_L2_RTC-S1_(T?\w+-\d+-IW[123])_"

    match = re.search(pattern, granule_id)
    if match:
        return match.group(1)

    return None


def deduplicate_by_acquisition_time(granules: GranuleList) -> GranuleList:
    """
    Keep only the latest processing version for each acquisition time.

    When multiple products exist at the same acquisition time, this function
    keeps only the one with the latest granule_id (proxy for processing time).

    Args:
        granules: List of RtcGranule objects

    Returns:
        Deduplicated list with one granule per unique acquisition time
    """
    from collections import defaultdict

    by_acq_time = defaultdict(list)
    for granule in granules:
        by_acq_time[granule.acquisition_time].append(granule)

    # For each acquisition time, keep the one with latest granule_id (proxy for processing time)
    deduplicated = []
    for acq_time, grp in by_acq_time.items():
        # Sort by granule_id (later processing times have later IDs typically)
        latest = sorted(grp, key=lambda g: g.granule_id)[-1]
        deduplicated.append(latest)

        # Log if we deduplicated anything
        if len(grp) > 1:
            logger.info(
                "Deduplicated %d granules at acquisition time %s, kept: %s",
                len(grp),
                acq_time.isoformat(),
                latest.granule_id,
            )

    # Return sorted by acquisition time for consistent ordering
    return sorted(deduplicated, key=lambda g: g.acquisition_time)


# ============================================================================
# Lookback Window Logic
# ============================================================================


def calculate_lookback_window(t0: datetime, years_back: int, window_size_days: int) -> LookbackWindow:
    """
    Calculate a backward-looking lookback window ending at t0 - years_back years.

    Args:
        t0: Reference time
        years_back: Number of years to look back (1, 2, or 3)
        window_size_days: Size of the lookback window in days (e.g., 60 means the previous 60 days)

    Returns:
        LookbackWindow with window_start, window_center, window_end where:
        - window_end: t0 - years_back years (the target date)
        - window_start: window_end - window_size_days (looking backward)
        - window_center: midpoint of the window (for reference)

    Example:
        For t0=2025-09-25, years_back=1, window_size_days=60:
        - window_end = 2024-09-25 (target date)
        - window_start = 2024-07-27 (60 days before target)
        - Files closest to 2024-09-25 are selected
    """
    # Calculate the target date (end of the window)
    days_back = years_back * 365
    window_end = t0 - timedelta(days=days_back)

    # Look backward from the target date
    window_start = window_end - timedelta(days=window_size_days)

    # Calculate midpoint for reference
    window_center = window_start + timedelta(days=window_size_days // 2)

    return LookbackWindow(window_start, window_center, window_end)


def select_files_in_window(
    available_files: GranuleList, lookback_window: LookbackWindow, max_files: int
) -> GranuleList:
    """
    Select files within a window, choosing those closest to the window end.

    Args:
        available_files: GranuleList of available files
        lookback_window: LookbackWindow object
        max_files: Maximum number of files to select

    Returns:
        GranuleList of selected files, sorted by proximity to window end (closest first)
    """
    # Filter files within the window
    files_in_window = [
        file
        for file in available_files
        if lookback_window.window_start <= file.acquisition_time <= lookback_window.window_end
    ]

    # Deduplicate by acquisition time (keep latest processing version)
    files_in_window = deduplicate_by_acquisition_time(files_in_window)

    # Sort by distance from window end (target date), closest first
    files_in_window.sort(key=lambda f: abs((f.acquisition_time - lookback_window.window_end).total_seconds()))

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
                "No files found in window w%d (target date: %s, range: %s to %s)",
                years_back,
                lookback_window.window_end.isoformat(),
                lookback_window.window_start.isoformat(),
                lookback_window.window_end.isoformat(),
            )

        results.append(selected_files)

    return tuple(results)


async def query_and_select_baseline_products_for_dist_s1(
    tile_id: str,
    t0: datetime,
    window_configs: list[tuple[int, int, int]],
    time_tolerance_minutes: int = 10,
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
    auto_bbox: bool = True,
) -> dict:
    """
    Complete workflow for DIST-S1 baseline product selection.

    This function implements the full workflow:
    1. Find all RTC bursts that overlap the tile at acquisition time t0 (±tolerance)
    2. For each burst found, query CMR for historical data in lookback windows
    3. Perform lookback window selection for each burst independently

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR" or "T168")
        t0: Acquisition time from DIST-S1 native ID
        window_configs: List of (years_back, window_size_days, max_files) tuples
        time_tolerance_minutes: Time tolerance for finding bursts at t0 (default 10 minutes)
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Bounding box in format "west,south,east,north" (optional, will auto-derive if not provided)
        auto_bbox: If True and bbox is None, automatically derive bbox from tile_id (default True)

    Returns:
        Dictionary mapping baseline_id (burst-subswath) to baseline product data:
        {
            "359429-IW1": {
                "burst_id": "359429",
                "subswath": "IW1",
                "t0": [RtcGranule, ...],  # Granules at acquisition time
                "w1": [RtcGranule, ...],
                "w2": [RtcGranule, ...],
                "w3": [RtcGranule, ...]
            },
            ...
        }

    Example:
        If 16 bursts overlap the tile at t0, this returns 16 baseline products,
        each with granules at t0 plus up to 8+6+6=20 input files from the lookback windows.
    """
    # Step 1: Find active bursts at acquisition time
    logger.info("Step 1: Finding RTC bursts at acquisition time %s for tile %s", t0.isoformat(), tile_id)

    # Auto-derive bbox if needed
    if bbox is None and auto_bbox:
        bbox = get_bbox_from_tile_id(tile_id)
        if bbox:
            logger.info("Auto-derived bounding box from tile %s: %s", tile_id, bbox)

    active_bursts, t0_granules = await query_rtc_bursts_at_acquisition_time(
        tile_id=tile_id,
        t0=t0,
        time_tolerance_minutes=time_tolerance_minutes,
        provider=provider,
        collection=collection,
        bbox=bbox,
        auto_bbox=False,  # Already handled above
    )

    if not active_bursts:
        logger.warning("No RTC bursts found at acquisition time %s for tile %s", t0.isoformat(), tile_id)
        return {}

    logger.info("Found %d active burst+subswath combinations at t0", len(active_bursts))
    total_t0_granules = sum(len(granules) for granules in t0_granules.values())
    logger.info("Found %d RTC granules at t0 across all bursts", total_t0_granules)

    # Determine dominant polarization at t0 and filter bursts to ensure consistency
    # This prevents mixing HH+HV and VV+VH bursts in the same product
    polarization_counts = {}
    for baseline_id, granules in t0_granules.items():
        if granules and granules[0].polarization:
            pol_tuple = tuple(sorted(granules[0].polarization))
            polarization_counts[pol_tuple] = polarization_counts.get(pol_tuple, 0) + 1

    if polarization_counts:
        # Use the most common polarization
        dominant_pol = max(polarization_counts, key=polarization_counts.get)
        dominant_pol_set = set(dominant_pol)
        logger.info(
            "Dominant polarization at t0: %s (%d bursts)", list(dominant_pol), polarization_counts[dominant_pol]
        )

        # Filter active bursts and t0_granules to only include dominant polarization
        filtered_active_bursts = []
        filtered_t0_granules = {}

        for burst_id, subswath in active_bursts:
            baseline_id = f"{burst_id}-{subswath}"
            granules = t0_granules.get(baseline_id, [])

            if granules and granules[0].polarization:
                if set(granules[0].polarization) == dominant_pol_set:
                    filtered_t0_granules[baseline_id] = granules
                    filtered_active_bursts.append((burst_id, subswath))
                else:
                    logger.info(
                        "  Filtered out burst %s with non-dominant polarization %s",
                        baseline_id,
                        granules[0].polarization,
                    )
            else:
                # No polarization info or no granules - keep it
                filtered_t0_granules[baseline_id] = granules
                filtered_active_bursts.append((burst_id, subswath))

        if len(filtered_active_bursts) < len(active_bursts):
            logger.info(
                "Filtered bursts by polarization: %d -> %d (keeping %s)",
                len(active_bursts),
                len(filtered_active_bursts),
                list(dominant_pol),
            )

        active_bursts = filtered_active_bursts
        t0_granules = filtered_t0_granules

    # Step 2 & 3: For each active burst, query lookback windows and select files
    baseline_products = {}

    for burst_id, subswath in active_bursts:
        baseline_id = f"{burst_id}-{subswath}"
        logger.info("Processing burst %s...", baseline_id)

        # Query historical data for this specific burst
        burst_granules = await query_rtc_granules_for_burst(
            tile_id=tile_id,
            burst_id=burst_id,
            subswath=subswath,
            t0=t0,
            window_configs=window_configs,
            provider=provider,
            collection=collection,
            bbox=bbox,
        )

        logger.info("  Found %d historical granules for burst %s", len(burst_granules), baseline_id)

        # Get t0 granules for this burst
        t0_burst_granules = t0_granules.get(baseline_id, [])

        # Determine the polarization from t0 granules and filter historical granules to match
        # This ensures consistency: if t0 is HH+HV, we only use HH+HV from lookback windows
        if t0_burst_granules:
            # Get polarization from first t0 granule (they should all be the same)
            reference_pol = t0_burst_granules[0].polarization
            if reference_pol:
                reference_pol_set = set(reference_pol)
                logger.debug("  Reference polarization from t0: %s", reference_pol)

                # Filter historical granules to match this polarization
                filtered_granules = []
                for granule in burst_granules:
                    if granule.polarization and set(granule.polarization) == reference_pol_set:
                        filtered_granules.append(granule)

                if len(filtered_granules) < len(burst_granules):
                    logger.info(
                        "  Filtered historical granules by polarization: %d -> %d (keeping %s)",
                        len(burst_granules),
                        len(filtered_granules),
                        reference_pol,
                    )
                burst_granules = filtered_granules

        # Perform lookback window selection for this burst (using polarization-filtered granules)
        w1, w2, w3 = select_dist_s1_input_files(t0, burst_granules, window_configs)

        baseline_products[baseline_id] = {
            "burst_id": burst_id,
            "subswath": subswath,
            "t0": t0_burst_granules,
            "w1": w1,
            "w2": w2,
            "w3": w3,
        }

        logger.info(
            "  Selected files for %s: t0=%d, w1=%d, w2=%d, w3=%d (total=%d)",
            baseline_id,
            len(t0_burst_granules),
            len(w1),
            len(w2),
            len(w3),
            len(t0_burst_granules) + len(w1) + len(w2) + len(w3),
        )

    logger.info("Generated %d baseline products", len(baseline_products))

    # Filter out baseline products that have no historical granules
    # If all lookback windows are empty, the product can't be generated
    filtered_baseline_products = {}
    for baseline_id, product in baseline_products.items():
        total_historical = len(product["w1"]) + len(product["w2"]) + len(product["w3"])
        if total_historical > 0:
            filtered_baseline_products[baseline_id] = product
        else:
            logger.info(
                "Filtered out baseline %s: has t0 granules but no historical data (w1+w2+w3=0)",
                baseline_id,
            )

    if len(filtered_baseline_products) < len(baseline_products):
        logger.info(
            "Filtered baselines by historical data availability: %d -> %d",
            len(baseline_products),
            len(filtered_baseline_products),
        )

    return filtered_baseline_products


def select_dist_s1_baseline_products(
    t0: datetime, available_files: GranuleList, window_configs: list[tuple[int, int, int]]
) -> dict:
    """
    Select input files for DIST-S1 algorithm, grouped by burst+subswath baseline products.

    For each unique burst+subswath combination, this function performs independent lookback
    window selection. Each burst+subswath gets its own set of (w1, w2, w3) files,
    which constitutes one "baseline product".

    Args:
        t0: Reference time
        available_files: GranuleList of available RTC files
        window_configs: List of (years_back, window_size_days, max_files) tuples
                       for w1, w2, w3 respectively

    Returns:
        Dictionary mapping baseline_id (burst-subswath) to baseline product data:
        {
            "359429-IW1": {
                "burst_id": "359429",
                "subswath": "IW1",
                "w1": [RtcGranule, ...],
                "w2": [RtcGranule, ...],
                "w3": [RtcGranule, ...]
            },
            ...
        }

    Example:
        If 16 bursts with 3 subswaths each overlap a tile, this returns 48 baseline products
        (one for each burst+subswath combination).
    """
    from collections import defaultdict

    # Group granules by burst+subswath
    grouped_granules = defaultdict(list)

    for granule in available_files:
        burst_id, subswath = extract_burst_and_subswath_from_granule_id(granule.granule_id)

        if burst_id is None or subswath is None:
            logger.warning("Could not extract burst/subswath from granule ID: %s", granule.granule_id)
            continue

        # Use "burst-subswath" as the baseline identifier
        baseline_id = f"{burst_id}-{subswath}"
        grouped_granules[baseline_id].append(granule)

    # For each baseline (burst+subswath), perform lookback selection
    baseline_products = {}

    for baseline_id, granules in sorted(grouped_granules.items()):
        burst_id, subswath = baseline_id.split("-", 1)

        # Apply lookback window selection for this baseline
        w1, w2, w3 = select_dist_s1_input_files(t0, granules, window_configs)

        baseline_products[baseline_id] = {
            "burst_id": burst_id,
            "subswath": subswath,
            "w1": w1,
            "w2": w2,
            "w3": w3,
        }

    logger.info("Generated %d baseline products from %d RTC granules", len(baseline_products), len(available_files))

    return baseline_products


# ============================================================================
# CMR Query Functions
# ============================================================================


def get_bbox_from_tile_id(tile_id: str, margin_km: float = 75.0) -> str:
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


async def query_rtc_bursts_at_acquisition_time(
    tile_id: str,
    t0: datetime,
    time_tolerance_minutes: int = 10,
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
    auto_bbox: bool = True,
) -> tuple[list[tuple[str, str]], dict[str, list]]:
    """
    Query CMR for RTC bursts at the acquisition time to identify active bursts.

    This function finds which RTC bursts overlap the tile at approximately the
    acquisition time, within a specified time tolerance.

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR" or "T168")
        t0: Acquisition time
        time_tolerance_minutes: Time tolerance in minutes (default 10)
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Bounding box in format "west,south,east,north" (optional, will auto-derive if not provided)
        auto_bbox: If True and bbox is None, automatically derive bbox from tile_id (default True)

    Returns:
        Tuple of:
        - List of (burst_id, subswath) tuples representing active bursts at t0
        - Dictionary mapping "burst_id-subswath" to list of RtcGranule objects at t0
    """
    # Try to get valid bursts from the lookup table first
    valid_bursts_from_db = get_bursts_for_tile_from_db(tile_id)

    # Auto-derive bbox from tile_id if not provided and lookup table not available
    if bbox is None and auto_bbox and valid_bursts_from_db is None:
        bbox = get_bbox_from_tile_id(tile_id)
        if bbox:
            logger.info("Auto-derived bounding box from tile %s: %s", tile_id, bbox)

    # Create time range around t0 (±tolerance)
    time_start = t0 - timedelta(minutes=time_tolerance_minutes)
    time_end = t0 + timedelta(minutes=time_tolerance_minutes)

    timerange = DateTimeRange(
        start_date=time_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_date=time_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    logger.info("Querying RTC bursts at acquisition time %s (±%d min)", t0.isoformat(), time_tolerance_minutes)
    logger.info("  Time range: %s to %s", timerange.start_date, timerange.end_date)

    # Query CMR
    cmr_results = await async_query_cmr_v2(
        timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox
    )

    logger.info("  Found %d CMR results at acquisition time", len(cmr_results))

    # Extract unique burst+subswath combinations (dual-pol only) and collect granules
    active_bursts = set()
    t0_granules_by_burst = {}  # Map "burst_id-subswath" -> [RtcGranule, ...]
    filtered_single_pol = 0
    filtered_not_in_db = 0

    for result in cmr_results:
        umm = result.get("umm", {})
        granule_id = umm.get("GranuleUR")
        if not granule_id:
            continue

        # Extract acquisition time
        temporal = umm.get("TemporalExtent", {})
        range_dt = temporal.get("RangeDateTime", {})
        acq_time_str = range_dt.get("BeginningDateTime")
        if not acq_time_str:
            continue

        acq_time = isoparse(acq_time_str.replace("Z", "+00:00"))

        # Extract polarization
        polarization = _extract_polarization_from_umm(umm)

        # Check if dual-polarization (HH+HV or VV+VH)
        if not polarization or len(polarization) != 2:
            filtered_single_pol += 1
            logger.debug("  Filtered out non-dual-pol granule: %s (pol=%s)", granule_id, polarization)
            continue

        pol_set = set(polarization)
        if pol_set not in [{"HH", "HV"}, {"VV", "VH"}]:
            filtered_single_pol += 1
            logger.debug("  Filtered out invalid dual-pol granule: %s (pol=%s)", granule_id, polarization)
            continue

        burst_id, subswath = extract_burst_and_subswath_from_granule_id(granule_id)
        if burst_id and subswath:
            # If we have lookup table data, filter by it
            if valid_bursts_from_db is not None:
                # Extract the full burst ID from the RTC granule ID (includes RTC tile)
                # Example: "T168-359429-IW2" from "OPERA_L2_RTC-S1_T168-359429-IW2_..."
                full_burst_id = extract_full_burst_id_from_granule_id(granule_id)

                # Check if this burst is in the lookup table
                if full_burst_id and full_burst_id not in valid_bursts_from_db:
                    filtered_not_in_db += 1
                    logger.debug("  Filtered out burst not in lookup table: %s (burst=%s)", granule_id, full_burst_id)
                    continue
                else:
                    logger.info("  ACCEPTED burst from lookup table: %s", full_burst_id)  # ADD THIS
            else:
                logger.warning("  WARNING: Lookup table is None, not filtering bursts!")  # ADD THIS

            active_bursts.add((burst_id, subswath))

            # Store the granule for this burst
            baseline_id = f"{burst_id}-{subswath}"
            if baseline_id not in t0_granules_by_burst:
                t0_granules_by_burst[baseline_id] = []

            t0_granules_by_burst[baseline_id].append(
                RtcGranule(granule_id=granule_id, acquisition_time=acq_time, polarization=polarization)
            )

    logger.info("  Identified %d unique dual-pol burst+subswath combinations at t0", len(active_bursts))
    if filtered_single_pol > 0:
        logger.info("  Filtered out %d single-polarization or invalid granules", filtered_single_pol)
    if valid_bursts_from_db is not None and filtered_not_in_db > 0:
        logger.info("  Filtered out %d granules not in lookup table", filtered_not_in_db)

    # Deduplicate t0 granules for each burst (keep latest processing version per acquisition time)
    for baseline_id, granules in t0_granules_by_burst.items():
        original_count = len(granules)
        t0_granules_by_burst[baseline_id] = deduplicate_by_acquisition_time(granules)
        deduped_count = len(t0_granules_by_burst[baseline_id])
        if original_count > deduped_count:
            logger.info("  Deduplicated t0 granules for %s: %d -> %d", baseline_id, original_count, deduped_count)

    return sorted(list(active_bursts)), t0_granules_by_burst


async def query_rtc_granules_for_burst(
    tile_id: str,
    burst_id: str,
    subswath: str,
    t0: datetime,
    window_configs: list[tuple[int, int, int]],
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
) -> GranuleList:
    """
    Query CMR for RTC granules for a specific burst within lookback windows.

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR" or "T168")
        burst_id: Burst ID (e.g., "217642")
        subswath: Subswath (e.g., "IW2")
        t0: Reference time for lookback calculation
        window_configs: List of (years_back, window_size_days, max_files) tuples
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Bounding box in format "west,south,east,north"

    Returns:
        List of RtcGranule objects for this burst from all windows
    """
    all_granules = []

    # Query each window separately
    for years_back, window_size_days, max_files in window_configs:
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        # Create time range for this specific window
        timerange = DateTimeRange(
            start_date=lookback_window.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=lookback_window.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        logger.debug(
            "  Querying window w%d for burst %s-%s: %s to %s",
            years_back,
            burst_id,
            subswath,
            timerange.start_date,
            timerange.end_date,
        )

        # Query CMR
        cmr_results = await async_query_cmr_v2(
            timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox
        )

        # Filter for this specific burst+subswath
        matched_count = 0
        for result in cmr_results:
            umm = result.get("umm", {})
            granule_id = umm.get("GranuleUR")
            if not granule_id:
                continue

            # Check if this granule matches our burst+subswath
            g_burst_id, g_subswath = extract_burst_and_subswath_from_granule_id(granule_id)
            if g_burst_id != burst_id or g_subswath != subswath:
                continue

            # Extract acquisition time
            acquisition_time = _extract_acquisition_time_from_umm(umm)
            if not acquisition_time:
                continue

            # Extract polarization
            polarization = _extract_polarization_from_umm(umm)

            all_granules.append(RtcGranule(granule_id, acquisition_time, polarization))
            matched_count += 1

        logger.debug("    Found %d granules for burst %s-%s in w%d", matched_count, burst_id, subswath, years_back)

    return all_granules


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

            # Extract polarization
            polarization = _extract_polarization_from_umm(umm)

            all_granules.append(RtcGranule(granule_id, acquisition_time, polarization))
            matched_count += 1

        logger.info("  Accepted %d RTC granules within bbox", matched_count)

    logger.info("Total granules after filtering: %d", len(all_granules))
    return all_granules


def _extract_polarization_from_umm(umm: dict) -> Optional[list]:
    """
    Extract polarization from UMM-JSON metadata.

    Args:
        umm: UMM section of CMR response

    Returns:
        List of polarizations (e.g., ["VV", "VH"]) or None if not found
    """
    additional_attributes = umm.get("AdditionalAttributes", [])

    for attr in additional_attributes:
        if attr.get("Name") == "POLARIZATION":
            return attr.get("Values")  # e.g., ["VV", "VH"]

    return None


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
            return await _process_single_query_impl(tile_id, time, window_configs, bbox, auto_bbox)
    else:
        return await _process_single_query_impl(tile_id, time, window_configs, bbox, auto_bbox)


async def _process_single_query_impl(
    tile_id: str,
    time: datetime,
    window_configs: list[tuple[int, int, int]],
    bbox: Optional[str],
    auto_bbox: bool,
) -> dict:
    """
    Internal implementation of process_single_query.

    This implements the complete DIST-S1 workflow:
    1. Find all RTC bursts at acquisition time
    2. For each burst, query lookback windows and select files
    """
    # Use the new workflow that starts with finding active bursts at t0
    baseline_products = await query_and_select_baseline_products_for_dist_s1(
        tile_id=tile_id,
        t0=time,
        window_configs=window_configs,
        time_tolerance_minutes=10,
        bbox=bbox,
        auto_bbox=auto_bbox,
    )

    if not baseline_products:
        logger.warning("No baseline products generated for tile %s at time %s", tile_id, time.isoformat())
        return None

    return {
        "tile_id": tile_id,
        "reference_time": time,
        "baseline_products": baseline_products,
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

  # Custom window size (previous 30 days instead of default 60 days)
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
        help="Size of backward-looking window in days (default: 60, meaning previous 60 days from target date)",
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
            logger.info(
                "[%d/%d] Parsed %s: tile_id=%s, time=%s", i, len(native_ids), native_id, tile_id, time.isoformat()
            )
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

    # Output results based on format
    if args.output == "json":
        # JSON output with new baseline product structure
        if len(results) == 1:
            # Single query mode
            result = results[0]
            tile_id = result["tile_id"]
            time = result["reference_time"]
            baseline_products = result["baseline_products"]

            output = {
                "query": {
                    "tile_id": tile_id,
                    "reference_time": time.isoformat(),
                    "window_size_days": args.window_size,
                    "max_files": list(args.max_files),
                    "bbox": args.bbox,
                },
                "baseline_products": {},
                "summary": {
                    "total_baselines": len(baseline_products),
                    "total_granules": 0,
                },
            }

            # Format each baseline product
            for baseline_id, product in baseline_products.items():
                total_files = len(product["t0"]) + len(product["w1"]) + len(product["w2"]) + len(product["w3"])

                output["baseline_products"][baseline_id] = {
                    "burst_id": product["burst_id"],
                    "subswath": product["subswath"],
                    "t0": {
                        "description": "RTC granules at acquisition time",
                        "granules": [g.to_dict() for g in product["t0"]],
                        "count": len(product["t0"]),
                    },
                    "windows": {
                        "w1": {
                            "years_back": 1,
                            "window": calculate_lookback_window(time, 1, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in product["w1"]],
                            "count": len(product["w1"]),
                        },
                        "w2": {
                            "years_back": 2,
                            "window": calculate_lookback_window(time, 2, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in product["w2"]],
                            "count": len(product["w2"]),
                        },
                        "w3": {
                            "years_back": 3,
                            "window": calculate_lookback_window(time, 3, args.window_size).to_dict(),
                            "granules": [g.to_dict() for g in product["w3"]],
                            "count": len(product["w3"]),
                        },
                    },
                    "total_granules": total_files,
                }
                output["summary"]["total_granules"] += total_files

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

            total_baselines = 0
            total_granules = 0

            for result in results:
                tile_id = result["tile_id"]
                time = result["reference_time"]
                baseline_products = result["baseline_products"]

                result_entry = {
                    "native_id": result.get("native_id"),
                    "tile_id": tile_id,
                    "reference_time": time.isoformat(),
                    "baseline_products": {},
                }

                result_total_granules = 0
                for baseline_id, product in baseline_products.items():
                    files_count = len(product["t0"]) + len(product["w1"]) + len(product["w2"]) + len(product["w3"])

                    result_entry["baseline_products"][baseline_id] = {
                        "burst_id": product["burst_id"],
                        "subswath": product["subswath"],
                        "t0": {
                            "description": "RTC granules at acquisition time",
                            "granules": [g.to_dict() for g in product["t0"]],
                            "count": len(product["t0"]),
                        },
                        "windows": {
                            "w1": {
                                "years_back": 1,
                                "window": calculate_lookback_window(time, 1, args.window_size).to_dict(),
                                "granules": [g.to_dict() for g in product["w1"]],
                                "count": len(product["w1"]),
                            },
                            "w2": {
                                "years_back": 2,
                                "window": calculate_lookback_window(time, 2, args.window_size).to_dict(),
                                "granules": [g.to_dict() for g in product["w2"]],
                                "count": len(product["w2"]),
                            },
                            "w3": {
                                "years_back": 3,
                                "window": calculate_lookback_window(time, 3, args.window_size).to_dict(),
                                "granules": [g.to_dict() for g in product["w3"]],
                                "count": len(product["w3"]),
                            },
                        },
                        "total_granules": files_count,
                    }
                    result_total_granules += files_count

                result_entry["total_granules"] = result_total_granules
                result_entry["total_baselines"] = len(baseline_products)

                output["results"].append(result_entry)

                total_baselines += len(baseline_products)
                total_granules += result_total_granules

            # Add summary
            output["summary"] = {
                "total_queries": len(results),
                "total_baselines": total_baselines,
                "total_granules": total_granules,
            }

        print(json.dumps(output, indent=2))

    elif args.output == "ids":
        # Output granule IDs only, one per line (t0 + lookback windows)
        for result in results:
            baseline_products = result["baseline_products"]
            for baseline_id, product in baseline_products.items():
                # Include t0 granules and lookback window granules
                for window_name in ["t0", "w1", "w2", "w3"]:
                    for g in product[window_name]:
                        print(g.granule_id)

    else:
        # Text output (human-readable)
        if len(results) == 1:
            # Single query mode
            result = results[0]
            tile_id = result["tile_id"]
            time = result["reference_time"]
            baseline_products = result["baseline_products"]

            print("\n" + "=" * 80)
            print("DIST-S1 Baseline Product Selection Results")
            print("=" * 80 + "\n")

            print(f"Tile: {tile_id}")
            print(f"Acquisition time: {time.isoformat()}")
            print(f"Found {len(baseline_products)} baseline products (unique burst+subswath combinations)\n")

            total_files = 0
            for baseline_id, product in sorted(baseline_products.items()):
                burst_id = product["burst_id"]
                subswath = product["subswath"]
                t0 = product["t0"]
                w1 = product["w1"]
                w2 = product["w2"]
                w3 = product["w3"]

                baseline_total = len(t0) + len(w1) + len(w2) + len(w3)
                total_files += baseline_total

                print("-" * 80)
                print(f"Baseline: {baseline_id} (burst={burst_id}, subswath={subswath})")
                print(f"Total files: {baseline_total} (t0={len(t0)}, w1={len(w1)}, w2={len(w2)}, w3={len(w3)})")
                print()

                # Show t0 granules
                print(f"  Acquisition Time (t0):")
                print(f"    Files found: {len(t0)}")
                if len(t0) > 0:
                    for g in t0:
                        print(f"      {g.acquisition_time.isoformat()}: {g.granule_id}")
                else:
                    print("      (No granules found)")
                print()

                for window_name, granules, years_back in [
                    ("Window 1", w1, 1),
                    ("Window 2", w2, 2),
                    ("Window 3", w3, 3),
                ]:
                    lookback_window = calculate_lookback_window(time, years_back, args.window_size)

                    years_suffix = "s" if years_back > 1 else ""
                    print(f"  {window_name} (t0 - {years_back} year{years_suffix}):")
                    print(f"    Target date: {lookback_window.window_end.isoformat()}")
                    print(
                        f"    Range: {lookback_window.window_start.isoformat()} to {lookback_window.window_end.isoformat()}"
                    )
                    print(f"    Files found: {len(granules)}/{args.max_files[years_back - 1]}")

                    if len(granules) > 0:
                        for g in granules:
                            days_from_target = (g.acquisition_time - lookback_window.window_end).days
                            sign = "+" if days_from_target >= 0 else ""
                            print(f"      {g.acquisition_time.isoformat()} ({sign}{days_from_target}d): {g.granule_id}")
                    else:
                        print("      (No granules found)")

                    print()

            print("=" * 80)
            print(f"Total baselines: {len(baseline_products)}")
            print(f"Total files selected: {total_files}")
            print("=" * 80 + "\n")

        else:
            # Batch mode
            print("\n" + "=" * 80)
            print("DIST-S1 Baseline Product Selection Results (Batch)")
            print("=" * 80 + "\n")

            grand_total_files = 0
            grand_total_baselines = 0

            for i, result in enumerate(results, 1):
                tile_id = result["tile_id"]
                time = result["reference_time"]
                baseline_products = result["baseline_products"]
                native_id = result.get("native_id")

                print(f"[{i}/{len(results)}] {native_id}")
                print(f"  Tile: {tile_id}, Time: {time.isoformat()}")
                print(f"  Baselines found: {len(baseline_products)}")

                result_total_files = 0
                for baseline_id, product in baseline_products.items():
                    t0_count = len(product["t0"])
                    w1_count = len(product["w1"])
                    w2_count = len(product["w2"])
                    w3_count = len(product["w3"])
                    baseline_total = t0_count + w1_count + w2_count + w3_count
                    result_total_files += baseline_total

                print(f"  Total files for this query: {result_total_files}")
                print()

                grand_total_files += result_total_files
                grand_total_baselines += len(baseline_products)

            print("=" * 80)
            print(f"Processed {len(results)} queries")
            print(f"Total baselines: {grand_total_baselines}")
            print(f"Total files selected: {grand_total_files}")
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
