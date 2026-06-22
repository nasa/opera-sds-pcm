#!/usr/bin/env python3
"""
CLI tool for DIST-S1 lookback window CMR queries.

This tool queries CMR for RTC-S1 files within three backward-looking lookback windows
ending at t0 - 1 year, t0 - 2 years, and t0 - 3 years. Each window looks backward
a specified number of days from its target date.

Files are selected as the n closest files to the END of each window (target date).
For each unique burst+subswath combination, lookback window selection is performed
independently, generating separate "baseline products".

The tool supports three modes:
1. Single query: Query for a specific tile at a specific acquisition time
2. Batch mode: Process multiple tiles/times from an input file
3. Temporal window mode: Forecast the expected number of DIST-S1 jobs that would be
   triggered across all tiles between a start and end date
"""

import argparse
import asyncio
import json
import logging
import math
import os
import re
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dateutil.parser import isoparse

from data_subscriber.cmr import DateTimeRange, async_query_cmr_v2
from data_subscriber.dist_s1_utils import localize_dist_burst_db
from tools.dist_s1_input_formatters import (
    format_ids_output,
    format_json_output,
    format_temporal_window_output,
    format_text_output,
)
from tools.ops.cmr_audit.cmr_audit_utils import extract_fields

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)

DIST_S1_NATIVE_ID_REGEX = (
    r"OPERA_L3_DIST(?:-ALERT)?-S1_"
    r"(?P<tile_id>T?\w+)_"
    r"(?P<acq_time>\d{8}T\d{6}Z)_"
    r"(?P<prod_time>\d{8}T\d{6}Z)_"
    r"S1[A-D]?_30_v[\d.]+.*"
)

class DistBurstDb:
    """Provides a clean interface over the DIST-S1 MGRS burst lookup table.

    Wraps the raw 4-tuple (dist_products, bursts_to_products, product_to_bursts, all_tile_ids)
    returned by localize_dist_burst_db() and exposes domain-level queries.
    """

    def __init__(self, dist_products: dict, bursts_to_products: dict, product_to_bursts: dict):
        self._dist_products = dist_products  # tile_id -> set of product_ids
        self._bursts_to_products = bursts_to_products  # burst_id -> set of product_ids
        self._product_to_bursts = product_to_bursts  # product_id -> set of burst_ids

    @classmethod
    def load(cls) -> "DistBurstDb":
        """Load the burst database from localize_dist_burst_db()."""
        dist_products, bursts_to_products, product_to_bursts, _ = localize_dist_burst_db()
        return cls(dist_products, bursts_to_products, product_to_bursts)

    def bursts_by_track(self, tile_id: str) -> Optional[dict]:
        """Get burst IDs organized by track (product) for a tile. Returns None if not found."""
        normalized_tile = tile_id if not tile_id.startswith("T") else tile_id[1:]
        tile_variants = [normalized_tile, f"T{normalized_tile}"]

        for tile_variant in tile_variants:
            if tile_variant in self._dist_products:
                product_ids = self._dist_products[tile_variant]
                bursts_by_track = {}
                for product_id in product_ids:
                    if product_id in self._product_to_bursts:
                        bursts_by_track[product_id] = set(self._product_to_bursts[product_id])
                logger.info("Found %d tracks for tile %s from lookup table", len(bursts_by_track), tile_variant)
                return bursts_by_track

        logger.warning("Tile %s not found in lookup table", tile_id)
        return None

    def bursts_for_tile(self, tile_id: str) -> Optional[set]:
        """Get all burst IDs that overlap a given MGRS tile."""
        bursts_by_track = self.bursts_by_track(tile_id)
        if bursts_by_track is None:
            logger.warning("Falling back to bbox-based search")
            return None
        all_bursts = set()
        for bursts in bursts_by_track.values():
            all_bursts.update(bursts)
        logger.info("Found %d bursts for tile %s from lookup table", len(all_bursts), tile_id)
        return all_bursts

    def products_for_burst(self, burst_id: str) -> set:
        """Get product IDs that contain a given burst."""
        return self._bursts_to_products.get(burst_id, set())

    def sample_burst_ids(self, n: int = 5) -> list:
        """Return a sample of burst IDs from the database (for debugging)."""
        return list(self._bursts_to_products.keys())[:n]


try:
    _burst_db = DistBurstDb.load()
except Exception as e:
    logger.error("Could not load MGRS burst lookup table: %s", e)
    _burst_db = None


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
        """Check if this granule has dual polarization (HH+HV or VV+VH)."""
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


def parse_dist_s1_native_id(native_id: str) -> tuple:
    """Parse a DIST-S1 native ID to extract tile ID and acquisition time."""

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


def _mgrs_tile_to_bbox(tile_id: str, margin_km: float = 15.0) -> tuple:
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

        if tile_id.startswith("T"):
            tile_id = tile_id[1:]
        if len(tile_id) >= 2 and tile_id[0] == "0" and tile_id[1].isdigit():
            tile_id = tile_id[1:]

        mgrs_obj = mgrs.MGRS()
        lat_ll, lon_ll = mgrs_obj.toLatLon(tile_id)

        grid_square_size_km = 100.0
        lat_deg_per_km = 1.0 / 111.0

        # Use center latitude for longitude scaling
        lat_center = lat_ll + (grid_square_size_km * lat_deg_per_km) / 2.0
        cos_lat = math.cos(math.radians(lat_center))

        # Avoid division by zero near poles
        cos_lat = max(cos_lat, 0.01)
        lon_deg_per_km = 1.0 / (111.0 * cos_lat)

        grid_size_lat = grid_square_size_km * lat_deg_per_km
        grid_size_lon = grid_square_size_km * lon_deg_per_km
        margin_lat = margin_km * lat_deg_per_km
        margin_lon = margin_km * lon_deg_per_km

        lat_min = max(lat_ll - margin_lat, -90.0)
        lat_max = min(lat_ll + grid_size_lat + margin_lat, 90.0)
        lon_min = lon_ll - margin_lon
        lon_max = lon_ll + grid_size_lon + margin_lon

        # Detect antimeridian crossing and warn — split queries may be needed
        crosses_antimeridian = lon_min < -180 or lon_max > 180
        if crosses_antimeridian:
            logger.warning(
                "MGRS tile '%s' bbox crosses antimeridian — CMR bbox query may be unreliable. "
                "Consider using a polygon query instead.",
                tile_id,
            )
            lon_min = max(lon_min, -180.0)
            lon_max = min(lon_max, 180.0)

        return (lon_min, lat_min, lon_max, lat_max)

    except ImportError:
        logger.error("'mgrs' library not installed. Install with: pip install mgrs")
        return None
    except Exception as e:
        logger.error("Could not convert MGRS tile '%s' to bbox: %s", tile_id, e)
        return None


def get_bursts_by_track_from_db(tile_id: str) -> Optional[dict]:
    """Get burst IDs organized by track (product) for a tile. Returns None if not found."""
    if _burst_db is None:
        logger.warning("Burst DB not loaded, cannot look up tile %s", tile_id)
        return None
    return _burst_db.bursts_by_track(tile_id)


def get_bursts_for_tile_from_db(tile_id: str) -> Optional[set]:
    """Get all burst IDs that overlap a given MGRS tile from the lookup table."""
    if _burst_db is None:
        logger.warning("Burst DB not loaded, cannot look up tile %s", tile_id)
        return None
    return _burst_db.bursts_for_tile(tile_id)


def identify_track_from_active_bursts(active_burst_ids: set, bursts_by_track: dict) -> tuple:
    """Identify which track the active bursts belong to. Returns (product_id, expected_bursts) or (None, None)."""
    if not bursts_by_track:
        return None, None

    # Find the track whose burst set has the most overlap with active bursts
    best_match_product = None
    best_match_score = 0
    best_match_bursts = None

    for product_id, expected_bursts in bursts_by_track.items():
        # Count how many active bursts match this track
        overlap = len(active_burst_ids & expected_bursts)

        if overlap > best_match_score:
            best_match_score = overlap
            best_match_product = product_id
            best_match_bursts = expected_bursts

    if best_match_product:
        logger.info(
            "Identified track %s with %d/%d burst matches",
            best_match_product,
            best_match_score,
            len(best_match_bursts),
        )
        return best_match_product, best_match_bursts

    return None, None


def extract_full_burst_id_from_granule_id(granule_id: str) -> str:
    """Extract full burst ID (e.g., 'T168-359429-IW2') from RTC granule ID."""
    match = re.search(r"OPERA_L2_RTC-S1_(T?\w+-\d+-IW[123])_", granule_id)
    return match.group(1) if match else None


def extract_burst_and_subswath_from_granule_id(granule_id: str) -> tuple:
    """Extract burst ID and subswath from an RTC granule ID."""
    full_burst_id = extract_full_burst_id_from_granule_id(granule_id)
    if not full_burst_id:
        return None, None
    # Parse full burst ID like "T168-359429-IW2" to get components
    parts = full_burst_id.split("-")
    if len(parts) >= 3:
        return parts[-2], parts[-1]  # burst_id, subswath
    return None, None


def deduplicate_by_acquisition_time(granules: GranuleList) -> GranuleList:
    """Keep only the latest processing version for each acquisition time."""
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


def calculate_lookback_window(t0: datetime, years_back: int, window_size_days: int) -> LookbackWindow:
    """Calculate backward-looking window ending at t0 - years_back years."""
    # Calculate the target date (end of the window)
    days_back = years_back * 365
    window_end = t0 - timedelta(days=days_back)

    # Look backward from the target date
    window_start = window_end - timedelta(days=window_size_days)

    # Calculate midpoint for reference
    window_center = window_start + timedelta(days=window_size_days // 2)

    return LookbackWindow(window_start, window_center, window_end)


def cluster_acquisition_times(granules: list[dict], time_tolerance_minutes: int = 10) -> dict[datetime, list[dict]]:
    """
    Group granules by acquisition time with temporal clustering.

    Granules with acquisition times within time_tolerance_minutes of each other
    are grouped into the same cluster. The representative time for each cluster
    is the earliest acquisition time in that cluster.

    Args:
        granules: List of granule dictionaries with 'acquisition_time' field
        time_tolerance_minutes: Maximum time difference (in minutes) to group granules together

    Returns:
        Dictionary mapping representative_time -> list of granules in that cluster
    """
    if not granules:
        return {}

    # Sort granules by acquisition time
    sorted_granules = sorted(granules, key=lambda g: g["acquisition_time"])

    clusters = {}
    current_cluster_time = None
    current_cluster = []

    for granule in sorted_granules:
        acq_time = granule["acquisition_time"]

        if current_cluster_time is None:
            # Start first cluster
            current_cluster_time = acq_time
            current_cluster = [granule]
        else:
            # Check if this granule belongs to the current cluster
            time_diff = abs((acq_time - current_cluster_time).total_seconds() / 60.0)

            if time_diff <= time_tolerance_minutes:
                # Add to current cluster
                current_cluster.append(granule)
            else:
                # Save current cluster and start a new one
                clusters[current_cluster_time] = current_cluster
                current_cluster_time = acq_time
                current_cluster = [granule]

    # Save the last cluster
    if current_cluster:
        clusters[current_cluster_time] = current_cluster

    logger.info(f"Clustered {len(granules)} granules into {len(clusters)} acquisition time groups")
    return clusters


def select_files_in_window(
    available_files: GranuleList, lookback_window: LookbackWindow, max_files: int
) -> GranuleList:
    """Select files within a window, choosing those closest to the window end."""
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
) -> dict:
    """
    Complete DIST-S1 baseline product selection workflow using consolidated queries.

    This implementation optimizes the workflow to reduce CMR queries by:
    1. First querying for t0 granules to identify active bursts (unchanged)
    2. Then querying for all historical data across the tile area at once
       (one query per lookback window rather than per-burst)
    3. Processing and filtering the results in memory to select appropriate
       granules for each burst

    Returns dict mapping baseline_id to baseline product data with t0, w1, w2, w3 granules.
    Returns empty dict if no bursts found or incomplete burst coverage for the track.

    Args:
        tile_id: MGRS tile ID
        t0: Reference time
        window_configs: List of (years_back, window_size_days, max_files) tuples
        time_tolerance_minutes: Tolerance for t0 acquisition time matching
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
    """
    # Step 1: Find active bursts at acquisition time (unchanged from original)
    logger.info("Step 1: Finding RTC bursts at acquisition time %s for tile %s", t0.isoformat(), tile_id)

    # Get expected bursts organized by track from lookup table
    bursts_by_track = get_bursts_by_track_from_db(tile_id)

    # Auto-derive bbox from tile_id
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
    )

    if not active_bursts:
        logger.warning("No RTC bursts found at acquisition time %s for tile %s", t0.isoformat(), tile_id)
        return {
            "baseline_products": {},
            "diagnostics": {
                "active_bursts_found": 0,
                "baselines_with_t0_data": 0,
                "baselines_filtered_no_historical": 0,
                "reason": "No RTC bursts found at acquisition time",
            },
        }

    logger.info("Found %d active burst+subswath combinations at t0", len(active_bursts))
    total_t0_granules = sum(len(granules) for granules in t0_granules.values())
    logger.info("Found %d RTC granules at t0 across all bursts", total_t0_granules)

    # Note: We allow different bursts to have different polarizations (e.g., burst A with HH+HV, burst B with VV+VH)
    # Polarization consistency is enforced per-burst in the processing below

    # Log burst coverage information (for tracking purposes only - partial coverage is now acceptable)
    if bursts_by_track is not None:
        # Extract RTC tile prefix from the t0 granule IDs to construct full burst IDs
        # Example: from "OPERA_L2_RTC-S1_T168-359429-IW2_..." extract "T168"
        rtc_tile_prefix = None
        for baseline_id, granules in t0_granules.items():
            if granules:
                full_burst_id = extract_full_burst_id_from_granule_id(granules[0].granule_id)
                if full_burst_id:
                    # Extract tile prefix (e.g., "T168" from "T168-359429-IW2")
                    rtc_tile_prefix = full_burst_id.split("-")[0]
                    break

        if rtc_tile_prefix:
            # Convert active bursts to full burst IDs for comparison
            active_full_burst_ids = set()
            for burst_id, subswath in active_bursts:
                full_burst_id = f"{rtc_tile_prefix}-{burst_id}-{subswath}"
                active_full_burst_ids.add(full_burst_id)

            # Identify which track (product) these bursts belong to
            product_id, expected_bursts_for_track = identify_track_from_active_bursts(
                active_full_burst_ids, bursts_by_track
            )

            if product_id and expected_bursts_for_track:
                # Check burst coverage status (informational only)
                missing_bursts = expected_bursts_for_track - active_full_burst_ids

                if missing_bursts:
                    logger.info(
                        "Partial burst coverage for track %s (tile %s): Found %d/%d expected bursts",
                        product_id,
                        tile_id,
                        len(active_full_burst_ids),
                        len(expected_bursts_for_track),
                    )
                    logger.info("Missing bursts: %s", sorted(missing_bursts))
                    logger.info("Proceeding with partial coverage")
                else:
                    logger.info(
                        "✓ Complete burst coverage for track %s: Found t0 data for all %d expected bursts",
                        product_id,
                        len(expected_bursts_for_track),
                    )
            else:
                logger.info(
                    "Could not identify track from active bursts - proceeding with available bursts. "
                    "This may indicate a new track or configuration issue."
                )
        else:
            logger.info("Could not extract RTC tile prefix from granules, proceeding with available bursts")

    # Step 2: Query all historical data for the tile at once
    logger.info("Step 2: Querying all historical data for tile %s across all windows", tile_id)
    historical_data = await query_historical_data_for_tile(
        tile_id=tile_id,
        t0=t0,
        window_configs=window_configs,
        provider=provider,
        collection=collection,
        bbox=bbox,
    )

    # Log statistics about historical data retrieved
    for years_back, granules in historical_data.items():
        logger.info(
            "Retrieved %d historical granules for window w%d (years_back=%d)", len(granules), years_back, years_back
        )

    total_historical_granules = sum(len(granules) for granules in historical_data.values())
    logger.info("Retrieved %d total historical granules across all windows", total_historical_granules)

    # Step 3: Process each burst using the consolidated historical data
    logger.info("Step 3: Processing %d bursts using pre-queried historical data", len(active_bursts))

    # Create a dictionary to map burst IDs to their historical granules for quick lookup
    # This avoids needing to scan through all historical granules for each burst
    historical_granules_by_burst = {}

    for years_back, all_granules in historical_data.items():
        for granule in all_granules:
            burst_id, subswath = extract_burst_and_subswath_from_granule_id(granule.granule_id)
            if burst_id and subswath:
                burst_key = f"{burst_id}-{subswath}"

                if burst_key not in historical_granules_by_burst:
                    historical_granules_by_burst[burst_key] = []

                historical_granules_by_burst[burst_key].append(granule)

    # Process all bursts — this is pure in-memory filtering, no I/O needed
    baseline_products = {}
    for burst_id, subswath in active_bursts:
        baseline_id = f"{burst_id}-{subswath}"
        logger.info("Processing burst %s...", baseline_id)

        # Get historical granules for this specific burst
        burst_granules = historical_granules_by_burst.get(baseline_id, [])

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
                filtered_granules = [
                    granule
                    for granule in burst_granules
                    if granule.polarization and set(granule.polarization) == reference_pol_set
                ]

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

        logger.info(
            "  Selected files for %s: t0=%d, w1=%d, w2=%d, w3=%d (total=%d)",
            baseline_id,
            len(t0_burst_granules),
            len(w1),
            len(w2),
            len(w3),
            len(t0_burst_granules) + len(w1) + len(w2) + len(w3),
        )

        baseline_products[baseline_id] = {
            "burst_id": burst_id,
            "subswath": subswath,
            "t0": t0_burst_granules,
            "w1": w1,
            "w2": w2,
            "w3": w3,
        }

    logger.info("Generated %d baseline products", len(baseline_products))

    # Filter out baseline products that have no historical granules
    # If all lookback windows are empty, the product can't be generated
    filtered_baseline_products = {}
    filtered_out_count = 0
    for baseline_id, product in baseline_products.items():
        total_historical = len(product["w1"]) + len(product["w2"]) + len(product["w3"])
        if total_historical > 0:
            filtered_baseline_products[baseline_id] = product
        else:
            filtered_out_count += 1
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

    # Return both the products and diagnostic information
    return {
        "baseline_products": filtered_baseline_products,
        "diagnostics": {
            "active_bursts_found": len(active_bursts),
            "baselines_with_t0_data": len(baseline_products),
            "baselines_with_historical_data": len(filtered_baseline_products),
            "baselines_filtered_no_historical": filtered_out_count,
            "query_optimization": {
                "original_queries_saved": 3 * len(active_bursts) - 3,  # We make 3 queries instead of 3 * num_bursts
                "reduction_percent": round((3 * len(active_bursts) - 3) / (3 * len(active_bursts) + 1) * 100, 1),
            },
        },
    }


def get_bbox_from_tile_id(tile_id: str, margin_km: float = 75.0) -> str:
    """Get bounding box string from MGRS tile ID in format 'west,south,east,north'."""
    result = _mgrs_tile_to_bbox(tile_id, margin_km)
    if result is None:
        return None

    lon_min, lat_min, lon_max, lat_max = result

    # Format as "west,south,east,north"
    return f"{lon_min},{lat_min},{lon_max},{lat_max}"


async def query_historical_data_for_tile(
    tile_id: str,
    t0: datetime,
    window_configs: list[tuple[int, int, int]],
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
) -> dict[int, GranuleList]:
    """
    Query all historical RTC granules for a tile across all lookback windows.

    This function performs one query per lookback window for the entire tile area,
    rather than querying each burst separately. This drastically reduces the number
    of CMR queries needed while still retrieving all necessary data.

    Features:
    - One query per lookback window instead of per-burst queries
    - Pagination support for large result sets (tiles with many bursts)
    - Concurrent querying of different windows

    Args:
        tile_id: MGRS tile ID
        t0: Reference time
        window_configs: List of (years_back, window_size_days, max_files) tuples
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Optional bounding box (if None, will be auto-derived from tile_id)

    Returns:
        Dictionary mapping years_back -> list of RtcGranules
    """
    # Auto-derive bbox from tile_id if not provided
    if bbox is None:
        bbox = get_bbox_from_tile_id(tile_id)
        if bbox:
            logger.info("Auto-derived bounding box from tile %s: %s", tile_id, bbox)

    results = {}  # years_back -> granules

    # Query each window in parallel
    async def query_window(years_back: int, window_size_days: int):
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        # Create time range for this window
        timerange = DateTimeRange(
            start_date=lookback_window.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=lookback_window.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        logger.info("Querying historical window w%d: %s to %s", years_back, timerange.start_date, timerange.end_date)

        logger.info("Querying CMR for tile %s in w%d", tile_id, years_back)
        window_granules = []
        with tempfile.TemporaryDirectory() as tmpdir:
            rtc_paths = await async_query_cmr_v2(
                timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox, output_dir=tmpdir
            )
            rtc_records = extract_fields(rtc_paths, ["umm.GranuleUR", "umm.TemporalExtent", "umm.AdditionalAttributes"])

            logger.info("Found %d CMR results for tile %s in w%d", len(rtc_records), tile_id, years_back)

        # Convert CMR results to RtcGranule objects
        for rtc_record in rtc_records:
            granule_id = rtc_record.get("GranuleUR")
            if not granule_id:
                continue

            # Extract acquisition time
            acquisition_time = _extract_acquisition_time_from_umm(rtc_record.get("umm.TemporalExtent", {}))
            if not acquisition_time:
                continue

            # Extract polarization
            polarization = _extract_polarization_from_umm(rtc_record.get("umm.AdditionalAttributes", {}))

            window_granules.append(RtcGranule(granule_id, acquisition_time, polarization))

        logger.info("Total granules retrieved for window w%d: %d", years_back, len(window_granules))
        results[years_back] = window_granules

    # Query all windows concurrently
    tasks = [query_window(years_back, window_size_days) for years_back, window_size_days, _max_files in window_configs]
    await asyncio.gather(*tasks)

    return results


async def query_rtc_bursts_at_acquisition_time(
    tile_id: str,
    t0: datetime,
    time_tolerance_minutes: int = 10,
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
) -> tuple[list[tuple[str, str]], dict[str, list]]:
    """
    Query for RTC bursts at acquisition time.

    Returns (active_bursts, t0_granules_by_burst).
    """
    # Try to get valid bursts from the lookup table first
    valid_bursts_from_db = get_bursts_for_tile_from_db(tile_id)

    # Auto-derive bbox from tile_id if not provided and lookup table not available
    if bbox is None and valid_bursts_from_db is None:
        bbox = get_bbox_from_tile_id(tile_id)
        if bbox:
            logger.info("Auto-derived bounding box from tile %s: %s", tile_id, bbox)

    # Create time range around t0 (±tolerance)
    time_start = t0 - timedelta(minutes=time_tolerance_minutes)
    time_end = t0 + timedelta(minutes=time_tolerance_minutes)

    logger.info("Querying RTC bursts at acquisition time %s (±%d min)", t0.isoformat(), time_tolerance_minutes)
    logger.info("  Time range: %s to %s", time_start.isoformat(), time_end.isoformat())

    t0_rtc_granules = []
    timerange = DateTimeRange(
        start_date=time_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_date=time_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    logger.debug("Querying CMR for t0 granules")
    with tempfile.TemporaryDirectory() as tmpdir:
        rtc_paths = await async_query_cmr_v2(
            timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox, output_dir=tmpdir
        )
        rtc_records = extract_fields(rtc_paths, ["umm.GranuleUR", "umm.TemporalExtent", "umm.AdditionalAttributes"])

        logger.info("  Found %d CMR results at acquisition time", len(rtc_records))

    # Convert CMR UMM-JSON results to RtcGranule objects
    for rtc_record in rtc_records:
        granule_id = rtc_record.get("umm.GranuleUR")
        if not granule_id:
            continue
        acquisition_time = _extract_acquisition_time_from_umm(rtc_record.get("umm.TemporalExtent", {}))
        if not acquisition_time:
            continue
        polarization = _extract_polarization_from_umm(rtc_record.get("umm.AdditionalAttributes", {}))
        t0_rtc_granules.append(RtcGranule(granule_id, acquisition_time, polarization))

    # Extract unique burst+subswath combinations (dual-pol only) and collect granules
    active_bursts = set()
    t0_granules_by_burst = {}  # Map "burst_id-subswath" -> [RtcGranule, ...]
    filtered_single_pol = 0
    filtered_not_in_db = 0

    for granule in t0_rtc_granules:
        granule_id = granule.granule_id
        acq_time = granule.acquisition_time
        polarization = granule.polarization

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
                    logger.debug("  Accepted burst from lookup table: %s", full_burst_id)
            else:
                logger.debug("  Lookup table not available, accepting all bursts from CMR query")

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


def _extract_polarization_from_umm(additional_attributes: dict) -> Optional[list]:
    """Extract polarization from UMM-JSON metadata."""
    for attr in additional_attributes:
        if attr.get("Name") == "POLARIZATION":
            return attr.get("Values")  # e.g., ["VV", "VH"]

    return None


def _extract_acquisition_time_from_umm(temporal_extent: dict) -> Optional[datetime]:
    """Extract acquisition time from UMM-JSON metadata."""
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
    return None


def _parse_inputs_from_args(args) -> list[tuple[Optional[str], str, datetime, Optional[str]]]:
    """Parse inputs from args, return list of (native_id, tile_id, time, acq_group) tuples."""
    if args.native_id:
        tile_id, time = parse_dist_s1_native_id(args.native_id)
        if tile_id is None or time is None:
            logger.error("Failed to parse DIST-S1 native ID: %s", args.native_id)
            logger.error("Expected format: OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1")
            sys.exit(1)
        logger.info("Parsed native ID: tile_id=%s, time=%s", tile_id, time.isoformat())
        return [(args.native_id, tile_id, time, None)]

    # Single query with tile_id and time
    if args.tile_id and args.time:
        # Extract the acquisition group suffix (_0, _1, etc.) but preserve it
        # E.g., "04WDU_1" -> "04WDU" with acq_group "1"
        acq_group = None

        if "_" in args.tile_id and not args.tile_id.startswith("OPERA_"):
            parts = args.tile_id.rsplit("_", 1)
            tile_id = parts[0]
            acq_group = parts[1]

        else:
            tile_id = args.tile_id
        return [(None, tile_id, args.time, acq_group)]

    # Batch mode from input file
    if args.input_file:
        logger.info("Reading entries from file: %s", args.input_file)
        try:
            with open(args.input_file, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error("Input file not found: %s", args.input_file)
            sys.exit(1)
        except Exception as e:
            logger.error("Failed to read input file: %s", e)
            sys.exit(1)

        if not lines:
            logger.error("No entries found in input file")
            sys.exit(1)

        logger.info("Processing %d entries from file...", len(lines))

        # Parse all entries - support both formats:
        # 1. Simple format: tile_id,timestamp (e.g., "04WDU_1,20260225T035340Z")
        # 2. Full DIST-S1 native ID (e.g., "OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_...")
        parsed_items = []
        for i, line in enumerate(lines, 1):
            # Try simple format first (tile_id,timestamp)
            # Format: mgrs_tile_id_acq_group,timestamp (e.g., "04WDU_1,20260225T035340Z")
            if "," in line and not line.startswith("OPERA_"):
                parts = line.split(",", 1)
                if len(parts) == 2:
                    tile_id_with_group = parts[0].strip()
                    time_str = parts[1].strip()

                    # Extract the acquisition group suffix (_0, _1, etc.) but preserve it
                    # E.g., "04WDU_1" -> "04WDU" with acq_group "1"
                    acq_group = None
                    if "_" in tile_id_with_group:
                        parts = tile_id_with_group.rsplit("_", 1)
                        tile_id = parts[0]
                        acq_group = parts[1]
                    else:
                        tile_id = tile_id_with_group

                    try:
                        time = parse_datetime(time_str)
                        logger.debug(
                            "[%d/%d] Parsed simple format: tile_id=%s (from %s), acq_group=%s, time=%s",
                            i,
                            len(lines),
                            tile_id,
                            tile_id_with_group,
                            acq_group,
                            time.isoformat(),
                        )
                        parsed_items.append((None, tile_id, time, acq_group))
                        continue
                    except Exception as e:
                        logger.warning("[%d/%d] Failed to parse simple format '%s': %s", i, len(lines), line, e)

            # Try full DIST-S1 native ID format
            tile_id, time = parse_dist_s1_native_id(line)
            if tile_id is None or time is None:
                logger.warning("[%d/%d] Failed to parse entry, skipping: %s", i, len(lines), line)
                continue
            logger.info("[%d/%d] Parsed native ID: tile_id=%s, time=%s", i, len(lines), tile_id, time.isoformat())
            parsed_items.append((line, tile_id, time, None))

        if not parsed_items:
            logger.error("No valid entries to process")
            sys.exit(1)

        return parsed_items

    # Should not reach here due to argparse validation
    logger.error("No valid input provided")
    sys.exit(1)


def parse_max_files(value: str) -> tuple[int, int, int]:
    """Parse max_files argument in format "W1,W2,W3" (e.g., "4,3,3")."""
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
    """Parse datetime string in ISO format."""
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
    semaphore: Optional[asyncio.Semaphore] = None,
) -> dict:
    """
    Process a single DIST-S1 lookback query.

    Args:
        tile_id: MGRS tile ID
        time: Reference time
        window_configs: Window configurations (years_back, window_size_days, max_files)
        semaphore: Optional semaphore for rate limiting concurrent requests

    Returns:
        Dictionary with query results
    """
    # Use semaphore for rate limiting if provided
    if semaphore:
        async with semaphore:
            return await _process_single_query_impl(tile_id, time, window_configs)
    else:
        return await _process_single_query_impl(tile_id, time, window_configs)


async def _process_single_query_impl(
    tile_id: str,
    time: datetime,
    window_configs: list[tuple[int, int, int]],
) -> dict:
    """
    Internal implementation of process_single_query.

    This implements the complete DIST-S1 workflow:
    1. Find all RTC bursts at acquisition time
    2. For each burst, query lookback windows and select files
    """
    # Use the new workflow that starts with finding active bursts at t0
    result = await query_and_select_baseline_products_for_dist_s1(
        tile_id=tile_id,
        t0=time,
        window_configs=window_configs,
        time_tolerance_minutes=10,
    )

    baseline_products = result["baseline_products"]
    diagnostics = result["diagnostics"]

    if not baseline_products:
        logger.warning("No baseline products generated for tile %s at time %s", tile_id, time.isoformat())
        logger.debug("  Diagnostics: %s", diagnostics)
        return None

    return {
        "tile_id": tile_id,
        "reference_time": time,
        "baseline_products": baseline_products,
        "diagnostics": diagnostics,
    }


async def query_temporal_window_jobs(
    start_date: datetime,
    end_date: datetime,
    window_configs: list[tuple[int, int, int]],
    max_concurrent: int = 3,
) -> dict:
    """
    Query CMR for RTC granules across a temporal window and forecast DIST-S1 jobs.

    This function determines how many DIST-S1 jobs would be triggered between
    start_date and end_date for all tiles that have RTC data in that period.
    A job is counted as "sufficient" if it has both complete burst coverage at t0
    and sufficient historical baseline data in the lookback windows.

    Args:
        start_date: Start of temporal window
        end_date: End of temporal window
        window_configs: List of (years_back, window_size_days, max_files) tuples
        max_concurrent: Maximum concurrent tile queries

    Returns:
        Dictionary with summary statistics and detailed results
    """
    logger.info("=" * 80)
    logger.info("DIST-S1 Temporal Window Job Forecast")
    logger.info("=" * 80)
    logger.info("Query Period: %s to %s", start_date.isoformat(), end_date.isoformat())
    logger.info(
        "Window Configuration: w1=%d, w2=%d, w3=%d (%d-day windows)",
        window_configs[0][2],
        window_configs[1][2],
        window_configs[2][2],
        window_configs[0][1],
    )

    # Step 1: Query CMR for all RTC granules in the temporal window
    logger.info("Step 1: Querying CMR for RTC granules in temporal window...")
    timerange = DateTimeRange(
        start_date=start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_date=end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        rtc_paths = await async_query_cmr_v2(
            timerange=timerange,
            provider="ASF",
            collection="OPERA_L2_RTC-S1_V1",
            token=None,
            bbox=None,
            output_dir=tmpdir,
        )
        rtc_records = extract_fields(rtc_paths, ["umm.GranuleUR", "umm.TemporalExtent"])

        logger.info("Found %d RTC granules in temporal window", len(rtc_records))

    if not rtc_records:
        logger.warning("No RTC granules found in temporal window")
        return {
            "query": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "window_configs": window_configs,
            },
            "summary": {
                "total_tiles": 0,
                "total_acquisition_times": 0,
                "jobs_with_sufficient_inputs": 0,
                "jobs_with_insufficient_inputs": 0,
            },
            "jobs_by_tile": {},
            "jobs_by_date": {},
            "details": [],
        }

    # Load the burst database to map bursts to MGRS tiles
    logger.info("Step 2: Loading DIST-S1 burst database...")

    # Step 3: Parse granules and extract acquisition times and burst IDs
    logger.info("Step 3: Parsing granules and extracting metadata...")
    granules_with_metadata = []

    for rtc_record in rtc_records:
        granule_id = rtc_record.get("GranuleUR")
        if not granule_id:
            continue

        # Extract acquisition time
        acquisition_time = _extract_acquisition_time_from_umm(rtc_record.get("umm.TemporalExtent", {}))
        if not acquisition_time:
            continue

        # Extract full burst ID in database format (e.g., "T058-123813-IW3")
        full_burst_id = extract_full_burst_id_from_granule_id(granule_id)
        if not full_burst_id:
            continue

        granules_with_metadata.append(
            {
                "granule_id": granule_id,
                "acquisition_time": acquisition_time,
                "full_burst_id": full_burst_id,
            }
        )

    # Step 4: Cluster by acquisition time
    logger.info("Step 4: Clustering by acquisition time...")
    acquisition_clusters = cluster_acquisition_times(granules_with_metadata, time_tolerance_minutes=10)
    logger.info("Identified %d unique acquisition times", len(acquisition_clusters))

    # Step 5: For each acquisition time, map bursts to MGRS tiles using the database
    logger.info("Step 5: Mapping bursts to MGRS tiles...")
    tile_time_pairs = []

    # Debug: check some burst IDs
    sample_burst_ids = set()
    for acq_time, cluster_granules in list(acquisition_clusters.items())[:1]:
        sample_burst_ids = set(g["full_burst_id"] for g in cluster_granules[:5])
        logger.debug("Sample burst IDs from granules: %s", sample_burst_ids)
        logger.debug("Sample burst IDs from database: %s", _burst_db.sample_burst_ids() if _burst_db else [])

    for acq_time, cluster_granules in acquisition_clusters.items():
        # Get unique burst IDs for this acquisition time
        burst_ids_in_cluster = set(g["full_burst_id"] for g in cluster_granules)

        # Map burst IDs to DIST-S1 products and extract MGRS tile IDs
        mgrs_tiles_in_cluster = set()
        matched_bursts = 0
        for burst_id in burst_ids_in_cluster:
            product_ids = _burst_db.products_for_burst(burst_id) if _burst_db else set()
            if product_ids:
                matched_bursts += 1
                for product_id in product_ids:
                    # Extract MGRS tile ID from product_id (format: "TILE_ID_acq_group")
                    mgrs_tile_id = product_id.rsplit("_", 1)[0]
                    mgrs_tiles_in_cluster.add(mgrs_tile_id)

        if matched_bursts == 0:
            logger.warning(
                "No burst matches found for acquisition time %s (checked %d bursts)",
                acq_time.isoformat(),
                len(burst_ids_in_cluster),
            )

        # Create a (tile, time) pair for each unique MGRS tile at this acquisition time
        for mgrs_tile_id in mgrs_tiles_in_cluster:
            tile_time_pairs.append((mgrs_tile_id, acq_time))

    logger.info("Found %d unique (tile, acquisition_time) pairs to analyze", len(tile_time_pairs))

    # Step 6: Check each (tile, time) pair for sufficient inputs
    logger.info("Step 6: Checking input sufficiency for each (tile, time) pair...")
    logger.info("This will query lookback windows for each pair (may take a while)...")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def check_tile_time(tile_id: str, time: datetime):
        """Check if a tile at a given time has sufficient inputs."""
        async with semaphore:
            try:
                result = await query_and_select_baseline_products_for_dist_s1(
                    tile_id=tile_id,
                    t0=time,
                    window_configs=window_configs,
                    time_tolerance_minutes=10,
                )

                baseline_products = result["baseline_products"]
                diagnostics = result["diagnostics"]

                # Job is sufficient if we got baseline products (means complete bursts + historical data)
                is_sufficient = len(baseline_products) > 0

                # Generate a more descriptive reason if insufficient
                reason = ""
                if not is_sufficient:
                    active_bursts = diagnostics.get("active_bursts_found", 0)
                    with_t0 = diagnostics.get("baselines_with_t0_data", 0)
                    filtered = diagnostics.get("baselines_filtered_no_historical", 0)

                    if active_bursts == 0:
                        reason = "No dual-pol bursts found at acquisition time (t0)"
                    elif with_t0 == 0:
                        reason = f"Found {active_bursts} bursts at t0, but none had complete dual-pol data"
                    elif filtered > 0 and with_t0 == filtered:
                        reason = f"Found {with_t0} bursts at t0, but all lacked historical lookback data (w1+w2+w3=0)"
                    else:
                        reason = f"Incomplete burst coverage: {active_bursts} bursts found, {with_t0} with t0 data, {filtered} filtered for no historical data"

                return {
                    "tile_id": tile_id,
                    "acquisition_time": time,
                    "is_sufficient": is_sufficient,
                    "baseline_count": len(baseline_products),
                    "baseline_products": baseline_products,
                    "diagnostics": diagnostics,
                    "reason": reason,
                }
            except Exception as e:
                logger.warning(f"Error checking {tile_id} at {time.isoformat()}: {e}")
                return {
                    "tile_id": tile_id,
                    "acquisition_time": time,
                    "is_sufficient": False,
                    "baseline_count": 0,
                    "baseline_products": {},
                    "diagnostics": {},
                    "reason": f"Error: {str(e)}",
                }

    # Process all pairs concurrently
    tasks = [check_tile_time(tile, time) for tile, time in tile_time_pairs]
    results = await asyncio.gather(*tasks)

    # Step 7: Aggregate results
    logger.info("Step 7: Aggregating results...")
    jobs_sufficient = [r for r in results if r["is_sufficient"]]
    jobs_insufficient = [r for r in results if not r["is_sufficient"]]

    # Group by tile
    jobs_by_tile = defaultdict(lambda: {"sufficient": 0, "insufficient": 0})
    for result in results:
        tile = result["tile_id"]
        if result["is_sufficient"]:
            jobs_by_tile[tile]["sufficient"] += 1
        else:
            jobs_by_tile[tile]["insufficient"] += 1

    # Group by date
    jobs_by_date = defaultdict(lambda: {"sufficient": 0, "insufficient": 0})
    for result in results:
        date_str = result["acquisition_time"].date().isoformat()
        if result["is_sufficient"]:
            jobs_by_date[date_str]["sufficient"] += 1
        else:
            jobs_by_date[date_str]["insufficient"] += 1

    unique_tiles = set(r["tile_id"] for r in results)

    logger.info("=" * 80)
    logger.info("Summary:")
    logger.info("  Unique tiles with RTC data: %d", len(unique_tiles))
    logger.info("  Total acquisition times analyzed: %d", len(tile_time_pairs))
    logger.info("  Jobs with sufficient inputs: %d", len(jobs_sufficient))
    logger.info("  Jobs with insufficient inputs: %d", len(jobs_insufficient))
    logger.info("=" * 80)

    return {
        "query": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "window_configs": window_configs,
        },
        "summary": {
            "total_tiles": len(unique_tiles),
            "total_acquisition_times": len(tile_time_pairs),
            "jobs_with_sufficient_inputs": len(jobs_sufficient),
            "jobs_with_insufficient_inputs": len(jobs_insufficient),
        },
        "jobs_by_tile": dict(jobs_by_tile),
        "jobs_by_date": dict(jobs_by_date),
        "details": results,
    }


async def _process_batch_queries(
    parsed_items: list[tuple[Optional[str], str, datetime]],
    window_configs: list[tuple[int, int, int]],
    max_concurrent: int,
) -> list[dict]:
    """Process multiple queries concurrently, return list of result dicts."""
    logger.info("Querying CMR with max %d concurrent requests...", max_concurrent)

    # Create semaphore for rate limiting
    semaphore = asyncio.Semaphore(max_concurrent)

    # Create task for each query
    async def process_with_metadata(native_id: Optional[str], tile_id: str, time: datetime, acq_group: Optional[str]):
        result = await process_single_query(
            tile_id=tile_id,
            time=time,
            window_configs=window_configs,
            semaphore=semaphore,
        )
        if result:
            if native_id:
                result["native_id"] = native_id
            if acq_group:
                result["acq_group"] = acq_group
        return result

    # Process all items concurrently
    tasks = [
        process_with_metadata(native_id, tile_id, time, acq_group)
        for native_id, tile_id, time, acq_group in parsed_items
    ]
    results = await asyncio.gather(*tasks)

    # Filter out None results
    results = [r for r in results if r is not None]

    if not results:
        logger.error("No valid missing products obtained")
        sys.exit(1)

    logger.info("Successfully determined %d/%d missing products", len(results), len(parsed_items))
    return results


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

  # Custom max files per window (w1=4, w2=3, w3=3)
  %(prog)s T102 2025-09-25T12:00:00Z --max-files 4,3,3

  # JSON output
  %(prog)s T168 2025-09-25T12:00:00Z --output json

  # Granule IDs only
  %(prog)s T168 2025-09-25T12:00:00Z --output ids

  # Temporal window analysis: forecast jobs for 1-week period
  %(prog)s --temporal-window --start-date 2025-09-01T00:00:00Z --end-date 2025-09-08T00:00:00Z

  # Temporal window with JSON output (saved to file with full details)
  %(prog)s --temporal-window --start-date 2025-09-01T00:00:00Z --end-date 2025-09-08T00:00:00Z --output json --output-file results.json
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
        "--temporal-window",
        action="store_true",
        help="Enable temporal window analysis mode to forecast jobs between start and end dates",
    )

    parser.add_argument(
        "--start-date",
        type=parse_datetime,
        metavar="DATETIME",
        help="Start date for temporal window analysis (ISO format, e.g., 2025-09-01T00:00:00Z)",
    )

    parser.add_argument(
        "--end-date",
        type=parse_datetime,
        metavar="DATETIME",
        help="End date for temporal window analysis (ISO format, e.g., 2025-09-08T00:00:00Z)",
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
        default=(4, 3, 3),
        metavar="W1,W2,W3",
        help="Max files per window as comma-separated list (default: 4,3,3)",
    )

    parser.add_argument(
        "--output",
        choices=["text", "json", "ids"],
        default="text",
        help="Output format: text (human-readable), json (structured), ids (granule IDs only)",
    )

    parser.add_argument(
        "--output-file",
        type=str,
        metavar="PATH",
        help="Output file path for JSON format (default: auto-generated based on mode and timestamp)",
    )

    parser.add_argument(
        "--log-file",
        type=str,
        metavar="PATH",
        help="Save logs to file (in addition to console output). If not specified, logs are only shown on console.",
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=3,
        metavar="N",
        help="Maximum number of concurrent queries in batch mode (default: 3). Note: each query makes 3 CMR requests (one per window).",
    )

    args = parser.parse_args()

    # Set up file logging if requested
    if args.log_file:
        file_handler = logging.FileHandler(args.log_file, mode="w")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logging.getLogger().addHandler(file_handler)
        logger.info(f"Logging to file: {args.log_file}")

    # Validate input mode
    if args.temporal_window:
        # Temporal window mode validation
        if not args.start_date or not args.end_date:
            parser.error("--temporal-window requires both --start-date and --end-date")
        if args.start_date >= args.end_date:
            parser.error("--start-date must be before --end-date")
        if args.input_file or args.native_id or args.tile_id or args.time:
            parser.error("--temporal-window is incompatible with --input-file, --native-id, or tile_id/time arguments")
    else:
        # Single query or batch mode validation
        input_modes = sum([bool(args.input_file), bool(args.native_id), bool(args.tile_id and args.time)])
        if input_modes == 0:
            parser.error(
                "Must provide either --temporal-window, --input-file, --native-id, or both tile_id and time arguments"
            )
        if input_modes > 1:
            parser.error("Cannot combine --input-file with --native-id or tile_id/time arguments")

    # Build window configurations
    window_configs = [
        (1, args.window_size, args.max_files[0]),  # w1: 1 year back
        (2, args.window_size, args.max_files[1]),  # w2: 2 years back
        (3, args.window_size, args.max_files[2]),  # w3: 3 years back
    ]

    # Handle temporal window mode
    if args.temporal_window:
        results = await query_temporal_window_jobs(
            start_date=args.start_date,
            end_date=args.end_date,
            window_configs=window_configs,
            max_concurrent=args.max_concurrent,
        )

        output = format_temporal_window_output(results, args)
        print(output)
        return

    # Parse inputs from command-line arguments
    parsed_items = _parse_inputs_from_args(args)

    # Log query parameters
    if len(parsed_items) == 1 and not args.input_file:
        # Single query mode - log details
        native_id, tile_id, time, acq_group = parsed_items[0]
        logger.info("=" * 80)
        logger.info("DIST-S1 Lookback Window Query")
        logger.info("=" * 80)
        if native_id:
            logger.info("Native ID: %s", native_id)
        logger.info("Tile ID: %s", tile_id)
        if acq_group:
            logger.info("Acquisition Group: %s", acq_group)
        logger.info("Reference time (t0): %s", time.isoformat())
        logger.info("Window size: -%d days", args.window_size)
        logger.info(
            "Max files per window: w1=%d, w2=%d, w3=%d", args.max_files[0], args.max_files[1], args.max_files[2]
        )
        logger.info("Bounding box: Will auto-derive from tile ID")
        logger.info("=" * 80)
    else:
        # Batch mode
        logger.info("=" * 80)
        logger.info("DIST-S1 Lookback Window Query (Batch Mode)")
        logger.info("=" * 80)
        logger.info("Total queries: %d", len(parsed_items))
        logger.info("Window size: ±%d days", args.window_size)
        logger.info(
            "Max files per window: w1=%d, w2=%d, w3=%d", args.max_files[0], args.max_files[1], args.max_files[2]
        )
        logger.info("=" * 80)

    # Process queries (single or batch)
    if len(parsed_items) == 1:
        # Single query
        native_id, tile_id, time, acq_group = parsed_items[0]
        result = await process_single_query(
            tile_id=tile_id,
            time=time,
            window_configs=window_configs,
        )

        if not result:
            logger.error("No granules found for tile %s in any lookback window", tile_id)
            sys.exit(1)

        results = [result]
    else:
        # Batch processing
        results = await _process_batch_queries(
            parsed_items=parsed_items,
            window_configs=window_configs,
            max_concurrent=args.max_concurrent,
        )

        # Save list of known missing products
        if results:
            if args.input_file:
                filename = args.input_file.replace("potential", "validated")
            else:
                filename = f"DIST_S1_validated_missing_products_{datetime.now().strftime('%Y%m%dT%H%M%SZ')}.txt"
            filepath = os.path.join(".", filename)

            with open(filepath, "w") as f:
                for result in results:
                    tile_id = result["tile_id"]
                    # Add acquisition group if present
                    if "acq_group" in result and result["acq_group"]:
                        tile_id = f"{tile_id}_{result['acq_group']}"
                    f.write(f"{tile_id},{result['reference_time'].strftime('%Y%m%dT%H%M%SZ')}\n")

            logger.info(f"Wrote {len(results)} missing products to {filepath}")

    # Format and output results
    if args.output == "json":
        output = format_json_output(results, args)
        json_str = json.dumps(output, indent=2)

        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(json_str)
            # Print summary instead of full JSON
            tile_id = results[0]["tile_id"] if len(results) == 1 else f"{len(results)} queries"
            print(f"\nSaved detailed results to: {args.output_file}")
            print(f"Query: {tile_id}")
        else:
            print(json_str)
    elif args.output == "ids":
        formatted_ids = format_ids_output(results)
        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(formatted_ids)
        else:
            print(formatted_ids)
    else:
        print(format_text_output(results, args))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)
