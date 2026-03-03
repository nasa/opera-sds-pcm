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
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dateutil.parser import isoparse

from data_subscriber.cmr import DateTimeRange, async_query_cmr_v2
from data_subscriber.dist_s1_utils import localize_dist_burst_db

# RTC cache support (optional - only available when deployed)
try:
    from opera_commons.es_connection import get_grq_es

    RTC_CACHE_AVAILABLE = True
except ImportError:
    RTC_CACHE_AVAILABLE = False
    get_grq_es = None

CMR_RTC_CACHE_INDEX = "cmr_rtc_cache"

DIST_S1_NATIVE_ID_REGEX = (
    r"OPERA_L3_DIST(?:-ALERT)?-S1_"
    r"(?P<tile_id>T?\w+)_"
    r"(?P<acq_time>\d{8}T\d{6}Z)_"
    r"(?P<prod_time>\d{8}T\d{6}Z)_"
    r"S1_30_v[\d.]+.*"
)

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)


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


# Type alias for granule lists
GranuleList = list[RtcGranule]


def get_rtc_cache_connection():
    """
    Get RTC cache ElasticSearch connection if available.

    Returns None if cache is not available (e.g., running locally).
    The cache is only available when deployed in OPERA SDS environment.
    """
    if not RTC_CACHE_AVAILABLE:
        logger.debug("RTC cache not available: opera_commons.es_connection module not found")
        return None

    try:
        grq_es = get_grq_es(logger)
        # Test connection with a minimal query
        grq_es.search(index=CMR_RTC_CACHE_INDEX, body={"size": 0})
        logger.info("Successfully connected to RTC cache")
        return grq_es
    except Exception as e:
        logger.info(f"RTC cache not available (likely running locally): {e}")
        return None


def query_rtc_cache_by_bursts_and_time(
    grq_es,
    burst_ids: list[str],
    start_time: datetime,
    end_time: datetime,
) -> list[dict]:
    """
    Query RTC cache for granules matching burst IDs within a time range.

    Args:
        grq_es: ElasticSearch connection
        burst_ids: List of burst IDs to query (e.g., ["T168-359429-IW2"])
        start_time: Start of time range
        end_time: End of time range

    Returns:
        List of granule metadata dictionaries from cache
    """
    if not burst_ids:
        return []

    # Build query for burst IDs and time range
    should_clauses = [{"term": {"burst_id.keyword": bid}} for bid in burst_ids]

    query = {
        "query": {
            "bool": {
                "must": [
                    {"bool": {"should": should_clauses, "minimum_should_match": 1}},
                    {
                        "range": {
                            "acquisition_timestamp": {
                                "gte": start_time.isoformat(),
                                "lte": end_time.isoformat(),
                            }
                        }
                    },
                ]
            }
        },
        "size": 10000,  # Large enough for typical queries
    }

    result = grq_es.search(index=CMR_RTC_CACHE_INDEX, body=query)
    hits = result["hits"]["hits"]

    logger.debug(f"RTC cache query returned {len(hits)} granules for {len(burst_ids)} burst(s)")

    return [hit["_source"] for hit in hits]


def cache_results_to_rtc_granules(cache_results: list[dict]) -> GranuleList:
    """
    Convert RTC cache results to RtcGranule objects.

    Args:
        cache_results: List of cache document sources

    Returns:
        List of RtcGranule objects
    """
    granules = []
    for result in cache_results:
        granule_id = result.get("granule_id")
        if not granule_id:
            continue

        # Parse acquisition time
        acq_time_str = result.get("acquisition_timestamp")
        if isinstance(acq_time_str, str):
            acq_time = isoparse(acq_time_str)
            # Remove timezone info to match CMR behavior
            acq_time = acq_time.replace(tzinfo=None)
        elif isinstance(acq_time_str, datetime):
            acq_time = acq_time_str.replace(tzinfo=None)
        else:
            continue

        # Extract polarization from granule_id if not in cache
        # RTC granules are typically dual-pol (VV+VH or HH+HV)
        polarization = None
        if "VV" in granule_id or "VH" in granule_id:
            polarization = ["VV", "VH"]
        elif "HH" in granule_id or "HV" in granule_id:
            polarization = ["HH", "HV"]

        granules.append(RtcGranule(granule_id, acq_time, polarization))

    return granules


def _convert_cache_granules_to_cmr_format(granules: GranuleList) -> list[dict]:
    """
    Convert RtcGranule objects to CMR-like format for consistent processing.

    Args:
        granules: List of RtcGranule objects

    Returns:
        List of dictionaries in CMR UMM-JSON format
    """
    cmr_format = []
    for granule in granules:
        # Create a minimal UMM-JSON structure that matches what CMR returns
        umm_doc = {
            "umm": {
                "GranuleUR": granule.granule_id,
                "TemporalExtent": {
                    "RangeDateTime": {"BeginningDateTime": granule.acquisition_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
                },
                "AdditionalAttributes": [],
            }
        }

        # Add polarization if available
        if granule.polarization:
            umm_doc["umm"]["AdditionalAttributes"].append({"Name": "POLARIZATION", "Values": granule.polarization})

        cmr_format.append(umm_doc)

    return cmr_format


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


def get_bursts_by_track_from_db(tile_id: str) -> dict:
    """Get burst IDs organized by track (product) for a tile. Returns None if not found."""
    try:
        dist_products, _, product_to_bursts, _ = localize_dist_burst_db()
    except Exception as e:
        logger.warning("Could not load MGRS burst lookup table: %s", e)
        return None

    # Normalize tile ID
    normalized_tile = tile_id if not tile_id.startswith("T") else tile_id[1:]
    tile_variants = [normalized_tile, f"T{normalized_tile}"]

    for tile_variant in tile_variants:
        if tile_variant in dist_products:
            # Get all product IDs (tracks) for this tile
            product_ids = dist_products[tile_variant]

            # Build dict mapping product to its bursts
            bursts_by_track = {}
            for product_id in product_ids:
                if product_id in product_to_bursts:
                    bursts = product_to_bursts[product_id]
                    bursts_by_track[product_id] = set(bursts)

            logger.info("Found %d tracks for tile %s from lookup table", len(bursts_by_track), tile_variant)
            return bursts_by_track

    logger.warning("Tile %s not found in lookup table", tile_id)
    return None


def get_bursts_for_tile_from_db(tile_id: str) -> set:
    """Get all burst IDs that overlap a given MGRS tile from the lookup table."""
    bursts_by_track = get_bursts_by_track_from_db(tile_id)
    if bursts_by_track is None:
        logger.warning("Falling back to bbox-based search")
        return None
    # Flatten all bursts from all tracks into a single set
    all_bursts = set()
    for bursts in bursts_by_track.values():
        all_bursts.update(bursts)
    logger.info("Found %d bursts for tile %s from lookup table", len(all_bursts), tile_id)
    return all_bursts


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
    bbox: Optional[str] = None,
    auto_bbox: bool = True,
    grq_es=None,
) -> dict:
    """
    Complete DIST-S1 baseline product selection workflow.

    Returns dict mapping baseline_id to baseline product data with t0, w1, w2, w3 granules.
    Returns empty dict if no bursts found or incomplete burst coverage for the track.

    Args:
        grq_es: Optional ElasticSearch connection for RTC cache queries. If provided,
                will attempt to use cache before falling back to CMR queries.
    """
    # Step 1: Find active bursts at acquisition time
    logger.info("Step 1: Finding RTC bursts at acquisition time %s for tile %s", t0.isoformat(), tile_id)

    # Get expected bursts organized by track from lookup table
    bursts_by_track = get_bursts_by_track_from_db(tile_id)

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
        grq_es=grq_es,
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

    # Check if we have t0 data for all expected bursts for the specific track
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
                # Check if we have all bursts for this specific track
                missing_bursts = expected_bursts_for_track - active_full_burst_ids

                if missing_bursts:
                    logger.error(
                        "INCOMPLETE COVERAGE: Missing t0 data for %d/%d expected bursts in track %s (tile %s)",
                        len(missing_bursts),
                        len(expected_bursts_for_track),
                        product_id,
                        tile_id,
                    )
                    logger.error("Expected bursts for track %s: %d", product_id, len(expected_bursts_for_track))
                    logger.error("Found bursts at t0: %d", len(active_full_burst_ids))
                    logger.error("Missing bursts: %s", sorted(missing_bursts))
                    logger.error(
                        "Cannot proceed with input enumeration - DIST-S1 job should not be submitted without complete burst coverage for this track"
                    )
                    return {}

                logger.info(
                    "✓ Complete burst coverage for track %s: Found t0 data for all %d expected bursts",
                    product_id,
                    len(expected_bursts_for_track),
                )
            else:
                logger.warning(
                    "Could not identify track from active bursts - skipping completeness check. "
                    "This may indicate a new track or configuration issue."
                )
        else:
            logger.warning("Could not extract RTC tile prefix from granules, skipping completeness check")

    # Step 2 & 3: For each active burst, query lookback windows and select files
    async def process_burst(burst_id: str, subswath: str):
        """Process a single burst: query historical data and select files."""
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
            grq_es=grq_es,
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

        logger.info(
            "  Selected files for %s: t0=%d, w1=%d, w2=%d, w3=%d (total=%d)",
            baseline_id,
            len(t0_burst_granules),
            len(w1),
            len(w2),
            len(w3),
            len(t0_burst_granules) + len(w1) + len(w2) + len(w3),
        )

        return baseline_id, {
            "burst_id": burst_id,
            "subswath": subswath,
            "t0": t0_burst_granules,
            "w1": w1,
            "w2": w2,
            "w3": w3,
        }

    # Process all bursts concurrently
    tasks = [process_burst(burst_id, subswath) for burst_id, subswath in active_bursts]
    burst_results = await asyncio.gather(*tasks)

    # Collect results into baseline_products dict
    baseline_products = {}
    for baseline_id, product in burst_results:
        baseline_products[baseline_id] = product

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


def get_bbox_from_tile_id(tile_id: str, margin_km: float = 75.0) -> str:
    """Get bounding box string from MGRS tile ID in format 'west,south,east,north'."""
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
    grq_es=None,
) -> tuple[list[tuple[str, str]], dict[str, list]]:
    """
    Query for RTC bursts at acquisition time.

    Tries RTC cache first if available, falls back to CMR.
    Returns (active_bursts, t0_granules_by_burst).
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

    logger.info("Querying RTC bursts at acquisition time %s (±%d min)", t0.isoformat(), time_tolerance_minutes)
    logger.info("  Time range: %s to %s", time_start.isoformat(), time_end.isoformat())

    # Try RTC cache first if available
    cmr_results = []
    if grq_es and valid_bursts_from_db:
        try:
            logger.debug("Attempting RTC cache query for t0 granules")
            # Query cache for all bursts associated with this tile
            cache_results = query_rtc_cache_by_bursts_and_time(grq_es, list(valid_bursts_from_db), time_start, time_end)
            if cache_results:
                logger.info("  Found %d results from RTC cache at acquisition time", len(cache_results))
                # Convert cache results to granule format for processing
                cache_granules = cache_results_to_rtc_granules(cache_results)
                # Convert to CMR-like format for consistent processing below
                cmr_results = _convert_cache_granules_to_cmr_format(cache_granules)
        except Exception as e:
            logger.warning("RTC cache query failed: %s, falling back to CMR", e)
            cmr_results = []

    # Fall back to CMR if cache didn't return results
    if not cmr_results:
        timerange = DateTimeRange(
            start_date=time_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=time_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        logger.debug("Querying CMR for t0 granules")
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


async def query_rtc_granules_for_burst(
    tile_id: str,
    burst_id: str,
    subswath: str,
    t0: datetime,
    window_configs: list[tuple[int, int, int]],
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
    grq_es=None,
) -> GranuleList:
    """
    Query for RTC granules for a specific burst within lookback windows.

    Tries RTC cache first if available, falls back to CMR.
    """
    # Try to construct the full burst ID for cache queries
    # We need format like "T168-359429-IW2" instead of just "359429"
    valid_bursts_from_db = get_bursts_for_tile_from_db(tile_id)
    full_burst_id = None
    if valid_bursts_from_db:
        # Search for a burst that matches our burst_id and subswath
        for db_burst_id in valid_bursts_from_db:
            # db_burst_id format: "T168-359429-IW2"
            parts = db_burst_id.split("-")
            if len(parts) == 3 and parts[1] == burst_id and parts[2] == subswath:
                full_burst_id = db_burst_id
                break

    async def query_window(years_back: int, window_size_days: int):
        """Query a single window and return filtered granules."""
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        logger.debug(
            "  Querying window w%d for burst %s-%s: %s to %s",
            years_back,
            burst_id,
            subswath,
            lookback_window.window_start.isoformat(),
            lookback_window.window_end.isoformat(),
        )

        # Try RTC cache first if available and we have the full burst ID
        window_granules = []
        if grq_es and full_burst_id:
            try:
                logger.debug("    Attempting RTC cache query for burst %s in w%d", full_burst_id, years_back)
                cache_results = query_rtc_cache_by_bursts_and_time(
                    grq_es, [full_burst_id], lookback_window.window_start, lookback_window.window_end
                )
                if cache_results:
                    window_granules = cache_results_to_rtc_granules(cache_results)
                    logger.debug(
                        "    Found %d granules from RTC cache for burst %s-%s in w%d",
                        len(window_granules),
                        burst_id,
                        subswath,
                        years_back,
                    )
            except Exception as e:
                logger.warning("    RTC cache query failed for w%d: %s, falling back to CMR", years_back, e)

        # Fall back to CMR if cache didn't return results
        if not window_granules:
            timerange = DateTimeRange(
                start_date=lookback_window.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date=lookback_window.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )

            logger.debug("    Querying CMR for burst %s-%s in w%d", burst_id, subswath, years_back)
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

                window_granules.append(RtcGranule(granule_id, acquisition_time, polarization))
                matched_count += 1

            logger.debug(
                "    Found %d granules from CMR for burst %s-%s in w%d", matched_count, burst_id, subswath, years_back
            )

        return window_granules

    # Query all windows concurrently
    tasks = [query_window(years_back, window_size_days) for years_back, window_size_days, _ in window_configs]
    window_results = await asyncio.gather(*tasks)

    # Combine results from all windows
    all_granules = []
    for window_granules in window_results:
        all_granules.extend(window_granules)

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

    async def query_window(years_back: int, window_size_days: int):
        """Query a single window and return granules."""
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
        window_granules = []
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

            window_granules.append(RtcGranule(granule_id, acquisition_time, polarization))
            matched_count += 1

        logger.info("  Accepted %d RTC granules within bbox", matched_count)
        return window_granules

    # Query all windows concurrently
    tasks = [query_window(years_back, window_size_days) for years_back, window_size_days, _ in window_configs]
    window_results = await asyncio.gather(*tasks)

    # Combine results from all windows
    all_granules = []
    for window_granules in window_results:
        all_granules.extend(window_granules)

    logger.info("Total granules after filtering: %d", len(all_granules))
    return all_granules


def _extract_polarization_from_umm(umm: dict) -> Optional[list]:
    """Extract polarization from UMM-JSON metadata."""
    additional_attributes = umm.get("AdditionalAttributes", [])

    for attr in additional_attributes:
        if attr.get("Name") == "POLARIZATION":
            return attr.get("Values")  # e.g., ["VV", "VH"]

    return None


def _extract_acquisition_time_from_umm(umm: dict) -> Optional[datetime]:
    """Extract acquisition time from UMM-JSON metadata."""
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
    return None


def _parse_inputs_from_args(args) -> list[tuple[Optional[str], str, datetime]]:
    """Parse inputs from args, return list of (native_id, tile_id, time) tuples."""
    if args.native_id:
        tile_id, time = parse_dist_s1_native_id(args.native_id)
        if tile_id is None or time is None:
            logger.error("Failed to parse DIST-S1 native ID: %s", args.native_id)
            logger.error("Expected format: OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250925T212111Z_S1_30_v0.1")
            sys.exit(1)
        logger.info("Parsed native ID: tile_id=%s, time=%s", tile_id, time.isoformat())
        return [(args.native_id, tile_id, time)]

    # Single query with tile_id and time
    if args.tile_id and args.time:
        return [(None, args.tile_id, args.time)]

    # Batch mode from input file
    if args.input_file:
        logger.info("Reading native IDs from file: %s", args.input_file)
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

        logger.info("Processing %d native IDs from file...", len(native_ids))

        # Parse all native IDs
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
    bbox: Optional[str],
    auto_bbox: bool,
    semaphore: Optional[asyncio.Semaphore] = None,
    grq_es=None,
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
            return await _process_single_query_impl(tile_id, time, window_configs, bbox, auto_bbox, grq_es)
    else:
        return await _process_single_query_impl(tile_id, time, window_configs, bbox, auto_bbox, grq_es)


async def _process_single_query_impl(
    tile_id: str,
    time: datetime,
    window_configs: list[tuple[int, int, int]],
    bbox: Optional[str],
    auto_bbox: bool,
    grq_es=None,
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
        grq_es=grq_es,
    )

    if not baseline_products:
        logger.warning("No baseline products generated for tile %s at time %s", tile_id, time.isoformat())
        return None

    return {
        "tile_id": tile_id,
        "reference_time": time,
        "baseline_products": baseline_products,
    }


async def query_temporal_window_jobs(
    start_date: datetime,
    end_date: datetime,
    window_configs: list[tuple[int, int, int]],
    bbox: Optional[str] = None,
    sample_interval_days: Optional[int] = None,
    max_concurrent: int = 3,
    grq_es=None,
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
        bbox: Optional bounding box filter
        sample_interval_days: Optional sampling interval to check every N days
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

    cmr_results = await async_query_cmr_v2(
        timerange=timerange, provider="ASF", collection="OPERA_L2_RTC-S1_V1", token=None, bbox=bbox
    )

    logger.info("Found %d RTC granules in temporal window", len(cmr_results))

    if not cmr_results:
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
    try:
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()
    except Exception as e:
        logger.error("Failed to load burst database: %s", e)
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

    # Step 3: Parse granules and extract acquisition times and burst IDs
    logger.info("Step 3: Parsing granules and extracting metadata...")
    granules_with_metadata = []

    for result in cmr_results:
        umm = result.get("umm", {})
        granule_id = umm.get("GranuleUR")
        if not granule_id:
            continue

        # Extract acquisition time
        acquisition_time = _extract_acquisition_time_from_umm(umm)
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
        logger.info("Sample burst IDs from granules: %s", sample_burst_ids)
        logger.info("Sample burst IDs from database: %s", list(bursts_to_products.keys())[:5])

    for acq_time, cluster_granules in acquisition_clusters.items():
        # Get unique burst IDs for this acquisition time
        burst_ids_in_cluster = set(g["full_burst_id"] for g in cluster_granules)

        # Map burst IDs to DIST-S1 products and extract MGRS tile IDs
        mgrs_tiles_in_cluster = set()
        matched_bursts = 0
        for burst_id in burst_ids_in_cluster:
            if burst_id in bursts_to_products:
                matched_bursts += 1
                product_ids = bursts_to_products[burst_id]
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

    # Optional: Apply sampling
    if sample_interval_days:
        logger.info("Applying sampling: checking every %d days", sample_interval_days)
        sampled_pairs = []
        checked_dates = set()

        for tile, acq_time in sorted(tile_time_pairs, key=lambda x: x[1]):
            acq_date = acq_time.date()
            days_since_start = (acq_date - start_date.date()).days
            interval_index = days_since_start // sample_interval_days

            date_key = (tile, interval_index)
            if date_key not in checked_dates:
                checked_dates.add(date_key)
                sampled_pairs.append((tile, acq_time))

        logger.info("Sampled down to %d pairs", len(sampled_pairs))
        tile_time_pairs = sampled_pairs

    # Step 6: Check each (tile, time) pair for sufficient inputs
    logger.info("Step 6: Checking input sufficiency for each (tile, time) pair...")
    logger.info("This will query lookback windows for each pair (may take a while)...")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def check_tile_time(tile_id: str, time: datetime):
        """Check if a tile at a given time has sufficient inputs."""
        async with semaphore:
            try:
                baseline_products = await query_and_select_baseline_products_for_dist_s1(
                    tile_id=tile_id,
                    t0=time,
                    window_configs=window_configs,
                    time_tolerance_minutes=10,
                    bbox=bbox if bbox else None,
                    auto_bbox=True if not bbox else False,
                    grq_es=grq_es,
                )

                # Job is sufficient if we got baseline products (means complete bursts + historical data)
                is_sufficient = len(baseline_products) > 0

                return {
                    "tile_id": tile_id,
                    "acquisition_time": time,
                    "is_sufficient": is_sufficient,
                    "baseline_count": len(baseline_products),
                    "baseline_products": baseline_products,
                    "reason": "" if is_sufficient else "Incomplete burst coverage or missing lookback data",
                }
            except Exception as e:
                logger.warning(f"Error checking {tile_id} at {time.isoformat()}: {e}")
                return {
                    "tile_id": tile_id,
                    "acquisition_time": time,
                    "is_sufficient": False,
                    "baseline_count": 0,
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
            "sample_interval_days": sample_interval_days,
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
    bbox: Optional[str],
    auto_bbox: bool,
    max_concurrent: int,
    grq_es=None,
) -> list[dict]:
    """Process multiple queries concurrently, return list of result dicts."""
    logger.info("Querying CMR with max %d concurrent requests...", max_concurrent)

    # Create semaphore for rate limiting
    semaphore = asyncio.Semaphore(max_concurrent)

    # Create task for each query
    async def process_with_metadata(native_id: Optional[str], tile_id: str, time: datetime):
        result = await process_single_query(
            tile_id=tile_id,
            time=time,
            window_configs=window_configs,
            bbox=bbox,
            auto_bbox=auto_bbox,
            semaphore=semaphore,
            grq_es=grq_es,
        )
        if result and native_id:
            result["native_id"] = native_id
        return result

    # Process all items concurrently
    tasks = [process_with_metadata(native_id, tile_id, time) for native_id, tile_id, time in parsed_items]
    results = await asyncio.gather(*tasks)

    # Filter out None results
    results = [r for r in results if r is not None]

    if not results:
        logger.error("No valid results obtained")
        sys.exit(1)

    logger.info("Successfully processed %d/%d queries", len(results), len(parsed_items))
    return results


def _format_baseline_product_json(product: dict, time: datetime, window_size: int) -> dict:
    """Format a single baseline product for JSON output."""
    windows = {}
    for years_back, window_name in [(1, "w1"), (2, "w2"), (3, "w3")]:
        windows[window_name] = {
            "years_back": years_back,
            "window": calculate_lookback_window(time, years_back, window_size).to_dict(),
            "granules": [g.to_dict() for g in product[window_name]],
            "count": len(product[window_name]),
        }

    total_files = len(product["t0"]) + len(product["w1"]) + len(product["w2"]) + len(product["w3"])
    return {
        "burst_id": product["burst_id"],
        "subswath": product["subswath"],
        "t0": {
            "description": "RTC granules at acquisition time",
            "granules": [g.to_dict() for g in product["t0"]],
            "count": len(product["t0"]),
        },
        "windows": windows,
        "total_granules": total_files,
    }


def _format_json_output(results: list[dict], args) -> dict:
    """Format results as JSON output."""
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

        for baseline_id, product in baseline_products.items():
            formatted = _format_baseline_product_json(product, time, args.window_size)
            output["baseline_products"][baseline_id] = formatted
            output["summary"]["total_granules"] += formatted["total_granules"]
    else:
        # Batch mode
        output = {
            "query": {"window_size_days": args.window_size, "max_files": list(args.max_files), "bbox": args.bbox},
            "results": [],
        }
        total_baselines = total_granules = 0
        for result in results:
            baseline_products = result["baseline_products"]
            result_entry = {
                "native_id": result.get("native_id"),
                "tile_id": result["tile_id"],
                "reference_time": result["reference_time"].isoformat(),
                "baseline_products": {},
            }
            result_total_granules = 0
            for baseline_id, product in baseline_products.items():
                formatted = _format_baseline_product_json(product, result["reference_time"], args.window_size)
                result_entry["baseline_products"][baseline_id] = formatted
                result_total_granules += formatted["total_granules"]
            result_entry["total_granules"] = result_total_granules
            result_entry["total_baselines"] = len(baseline_products)
            output["results"].append(result_entry)
            total_baselines += len(baseline_products)
            total_granules += result_total_granules
        output["summary"] = {
            "total_queries": len(results),
            "total_baselines": total_baselines,
            "total_granules": total_granules,
        }
    return output


def _format_ids_output(results: list[dict]) -> str:
    """Format results as granule IDs only (one per line)."""
    ids = []
    for result in results:
        for product in result["baseline_products"].values():
            for window_name in ["t0", "w1", "w2", "w3"]:
                ids.extend(g.granule_id for g in product[window_name])
    return "\n".join(ids)


def _format_temporal_window_json(results: dict, args) -> dict:
    """Format temporal window results for JSON serialization."""
    formatted_details = []
    for detail in results["details"]:
        formatted_detail = {
            "tile_id": detail["tile_id"],
            "acquisition_time": detail["acquisition_time"].isoformat(),
            "is_sufficient": detail["is_sufficient"],
            "baseline_count": detail["baseline_count"],
            "reason": detail.get("reason", ""),
        }

        # Format baseline_products if present
        if "baseline_products" in detail and detail["baseline_products"]:
            formatted_baselines = {}
            for baseline_id, product in detail["baseline_products"].items():
                formatted_baselines[baseline_id] = _format_baseline_product_json(
                    product, detail["acquisition_time"], args.window_size
                )
            formatted_detail["baseline_products"] = formatted_baselines

        formatted_details.append(formatted_detail)

    return {
        "query": results["query"],
        "summary": results["summary"],
        "jobs_by_tile": results["jobs_by_tile"],
        "jobs_by_date": results["jobs_by_date"],
        "details": formatted_details,
    }


def _format_temporal_window_output(results: dict, args) -> str:
    """Format temporal window analysis results."""
    if args.output == "json":
        # Determine output filename
        if args.output_file:
            output_file = args.output_file
        else:
            # Auto-generate filename based on query parameters
            start = results["query"]["start_date"].replace(":", "").replace("-", "")[:8]
            end = results["query"]["end_date"].replace(":", "").replace("-", "")[:8]
            output_file = f"temporal_window_analysis_{start}_{end}.json"

        # Format results for JSON serialization
        formatted_results = _format_temporal_window_json(results, args)

        # Save full results to file
        with open(output_file, "w") as f:
            json.dump(formatted_results, f, indent=2)

        # Return summary message
        summary = results["summary"]
        return (
            f"\nSaved detailed results to: {output_file}\n"
            f"Summary: {summary['jobs_with_sufficient_inputs']}/{summary['total_acquisition_times']} "
            f"jobs have sufficient inputs across {summary['total_tiles']} tiles\n"
        )

    # Text output format
    lines = []
    query = results["query"]
    summary = results["summary"]
    jobs_by_tile = results["jobs_by_tile"]
    jobs_by_date = results["jobs_by_date"]

    lines.extend(
        [
            "",
            "=" * 80,
            "DIST-S1 Temporal Window Job Forecast",
            "=" * 80,
            f"Query Period: {query['start_date']} to {query['end_date']}",
        ]
    )

    # Add window config info
    wc = query["window_configs"]
    lines.append(f"Window Configuration: w1={wc[0][2]}, w2={wc[1][2]}, w3={wc[2][2]} ({wc[0][1]}-day windows)")

    if query.get("sample_interval_days"):
        lines.append(f"Sampling Interval: Every {query['sample_interval_days']} days")

    lines.extend(
        [
            "",
            "Summary:",
            f"  Unique tiles with RTC data: {summary['total_tiles']}",
            f"  Total acquisition times analyzed: {summary['total_acquisition_times']}",
            f"  Jobs with sufficient inputs: {summary['jobs_with_sufficient_inputs']}",
            f"  Jobs with insufficient inputs: {summary['jobs_with_insufficient_inputs']}",
        ]
    )

    # Breakdown by tile (top 10 or all if fewer)
    if jobs_by_tile:
        lines.extend(
            [
                "",
                "Breakdown by Tile (top 15):",
            ]
        )

        # Sort by total jobs (sufficient + insufficient)
        sorted_tiles = sorted(
            jobs_by_tile.items(), key=lambda x: x[1]["sufficient"] + x[1]["insufficient"], reverse=True
        )

        for tile, counts in sorted_tiles[:15]:
            total = counts["sufficient"] + counts["insufficient"]
            lines.append(
                f"  {tile:10s}: {total:3d} jobs ({counts['sufficient']:3d} sufficient, {counts['insufficient']:3d} insufficient)"
            )

    # Breakdown by date (show all dates with data)
    if jobs_by_date:
        lines.extend(
            [
                "",
                "Breakdown by Date:",
            ]
        )

        for date_str in sorted(jobs_by_date.keys()):
            counts = jobs_by_date[date_str]
            total = counts["sufficient"] + counts["insufficient"]
            lines.append(
                f"  {date_str}: {total:3d} jobs ({counts['sufficient']:3d} sufficient, {counts['insufficient']:3d} insufficient)"
            )

    # Show insufficient jobs if there are any (and not too many)
    insufficient_jobs = [d for d in results["details"] if not d["is_sufficient"]]
    if insufficient_jobs and len(insufficient_jobs) <= 20:
        lines.extend(
            [
                "",
                "Insufficient Jobs (missing data):",
            ]
        )
        for job in insufficient_jobs:
            lines.append(f"  {job['tile_id']:10s} @ {job['acquisition_time'].isoformat()}: {job['reason']}")
    elif len(insufficient_jobs) > 20:
        lines.extend(
            [
                "",
                f"Insufficient Jobs: {len(insufficient_jobs)} jobs have insufficient data",
                "  (Use --output json --full-output for complete list)",
            ]
        )

    lines.extend(["=" * 80, ""])

    return "\n".join(lines)


def _format_text_output(results: list[dict], args) -> str:
    """Format results as human-readable text output."""
    lines = []
    if len(results) == 1:
        # Single query mode
        result = results[0]
        tile_id, time, baseline_products = result["tile_id"], result["reference_time"], result["baseline_products"]
        lines.extend(
            [
                "\n" + "=" * 80,
                "DIST-S1 Baseline Product Selection Results",
                "=" * 80 + "\n",
                f"Tile: {tile_id}",
                f"Acquisition time: {time.isoformat()}",
                f"Found {len(baseline_products)} baseline products (unique burst+subswath combinations)\n",
            ]
        )

        total_files = 0
        for baseline_id, product in sorted(baseline_products.items()):
            t0, w1, w2, w3 = product["t0"], product["w1"], product["w2"], product["w3"]
            baseline_total = len(t0) + len(w1) + len(w2) + len(w3)
            total_files += baseline_total
            lines.extend(
                [
                    "-" * 80,
                    f"Baseline: {baseline_id} (burst={product['burst_id']}, subswath={product['subswath']})",
                    f"Total files: {baseline_total} (t0={len(t0)}, w1={len(w1)}, w2={len(w2)}, w3={len(w3)})",
                    "",
                    "  Acquisition Time (t0):",
                    f"    Files found: {len(t0)}",
                ]
            )
            lines.extend(f"      {g.acquisition_time.isoformat()}: {g.granule_id}" for g in t0) if t0 else lines.append(
                "      (No granules found)"
            )
            lines.append("")

            for window_name, granules, years_back in [("Window 1", w1, 1), ("Window 2", w2, 2), ("Window 3", w3, 3)]:
                window = calculate_lookback_window(time, years_back, args.window_size)
                lines.extend(
                    [
                        f"  {window_name} (t0 - {years_back} year{'s' if years_back > 1 else ''}):",
                        f"    Target date: {window.window_end.isoformat()}",
                        f"    Range: {window.window_start.isoformat()} to {window.window_end.isoformat()}",
                        f"    Files found: {len(granules)}/{args.max_files[years_back - 1]}",
                    ]
                )
                if granules:
                    for g in granules:
                        days = (g.acquisition_time - window.window_end).days
                        lines.append(
                            f"      {g.acquisition_time.isoformat()} ({'+' if days >= 0 else ''}{days}d): {g.granule_id}"
                        )
                else:
                    lines.append("      (No granules found)")
                lines.append("")

        lines.extend(
            [
                "=" * 80,
                f"Total baselines: {len(baseline_products)}",
                f"Total files selected: {total_files}",
                "=" * 80 + "\n",
            ]
        )

    else:
        # Batch mode
        lines.extend(["\n" + "=" * 80, "DIST-S1 Baseline Product Selection Results (Batch)", "=" * 80 + "\n"])
        grand_total_files = grand_total_baselines = 0
        for i, result in enumerate(results, 1):
            baseline_products = result["baseline_products"]
            result_total_files = sum(
                len(p["t0"]) + len(p["w1"]) + len(p["w2"]) + len(p["w3"]) for p in baseline_products.values()
            )
            lines.extend(
                [
                    f"[{i}/{len(results)}] {result.get('native_id')}",
                    f"  Tile: {result['tile_id']}, Time: {result['reference_time'].isoformat()}",
                    f"  Baselines found: {len(baseline_products)}",
                    f"  Total files for this query: {result_total_files}",
                    "",
                ]
            )
            grand_total_files += result_total_files
            grand_total_baselines += len(baseline_products)
        lines.extend(
            [
                "=" * 80,
                f"Processed {len(results)} queries",
                f"Total baselines: {grand_total_baselines}",
                f"Total files selected: {grand_total_files}",
                "=" * 80 + "\n",
            ]
        )
    return "\n".join(lines)


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

  # Sample every 3 days for faster analysis of long periods
  %(prog)s --temporal-window --start-date 2025-08-01T00:00:00Z --end-date 2025-09-30T00:00:00Z --sample-interval 3
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
        "--sample-interval",
        type=int,
        metavar="DAYS",
        help="Optional: Sample acquisitions every N days in temporal window mode (for faster analysis of long time periods)",
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
        "--output-file",
        type=str,
        metavar="PATH",
        help="Output file path for JSON format (default: auto-generated based on mode and timestamp)",
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

    # Try to establish RTC cache connection (only available when deployed)
    grq_es = get_rtc_cache_connection()
    if grq_es:
        logger.info("RTC cache connection established - queries will use cache with CMR fallback")
    else:
        logger.info("RTC cache not available - queries will use CMR directly")

    # Handle temporal window mode
    if args.temporal_window:
        results = await query_temporal_window_jobs(
            start_date=args.start_date,
            end_date=args.end_date,
            window_configs=window_configs,
            bbox=args.bbox,
            sample_interval_days=args.sample_interval,
            max_concurrent=args.max_concurrent,
            grq_es=grq_es,
        )

        output = _format_temporal_window_output(results, args)
        print(output)
        return

    # Parse inputs from command-line arguments
    parsed_items = _parse_inputs_from_args(args)

    # Log query parameters
    if len(parsed_items) == 1 and not args.input_file:
        # Single query mode - log details
        native_id, tile_id, time = parsed_items[0]
        logger.info("=" * 80)
        logger.info("DIST-S1 Lookback Window Query")
        logger.info("=" * 80)
        if native_id:
            logger.info("Native ID: %s", native_id)
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
        native_id, tile_id, time = parsed_items[0]
        result = await process_single_query(
            tile_id=tile_id,
            time=time,
            window_configs=window_configs,
            bbox=args.bbox,
            auto_bbox=not args.no_auto_bbox,
            grq_es=grq_es,
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
            bbox=args.bbox,
            auto_bbox=not args.no_auto_bbox,
            max_concurrent=args.max_concurrent,
            grq_es=grq_es,
        )

    # Format and output results
    if args.output == "json":
        output = _format_json_output(results, args)
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
        print(_format_ids_output(results))
    else:
        print(_format_text_output(results, args))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error("ERROR: %s", e)
        sys.exit(1)
