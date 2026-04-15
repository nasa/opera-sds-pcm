#!/usr/bin/env python3
from __future__ import annotations

"""
CMR Audit: Burst-Level Coverage Checker for OPERA CSLC-S1 and RTC-S1

This tool audits OPERA product coverage at the Sentinel-1 burst level:

    1. Query CMR for Sentinel-1 SLC granules covering a GeoJSON region
    2. Filter SLCs by exact polygon intersection (not just bounding box)
    3. Query ASF SLC-BURST API to get burst IDs for each SLC
    4. Deduplicate bursts (same burst can appear in overlapping SLCs)
    5. Query CMR for OPERA products matching those burst IDs
    6. Report coverage statistics and missing products

Key Features:
    - File-based request caching (configurable TTL)
    - Low-memory streaming mode for long date ranges (JSONL output)
    - Polygon intersection filtering for accurate spatial matching
    - Burst deduplication across overlapping SLC acquisitions
    - Configurable buffer for capturing SLCs at boundary edges

Example Usage:
    # Standard mode (results in memory)
    python cmr_audit_burst_coverage.py \\
        --start-datetime 2024-01-01T00:00:00Z \\
        --end-datetime 2024-01-07T23:59:59Z \\
        --geojson ./north_america.geojson \\
        --output coverage.json

    # Low-memory mode for long date ranges (streaming to JSONL)
    python cmr_audit_burst_coverage.py \\
        --start-datetime 2016-01-01T00:00:00Z \\
        --end-datetime 2025-12-31T23:59:59Z \\
        --geojson ./north_america.geojson \\
        --low-memory --chunk-days 30 \\
        --output coverage.jsonl
"""

import argparse
import asyncio
import gc
import hashlib
import json
import logging
import logging.handlers
import re
import sys
import time
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Iterator

import aiohttp
from dateutil.parser import isoparse

from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules, init_logging
from tools.ops.cmr_audit.slc_annotation_extract import (
    get_slc_download_url,
    get_edl_token,
    extract_annotations,
    parse_burst_anx_times,
    derive_burst_ids,
)

# Suppress noisy loggers
logging.getLogger("compact_json.formatter").setLevel(logging.INFO)

# =============================================================================
# Constants
# =============================================================================

CMR_GRANULE_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
ASF_BURST_API_URL = "https://api.daac.asf.alaska.edu/services/search/param"

# OPERA product short names in CMR
OPERA_PRODUCTS = {
    "CSLC-S1": "OPERA_L2_CSLC-S1_V1",
    "RTC-S1": "OPERA_L2_RTC-S1_V1",
}

# Sentinel-1 platform name mapping (ASF API format)
PLATFORM_MAP = {
    "S1A": "SENTINEL-1A",
    "S1B": "SENTINEL-1B",
    "S1C": "SENTINEL-1C",
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True)
class BurstInfo:
    """
    Sentinel-1 burst identifier.

    Bursts are the fundamental unit of Sentinel-1 IW mode acquisitions.
    Each burst is uniquely identified by track number, burst number, and subswath.
    """
    track: int       # Relative orbit number (1-175)
    burst_num: int   # ESA burst ID within the track
    subswath: str    # IW1, IW2, or IW3

    @classmethod
    def from_asf_id(cls, asf_id: str) -> "BurstInfo":
        """Parse ASF fullBurstID format: '035_073254_IW1'"""
        track, burst_num, subswath = asf_id.split("_")
        return cls(int(track), int(burst_num), subswath.upper())

    @property
    def asf_id(self) -> str:
        """ASF format: 035_073254_IW1"""
        return f"{self.track:03d}_{self.burst_num:06d}_{self.subswath}"

    @property
    def opera_id(self) -> str:
        """OPERA format: T035_073254_IW1"""
        return f"T{self.track:03d}_{self.burst_num:06d}_{self.subswath}"

    @property
    def filename_pattern(self) -> str:
        """Pattern used in OPERA filenames: T035-073254-IW1"""
        return f"T{self.track:03d}-{self.burst_num:06d}-{self.subswath}"


@dataclass
class SLCGranule:
    """Sentinel-1 SLC granule metadata parsed from CMR native-id."""
    native_id: str
    platform: str          # S1A, S1B, S1C
    start_time: datetime
    end_time: datetime
    absolute_orbit: int
    bursts: list[BurstInfo] = field(default_factory=list)

    # Regex pattern for parsing SLC native-id
    _PATTERN = re.compile(
        r'(S1[ABC])_IW_SLC__\w+_'
        r'(\d{8}T\d{6})_(\d{8}T\d{6})_'
        r'(\d{6})_[\dA-F]+_[\dA-F]+-SLC'
    )

    @classmethod
    def from_native_id(cls, native_id: str) -> Optional["SLCGranule"]:
        """Parse SLC granule from CMR native-id, returns None if unparseable."""
        match = cls._PATTERN.match(native_id)
        if not match:
            return None
        platform, start_str, end_str, abs_orbit = match.groups()
        return cls(
            native_id=native_id,
            platform=platform,
            start_time=datetime.strptime(start_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc),
            end_time=datetime.strptime(end_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc),
            absolute_orbit=int(abs_orbit),
        )


@dataclass
class ExpectedBurst:
    """An expected OPERA product for a specific burst acquisition."""
    burst: BurstInfo
    acquisition_time: datetime
    platform: str
    polarization: str
    slc_native_id: str  # Source SLC for traceability
    slc_end_time: datetime | None = None  # End of SLC acquisition window

    def to_dict(self) -> dict:
        return {
            "burst_id": self.burst.opera_id,
            "burst_pattern": self.burst.filename_pattern,
            "acquisition_time": self.acquisition_time.isoformat(),
            "platform": self.platform,
            "polarization": self.polarization,
            "slc_native_id": self.slc_native_id,
        }


# =============================================================================
# Request Caching
# =============================================================================

class RequestCache:
    """
    File-based cache for API responses to speed up repeated runs.

    Stores responses as JSON files permanently (no TTL expiration).
    Uses subdirectories based on hash prefix for filesystem performance.
    Use --clear-cache to manually invalidate all cached data.
    """
    DEFAULT_DIR = Path.home() / ".cache" / "cmr_audit_burst"

    def __init__(self, cache_dir: Path = None, enabled: bool = True,
                 recheck_dates: set[str] = None):
        self.cache_dir = Path(cache_dir) if cache_dir else self.DEFAULT_DIR
        self.enabled = enabled
        self.recheck_dates = recheck_dates or set()
        self.hits = 0
        self.misses = 0

        if self.enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, namespace: str, params: dict) -> Path:
        """Generate cache file path from namespace and parameters."""
        param_str = json.dumps(params, sort_keys=True)
        hash_digest = hashlib.sha256(param_str.encode()).hexdigest()[:16]
        key = f"{namespace}_{hash_digest}"
        return self.cache_dir / key[:2] / f"{key}.json"

    def get(self, namespace: str, params: dict) -> Optional[any]:
        """Get cached value if exists.

        When recheck_dates is set, cmr_opera lookups for those dates
        bypass the cache (return None) so that CMR is re-queried.
        """
        if not self.enabled:
            return None

        # Bypass cache for cmr_opera entries on recheck dates
        if (self.recheck_dates and namespace == "cmr_opera"
                and params.get("date") in self.recheck_dates):
            self.misses += 1
            return None

        path = self._cache_path(namespace, params)
        if not path.exists():
            self.misses += 1
            return None

        try:
            with open(path) as f:
                cached = json.load(f)
            self.hits += 1
            return cached.get("data")
        except (json.JSONDecodeError, IOError):
            self.misses += 1
            return None

    def set(self, namespace: str, params: dict, data: any) -> None:
        """Store value in cache."""
        if not self.enabled:
            return
        path = self._cache_path(namespace, params)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump({"data": data}, f)
        except IOError:
            pass

    def clear(self, namespace: str = None) -> int:
        """Clear cached files. If namespace given, only clear that namespace.

        Valid namespaces: asf_bursts, cmr_slc, cmr_opera
        """
        if not self.cache_dir.exists():
            return 0
        pattern = f"{namespace}_*.json" if namespace else "*.json"
        count = sum(1 for p in self.cache_dir.rglob(pattern) if p.unlink() or True)
        # Clean up empty directories
        for d in self.cache_dir.rglob("*"):
            if d.is_dir():
                try:
                    d.rmdir()
                except OSError:
                    pass
        return count

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / total if total > 0 else 0,
            "enabled": self.enabled,
        }


# Global cache instance
_cache: Optional[RequestCache] = None


def get_cache() -> RequestCache:
    global _cache
    if _cache is None:
        _cache = RequestCache(enabled=False)
    return _cache


def init_cache(cache_dir: Path = None, enabled: bool = True,
               recheck_dates: set[str] = None) -> RequestCache:
    global _cache
    _cache = RequestCache(cache_dir, enabled, recheck_dates)
    return _cache


# =============================================================================
# GeoJSON Utilities
# =============================================================================

def load_geojson(path_or_url: str) -> dict:
    """Load GeoJSON from local file path or URL."""
    import requests
    if path_or_url.startswith(('http://', 'https://')):
        response = requests.get(path_or_url, timeout=60)
        response.raise_for_status()
        return response.json()
    with open(path_or_url) as f:
        return json.load(f)


def geojson_to_bbox(geojson: dict) -> tuple[float, float, float, float]:
    """
    Extract bounding box from GeoJSON.

    Returns (min_lon, min_lat, max_lon, max_lat).
    """
    def extract_coords(c):
        """Recursively extract coordinate pairs from nested structure."""
        if isinstance(c[0], (int, float)):
            return [c]
        return [coord for item in c for coord in extract_coords(item)]

    all_coords = []
    for feature in geojson.get('features', [geojson]):
        geom = feature.get('geometry', feature)
        all_coords.extend(extract_coords(geom.get('coordinates', [])))

    if not all_coords:
        raise ValueError("No coordinates found in GeoJSON")

    lons, lats = zip(*[(c[0], c[1]) for c in all_coords])
    return min(lons), min(lats), max(lons), max(lats)


def geojson_to_shapely(geojson: dict):
    """Convert GeoJSON to Shapely geometry (union of all features)."""
    from shapely.geometry import shape
    from shapely.ops import unary_union

    geometries = []
    for feature in geojson.get('features', [geojson]):
        geom = feature.get('geometry', feature)
        try:
            s = shape(geom)
            # Fix invalid geometries with buffer(0)
            geometries.append(s if s.is_valid else s.buffer(0))
        except Exception:
            continue

    if not geometries:
        raise ValueError("No valid geometries found in GeoJSON")
    return unary_union(geometries)


def polygon_intersects_geojson(cmr_points: list[dict], geojson_geom) -> bool:
    """
    Check if a CMR polygon intersects the GeoJSON geometry.

    Args:
        cmr_points: List of dicts with 'Latitude'/'Longitude' keys from CMR UMM
        geojson_geom: Shapely geometry to test intersection against
    """
    from shapely.geometry import Polygon

    coords = [(p.get('Longitude', p.get('lon')), p.get('Latitude', p.get('lat')))
              for p in cmr_points]
    if not coords:
        return False

    # Close polygon if needed
    if coords[0] != coords[-1]:
        coords.append(coords[0])

    try:
        poly = Polygon(coords)
        if not poly.is_valid:
            poly = poly.buffer(0)
        return geojson_geom.intersects(poly)
    except Exception:
        return False


# =============================================================================
# Time Utilities
# =============================================================================

def generate_time_chunks(start: datetime, end: datetime, days: int = 30) -> Iterator[tuple[datetime, datetime]]:
    """
    Generate time chunks for processing long date ranges.

    Yields (chunk_start, chunk_end) tuples, each spanning at most 'days' days.
    """
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=days), end)
        yield (current, chunk_end)
        current = chunk_end


# =============================================================================
# ASF Burst API
# =============================================================================

async def _parse_asf_burst_response(
    data: list, polarization: str = None, slc_product_id: str = None,
) -> list[str]:
    """Parse ASF SLC-BURST API response and return unique burst IDs.

    Parameters
    ----------
    data : list
        Raw JSON response from the ASF SLC-BURST API.
    polarization : str, optional
        Filter to this polarization (e.g. "VV").
    slc_product_id : str, optional
        Parent SLC product ID (native_id without the ``-SLC`` suffix).
        When provided, only bursts whose ``downloadUrl`` references this
        SLC are returned.  This prevents including bursts from adjacent
        SLCs whose acquisition windows overlap.
    """
    items = data[0] if data and isinstance(data[0], list) else data
    seen = set()
    burst_ids = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if polarization and item.get('polarization', '').upper() != polarization.upper():
            continue
        if slc_product_id:
            download_url = item.get('downloadUrl', '')
            if slc_product_id not in download_url:
                continue
        bid = item.get('burst', {}).get('fullBurstID')
        if bid and bid not in seen:
            seen.add(bid)
            burst_ids.append(bid)
    return burst_ids


_edl_token: str | None = None  # Lazy-initialized on first annotation derivation


async def _derive_bursts_from_annotation(
    slc: SLCGranule,
    asf_burst_ids: list[str],
    ref_burst_anx_time: float,
    polarization: str,
) -> list[str] | None:
    """Derive complete burst IDs from SLC annotation XMLs.

    Uses one ASF burst as reference anchor to compute burst numbers for all
    bursts found in the annotation XMLs. This ensures complete burst coverage
    regardless of how many bursts ASF returned.
    """
    global _edl_token
    logger = logging.getLogger(__name__)

    try:
        # Lazy-init EDL token
        if _edl_token is None:
            try:
                _edl_token = await asyncio.to_thread(get_edl_token)
            except Exception as exc:
                logger.warning(f"EDL token acquisition failed: {exc}")
                _edl_token = ""  # sentinel to avoid retrying
                return None

        if _edl_token == "":
            return None

        # Get download URL and extract annotations
        zip_url = await asyncio.to_thread(get_slc_download_url, slc.native_id)
        annotations = await asyncio.to_thread(extract_annotations, zip_url, _edl_token)
        anx_times = parse_burst_anx_times(annotations)

        if not anx_times:
            logger.warning(f"No ANX times parsed from annotations for {slc.native_id}")
            return None

        # Parse reference burst from first ASF burst ID
        ref_bid = asf_burst_ids[0]
        ref_track_str, ref_burst_str, ref_sw = ref_bid.split("_")
        ref_track = int(ref_track_str)
        ref_burst_num = int(ref_burst_str)

        all_ids = derive_burst_ids(
            anx_times, ref_track, ref_burst_num, ref_burst_anx_time, ref_sw,
        )
        return all_ids

    except Exception as exc:
        logger.warning(f"Annotation burst derivation failed for {slc.native_id}: {exc}")
        return None


def _estimate_expected_bursts(slc: "SLCGranule") -> int:
    """Estimate expected burst count from SLC duration.

    IW mode acquires ~1 burst per ~3.0s across 3 subswaths.
    For a single polarization: duration / 3.0 * 3.

    Validated against ESA annotation XMLs via slc_annotation_extract.py:
      103F (13s) → 12 VV bursts (estimate: 13)
      A508 (30s) → 30 VV bursts (estimate: 30)
    """
    duration_s = (slc.end_time - slc.start_time).total_seconds()
    return max(1, int(duration_s / 3.0 * 3))


async def fetch_bursts_for_slc(
    slc: SLCGranule,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    polarization: str = None,
) -> list[BurstInfo]:
    """
    Query ASF SLC-BURST API to get burst IDs for an SLC granule.

    Uses caching and retry logic with exponential backoff for rate limiting.
    Always derives complete burst set from annotation XMLs when ASF returns
    at least one burst (ASF data is used only as a reference anchor).
    """
    logger = logging.getLogger(__name__)
    cache = get_cache()

    # Build cache key.  Includes 'slc' and '_v' (version) so that results
    # cached before logic changes are automatically invalidated.
    cache_params = {
        'platform': slc.platform,
        'orbit': slc.absolute_orbit,
        'start': slc.start_time.isoformat(),
        'end': slc.end_time.isoformat(),
        'pol': polarization,
        'slc': slc.native_id,
        '_v': 5,  # bump when burst-fetching logic changes
    }

    # Check cache
    cached = cache.get("asf_bursts", cache_params)
    if cached is not None:
        if cached:  # non-empty cached result
            return [BurstInfo.from_asf_id(bid) for bid in cached]
        else:  # empty cached result (polarization mismatch)
            return []

    # Build API request
    params = {
        'dataset': 'SLC-BURST',
        'platform': PLATFORM_MAP.get(slc.platform, slc.platform),
        'absoluteOrbit': slc.absolute_orbit,
        'start': slc.start_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'end': slc.end_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'output': 'json',
        'maxResults': 500,
    }
    if polarization:
        params['polarization'] = polarization.upper()

    url = f"{ASF_BURST_API_URL}?{urllib.parse.urlencode(params)}"

    # Derive parent SLC product ID for filtering bursts from adjacent SLCs.
    # native_id format: S1A_IW_SLC__1SDV_..._EA33-SLC → strip "-SLC" suffix.
    slc_product_id = slc.native_id.removesuffix("-SLC")

    # Retry with exponential backoff, keeping the best response
    best_burst_ids = []
    best_ref_anx = 0.0  # Reference ANX time for annotation fallback
    successful_attempts = 0

    for attempt in range(3):
        async with sem:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 429:  # Rate limited
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if resp.status != 200:
                        return []

                    data = await resp.json()
                    burst_ids = await _parse_asf_burst_response(data, polarization, slc_product_id)
                    successful_attempts += 1

                    # Keep the response with the most bursts
                    if burst_ids and len(burst_ids) > len(best_burst_ids):
                        best_burst_ids = burst_ids
                        # Save reference ANX time for annotation fallback
                        items = data[0] if data and isinstance(data[0], list) else data
                        ref_bid = burst_ids[0]
                        for item in items:
                            if isinstance(item, dict) and item.get('burst', {}).get('fullBurstID') == ref_bid:
                                best_ref_anx = float(item['burst'].get('azimuthAnxTime', 0))
                                break

                    # Got at least one burst — can proceed to annotation fallback
                    if best_burst_ids:
                        break

                    # 0 bursts — retry (unless consistently empty)
                    if successful_attempts >= 2:
                        # Two successful requests both returned 0 bursts.
                        # This is likely a polarization mismatch (e.g., SDH SLC
                        # queried with VV), not a transient issue. Cache as empty.
                        logger.debug(
                            f"No bursts for {slc.native_id} after "
                            f"{successful_attempts} attempts — caching empty result "
                            f"(likely polarization mismatch)"
                        )
                        cache.set("asf_bursts", cache_params, [])
                        return []

                    await asyncio.sleep(2 ** attempt)

            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                logger.debug(f"Failed to get bursts for {slc.native_id}")
                break

    # If ASF returned at least one burst, derive complete set from annotation XMLs
    # (ASF burst is used as reference anchor for burst ID calculation)
    if best_burst_ids:
        all_ids = await _derive_bursts_from_annotation(
            slc, best_burst_ids, best_ref_anx, polarization or "VV",
        )
        if all_ids:
            logger.info(
                f"Annotation-derived bursts for {slc.native_id}: "
                f"{len(all_ids)} (ASF reference: {len(best_burst_ids)})"
            )
            cache.set("asf_bursts", cache_params, all_ids)
            return [BurstInfo.from_asf_id(bid) for bid in all_ids]
        else:
            # Annotation derivation failed — fall back to ASF data only
            logger.warning(
                f"Annotation derivation failed for {slc.native_id}, "
                f"using ASF data only ({len(best_burst_ids)} bursts). Result NOT cached."
            )
            return [BurstInfo.from_asf_id(bid) for bid in best_burst_ids]

    return []


# =============================================================================
# CMR Queries
# =============================================================================

async def fetch_slc_granules(
    start: datetime,
    end: datetime,
    bbox_str: str,
) -> tuple[set[str], dict]:
    """
    Fetch SLC granules from CMR for all Sentinel-1 platforms.

    Returns (set of native_ids, dict mapping native_id -> CMR item details).
    """
    logger = logging.getLogger(__name__)
    cache = get_cache()

    all_ids = set()
    all_details = {}

    for platform in ["SENTINEL-1A", "SENTINEL-1B", "SENTINEL-1C"]:
        collection = f"{platform}_SLC"
        start_str = start.isoformat().replace("+00:00", "Z")
        end_str = end.isoformat().replace("+00:00", "Z")

        # Check cache
        cache_params = {'collection': collection, 'start': start_str, 'end': end_str, 'bbox': bbox_str}
        cached = cache.get("cmr_slc", cache_params)

        if cached is not None:
            logger.debug(f"Cache hit for {collection}")
            all_details.update(cached)
            all_ids.update(cached.keys())
        else:
            # Query CMR
            ids, details = await async_get_cmr_granules(
                collection,
                temporal_date_start=start_str,
                temporal_date_end=end_str,
                platform_short_name=platform,
                bounding_box=bbox_str,
            )
            cache.set("cmr_slc", cache_params, details)
            all_details.update(details)
            all_ids.update(ids)
            logger.info(f"  {platform}: {len(ids)} granules")

    return all_ids, all_details


async def fetch_opera_products(
    burst: BurstInfo,
    acq_time: datetime,
    product_type: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    slc_end_time: datetime | None = None,
) -> set[str]:
    """
    Query CMR for OPERA products matching a burst ID and acquisition date.

    When the SLC acquisition spans midnight (slc_end_time falls on a
    different date than acq_time), the search window is extended to cover
    both days so that bursts near the end of the SLC are not missed.

    Returns set of matching product native-ids.
    """
    cache = get_cache()
    short_name = OPERA_PRODUCTS.get(product_type)

    # Determine the date range for the CMR temporal search.
    # Normally search within the acquisition day, but extend to the
    # next day if the SLC spans midnight.
    start_dt = acq_time.replace(hour=0, minute=0, second=0, microsecond=0)
    if slc_end_time and slc_end_time.date() > acq_time.date():
        end_dt = slc_end_time.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        end_dt = acq_time.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Check cache (include end date so midnight-spanning SLCs get
    # separate cache entries from same-day SLCs)
    cache_params = {
        'burst': burst.filename_pattern,
        'date': acq_time.strftime('%Y-%m-%d'),
        'end_date': end_dt.strftime('%Y-%m-%d'),
        'product': product_type,
    }
    cached = cache.get("cmr_opera", cache_params)
    if cached is not None:
        return set(cached)

    body = (
        f"provider=ASF&short_name[]={short_name}"
        f"&native-id=*{burst.filename_pattern}*"
        "&options[native-id][pattern]=true"
        f"&temporal[]={urllib.parse.quote(start_dt.isoformat(), safe='/:')},{urllib.parse.quote(end_dt.isoformat(), safe='/:')}"
        "&page_size=100"
    )

    for attempt in range(3):
        async with sem:
            try:
                async with session.post(
                    CMR_GRANULE_URL,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(0.5 * (2 ** attempt))
                        continue
                    if resp.status != 200:
                        return set()

                    data = await resp.json()
                    product_ids = [item["meta"]["native-id"] for item in data.get("items", [])]
                    cache.set("cmr_opera", cache_params, product_ids)
                    return set(product_ids)

            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                return set()

    return set()


# =============================================================================
# JSONL Streaming Writer
# =============================================================================

class JSONLWriter:
    """
    Streaming writer for JSONL output format.

    Writes each record as a separate JSON line, flushing immediately
    to ensure data persistence. Used in low-memory mode.
    """

    def __init__(self, path: str, metadata: dict = None):
        self.path = path
        self.file = open(path, 'w')
        if metadata:
            self._write({"_type": "metadata", **metadata})

    def _write(self, obj: dict):
        self.file.write(json.dumps(obj, default=str) + '\n')
        self.file.flush()

    def write_chunk(self, chunk_start: datetime, chunk_end: datetime,
                    product_type: str, found: list, missing: list):
        """Write results for a single time chunk."""
        self._write({
            "_type": "chunk_result",
            "chunk_start": chunk_start.isoformat(),
            "chunk_end": chunk_end.isoformat(),
            "product_type": product_type,
            "found_count": len(found),
            "missing_count": len(missing),
            "found": found,
            "missing": missing,
        })

    def write_summary(self, summary: dict):
        self._write({"_type": "summary", **summary})

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# =============================================================================
# Core Audit Logic
# =============================================================================

async def process_slcs_to_expected_bursts(
    slcs: list[SLCGranule],
    polarizations: list[str],
    max_concurrent: int = 5,
) -> tuple[int, list[ExpectedBurst]]:
    """
    Fetch burst IDs for SLCs and build deduplicated list of expected products.

    Returns (total_raw_bursts, deduplicated_expected_bursts).
    """
    logger = logging.getLogger(__name__)

    # Fetch bursts from ASF API
    primary_pol = polarizations[0] if polarizations else None
    logger.info(f"  Fetching burst IDs from ASF (polarization: {primary_pol})...")

    sem = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_bursts_for_slc(slc, session, sem, primary_pol) for slc in slcs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for slc, result in zip(slcs, results):
            slc.bursts = result if not isinstance(result, Exception) else []

    total_raw = sum(len(slc.bursts) for slc in slcs)
    logger.info(f"  Total bursts (before dedup): {total_raw:,}")

    # Deduplicate: same burst can appear in overlapping SLCs
    # Key: (burst_id, date, polarization) -> first occurrence wins
    unique: dict[tuple, ExpectedBurst] = {}
    for slc in slcs:
        for burst in slc.bursts:
            for pol in polarizations:
                key = (burst.asf_id, slc.start_time.date(), pol)
                if key not in unique:
                    unique[key] = ExpectedBurst(
                        burst=burst,
                        acquisition_time=slc.start_time,
                        platform=slc.platform,
                        polarization=pol,
                        slc_native_id=slc.native_id,
                        slc_end_time=slc.end_time,
                    )

    expected = list(unique.values())
    logger.info(f"  Unique bursts after dedup: {len(expected):,}")
    return total_raw, expected


async def check_coverage_for_bursts(
    expected_bursts: list[ExpectedBurst],
    product_type: str,
    max_concurrent: int = 50,
) -> tuple[list[dict], list[dict]]:
    """
    Check CMR for OPERA products matching expected bursts.

    Returns (found_list, missing_list) as dicts ready for JSON output.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"  Checking {product_type} coverage...")

    # Group by (burst_id, date) to reduce API calls
    groups: dict[tuple, list[ExpectedBurst]] = defaultdict(list)
    for exp in expected_bursts:
        key = (exp.burst.asf_id, exp.acquisition_time.date())
        groups[key].append(exp)

    sem = asyncio.Semaphore(max_concurrent)

    async def check_group(group: list[ExpectedBurst], session: aiohttp.ClientSession) -> tuple[list[dict], list[dict]]:
        """Check a group of expected bursts (same burst+date, different polarizations)."""
        burst = group[0].burst
        acq_time = group[0].acquisition_time
        slc_end = group[0].slc_end_time

        found_products = await fetch_opera_products(burst, acq_time, product_type, session, sem, slc_end_time=slc_end)

        found, missing = [], []
        for exp in group:
            if product_type == "RTC-S1":
                # RTC produces one granule per burst covering all polarizations.
                # The native-id contains pixel spacing (_30_), not polarization.
                match = next(iter(found_products), None)
            else:
                # CSLC produces one granule per polarization — match by pol in name.
                match = next((p for p in found_products if exp.polarization in p), None)
            if match:
                found.append({**exp.to_dict(), "opera_product_id": match})
            else:
                missing.append(exp.to_dict())
        return found, missing

    # Process in batches for progress reporting
    all_found, all_missing = [], []
    group_list = list(groups.values())

    batch_size = 100
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(group_list), batch_size):
            tasks = [check_group(g, session) for g in group_list[i:i + batch_size]]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue
                found, missing = result
                all_found.extend(found)
                all_missing.extend(missing)

            logger.info(f"    Checked {min(i + batch_size, len(group_list))}/{len(group_list)} groups, "
                       f"found {len(all_found)}, missing {len(all_missing)}")

    return all_found, all_missing


async def audit_burst_coverage(
    start_datetime: datetime,
    end_datetime: datetime,
    geojson_path: str,
    product_types: list[str],
    polarizations: list[str],
    low_memory: bool = False,
    output_path: str = None,
    chunk_days: int = 30,
    buffer_deg: float = 0.5,
) -> dict:
    """
    Main audit function: check OPERA product coverage for bursts in a region.

    In low-memory mode, results are streamed to JSONL file incrementally.
    In standard mode, all results are returned in a dict.

    Args:
        start_datetime: Start of temporal range
        end_datetime: End of temporal range
        geojson_path: Path to GeoJSON defining spatial region
        product_types: List of product types to check ('CSLC-S1', 'RTC-S1')
        polarizations: List of polarizations ('VV', 'VH', etc.)
        low_memory: If True, process in chunks and stream to JSONL
        output_path: Output file path (required for low_memory mode)
        chunk_days: Days per chunk in low_memory mode
        buffer_deg: Buffer in degrees to expand the GeoJSON boundary (default: 0.5)

    Returns:
        dict with coverage statistics (detailed results in file if low_memory)
    """
    logger = logging.getLogger(__name__)

    # Load and parse GeoJSON
    logger.info(f"Loading GeoJSON from {geojson_path}")
    geojson = load_geojson(geojson_path)
    bbox = geojson_to_bbox(geojson)

    # Apply buffer to bounding box (clamp to valid ranges)
    if buffer_deg > 0:
        bbox = (
            max(-180.0, bbox[0] - buffer_deg),  # min_lon
            max(-90.0, bbox[1] - buffer_deg),   # min_lat
            min(180.0, bbox[2] + buffer_deg),   # max_lon
            min(90.0, bbox[3] + buffer_deg),    # max_lat
        )
        logger.info(f"Applied {buffer_deg}° buffer to bounding box")

    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    logger.info(f"Bounding box: {bbox_str}")

    # Create Shapely geometry for polygon intersection filtering
    logger.info("Creating geometry for polygon filtering")
    geojson_geom = geojson_to_shapely(geojson)

    # Apply buffer to geometry for intersection checks
    if buffer_deg > 0:
        geojson_geom = geojson_geom.buffer(buffer_deg)
        logger.info(f"Applied {buffer_deg}° buffer to geometry")

    # Determine time chunks
    if low_memory:
        chunks = list(generate_time_chunks(start_datetime, end_datetime, chunk_days))
        logger.info(f"Low-memory mode: {len(chunks)} chunks of ~{chunk_days} days")
    else:
        chunks = [(start_datetime, end_datetime)]

    # Initialize tracking
    totals = {"slcs": 0, "bursts_raw": 0, "bursts_unique": 0}
    product_stats = {pt: {"found": 0, "missing": 0, "expected": 0} for pt in product_types}
    all_results = {"found": defaultdict(list), "missing": defaultdict(list)}

    # Setup JSONL writer for low-memory mode
    writer = None
    if low_memory and output_path:
        writer = JSONLWriter(output_path, {
            "start_datetime": start_datetime.isoformat(),
            "end_datetime": end_datetime.isoformat(),
            "geojson": geojson_path,
            "polarizations": polarizations,
            "product_types": product_types,
            "chunk_days": chunk_days,
        })

    try:
        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            if low_memory:
                logger.info(f"Processing chunk {chunk_idx}/{len(chunks)}: "
                           f"{chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")

            # Step 1: Fetch SLC granules from CMR
            logger.info("  Querying CMR for SLC granules...")
            slc_ids, slc_details = await fetch_slc_granules(chunk_start, chunk_end, bbox_str)
            logger.info(f"  Found {len(slc_details)} SLCs from bbox query")

            # Step 2: Filter by polygon intersection
            filtered_ids = set()
            for native_id, item in slc_details.items():
                try:
                    points = (item.get("umm", {})
                              .get("SpatialExtent", {})
                              .get("HorizontalSpatialDomain", {})
                              .get("Geometry", {})
                              .get("GPolygons", [{}])[0]
                              .get("Boundary", {})
                              .get("Points", []))
                    if points and polygon_intersects_geojson(points, geojson_geom):
                        filtered_ids.add(native_id)
                except Exception:
                    filtered_ids.add(native_id)  # Include on error

            logger.info(f"  After polygon filtering: {len(filtered_ids)} SLCs")

            # Free memory
            del slc_ids, slc_details
            gc.collect()

            # Step 3: Parse SLC granules, filtering out incompatible polarizations
            # SLC native IDs encode polarization mode: SDV (VV+VH), SDH (HH+HV),
            # SSV (VV only), SSH (HH only). Skip SLCs that can't have requested pols.
            pol_modes = set()
            for pol in polarizations:
                if pol.upper() in ('VV', 'VH'):
                    pol_modes.update(('SDV', 'SSV'))
                elif pol.upper() in ('HH', 'HV'):
                    pol_modes.update(('SDH', 'SSH'))

            slcs = []
            skipped_pol = 0
            for nid in filtered_ids:
                # Check polarization mode from native ID (e.g., S1A_IW_SLC__1SDV_...)
                if pol_modes and not any(mode in nid for mode in pol_modes):
                    skipped_pol += 1
                    continue
                slc = SLCGranule.from_native_id(nid)
                if slc:
                    slcs.append(slc)
            if skipped_pol:
                logger.info(f"  Skipped {skipped_pol} SLCs with incompatible polarization")

            del filtered_ids
            gc.collect()

            if not slcs:
                logger.info("  No valid SLCs in this chunk, skipping")
                continue

            totals["slcs"] += len(slcs)

            # Step 4: Fetch bursts and build expected products list
            raw_count, expected_bursts = await process_slcs_to_expected_bursts(slcs, polarizations)
            totals["bursts_raw"] += raw_count
            totals["bursts_unique"] += len(expected_bursts)

            del slcs
            gc.collect()

            if not expected_bursts:
                continue

            # Step 5: Check coverage for each product type
            for product_type in product_types:
                found, missing = await check_coverage_for_bursts(expected_bursts, product_type)

                product_stats[product_type]["found"] += len(found)
                product_stats[product_type]["missing"] += len(missing)
                product_stats[product_type]["expected"] += len(expected_bursts)

                if low_memory and writer:
                    writer.write_chunk(chunk_start, chunk_end, product_type, found, missing)
                else:
                    all_results["found"][product_type].extend(found)
                    all_results["missing"][product_type].extend(missing)

                logger.info(f"    {product_type}: {len(found)} found, {len(missing)} missing")

                del found, missing
                gc.collect()

            del expected_bursts
            gc.collect()

            if low_memory:
                logger.info(f"  Chunk {chunk_idx} complete")

    finally:
        if writer:
            # Write final summary
            summary = {
                "total_slcs": totals["slcs"],
                "total_bursts_raw": totals["bursts_raw"],
                "total_unique_bursts": totals["bursts_unique"],
                "products": {},
            }
            for pt in product_types:
                stats = product_stats[pt]
                coverage = (stats["found"] / stats["expected"] * 100) if stats["expected"] > 0 else 100.0
                summary["products"][pt] = {
                    "expected_count": stats["expected"],
                    "found_count": stats["found"],
                    "missing_count": stats["missing"],
                    "coverage_percent": round(coverage, 2),
                }
            writer.write_summary(summary)
            writer.close()

    # Build results dict
    results = {
        "metadata": {
            "start_datetime": start_datetime.isoformat(),
            "end_datetime": end_datetime.isoformat(),
            "geojson": geojson_path,
            "slc_count": totals["slcs"],
            "total_bursts_raw": totals["bursts_raw"],
            "unique_bursts": totals["bursts_unique"],
            "polarizations": polarizations,
        },
        "products": {},
    }

    for pt in product_types:
        stats = product_stats[pt]
        coverage = (stats["found"] / stats["expected"] * 100) if stats["expected"] > 0 else 100.0
        results["products"][pt] = {
            "expected_count": stats["expected"],
            "found_count": stats["found"],
            "missing_count": stats["missing"],
            "coverage_percent": round(coverage, 2),
        }
        if not low_memory:
            results["products"][pt]["found"] = all_results["found"][pt]
            results["products"][pt]["missing"] = all_results["missing"][pt]

    return results


# =============================================================================
# CLI
# =============================================================================

def print_report(results: dict, show_missing: int = 20):
    """Print human-readable coverage report to console."""
    print("\n" + "=" * 70)
    print("OPERA Burst-Level Coverage Audit Report")
    print("=" * 70)

    meta = results["metadata"]
    print(f"Time Range: {meta['start_datetime']} to {meta['end_datetime']}")
    print(f"GeoJSON: {meta['geojson']}")
    print(f"SLC Granules: {meta['slc_count']:,}")
    print(f"Total Bursts (raw): {meta['total_bursts_raw']:,}")
    print(f"Unique Bursts: {meta['unique_bursts']:,}")
    print(f"Polarizations: {meta.get('polarizations', ['VV'])}")

    cache = get_cache()
    if cache.enabled:
        stats = cache.stats()
        print(f"Cache: {stats['hits']:,} hits, {stats['misses']:,} misses "
              f"({stats['hit_rate']:.1%} hit rate)")

    print("-" * 70)

    for product_type, coverage in results["products"].items():
        print(f"\n{product_type}:")
        print(f"  Expected:  {coverage['expected_count']:,}")
        print(f"  Found:     {coverage['found_count']:,}")
        print(f"  Missing:   {coverage['missing_count']:,}")
        print(f"  Coverage:  {coverage['coverage_percent']:.1f}%")

        if show_missing > 0 and coverage.get("missing"):
            print(f"\n  First {min(show_missing, len(coverage['missing']))} missing:")
            for item in coverage["missing"][:show_missing]:
                print(f"    {item['burst_pattern']} | {item['acquisition_time'][:10]} | "
                      f"{item['platform']} {item['polarization']}")
            if len(coverage["missing"]) > show_missing:
                print(f"    ... and {len(coverage['missing']) - show_missing} more")

    print("\n" + "=" * 70)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit OPERA CSLC-S1 and RTC-S1 coverage at burst level",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument("--start-datetime", required=True,
                        help="Start datetime (ISO format with timezone)")
    parser.add_argument("--end-datetime", required=True,
                        help="End datetime (ISO format with timezone)")
    parser.add_argument("--geojson", "-g", required=True,
                        help="Path or URL to GeoJSON file")

    # Product options
    parser.add_argument("--do-cslc", type=lambda x: x.lower() in ('true', 'yes', '1'),
                        default=True, help="Check CSLC-S1 (default: true)")
    parser.add_argument("--do-rtc", type=lambda x: x.lower() in ('true', 'yes', '1'),
                        default=True, help="Check RTC-S1 (default: true)")
    parser.add_argument("--polarizations", nargs="+", default=["VV"],
                        help="Polarizations to check (default: VV)")

    # Output options
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--format", default="json", choices=["json", "txt"],
                        help="Output format (default: json)")
    parser.add_argument("--show-missing", type=int, default=20,
                        help="Show first N missing products in console (default: 20)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    # Cache options
    parser.add_argument("--cache-dir", type=Path, default=RequestCache.DEFAULT_DIR,
                        help=f"Cache directory (default: {RequestCache.DEFAULT_DIR})")
    parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    parser.add_argument("--clear-cache", action="store_true", help="Clear all cached data before running")
    parser.add_argument("--clear-cache-namespace", type=str, metavar="NAMESPACE",
                        choices=["asf_bursts", "cmr_slc", "cmr_opera"],
                        help="Clear only a specific cache namespace before running "
                             "(asf_bursts, cmr_slc, cmr_opera)")
    parser.add_argument("--recheck-dates", type=str, metavar="DATES",
                        help="Comma-separated dates (YYYY-MM-DD) to recheck. "
                             "Bypasses the cmr_opera cache for these dates, "
                             "forcing fresh CMR queries while reusing all other "
                             "cached data.")
    parser.add_argument("--recheck-dates-file", type=str, metavar="FILE",
                        help="File containing dates (YYYY-MM-DD) to recheck, "
                             "one per line. Same behavior as --recheck-dates.")

    # Low-memory options
    parser.add_argument("--low-memory", action="store_true",
                        help="Stream results to JSONL (for long date ranges)")
    parser.add_argument("--chunk-days", type=int, default=30,
                        help="Days per chunk in low-memory mode (default: 30)")

    # Spatial buffer option
    parser.add_argument("--buffer-deg", type=float, default=0.5,
                        help="Buffer in degrees to expand the GeoJSON boundary (default: 0.5). "
                             "Use ~0.15 (~15 km) to capture SLCs at boundary edges.")

    return parser


async def main():
    args = create_parser().parse_args()

    # Setup logging
    init_logging('cmr_audit_burst_coverage.log', 'cmr_audit_burst_coverage-error.log',
                 level=args.log_level)
    logger = logging.getLogger(__name__)

    # Parse recheck dates (from --recheck-dates and/or --recheck-dates-file)
    recheck_dates = set()
    if args.recheck_dates:
        recheck_dates.update(d.strip() for d in args.recheck_dates.split(","))
    if args.recheck_dates_file:
        with open(args.recheck_dates_file) as f:
            recheck_dates.update(line.strip() for line in f if line.strip())
    if recheck_dates:
        logger.info(f"Rechecking {len(recheck_dates)} dates: {sorted(recheck_dates)}")

    # Setup cache
    cache = init_cache(args.cache_dir, not args.no_cache, recheck_dates)
    if args.clear_cache:
        deleted = cache.clear()
        logger.info(f"Cleared {deleted} cached files")
    elif args.clear_cache_namespace:
        deleted = cache.clear(args.clear_cache_namespace)
        logger.info(f"Cleared {deleted} cached files in namespace '{args.clear_cache_namespace}'")
    if cache.enabled:
        logger.info(f"Cache enabled: {cache.cache_dir}")

    # Parse datetimes
    start_dt = isoparse(args.start_datetime)
    end_dt = isoparse(args.end_datetime)
    if not start_dt.tzinfo or not end_dt.tzinfo:
        logger.error("Datetimes must include timezone (e.g., 2024-01-01T00:00:00Z)")
        return 1

    # Determine product types
    product_types = []
    if args.do_cslc:
        product_types.append("CSLC-S1")
    if args.do_rtc:
        product_types.append("RTC-S1")

    if not product_types:
        logger.error("At least one product type must be enabled")
        return 1

    # Low-memory mode validation
    if args.low_memory and not args.output:
        logger.error("--output required with --low-memory")
        return 1

    # Ensure JSONL extension for low-memory mode
    output_path = args.output
    if args.low_memory and output_path and not output_path.endswith('.jsonl'):
        output_path = output_path.rsplit('.', 1)[0] + '.jsonl'
        logger.info(f"Using JSONL output: {output_path}")

    # Run audit
    results = await audit_burst_coverage(
        start_datetime=start_dt,
        end_datetime=end_dt,
        geojson_path=args.geojson,
        product_types=product_types,
        polarizations=args.polarizations,
        low_memory=args.low_memory,
        output_path=output_path,
        chunk_days=args.chunk_days,
        buffer_deg=args.buffer_deg,
    )

    # Print report
    print_report(results, show_missing=args.show_missing if not args.low_memory else 0)

    # Write output (non-low-memory mode)
    if output_path and not args.low_memory:
        with open(output_path, 'w') as f:
            if args.format == "json":
                json.dump(results, f, indent=2, default=str)
            else:  # txt
                for pt, cov in results["products"].items():
                    f.write(f"# {pt} Missing Products\n")
                    for item in cov.get("missing", []):
                        f.write(f"{item['burst_pattern']},{item['acquisition_time']},"
                               f"{item['platform']},{item['polarization']}\n")
                    f.write("\n")
        logger.info(f"Results written to {output_path}")

    if args.low_memory:
        print(f"\nDetailed results written to: {output_path}")

    # Log cache stats
    if cache.enabled:
        stats = cache.stats()
        logger.info(f"Cache: {stats['hits']} hits, {stats['misses']} misses, "
                   f"{stats['hit_rate']:.1%} hit rate")

    # Return exit code based on coverage
    has_missing = any(c["missing_count"] > 0 for c in results["products"].values())
    return 1 if has_missing else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
