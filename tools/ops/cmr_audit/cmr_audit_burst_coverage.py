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
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import cache
from pathlib import Path
from typing import Optional, Iterator

import aiohttp
from dateutil.parser import isoparse

from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules, init_logging
from tools.ops.cmr_audit.cmr_client import (
    IncompleteCmrResultsError,
    is_transient_cmr_error,
    page_incomplete_reason,
    shortfall_reason,
)
from tools.ops.cmr_audit.slc_annotation_extract import (
    get_slc_download_url,
    get_edl_token,
    extract_slc_metadata,
    derive_burst_ids_from_metadata,
)

# Suppress noisy loggers
logging.getLogger("compact_json.formatter").setLevel(logging.INFO)

# =============================================================================
# Constants
# =============================================================================

CMR_GRANULE_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

# OPERA product short names in CMR
OPERA_PRODUCTS = {
    "CSLC-S1": "OPERA_L2_CSLC-S1_V1",
    "RTC-S1": "OPERA_L2_RTC-S1_V1",
}

# Sentinel-1 platform name mapping (kept for reference, no longer used in queries)
PLATFORM_MAP = {
    "S1A": "SENTINEL-1A",
    "S1B": "SENTINEL-1B",
    "S1C": "SENTINEL-1C",
    "S1D": "SENTINEL-1D",
}

GRQ_SLC_INDEX = 'grq_*_l1_s1_slc-*'
GRQ_RTC_INDEX = 'grq_*_l2_rtc_s1-*'
GRQ_CSLC_INDEX = 'grq_*_l2_cslc_s1-*'

PRODUCT_INDEX_MAP = {
    'SLC-S1': GRQ_SLC_INDEX,
    'RTC-S1': GRQ_RTC_INDEX,
    'CSLC-S1': GRQ_CSLC_INDEX,
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
    platform: str          # S1A, S1B, S1C, S1D
    start_time: datetime
    end_time: datetime
    absolute_orbit: int
    bursts: list[BurstInfo] = field(default_factory=list)

    # Regex pattern for parsing SLC native-id
    _PATTERN = re.compile(
        r'(S1[A-D])_IW_SLC__\w+_'
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


@dataclass
class ProductLookup:
    """What CMR (or GRQ) said about the products for one burst and date.

    ``verified`` is False when the lookup did not get a complete answer (5xx or network error after
    retries, fewer items than CMR counted, timed-out query). Bursts without a match in an unverified
    lookup are reported as unverified, not missing, so a CMR hiccup cannot masquerade as a gap.
    """
    products: set
    verified: bool = True
    reason: str = ""


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
    # Namespaces holding CMR answers; only entries marked complete are trusted (see get()).
    CMR_ANSWER_NAMESPACES = ("cmr_slc", "cmr_opera")

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

    def _window_touches_recheck_date(self, start_str, end_str, inclusive_end: bool) -> bool:
        """True when a cached entry's window covers a recheck date.

        cmr_slc listings span [start, end) datetimes; cmr_opera lookups span [date, end_date] days
        (end_date is the next day when the SLC crosses midnight).
        """
        try:
            start = isoparse(start_str)
            end = isoparse(end_str)
        except (KeyError, ValueError, TypeError):
            return False
        day = start.date()
        if inclusive_end:
            last = end.date()
        else:
            last = (end - timedelta(microseconds=1)).date() if end > start else day
        while day <= last:
            if day.isoformat() in self.recheck_dates:
                return True
            day += timedelta(days=1)
        return False

    def _bypass_for_recheck(self, namespace: str, params: dict) -> bool:
        if not self.recheck_dates:
            return False
        if namespace == "cmr_opera":
            return self._window_touches_recheck_date(params.get("date"), params.get("end_date", params.get("date")), True)
        if namespace == "cmr_slc":
            return self._window_touches_recheck_date(params.get("start"), params.get("end"), False)
        return False

    def get(self, namespace: str, params: dict) -> Optional[any]:
        """Get cached value if exists.

        When recheck_dates is set, cmr_opera lookups whose date..end_date and
        cmr_slc listings whose start..end window cover one of them bypass the
        cache (return None) so that CMR is re-queried.
        """
        if not self.enabled:
            return None

        if self._bypass_for_recheck(namespace, params):
            self.misses += 1
            return None

        path = self._cache_path(namespace, params)
        if not path.exists():
            self.misses += 1
            return None

        try:
            with open(path) as f:
                cached = json.load(f)
        except (json.JSONDecodeError, IOError):
            self.misses += 1
            return None
        if namespace in self.CMR_ANSWER_NAMESPACES and not cached.get("complete"):
            # A CMR answer written before answers were checked against CMR's count may be a
            # truncated listing, so it is re-fetched (and rewritten with the flag) instead of trusted.
            # asf_bursts entries come from the SLC annotation files, not from CMR, and stay valid.
            self.misses += 1
            return None
        self.hits += 1
        return cached.get("data")

    def set(self, namespace: str, params: dict, data: any) -> None:
        """Store a value that was verified complete; only such values are ever written."""
        if not self.enabled:
            return
        path = self._cache_path(namespace, params)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump({"data": data, "complete": True}, f)
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


def geojson_to_bbox(geojson: dict, buffer_deg: float = 0.0) -> tuple[float, float, float, float]:
    """
    Bounding box of the GeoJSON in CMR's order: (west, south, east, north).

    Longitude is a circle, so the box is the shortest arc of longitude that covers every coordinate.
    A region with land on both sides of the antimeridian (North America: the western Aleutians and
    Guam/CNMI lie east of 180°) gets a box whose west edge is east of its east edge, e.g. 144..-64,
    which CMR reads as crossing 180°. The plain min/max box for that region, -179..180, spanned every
    longitude and listed about ten SLCs for each one the polygon filter kept.

    buffer_deg widens the box on every side: latitude is clamped to [-90, 90], longitude wraps at 180°,
    and a buffer that closes the circle gives the global box.
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
    west, east = shortest_lon_arc(lons)
    south, north = min(lats), max(lats)
    if buffer_deg > 0:
        if (east - west) % 360.0 + 2 * buffer_deg >= 360.0:
            west, east = -180.0, 180.0
        else:
            west = _wrap_lon(west - buffer_deg)
            east = _wrap_lon(east + buffer_deg)
            if east == -180.0:
                east = 180.0
        south, north = max(-90.0, south - buffer_deg), min(90.0, north + buffer_deg)
    return west, south, east, north


def shortest_lon_arc(lons) -> tuple[float, float]:
    """
    (west, east) edges of the shortest arc of longitude that covers every value in lons.

    The arc starts after the widest gap between neighbouring longitudes. When that gap straddles the
    antimeridian the result is the ordinary (min, max) box; otherwise the arc runs through 180° and
    west > east, the form CMR takes for a box that crosses the antimeridian.
    """
    unique = sorted(set(lons))
    if len(unique) == 1:
        return unique[0], unique[0]
    gaps = [(unique[i + 1] - unique[i], i) for i in range(len(unique) - 1)]
    widest_gap, after = max(gaps)
    if unique[0] + 360.0 - unique[-1] >= widest_gap:
        return unique[0], unique[-1]
    return unique[after + 1], unique[after]


def _wrap_lon(lon: float) -> float:
    """Longitude folded into [-180, 180)."""
    return (lon + 180.0) % 360.0 - 180.0


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
# Burst ID derivation (annotation XML + manifest.safe via HTTP range requests)
# =============================================================================

_edl_token: str | None = None  # Lazy-initialized on first annotation derivation


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
    """Derive burst IDs for an SLC using its own annotation XMLs + manifest.safe.

    Self-contained per SLC — no ASF SLC-BURST API call. Uses the ESA
    burst-ID formula (same as OPERA's PGE via s1-reader). This avoids the
    "boundary-overlap" bug where ASF's time-range query returns bursts from
    adjacent SLC acquisitions on different tracks, causing the audit to
    anchor on the wrong SLC and produce off-by-one track numbers.

    Results are cached under the ``asf_bursts`` namespace (name kept for
    backward compatibility with the cache layout). The cache version key
    distinguishes annotation-only derivation from older ASF-anchored entries.
    """
    global _edl_token
    logger = logging.getLogger(__name__)
    cache = get_cache()

    # Build cache key. The 'derivation' field distinguishes annotation-only
    # results from older ASF-anchored entries with the same SLC.
    cache_params = {
        'platform': slc.platform,
        'orbit': slc.absolute_orbit,
        'start': slc.start_time.isoformat(),
        'end': slc.end_time.isoformat(),
        'slc': slc.native_id,
        'derivation': 'annotation_v1',
        '_v': 6,  # bump when burst-derivation logic changes
    }

    cached = cache.get("asf_bursts", cache_params)
    if cached is not None:
        if cached:
            return [BurstInfo.from_asf_id(bid) for bid in cached]
        else:
            return []

    # Lazy-init EDL token (sentinel "" means previously failed; don't retry)
    if _edl_token is None:
        try:
            _edl_token = await asyncio.to_thread(get_edl_token)
        except Exception as exc:
            logger.warning(f"EDL token acquisition failed: {exc}")
            _edl_token = ""
            return []
    if _edl_token == "":
        return []

    if get_grq() is not None:
        es = get_grq()
        res = es.search(index=GRQ_SLC_INDEX, body={
            "query": {
                'prefix': {
                    'id.keyword': slc.native_id.removesuffix('-SLC').split('-r')[0]
                }
            }
        })

        if res['hits']['total']['value'] == 1:
            slc_metadata = res['hits']['hits'][0]['_source']['metadata']
            burst_ids = slc_metadata.get('bursts', [])

            if burst_ids:
                burst_ids = [b.lower().removeprefix('t') for b in burst_ids]
                logger.info(f"GRQ indexed bursts for {slc.native_id}: {len(burst_ids)}")
                cache.set("asf_bursts", cache_params, burst_ids)
                return [BurstInfo.from_asf_id(bid) for bid in burst_ids]

    # Fetch SLC metadata (annotation XMLs + manifest.safe) via HTTP range
    # requests — typically ~1 MB total instead of 4-8 GB for the full ZIP.
    try:
        zip_url = await asyncio.to_thread(get_slc_download_url, slc.native_id)
        annotations, manifest_bytes = await asyncio.to_thread(
            extract_slc_metadata, zip_url, _edl_token,
        )
        burst_ids = derive_burst_ids_from_metadata(annotations, manifest_bytes)
    except Exception as exc:
        logger.warning(
            f"Burst derivation failed for {slc.native_id}: {exc}. "
            f"Result NOT cached."
        )
        return []

    if not burst_ids:
        logger.warning(f"No bursts derived for {slc.native_id}. Result NOT cached.")
        return []

    logger.info(
        f"Annotation-derived bursts for {slc.native_id}: {len(burst_ids)}"
    )
    cache.set("asf_bursts", cache_params, burst_ids)
    return [BurstInfo.from_asf_id(bid) for bid in burst_ids]


@cache
def get_grq():
    """Try once to get a GRQ ES connection.

    Imported here, not at module level: opera_commons.es_connection needs hysds, which only exists on
    cluster hosts, and the tool is also run off-cluster (opera-monitoring launchpad) against CMR.
    """
    try:
        from opera_commons.es_connection import get_grq_es
        es = get_grq_es().es
        return es if es.ping() else None
    except Exception:
        return None


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

    The listing is the audit's expected set, so it is only accepted (and cached) when CMR returned
    every granule it counted. Otherwise IncompleteCmrResultsError propagates and nothing is cached.
    """
    logger = logging.getLogger(__name__)
    cache = get_cache()

    all_ids = set()
    all_details = {}

    for platform in ["SENTINEL-1A", "SENTINEL-1B", "SENTINEL-1C", "SENTINEL-1D"]:
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
            # Query CMR; an incomplete listing raises before anything is cached (the chunk loop records it)
            ids, details = await async_get_cmr_granules(
                collection,
                temporal_date_start=start_str,
                temporal_date_end=end_str,
                platform_short_name=platform,
                bounding_box=bbox_str,
                verify_complete=True,
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
    use_grq: bool = False,
) -> ProductLookup:
    """
    Query CMR for OPERA products matching a burst ID and acquisition date.

    When the SLC acquisition spans midnight (slc_end_time falls on a
    different date than acq_time), the search window is extended to cover
    both days so that bursts near the end of the SLC are not missed.

    Returns a ProductLookup: the matching product native-ids and whether the
    answer was complete. Only complete answers are cached.
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
        return ProductLookup(set(cached))

    if use_grq and (es := get_grq()) is not None:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "metadata.acquisition_ts": {
                                    "gte": start_dt.isoformat(),
                                    "lte": end_dt.isoformat()
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {
                                            "metadata.burst_id.keyword": burst.filename_pattern
                                        }
                                    },
                                    {
                                        "term": {
                                            "metadata.Files.burst_id.keyword": burst.filename_pattern
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            }
        }

        # An explicit size: the client default is 10, fewer than one burst and day can legitimately have
        # (polarizations, re-processed revisions). The answer is checked against hits.total like CMR's.
        # Any failure here (opensearch-py connection/transport errors, an unexpected response shape) is a
        # lookup a re-run would fix, so it is reported as unverified rather than allowed to abort the audit;
        # those exception types are not importable off-cluster, hence the broad catch.
        size = 500
        try:
            res = es.search(index=PRODUCT_INDEX_MAP[product_type], body=query, size=size)
            hits = res['hits']['hits']
            total = res['hits'].get('total')
            total = total.get('value') if isinstance(total, dict) else total
            product_ids = set(hit['_id'] for hit in hits)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                f"GRQ lookup failed for burst {burst.filename_pattern}: {exc} - unverified")
            return ProductLookup(set(), False, f"GRQ error: {exc}")
        if total is not None and total > len(hits):
            return ProductLookup(product_ids, False, f"GRQ returned {len(hits)} of {total}")
        return ProductLookup(product_ids)
    else:
        body = (
            f"provider=ASF&short_name[]={short_name}"
            f"&native-id=*{burst.filename_pattern}*"
            "&options[native-id][pattern]=true"
            f"&temporal[]={urllib.parse.quote(start_dt.isoformat(), safe='/:')},{urllib.parse.quote(end_dt.isoformat(), safe='/:')}"
            "&page_size=100"
        )
        logger = logging.getLogger(__name__)

        attempts = 3
        for attempt in range(attempts):
            async with sem:
                try:
                    product_ids, hits, problem, status = await _cmr_product_pages(session, body)
                except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                    if not is_transient_cmr_error(exc):
                        raise
                    product_ids, problem = [], f"CMR network error: {exc}"
                else:
                    if status != 200:  # 429/5xx; anything else raised inside _cmr_product_pages
                        product_ids, problem = [], f"CMR status {status}"

            if problem is None:
                cache.set("cmr_opera", cache_params, product_ids)
                return ProductLookup(set(product_ids))
            if attempt < attempts - 1:
                await asyncio.sleep(0.5 * (2 ** attempt))
                continue
            # Retries exhausted: whatever came back is usable but unverified, and NOT cached, so a
            # transient failure cannot poison the cache with a false-empty answer.
            logger.warning(f"{problem} for burst {burst.filename_pattern} after {attempts} attempts - unverified, NOT caching")
            return ProductLookup(set(product_ids), False, f"{problem} after {attempts} attempts")

    return ProductLookup(set(), False, "no CMR response")


async def _cmr_product_pages(session: aiohttp.ClientSession, body: str) -> tuple[list[str], Optional[int], Optional[str], int]:
    """Posts one product query and follows CMR-Search-After until every counted item is in hand.

    Returns (product_ids, hits, problem, http_status). On a non-200 status the ids gathered so far are
    returned with that status and no problem text; otherwise ``problem`` is the shared cmr_client
    verdict (page_incomplete_reason / shortfall_reason) on why the answer is not complete, or None.
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    product_ids: list[str] = []
    hits = None
    while True:
        async with session.post(
            CMR_GRANULE_URL, data=body, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status != 200:
                if resp.status == 429 or resp.status >= 500:
                    return product_ids, hits, None, resp.status
                # A bad request would repeat on every attempt and every run; surface it like the SLC listing path.
                resp.raise_for_status()
            data = await resp.json()
            page_items = [item["meta"]["native-id"] for item in data.get("items", [])]
            product_ids.extend(page_items)
            if hits is None:
                hits = data.get("hits")
            problem = page_incomplete_reason(data, resp.headers)
            if problem:
                return product_ids, hits, problem, 200
            search_after = resp.headers.get("CMR-Search-After")
        if not page_items or not search_after or (hits is not None and len(product_ids) >= hits):
            break
        headers["CMR-Search-After"] = search_after
    return product_ids, hits, shortfall_reason(hits, len(product_ids)), 200


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
                    product_type: str, found: list, missing: list,
                    slc_count: Optional[int] = None, unverified: Optional[list] = None):
        """Write results for a single time chunk."""
        unverified = unverified or []
        record = {
            "_type": "chunk_result",
            "chunk_start": chunk_start.isoformat(),
            "chunk_end": chunk_end.isoformat(),
            "product_type": product_type,
            "found_count": len(found),
            "missing_count": len(missing),
            "unverified_count": len(unverified),
            "found": found,
            "missing": missing,
            "unverified": unverified,
        }
        if slc_count is not None:
            record["slc_count"] = slc_count
        self._write(record)

    def write_chunk_error(self, chunk_start: datetime, chunk_end: datetime, error: str):
        """Record a chunk that produced no result (e.g. CMR returned an incomplete SLC listing)."""
        self._write({
            "_type": "chunk_error",
            "chunk_start": chunk_start.isoformat(),
            "chunk_end": chunk_end.isoformat(),
            "error": error,
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

    # Derive bursts per SLC (annotation XMLs + manifest.safe via HTTP range)
    primary_pol = polarizations[0] if polarizations else None
    logger.info(f"  Deriving burst IDs from SLC annotations (polarization: {primary_pol})...")

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
    use_grq: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Check CMR for OPERA products matching expected bursts.

    Returns (found_list, missing_list, unverified_list) as dicts ready for JSON output. A burst is
    unverified when its lookup got no complete answer (see ProductLookup) or raised; those need a
    re-run (e.g. --recheck-dates) and must not be read as gaps.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"  Checking {product_type} coverage...")

    # Group by (burst_id, date) to reduce API calls
    groups: dict[tuple, list[ExpectedBurst]] = defaultdict(list)
    for exp in expected_bursts:
        key = (exp.burst.asf_id, exp.acquisition_time.date())
        groups[key].append(exp)

    sem = asyncio.Semaphore(max_concurrent)

    async def check_group(group: list[ExpectedBurst],
                          session: aiohttp.ClientSession) -> tuple[list[dict], list[dict], list[dict]]:
        """Check a group of expected bursts (same burst+date, different polarizations)."""
        burst = group[0].burst
        acq_time = group[0].acquisition_time
        slc_end = group[0].slc_end_time

        lookup = await fetch_opera_products(
            burst, acq_time, product_type, session, sem, slc_end_time=slc_end, use_grq=use_grq
        )

        found, missing, unverified = [], [], []
        for exp in group:
            if product_type == "RTC-S1":
                # RTC produces one granule per burst covering all polarizations.
                # The native-id contains pixel spacing (_30_), not polarization.
                match = next(iter(lookup.products), None)
            else:
                # CSLC produces one granule per polarization — match by pol in name.
                match = next((p for p in lookup.products if exp.polarization in p), None)
            if match:
                found.append({**exp.to_dict(), "opera_product_id": match})
            elif lookup.verified:
                missing.append(exp.to_dict())
            else:
                unverified.append({**exp.to_dict(), "reason": lookup.reason})
        return found, missing, unverified

    # Process in batches for progress reporting
    all_found, all_missing, all_unverified = [], [], []
    group_list = list(groups.values())

    batch_size = 100
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(group_list), batch_size):
            batch = group_list[i:i + batch_size]
            tasks = [check_group(g, session) for g in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for group, result in zip(batch, results):
                if isinstance(result, BaseException):
                    if not is_transient_cmr_error(result):
                        raise result  # a bad request or a bug, not something a re-run would fix
                    # CMR/network trouble: the whole group is unverified, not silently dropped.
                    all_unverified.extend({**exp.to_dict(), "reason": f"lookup error: {result}"} for exp in group)
                    continue
                found, missing, unverified = result
                all_found.extend(found)
                all_missing.extend(missing)
                all_unverified.extend(unverified)

            logger.info(f"    Checked {min(i + batch_size, len(group_list))}/{len(group_list)} groups, "
                       f"found {len(all_found)}, missing {len(all_missing)}, unverified {len(all_unverified)}")

    return all_found, all_missing, all_unverified


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
    use_grq: bool = False,
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
    bbox = geojson_to_bbox(geojson, buffer_deg)  # (west, south, east, north), buffer included
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    logger.info(f"Bounding box (west,south,east,north, {buffer_deg}° buffer): {bbox_str}"
                + (" - crosses the antimeridian" if bbox[0] > bbox[2] else ""))

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
    failed_chunks = []  # chunks with no complete SLC listing from CMR; they need a re-run
    unverified_days = set()  # acquisition dates with bursts whose product lookup got no complete answer
    product_stats = {pt: {"found": 0, "missing": 0, "unverified": 0, "expected": 0} for pt in product_types}
    all_results = {"found": defaultdict(list), "missing": defaultdict(list), "unverified": defaultdict(list)}

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
            try:
                slc_ids, slc_details = await fetch_slc_granules(chunk_start, chunk_end, bbox_str)
            except (IncompleteCmrResultsError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                # No expected set for this chunk (CMR under-returned, or failed after retries): record it
                # and move on rather than audit a partial list or abort the whole run. A permanent
                # failure (4xx: bad bbox, payload too large) would repeat on every chunk, so it surfaces.
                if not isinstance(exc, IncompleteCmrResultsError) and not is_transient_cmr_error(exc):
                    raise
                reason = "incomplete" if isinstance(exc, IncompleteCmrResultsError) else "failed"
                error = f"CMR SLC listing {reason}: {exc}"
                logger.error(f"  Chunk {chunk_idx} skipped, {error}")
                failed_chunks.append({
                    "chunk_start": chunk_start.isoformat(),
                    "chunk_end": chunk_end.isoformat(),
                    "error": error,
                })
                if writer:
                    writer.write_chunk_error(chunk_start, chunk_end, error)
                continue
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
            chunk_slc_count = len(slcs)

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
                found, missing, unverified = await check_coverage_for_bursts(
                    expected_bursts, product_type, use_grq=use_grq)

                product_stats[product_type]["found"] += len(found)
                product_stats[product_type]["missing"] += len(missing)
                product_stats[product_type]["unverified"] += len(unverified)
                product_stats[product_type]["expected"] += len(expected_bursts)
                unverified_days.update(u["acquisition_time"][:10] for u in unverified)

                if low_memory and writer:
                    writer.write_chunk(chunk_start, chunk_end, product_type, found, missing,
                                       slc_count=chunk_slc_count, unverified=unverified)
                else:
                    all_results["found"][product_type].extend(found)
                    all_results["missing"][product_type].extend(missing)
                    all_results["unverified"][product_type].extend(unverified)

                logger.info(f"    {product_type}: {len(found)} found, {len(missing)} missing, "
                            f"{len(unverified)} unverified")

                del found, missing, unverified
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
                "failed_chunks": failed_chunks,
                "unverified_days": sorted(unverified_days),
                "products": {pt: _product_summary(product_stats[pt]) for pt in product_types},
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
            "failed_chunks": failed_chunks,
            "unverified_days": sorted(unverified_days),
        },
        "products": {},
    }

    for pt in product_types:
        results["products"][pt] = _product_summary(product_stats[pt])
        if not low_memory:
            results["products"][pt]["found"] = all_results["found"][pt]
            results["products"][pt]["missing"] = all_results["missing"][pt]
            results["products"][pt]["unverified"] = all_results["unverified"][pt]

    return results


def _product_summary(stats: dict) -> dict:
    """Counts for one product type; coverage is over verified bursts only, unverified ones are reported apart.

    With nothing verified (everything unverified, or nothing expected) coverage is 0: there is no
    evidence of coverage, and the readers (burst_coverage_status) report 0 for the same file.
    """
    verified = stats["found"] + stats["missing"]
    coverage = (stats["found"] / verified * 100) if verified > 0 else 0.0
    return {
        "expected_count": stats["expected"],
        "found_count": stats["found"],
        "missing_count": stats["missing"],
        "unverified_count": stats["unverified"],
        "coverage_percent": round(coverage, 2),
    }


def exit_code_for(results: dict, zero_on_missing=False) -> int:
    """2 = audit incomplete (chunks skipped or bursts unverified: re-run those dates), 1 = missing products, 0 = clean."""
    meta = results["metadata"]
    if meta.get("failed_chunks") or meta.get("unverified_days"):
        return 2
    has_missing = any(c["missing_count"] > 0 for c in results["products"].values())
    return 1 if has_missing and not zero_on_missing else 0


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

    failed = meta.get("failed_chunks") or []
    if failed:
        print(f"Chunks skipped (incomplete CMR SLC listing, re-run these dates): {len(failed)}")
        for fc in failed[:10]:
            print(f"  {fc['chunk_start'][:10]} to {fc['chunk_end'][:10]}: {fc['error']}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")

    unverified_days = meta.get("unverified_days") or []
    if unverified_days:
        print(f"Dates with unverified bursts (CMR gave no complete answer, re-run with --recheck-dates): "
              f"{','.join(unverified_days)}")

    print("-" * 70)

    for product_type, coverage in results["products"].items():
        print(f"\n{product_type}:")
        print(f"  Expected:   {coverage['expected_count']:,}")
        print(f"  Found:      {coverage['found_count']:,}")
        print(f"  Missing:    {coverage['missing_count']:,}")
        print(f"  Unverified: {coverage.get('unverified_count', 0):,}")
        print(f"  Coverage:   {coverage['coverage_percent']:.1f}% (of verified bursts)")

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
                             "Bypasses the cmr_opera cache for these dates and "
                             "the cmr_slc cache for listings that cover them, "
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

    parser.add_argument('--coverage-target', choices=['CMR', 'GRQ'],
                        default='CMR',
                        help="Source of RTC granules to search for coverage. (Default: CMR)")

    # Misc option
    parser.add_argument('--zero-on-missing', action='store_true', dest='zero',
                        help='Exit zero even if missing products are found')

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
        use_grq=args.coverage_target == 'GRQ',
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
                    for label, key in (("Missing", "missing"), ("Unverified", "unverified")):
                        if key == "unverified" and not cov.get(key):
                            continue
                        f.write(f"# {pt} {label} Products\n")
                        for item in cov.get(key, []):
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

    failed_chunks = results["metadata"].get("failed_chunks") or []
    if failed_chunks:
        logger.error(f"{len(failed_chunks)} chunk(s) skipped because CMR returned an incomplete SLC listing; "
                     "re-run those dates (e.g. with --recheck-dates).")
    unverified_days = results["metadata"].get("unverified_days") or []
    if unverified_days:
        logger.error(f"Bursts on {len(unverified_days)} date(s) could not be verified against CMR: "
                     f"{','.join(unverified_days)}; re-run them with --recheck-dates.")
    return exit_code_for(results, zero_on_missing=args.zero)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
