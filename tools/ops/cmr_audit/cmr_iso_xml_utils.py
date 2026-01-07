"""
Shared utilities for fetching and parsing ISO XML metadata from CMR.

This module provides common functions for extracting input granule information
from ISO XML metadata files associated with OPERA products in CMR.

These utilities can be used by various CMR audit tools to retrieve lineage
information when GRQ Elasticsearch is not available.

Supports optional file-based caching to avoid re-downloading ISO XML files
across multiple runs.
"""

import hashlib
import logging
import os
import time
import random
import threading
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
import xml.etree.ElementTree as ET

# Default retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds

# HTTP status codes that should trigger a retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Thread-local storage for session reuse
_thread_local = threading.local()

# Cache configuration (module-level state)
_cache_config = {
    'enabled': False,
    'cache_dir': None,
    'hits': 0,
    'misses': 0
}
_cache_lock = threading.Lock()


def configure_iso_xml_cache(cache_dir=None, enabled=True):
    """
    Configure ISO XML file caching.

    Args:
        cache_dir: Directory path for cache files. If None, caching is disabled.
        enabled: Whether caching is enabled (default: True if cache_dir is provided)

    Returns:
        Dict with cache configuration status
    """
    global _cache_config

    with _cache_lock:
        if cache_dir:
            cache_path = Path(cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)
            _cache_config['cache_dir'] = cache_path
            _cache_config['enabled'] = enabled
            _cache_config['hits'] = 0
            _cache_config['misses'] = 0
            logging.info(f"ISO XML cache enabled at: {cache_path}")
        else:
            _cache_config['enabled'] = False
            _cache_config['cache_dir'] = None
            logging.debug("ISO XML cache disabled")

        return {
            'enabled': _cache_config['enabled'],
            'cache_dir': str(_cache_config['cache_dir']) if _cache_config['cache_dir'] else None
        }


def get_cache_stats():
    """
    Get cache hit/miss statistics.

    Returns:
        Dict with 'hits', 'misses', and 'hit_rate' keys
    """
    with _cache_lock:
        hits = _cache_config['hits']
        misses = _cache_config['misses']
        total = hits + misses
        hit_rate = (hits / total * 100) if total > 0 else 0
        return {
            'hits': hits,
            'misses': misses,
            'total': total,
            'hit_rate': hit_rate
        }


def _get_cache_key(url):
    """
    Generate a cache key (filename) from a URL.

    Uses SHA256 hash of the URL to create a safe filename.
    Also extracts the product ID from the URL for human readability.

    Args:
        url: ISO XML URL

    Returns:
        Cache filename string
    """
    # Extract product ID from URL for readability (e.g., OPERA_L3_DISP-S1_...)
    # URL format: https://.../OPERA_L3_DISP-S1_IW_F12345_VV_..._v1.0_....iso.xml
    url_parts = url.split('/')
    filename = url_parts[-1] if url_parts else ''

    # Create a short hash for uniqueness
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:12]

    # Combine product name (if found) with hash
    if filename.endswith('.iso.xml'):
        # Remove .iso.xml extension for cleaner cache name
        base_name = filename[:-8]
        cache_key = f"{base_name}_{url_hash}.xml"
    else:
        cache_key = f"{url_hash}.xml"

    return cache_key


def _get_from_cache(url):
    """
    Try to retrieve ISO XML content from cache.

    Args:
        url: ISO XML URL

    Returns:
        Cached XML content as string, or None if not cached
    """
    if not _cache_config['enabled'] or not _cache_config['cache_dir']:
        return None

    cache_key = _get_cache_key(url)
    cache_path = _cache_config['cache_dir'] / cache_key

    try:
        if cache_path.exists():
            with open(cache_path, 'r', encoding='utf-8') as f:
                content = f.read()
            with _cache_lock:
                _cache_config['hits'] += 1
            logging.debug(f"Cache hit for {url}")
            return content
    except Exception as e:
        logging.debug(f"Cache read error for {url}: {e}")

    return None


def _save_to_cache(url, content):
    """
    Save ISO XML content to cache.

    Args:
        url: ISO XML URL
        content: XML content as string
    """
    if not _cache_config['enabled'] or not _cache_config['cache_dir']:
        return

    if content is None:
        return

    cache_key = _get_cache_key(url)
    cache_path = _cache_config['cache_dir'] / cache_key

    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.debug(f"Cached ISO XML for {url}")
    except Exception as e:
        logging.debug(f"Cache write error for {url}: {e}")


def _get_session():
    """
    Get a thread-local requests session with connection pooling.
    Reusing sessions significantly improves performance for multiple requests
    to the same host by keeping connections alive.
    """
    if not hasattr(_thread_local, 'session'):
        session = requests.Session()
        # Configure adapter with connection pooling
        # pool_connections: number of different hosts to maintain pools for
        # pool_maxsize: max connections per host pool
        adapter = HTTPAdapter(
            pool_connections=20,  # Cache pools for up to 20 different hosts
            pool_maxsize=20,  # Up to 20 connections per host
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        _thread_local.session = session
    return _thread_local.session


def _calculate_backoff_delay(attempt, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """
    Calculate exponential backoff delay with jitter.

    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Delay in seconds
    """
    # Exponential backoff: base_delay * 2^attempt
    delay = base_delay * (2 ** attempt)
    # Add jitter (random factor between 0.5 and 1.5)
    delay = delay * (0.5 + random.random())
    # Cap at max_delay
    return min(delay, max_delay)


def fetch_iso_xml(iso_xml_url, timeout=30, max_retries=DEFAULT_MAX_RETRIES,
                  base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """
    Fetch ISO XML content from a URL with exponential backoff retry.

    Uses a thread-local session with connection pooling for better performance
    when making many requests to the same hosts.

    If caching is enabled via configure_iso_xml_cache(), will check cache first
    and save fetched content to cache.

    Args:
        iso_xml_url: URL to the ISO XML metadata file
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay for exponential backoff in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 30.0)

    Returns:
        XML content as string, or None if fetch fails after all retries
    """
    # Check cache first
    cached_content = _get_from_cache(iso_xml_url)
    if cached_content is not None:
        return cached_content

    # Track cache miss
    with _cache_lock:
        _cache_config['misses'] += 1

    session = _get_session()
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = session.get(iso_xml_url, timeout=timeout)
            response.raise_for_status()
            content = response.text
            # Save to cache on successful fetch
            _save_to_cache(iso_xml_url, content)
            return content

        except requests.exceptions.HTTPError as e:
            last_exception = e
            status_code = e.response.status_code if e.response is not None else None

            if status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                delay = _calculate_backoff_delay(attempt, base_delay, max_delay)
                logging.debug(f"Retryable HTTP {status_code} error fetching {iso_xml_url}, "
                             f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(delay)
                continue
            else:
                logging.warning(f"Failed to fetch ISO XML from {iso_xml_url}: {e}")
                return None

        except requests.exceptions.RequestException as e:
            last_exception = e
            # Retry on connection errors, timeouts, etc.
            if attempt < max_retries:
                delay = _calculate_backoff_delay(attempt, base_delay, max_delay)
                logging.debug(f"Request error fetching {iso_xml_url}: {e}, "
                             f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(delay)
                continue
            else:
                logging.warning(f"Failed to fetch ISO XML from {iso_xml_url} after {max_retries + 1} attempts: {e}")
                return None

    logging.warning(f"Failed to fetch ISO XML from {iso_xml_url} after {max_retries + 1} attempts: {last_exception}")
    return None


def parse_iso_xml_input_granules(xml_content, filename_filter=None):
    """
    Parse ISO XML content and extract input granule filenames from FileName elements.

    Args:
        xml_content: XML content as string
        filename_filter: Optional callable that takes a filename string and returns
                        True if it should be included, False otherwise.
                        If None, all FileName elements are returned.

    Returns:
        List of input granule filenames, or empty list if parsing fails
    """
    if xml_content is None:
        return []

    try:
        root = ET.fromstring(xml_content)

        # Look for FileName elements which contain input granule info
        # The namespace-agnostic approach works better for varying ISO XML formats
        input_granules = []
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if tag_name == 'FileName' and elem.text:
                text = elem.text.strip()
                if filename_filter is None or filename_filter(text):
                    input_granules.append(text)

        return input_granules

    except ET.ParseError as e:
        logging.warning(f"Failed to parse ISO XML: {e}")
        return []


def fetch_and_parse_iso_xml_input_granules(iso_xml_url, filename_filter=None, timeout=60):
    """
    Fetch ISO XML from URL and extract input granule filenames.

    This is a convenience function that combines fetch_iso_xml and
    parse_iso_xml_input_granules.

    Args:
        iso_xml_url: URL to the ISO XML metadata file
        filename_filter: Optional callable that takes a filename string and returns
                        True if it should be included, False otherwise.
        timeout: Request timeout in seconds (default: 60)

    Returns:
        List of input granule filenames, or empty list if fetch/parse fails
    """
    xml_content = fetch_iso_xml(iso_xml_url, timeout=timeout)
    return parse_iso_xml_input_granules(xml_content, filename_filter=filename_filter)


def get_iso_xml_url_from_umm(umm_obj):
    """
    Extract the ISO XML URL from a CMR UMM object.

    Args:
        umm_obj: A CMR UMM object (dict with 'umm' key)

    Returns:
        ISO XML URL string, or None if not found
    """
    umm = umm_obj.get("umm", {})
    related_urls = umm.get("RelatedUrls", [])

    for url_info in related_urls:
        url = url_info.get("URL", "")
        # ISO XML files end with .iso.xml
        if url.endswith(".iso.xml"):
            return url

    return None


def cslc_filename_filter(filename):
    """
    Filter function for CSLC input granules.

    Returns True for CSLC files that are not STATIC or COMPRESSED.

    Args:
        filename: Filename string to check

    Returns:
        True if filename represents a regular CSLC file, False otherwise
    """
    return 'CSLC' in filename and 'STATIC' not in filename and 'COMPRESSED' not in filename


def normalize_cslc_filename(filename):
    """
    Normalize a CSLC filename by removing common extensions.

    Args:
        filename: CSLC filename (may include .h5 extension)

    Returns:
        Normalized filename without .h5 extension
    """
    if filename.endswith('.h5'):
        return filename[:-3]
    return filename


def fetch_cslc_input_granules_from_iso_xml(iso_xml_url, timeout=60):
    """
    Fetch ISO XML and extract CSLC input granule IDs (without .h5 extension).

    This is a specialized function for extracting CSLC lineage from DISP-S1
    or similar products that use CSLCs as input.

    Args:
        iso_xml_url: URL to the ISO XML metadata file
        timeout: Request timeout in seconds (default: 60)

    Returns:
        List of CSLC granule IDs (without .h5 extension), or empty list if fetch/parse fails
    """
    granules = fetch_and_parse_iso_xml_input_granules(
        iso_xml_url,
        filename_filter=cslc_filename_filter,
        timeout=timeout
    )
    return [normalize_cslc_filename(g) for g in granules]


def extract_all_related_urls(umm_obj):
    """
    Extract all RelatedUrls from a CMR UMM object.

    Args:
        umm_obj: A CMR UMM object (dict with 'umm' key)

    Returns:
        List of dicts containing URL info (Type, Subtype, URL)
    """
    umm = umm_obj.get("umm", {})
    return umm.get("RelatedUrls", [])


def get_data_urls_from_umm(umm_obj, url_type="GET DATA"):
    """
    Extract data download URLs from a CMR UMM object.

    Args:
        umm_obj: A CMR UMM object (dict with 'umm' key)
        url_type: Type of URL to filter for (default: "GET DATA")

    Returns:
        List of URL strings matching the specified type
    """
    related_urls = extract_all_related_urls(umm_obj)
    return [
        url_info.get("URL", "")
        for url_info in related_urls
        if url_info.get("Type") == url_type
    ]
