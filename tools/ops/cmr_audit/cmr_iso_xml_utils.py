"""
Shared utilities for fetching and parsing ISO XML metadata from CMR.

This module provides common functions for extracting input granule information
from ISO XML metadata files associated with OPERA products in CMR.

These utilities can be used by various CMR audit tools to retrieve lineage
information when GRQ Elasticsearch is not available.
"""

import logging
import time
import random
import requests
import xml.etree.ElementTree as ET

# Default retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds

# HTTP status codes that should trigger a retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


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


def fetch_iso_xml(iso_xml_url, timeout=60, max_retries=DEFAULT_MAX_RETRIES,
                  base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """
    Fetch ISO XML content from a URL with exponential backoff retry.

    Args:
        iso_xml_url: URL to the ISO XML metadata file
        timeout: Request timeout in seconds (default: 60)
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay for exponential backoff in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 30.0)

    Returns:
        XML content as string, or None if fetch fails after all retries
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(iso_xml_url, timeout=timeout)
            response.raise_for_status()
            return response.text

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
