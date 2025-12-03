"""
Shared utilities for fetching and parsing ISO XML metadata from CMR.

This module provides common functions for extracting input granule information
from ISO XML metadata files associated with OPERA products in CMR.

These utilities can be used by various CMR audit tools to retrieve lineage
information when GRQ Elasticsearch is not available.
"""

import logging
import requests
import xml.etree.ElementTree as ET


def fetch_iso_xml(iso_xml_url, timeout=60):
    """
    Fetch ISO XML content from a URL.

    Args:
        iso_xml_url: URL to the ISO XML metadata file
        timeout: Request timeout in seconds (default: 60)

    Returns:
        XML content as string, or None if fetch fails
    """
    try:
        response = requests.get(iso_xml_url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to fetch ISO XML from {iso_xml_url}: {e}")
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
