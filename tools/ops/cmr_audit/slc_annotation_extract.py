#!/usr/bin/env python3
"""Extract annotation XMLs from Sentinel-1 SLC ZIP files via HTTP range requests.

Determines the **authoritative burst count** for a Sentinel-1 IW SLC by
reading the ``<burstList>`` elements in ESA's annotation XML files, which
are embedded inside the SLC ZIP archive hosted at ASF.

Instead of downloading the entire multi-GB ZIP, this tool uses HTTP Range
requests to fetch only the ZIP central directory and the annotation XML
entries (typically ~0.6–1.4 MB total).

Prerequisites
-------------
* A ``~/.netrc`` entry for ``urs.earthdata.nasa.gov``::

      machine urs.earthdata.nasa.gov login <EDL_USER> password <EDL_PASS>

* The ``requests`` Python package (already a project dependency).

Usage
-----
Basic — report burst counts per subswath::

    python slc_annotation_extract.py \\
        S1A_IW_SLC__1SDV_20170227T133921_20170227T133934_015470_019672_103F-SLC

Save the raw annotation XMLs locally for inspection::

    python slc_annotation_extract.py --save-xml -v \\
        S1B_IW_SLC__1SDV_20170228T031845_20170228T031915_004495_007D2E_A508-SLC

Example output::

    Burst counts for S1B_IW_SLC__1SDV_20170228T031845_20170228T031915_004495_007D2E_A508-SLC:
    ------------------------------------------------------------
      IW1 VH: 10 bursts
      IW1 VV: 10 bursts
      IW2 VH: 10 bursts
      IW2 VV: 10 bursts
      IW3 VH: 10 bursts
      IW3 VV: 10 bursts
    ------------------------------------------------------------
      Total (VV): 30 bursts across 3 subswaths

How it works
------------
1. **Resolve download URL** — queries CMR for the SLC granule and extracts
   the HTTPS ``.zip`` URL from ``RelatedUrls``.
2. **Authenticate** — obtains an EarthData Login (EDL) Bearer token via
   ``~/.netrc`` credentials.
3. **Remote ZIP extraction** — uses ``HTTPRangeFile``, a file-like object
   backed by HTTP Range requests, which is passed directly to Python's
   ``zipfile.ZipFile``.  This reads only the End-of-Central-Directory
   record and the Central Directory (~10 KB from the end of the file),
   then fetches individual annotation XML entries by offset.  The ASF
   download URL redirects through EDL to a CloudFront signed URL; the
   redirect chain is resolved once and the signed URL is reused for all
   subsequent Range GETs.
4. **Parse annotations** — each IW-mode annotation XML contains a
   ``<burstList count="N">`` element giving the burst count for that
   subswath and polarization.  Counts are aggregated and reported.

Motivation
----------
ASF's SLC-BURST API can intermittently return **partial responses** (fewer
bursts than actually exist in the SLC) while still returning HTTP 200.
This tool provides ground-truth burst counts from ESA's own metadata,
enabling verification of ASF API results without downloading the full SLC.
"""

import argparse
import io
import logging
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Extract annotation XMLs from a Sentinel-1 SLC ZIP hosted at ASF "
            "using HTTP range requests (no full download)."
        ),
    )
    parser.add_argument(
        "slc_native_id",
        help=(
            "SLC native ID, e.g. "
            "S1A_IW_SLC__1SDV_20170227T133921_20170227T133934_015470_019672_103F-SLC"
        ),
    )
    parser.add_argument(
        "--edl-endpoint",
        default="urs.earthdata.nasa.gov",
        help="EarthData Login endpoint (default: %(default)s)",
    )
    parser.add_argument(
        "--save-xml",
        action="store_true",
        help="Save extracted annotation XMLs to the current directory",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging",
    )
    return parser


# ---------------------------------------------------------------------------
# 1. Resolve SLC download URL via CMR
# ---------------------------------------------------------------------------

CMR_GRANULE_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"


def get_slc_download_url(slc_native_id: str) -> str:
    """Query CMR for the SLC granule and return its HTTPS download URL."""
    body = f"provider=ASF&native_id={slc_native_id}&page_size=1"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(CMR_GRANULE_URL, data=body, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    if data.get("hits", 0) == 0:
        raise ValueError(f"No granule found in CMR for native_id: {slc_native_id}")

    item = data["items"][0]
    related_urls = item["umm"].get("RelatedUrls", [])
    for ru in related_urls:
        url = ru.get("URL", "")
        if url.startswith("https://") and url.endswith(".zip"):
            return url

    # Fallback: any HTTPS URL containing the product name
    product_name = slc_native_id.rsplit("-", 1)[0]  # strip "-SLC" suffix
    for ru in related_urls:
        url = ru.get("URL", "")
        if "https://" in url and product_name in url:
            return url

    raise ValueError(
        f"Could not find HTTPS download URL in CMR response for {slc_native_id}. "
        f"RelatedUrls: {[ru.get('URL') for ru in related_urls]}"
    )


# ---------------------------------------------------------------------------
# 2. EarthData Login authentication
# ---------------------------------------------------------------------------

def _parse_netrc(machine: str) -> tuple[str, str] | None:
    """Parse ~/.netrc for *machine*, tolerating malformed macdef blocks."""
    netrc_path = pathlib.Path.home() / ".netrc"
    if not netrc_path.exists():
        return None
    text = netrc_path.read_text()
    pattern = rf"machine\s+{re.escape(machine)}\s+login\s+(\S+)\s+password\s+(\S+)"
    m = re.search(pattern, text)
    if m:
        return m.group(1), m.group(2)
    return None


def get_edl_token(edl_endpoint: str = "urs.earthdata.nasa.gov") -> str:
    """Obtain an EDL Bearer token using ~/.netrc credentials."""
    from requests.auth import HTTPBasicAuth

    creds = _parse_netrc(edl_endpoint)
    if creds is None:
        raise SystemExit(
            f"ERROR: No entry for {edl_endpoint} in ~/.netrc. "
            f"Add: machine {edl_endpoint} login <user> password <pass>"
        )
    username, password = creds
    auth = HTTPBasicAuth(username, password)

    # Check for existing tokens
    resp = requests.get(f"https://{edl_endpoint}/api/users/tokens", auth=auth)
    resp.raise_for_status()
    tokens = resp.json()
    if tokens:
        return tokens[0]["access_token"]

    # Create a new token
    resp = requests.post(f"https://{edl_endpoint}/api/users/token", auth=auth)
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# 3. HTTP Range-request file-like wrapper for remote ZIP reading
# ---------------------------------------------------------------------------

class HTTPRangeFile:
    """File-like object backed by HTTP Range requests.

    Implements read/seek/tell so that ``zipfile.ZipFile`` can open a
    remote ZIP without downloading the entire file.
    """

    def __init__(self, url: str, token: str):
        self.url = url
        self._pos = 0
        self._request_count = 0
        self._bytes_downloaded = 0

        # ASF datapool URLs redirect:
        #   datapool.asf → 307 → sentinel1.asf → 302 → EDL → sentinel1 → CloudFront
        # The final CloudFront URL is a signed URL (query-string auth).
        # We resolve the chain using the same two-step approach as the
        # existing download code, then use a plain session for Range GETs.
        self._resolved_url = self._resolve_redirect(url, token)
        self._range_session = requests.Session()

        # Discover file size via a small Range GET (CloudFront may reject HEAD).
        probe = self._range_session.get(
            self._resolved_url, headers={"Range": "bytes=-1"}
        )
        if probe.status_code == 206:
            cr = probe.headers.get("Content-Range", "")
            if "/" in cr:
                self._size = int(cr.rsplit("/", 1)[1])
            else:
                raise IOError(f"Unexpected Content-Range header: {cr}")
        elif probe.status_code == 200:
            self._size = int(probe.headers["Content-Length"])
            logger.warning(
                "Server ignored Range header; reads will download full content"
            )
        else:
            raise IOError(f"Probe request failed with HTTP {probe.status_code}")

        self._request_count += 1
        self._bytes_downloaded += len(probe.content)
        logger.debug("Remote ZIP: %s bytes (%.1f GB)", self._size, self._size / 1e9)

    @staticmethod
    def _resolve_redirect(url: str, token: str) -> str:
        """Follow ASF → EDL → CloudFront redirect chain.

        Uses the same two-step approach as the existing download code:
        1. GET initial URL without following redirects → get Location
        2. GET Location with Bearer token, following all subsequent redirects
        """
        # Step 1: get first redirect
        r1 = requests.get(url, allow_redirects=False)
        if r1.status_code not in (301, 302, 303, 307, 308):
            raise IOError(f"Expected redirect from {url}, got HTTP {r1.status_code}")
        location = r1.headers["Location"]
        logger.debug("Redirect step 1: %s -> %s", url, location)

        # Step 2: follow with Bearer token (handles EDL OAuth + CloudFront)
        headers = {"Authorization": f"Bearer {token}"}
        r2 = requests.get(location, headers=headers, allow_redirects=True, stream=True)
        r2.close()
        r2.raise_for_status()
        logger.debug("Resolved URL: %s", r2.url)
        return r2.url

    @property
    def size(self) -> int:
        return self._size

    # -- file-like interface ------------------------------------------------

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            size = self._size - self._pos

        start = self._pos
        end = min(start + size - 1, self._size - 1)
        if start > end:
            return b""

        headers = {"Range": f"bytes={start}-{end}"}
        resp = self._range_session.get(self._resolved_url, headers=headers)

        if resp.status_code == 200:
            raise IOError(
                "Server returned 200 instead of 206 Partial Content. "
                "Range requests may not be supported for this URL."
            )
        if resp.status_code != 206:
            raise IOError(
                f"HTTP {resp.status_code} for Range bytes={start}-{end}"
            )

        data = resp.content
        self._pos += len(data)
        self._request_count += 1
        self._bytes_downloaded += len(data)
        return data


# ---------------------------------------------------------------------------
# 4. Extract annotation XMLs from remote ZIP
# ---------------------------------------------------------------------------

def extract_annotations(zip_url: str, token: str) -> dict[str, bytes]:
    """Extract annotation XML files from a remote SLC ZIP via range requests.

    Returns a dict mapping ZIP entry path → XML bytes.
    """
    remote = HTTPRangeFile(zip_url, token)

    annotations: dict[str, bytes] = {}
    with zipfile.ZipFile(remote) as zf:
        for entry in zf.namelist():
            if (
                "/annotation/" in entry
                and entry.endswith(".xml")
                and "/calibration/" not in entry
            ):
                logger.debug("Extracting: %s", entry)
                annotations[entry] = zf.read(entry)

    logger.info(
        "Extracted %d annotation XMLs (%d HTTP requests, %.1f KB downloaded)",
        len(annotations),
        remote._request_count,
        remote._bytes_downloaded / 1024,
    )
    return annotations


# ---------------------------------------------------------------------------
# 5. Parse burst information from annotation XML
# ---------------------------------------------------------------------------

def parse_burst_count(xml_bytes: bytes) -> int:
    """Return burst count from the <burstList count="N"> element."""
    root = ET.fromstring(xml_bytes)
    burst_list = root.find(".//{*}burstList")
    if burst_list is None:
        burst_list = root.find(".//burstList")
    if burst_list is not None:
        return int(burst_list.get("count", "0"))
    return 0


def parse_burst_anx_times(annotations: dict[str, bytes]) -> dict[str, list[float]]:
    """Parse azimuthAnxTime for each burst, keyed by subswath.

    Only processes one polarization per subswath (VV preferred) since burst
    structure is identical across polarizations.

    Returns e.g. {"IW1": [2126.26, 2129.02, 2131.78, 2134.53], ...}
    """
    # First pass: collect available (subswath, polarization) pairs
    entries: dict[str, dict[str, str]] = {}  # subswath -> {pol -> path}
    for path in sorted(annotations):
        filename = path.rsplit("/", 1)[-1]
        parts = filename.split("-")
        subswath = parts[1].upper()
        polarization = parts[3].upper()
        entries.setdefault(subswath, {})[polarization] = path

    # Second pass: parse one polarization per subswath (prefer VV)
    result: dict[str, list[float]] = {}
    for subswath in sorted(entries):
        pols = entries[subswath]
        chosen_path = pols.get("VV") or next(iter(pols.values()))
        xml_bytes = annotations[chosen_path]
        root = ET.fromstring(xml_bytes)

        burst_list = root.find(".//{*}burstList")
        if burst_list is None:
            burst_list = root.find(".//burstList")
        if burst_list is None:
            continue

        anx_times = []
        for burst_el in burst_list:
            if burst_el.tag.endswith("burst") or burst_el.tag == "burst":
                anx_el = burst_el.find("{*}azimuthAnxTime")
                if anx_el is None:
                    anx_el = burst_el.find("azimuthAnxTime")
                if anx_el is not None and anx_el.text:
                    anx_times.append(float(anx_el.text))
        if anx_times:
            result[subswath] = anx_times

    return result


def derive_burst_ids(
    anx_times: dict[str, list[float]],
    track: int,
    reference_burst_num: int,
    reference_anx_time: float,
    reference_subswath: str,
) -> list[str]:
    """Derive complete burst IDs from annotation ANX times + one known ASF burst.

    Algorithm:
    1. Compute T_cycle from consecutive ANX times in the reference subswath.
    2. Derive burst_nums for the reference subswath via:
           offset = reference_burst_num - floor(reference_anx_time / T_cycle)
           burst_num_i = floor(anx_time_i / T_cycle) + offset
    3. Assign the same burst_nums positionally to other subswaths.
       (All subswaths share the same burst_num at each position index.)

    This avoids cross-subswath rounding errors from applying floor() to
    ANX times in different subswaths, where the ~0.9–1.9 s intra-row
    offset can push floor() across a T_cycle boundary.

    Returns list of ASF-format burst IDs (e.g. ["173_370215_IW1", ...]).
    """
    import math

    # Compute T_cycle from two consecutive ANX times in the reference subswath
    ref_times = anx_times.get(reference_subswath)
    if not ref_times or len(ref_times) < 2:
        raise ValueError(
            f"Need at least 2 bursts in {reference_subswath} to compute T_cycle, "
            f"got {len(ref_times) if ref_times else 0}"
        )
    t_cycle = ref_times[1] - ref_times[0]
    if t_cycle <= 0:
        raise ValueError(f"Invalid T_cycle={t_cycle} from {reference_subswath} ANX times")

    # Compute offset using the known (burst_num, anx_time) pair
    offset = reference_burst_num - math.floor(reference_anx_time / t_cycle)

    # Derive burst_nums for the reference subswath
    ref_burst_nums = [
        math.floor(t / t_cycle) + offset for t in ref_times
    ]

    # Assign the same burst_nums positionally to all subswaths
    burst_ids = []
    for subswath in sorted(anx_times):
        sw_times = anx_times[subswath]
        if len(sw_times) == len(ref_burst_nums):
            # Same number of bursts — use positional alignment
            for i, burst_num in enumerate(ref_burst_nums):
                burst_ids.append(f"{track:03d}_{burst_num:06d}_{subswath}")
        else:
            # Different burst count — fall back to floor formula for this subswath
            for anx_time in sw_times:
                burst_num = math.floor(anx_time / t_cycle) + offset
                burst_ids.append(f"{track:03d}_{burst_num:06d}_{subswath}")

    return burst_ids


def analyze_annotations(annotations: dict[str, bytes]) -> list[dict]:
    """Parse all annotation XMLs and return per-subswath burst info."""
    results = []
    for path in sorted(annotations):
        filename = path.rsplit("/", 1)[-1]
        parts = filename.split("-")
        subswath = parts[1].upper()
        polarization = parts[3].upper()
        burst_count = parse_burst_count(annotations[path])
        results.append({
            "subswath": subswath,
            "polarization": polarization,
            "burst_count": burst_count,
            "filename": filename,
        })
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = create_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(message)s",
    )

    # 1. Resolve download URL
    logger.info("Looking up SLC: %s", args.slc_native_id)
    zip_url = get_slc_download_url(args.slc_native_id)
    logger.info("Download URL: %s", zip_url)

    # 2. Authenticate
    token = get_edl_token(args.edl_endpoint)
    logger.debug("EDL token acquired")

    # 3. Extract annotation XMLs via range requests
    logger.info("Reading ZIP central directory via HTTP range requests...")
    annotations = extract_annotations(zip_url, token)

    if not annotations:
        logger.error("No annotation XML files found in ZIP")
        sys.exit(1)

    # 4. Parse and report
    results = analyze_annotations(annotations)

    total_vv = 0
    print(f"\nBurst counts for {args.slc_native_id}:")
    print("-" * 60)
    for r in results:
        print(f"  {r['subswath']} {r['polarization']}: {r['burst_count']} bursts")
        if r["polarization"] == "VV":
            total_vv += r["burst_count"]
    print("-" * 60)
    vv_count = sum(1 for r in results if r["polarization"] == "VV")
    print(f"  Total (VV): {total_vv} bursts across {vv_count} subswaths")

    # 5. Optionally save XMLs
    if args.save_xml:
        for path, xml_bytes in annotations.items():
            filename = path.rsplit("/", 1)[-1]
            with open(filename, "wb") as f:
                f.write(xml_bytes)
            print(f"  Saved: {filename}")


if __name__ == "__main__":
    main()
