#!/usr/bin/env python3
"""
End-to-End Pipeline:
1. Query NASA CMR (ASF) for OPERA RTC-S1 granules derived from Sentinel-1 SLC inputs.
2. Extract OPERA Burst IDs (e.g. T064-135225-IW1) directly from confirmed granules.
3. Download the MGRS burst lookup table from AWS S3 (if not present locally).
4. Map extracted Burst IDs to MGRS Tile Acquisition Groups (e.g. 37NBB_2).

Usage:
    python slc_to_mgrs_tiles.py --input slc_names.txt [OPTIONS]
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Optional imports handled gracefully
try:
    import boto3
    import pandas as pd
except ImportError:
    print("[ERROR] 'boto3' and 'pandas' (with pyarrow/fastparquet) are required.")
    print("        Install them via: pip install boto3 pandas pyarrow")
    sys.exit(1)


# ── Configuration Defaults ───────────────────────────────────────────────────

CMR_BASE_URL  = "https://cmr.earthdata.nasa.gov/search"
CMR_PROVIDER  = "ASF"
RTC_SHORTNAME = "OPERA_L2_RTC-S1_V1"

PAGE_SIZE     = 200    # granules per CMR page
REQUEST_DELAY = 0.25   # seconds between CMR requests

# S3 Ancillaries Lookup Configuration
S3_BUCKET = "opera-ancillaries"
S3_KEY = "dist_s1/mgrs_burst_lookup_table_2025-11-19.parquet"
LOCAL_PARQUET_PATH = "mgrs_burst_lookup_table_2025-11-19.parquet"


# ── Step 1 Helpers: CMR Search & SLC Parsing ────────────────────────────────

def cmr_get_umm(params: dict, debug: bool = False, timeout: int = 60) -> dict:
    """GET /granules.umm_json and return parsed JSON."""
    query = urllib.parse.urlencode(params, doseq=True)
    url   = f"{CMR_BASE_URL}/granules.umm_json?{query}"
    if debug:
        print(f"\n  [DEBUG] {url}")
    req = urllib.request.Request(
        url,
        headers={
            "Accept":     "application/json",
            "User-Agent": "rtc-slc-provenance-query/5.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


_SLC_RE = re.compile(
    r"^(?P<platform>S1[ABC])_IW_SLC__1S[DH]V_"
    r"(?P<start>\d{8}T\d{6})_"
    r"(?P<stop>\d{8}T\d{6})_"
    r"(?P<orbit>\d{6})_"
    r"(?P<dtake>[0-9A-Fa-f]{6})_"
    r"(?P<checksum>[0-9A-Fa-f]{4})"
    r"(?:-\S+)?$"
)

def parse_slc(name: str) -> dict:
    m = _SLC_RE.match(name.strip())
    if not m:
        raise ValueError(f"Cannot parse SLC name: {name!r}")
    fmt = "%Y%m%dT%H%M%S"
    return {
        "platform":  m.group("platform").upper(),
        "start_str": m.group("start"),
        "stop_str":  m.group("stop"),
        "start_dt":  datetime.strptime(m.group("start"), fmt).replace(tzinfo=timezone.utc),
        "stop_dt":   datetime.strptime(m.group("stop"),  fmt).replace(tzinfo=timezone.utc),
    }


def build_native_id_pattern(parsed: dict) -> str:
    date_hour = parsed["start_str"][:11]   # YYYYMMDDTHH
    return f"OPERA_L2_RTC-S1_*_{date_hour}*_*_S1?_30_v1.0*"


def normalise_slc_name(raw: str) -> str:
    name = raw.strip()
    return re.sub(r"-[^_-]+$", "", name)


def search_rtc_for_slc(
    slc_name: str,
    debug: bool = False,
) -> tuple[list[dict], list[dict]]:
    parsed   = parse_slc(slc_name)
    scene_id = normalise_slc_name(slc_name)

    native_id = build_native_id_pattern(parsed)
    t_start   = parsed["start_dt"].strftime("%Y-%m-%dT%H:%M:%SZ")
    t_stop    = (parsed["stop_dt"] + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")

    base_params = {
        "provider":                    CMR_PROVIDER,
        "ShortName":                   RTC_SHORTNAME,
        "native-id":                   native_id,
        "options[native-id][pattern]": "true",
        "temporal":                    f"{t_start},{t_stop}",
        "page_size":                   PAGE_SIZE,
    }

    all_items: list[dict] = []
    page_num = 1

    while True:
        params = {**base_params, "page_num": page_num}
        data   = cmr_get_umm(params, debug=debug)
        items  = data.get("items", [])
        all_items.extend(items)

        if len(items) < PAGE_SIZE:
            break
        page_num += 1
        time.sleep(REQUEST_DELAY)

    confirmed:   list[dict] = []
    unconfirmed: list[dict] = []

    for item in all_items:
        input_granules    = item.get("umm", {}).get("InputGranules", [])
        normalised_inputs = [normalise_slc_name(ig) for ig in input_granules]
        if scene_id in normalised_inputs:
            confirmed.append(item)
        else:
            unconfirmed.append(item)

    return confirmed, unconfirmed


def format_item(item: dict) -> dict:
    meta = item.get("meta", {})
    umm  = item.get("umm", {})
    tp   = umm.get("TemporalExtent", {}).get("RangeDateTime", {})
    urls = [
        r.get("URL") for r in umm.get("RelatedUrls", [])
        if r.get("Type") in ("GET DATA", "GET RELATED VISUALIZATION", "USE SERVICE API")
    ]
    return {
        "granule_ur":             umm.get("GranuleUR"),
        "concept_id":             meta.get("concept-id"),
        "collection_concept_id":  meta.get("collection-concept-id"),
        "time_start":             tp.get("BeginningDateTime"),
        "time_end":               tp.get("EndingDateTime"),
        "urls":                   [u for u in urls if u],
        "input_granules":         umm.get("InputGranules", []),
    }


def read_slc_names(path: str) -> list[str]:
    names = []
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return names


# ── Step 2 Helpers: S3 Download & Parquet Mapping ────────────────────────────

def download_s3_file(bucket: str, key: str, local_path: str):
    if Path(local_path).exists():
        print(f"[*] Local copy of {local_path} found. Skipping download.")
        return

    print(f"[*] Downloading s3://{bucket}/{key}...")
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)
    print("[+] Download complete.")


def extract_burst_ids_from_strings(granule_urs: list[str]) -> list[str]:
    """Parses RTC granule_ur strings to extract burst IDs like T130-279228-IW3."""
    burst_pattern = re.compile(r"(T\d{3}-\d{6}-IW\d)", re.IGNORECASE)
    burst_ids = []
    for g_ur in granule_urs:
        matches = burst_pattern.findall(g_ur)
        burst_ids.extend([b.upper() for b in matches])

    unique_burst_ids = list(set(burst_ids))
    print(f"[+] Extracted {len(unique_burst_ids)} unique burst IDs from confirmed RTC granules.")
    return unique_burst_ids


# ── CLI Construction ─────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Find OPERA RTC granules and map them to MGRS Tile Acquisition Groups."
    )
    p.add_argument("--input", "-i", required=True, metavar="FILE",
                   help="Text file with one SLC granule name per line.")
    p.add_argument("--output-json", "-o", default="rtc_query_results.json", metavar="FILE",
                   help="JSON summary output file (default: rtc_query_results.json).")
    p.add_argument("--output-tiles", "-t", default="matched_tile_acq_groups.txt", metavar="FILE",
                   help="Text file to write MGRS Tile Acquisition Groups (default: matched_tile_acq_groups.txt).")
    p.add_argument("--provider", default=CMR_PROVIDER,
                   help=f"CMR provider (default: {CMR_PROVIDER}).")
    p.add_argument("--short-name", default=RTC_SHORTNAME,
                   help=f"CMR collection short name (default: {RTC_SHORTNAME}).")
    p.add_argument("--delay", type=float, default=REQUEST_DELAY, metavar="SECONDS",
                   help=f"Delay between CMR requests (default: {REQUEST_DELAY}s).")
    p.add_argument("--debug", action="store_true",
                   help="Print each CMR URL and page stats.")
    return p


# ── Main Pipeline ────────────────────────────────────────────────────────────

def main() -> None:
    args = build_parser().parse_args()

    global CMR_PROVIDER, RTC_SHORTNAME
    CMR_PROVIDER  = args.provider
    RTC_SHORTNAME = args.short_name

    print("=" * 70)
    print("STEP 1: CMR RTC Product Query (OPERA RTC-S1 / ASF)")
    print("=" * 70)

    # 1. Load SLC names
    print(f"\n[1] Reading SLC names from: {args.input}")
    slc_names = read_slc_names(args.input)
    if not slc_names:
        print("    [ERROR] No SLC names found. Exiting.")
        sys.exit(1)
    print(f"    {len(slc_names)} SLC name(s) loaded.")

    # 2. Query each SLC
    print(f"\n[2] Querying CMR for {len(slc_names)} SLC input(s) …\n")

    results:     dict[str, list[dict]] = {}
    unconfirmed: dict[str, list[dict]] = {}
    not_found:   list[str]             = []

    for idx, slc in enumerate(slc_names, start=1):
        label = f"[{idx:3d}/{len(slc_names)}]"
        print(f"  {label} {slc}", end="  …  ", flush=True)

        try:
            confirmed, unconfrmd = search_rtc_for_slc(slc, debug=args.debug)
        except ValueError as exc:
            print(f"SKIP ({exc})")
            results[slc]     = []
            not_found.append(slc)
            continue
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            print(f"HTTP {exc.code} ERROR\n    {body[:300]}", file=sys.stderr)
            results[slc]     = []
            not_found.append(slc)
            continue

        results[slc]     = [format_item(it) for it in confirmed]
        unconfirmed[slc] = [format_item(it) for it in unconfrmd]

        parts = [f"{len(confirmed)} confirmed"]
        if unconfrmd:
            parts.append(f"{len(unconfrmd)} unconfirmed")
        print(", ".join(parts) if confirmed or unconfrmd else "none found")

        if not confirmed:
            not_found.append(slc)

        time.sleep(args.delay)

    # 3. Save JSON and Granule UR List
    total_rtc = sum(len(v) for v in results.values())
    total_unconfirmed = sum(len(v) for v in unconfirmed.values())

    with open(args.output_json, "w") as fh:
        json.dump(
            {
                "query_info": {
                    "input_file":            args.input,
                    "provider":              CMR_PROVIDER,
                    "short_name":            RTC_SHORTNAME,
                    "slc_inputs_count":      len(slc_names),
                    "confirmed_rtc_count":   total_rtc,
                    "unconfirmed_rtc_count": total_unconfirmed,
                },
                "results":     results,
                "unconfirmed": unconfirmed,
                "not_found":   not_found,
            },
            fh, indent=2, default=str,
        )
    print(f"\n[✓] Full results JSON saved to: {args.output_json}")

    granule_ur_file = args.output_json.replace(".json", "_granule_ids.txt")
    if not granule_ur_file.endswith("_granule_ids.txt"):
        granule_ur_file = args.output_json + "_granule_ids.txt"

    confirmed_urs = [
        g["granule_ur"]
        for granules in results.values()
        for g in granules
        if g.get("granule_ur")
    ]
    with open(granule_ur_file, "w") as fh:
        fh.write("\n".join(confirmed_urs) + ("\n" if confirmed_urs else ""))
    print(f"[✓] Confirmed RTC granule IDs saved to: {granule_ur_file}")

    if not confirmed_urs:
        print("\n[-] No confirmed RTC granules found. Pipeline complete (0 tiles found).")
        return

    # ── STEP 2: Map Bursts to MGRS Tile Acquisition Groups ───────────────────

    print("\n" + "=" * 70)
    print("STEP 2: Map Burst IDs to MGRS Tile Acquisition Groups")
    print("=" * 70)

    # 1. Download Parquet ancillary table
    download_s3_file(S3_BUCKET, S3_KEY, LOCAL_PARQUET_PATH)

    # 2. Extract Burst IDs from CMR results directly in-memory
    burst_ids = extract_burst_ids_from_strings(confirmed_urs)
    if not burst_ids:
        print("[-] Could not parse any valid burst IDs from granule URs.")
        return

    # 3. Read Parquet and process
    print(f"[*] Reading Parquet database: {LOCAL_PARQUET_PATH}")
    df = pd.read_parquet(LOCAL_PARQUET_PATH)

    df['jpl_burst_id'] = df['jpl_burst_id'].astype(str).str.upper()
    filtered_df = df[df['jpl_burst_id'].isin(burst_ids)].copy()

    if filtered_df.empty:
        print("[-] Zero matches found in the lookup table for the retrieved burst IDs.")
        return

    # 4. Generate composite tile_acq string
    filtered_df['tile_acq'] = (
        filtered_df['mgrs_tile_id'].astype(str) + "_" + 
        filtered_df['acq_group_id_within_mgrs_tile'].astype(str)
    )

    unique_tile_groups = sorted(filtered_df['tile_acq'].unique())

    # 5. Output results
    print(f"\n=== Resulting MGRS Tile Acquisition Groups ({len(unique_tile_groups)} total) ===")
    with open(args.output_tiles, "w") as f_out:
        for group in unique_tile_groups:
            print(group)
            f_out.write(f"{group}\n")

    print(f"\n[+] Successfully saved tile acquisition groups to: {args.output_tiles}")


if __name__ == "__main__":
    main()
