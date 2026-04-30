#!/usr/bin/env python3
"""
Query GRQ OpenSearch and CMR to find DISP-S1 and CCSLC products generated from
the affected K-cycles identified in affected_kcycles.json.

Key mapping:
  - K-cycle N covers sequential sensing time indices [N*k, (N+1)*k)
  - acquisition_cycle in GRQ/CMR is a DAY INDEX (days since first sensing time)
  - We compute the day_index at the start of each affected K-cycle from the constDB
  - For DISP-S1 products, the acquisition_cycle = day_index of the LAST sensing
    time in the K-cycle. So any DISP product whose acquisition_cycle >= the day_index
    of the FIRST sensing time in the affected K-cycle needs deletion.

Sources:
  - GRQ (localhost:9201): DISP-S1 products + CCSLC products on current cluster
  - CMR (cmr.earthdata.nasa.gov): DISP-S1 products delivered to DAAC (all clusters)
  - CCSLC is NOT delivered to DAAC

S3 paths: reported as dataset directory (not individual files).
"""

import json
import math
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from collections import defaultdict

AFFECTED_JSON = Path(__file__).parent / "affected_kcycles.json"
OLD_CONSTDB = Path(__file__).parent.parent / "disp_s1_consistent_burst_db" / \
    "opera-disp-s1-consistent-burst-ids-2025-06-30-2016-07-01_to_2024-12-31.json"
K = 15

GRQ_URL = "http://localhost:9201"
DISP_PRODUCT_INDEX = "grq_v1.0_l3_disp_s1-*"
CCSLC_PRODUCT_INDEX = "grq_1_l2_cslc_s1_compressed-*"

CMR_BASE = "https://cmr.earthdata.nasa.gov/search"
CMR_DISP_COLLECTION = "C3294057315-ASF"  # OPERA_L3_DISP-S1_V1
CMR_PAGE_SIZE = 2000


# ---------- constDB helpers ----------

def load_constdb(path):
    """Returns dict: { frame_id (int): { 'sensing_times': [...], 'day_indices': [...] } }"""
    with open(path) as f:
        db = json.load(f)

    result = {}
    for frame_str, frame_data in db["data"].items():
        times = sorted(datetime.fromisoformat(t) for t in frame_data["sensing_time_list"])
        if not times:
            continue
        first = times[0]
        day_indices = [round((t - first).total_seconds() / 86400) for t in times]
        result[int(frame_str)] = {
            "sensing_times": times,
            "day_indices": day_indices,
        }
    return result


def get_min_day_index_for_kcycle(frame_data, kcycle, k=15):
    """Get the day_index of the first sensing time in the given K-cycle."""
    idx = kcycle * k
    if idx < len(frame_data["day_indices"]):
        return frame_data["day_indices"][idx]
    return None


# ---------- OpenSearch helpers ----------

def opensearch_query(base_url, index, query, size=10000):
    """Execute an OpenSearch query with scroll and return all hits."""
    all_hits = []
    url = f"{base_url}/{index}/_search?scroll=2m&size={size}"
    body = json.dumps(query).encode()
    req = Request(url, data=body, headers={"Content-Type": "application/json"})
    resp = json.loads(urlopen(req, timeout=60).read())

    scroll_id = resp.get("_scroll_id")
    hits = resp["hits"]["hits"]
    all_hits.extend(hits)
    total_val = resp["hits"]["total"]["value"]
    total_rel = resp["hits"]["total"]["relation"]
    total = total_val  # may be approximate if relation is "gte"

    while hits:
        scroll_url = f"{base_url}/_search/scroll"
        scroll_body = json.dumps({"scroll": "2m", "scroll_id": scroll_id}).encode()
        scroll_req = Request(scroll_url, data=scroll_body, headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(scroll_req, timeout=60).read())
        hits = resp["hits"]["hits"]
        all_hits.extend(hits)

    # Clear scroll
    if scroll_id:
        try:
            clear_url = f"{base_url}/_search/scroll"
            clear_body = json.dumps({"scroll_id": scroll_id}).encode()
            clear_req = Request(clear_url, data=clear_body, headers={"Content-Type": "application/json"},
                                method="DELETE")
            urlopen(clear_req, timeout=10)
        except Exception:
            pass

    return all_hits


def _os_count(base_url, index, frame_id):
    """Quick count of all products for a frame (any acquisition_cycle)."""
    url = f"{base_url}/{index}/_count"
    body = json.dumps({"query": {"term": {"metadata.frame_id": frame_id}}}).encode()
    req = Request(url, data=body, headers={"Content-Type": "application/json"})
    return json.loads(urlopen(req, timeout=30).read())["count"]


def build_grq_query(frame_id, min_day_index, source_fields=None):
    """Build query for products with acquisition_cycle >= min_day_index."""
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.frame_id": frame_id}},
                    {"range": {"metadata.acquisition_cycle": {"gte": min_day_index}}}
                ]
            }
        },
        "sort": [{"metadata.acquisition_cycle": "asc"}]
    }
    if source_fields:
        query["_source"] = source_fields
    return query


# ---------- CMR helpers ----------

def query_cmr_disp_by_frame(frame_id):
    """Query CMR for all DISP-S1 granules for a given frame.
    Uses readable_granule_name wildcard: *F{frame:05d}*
    Returns list of dicts with id, title, s3_dir, concept_id.
    """
    pattern = f"*F{frame_id:05d}*"
    all_granules = []
    page = 1

    while True:
        url = (f"{CMR_BASE}/granules.json"
               f"?collection_concept_id={CMR_DISP_COLLECTION}"
               f"&readable_granule_name={pattern}"
               f"&options%5Breadable_granule_name%5D%5Bpattern%5D=true"
               f"&page_size={CMR_PAGE_SIZE}&page_num={page}"
               f"&sort_key=start_date")
        req = Request(url)
        try:
            resp = json.loads(urlopen(req, timeout=120).read())
        except (HTTPError, TimeoutError, URLError) as e:
            if isinstance(e, HTTPError) and e.code == 429:
                time.sleep(2)
                continue
            # Retry once on timeout
            time.sleep(3)
            try:
                resp = json.loads(urlopen(req, timeout=120).read())
            except Exception:
                print(f"  WARNING: CMR query failed for frame {frame_id}: {e}")
                break

        entries = resp["feed"].get("entry", [])
        if not entries:
            break

        for e in entries:
            title = e["title"]
            concept_id = e["id"]

            # Extract S3 dataset directory from s3 data links
            s3_dir = None
            for link in e.get("links", []):
                href = link.get("href", "")
                rel = link.get("rel", "")
                if "s3#" in rel and "asf-cumulus-prod-opera-products" in href and href.endswith(".nc"):
                    # Directory is everything up to the last /
                    s3_dir = href.rsplit("/", 1)[0]
                    break

            all_granules.append({
                "title": title,
                "concept_id": concept_id,
                "s3_dir": s3_dir,
            })

        if len(entries) < CMR_PAGE_SIZE:
            break
        page += 1
        time.sleep(0.2)  # rate limit

    return all_granules


def parse_disp_product_name(name):
    """Parse OPERA_L3_DISP-S1_IW_F{frame}_VV_{ref}_{sec}_v1.0_{creation}
    Returns (frame_id, ref_datetime, sec_datetime) or None."""
    m = re.match(
        r'OPERA_L3_DISP-S1_IW_F(\d+)_VV_(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)_v[\d.]+',
        name
    )
    if m:
        frame = int(m.group(1))
        ref_dt = datetime.strptime(m.group(2), "%Y%m%dT%H%M%SZ")
        sec_dt = datetime.strptime(m.group(3), "%Y%m%dT%H%M%SZ")
        return frame, ref_dt, sec_dt
    return None


def find_day_index_for_time(frame_data, target_dt):
    """Compute the day_index for target_dt relative to the frame's first sensing time.

    First tries to match against a known constDB sensing time (within 30 min).
    If no match (e.g. target_dt is beyond the constDB date range), computes the
    day_index directly from the time delta to the first sensing time.
    """
    times = frame_data["sensing_times"]
    days = frame_data["day_indices"]
    first_time = times[0]

    # Try exact match against constDB sensing times
    best_idx = None
    best_delta = None
    for i, t in enumerate(times):
        delta = abs((t - target_dt).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = i
    if best_idx is not None and best_delta < 1800:  # within 30 min
        return days[best_idx], best_idx

    # No constDB match — compute day_index directly (for dates beyond constDB range)
    day_idx = round((target_dt - first_time).total_seconds() / 86400)
    return day_idx, None


def extract_s3_dir_from_grq(hit):
    """Extract S3 dataset directory from GRQ product hit."""
    meta = hit["_source"]["metadata"]
    s3_paths = meta.get("product_s3_paths", [])
    if s3_paths:
        # Take first path and get directory
        return s3_paths[0].rsplit("/", 1)[0]
    return None


# ---------- Main ----------

def main():
    with open(AFFECTED_JSON) as f:
        affected = json.load(f)

    constdb = load_constdb(OLD_CONSTDB)

    print(f"Loaded {len(affected)} affected frames")
    print(f"Loaded {len(constdb)} frames from constDB")

    # Test GRQ connectivity
    try:
        req = Request(f"{GRQ_URL}/_cluster/health")
        resp = json.loads(urlopen(req, timeout=5).read())
        print(f"GRQ: cluster={resp['cluster_name']}, status={resp['status']}")
    except Exception as e:
        print(f"GRQ: FAILED - {e}")
        sys.exit(1)

    # Test CMR connectivity
    try:
        req = Request(f"{CMR_BASE}/collections.json?short_name=OPERA_L3_DISP-S1_V1&page_size=1")
        resp = json.loads(urlopen(req, timeout=10).read())
        print(f"CMR: OK (DISP-S1 collection: {CMR_DISP_COLLECTION})")
    except Exception as e:
        print(f"CMR: FAILED - {e}")
        sys.exit(1)

    all_disp_grq = []
    all_ccslc_grq = []
    all_disp_cmr = []
    frames_no_products = []  # frames with nothing found anywhere

    print(f"\n{'='*130}")
    print(f"{'Frame':>8}  {'KCycle':>6}  {'MinDayIdx':>9}  "
          f"{'DISP GRQ':>8}  {'CCSLC GRQ':>9}  {'DISP CMR':>8}  {'Status'}")
    print(f"{'-'*130}")

    for entry in affected:
        frame_id = entry["frame_id"]
        if not entry["kcycles_to_delete"]:
            print(f"{frame_id:>8}  No K-cycles to delete (new frame with 0 sensing times) - SKIPPING")
            continue
        first_kcycle = entry["kcycles_to_delete"][0]
        total_kcycles = entry["total_kcycles"]

        frame_data = constdb.get(frame_id)
        if not frame_data:
            print(f"{frame_id:>8}  Frame not in constDB - SKIPPING")
            continue

        min_day_index = get_min_day_index_for_kcycle(frame_data, first_kcycle, K)
        if min_day_index is None:
            print(f"{frame_id:>8}  {first_kcycle:>6}  K-cycle start beyond sensing time list - SKIPPING")
            continue

        # When K-cycle 0 is affected, ALL products for the frame need deletion
        # (including those with sensing times added before the old constDB's first time,
        # which would have negative day indices relative to the old constDB).
        delete_all = (first_kcycle == 0)

        # 1. Query GRQ for DISP-S1 products
        if delete_all:
            disp_query = {
                "query": {"term": {"metadata.frame_id": frame_id}},
                "sort": [{"metadata.acquisition_cycle": "asc"}]
            }
            disp_query["_source"] = ["id", "metadata.frame_id", "metadata.acquisition_cycle",
                                     "metadata.Files.ref_datetime", "metadata.Files.sec_datetime",
                                     "metadata.product_s3_paths"]
        else:
            disp_query = build_grq_query(
                frame_id, min_day_index,
                source_fields=["id", "metadata.frame_id", "metadata.acquisition_cycle",
                               "metadata.Files.ref_datetime", "metadata.Files.sec_datetime",
                               "metadata.product_s3_paths"]
            )
        disp_hits = opensearch_query(GRQ_URL, DISP_PRODUCT_INDEX, disp_query)

        # 2. Query GRQ for CCSLC products
        if delete_all:
            ccslc_query = {
                "query": {"term": {"metadata.frame_id": frame_id}},
                "sort": [{"metadata.acquisition_cycle": "asc"}]
            }
            ccslc_query["_source"] = ["id", "metadata.frame_id", "metadata.acquisition_cycle",
                                      "metadata.burst_id", "metadata.ccslc_m_index",
                                      "metadata.product_s3_paths"]
        else:
            ccslc_query = build_grq_query(
                frame_id, min_day_index,
                source_fields=["id", "metadata.frame_id", "metadata.acquisition_cycle",
                               "metadata.burst_id", "metadata.ccslc_m_index",
                               "metadata.product_s3_paths"]
            )
        ccslc_hits = opensearch_query(GRQ_URL, CCSLC_PRODUCT_INDEX, ccslc_query)

        # 3. Query CMR for DISP-S1 granules
        cmr_granules = query_cmr_disp_by_frame(frame_id)

        # Filter CMR granules to only those in affected K-cycles
        # When delete_all, include ALL granules for this frame
        cmr_affected = []
        for g in cmr_granules:
            parsed = parse_disp_product_name(g["title"])
            if not parsed:
                continue
            _, _, sec_dt = parsed
            day_idx, seq_idx = find_day_index_for_time(frame_data, sec_dt)
            if delete_all or (day_idx is not None and day_idx >= min_day_index):
                g["day_index"] = day_idx
                g["sequential_index"] = seq_idx
                g["kcycle"] = seq_idx // K if seq_idx is not None else None
                cmr_affected.append(g)

        # For frames with no affected products, check if ANY products exist
        # (to distinguish "affected K-cycles not yet processed" from "truly missing")
        total_cmr = len(cmr_granules)  # all CMR granules for this frame, not just affected

        # Determine status
        has_disp_grq = len(disp_hits) > 0
        has_ccslc_grq = len(ccslc_hits) > 0
        has_disp_cmr = len(cmr_affected) > 0

        if not has_disp_grq and not has_ccslc_grq and not has_disp_cmr:
            # Count ALL products for this frame (any K-cycle) via _count API
            any_ccslc_count = _os_count(GRQ_URL, CCSLC_PRODUCT_INDEX, frame_id)
            any_disp_count = _os_count(GRQ_URL, DISP_PRODUCT_INDEX, frame_id)

            if any_ccslc_count > 0 or any_disp_count > 0 or total_cmr > 0:
                status = (f"Affected K-cycles not yet processed "
                          f"(frame has {any_disp_count} DISP/{any_ccslc_count} CCSLC in GRQ, "
                          f"{total_cmr} DISP in CMR for earlier K-cycles)")
                reason = "not_yet_processed"
            else:
                status = "!! NO PRODUCTS FOUND ANYWHERE"
                reason = "truly_missing"
                any_ccslc_count = 0
                any_disp_count = 0

            frames_no_products.append({
                "frame_id": frame_id,
                "affected_kcycles_start": first_kcycle,
                "min_day_index": min_day_index,
                "any_disp_grq": any_disp_count,
                "any_ccslc_grq": any_ccslc_count,
                "any_disp_cmr": total_cmr,
                "reason": reason,
            })
        elif not has_disp_grq and not has_disp_cmr:
            status = "CCSLC only (no DISP anywhere)"
        elif has_disp_cmr and not has_disp_grq:
            status = "DISP in CMR only (prev cluster)"
        elif has_disp_grq and not has_disp_cmr:
            status = "DISP in GRQ only (not yet at DAAC)"
        else:
            status = "OK"

        print(f"{frame_id:>8}  {first_kcycle:>6}  {min_day_index:>9}  "
              f"{len(disp_hits):>8}  {len(ccslc_hits):>9}  {len(cmr_affected):>8}  {status}")

        # Collect results
        for h in disp_hits:
            all_disp_grq.append({
                "id": h["_source"]["id"],
                "frame_id": h["_source"]["metadata"]["frame_id"],
                "acquisition_cycle": h["_source"]["metadata"]["acquisition_cycle"],
                "s3_dir": extract_s3_dir_from_grq(h),
                "index": h["_index"],
                "source": "grq",
            })

        for h in ccslc_hits:
            all_ccslc_grq.append({
                "id": h["_source"]["id"],
                "frame_id": h["_source"]["metadata"]["frame_id"],
                "acquisition_cycle": h["_source"]["metadata"]["acquisition_cycle"],
                "burst_id": h["_source"]["metadata"].get("burst_id"),
                "ccslc_m_index": h["_source"]["metadata"].get("ccslc_m_index"),
                "s3_dir": extract_s3_dir_from_grq(h),
                "index": h["_index"],
                "source": "grq",
            })

        for g in cmr_affected:
            all_disp_cmr.append({
                "title": g["title"],
                "concept_id": g["concept_id"],
                "s3_dir": g["s3_dir"],
                "frame_id": frame_id,
                "day_index": g["day_index"],
                "kcycle": g["kcycle"],
                "source": "cmr",
            })

    print(f"{'='*130}")

    # Merge DISP products: GRQ + CMR (deduplicate by product ID/title)
    grq_disp_ids = {d["id"] for d in all_disp_grq}
    disp_cmr_only = [d for d in all_disp_cmr if d["title"] not in grq_disp_ids]
    disp_both = [d for d in all_disp_cmr if d["title"] in grq_disp_ids]

    print(f"\nSummary:")
    print(f"  DISP-S1 products in GRQ (current cluster):  {len(all_disp_grq)}")
    print(f"  DISP-S1 granules in CMR (all clusters):     {len(all_disp_cmr)}")
    print(f"    - Also in GRQ (overlap):                  {len(disp_both)}")
    print(f"    - CMR only (previous clusters):           {len(disp_cmr_only)}")
    print(f"  CCSLC products in GRQ:                      {len(all_ccslc_grq)}")
    print(f"  Frames with NO products found:              {len(frames_no_products)}")
    if frames_no_products:
        print(f"    Frames: {frames_no_products}")

    # Write output files
    out_dir = Path(__file__).parent

    disp_out = out_dir / "disp_products_to_delete.json"
    with open(disp_out, "w") as f:
        json.dump({
            "grq_products": all_disp_grq,
            "cmr_granules": all_disp_cmr,
            "cmr_only": disp_cmr_only,
        }, f, indent=2)
    print(f"\n  DISP products -> {disp_out}")

    ccslc_out = out_dir / "ccslc_products_to_delete.json"
    with open(ccslc_out, "w") as f:
        json.dump(all_ccslc_grq, f, indent=2)
    print(f"  CCSLC products -> {ccslc_out}")

    no_products_out = out_dir / "frames_no_products.json"
    with open(no_products_out, "w") as f:
        json.dump(frames_no_products, f, indent=2)
    print(f"  Frames w/no products -> {no_products_out}")

    # Per-frame DISP detail
    print(f"\n{'='*130}")
    print("DISP-S1 detail per frame:")
    print(f"{'-'*130}")
    frame_disp = defaultdict(lambda: {"grq": [], "cmr": []})
    for d in all_disp_grq:
        frame_disp[d["frame_id"]]["grq"].append(d["acquisition_cycle"])
    for d in all_disp_cmr:
        frame_disp[d["frame_id"]]["cmr"].append(d["day_index"])
    for fid in sorted(frame_disp):
        grq_acs = sorted(frame_disp[fid]["grq"])
        cmr_acs = sorted(frame_disp[fid]["cmr"])
        print(f"  Frame {fid:>6}: GRQ acq_cycles={grq_acs}")
        print(f"               CMR day_indices={cmr_acs}")


if __name__ == "__main__":
    main()
