#!/usr/bin/env python3
"""
delete_disp_s1_large_gap.py

Purge CCSLC and DISP-S1 products belonging to k-cycles that span a large
(> 2 year) sensing-time gap, and produce a removal list for the DAAC.

Frames that stopped acquiring (e.g. across the S1B outage) and resumed years
later were processed on the absolute k=15 grid, so the ministack straddling the
gap phase-linked pre-gap and post-gap acquisitions.  Those products are
interferometrically invalid, and every CCSLC after them inherits the
contamination through the lineage.  This tool removes them so the frame can be
reprocessed with gap-aware (phased) historical processing.

What is affected, per frame:

* ``gap_start_date`` - the last sensing date before the frame's first gap
  larger than ``--gap-days`` (default 730), read from the consistent burst
  database (or taken from a ``--gap-list`` report).
* ``last_clean_boundary_date`` - the most recent CCSLC ``last_date`` at or
  before ``gap_start_date``.  This is anchored on the CCSLCs that actually
  exist rather than on burst-database positions, so it stays correct when the
  database vintage used for processing differs from the one used for analysis.
  With no CCSLC evidence it falls back to burst-database k-cycle math, and a
  frame with fewer than k pre-gap dates has no clean boundary at all (every
  product is affected).
* affected CCSLCs - ``last_date`` after ``gap_start_date``.
* affected DISP-S1 products - secondary date after ``last_clean_boundary_date``
  (all products when there is no clean boundary).

Everything at or before the boundary is valid history and is kept.

Subcommands:

  audit      inventory GRQ, S3 and CMR; derive the affected sets; write
             manifests and a summary.  Read-only.
  asf-list   turn an audit into a CSV removal list for the DAAC.
  execute    delete the manifest rows from GRQ and S3.
  verify     re-inventory and check the affected sets are gone and the kept
             sets are untouched.

Every subcommand is dry-run by default; ``execute`` mutates nothing unless
``--execute`` is given, and then only after a typed confirmation.  Selection
happens once, in ``audit``: ``execute`` consumes the reviewed manifests and
never recomputes what to delete.

Runs on the Mozart instance, e.g.::

    cd ~/mozart/ops/opera-pcm
    ~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py audit \\
        --gap-list ~/DISP-S1/large_gap_2yrs/prior0_large_gap.txt \\
        --cbdb ~/DISP-S1/large_gap_2yrs/opera-disp-s1-consistent-burst-ids-2026-08-10-2016-07-01_to_2026-04-30.json \\
        --run-dir ~/DISP-S1/large_gap_2yrs/opera_2610_run

Connection settings come from ``~/.sds/config`` and ``~/.netrc-os``; both can be
overridden on the command line.  S3 uses standard boto3 credential resolution.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import netrc
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urlparse

import boto3
import requests
import yaml
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch

LOGGER = logging.getLogger("delete_disp_s1_large_gap")

# --- indices --------------------------------------------------------------
# The trailing "-" keeps the monthly product indices and leaves the *_static
# indices alone.
CCSLC_INDEX = "grq_*_l2_cslc_s1_compressed-*"
L3_INDEX = "grq_*_l3_disp_s1-*"
CSC_INDEX = "grq_*_cslc_s1-cycle-state-config-*"

PARKED_PREFIX = "parked_disp_s1_large_gap"

# --- S3 layout ------------------------------------------------------------
CCSLC_S3_ROOT = "products/CSLC_S1_COMPRESSED"
L3_S3_ROOT = "products/DISP_S1"

# --- CMR ------------------------------------------------------------------
CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
CMR_UAT_GRANULES_URL = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"
CMR_SHORT_NAME = "OPERA_L3_DISP-S1_V1"
CMR_PAGE_SIZE = 2000

DEFAULT_GAP_DAYS = 730
DEFAULT_K = 15
DEFAULT_HARD_CAP = 10_000
ES_CHUNK = 1000
S3_DELETE_CHUNK = 1000
ES_SCROLL_TTL = "2m"
ES_SCROLL_PAGE = 1000

# OPERA_L2_COMPRESSED-CSLC-S1_F24726_T093-197801-IW1_20210818T000000Z_
#   20210818T000000Z_20250704T000000Z_20250801T012345Z_VV_v1.0
CCSLC_RE = re.compile(
    r"^OPERA_L2_COMPRESSED-CSLC-S1_F(?P<frame>\d{5})_(?P<burst>\w{4}-\w{6}-\w{3})_"
    r"(?P<ref_date>\d{8})T\d{6}Z_(?P<first_date>\d{8})T\d{6}Z_(?P<last_date>\d{8})T\d{6}Z_"
    r"(?P<creation_ts>\d{8}T\d{6}Z)_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_"
    r"(?P<version>v\d+[.]\d+)(?:[.]h5)?$"
)

# OPERA_L3_DISP-S1_IW_F24726_VV_20210818T013300Z_20250704T013300Z_v1.0_
#   20250801T012345Z
L3_RE = re.compile(
    r"^OPERA_L3_DISP-S1_IW_F(?P<frame>\d{5})_(?P<pol>VV|VH|HH|HV)_"
    r"(?P<ref_date>\d{8})T\d{6}Z_(?P<sec_date>\d{8})T\d{6}Z_"
    r"(?P<version>v\d+[.]\d+)_(?P<creation_ts>\d{8}T\d{6}Z)$"
)

# cslc_s1-cycle-f16947-20260305-state-config
CSC_ID_RE = re.compile(
    r"^cslc_s1-cycle-f(?P<frame>\d+)-(?P<sensing_date>\d{8})-state-config$"
)

# "FRAME 24726" / "  GAP: 2021-12-16T01:33:07 -> 2025-05-29T01:32:39 (1259 days)"
GAP_LIST_FRAME_RE = re.compile(r"^FRAME\s+(?P<frame>\d+)\s*$")
GAP_LIST_GAP_RE = re.compile(
    r"^\s*GAP:\s+(?P<start>\S+)\s+->\s+(?P<end>\S+)\s+\((?P<days>\d+)\s+days\)"
)


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _chunked(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _first_scalar(val):
    if isinstance(val, (list, tuple)):
        return val[0] if val else None
    return val


def _ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _parse_cbdb_time(value: str) -> datetime:
    return datetime.fromisoformat(value.split("Z")[0])


def utcstamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# configuration / clients
# ---------------------------------------------------------------------------

def load_sds_config(path: Path) -> dict:
    """Parse ``~/.sds/config`` permissively (skipping un-rendered templates)."""
    if not path.exists():
        LOGGER.warning("no config at %s", path)
        return {}
    text = path.read_text()
    text = re.sub(r"\$\{[^}]+\}", "", text)
    text = re.sub(r"\{\{[^}]+\}\}", "", text)
    try:
        return yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        LOGGER.warning("could not parse %s as YAML: %s", path, e)
        return {}


def _netrc_auth(netrc_path: Path) -> tuple[str, str] | None:
    if not netrc_path.exists():
        return None
    try:
        creds = netrc.netrc(str(netrc_path)).authenticators("default")
    except (netrc.NetrcParseError, OSError) as e:
        LOGGER.warning("could not parse %s: %s", netrc_path, e)
        return None
    if not creds:
        return None
    return creds[0], creds[2]


def build_opensearch_client(args, cfg: dict) -> OpenSearch:
    es_url = args.es_url
    if not es_url:
        protocol = _first_scalar(cfg.get("GRQ_ES_PROTOCOL")) or "http"
        # GRQ_ES_PVT_IP is present but empty on some venues; fall back in turn.
        host = (
            _first_scalar(cfg.get("GRQ_ES_PVT_IP"))
            or _first_scalar(cfg.get("GRQ_ES_PUB_IP"))
            or _first_scalar(cfg.get("GRQ_PVT_IP"))
            or _first_scalar(cfg.get("GRQ_ES_FQDN"))
        )
        port = _first_scalar(cfg.get("GRQ_ES_PORT")) or 9200
        if not host:
            sys.exit(
                "error: could not resolve the GRQ OpenSearch host - pass --es-url or set "
                "GRQ_ES_PVT_IP / GRQ_ES_PUB_IP / GRQ_PVT_IP in ~/.sds/config"
            )
        es_url = f"{protocol}://{host}:{port}"

    parsed = urlparse(es_url)
    use_ssl = parsed.scheme == "https"

    http_auth: tuple[str, str] | None = None
    if args.es_user and args.es_password:
        http_auth = (args.es_user, args.es_password)
    elif use_ssl:
        http_auth = _netrc_auth(Path(args.netrc_os).expanduser())

    client = OpenSearch(
        hosts=[es_url],
        http_compress=True,
        http_auth=http_auth,
        use_ssl=use_ssl,
        verify_certs=args.verify_certs,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=120,
        max_retries=3,
        retry_on_timeout=True,
    )
    client.info()  # fail fast on bad config
    LOGGER.info("connected to OpenSearch at %s (ssl=%s)", es_url, use_ssl)
    return client


def resolve_buckets(args, cfg: dict) -> tuple[str, str]:
    lts = args.lts_bucket or _first_scalar(cfg.get("LTS_BUCKET"))
    rs = args.rs_bucket or _first_scalar(cfg.get("DATASET_BUCKET"))
    if not lts:
        sys.exit("error: LTS bucket not resolved - pass --lts-bucket or set LTS_BUCKET")
    if not rs:
        sys.exit("error: product bucket not resolved - pass --rs-bucket or set DATASET_BUCKET")
    LOGGER.info("buckets: CCSLC=s3://%s  DISP-S1=s3://%s", lts, rs)
    return lts, rs


# ---------------------------------------------------------------------------
# granule id parsing
# ---------------------------------------------------------------------------

def parse_ccslc_id(granule: str) -> dict | None:
    m = CCSLC_RE.match(granule)
    if not m:
        return None
    d = m.groupdict()
    d["frame_id"] = int(d.pop("frame"))
    return d


def parse_l3_id(granule: str) -> dict | None:
    m = L3_RE.match(granule)
    if not m:
        return None
    d = m.groupdict()
    d["frame_id"] = int(d.pop("frame"))
    return d


# ---------------------------------------------------------------------------
# frame input
# ---------------------------------------------------------------------------

@dataclass
class FrameInput:
    frame_id: int
    # gap windows as reported by the gap-list file, when one was supplied
    reported_gaps: list[dict] = field(default_factory=list)


def read_gap_list(path: Path) -> list[FrameInput]:
    """Read a ``prior<N>_large_gap.txt`` report (FRAME / GAP lines)."""
    frames: list[FrameInput] = []
    current: FrameInput | None = None
    for raw in path.read_text().splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        m = GAP_LIST_FRAME_RE.match(line.strip())
        if m:
            current = FrameInput(frame_id=int(m.group("frame")))
            frames.append(current)
            continue
        g = GAP_LIST_GAP_RE.match(line)
        if g and current is not None:
            current.reported_gaps.append(
                {
                    "start": g.group("start"),
                    "end": g.group("end"),
                    "days": int(g.group("days")),
                }
            )
    LOGGER.info("loaded %d frames from %s", len(frames), path)
    return frames


def read_frames_file(path: Path) -> list[FrameInput]:
    """Read a ``{"frames": [...]}`` (or ``{"frame": [...]}``) JSON list."""
    data = json.loads(path.read_text())
    if isinstance(data, list):
        ids = data
    else:
        ids = data.get("frames") or data.get("frame") or []
    frames = [FrameInput(frame_id=int(f)) for f in ids]
    LOGGER.info("loaded %d frames from %s", len(frames), path)
    return frames


def parse_frames_arg(value: str) -> list[FrameInput]:
    return [FrameInput(frame_id=int(v.strip())) for v in value.split(",") if v.strip()]


def resolve_frame_inputs(args) -> list[FrameInput]:
    if args.gap_list:
        frames = read_gap_list(args.gap_list)
    elif args.frames_file:
        frames = read_frames_file(args.frames_file)
    elif args.frames:
        frames = parse_frames_arg(args.frames)
    else:
        sys.exit("error: one of --gap-list, --frames-file or --frames is required")
    # de-duplicate, keep the first occurrence's reported gaps
    seen: dict[int, FrameInput] = {}
    for f in frames:
        seen.setdefault(f.frame_id, f)
    return [seen[k] for k in sorted(seen)]


# ---------------------------------------------------------------------------
# burst database (consistent burst db) gap math
# ---------------------------------------------------------------------------

def load_cbdb(path: Path | None) -> dict:
    if not path:
        return {}
    data = json.loads(path.read_text())
    frames = data.get("data", data)
    LOGGER.info("loaded burst database with %d frames from %s", len(frames), path)
    return frames


def cbdb_gap_analysis(cbdb: dict, frame_id: int, gap_days: int, k: int) -> dict:
    """Derive gap position and the k-aligned pre-gap block for one frame."""
    entry = cbdb.get(str(frame_id))
    if entry is None:
        return {"in_cbdb": False}

    sensing = entry.get("sensing_time_list") or []
    # the annotated variant of the database maps each time to a mode label
    times = sorted(_parse_cbdb_time(t) for t in sensing)
    n = len(times)
    out = {
        "in_cbdb": True,
        "n_dates": n,
        "n_bursts": len(entry.get("burst_id_list") or []),
        "gap_count": 0,
    }
    gaps = []
    for i in range(1, n):
        delta = (times[i] - times[i - 1]).days
        if delta > gap_days:
            gaps.append(
                {
                    "index": i,
                    "start": _ymd(times[i - 1]),
                    "end": _ymd(times[i]),
                    "days": delta,
                }
            )
    out["gap_count"] = len(gaps)
    if not gaps:
        return out

    first = gaps[0]
    pregap_len = first["index"]
    h01_len = k * (pregap_len // k)
    out.update(
        {
            "gaps": gaps,
            "gap_start_date": first["start"],
            "gap_end_date": first["end"],
            "gap_days": first["days"],
            "pregap_len": pregap_len,
            "h01_len": h01_len,
            "cbdb_boundary_date": _ymd(times[h01_len - 1]) if h01_len else None,
            "cbdb_max_affected_dates": n - h01_len,
        }
    )
    return out


# ---------------------------------------------------------------------------
# OpenSearch inventory
# ---------------------------------------------------------------------------

def _scroll_search(es: OpenSearch, index: str, body: dict) -> Iterator[dict]:
    body = {**body, "size": ES_SCROLL_PAGE}
    page = es.search(index=index, body=body, scroll=ES_SCROLL_TTL, ignore_unavailable=True)
    scroll_id = page.get("_scroll_id")
    try:
        while True:
            hits = page.get("hits", {}).get("hits", [])
            if not hits:
                return
            yield from hits
            page = es.scroll(scroll_id=scroll_id, scroll=ES_SCROLL_TTL)
            new_id = page.get("_scroll_id")
            if new_id and new_id != scroll_id:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
                scroll_id = new_id
    finally:
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _id_prefix_query(prefix: str) -> dict:
    # ``id`` mirrors ``_id`` on GRQ product docs and, unlike ``_id``, supports
    # prefix queries.
    return {"prefix": {"id.keyword": prefix}}


def grq_ccslc_for_frame(es: OpenSearch, frame_id: int) -> dict[str, list[str]]:
    """granule id -> list of indices holding it."""
    prefix = f"OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id:05d}_"
    out: dict[str, list[str]] = {}
    body = {"query": _id_prefix_query(prefix), "_source": False}
    for hit in _scroll_search(es, CCSLC_INDEX, body):
        out.setdefault(hit["_id"], []).append(hit["_index"])
    return out


def grq_l3_for_frame(es: OpenSearch, frame_id: int) -> dict[str, list[str]]:
    prefix = f"OPERA_L3_DISP-S1_IW_F{frame_id:05d}_"
    out: dict[str, list[str]] = {}
    body = {"query": _id_prefix_query(prefix), "_source": False}
    for hit in _scroll_search(es, L3_INDEX, body):
        out.setdefault(hit["_id"], []).append(hit["_index"])
    return out


def grq_csc_for_frame(es: OpenSearch, frame_id: int) -> dict[str, dict]:
    """cycle-state-config doc id -> {index, sensing_date}."""
    out: dict[str, dict] = {}
    body = {
        "query": {"term": {"metadata.frame_id": frame_id}},
        "_source": ["metadata.sensing_date", "metadata.frame_id"],
    }
    for hit in _scroll_search(es, CSC_INDEX, body):
        meta = (hit.get("_source") or {}).get("metadata") or {}
        sensing = meta.get("sensing_date")
        if not sensing:
            m = CSC_ID_RE.match(hit["_id"])
            sensing = m.group("sensing_date") if m else None
        if sensing:
            sensing = str(sensing).replace("-", "")[:8]
        out[hit["_id"]] = {"index": hit["_index"], "sensing_date": sensing}
    return out


# ---------------------------------------------------------------------------
# S3 inventory
# ---------------------------------------------------------------------------

def s3_datasets(s3_client, bucket: str, prefix: str) -> set[str]:
    """Names of the immediate child "directories" under ``prefix``."""
    names: set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                names.add(cp["Prefix"].rstrip("/").rsplit("/", 1)[-1])
    except ClientError as e:
        LOGGER.error("S3 list failed for s3://%s/%s: %s", bucket, prefix, e)
    return names


def s3_ccslc_for_frame(s3_client, bucket: str, frame_id: int) -> set[str]:
    # The frame-scoped prefix cuts the listing down to this frame's datasets;
    # the delimiter makes S3 return the dataset directories instead of every file.
    return s3_datasets(
        s3_client, bucket, f"{CCSLC_S3_ROOT}/OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id:05d}_"
    )


def s3_l3_for_frame(s3_client, bucket: str, frame_id: int) -> set[str]:
    return s3_datasets(s3_client, bucket, f"{L3_S3_ROOT}/F{frame_id:05d}/")


# ---------------------------------------------------------------------------
# CMR inventory
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _cmr_session() -> requests.Session:
    session = getattr(_thread_local, "cmr_session", None)
    if session is None:
        session = requests.Session()
        _thread_local.cmr_session = session
    return session


def cmr_l3_for_frame(frame_id: int, endpoint: str = "OPS", max_retries: int = 4) -> dict[str, dict]:
    """granule UR -> {concept_id, size_bytes, end_time} for one frame."""
    url = CMR_UAT_GRANULES_URL if endpoint.upper() == "UAT" else CMR_GRANULES_URL
    params = {
        "ShortName[]": CMR_SHORT_NAME,
        "attribute[]": f"int,FRAME_NUMBER,{frame_id}",
        "page_size": CMR_PAGE_SIZE,
    }
    session = _cmr_session()
    out: dict[str, dict] = {}
    search_after = None
    while True:
        headers = {"CMR-Search-After": search_after} if search_after else {}
        for attempt in range(max_retries + 1):
            try:
                resp = session.get(url, params=params, headers=headers, timeout=120)
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                    time.sleep(min(2 ** attempt, 20))
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException:
                if attempt >= max_retries:
                    raise
                time.sleep(min(2 ** attempt, 20))
        payload = resp.json()
        items = payload.get("items", []) or []
        for item in items:
            umm = item.get("umm", {}) or {}
            meta = item.get("meta", {}) or {}
            granule = umm.get("GranuleUR")
            if not granule:
                continue
            size = 0
            for adi in (umm.get("DataGranule", {}) or {}).get(
                "ArchiveAndDistributionInformation", []
            ) or []:
                if adi.get("SizeInBytes"):
                    size += int(adi["SizeInBytes"])
            end_time = None
            try:
                end_time = umm["TemporalExtent"]["RangeDateTime"]["EndingDateTime"]
            except (KeyError, TypeError):
                pass
            out[granule] = {
                "concept_id": meta.get("concept-id"),
                "size_bytes": size,
                "end_time": end_time,
            }
        search_after = resp.headers.get("CMR-Search-After")
        if not search_after or not items:
            break
    return out


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------

@dataclass
class FrameAudit:
    frame_id: int
    priority: str
    in_cbdb: bool = False
    gap_count: int = 0
    n_dates: int | None = None
    n_bursts: int | None = None
    gap_start_date: str | None = None
    gap_end_date: str | None = None
    gap_days: int | None = None
    pregap_len: int | None = None
    h01_len: int | None = None
    cbdb_boundary_date: str | None = None
    last_clean_boundary_date: str | None = None
    boundary_source: str = "none"
    reported_gaps: list[dict] = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)


def derive_boundary(
    ccslc_last_dates: Iterable[str], gap_start_date: str | None, cbdb_boundary: str | None
) -> tuple[str | None, str]:
    """Most recent CCSLC ``last_date`` at or before the gap; else burst-db math."""
    if gap_start_date:
        clean = sorted(d for d in ccslc_last_dates if d <= gap_start_date)
        if clean:
            return clean[-1], "ccslc-evidence"
    if cbdb_boundary:
        return cbdb_boundary, "burst-db"
    return None, "none"


def audit_frame(
    frame_in: FrameInput,
    *,
    priority: str,
    cbdb: dict,
    gap_days: int,
    k: int,
    es: OpenSearch,
    s3_client,
    lts_bucket: str,
    rs_bucket: str,
    use_cmr: bool,
    cmr_endpoint: str,
    include_state: bool,
) -> tuple[FrameAudit, list[dict], list[dict], list[dict]]:
    """Inventory one frame and classify its granules.

    Returns ``(audit, ccslc_rows, l3_rows, csc_rows)`` where the rows are the
    affected (deletion) manifest entries.
    """
    fid = frame_in.frame_id
    audit = FrameAudit(frame_id=fid, priority=priority, reported_gaps=frame_in.reported_gaps)

    gap = cbdb_gap_analysis(cbdb, fid, gap_days, k) if cbdb else {"in_cbdb": False}
    audit.in_cbdb = bool(gap.get("in_cbdb"))
    audit.n_dates = gap.get("n_dates")
    audit.n_bursts = gap.get("n_bursts")
    audit.gap_count = gap.get("gap_count", 0)
    audit.gap_start_date = gap.get("gap_start_date")
    audit.gap_end_date = gap.get("gap_end_date")
    audit.gap_days = gap.get("gap_days")
    audit.pregap_len = gap.get("pregap_len")
    audit.h01_len = gap.get("h01_len")
    audit.cbdb_boundary_date = gap.get("cbdb_boundary_date")

    if not audit.in_cbdb:
        audit.anomalies.append("not-in-burst-db")
    elif audit.gap_count == 0:
        audit.anomalies.append("no-gap-in-burst-db")

    # Fall back to the gap window reported by the input list when the burst
    # database on hand does not show one (different vintage, dropped frame...).
    if not audit.gap_start_date and frame_in.reported_gaps:
        reported = frame_in.reported_gaps[0]
        audit.gap_start_date = reported["start"].split("T")[0].replace("-", "")
        audit.gap_end_date = reported["end"].split("T")[0].replace("-", "")
        audit.gap_days = reported["days"]
        audit.anomalies.append("gap-from-input-list")

    # --- inventory --------------------------------------------------------
    grq_ccslc = grq_ccslc_for_frame(es, fid)
    s3_ccslc = s3_ccslc_for_frame(s3_client, lts_bucket, fid)
    grq_l3 = grq_l3_for_frame(es, fid)
    s3_l3 = s3_l3_for_frame(s3_client, rs_bucket, fid)
    cmr_l3 = cmr_l3_for_frame(fid, cmr_endpoint) if use_cmr else {}

    all_ccslc = set(grq_ccslc) | s3_ccslc
    all_l3 = set(grq_l3) | s3_l3 | set(cmr_l3)

    parsed_ccslc: dict[str, dict] = {}
    for granule in sorted(all_ccslc):
        meta = parse_ccslc_id(granule)
        if meta is None:
            audit.anomalies.append(f"unparseable-ccslc-id:{granule}")
            continue
        parsed_ccslc[granule] = meta

    parsed_l3: dict[str, dict] = {}
    for granule in sorted(all_l3):
        meta = parse_l3_id(granule)
        if meta is None:
            audit.anomalies.append(f"unparseable-l3-id:{granule}")
            continue
        parsed_l3[granule] = meta

    # --- boundary ---------------------------------------------------------
    boundary, source = derive_boundary(
        (m["last_date"] for m in parsed_ccslc.values()),
        audit.gap_start_date,
        audit.cbdb_boundary_date,
    )
    audit.last_clean_boundary_date = boundary
    audit.boundary_source = source
    if boundary is None:
        audit.anomalies.append("no-clean-boundary-every-product-affected")

    # --- classify ---------------------------------------------------------
    ccslc_rows: list[dict] = []
    if audit.gap_start_date:
        for granule, meta in parsed_ccslc.items():
            if meta["last_date"] > audit.gap_start_date:
                ccslc_rows.append(
                    {
                        "frame_id": fid,
                        "priority": priority,
                        "granule": granule,
                        "burst_id": meta["burst"],
                        "ref_date": meta["ref_date"],
                        "first_date": meta["first_date"],
                        "last_date": meta["last_date"],
                        "creation_ts": meta["creation_ts"],
                        "es_indices": sorted(grq_ccslc.get(granule, [])),
                        "in_s3": granule in s3_ccslc,
                        "s3_bucket": lts_bucket,
                        "s3_prefix": f"{CCSLC_S3_ROOT}/{granule}/",
                        "reason": f"ccslc last_date {meta['last_date']} > gap start {audit.gap_start_date}",
                    }
                )

    l3_rows: list[dict] = []
    for granule, meta in parsed_l3.items():
        affected = boundary is None or meta["sec_date"] > boundary
        if not affected:
            continue
        cmr_meta = cmr_l3.get(granule, {})
        l3_rows.append(
            {
                "frame_id": fid,
                "priority": priority,
                "granule": granule,
                "ref_date": meta["ref_date"],
                "sec_date": meta["sec_date"],
                "creation_ts": meta["creation_ts"],
                "es_indices": sorted(grq_l3.get(granule, [])),
                "in_s3": granule in s3_l3,
                "s3_bucket": rs_bucket,
                "s3_prefix": f"{L3_S3_ROOT}/F{fid:05d}/{granule}/",
                "in_cmr": granule in cmr_l3,
                "cmr_concept_id": cmr_meta.get("concept_id"),
                "cmr_size_bytes": cmr_meta.get("size_bytes"),
                "reason": (
                    "no clean pre-gap boundary"
                    if boundary is None
                    else f"secondary date {meta['sec_date']} > boundary {boundary}"
                ),
            }
        )

    csc_rows: list[dict] = []
    if include_state:
        for doc_id, info in grq_csc_for_frame(es, fid).items():
            sensing = info.get("sensing_date")
            if not sensing:
                continue
            if boundary is None or sensing > boundary:
                csc_rows.append(
                    {
                        "frame_id": fid,
                        "priority": priority,
                        "doc_id": doc_id,
                        "es_indices": [info["index"]],
                        "sensing_date": sensing,
                        "reason": (
                            "no clean pre-gap boundary"
                            if boundary is None
                            else f"sensing date {sensing} > boundary {boundary}"
                        ),
                    }
                )

    # --- anomalies + counts ----------------------------------------------
    grq_only = set(grq_ccslc) - s3_ccslc
    s3_only = s3_ccslc - set(grq_ccslc)
    if grq_only:
        audit.anomalies.append(f"ccslc-in-grq-not-s3:{len(grq_only)}")
    if s3_only:
        audit.anomalies.append(f"ccslc-in-s3-not-grq:{len(s3_only)}")

    # duplicate CCSLCs: same burst and window, different creation time
    window_keys: dict[tuple[str, str, str], int] = {}
    for meta in parsed_ccslc.values():
        key = (meta["burst"], meta["first_date"], meta["last_date"])
        window_keys[key] = window_keys.get(key, 0) + 1
    dup_windows = sum(1 for v in window_keys.values() if v > 1)
    if dup_windows:
        audit.anomalies.append(f"duplicate-ccslc-windows:{dup_windows}")

    # a boundary whose burst count falls short of the frame's burst count
    if audit.n_bursts:
        per_boundary: dict[str, int] = {}
        for meta in parsed_ccslc.values():
            per_boundary[meta["last_date"]] = per_boundary.get(meta["last_date"], 0) + 1
        partial = [d for d, c in per_boundary.items() if c < audit.n_bursts]
        if partial:
            audit.anomalies.append(f"partial-ccslc-boundaries:{','.join(sorted(partial))}")

    audit.counts = {
        "ccslc_grq_docs": len(grq_ccslc),
        "ccslc_s3_datasets": len(s3_ccslc),
        "ccslc_granules": len(parsed_ccslc),
        "ccslc_affected": len(ccslc_rows),
        "ccslc_affected_grq": sum(1 for r in ccslc_rows if r["es_indices"]),
        "ccslc_affected_s3": sum(1 for r in ccslc_rows if r["in_s3"]),
        "ccslc_keep": len(parsed_ccslc) - len(ccslc_rows),
        "ccslc_keep_grq": len(grq_ccslc) - sum(1 for r in ccslc_rows if r["es_indices"]),
        "ccslc_keep_s3": len(s3_ccslc) - sum(1 for r in ccslc_rows if r["in_s3"]),
        "l3_grq_docs": len(grq_l3),
        "l3_s3_datasets": len(s3_l3),
        "l3_cmr_granules": len(cmr_l3),
        "l3_granules": len(parsed_l3),
        "l3_affected": len(l3_rows),
        "l3_affected_grq": sum(1 for r in l3_rows if r["es_indices"]),
        "l3_affected_s3": sum(1 for r in l3_rows if r["in_s3"]),
        "l3_affected_cmr": sum(1 for r in l3_rows if r["in_cmr"]),
        "l3_keep": len(parsed_l3) - len(l3_rows),
        "l3_keep_grq": len(grq_l3) - sum(1 for r in l3_rows if r["es_indices"]),
        "l3_keep_s3": len(s3_l3) - sum(1 for r in l3_rows if r["in_s3"]),
        "l3_keep_cmr": len(cmr_l3) - sum(1 for r in l3_rows if r["in_cmr"]),
        "csc_affected": len(csc_rows),
    }
    return audit, ccslc_rows, l3_rows, csc_rows


# ---------------------------------------------------------------------------
# manifests / reports
# ---------------------------------------------------------------------------

def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")
    LOGGER.info("wrote %d rows to %s", len(rows), path)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def summarize(audits: list[FrameAudit]) -> dict:
    totals: dict[str, int] = {}
    for a in audits:
        for key, val in a.counts.items():
            totals[key] = totals.get(key, 0) + int(val or 0)
    return {
        "frames": len(audits),
        "frames_with_affected": sum(
            1
            for a in audits
            if a.counts.get("ccslc_affected") or a.counts.get("l3_affected")
        ),
        "frames_clean": sum(
            1
            for a in audits
            if not a.counts.get("ccslc_affected") and not a.counts.get("l3_affected")
        ),
        "frames_no_clean_boundary": sum(
            1 for a in audits if a.last_clean_boundary_date is None
        ),
        "frames_not_in_burst_db": sum(1 for a in audits if not a.in_cbdb),
        "totals": totals,
    }


def write_summary_md(path: Path, label: str, summary: dict, audits: list[FrameAudit]) -> None:
    t = summary["totals"]
    lines = [
        f"# DISP-S1 large-gap purge audit - {label}",
        "",
        f"generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Totals",
        "",
        "| metric | value |",
        "| --- | --- |",
        f"| frames audited | {summary['frames']} |",
        f"| frames with something to purge | {summary['frames_with_affected']} |",
        f"| frames already clean | {summary['frames_clean']} |",
        f"| frames with no clean pre-gap boundary | {summary['frames_no_clean_boundary']} |",
        f"| frames not in burst database | {summary['frames_not_in_burst_db']} |",
        f"| CCSLC affected (granules) | {t.get('ccslc_affected', 0)} |",
        f"| CCSLC affected in GRQ | {t.get('ccslc_affected_grq', 0)} |",
        f"| CCSLC affected in S3 | {t.get('ccslc_affected_s3', 0)} |",
        f"| CCSLC kept | {t.get('ccslc_keep', 0)} |",
        f"| DISP-S1 affected (granules) | {t.get('l3_affected', 0)} |",
        f"| DISP-S1 affected in GRQ | {t.get('l3_affected_grq', 0)} |",
        f"| DISP-S1 affected in S3 | {t.get('l3_affected_s3', 0)} |",
        f"| DISP-S1 affected in CMR (DAAC removal list) | {t.get('l3_affected_cmr', 0)} |",
        f"| DISP-S1 kept | {t.get('l3_keep', 0)} |",
        f"| cycle-state-configs affected | {t.get('csc_affected', 0)} |",
        "",
        "## Frames with work",
        "",
        "| frame | gap start | boundary | source | CCSLC aff/keep | DISP-S1 aff/keep | in CMR |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for a in sorted(audits, key=lambda x: x.frame_id):
        c = a.counts
        if not (c.get("ccslc_affected") or c.get("l3_affected")):
            continue
        lines.append(
            f"| {a.frame_id} | {a.gap_start_date or '-'} | "
            f"{a.last_clean_boundary_date or 'NONE'} | {a.boundary_source} | "
            f"{c.get('ccslc_affected', 0)}/{c.get('ccslc_keep', 0)} | "
            f"{c.get('l3_affected', 0)}/{c.get('l3_keep', 0)} | "
            f"{c.get('l3_affected_cmr', 0)} |"
        )
    anomalous = [a for a in audits if a.anomalies]
    lines += ["", f"## Anomalies ({len(anomalous)} frames)", ""]
    for a in sorted(anomalous, key=lambda x: x.frame_id):
        lines.append(f"* {a.frame_id}: {'; '.join(a.anomalies)}")
    path.write_text("\n".join(lines) + "\n")
    LOGGER.info("wrote %s", path)


# ---------------------------------------------------------------------------
# subcommand: audit
# ---------------------------------------------------------------------------

def run_paths(args) -> dict:
    label = args.priority_label
    d = args.run_dir
    return {
        "audit": d / f"audit_{label}.json",
        "ccslc": d / f"manifest_ccslc_{label}.jsonl",
        "l3": d / f"manifest_l3_{label}.jsonl",
        "csc": d / f"manifest_csc_{label}.jsonl",
        "summary": d / f"summary_{label}.md",
        "asf": d / f"asf_removal_{label}.csv",
        "verify": d / f"verify_{label}.md",
    }


def cmd_audit(args) -> int:
    cfg = load_sds_config(args.config)
    es = build_opensearch_client(args, cfg)
    lts_bucket, rs_bucket = resolve_buckets(args, cfg)
    s3_client = boto3.client("s3")
    cbdb = load_cbdb(args.cbdb)
    frames = resolve_frame_inputs(args)
    paths = run_paths(args)
    args.run_dir.mkdir(parents=True, exist_ok=True)

    if not cbdb and not any(f.reported_gaps for f in frames):
        LOGGER.error(
            "no burst database (--cbdb) and no gap windows in the frame input: "
            "there is no way to locate the gaps"
        )
        return 2

    LOGGER.info(
        "auditing %d frames (label=%s, gap-days=%d, k=%d, cmr=%s, state=%s)",
        len(frames), args.priority_label, args.gap_days, args.k,
        "on" if not args.no_cmr else "off", "on" if args.state else "off",
    )

    audits: list[FrameAudit] = []
    ccslc_rows: list[dict] = []
    l3_rows: list[dict] = []
    csc_rows: list[dict] = []
    failures: list[int] = []

    def work(frame_in: FrameInput):
        return audit_frame(
            frame_in,
            priority=args.priority_label,
            cbdb=cbdb,
            gap_days=args.gap_days,
            k=args.k,
            es=es,
            s3_client=s3_client,
            lts_bucket=lts_bucket,
            rs_bucket=rs_bucket,
            use_cmr=not args.no_cmr,
            cmr_endpoint=args.cmr_endpoint,
            include_state=args.state,
        )

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for frame_in, result in zip(frames, pool.map(_safe(work), frames)):
            done += 1
            if result is None:
                failures.append(frame_in.frame_id)
                continue
            audit, c_rows, l_rows, s_rows = result
            audits.append(audit)
            ccslc_rows.extend(c_rows)
            l3_rows.extend(l_rows)
            csc_rows.extend(s_rows)
            if done % 25 == 0 or done == len(frames):
                LOGGER.info("audited %d/%d frames", done, len(frames))

    summary = summarize(audits)
    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "label": args.priority_label,
        "gap_days": args.gap_days,
        "k": args.k,
        "cbdb": str(args.cbdb) if args.cbdb else None,
        "frame_source": str(args.gap_list or args.frames_file or args.frames),
        "buckets": {"ccslc": lts_bucket, "disp_s1": rs_bucket},
        "cmr": None if args.no_cmr else args.cmr_endpoint,
        "failed_frames": failures,
        "summary": summary,
        "frames": [asdict(a) for a in sorted(audits, key=lambda x: x.frame_id)],
    }
    paths["audit"].write_text(json.dumps(payload, indent=2, sort_keys=False))
    LOGGER.info("wrote %s", paths["audit"])
    write_jsonl(paths["ccslc"], ccslc_rows)
    write_jsonl(paths["l3"], l3_rows)
    if args.state:
        write_jsonl(paths["csc"], csc_rows)
    write_summary_md(paths["summary"], args.priority_label, summary, audits)

    t = summary["totals"]
    print()
    print(f"audit complete: {args.priority_label}")
    print(f"  frames audited            : {summary['frames']}")
    print(f"  frames with work          : {summary['frames_with_affected']}")
    print(f"  frames already clean      : {summary['frames_clean']}")
    print(f"  CCSLC affected            : {t.get('ccslc_affected', 0)} "
          f"(GRQ {t.get('ccslc_affected_grq', 0)}, S3 {t.get('ccslc_affected_s3', 0)}) "
          f"| keep {t.get('ccslc_keep', 0)}")
    print(f"  DISP-S1 affected          : {t.get('l3_affected', 0)} "
          f"(GRQ {t.get('l3_affected_grq', 0)}, S3 {t.get('l3_affected_s3', 0)}, "
          f"CMR {t.get('l3_affected_cmr', 0)}) | keep {t.get('l3_keep', 0)}")
    if args.state:
        print(f"  cycle-state-configs       : {t.get('csc_affected', 0)}")
    if failures:
        print(f"  FRAMES THAT FAILED        : {failures}")
    print()
    print("nothing was modified. review the manifests, then run 'execute'.")
    return 1 if failures else 0


def _safe(fn):
    def wrapper(item):
        try:
            return fn(item)
        except Exception as e:  # keep one bad frame from killing the run
            LOGGER.error("frame %s failed: %s", getattr(item, "frame_id", item), e, exc_info=True)
            return None
    return wrapper


# ---------------------------------------------------------------------------
# subcommand: asf-list
# ---------------------------------------------------------------------------

ASF_COLUMNS = [
    "granule_ur",
    "frame_id",
    "priority",
    "reference_date",
    "secondary_date",
    "cmr_concept_id",
    "size_mb",
    "reason",
]


def cmd_asf_list(args) -> int:
    paths = run_paths(args)
    rows = read_jsonl(paths["l3"])
    if not rows:
        LOGGER.warning("no DISP-S1 manifest rows in %s", paths["l3"])
    cmr_rows = [r for r in rows if r.get("in_cmr")]
    cmr_rows.sort(key=lambda r: (r["frame_id"], r["sec_date"], r["granule"]))

    paths["asf"].parent.mkdir(parents=True, exist_ok=True)
    with paths["asf"].open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(ASF_COLUMNS)
        for r in cmr_rows:
            size = r.get("cmr_size_bytes") or 0
            writer.writerow(
                [
                    r["granule"],
                    r["frame_id"],
                    r.get("priority", ""),
                    r["ref_date"],
                    r["sec_date"],
                    r.get("cmr_concept_id") or "",
                    f"{size / (1024 * 1024):.1f}" if size else "",
                    r.get("reason", ""),
                ]
            )
    total_bytes = sum(r.get("cmr_size_bytes") or 0 for r in cmr_rows)
    frames = sorted({r["frame_id"] for r in cmr_rows})
    LOGGER.info("wrote %d rows to %s", len(cmr_rows), paths["asf"])
    print()
    print(f"DAAC removal list: {paths['asf']}")
    print(f"  granules : {len(cmr_rows)}")
    print(f"  frames   : {len(frames)}")
    print(f"  size     : {total_bytes / (1024 ** 3):.2f} GiB")
    not_in_cmr = len(rows) - len(cmr_rows)
    if not_in_cmr:
        print(f"  note     : {not_in_cmr} affected products are not in CMR (local only)")
    return 0


# ---------------------------------------------------------------------------
# subcommand: execute
# ---------------------------------------------------------------------------

def backup_docs(
    es: OpenSearch, ids_by_index: dict[str, list[str]], out_path: Path, *, dry_run: bool
) -> int:
    """Write full documents to NDJSON so a delete can be undone."""
    if dry_run:
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out_path.open("w") as f:
        for index, ids in ids_by_index.items():
            for chunk in _chunked(ids, ES_CHUNK):
                body = {"query": {"terms": {"_id": chunk}}}
                for hit in _scroll_search(es, index, body):
                    f.write(
                        json.dumps(
                            {
                                "_index": hit["_index"],
                                "_id": hit["_id"],
                                "_source": hit.get("_source", {}),
                            }
                        )
                        + "\n"
                    )
                    written += 1
    LOGGER.info("backed up %d docs to %s", written, out_path)
    return written


def park_docs(
    es: OpenSearch, ids_by_index: dict[str, list[str]], parked_index: str, *, dry_run: bool
) -> int:
    """Copy documents into a parked index (same ids) before deleting them."""
    if dry_run:
        return 0
    total = 0
    for index, ids in ids_by_index.items():
        for chunk in _chunked(ids, ES_CHUNK):
            body = {
                "source": {"index": index, "query": {"terms": {"_id": chunk}}},
                "dest": {"index": parked_index, "op_type": "index"},
            }
            try:
                resp = es.reindex(body=body, wait_for_completion=True, refresh=True)
                total += int(resp.get("created", 0)) + int(resp.get("updated", 0))
            except Exception as e:
                LOGGER.error("reindex to %s failed for %s: %s", parked_index, index, e)
                raise
    LOGGER.info("parked %d docs in %s", total, parked_index)
    return total


def delete_es_docs(es: OpenSearch, ids_by_index: dict[str, list[str]], *, dry_run: bool) -> tuple[int, int]:
    planned = sum(len(v) for v in ids_by_index.values())
    if dry_run:
        return planned, 0
    deleted = 0
    errors = 0
    for index, ids in ids_by_index.items():
        for chunk in _chunked(ids, ES_CHUNK):
            try:
                resp = es.delete_by_query(
                    index=index,
                    body={"query": {"terms": {"_id": chunk}}},
                    refresh=True,
                    wait_for_completion=True,
                    conflicts="proceed",
                )
            except Exception as e:
                LOGGER.error("delete_by_query failed on %s (n=%d): %s", index, len(chunk), e)
                errors += len(chunk)
                continue
            deleted += int(resp.get("deleted", 0))
            failures = resp.get("failures", []) or []
            for f in failures[:5]:
                LOGGER.error("ES delete failure in %s: %s", index, f)
            errors += len(failures)
    return deleted, errors


def delete_s3_prefix(s3_client, bucket: str, key_prefix: str, *, dry_run: bool) -> tuple[int, int]:
    if not key_prefix or key_prefix.count("/") < 2:
        LOGGER.error("refusing to delete suspicious prefix s3://%s/%s", bucket, key_prefix)
        return 0, 1
    deleted = 0
    errors = 0
    batch: list[dict] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []) or []:
                batch.append({"Key": obj["Key"]})
                if len(batch) >= S3_DELETE_CHUNK:
                    d, e = _flush_s3_batch(s3_client, bucket, batch, dry_run=dry_run)
                    deleted += d
                    errors += e
                    batch = []
        if batch:
            d, e = _flush_s3_batch(s3_client, bucket, batch, dry_run=dry_run)
            deleted += d
            errors += e
    except ClientError as e:
        LOGGER.error("S3 list failed for s3://%s/%s: %s", bucket, key_prefix, e)
        errors += 1
    return deleted, errors


def _flush_s3_batch(s3_client, bucket: str, batch: list[dict], *, dry_run: bool) -> tuple[int, int]:
    if dry_run:
        return len(batch), 0
    try:
        resp = s3_client.delete_objects(
            Bucket=bucket, Delete={"Objects": batch, "Quiet": True}
        )
    except ClientError as e:
        LOGGER.error("delete_objects failed on s3://%s (n=%d): %s", bucket, len(batch), e)
        return 0, len(batch)
    errs = resp.get("Errors", []) or []
    for err in errs[:5]:
        LOGGER.error(
            "s3 delete error: bucket=%s key=%s code=%s message=%s",
            bucket, err.get("Key"), err.get("Code"), err.get("Message"),
        )
    return len(batch) - len(errs), len(errs)


def _ids_by_index(rows: list[dict], id_key: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for r in rows:
        for index in r.get("es_indices") or []:
            out.setdefault(index, []).append(r[id_key])
    return out


def confirm(prompt: str, expected: str) -> bool:
    try:
        answer = input(prompt).strip()
    except EOFError:
        return False
    return answer == expected


def cmd_execute(args) -> int:
    phases = [p for p, on in (("ccslc", args.ccslc), ("l3", args.l3), ("state", args.state)) if on]
    if not phases:
        LOGGER.error("pick at least one of --ccslc, --l3, --state")
        return 2

    dry_run = not args.execute
    paths = run_paths(args)
    cfg = load_sds_config(args.config)
    es = build_opensearch_client(args, cfg)
    s3_client = boto3.client("s3")

    manifests: dict[str, list[dict]] = {}
    if "ccslc" in phases:
        manifests["ccslc"] = read_jsonl(paths["ccslc"])
    if "l3" in phases:
        manifests["l3"] = read_jsonl(paths["l3"])
    if "state" in phases:
        manifests["state"] = read_jsonl(paths["csc"])

    if args.frames:
        wanted = {f.frame_id for f in parse_frames_arg(args.frames)}
        for key in manifests:
            manifests[key] = [r for r in manifests[key] if r["frame_id"] in wanted]
        LOGGER.info("restricted to frames %s", sorted(wanted))

    total_docs = sum(
        sum(len(r.get("es_indices") or []) for r in rows) for rows in manifests.values()
    )
    total_s3 = sum(
        sum(1 for r in rows if r.get("in_s3")) for rows in manifests.values()
    )
    total_rows = sum(len(rows) for rows in manifests.values())

    print()
    print(f"{'DRY RUN - nothing will be deleted' if dry_run else 'EXECUTE - this deletes data'}")
    print(f"  label            : {args.priority_label}")
    print(f"  phases           : {', '.join(phases)}")
    for key, rows in manifests.items():
        frames = sorted({r["frame_id"] for r in rows})
        docs = sum(len(r.get("es_indices") or []) for r in rows)
        s3n = sum(1 for r in rows if r.get("in_s3"))
        print(
            f"  {key:<16} : {len(rows)} granules over {len(frames)} frames "
            f"({docs} GRQ docs, {s3n} S3 datasets)"
        )
    print(f"  GRQ docs total   : {total_docs}")
    print(f"  S3 datasets total: {total_s3}")
    print()

    if total_rows == 0:
        print("manifest is empty - nothing to do")
        return 0

    # The cap is about blast radius, so it counts what actually gets deleted.
    # Products that only exist at the DAAC contribute rows but no local deletes;
    # they leave through the removal list instead.
    actionable = total_docs + total_s3
    if actionable == 0:
        print("nothing to delete locally - these products only exist in CMR")
        print("run 'asf-list' to produce the DAAC removal list")
        return 0

    if actionable > args.max_targets and not args.force:
        LOGGER.error(
            "this run would delete %d items (%d GRQ docs + %d S3 datasets), over the "
            "--max cap of %d; re-run with --force (or a higher --max) once the count "
            "has been reviewed",
            actionable, total_docs, total_s3, args.max_targets,
        )
        return 2

    if dry_run:
        for key, rows in manifests.items():
            for r in rows[: args.show]:
                target = r.get("granule") or r.get("doc_id")
                print(f"  would delete [{key}] {target}")
            if len(rows) > args.show:
                print(f"  ... and {len(rows) - args.show} more {key} rows")
        print()
        print("dry run only. re-run with --execute to delete.")
        return 0

    if not args.yes:
        if not confirm(
            f"\nType DELETE to remove {total_docs} GRQ docs and {total_s3} S3 datasets: ",
            "DELETE",
        ):
            print("aborted")
            return 1

    stamp = utcstamp()
    results: list[dict] = []
    for key, rows in manifests.items():
        if not rows:
            continue
        id_key = "doc_id" if key == "state" else "granule"
        ids_by_index = _ids_by_index(rows, id_key)

        backup_path = args.run_dir / f"backup_{args.priority_label}_{key}_{stamp}.ndjson"
        backup_docs(es, ids_by_index, backup_path, dry_run=False)
        if not args.skip_park:
            park_docs(es, ids_by_index, f"{PARKED_PREFIX}_{key}", dry_run=False)

        es_deleted, es_errors = delete_es_docs(es, ids_by_index, dry_run=False)

        s3_deleted = 0
        s3_errors = 0
        if key != "state":
            s3_rows = [r for r in rows if r.get("in_s3")]
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for d, e in pool.map(
                    lambda r: delete_s3_prefix(
                        s3_client, r["s3_bucket"], r["s3_prefix"], dry_run=False
                    ),
                    s3_rows,
                ):
                    s3_deleted += d
                    s3_errors += e

        LOGGER.info(
            "%s: deleted %d GRQ docs (%d errors), %d S3 objects (%d errors)",
            key, es_deleted, es_errors, s3_deleted, s3_errors,
        )
        results.append(
            {
                "phase": key,
                "granules": len(rows),
                "es_deleted": es_deleted,
                "es_errors": es_errors,
                "s3_objects_deleted": s3_deleted,
                "s3_errors": s3_errors,
                "backup": str(backup_path),
                "parked_index": None if args.skip_park else f"{PARKED_PREFIX}_{key}",
                "completed": datetime.now(timezone.utc).isoformat(),
            }
        )

    results_path = args.run_dir / f"results_{args.priority_label}_{stamp}.json"
    results_path.write_text(json.dumps(results, indent=2))
    print()
    print(f"done. results: {results_path}")
    for r in results:
        print(
            f"  {r['phase']:<8} GRQ {r['es_deleted']} deleted / {r['es_errors']} errors"
            f" | S3 objects {r['s3_objects_deleted']} deleted / {r['s3_errors']} errors"
        )
    print()
    print("run 'verify' next.")
    return 1 if any(r["es_errors"] or r["s3_errors"] for r in results) else 0


# ---------------------------------------------------------------------------
# subcommand: verify
# ---------------------------------------------------------------------------

def cmd_verify(args) -> int:
    paths = run_paths(args)
    if not paths["audit"].exists():
        LOGGER.error("no audit at %s - run 'audit' first", paths["audit"])
        return 2
    before = json.loads(paths["audit"].read_text())
    cfg = load_sds_config(args.config)
    es = build_opensearch_client(args, cfg)
    lts_bucket, rs_bucket = resolve_buckets(args, cfg)
    s3_client = boto3.client("s3")
    cbdb = load_cbdb(args.cbdb)

    prior = {int(f["frame_id"]): f for f in before["frames"]}
    frame_ids = sorted(prior)
    if args.frames:
        wanted = {f.frame_id for f in parse_frames_arg(args.frames)}
        frame_ids = [f for f in frame_ids if f in wanted]

    rows = []
    failures = 0
    for fid in frame_ids:
        old = prior[fid]
        frame_in = FrameInput(
            frame_id=fid, reported_gaps=[dict(g) for g in old.get("reported_gaps", [])]
        )
        audit, c_rows, l_rows, _ = audit_frame(
            frame_in,
            priority=args.priority_label,
            cbdb=cbdb,
            gap_days=args.gap_days,
            k=args.k,
            es=es,
            s3_client=s3_client,
            lts_bucket=lts_bucket,
            rs_bucket=rs_bucket,
            use_cmr=False,  # CMR is unaffected by a local purge
            cmr_endpoint=args.cmr_endpoint,
            include_state=False,
        )
        old_counts = old.get("counts", {})
        new_counts = audit.counts
        checks = {
            "ccslc_affected_gone": new_counts.get("ccslc_affected_grq", 0) == 0
            and new_counts.get("ccslc_affected_s3", 0) == 0,
            "l3_affected_gone": new_counts.get("l3_affected_grq", 0) == 0
            and new_counts.get("l3_affected_s3", 0) == 0,
            "ccslc_keep_intact": new_counts.get("ccslc_keep_grq", 0)
            == old_counts.get("ccslc_keep_grq", 0)
            and new_counts.get("ccslc_keep_s3", 0) == old_counts.get("ccslc_keep_s3", 0),
            "l3_keep_intact": new_counts.get("l3_keep_grq", 0) == old_counts.get("l3_keep_grq", 0)
            and new_counts.get("l3_keep_s3", 0) == old_counts.get("l3_keep_s3", 0),
            "boundary_unchanged": audit.last_clean_boundary_date
            == old.get("last_clean_boundary_date"),
        }
        ok = all(checks.values())
        if not ok:
            failures += 1
        rows.append(
            {
                "frame_id": fid,
                "ok": ok,
                "checks": checks,
                "before": old_counts,
                "after": new_counts,
                "boundary_before": old.get("last_clean_boundary_date"),
                "boundary_after": audit.last_clean_boundary_date,
            }
        )
        LOGGER.info("verify frame %s: %s", fid, "PASS" if ok else "FAIL")

    lines = [
        f"# DISP-S1 large-gap purge verification - {args.priority_label}",
        "",
        f"generated: {datetime.now(timezone.utc).isoformat()}",
        f"audit reference: {paths['audit']}",
        "",
        f"**{len(rows) - failures}/{len(rows)} frames pass**",
        "",
        "| frame | result | CCSLC left aff. | CCSLC kept (before/after) | "
        "DISP-S1 left aff. | DISP-S1 kept (before/after) |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in rows:
        a, b = r["after"], r["before"]
        lines.append(
            f"| {r['frame_id']} | {'PASS' if r['ok'] else 'FAIL'} | "
            f"{a.get('ccslc_affected_grq', 0)} GRQ / {a.get('ccslc_affected_s3', 0)} S3 | "
            f"{b.get('ccslc_keep_grq', 0)}/{a.get('ccslc_keep_grq', 0)} GRQ, "
            f"{b.get('ccslc_keep_s3', 0)}/{a.get('ccslc_keep_s3', 0)} S3 | "
            f"{a.get('l3_affected_grq', 0)} GRQ / {a.get('l3_affected_s3', 0)} S3 | "
            f"{b.get('l3_keep_grq', 0)}/{a.get('l3_keep_grq', 0)} GRQ, "
            f"{b.get('l3_keep_s3', 0)}/{a.get('l3_keep_s3', 0)} S3 |"
        )
    if failures:
        lines += ["", "## Failures", ""]
        for r in rows:
            if not r["ok"]:
                bad = [k for k, v in r["checks"].items() if not v]
                lines.append(f"* {r['frame_id']}: {', '.join(bad)}")
    paths["verify"].write_text("\n".join(lines) + "\n")
    (args.run_dir / f"verify_{args.priority_label}.json").write_text(
        json.dumps(rows, indent=2)
    )
    print()
    print(f"verification: {len(rows) - failures}/{len(rows)} frames pass -> {paths['verify']}")
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# logging / cli
# ---------------------------------------------------------------------------

def setup_logging(log_file: Path | None, verbose: bool) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    LOGGER.addHandler(sh)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=5)
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        LOGGER.addHandler(fh)


def add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--run-dir", type=Path, required=True,
                   help="directory for manifests, reports and logs")
    p.add_argument("--priority-label", default=None,
                   help="label for this run's files (default: derived from the frame input)")
    p.add_argument("--config", type=Path, default=Path("~/.sds/config").expanduser(),
                   help="path to .sds/config for ES and bucket settings")
    p.add_argument("--netrc-os", default="~/.netrc-os",
                   help="netrc file holding OpenSearch credentials (used when https)")
    p.add_argument("--es-url", help="OpenSearch URL (overrides the config file)")
    p.add_argument("--es-user", help="OpenSearch basic-auth user")
    p.add_argument("--es-password", help="OpenSearch basic-auth password")
    p.add_argument("--verify-certs", action="store_true",
                   help="verify TLS certificates (off by default: the cluster uses self-signed certs)")
    p.add_argument("--lts-bucket", help="override the CCSLC bucket (default LTS_BUCKET)")
    p.add_argument("--rs-bucket", help="override the DISP-S1 bucket (default DATASET_BUCKET)")
    p.add_argument("--cbdb", type=Path, help="consistent burst database JSON")
    p.add_argument("--gap-days", type=int, default=DEFAULT_GAP_DAYS,
                   help=f"a gap larger than this many days is a large gap (default {DEFAULT_GAP_DAYS})")
    p.add_argument("-k", type=int, default=DEFAULT_K, dest="k",
                   help=f"k-cycle length (default {DEFAULT_K})")
    p.add_argument("--cmr-endpoint", default="OPS", choices=["OPS", "UAT"])
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--log-file", type=Path)
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--dry-run", action="store_true",
                   help="explicit no-op: every subcommand is already read-only unless "
                        "'execute --execute' is used")


def add_frame_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--gap-list", type=Path,
                   help="large-gap report (FRAME / GAP lines), e.g. prior0_large_gap.txt")
    p.add_argument("--frames-file", type=Path, help='JSON file of the form {"frames": [...]}')
    p.add_argument("--frames", help="comma-separated frame ids")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="delete_disp_s1_large_gap.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)

    a = sub.add_parser("audit", help="inventory and classify (read-only)")
    add_common_args(a)
    add_frame_args(a)
    a.add_argument("--no-cmr", action="store_true",
                   help="skip CMR queries (no DAAC removal list can be produced)")
    a.add_argument("--state", action="store_true",
                   help="also collect affected cycle-state-config docs")
    a.set_defaults(func=cmd_audit)

    l = sub.add_parser("asf-list", help="write the DAAC removal list from an audit")
    add_common_args(l)
    l.set_defaults(func=cmd_asf_list)

    e = sub.add_parser("execute", help="delete the manifest rows (dry-run by default)")
    add_common_args(e)
    e.add_argument("--ccslc", action="store_true", help="purge CCSLCs")
    e.add_argument("--l3", action="store_true", help="purge DISP-S1 products")
    e.add_argument("--state", action="store_true", help="purge cycle-state-config docs")
    e.add_argument("--execute", action="store_true",
                   help="actually delete; without this the run is a dry run")
    e.add_argument("--yes", action="store_true", help="skip the typed DELETE confirmation")
    e.add_argument("--frames", help="restrict to these frame ids (comma-separated)")
    e.add_argument("--max", dest="max_targets", type=int, default=DEFAULT_HARD_CAP,
                   help=f"refuse to delete more than this many granules (default {DEFAULT_HARD_CAP})")
    e.add_argument("--force", action="store_true", help="override the --max cap")
    e.add_argument("--skip-park", action="store_true",
                   help="skip copying docs to the parked index (the NDJSON backup is always written)")
    e.add_argument("--show", type=int, default=20, help="rows to list in a dry run")
    e.set_defaults(func=cmd_execute)

    v = sub.add_parser("verify", help="re-inventory and check the purge")
    add_common_args(v)
    v.add_argument("--frames", help="restrict to these frame ids (comma-separated)")
    v.set_defaults(func=cmd_verify)

    args = p.parse_args(argv)

    if getattr(args, "dry_run", False) and getattr(args, "execute", False):
        p.error("--dry-run and --execute contradict each other")

    if args.priority_label is None:
        source = getattr(args, "gap_list", None) or getattr(args, "frames_file", None)
        if source:
            args.priority_label = Path(source).stem.replace("_large_gap", "")
        elif args.command == "audit":
            args.priority_label = "run"
        else:
            # asf-list/execute/verify work off an existing audit: infer the label
            # rather than silently reading the wrong manifest.
            found = sorted(args.run_dir.glob("audit_*.json"))
            if len(found) == 1:
                args.priority_label = found[0].stem[len("audit_"):]
            elif not found:
                p.error(
                    f"no audit_*.json in {args.run_dir} - run 'audit' first, or pass "
                    "--priority-label"
                )
            else:
                labels = ", ".join(f.stem[len("audit_"):] for f in found)
                p.error(f"--priority-label is required; {args.run_dir} holds: {labels}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    log_file = args.log_file or (args.run_dir / f"{args.command}_{args.priority_label}.log")
    setup_logging(log_file, args.verbose)
    LOGGER.info("%s %s", Path(sys.argv[0]).name, " ".join(sys.argv[1:]))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
