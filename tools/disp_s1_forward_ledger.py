#!/usr/bin/env python3
"""Account for every DISP-S1 forward sensing date of a set of frames. Read-only.

Gives every sensing date of every selected frame exactly one outcome, ties every L3 to one
date, checks each L3 is in GRQ, in S3 and delivered, and says for each frame what state it
is in and what an operator should do next. It answers the questions a forward campaign
raises and nothing else reconciles: which frames are making products, which have stopped
and why, and what is owed.

Expected dates are the frames' cycle state configs (CSCs) in GRQ -- live forward writes one
for every date it sees. --cmr re-queries OPS CMR for the frames' CSLC-S1 (VV, per burst) to
add the dates not catalogued yet (from each frame's forward start: its first KSC that is not
superseded), and sorts each stopped date's missing bursts into "not
imaged" (ASF SENTINEL-1_BURSTS) or "imaged, no OPERA CSLC". Without --cmr nothing calls CMR.

    python tools/disp_s1_forward_ledger.py --region 0
    python tools/disp_s1_forward_ledger.py --frame-id 9154 38504 --since 20260401 --cmr
    python tools/disp_s1_forward_ledger.py --region 0 --until 20261012 \\
        --products-since 2026-10-13T15:50:00Z --cmr --json ledger.json --csv ledger.csv \\
        --manifest l3_manifest.csv
    python tools/plot_disp_s1_forward_ledger.py ledger.json ledger.png

Outcomes, one per date:

    produced              exactly one forward L3 (and, on a k-boundary, a full CCSLC set
                          with exactly one disp_s1-ccslc-set marker)
    produced (historical) exactly one L3 from the historical SCIFLO for this secondary date
    in progress           fired; SCIFLO queued or running, or its trigger still pending
    superseded            the KSC is superseded (existing CCSLC, or a historical phase owns
                          the date); no forward product
    excluded (burst DB)   the consistent burst database left the pass out (db_excluded)
    blackout              inside the frame's blackout window
    skipped               an operator skipped the date (tools/skip_disp_s1_csc.py); reason noted
    stopped               partial CSC and no KSC: the frame cannot get past it (the first such
                          date of the frame)
    behind a stop         a later date with no KSC: a partial CSC (with --cmr the note says
                          whether CMR has every burst, i.e. not fully catalogued yet, or not,
                          i.e. another partial date), or (--cmr) a date not catalogued at all
    partial (passed)      partial CSC and no KSC, but later dates fired: it is outside every
                          later k-window and lineage, so nothing waits on it
    held behind a gap     KSC gap_unresolved; the note names the partial date holding it
    window filling        KSC incomplete, its k-window not full yet
    awaiting CCSLC        KSC complete, waiting for an earlier boundary's CCSLC set
    awaiting inputs       full window, still missing CCSLCs, static layers or ionosphere
    not whitelisted       KSC would fire, but its region is not in the
                          trigger-SCIFLO_L3_DISP_S1 whitelist
    pending               (--cmr) in OPS CMR, not catalogued yet; or a CSC the k-cycle
                          evaluator has not reached yet
    UNEXPLAINED           anything else: duplicate L3s, fired with no L3 and no SCIFLO
                          running, a failed SCIFLO, a complete CSC with no KSC, an L3 with
                          no KSC

Products: every L3 in GRQ (from --products-since, when given) and every DISP_S1 product
directory in the dataset bucket. An L3 missing from S3, an S3 product GRQ does not know
and an L3 not delivered after --delivery-grace-mins are product problems.

Exit code: 0 when everything is accounted for and every product is in place (dates still in
progress count as accounted for); 1 when anything is UNEXPLAINED or there is a product
problem; 2 when the only findings are frames stopped or held behind a partial date. So it
can run as a scheduled health check.

--manifest writes one row per L3 with its S3 location and files: what to hand to whoever
validates the products.
"""

import argparse
import csv
import json
import logging
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from data_subscriber.cslc import disp_s1_constants as c
from tools.disp_s1_duplicate_audit import CCSLC_RE, CCSLC_SET_RE, KSC_IN_JOB_RE, L3_RE, _scroll

logger = logging.getLogger("disp_s1_forward_ledger")

CSC_INDEX = "grq_*_cslc_s1-cycle-state-config*"
KSC_INDEX = "grq_*_disp_s1-kcycle-state-config*"
L3_INDEX = "grq_*_l3_disp_s1-*"
CCSLC_INDEX = "grq_*_l2_cslc_s1_compressed-*"
CCSLC_SET_INDEX = "grq_*_disp_s1-ccslc-set-*"
JOB_STATUS_INDEX = "job_status-*"
RULES_INDEX = "user_rules-grq"
SCIFLO_RULE = "trigger-SCIFLO_L3_DISP_S1"
SCIFLO_JOB_TYPE_PREFIX = "job-SCIFLO_L3_DISP_S1:"
S3_PRODUCT_PREFIX = "products/DISP_S1/"
BURSTS_COLLECTION = "C2709161906-ASF"   # ASF SENTINEL-1_BURSTS: was the burst imaged?

# The skip fields arrived with tools/skip_disp_s1_csc.py; read them by name so this also runs
# against a build that predates it.
SKIPPED = getattr(c, "SKIPPED", "skipped")
SKIP_REASON = getattr(c, "SKIP_REASON", "skip_reason")
SKIPPED_BY = getattr(c, "SKIPPED_BY", "skipped_by")
SKIPPED_AT = getattr(c, "SKIPPED_AT", "skipped_at")

# Outcomes, in report order.
PRODUCED = "produced"
PRODUCED_HIST = "produced (historical)"
IN_PROGRESS = "in progress"
SUPERSEDED = "superseded"
EXCLUDED_DB = "excluded (burst DB)"
BLACKOUT = "blackout"
SKIPPED_OUT = "skipped"
STOPPED = "stopped"
PARTIAL_PASSED = "partial (passed)"
BEHIND_STOP = "behind a stop"
HELD = "held behind a gap"
FILLING = "window filling"
AWAITING_CCSLC = "awaiting CCSLC"
AWAITING_INPUTS = "awaiting inputs"
NOT_WHITELISTED = "not whitelisted"
PENDING = "pending"
UNEXPLAINED = "UNEXPLAINED"
OUTCOMES = [PRODUCED, PRODUCED_HIST, IN_PROGRESS, SUPERSEDED, EXCLUDED_DB, BLACKOUT, SKIPPED_OUT, PARTIAL_PASSED,
            STOPPED, BEHIND_STOP, HELD, FILLING, AWAITING_CCSLC, AWAITING_INPUTS, NOT_WHITELISTED, PENDING, UNEXPLAINED]
# a date with one of these after a partial date shows the frame got past the partial
PAST_IT = (PRODUCED, PRODUCED_HIST, IN_PROGRESS, AWAITING_CCSLC, NOT_WHITELISTED)
# still moving on its own: no operator action, and not a finding
WAITING = (IN_PROGRESS, FILLING, AWAITING_CCSLC, AWAITING_INPUTS, PENDING)

ACTIVE_JOB = ("job-queued", "job-started")
DUP_SKIP_MSG_PREFIX = "dup skip"
KSC_ID_RE = re.compile(r"^disp_s1-kcycle-k\d+-m\d+-f(\d+)-(\d{8})-state-config$")
CSLC_UR_RE = re.compile(r"^OPERA_L2_CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8})T\d{6}Z_\d{8}T\d{6}Z_(S1[A-D])_(VV|VH|HH|HV)_")
GAP_DATE_RE = re.compile(r"(\d{8}) \(\d+/\d+\)")
TRACK_RE = re.compile(r"^T(\d{3})-")

# A KSC or CSC this young may simply not have been picked up by the next stage yet.
SETTLE_MINS = 30


# ---------------------------------------------------------------------------- helpers

def ymd(text):
    return text[:10].replace("-", "")


def parse_ts(text):
    """GRQ/Mozart timestamp ('2026-10-13T19:02:26.278', optional Z) as an aware UTC datetime, or None."""
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).rstrip("Z"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def minutes_old(text, now):
    ts = parse_ts(text)
    return (now - ts).total_seconds() / 60 if ts else None


def ksc_fires(meta):
    """Would trigger-SCIFLO_L3_DISP_S1 fire on this KSC, region whitelist aside?"""
    return bool(meta and meta.get(c.IS_COMPLETE) and meta.get(c.COMPRESSED_CSLC_FINAL)
                and not meta.get(c.GAP_UNRESOLVED) and not meta.get(c.SUPERSEDED_BY))


def track_of(burst_ids):
    tracks = Counter(int(m.group(1)) for b in burst_ids or () if (m := TRACK_RE.match(b)))
    return tracks.most_common(1)[0][0] if tracks else None


def s3_location(urls):
    """(bucket, key) of a GRQ dataset's s3 url; tolerates 's3://s3-us-west-2.amazonaws.com:80/<bucket>/<key>'."""
    for u in urls or ():
        if not u.startswith("s3://"):
            continue
        parts = u[len("s3://"):].split("/")
        if parts and "amazonaws.com" in parts[0]:
            parts = parts[1:]
        if len(parts) >= 2:
            return parts[0], "/".join(parts[1:]).rstrip("/")
    return None, None


def whitelist_from_query(query_string):
    """Regions trigger-SCIFLO_L3_DISP_S1 fires for, or None when it does not filter on region."""
    q = json.loads(query_string) if isinstance(query_string, str) else query_string
    for clause in q.get("bool", {}).get("must", []):
        terms = clause.get("terms", {})
        if "metadata.region_id" in terms:
            return [str(r) for r in terms["metadata.region_id"]]
    return None


# ---------------------------------------------------------------------------- loading (read-only)

def load_grq(es, frames, since, until, products_since):
    fq = {"terms": {"metadata.frame_id": sorted(frames)}}
    window = {}
    if since:
        window["gte"] = since
    if until:
        window["lte"] = until
    in_window = [fq] + ([{"range": {"metadata.sensing_date": window}}] if window else [])
    created = [{"range": {"creation_timestamp": {"gte": products_since}}}] if products_since else []

    csc = {}
    for h in _scroll(es, CSC_INDEX, {"bool": {"must": in_window}}, ["metadata", "creation_timestamp"]):
        m = h["_source"]["metadata"]
        csc[(int(m[c.FRAME_ID]), m[c.SENSING_DATE])] = dict(m, _created=h["_source"].get("creation_timestamp"))
    ksc = {}
    keep = [c.IS_COMPLETE, c.COMPRESSED_CSLC_FINAL, c.SAVE_COMPRESSED_CSLC, c.SUPERSEDED_BY, c.GAP_UNRESOLVED,
            c.COMPLETENESS_REASON, c.CYCLES_COMPLETE, c.CYCLES_EXPECTED, c.COMPRESSED_CSLC_PENDING, c.REGION_ID,
            c.FRAME_ID, c.SENSING_DATE]
    for h in _scroll(es, KSC_INDEX, {"bool": {"must": in_window}},
                     [f"metadata.{k}" for k in keep] + ["creation_timestamp"]):
        m = h["_source"]["metadata"]
        ksc[(int(m[c.FRAME_ID]), m[c.SENSING_DATE])] = dict(m, _id=h["_id"],
                                                             _created=h["_source"].get("creation_timestamp"))
    l3 = []
    for h in _scroll(es, L3_INDEX, {"bool": {"must": [fq] + created}},
                     ["metadata.frame_id", "metadata.input_granule_id", "daac_delivery_status",
                      "creation_timestamp", "urls"]):
        s = h["_source"]
        m = L3_RE.match(h["_id"])
        bucket, key = s3_location(s.get("urls"))
        l3.append({"id": h["_id"], "frame": int(s["metadata"]["frame_id"]),
                   "ref": m.group("ref") if m else None, "sec": m.group("sec") if m else None,
                   "input": s["metadata"].get("input_granule_id"), "delivery": s.get("daac_delivery_status"),
                   "created": s.get("creation_timestamp"), "bucket": bucket, "key": key})
    ccslc = defaultdict(set)          # (frame, last date) -> bursts
    for h in _scroll(es, CCSLC_INDEX, {"bool": {"must": [fq] + created}}, False):
        if m := CCSLC_RE.match(h["_id"]):
            ccslc[(int(m.group("frame")), m.group("last"))].add(m.group("burst"))
    markers = Counter()
    for h in _scroll(es, CCSLC_SET_INDEX, {"bool": {"must": created}} if created else {"match_all": {}}, False):
        if (m := CCSLC_SET_RE.match(h["_id"])) and int(m.group("frame")) in frames:
            markers[(int(m.group("frame")), m.group("last"))] += 1
    return {"csc": csc, "ksc": ksc, "l3": l3, "ccslc": ccslc, "markers": markers}


def load_jobs(mozart, products_since):
    """{ksc id: [SCIFLO_L3_DISP_S1 job]}, newest last."""
    must = [{"prefix": {"type": SCIFLO_JOB_TYPE_PREFIX}}]
    if products_since:
        must.append({"range": {"job.job_info.time_queued": {"gte": products_since}}})
    by_ksc = defaultdict(list)
    for h in _scroll(mozart, JOB_STATUS_INDEX, {"bool": {"must": must}},
                     ["job_id", "status", "short_error", "error", "msg", "job.job_info.time_queued"]):
        s = h["_source"]
        if m := KSC_IN_JOB_RE.search(s.get("job_id") or ""):
            by_ksc[m.group(1)].append({
                "job_id": s["job_id"], "status": s.get("status"), "msg": str(s.get("msg") or ""),
                "error": str(s.get("short_error") or s.get("error") or "")[:160],
                "queued": ((s.get("job") or {}).get("job_info") or {}).get("time_queued")})
    for jobs in by_ksc.values():
        jobs.sort(key=lambda j: j.get("queued") or "")
    return by_ksc


def load_whitelist(mozart):
    """The deployed trigger-SCIFLO_L3_DISP_S1 region whitelist; None means it fires for every region."""
    hits = mozart.search(index=RULES_INDEX, body={"query": {"term": {"rule_name": SCIFLO_RULE}}})["hits"]["hits"]
    if not hits:
        raise ValueError(f"no {SCIFLO_RULE} rule in {RULES_INDEX}")
    return whitelist_from_query(hits[0]["_source"]["query_string"])


def load_s3(bucket):
    """{product id: set(file names)} under products/DISP_S1/."""
    prods = defaultdict(set)
    try:
        import boto3
        pages = boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=S3_PRODUCT_PREFIX)
        keys = (o["Key"] for page in pages for o in page.get("Contents", []))
    except ImportError:
        out = subprocess.run(["aws", "s3", "ls", "--recursive", f"s3://{bucket}/{S3_PRODUCT_PREFIX}"],
                             capture_output=True, text=True, check=True).stdout
        keys = (line.split()[-1] for line in out.splitlines() if line.strip())
    for key in keys:
        parts = key.split("/")
        if len(parts) >= 5:              # products/DISP_S1/F<frame>/<product id>/<file>
            prods[parts[3]].add(parts[-1])
    return prods


class Cmr:
    """OPS CMR now: per frame, the VV bursts and platforms per sensing date; ASF burst imaging."""

    def __init__(self, until):
        from util.conf_util import SettingsConf
        import data_subscriber.cslc.cslc_catalog_ingest as cci_mod
        settings = SettingsConf().cfg
        self.cci = cci_mod.CslcCatalogIngest(settings, es_conn=None)
        self.host, self.token, *_ = cci_mod.get_cmr_token("OPS", settings)
        self.end = datetime.strptime(until, "%Y%m%d").strftime("%Y-%m-%dT23:59:59Z")

    def dates(self, frame, start_ymd):
        start = datetime.strptime(start_ymd, "%Y%m%d").strftime("%Y-%m-%dT00:00:00Z")
        by_date, plat = defaultdict(set), defaultdict(set)
        for it in self.cci._query_cmr_for_frame(frame, start, self.end, self.host, self.token):
            if (m := CSLC_UR_RE.match(it.get("umm", {}).get("GranuleUR", ""))) and m.group(4) == "VV":
                by_date[m.group(2)].add(m.group(1))
                plat[m.group(2)].add(m.group(3))
        return by_date, plat

    def imaged(self, bursts, date):
        """Which of these OPERA burst ids (T173-370326-IW3) ASF has an S1 burst SLC for on this date."""
        if not bursts:
            return set()
        native = {f"S1_{b.split('-')[1]}_{b.split('-')[2]}_{date}T": b for b in bursts}
        params = [("collection_concept_id", BURSTS_COLLECTION), ("options[native-id][pattern]", "true"),
                  ("page_size", "500")] + [("native-id[]", f"{n}*_VV_*") for n in native]
        url = f"https://{self.host}/search/granules.umm_json?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=120) as r:
            items = json.loads(r.read()).get("items", [])
        found = set()
        for it in items:
            gid = it.get("meta", {}).get("native-id") or it.get("umm", {}).get("GranuleUR", "")
            for prefix, b in native.items():
                if gid.startswith(prefix):
                    found.add(b)
        return found


def root_cause(missing, filled, imaged):
    if filled:
        return "filled since: OPS CMR now has every burst"
    if not imaged:
        return f"acquisition gap: {len(missing)} burst(s) not imaged"
    if imaged == missing:
        return f"OPS production gap: {len(imaged)} burst(s) imaged, no OPERA CSLC"
    return f"mixed: {len(imaged)} imaged without CSLC, {len(missing - imaged)} not imaged"


# ---------------------------------------------------------------------------- accounting

def classify(d, csc, ksc, fwd, hist, jobs, nb, ccslc_bursts, markers, whitelist, now):
    """(outcome, note) for one frame/date.

    csc/ksc are the date's state configs (or None), fwd/hist the forward and historical L3s
    for the date, jobs the SCIFLO_L3_DISP_S1 runs of its KSC, nb the frame's burst count,
    ccslc_bursts/markers what published at this date as a k-boundary.
    """
    boundary = bool(ksc and ksc.get(c.SAVE_COMPRESSED_CSLC) and not ksc.get(c.SUPERSEDED_BY))
    active = [j for j in jobs if j["status"] in ACTIVE_JOB]

    if len(fwd) > 1 or len(hist) > 1:
        return UNEXPLAINED, f"{len(fwd) + len(hist)} L3s for one date (duplicate)"
    if fwd:
        if ksc is None:
            return UNEXPLAINED, "forward L3 with no KSC"
        if boundary and (ccslc_bursts < (nb or ccslc_bursts) or markers != 1):
            if active:
                return IN_PROGRESS, "L3 published; CCSLC set still publishing"
            return UNEXPLAINED, f"boundary CCSLC set {ccslc_bursts}/{nb} bursts, {markers} marker(s)"
        return PRODUCED, ""
    if hist:
        return PRODUCED_HIST, ""

    if csc is not None and csc.get(c.BLACKOUT):
        return BLACKOUT, ""
    if csc is not None and csc.get(SKIPPED):
        return SKIPPED_OUT, (f"{csc.get(SKIP_REASON) or ''} (by {csc.get(SKIPPED_BY)} "
                             f"at {csc.get(SKIPPED_AT)})")
    if csc is not None and csc.get(c.DB_EXCLUDED):
        return EXCLUDED_DB, ""

    if ksc is None:
        if csc is None:
            return PENDING, "no cycle state config yet"
        if not csc.get(c.IS_COMPLETE):
            return STOPPED, csc.get(c.COMPLETENESS_REASON) or "partial cycle"
        age = minutes_old(csc.get("_created"), now)
        if age is not None and age < SETTLE_MINS:
            return PENDING, "complete CSC; k-cycle evaluator not done yet"
        return UNEXPLAINED, "complete CSC but no KSC (missed k-cycle trigger)"

    if ksc.get(c.SUPERSEDED_BY):
        return SUPERSEDED, f"superseded_by={ksc.get(c.SUPERSEDED_BY)}"
    if ksc_fires(ksc):
        if whitelist is not None and str(ksc.get(c.REGION_ID)) not in whitelist:
            return NOT_WHITELISTED, f"region {ksc.get(c.REGION_ID)} not in {whitelist}"
        if active:
            return IN_PROGRESS, "SCIFLO " + active[-1]["status"].replace("job-", "")
        if jobs:
            last = jobs[-1]
            if last["status"] == "job-failed":
                return UNEXPLAINED, f"SCIFLO failed: {last['error']}"
            if last["msg"].startswith(DUP_SKIP_MSG_PREFIX):
                return UNEXPLAINED, "SCIFLO found its product already published, but no L3 is in scope"
            return UNEXPLAINED, f"SCIFLO {last['status'].replace('job-', '')} without publishing an L3"
        age = minutes_old(ksc.get("_created"), now)
        if age is not None and age < SETTLE_MINS:
            return IN_PROGRESS, "KSC fired; SCIFLO trigger pending"
        return UNEXPLAINED, "fired, but no SCIFLO job found"
    if ksc.get(c.GAP_UNRESOLVED):
        return HELD, ksc.get(c.COMPLETENESS_REASON) or "gap_unresolved"
    if ksc.get(c.IS_COMPLETE):
        pending = ksc.get(c.COMPRESSED_CSLC_PENDING) or []
        return AWAITING_CCSLC, f"awaiting CCSLC sets at {','.join(pending)}" if pending else "awaiting CCSLC"
    if (ksc.get(c.CYCLES_COMPLETE) or 0) < (ksc.get(c.CYCLES_EXPECTED) or 0):
        return FILLING, f"k-window {ksc.get(c.CYCLES_COMPLETE)}/{ksc.get(c.CYCLES_EXPECTED)}"
    return AWAITING_INPUTS, ksc.get(c.COMPLETENESS_REASON) or ""


def blank_row(frame, track, date):
    row = {k: "" for k in ROW_COLUMNS}
    row.update({"frame": frame, "track": track, "date": date, "skipped": False, "boundary": False,
                "l3_count": 0, "delivered": 0})
    return row


def account(frames, data, frame_bursts, whitelist, now, s3=None, cmr=None, since=None, until=None,
            products_since=None, delivery_check=True, delivery_grace_mins=180):
    """Rows (one per frame/date), products (one per L3) and per-frame summaries."""
    csc, ksc, ccslc, markers = data["csc"], data["ksc"], data["ccslc"], data["markers"]
    jobs = data.get("jobs") or {}

    fwd_l3, hist_l3 = defaultdict(list), defaultdict(list)
    for p in data["l3"]:
        if (m := KSC_ID_RE.match(p.get("input") or "")) and int(m.group(1)) == p["frame"]:
            p["source"], p["key_date"] = "forward", m.group(2)
            fwd_l3[(p["frame"], m.group(2))].append(p)
        else:
            p["source"], p["key_date"] = "historical", p["sec"]
            hist_l3[(p["frame"], p["sec"])].append(p)

    def in_window(date):
        return date and (not since or date >= since) and (not until or date <= until)

    rows, info = [], {}
    for f in frames:
        bursts = frame_bursts.get(f)
        nb = len(bursts) if bursts else None
        info[f] = {"track": track_of(bursts), "bursts": nb, "in_burst_db": bursts is not None}
        dates = {d for (ff, d) in list(csc) + list(ksc) if ff == f}
        dates |= {d for (ff, d) in list(fwd_l3) + list(hist_l3) if ff == f and in_window(d)}
        frame_rows = {}
        for d in sorted(dates):
            k, cc = ksc.get((f, d)), csc.get((f, d))
            outcome, note = classify(d, cc, k, fwd_l3.get((f, d), []), hist_l3.get((f, d), []),
                                     jobs.get(k["_id"], []) if k else [], nb,
                                     len(ccslc.get((f, d), ())), markers.get((f, d), 0), whitelist, now)
            prods = fwd_l3.get((f, d), []) + hist_l3.get((f, d), [])
            js = jobs.get(k["_id"], []) if k else []
            boundary = bool(k and k.get(c.SAVE_COMPRESSED_CSLC) and not k.get(c.SUPERSEDED_BY))
            row = blank_row(f, info[f]["track"], d)
            row.update({"outcome": outcome, "note": note,
                   "csc_coverage": f"{cc.get(c.COVERAGE_ACTUAL)}/{cc.get(c.COVERAGE_EXPECTED)}" if cc else "",
                   "csc_complete": cc.get(c.IS_COMPLETE) if cc else None,
                   "db_excluded": cc.get(c.DB_EXCLUDED) if cc else None,
                   "skipped": bool(cc and cc.get(SKIPPED)),
                   "skip_reason": cc.get(SKIP_REASON) if cc and cc.get(SKIPPED) else None,
                   "missing_bursts": " ".join(cc.get(c.MISSING_BURST_IDS) or []) if cc and not cc.get(c.IS_COMPLETE) else "",
                   "ksc_complete": k.get(c.IS_COMPLETE) if k else None,
                   "ksc_final": k.get(c.COMPRESSED_CSLC_FINAL) if k else None,
                   "gap_unresolved": k.get(c.GAP_UNRESOLVED) if k else None,
                   "superseded_by": k.get(c.SUPERSEDED_BY) if k else None,
                   "boundary": boundary,
                   "ccslc_bursts": len(ccslc.get((f, d), ())) if boundary else None,
                   "ccslc_markers": markers.get((f, d), 0) if boundary else None,
                   "l3_count": len(prods), "l3_ids": " ".join(p["id"] for p in prods),
                   "delivered": sum(p.get("delivery") == "SUCCESS" for p in prods),
                   "sciflo": ",".join(j["status"].replace("job-", "") for j in js)})
            frame_rows[d] = row
            rows.append(row)

        # a partial date the frame already got past blocks nothing (it predates the lineage of every
        # later k-cycle that fired)
        for d, r in frame_rows.items():
            if r["outcome"] == STOPPED and any(o["outcome"] in PAST_IT for dd, o in frame_rows.items() if dd > d):
                r["outcome"] = PARTIAL_PASSED

        # only the first partial date stops the frame; later partial dates wait behind it (in a forward
        # test they are often just the bursts a neighbouring frame catalogued)
        stops = sorted(d for d, r in frame_rows.items() if r["outcome"] == STOPPED)
        for d in stops[1:]:
            frame_rows[d]["outcome"] = BEHIND_STOP
            frame_rows[d]["blocker"] = stops[0]
        stops = stops[:1]

        # name the partial date each held KSC is waiting on: the gap check's own detail, else the latest
        # stopped date before it
        for d, r in frame_rows.items():
            if r["outcome"] == HELD:
                named = GAP_DATE_RE.findall(r["note"])
                earlier = [s for s in stops if s < d]
                r["blocker"] = named[0] if named else (earlier[-1] if earlier else "")
                r["note"] = f"held by partial {r['blocker']}" if r["blocker"] else r["note"]

        # forward processing owns a frame's dates from its first KSC that is not superseded (or its first
        # forward L3); a date before that belongs to history, which OPS's products cover
        owned = [d for d, r in frame_rows.items()
                 if r["outcome"] not in (SUPERSEDED, PRODUCED_HIST) and (ksc.get((f, d)) or fwd_l3.get((f, d)))]
        info[f]["forward_start"] = min(owned) if owned else None
        if cmr is not None and info[f]["in_burst_db"]:
            start = max(filter(None, (since, info[f]["forward_start"])), default=None)
            if start:
                by_date, plat = cmr.dates(f, start)
                for d in sorted(by_date):
                    if not in_window(d) or d < start:
                        continue
                    now_n = f"{len(by_date[d])}/{nb}"
                    if d in frame_rows:
                        frame_rows[d]["cmr_now"], frame_rows[d]["platforms"] = now_n, ",".join(sorted(plat[d]))
                        continue
                    full = len(by_date[d]) >= (nb or 0)
                    after = [s for s in stops if s < d]
                    r = blank_row(f, info[f]["track"], d)
                    r.update({"outcome": BEHIND_STOP if after else PENDING, "blocker": after[0] if after else "",
                              "cmr_now": now_n, "platforms": ",".join(sorted(plat[d])),
                              "note": f"in OPS CMR ({'full' if full else 'partial'}), not catalogued yet"
                                      + (f"; behind the stop at {after[0]}" if after else "")})
                    frame_rows[d] = r
                    rows.append(r)
                for d, r in frame_rows.items():
                    if r["outcome"] == BEHIND_STOP and r["csc_coverage"]:
                        full = not (set(bursts) - by_date.get(d, set()))
                        r["note"] = (f"behind the stop at {r['blocker']}; "
                                     + ("CMR has every burst: not fully catalogued yet" if full
                                        else "partial in CMR too: another partial date"))
                for d in stops:
                    r = frame_rows[d]
                    missing = set((csc.get((f, d)) or {}).get(c.MISSING_BURST_IDS) or []) or \
                        (set(bursts) - by_date.get(d, set()))
                    filled = not (set(bursts) - by_date.get(d, set()))
                    imaged = set() if filled else cmr.imaged(missing, d)
                    r["missing_bursts"] = " ".join(sorted(missing))
                    r["root_cause"] = root_cause(missing, filled, imaged)
                    r["note"] = r["root_cause"]
        info[f]["stops"] = stops

    # --- products
    products = []
    rows_by_key = {(r["frame"], r["date"]): r for r in rows}
    grq_ids = set()
    for p in data["l3"]:
        grq_ids.add(p["id"])
        if p["frame"] not in info:
            continue
        r = rows_by_key.get((p["frame"], p["key_date"]))
        in_s3 = (p["id"] in s3) if s3 is not None else None
        problem = []
        if s3 is not None and not in_s3:
            problem.append("not in S3")
        if delivery_check and p.get("delivery") != "SUCCESS":
            age = minutes_old(p.get("created"), now)
            if p.get("delivery") or age is None or age >= delivery_grace_mins:
                problem.append(f"not delivered ({p.get('delivery') or 'no CNM status'})")
        products.append({"id": p["id"], "frame": p["frame"], "ref": p["ref"], "sec": p["sec"],
                         "source": p["source"], "date": p["key_date"], "in_grq": True, "in_s3": in_s3,
                         "s3_files": sorted(s3.get(p["id"], ())) if s3 is not None else [],
                         "bucket": p.get("bucket"), "key": p.get("key"), "delivery": p.get("delivery"),
                         "created": p.get("created"), "outcome": r["outcome"] if r else "ORPHAN (no ledger date)",
                         "problem": "; ".join(problem)})
    cutoff = re.sub(r"[-:]", "", products_since)[:15] + "Z" if products_since else None
    for pid in sorted(set(s3 or ()) - grq_ids):
        m = L3_RE.match(pid)
        if not m or int(m.group("frame")) not in info:
            continue
        produced_at = pid.rsplit("_", 1)[-1]
        if cutoff and produced_at < cutoff:
            continue            # made before this campaign
        products.append({"id": pid, "frame": int(m.group("frame")), "ref": m.group("ref"), "sec": m.group("sec"),
                         "source": "?", "date": "", "in_grq": False, "in_s3": True, "s3_files": sorted(s3[pid]),
                         "bucket": None, "key": None, "delivery": None, "created": None,
                         "outcome": "ORPHAN (in S3, not in GRQ)", "problem": "in S3, not in GRQ"})

    # --- per frame
    for f in frames:
        fr = [r for r in rows if r["frame"] == f]
        fp = [p for p in products if p["frame"] == f]
        cnt = Counter(r["outcome"] for r in fr)
        made = sorted(p["sec"] for p in fp if p["in_grq"] and p["sec"])
        unexplained = sorted((r for r in fr if r["outcome"] == UNEXPLAINED), key=lambda r: r["date"])
        stops = [r for r in fr if r["outcome"] == STOPPED]
        held = [r for r in fr if r["outcome"] == HELD]
        bad = [p for p in fp if p["problem"]]
        if not info[f]["in_burst_db"]:
            state, hint = "not in burst DB", "none: the frame is not in the consistent burst database"
        elif not fr:
            state, hint = "no data", "none: no cycle state configs in the window"
        elif unexplained or bad:
            state = "UNEXPLAINED" if unexplained else "product problem"
            hint = next_action(f, unexplained[0] if unexplained else None, bad[0] if bad else None)
        elif stops:
            state, hint = f"stopped {stops[0]['date']}", stop_action(f, stops[0])
        elif held:
            blocker = held[0]["blocker"] or "?"
            state = f"held by {blocker}"
            hint = stop_action(f, next((r for r in fr if r["date"] == blocker), {"date": blocker}))
        elif any(cnt[o] for o in WAITING):
            state, hint = "in progress", "wait"
        else:
            state, hint = "up to date", ""
        info[f].update({
            "state": state, "hint": hint,
            "summary": {"counts": {o: cnt.get(o, 0) for o in OUTCOMES},
                        "l3_grq": sum(1 for p in fp if p["in_grq"]),
                        "l3_s3": sum(1 for p in fp if p["in_s3"]),
                        "l3_delivered": sum(1 for p in fp if p["delivery"] == "SUCCESS"),
                        "product_problems": len(bad),
                        "boundaries": sum(1 for r in fr if r["boundary"]),
                        "boundaries_ok": sum(1 for r in fr if r["boundary"] and r["outcome"] == PRODUCED),
                        "first_l3": made[0] if made else None, "last_l3": made[-1] if made else None}})
    return rows, products, info


def stop_action(frame, row):
    d, cause = row["date"], row.get("root_cause") or ""
    skip = f"tools/skip_disp_s1_csc.py --frame-id {frame} --date {d} --reason \"...\""
    if cause.startswith("filled since"):
        return f"catalog {d} again (cslc_catalog_ingest) so its CSC completes"
    if cause.startswith("acquisition gap"):
        return f"skip {d}: {skip}"
    if cause.startswith("OPS production gap") or cause.startswith("mixed"):
        return f"get the missing CSLCs produced and catalogued, or skip {d}: {skip}"
    return f"skip {d} with {skip}, or get its missing bursts in (--cmr gives the cause)"


def next_action(frame, row, product):
    if row is not None:
        d, note = row["date"], row["note"]
        if note.startswith("complete CSC but no KSC"):
            return f"re-drive {d}: tools/reevaluate_disp_s1_forward_kscs.py --frame-id {frame} --dates {d} --apply"
        if note.startswith("SCIFLO failed"):
            return f"retry the failed SCIFLO_L3_DISP_S1 job for {d}"
        return f"investigate {d}: {note}"
    return f"investigate {product['id']}: {product['problem']}"


def exit_code(rows, products):
    if any(r["outcome"] == UNEXPLAINED for r in rows) or any(p["problem"] for p in products):
        return 1
    if any(r["outcome"] in (STOPPED, HELD) for r in rows):
        return 2
    return 0


# ---------------------------------------------------------------------------- output

SHORT = {PRODUCED: "prod", PRODUCED_HIST: "hist", IN_PROGRESS: "inprog", SUPERSEDED: "supers",
         EXCLUDED_DB: "excl", BLACKOUT: "blkout", SKIPPED_OUT: "skip", PARTIAL_PASSED: "passed",
         STOPPED: "stop", BEHIND_STOP: "behind", HELD: "held",
         FILLING: "fill", AWAITING_CCSLC: "a.ccslc", AWAITING_INPUTS: "a.inp", NOT_WHITELISTED: "notwl",
         PENDING: "pend", UNEXPLAINED: "UNEXPL"}
ROW_COLUMNS = ["frame", "track", "date", "outcome", "note", "blocker", "csc_coverage", "csc_complete", "db_excluded",
               "skipped", "skip_reason", "missing_bursts", "root_cause", "ksc_complete", "ksc_final",
               "gap_unresolved", "superseded_by", "boundary", "ccslc_bursts", "ccslc_markers", "l3_count",
               "delivered", "sciflo", "cmr_now", "platforms", "l3_ids"]
MANIFEST_COLUMNS = ["frame", "reference_date", "secondary_date", "product_id", "source", "s3_prefix", "nc_url",
                    "file_urls", "delivery", "created"]


def manifest_rows(products):
    """One row per L3 in GRQ: where it is in S3 and its files."""
    out = []
    for p in sorted((p for p in products if p["in_grq"]), key=lambda p: (p["frame"], p["sec"] or "", p["id"])):
        prefix = f"s3://{p['bucket']}/{p['key']}/" if p.get("bucket") and p.get("key") else ""
        files = [prefix + name for name in p["s3_files"]] if prefix else []
        nc = next((u for u in files if u.endswith(".nc")), prefix + p["id"] + ".nc" if prefix else "")
        out.append({"frame": p["frame"], "reference_date": p["ref"], "secondary_date": p["sec"], "product_id": p["id"],
                    "source": p["source"], "s3_prefix": prefix, "nc_url": nc, "file_urls": " ".join(files),
                    "delivery": p.get("delivery") or "", "created": p.get("created") or ""})
    return out


def write_csv(path, rows, columns):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def print_report(rows, products, info, meta):
    cols = [o for o in OUTCOMES if any(r["outcome"] == o for r in rows)] or [PRODUCED]
    print(f"\nDISP-S1 forward ledger, {meta['generated'][:16]}Z  sensing {meta['since'] or 'start'}..{meta['until'] or 'now'}"
          f"{'  products since ' + meta['products_since'] if meta['products_since'] else ''}  "
          f"CMR {'on' if meta['cmr'] else 'off'}  S3 {'on' if meta['s3'] else 'off'}  "
          f"whitelist {meta['whitelist'] if meta['whitelist'] is not None else 'none'}\n")
    print(f"{'frame':>6} {'trk':>4} " + " ".join(f"{SHORT[o]:>7}" for o in cols)
          + f" {'L3 grq/s3/dlv':>15} {'bnd ok':>7}  {'state':<18} next action")
    tot = Counter()
    for f, i in info.items():
        s = i.get("summary") or {"counts": {}}
        n = s["counts"]
        s3n = s.get("l3_s3", 0) if meta["s3"] else "-"
        print(f"{f:>6} {i['track'] or '-':>4} " + " ".join(f"{n.get(o) or '.':>7}" for o in cols)
              + f" {s.get('l3_grq', 0):>5}/{s3n}/{s.get('l3_delivered', 0):<6}"
              f" {s.get('boundaries_ok', 0):>3}/{s.get('boundaries', 0):<3}  {i.get('state', ''):<18} {i.get('hint', '')}")
        tot.update({o: n.get(o, 0) for o in cols})
        tot.update({k: s.get(k, 0) for k in ("l3_grq", "l3_s3", "l3_delivered", "boundaries_ok", "boundaries")})
    print(f"{'total':>6} {'':>4} " + " ".join(f"{tot[o]:>7}" for o in cols)
          + f" {tot['l3_grq']:>5}/{tot['l3_s3'] if meta['s3'] else '-'}/{tot['l3_delivered']:<6}"
            f" {tot['boundaries_ok']:>3}/{tot['boundaries']:<3}")
    for title, outcome in (("UNEXPLAINED", UNEXPLAINED), ("STOPPED", STOPPED), ("SKIPPED", SKIPPED_OUT)):
        sel = [r for r in rows if r["outcome"] == outcome]
        if sel:
            print(f"\n{title}: {len(sel)}")
            for r in sel[:100]:
                print(f"  {r['frame']:>6} {r['date']}  csc {r['csc_coverage'] or '-':>6}  cmr-now {r['cmr_now'] or '-':>6} "
                      f"{r['platforms'] or '':<8} {r['note']}")
    bad = [p for p in products if p["problem"]]
    if bad:
        print(f"\nPRODUCT PROBLEMS: {len(bad)}")
        for p in bad[:60]:
            print(f"  {p['id']}  {p['problem']}")


def main(argv=None, grq=None, mozart=None, s3=None, cmr=None, frame_bursts=None, region_db=None, now=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int, nargs="+", default=[], help="frames to account for")
    ap.add_argument("--region", nargs="+", default=[], help="every frame of these regions (region DB)")
    ap.add_argument("--since", help="first sensing date, YYYYMMDD")
    ap.add_argument("--until", help="last sensing date, YYYYMMDD (default: today)")
    ap.add_argument("--products-since", help="only L3s, CCSLCs and SCIFLO jobs created from this UTC time on, "
                                             "e.g. 2026-10-13T15:50:00Z (a campaign's start)")
    ap.add_argument("--cmr", action="store_true", help="re-query OPS CMR: uncatalogued dates and stop causes")
    ap.add_argument("--bucket", help="product bucket; default DATASET_BUCKET from settings.yaml")
    ap.add_argument("--no-s3", action="store_true", help="do not list the product bucket")
    ap.add_argument("--no-delivery-check", action="store_true", help="a venue without CNM delivery")
    ap.add_argument("--delivery-grace-mins", type=int, default=180,
                    help="an L3 younger than this with no CNM status yet is not a problem")
    ap.add_argument("--whitelist", nargs="*",
                    help="regions trigger-SCIFLO_L3_DISP_S1 fires for; default: read the deployed rule; "
                         "empty: no whitelist")
    ap.add_argument("--json", dest="json_path", help="write everything here (input to plot_disp_s1_forward_ledger.py)")
    ap.add_argument("--csv", dest="csv_path", help="write one row per frame/date here")
    ap.add_argument("--manifest", help="write one row per L3 with its S3 location and files here")
    args = ap.parse_args(argv)
    if not args.frame_id and not args.region:
        ap.error("give --frame-id or --region")
    for name in ("since", "until"):
        v = getattr(args, name)
        if v and not re.fullmatch(r"\d{8}", v):
            ap.error(f"--{name} must be YYYYMMDD")
    now = now or datetime.now(timezone.utc)
    until = args.until or now.strftime("%Y%m%d")

    assessed_end = None
    if frame_bursts is None:
        from data_subscriber.cslc_utils import localize_disp_burst_db_assessed_end, localize_disp_frame_burst_hist
        f2b, _, _ = localize_disp_frame_burst_hist()
        frame_bursts = {int(f): sorted(b.burst_ids) for f, b in f2b.items()}
        try:
            assessed_end = localize_disp_burst_db_assessed_end()
        except Exception as e:
            logger.warning(f"could not read the burst database's survey end: {e}")
    frames = set(args.frame_id)
    if args.region:
        if region_db is None:
            from data_subscriber.cslc_utils import _localize_region_db
            region_db = _localize_region_db()
        frames |= {int(f) for f, r in region_db.items() if str(r) in {str(x) for x in args.region}}
    frames = sorted(frames)
    if not frames:
        print("no frames selected")
        return 1

    if grq is None or mozart is None:
        from opera_commons.es_connection import get_grq_es, get_mozart_es
        grq = grq or get_grq_es(logger).es
        mozart = mozart or get_mozart_es(logger).es
    data = load_grq(grq, frames, args.since, until, args.products_since)
    data["jobs"] = load_jobs(mozart, args.products_since)
    if args.whitelist is not None:
        whitelist = [str(r) for r in args.whitelist] or None
    else:
        try:
            whitelist = load_whitelist(mozart)
        except Exception as e:
            logger.warning(f"could not read the {SCIFLO_RULE} whitelist ({e}); not checking regions")
            whitelist = None
    if s3 is None and not args.no_s3:
        bucket = args.bucket
        if not bucket:
            from util.conf_util import SettingsConf
            bucket = SettingsConf().cfg["DATASET_BUCKET"]
        s3 = load_s3(bucket)
    if cmr is None and args.cmr:
        cmr = Cmr(until)

    rows, products, info = account(frames, data, frame_bursts, whitelist, now, s3=None if args.no_s3 else s3,
                                   cmr=cmr if args.cmr else None, since=args.since, until=until,
                                   products_since=args.products_since, delivery_check=not args.no_delivery_check,
                                   delivery_grace_mins=args.delivery_grace_mins)
    meta = {"generated": now.isoformat(), "since": args.since, "until": until, "products_since": args.products_since,
            "cmr": bool(args.cmr), "s3": not args.no_s3, "whitelist": whitelist,
            "burst_db_assessed_end": assessed_end if isinstance(assessed_end, str) else None}
    print_report(rows, products, info, meta)

    if args.json_path:
        with open(args.json_path, "w") as fh:
            json.dump(dict(meta, frames={str(f): i for f, i in info.items()}, rows=rows, products=products),
                      fh, indent=1, default=str)
    if args.csv_path:
        write_csv(args.csv_path, sorted(rows, key=lambda r: (r["frame"], r["date"])), ROW_COLUMNS)
    if args.manifest:
        write_csv(args.manifest, manifest_rows(products), MANIFEST_COLUMNS)
    code = exit_code(rows, products)
    print(f"\nexit {code}: " + {0: "everything accounted for", 1: "UNEXPLAINED dates or product problems",
                                 2: "frames stopped or held behind a partial date"}[code])
    return code


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    sys.exit(main())
