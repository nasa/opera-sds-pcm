#!/usr/bin/env python3
"""Live status and accountability for a DISP-S1 processing campaign.

The processing-mode-annotated burst database is the SOURCE OF TRUTH for what a
campaign is supposed to do. Its phase labels say exactly which jobs will be
submitted and which products must exist when the campaign is finished. This tool
enumerates that expectation up front, then reconciles it against two live
sources:

    products   -- what actually published into GRQ
    job status -- what ran, what is running, and what FAILED

Everything reported is one of those three things. In particular a unit is only
"done" when its products exist, never merely because the batch proc cursor moved
past it; and a unit whose job failed is called out as failed rather than being
left to look pending forever.

A campaign self-disables the moment it completes, so selecting campaigns on the
batch proc's `enabled` flag would blind this tool at exactly the point an operator
most wants the report -- the end. Campaigns are selected on whether they were ever
given frames, and a finished one keeps reporting. To stay readable when a cluster
carries several old campaigns, one that is complete and clean collapses to a single
line; anything incomplete, stuck or failed is always shown in full.

    python disp_s1_campaign_status.py                    # every campaign
    python disp_s1_campaign_status.py --id <BATCH_PROC_ID>
    python disp_s1_campaign_status.py --failures         # only what needs an operator
    python disp_s1_campaign_status.py --all              # full detail even when complete
    python disp_s1_campaign_status.py --json > status.json

A campaign that does not start at the beginning of its frames -- a catch-up round whose
batch file gives each frame a starting cursor in frame_states -- owes nothing before
those cursors. The batch proc document keeps only the CURRENT cursor, so pass the batch
files the campaigns were created from; each is matched to its batch proc by label, and
every unit wholly before a frame's starting cursor is reported as "before start" instead
of being counted as owed (and, once the walk is past it, as MISSING):

    python disp_s1_campaign_status.py --start-from /path/to/priority_*.json \
        --since 2026-11-18T20:00:00Z

--products-since limits the product reads to those created from that time on, which is all a
catch-up owes and far lighter on OpenSearch. Several --id can be given, so one run (one read of
the jobs and of the burst database) covers every campaign of a round.

--since limits the job records read to that time onward. Job records are paged, so a
long campaign is never under-reported by a single-page cap.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_blackout import localize_disp_blackout_dates
from util.conf_util import SettingsConf

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200"

# ..._<sensing>T..Z_<lineage_start>T..Z_<boundary>T..Z_<created>T..Z_
CCSLC_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")
# OPERA_L2_COMPRESSED-CSLC-S1_F10601_T040-084801-IW1_<ref>..: the burst a CCSLC belongs to
CCSLC_BURST_RE = re.compile(r"_(T\d{3}-\d{6}-IW\d)_")
L3_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_")

# Every DISP-S1 job name carries the frame, and all but one carry enough to pin the
# job to a single k-set or forward date.
#   data-subscriber-query-timer-<label>_f24726-2016-07-09T01__03__16-2017-04-29T02__02__36-<ts>
JOB_QUERY_RE = re.compile(r"_f(\d+)-(\d{4}-\d{2}-\d{2})T[\d_]+-(\d{4}-\d{2}-\d{2})T")
#   job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-294_hist-<ts>
JOB_ACQ_RE = re.compile(r"-frame-(\d+)-latest_acq_index-(\d+)")
#   job-WF-cslc_download-frame-33065-acq_indices-0-to-438-<ts>
JOB_RANGE_RE = re.compile(r"-frame-(\d+)-acq_indices-(\d+)-to-(\d+)")
#   cslc_catalog_ingest-<label>_f17235-20210828-<ts>
#   SCIFLO_L3_DISP_S1__<rel>-disp_s1-kcycle-k15-m6-f33065-20201024-state-config-<ts>
# Both name a frame and a single sensing date. The query_hist name also carries _f<frame>
# but its dates are hyphenated (2016-07-09), so \d{8} cannot match them.
JOB_FRAME_DATE_RE = re.compile(r"[_-]f(\d+)-(\d{8})[-T_]")

DONE, RUNNING, FAILED, PENDING, SKIPPED = "done", "running", "failed", "pending", "skipped"
# The walk moved past this unit and nothing published. Distinct from FAILED (a job
# errored) and from RUNNING (still working): nothing is coming, because the walk
# treats a no-fire disposition as terminal and will never revisit the date.
MISSING = "missing"
# Wholly before the frame's starting cursor: earlier work, not owed by this campaign.
BEFORE = "before_start"
BAR = {DONE: "#", RUNNING: ">", FAILED: "X", PENDING: ".", SKIPPED: "~", MISSING: "o",
       BEFORE: "-"}
# worst-first, so a failed job is never masked by a completed one alongside it
SEVERITY = [FAILED, RUNNING, DONE]

JOB_TYPES = ("cslc_query_hist", "cslc_download", "cslc_catalog_ingest",
             "SCIFLO_L3_DISP_S1_hist", "SCIFLO_L3_DISP_S1")
FAILED_STATUSES = ("job-failed", "job-offline", "job-revoked")
RUNNING_STATUSES = ("job-started", "job-queued")
# The jobs that actually emit products. Only once one of these has COMPLETED does a
# missing product mean something went wrong; an upstream query or ingest completing
# just means the producer has not run yet.
TERMINAL_TYPES = ("SCIFLO_L3_DISP_S1_hist", "SCIFLO_L3_DISP_S1")


def es_post(path, body, size_note=None):
    out = subprocess.run(["curl", "-s", "-k", "--netrc-file", NETRC, "-XPOST", ES + path,
                          "-H", "Content-Type: application/json", "-d", json.dumps(body)],
                         capture_output=True, text=True, timeout=300)
    try:
        return json.loads(out.stdout)
    except ValueError:
        return {}


def es_scroll(path, body, page=5000):
    """Every hit of a search, paged with a scroll, so a large result is never cut short.

    Kept light on the cluster: index order (no scoring or sorting), a one-minute keep-alive,
    and the scroll context is released as soon as the last page is read.
    """
    sep = "&" if "?" in path else "?"
    body = dict(body, sort=["_doc"]) if "sort" not in body else body
    r = es_post("%s%sscroll=1m&size=%d" % (path, sep, page), body)
    hits = list(r.get("hits", {}).get("hits", []))
    scroll_id = r.get("_scroll_id")
    if "error" in r:
        print("WARNING: %s: %s" % (path, json.dumps(r["error"])[:300]), file=sys.stderr)
    last = len(hits)
    while scroll_id and last >= page:               # a short page is the last one
        r = es_post("/_search/scroll", {"scroll": "1m", "scroll_id": scroll_id})
        page_hits = r.get("hits", {}).get("hits", [])
        scroll_id = r.get("_scroll_id") or scroll_id
        last = len(page_hits)
        hits.extend(page_hits)
    if scroll_id:
        subprocess.run(["curl", "-s", "-k", "--netrc-file", NETRC, "-XDELETE", ES + "/_search/scroll",
                        "-H", "Content-Type: application/json", "-d", json.dumps({"scroll_id": scroll_id})],
                       capture_output=True, text=True, timeout=60)
    return hits


def truncated(response, what):
    """Warn rather than silently under-report -- a short page reads as 'less work done'."""
    hits = response.get("hits", {})
    total = (hits.get("total") or {}).get("value", 0)
    got = len(hits.get("hits", []))
    if total > got:
        print("WARNING: %s returned %d of %d documents; counts below are LOW"
              % (what, got, total), file=sys.stderr)
    return hits.get("hits", [])


# ------------------------------------------------------------------ what actually exists
# The product indices, named so a read touches only them and not every index behind the grq alias.
PRODUCT_INDEX = {"L3_DISP_S1": "grq_*_l3_disp_s1-*", "L2_CSLC_S1_COMPRESSED": "grq_*_l2_cslc_s1_compressed-*"}


def frames_products(frame_ids, created_since=None, chunk=400):
    """{frame: (l3_secondary_dates, ccslc_boundary_dates, ccslc_generations)} for many frames.

    Read with one scroll per dataset over all the frames, not two searches per frame: on a
    170-frame campaign the per-frame searches were nine tenths of the run time.

    created_since limits both reads to products created from that time on. A catch-up owes
    only what it makes itself, so this is all its accounting needs, and it keeps the read from
    scrolling years of earlier compressed CSLCs on every run. Earlier products are then not
    seen at all: compressed CSLC generations are counted among this campaign's own, and
    nothing is known about the boundaries before a frame's start.

    ccslc_generations counts the documents per (burst, boundary date). More than one is a
    second generation of the same compressed CSLC -- a re-run of that ministack -- which a
    raw document count would report as extra progress.
    """
    out = {int(f): (set(), set(), defaultdict(int)) for f in frame_ids}
    ids = sorted(out)
    for ds in ("L3_DISP_S1", "L2_CSLC_S1_COMPRESSED"):
        for at in range(0, len(ids), chunk):
            must = [{"term": {"dataset.keyword": ds}}, {"terms": {"metadata.frame_id": ids[at:at + chunk]}}]
            if created_since:
                must.append({"range": {"creation_timestamp": {"gte": created_since}}})
            hits = es_scroll("/%s/_search?ignore_unavailable=true&allow_no_indices=true" % PRODUCT_INDEX[ds],
                             {"query": {"bool": {"filter": must}}, "_source": ["id", "metadata.frame_id"]})
            for h in hits:
                src = h["_source"]
                pid = src["id"]
                try:
                    l3, cc, gens = out[int((src.get("metadata") or {}).get("frame_id"))]
                except (KeyError, TypeError, ValueError):
                    continue
                if ds == "L3_DISP_S1":
                    m = L3_RE.search(pid)
                    if m:
                        l3.add(m.group(2))
                else:
                    m = CCSLC_RE.search(pid)
                    b = CCSLC_BURST_RE.search(pid)
                    if m:
                        cc.add(m.group(3))
                        gens[(b.group(1) if b else pid, m.group(3))] += 1
    return {f: (l3, cc, dict(gens)) for f, (l3, cc, gens) in out.items()}


def frame_products(frame_id):
    """(l3_secondary_dates, ccslc_boundary_dates, ccslc_generations) for one frame."""
    return frames_products([frame_id])[int(frame_id)]


def campaign_jobs(since=None):
    """Every DISP-S1 job on the cluster, parsed down to (frame, position(s), state).

    Returns {frame_id: {position: [job, ...]}} plus a list of jobs that name a frame
    but no position, which are reported at frame level rather than dropped.
    """
    should = [{"prefix": {"type": "job-%s" % t}} for t in JOB_TYPES]
    query = {"bool": {"should": should, "minimum_should_match": 1}}
    if since:
        query["bool"]["filter"] = [{"range": {"@timestamp": {"gte": since}}}]
    hits = es_scroll("/job_status-*/_search", {
        "query": query,
        "_source": ["type", "status", "job.name", "job.job_id", "job.job_info.time_queued", "@timestamp"]})

    by_pos = defaultdict(lambda: defaultdict(list))
    frame_level = defaultdict(list)
    for h in hits:
        s = h["_source"]
        name = ((s.get("job") or {}).get("name")) or ""
        status = s.get("status") or ""
        if status == "job-deduped":
            continue  # a dedup is not work, and not a failure
        job = {"type": s.get("type", "").split(":")[0].replace("job-", ""),
               "status": status, "name": name,
               "id": (s.get("job") or {}).get("job_id") or "",
               "when": s.get("@timestamp", ""),
               "queued": ((s.get("job") or {}).get("job_info") or {}).get("time_queued") or ""}

        m = JOB_QUERY_RE.search(name)
        if m:
            job["frame"] = int(m.group(1))
            job["span"] = (m.group(2).replace("-", ""), m.group(3).replace("-", ""))
            frame_level[job["frame"]].append(job)
            continue
        m = JOB_FRAME_DATE_RE.search(name)
        if m:
            job["frame"] = int(m.group(1))
            job["date"] = m.group(2)
            frame_level[job["frame"]].append(job)
            continue
        m = JOB_ACQ_RE.search(name)
        if m:
            job["frame"] = int(m.group(1))
            job["acq"] = int(m.group(2))
            frame_level[job["frame"]].append(job)
            continue
        m = JOB_RANGE_RE.search(name)
        if m:
            job["frame"] = int(m.group(1))
            job["acq_range"] = (int(m.group(2)), int(m.group(3)))
            frame_level[job["frame"]].append(job)
            continue
        frame_level[0].append(job)          # unattributable; still surfaced if failed
    return by_pos, frame_level


def current_jobs(jobs):
    """The jobs that still say something about their unit: a failed or revoked job is left out when a
    job of the same type was queued after it and has not failed. An operator who revokes a job and
    submits it again leaves the revoked record behind; the unit is running, not failed."""
    def replaced(job):
        return any(o is not job and o["type"] == job["type"] and job_state(o["status"]) != FAILED
                   and job.get("queued") and o.get("queued", "") > job["queued"] for o in jobs)
    return [j for j in jobs if job_state(j["status"]) != FAILED or not replaced(j)]


def job_state(status):
    if status in FAILED_STATUSES:
        return FAILED
    if status in RUNNING_STATUSES:
        return RUNNING
    if status == "job-completed":
        return DONE
    return RUNNING


# ------------------------------------------------------------------ what SHOULD exist
def expected_units(frame, phases, k, ymd, acq_of):
    """Enumerate every unit of work the burst database says this frame owes.

    historical phase -> one unit per k-set; forward phase -> one unit per date;
    no_run -> a single unit that owes nothing.
    """
    units = []
    for p in phases:
        if p.label.startswith("historical_"):
            for n, start in enumerate(range(p.start_pos, p.start_pos + p.length, k), 1):
                positions = list(range(start, start + k))
                # the first date of the phase is the lineage reference and yields no product
                product_positions = [i for i in positions if i != p.start_pos]
                units.append({
                    "phase": p.label, "kind": "historical", "name": "k-set %d" % n,
                    "positions": positions, "product_positions": product_positions,
                    "boundary": ymd[start + k - 1],
                    "acqs": [acq_of[i] for i in positions],
                    "dates": "%s..%s" % (ymd[start], ymd[start + k - 1])})
        elif p.label.startswith("forward_"):
            for i in range(p.start_pos, p.start_pos + p.length):
                units.append({
                    "phase": p.label, "kind": "forward", "name": ymd[i],
                    "positions": [i], "product_positions": [i], "boundary": None,
                    "acqs": [acq_of[i]], "dates": ymd[i]})
        else:
            positions = list(range(p.start_pos, p.start_pos + p.length))
            units.append({
                "phase": p.label, "kind": "no_run", "name": "%d dates" % p.length,
                "positions": positions, "product_positions": [], "boundary": None,
                "acqs": [acq_of[i] for i in positions],
                "dates": "%s..%s" % (ymd[p.start_pos], ymd[p.start_pos + p.length - 1])})
    return units


def shift_day(ymd_str, days):
    return (datetime.strptime(ymd_str, "%Y%m%d") + timedelta(days=days)).strftime("%Y%m%d")


def attribute(units, jobs, ymd):
    """Hang each job off the unit it belongs to, using whatever key its name carries."""
    by_acq, by_date, by_start = {}, {}, {}
    for u in units:
        for a in u["acqs"]:
            by_acq[a] = u
        for i in u["positions"]:
            by_date[ymd[i]] = u
        by_start[ymd[u["positions"][0]]] = u
        u["jobs"] = []

    orphans = []
    for j in jobs:
        u = None
        if "span" in j:
            # The query window runs from 30 minutes before the k-set's first acquisition to 30 minutes after
            # its last. A frame imaged just after midnight UTC therefore has a window that starts on the
            # previous day, and one imaged just before midnight a window that ends on the next day.
            first, last = j["span"]
            u = (by_start.get(first) or by_date.get(first) or by_start.get(shift_day(first, 1))
                 or by_date.get(last) or by_date.get(shift_day(last, -1)))
        elif "date" in j:
            u = by_date.get(j["date"])
        elif "acq" in j:
            u = by_acq.get(j["acq"])
        elif "acq_range" in j:
            u = by_acq.get(j["acq_range"][1]) or by_acq.get(j["acq_range"][0])
        if u is None:
            orphans.append(j)
        else:
            u["jobs"].append(j)
    return orphans


def settle(units, l3_dates, ccslc_dates, ymd, cursor=None, start=None):
    """Decide each unit's state. Products win; job status explains what products cannot.

    `cursor` is the batch proc's position for this frame. A unit the cursor has already
    passed is finished as far as the walk is concerned -- it will never be revisited --
    so if it has no products and nothing running, the products are MISSING rather than
    pending or running. That is how a frame reaches a 100% cursor, self-disables, and
    still owes products: a forward date whose k-cycle state config returns a no-fire
    disposition is terminal, and the walk advances over it without ever submitting.

    `start` is the cursor the campaign STARTED from (the batch file's frame_states). A
    unit wholly before it was done by earlier work, so this campaign owes nothing for it:
    it is BEFORE, with nothing wanted, and can never become MISSING.
    """
    for u in units:
        if start is not None and u["positions"][-1] < start:
            u["status"] = BEFORE
            u["have"], u["want"] = 0, 0
            continue
        if u["kind"] == "no_run":
            u["status"] = SKIPPED
            u["have"], u["want"] = 0, 0
            continue

        want = len(u["product_positions"])
        have = sum(1 for i in u["product_positions"] if ymd[i] in l3_dates)
        u["have"], u["want"] = have, want

        complete = have >= want
        if u["kind"] == "historical":
            # a k-set is not done until its compressed CSLC boundary published, or the
            # next k-set has nothing to build on
            complete = complete and u["boundary"] in ccslc_dates

        states = {job_state(j["status"]) for j in current_jobs(u["jobs"])}
        produced = any(j["type"] in TERMINAL_TYPES and job_state(j["status"]) == DONE
                       for j in u["jobs"])
        if complete:
            u["status"] = DONE
        elif FAILED in states:
            u["status"] = FAILED
        elif RUNNING in states:
            u["status"] = RUNNING
        elif produced:
            # the job that emits the products finished and they are not there
            u["status"] = FAILED
        elif states:
            # only upstream jobs so far -- the producer has not run yet, not a failure
            u["status"] = RUNNING
        else:
            u["status"] = PENDING

        # the walk has gone past this unit and nothing is in flight for it
        passed = cursor is not None and cursor > u["positions"][-1]
        if passed and u["status"] in (PENDING, RUNNING) and not any(
                job_state(j["status"]) == RUNNING for j in u["jobs"]):
            u["status"] = MISSING
    return units


def per_date(units, l3_dates, ccslc_dates, ymd, phases):
    """Flatten the units to one entry per sensing date, for the timeline plot.

    Each date carries its own accountability state, so the plot can show the frame
    exactly as the burst database describes it and colour in what has actually
    happened on top.
    """
    # Every historical phase STARTS a compressed CSLC lineage at its first date. Only the
    # ones after the first also RESET one, abandoning the lineage before the gap -- that is
    # what is_new_lineage means and what lineage_transitions records. The plot needs both.
    lineage_starts = {p.start_pos for p in phases if p.label.startswith("historical_")}
    lineage_resets = {p.start_pos for p in phases if p.is_new_lineage}
    out = []
    for u in units:
        for i in u["positions"]:
            expects_product = i in u["product_positions"] and u["status"] != BEFORE
            if u["status"] == BEFORE:
                st = BEFORE
            elif expects_product and ymd[i] in l3_dates:
                st = DONE
            elif u["kind"] == "no_run":
                st = SKIPPED
            elif not expects_product:
                # the lineage reference date: no product of its own, so it is as
                # done as the unit that owns it
                st = DONE if u["status"] == DONE else u["status"]
            else:
                st = u["status"]
            out.append({"date": ymd[i], "position": i, "phase": u["phase"],
                        "kind": u["kind"], "status": st, "unit": u["name"],
                        "expects_product": expects_product,
                        "lineage_start": i in lineage_starts,
                        "lineage_reset": i in lineage_resets,
                        "boundary": u["boundary"] == ymd[i] and u["kind"] == "historical",
                        "boundary_published": (u["boundary"] == ymd[i]
                                               and ymd[i] in ccslc_dates)})
    out.sort(key=lambda e: e["position"])
    return out


def provenance(blackout):
    """Which ancillary files this picture was computed from.

    Both drift independently of the campaign, and a band that is drawn or not drawn
    changes how an operator reads an empty stretch. Say which files were used, and say
    so explicitly when the blackout file could not be read -- otherwise "no windows"
    is indistinguishable from "no file", and some frames legitimately have no windows.
    """
    try:
        cfg = SettingsConf().cfg
    except Exception:
        cfg = {}
    out = {"burst_db": os.path.basename(cfg.get("DISP_S1_BURST_DB_S3PATH", "") or "") or "unknown"}
    if blackout is None:
        out["blackout"] = None          # could not be read
    else:
        out["blackout"] = os.path.basename(
            cfg.get("DISP_S1_BLACKOUT_DATES_S3PATH", "") or "") or "unknown"
        out["blackout_frames"] = len(blackout)
    return out


def blackout_windows(frame_id, blackout, ymd):
    """The deployed blackout windows for this frame, clipped to its sensing range.

    A blackout window is a period the labeler deliberately excludes from processing -- a
    snow season, typically. It explains why a chunk has too few acquisitions to fill a
    k-set. It is not a failure and nothing is owed inside one.
    """
    if not blackout or not ymd:
        return []          # None means the file could not be read; {} means no windows
    lo, hi = ymd[0], ymd[-1]
    out = []
    for start, end in blackout.get(frame_id, []):
        a, b = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        if b < lo or a > hi:
            continue
        out.append([max(a, lo), min(b, hi)])
    return out


def blocking_failure(units):
    """The failure that has actually stopped the frame, or None.

    Only a HISTORICAL k-set can block. Its compressed CSLC gates every later k-set, so
    a failure there stops everything behind it and retrying a later k-set cannot help.
    Forward dates do not gate each other -- the walk advances past a date once its
    k-cycle state config reaches a terminal disposition -- so a failed forward date
    leaves a hole in the products and the frame carries on. That still needs an
    operator, and is still reported as FAILED, but it is not STUCK.
    """
    for u in units:
        if u["status"] in (DONE, SKIPPED, BEFORE):
            continue
        if u["status"] == FAILED:
            if u["kind"] == "historical":
                return u
            continue          # a failed forward date is a hole, not a blockage
        return None
    return None


# ------------------------------------------------------------------ rendering
def render_frame(r, verbose=True):
    fid = r["frame"]
    head = ("\n  frame %-7s %3d/%-4d products (%3d%%)   %2d/%-2d units done   %2d bursts"
            % (fid, r["products"], r["expected"], r["pct"], r["units_done"],
               r["units_total"], r["bursts"]))
    if r.get("start") is not None:
        head += "   from @%d" % r["start"]
    if r["stuck"]:
        head += "   *** STUCK ***"
    print(head)
    bar = " | ".join("".join(BAR.get(u["status"], "?") for u in ph["units"])
                     for ph in r["phases"])
    print("     [%s]" % bar)
    if not verbose:
        return
    for ph in r["phases"]:
        counts = defaultdict(int)
        for u in ph["units"]:
            counts[u["status"]] += 1
        tally = "  ".join("%d %s" % (n, s.replace("_", " ")) for s, n in sorted(counts.items()))
        print("     %-16s @%-4d %-3d dates   %s" % (ph["label"], ph["start"], ph["length"], tally))
        for u in ph["units"]:
            if u["status"] == MISSING:
                print("        ?? MISSING  %-10s %s   (%d/%d products) -- walk passed it, "
                      "nothing ran, nothing will retry"
                      % (u["name"], u["dates"], u["have"], u["want"]))
                continue
            if u["status"] == FAILED:
                print("        !! FAILED   %-10s %s   (%d/%d products)"
                      % (u["name"], u["dates"], u["have"], u["want"]))
                for j in u["jobs"]:
                    if job_state(j["status"]) == FAILED:
                        print("           %-24s %s" % (j["type"], j["status"]))
                        print("           %s" % j["id"])
            elif u["status"] == RUNNING:
                print("        -> RUNNING  %-10s %s   (%d/%d products)"
                      % (u["name"], u["dates"], u["have"], u["want"]))
        pending = [u for u in ph["units"] if u["status"] == PENDING]
        if pending:
            more = "  (+%d more)" % (len(pending) - 1) if len(pending) > 1 else ""
            print("           next     %-10s %s%s" % (pending[0]["name"], pending[0]["dates"], more))


def just_submitted(row):
    """Is the unit right behind the frame's cursor MISSING with no producing job yet?

    The walk moves the cursor past a k-set as it submits the query job, and that job's status
    record is indexed a few seconds later. It moves past a forward date when the date's k-cycle
    state config fires, and the trigger rule queues the SCIFLO some seconds after that. A report
    built in either window sees a unit the walk has passed with nothing running for it, which is
    exactly what MISSING means.
    """
    return any(u["status"] == MISSING and u["positions"][-1] + 1 == row["cursor"]
               and not any(j["type"] in TERMINAL_TYPES for j in u["jobs"])
               for ph in row.get("phases") or [] for u in ph["units"])


def render(proc_id, proc, frame_to_bursts, jobs_by_frame, blackout, args, start_from=None,
           refresh_jobs=None, settle_secs=0):
    """start_from: (source, {frame: starting cursor}) from the batch file, or None.

    refresh_jobs: a callable returning a fresh jobs-by-frame map. When a frame's newest unit
    looks MISSING only because its job record may not be indexed yet (see just_submitted),
    the jobs are read once more after settle_secs and those frames are settled again.
    """
    k = int(proc.get("k", 15))
    states = {int(f): int(c) for f, c in (proc.get("frame_states") or {}).items()}
    start_source, starts = start_from if start_from else (None, {})
    tot = defaultdict(int)

    products_since = getattr(args, "products_since", None)
    products = frames_products([f for f in states if f in frame_to_bursts], products_since)

    def frame_row(fid, jobs):
        fr = frame_to_bursts.get(fid)
        if fr is None:
            return None
        phases = getattr(fr, "phases", None)
        ymd = [d.strftime("%Y%m%d") for d in fr.sensing_datetimes]
        acq_of = [cslc_utils.determine_acquisition_cycle_cslc(d, fid, frame_to_bursts)
                  for d in fr.sensing_datetimes]
        l3, cc, cc_gens = products[fid]
        bursts = len(fr.burst_ids)
        start = starts.get(fid)

        if not phases:
            return {"frame": fid, "cursor": states[fid], "start": start, "bursts": bursts,
                    "quarantined": getattr(fr, "phase_error", "no phases"),
                    "products": 0, "expected": 0, "pct": 0,
                    "units_done": 0, "units_total": 0, "ccslc": 0,
                    "ccslc_expected": 0, "stuck": True, "phases": []}

        units = expected_units(fr, phases, k, ymd, acq_of)
        attribute(units, jobs.get(fid, []), ymd)
        settle(units, l3, cc, ymd, states[fid], start)

        owed = [u for u in units if u["status"] != BEFORE]
        want = sum(u["want"] for u in owed)
        have = sum(u["have"] for u in owed)
        owed_boundaries = {u["boundary"] for u in owed if u["kind"] == "historical"}
        cc_want = bursts * len(owed_boundaries)
        cc_have = sum(1 for (_, b) in cc_gens if b in owed_boundaries)
        # every boundary with more than one generation, owed or not: a forward date or a
        # later ministack builds on the earlier ones too, and picks one generation
        cc_extra = sorted({b for (_, b), n in cc_gens.items() if n > 1})
        blocked = blocking_failure(units)

        by_phase = []
        for p in phases:
            pu = [u for u in units if u["phase"] == p.label]
            by_phase.append({"label": p.label, "start": p.start_pos,
                             "length": p.length, "units": pu})

        return {
            "frame": fid, "cursor": states[fid], "start": start, "bursts": bursts,
            "products": have, "expected": want,
            "pct": int(have / want * 100) if want else 0,
            "units_done": sum(1 for u in owed if u["status"] == DONE),
            "units_missing": sum(1 for u in owed if u["status"] == MISSING),
            "units_total": sum(1 for u in owed if u["kind"] != "no_run"),
            "units_before_start": len(units) - len(owed),
            "ccslc": cc_have, "ccslc_expected": cc_want,
            "ccslc_boundaries_with_extra_generations": cc_extra,
            "failed_units": [u for u in units if u["status"] in (FAILED, MISSING)],
            "stuck": blocked is not None, "blocked_on": blocked["name"] if blocked else None,
            "phases": by_phase,
            "sensing": per_date(units, l3, cc, ymd, phases),
            "blackout": blackout_windows(fid, blackout, ymd)}

    rows = [r for r in (frame_row(fid, jobs_by_frame) for fid in sorted(states)) if r]
    suspects = [r["frame"] for r in rows if just_submitted(r)]
    if suspects and refresh_jobs is not None:
        time.sleep(settle_secs)
        fresh = refresh_jobs()
        again = {fid: frame_row(fid, fresh) for fid in suspects}
        rows = [again.get(r["frame"]) or r for r in rows]
    for r in rows:
        tot["have"] += r["products"]; tot["want"] += r["expected"]
        tot["cc_have"] += r["ccslc"]; tot["cc_want"] += r["ccslc_expected"]

    summary = {"batch_proc": proc_id, "label": proc.get("label"),
               "enabled": proc.get("enabled"), "k": k,
               "provenance": provenance(blackout),
               "start_from": start_source, "products_since": products_since,
               "products": tot["have"], "products_expected": tot["want"],
               "ccslc": tot["cc_have"], "ccslc_expected": tot["cc_want"],
               "stuck_frames": [r["frame"] for r in rows if r.get("stuck")],
               "frames_with_failures": [r["frame"] for r in rows if r.get("failed_units")],
               "frames_with_extra_ccslc_generations": [
                   r["frame"] for r in rows if r.get("ccslc_boundaries_with_extra_generations")],
               "frames": rows}

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
        return summary

    pct = int(tot["have"] / tot["want"] * 100) if tot["want"] else 0
    # A campaign that produced everything it owed and has nothing failed or missing
    # collapses to one line, so several finished campaigns on a cluster cannot bury the
    # one that still needs an operator. `failed_units` covers MISSING as well as FAILED,
    # so anything unaccounted for keeps its full table.
    done_and_clean = (tot["want"] > 0 and tot["have"] >= tot["want"]
                      and not summary["stuck_frames"] and not summary["frames_with_failures"])
    if done_and_clean and not args.all and not args.id:
        print("%-46s COMPLETE   %d/%d products   %d/%d compressed CSLCs"
              % (proc.get("label"), tot["have"], tot["want"], tot["cc_have"], tot["cc_want"]))
        return summary

    print("=" * 78)
    print("%s   [%s]" % (proc.get("label"), "enabled" if proc.get("enabled") else "disabled"))
    print("  %d/%d products (%d%%)   %d/%d compressed CSLCs   k=%d"
          % (tot["have"], tot["want"], pct, tot["cc_have"], tot["cc_want"], k))
    print("  counted from: %s" % (start_source or
                                  "the first date of every frame (no --start-from file "
                                  "matched this batch proc)"))
    if summary["stuck_frames"]:
        print("  STUCK: frame(s) %s -- a historical k-set failed and gates everything behind it"
              % ", ".join(str(f) for f in summary["stuck_frames"]))
    other = [f for f in summary["frames_with_failures"] if f not in summary["stuck_frames"]]
    if other:
        print("  FAILURES (not blocking): frame(s) %s -- units with no product, failed or "
              "missing; the frame keeps going" % ", ".join(str(f) for f in other))
    if summary["frames_with_extra_ccslc_generations"]:
        print("  NOTE: frame(s) %s have more than one generation of a compressed CSLC "
              "(same burst and boundary date)"
              % ", ".join(str(f) for f in summary["frames_with_extra_ccslc_generations"]))
    print("=" * 78)

    for r in rows:
        if r.get("quarantined"):
            print("\n  frame %-7s QUARANTINED: %s" % (r["frame"], r["quarantined"]))
            continue
        if args.failures and not (r["stuck"] or r.get("failed_units")):
            continue
        render_frame(r, verbose=True)

    print("\n  bar: one character per k-set (historical) or per date (forward),")
    print("       '|' separates phases, left to right in time order")
    print("  legend: # done   > running   X FAILED   o MISSING (never ran)   . pending   ~ no_run"
          "   - before start")
    return summary


def load_start_states(paths):
    """{batch proc label: (file, {frame: starting cursor})} from batch files.

    The files are the ones given to pcm_batch.py create. Only their label and
    frame_states are read.
    """
    out = {}
    for path in paths or []:
        with open(path) as f:
            spec = json.load(f)
        label = spec.get("label")
        states = spec.get("frame_states") or {}
        if not label or not states:
            print("WARNING: %s has no label or no frame_states; ignored" % path, file=sys.stderr)
            continue
        if label in out:
            print("WARNING: %s and %s both have label %r; using %s"
                  % (out[label][0], path, label, path), file=sys.stderr)
        out[label] = (path, {int(fid): int(c) for fid, c in states.items()})
    return out


def select_campaigns(procs, proc_id=None):
    """Pick which campaigns to report on, most recently active first.

    Deliberately NOT filtered on `enabled`. A campaign disables itself the moment it
    finishes, so selecting on that flag reports nothing from the point the work
    completed -- precisely when an operator needs the accounting, and precisely when
    they are checking whether anything was left owed. A campaign is anything that was
    given frames to process; whether it is still running is a property to display, not
    a precondition for being counted.

    An explicit id always wins, so a proc with no frame_states can still be inspected.
    """
    if proc_id:
        wanted = [proc_id] if isinstance(proc_id, str) else list(proc_id)
        return [(i, s) for i, s in procs if i in wanted]
    chosen = [(i, s) for i, s in procs if s.get("frame_states")]
    chosen.sort(key=lambda ps: ps[1].get("last_run_date") or "", reverse=True)
    return chosen


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--id", nargs="+", help="batch proc id(s); default is every cslc_query_hist campaign")
    ap.add_argument("--failures", action="store_true",
                    help="only show frames with a failure, blocking or not")
    ap.add_argument("--all", action="store_true",
                    help="render completed campaigns in full instead of one line")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--start-from", nargs="+", default=[], metavar="BATCH_FILE",
                    help="batch files the campaigns were created from; each is matched to its "
                         "batch proc by label, and its frame_states are the starting cursors")
    ap.add_argument("--since", help="only job records from this time on, e.g. 2026-11-18T20:00:00Z")
    ap.add_argument("--products-since",
                    help="only products created from this time on (a catch-up's start). Much lighter on "
                         "OpenSearch for frames with years of earlier products; earlier products are not seen")
    ap.add_argument("--settle-secs", type=int, default=20,
                    help="when a frame's newest unit has no job record yet, wait this long and read the "
                         "jobs once more before calling it MISSING (default 20; 0 reads again at once)")
    args = ap.parse_args()
    start_states = load_start_states(args.start_from)

    r = es_post("/batch_proc/_search?size=1000", {"query": {"match_all": {}}})
    procs = [(h["_id"], h["_source"]) for h in truncated(r, "batch_proc")
             if h["_source"].get("job_type") == "cslc_query_hist"]
    unused = set(start_states) - {proc.get("label") for _, proc in procs}
    for label in sorted(unused):
        print("WARNING: no batch proc is labelled %r (from %s)" % (label, start_states[label][0]),
              file=sys.stderr)
    procs = select_campaigns(procs, args.id)
    if not procs:
        print("no matching batch proc")
        return 1

    _, jobs_by_frame = campaign_jobs(args.since)
    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    try:
        blackout = localize_disp_blackout_dates()
    except Exception as e:
        print("WARNING: blackout dates could not be read (%s); windows omitted" % e,
              file=sys.stderr)
        blackout = None
    stuck = failures = False
    for pid, proc in procs:
        s = render(pid, proc, frame_to_bursts, jobs_by_frame, blackout, args,
                   start_states.get(proc.get("label")),
                   refresh_jobs=lambda: campaign_jobs(args.since)[1], settle_secs=args.settle_secs)
        stuck = stuck or bool(s.get("stuck_frames"))
        failures = failures or bool(s.get("frames_with_failures"))
    return 2 if stuck else (1 if failures else 0)


if __name__ == "__main__":
    sys.exit(main())
