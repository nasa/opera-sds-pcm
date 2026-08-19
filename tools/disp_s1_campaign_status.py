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
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_blackout import localize_disp_blackout_dates
from util.conf_util import SettingsConf

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200"

# ..._<sensing>T..Z_<lineage_start>T..Z_<boundary>T..Z_<created>T..Z_
CCSLC_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")
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
BAR = {DONE: "#", RUNNING: ">", FAILED: "X", PENDING: ".", SKIPPED: "~", MISSING: "o"}
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
def frame_products(frame_id):
    """Return (l3_secondary_dates, ccslc_boundary_dates, ccslc_doc_count)."""
    l3, cc = set(), set()
    cc_docs = 0
    for ds, acc in (("L3_DISP_S1", l3), ("L2_CSLC_S1_COMPRESSED", cc)):
        r = es_post("/grq/_search?size=10000", {
            "query": {"bool": {"must": [{"term": {"dataset.keyword": ds}},
                                        {"term": {"metadata.frame_id": frame_id}}]}},
            "_source": ["id"]})
        for h in truncated(r, "%s for frame %s" % (ds, frame_id)):
            pid = h["_source"]["id"]
            if ds == "L3_DISP_S1":
                m = L3_RE.search(pid)
                if m:
                    acc.add(m.group(2))
            else:
                cc_docs += 1
                m = CCSLC_RE.search(pid)
                if m:
                    acc.add(m.group(3))
    return l3, cc, cc_docs


def campaign_jobs():
    """Every DISP-S1 job on the cluster, parsed down to (frame, position(s), state).

    Returns {frame_id: {position: [job, ...]}} plus a list of jobs that name a frame
    but no position, which are reported at frame level rather than dropped.
    """
    should = [{"prefix": {"type": "job-%s" % t}} for t in JOB_TYPES]
    r = es_post("/job_status-*/_search?size=10000", {
        "query": {"bool": {"should": should, "minimum_should_match": 1}},
        "_source": ["type", "status", "job.name", "job.job_id", "@timestamp"]})
    hits = truncated(r, "job status")

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
               "when": s.get("@timestamp", "")}

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
            u = by_start.get(j["span"][0]) or by_date.get(j["span"][0])
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


def settle(units, l3_dates, ccslc_dates, ymd, cursor=None):
    """Decide each unit's state. Products win; job status explains what products cannot.

    `cursor` is the batch proc's position for this frame. A unit the cursor has already
    passed is finished as far as the walk is concerned -- it will never be revisited --
    so if it has no products and nothing running, the products are MISSING rather than
    pending or running. That is how a frame reaches a 100% cursor, self-disables, and
    still owes products: a forward date whose k-cycle state config returns a no-fire
    disposition is terminal, and the walk advances over it without ever submitting.
    """
    for u in units:
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

        states = {job_state(j["status"]) for j in u["jobs"]}
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
            expects_product = i in u["product_positions"]
            if expects_product and ymd[i] in l3_dates:
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
        if u["status"] in (DONE, SKIPPED):
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
        tally = "  ".join("%d %s" % (n, s) for s, n in sorted(counts.items()))
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


def render(proc_id, proc, frame_to_bursts, jobs_by_frame, blackout, args):
    k = int(proc.get("k", 15))
    states = {int(f): int(c) for f, c in (proc.get("frame_states") or {}).items()}
    rows = []
    tot = defaultdict(int)

    for fid in sorted(states):
        fr = frame_to_bursts.get(fid)
        if fr is None:
            continue
        phases = getattr(fr, "phases", None)
        ymd = [d.strftime("%Y%m%d") for d in fr.sensing_datetimes]
        acq_of = [cslc_utils.determine_acquisition_cycle_cslc(d, fid, frame_to_bursts)
                  for d in fr.sensing_datetimes]
        l3, cc, cc_docs = frame_products(fid)
        bursts = len(fr.burst_ids)

        if not phases:
            rows.append({"frame": fid, "cursor": states[fid], "bursts": bursts,
                         "quarantined": getattr(fr, "phase_error", "no phases"),
                         "products": len(l3), "expected": 0, "pct": 0,
                         "units_done": 0, "units_total": 0, "ccslc": cc_docs,
                         "ccslc_expected": 0, "stuck": True, "phases": []})
            continue

        units = expected_units(fr, phases, k, ymd, acq_of)
        attribute(units, jobs_by_frame.get(fid, []), ymd)
        settle(units, l3, cc, ymd, states[fid])

        want = sum(u["want"] for u in units)
        cc_want = sum(bursts for u in units if u["kind"] == "historical")
        blocked = blocking_failure(units)

        by_phase = []
        for p in phases:
            pu = [u for u in units if u["phase"] == p.label]
            by_phase.append({"label": p.label, "start": p.start_pos,
                             "length": p.length, "units": pu})

        rows.append({
            "frame": fid, "cursor": states[fid], "bursts": bursts,
            "products": len(l3), "expected": want,
            "pct": int(len(l3) / want * 100) if want else 0,
            "units_done": sum(1 for u in units if u["status"] == DONE),
            "units_missing": sum(1 for u in units if u["status"] == MISSING),
            "units_total": sum(1 for u in units if u["kind"] != "no_run"),
            "ccslc": cc_docs, "ccslc_expected": cc_want,
            "failed_units": [u for u in units if u["status"] in (FAILED, MISSING)],
            "stuck": blocked is not None, "blocked_on": blocked["name"] if blocked else None,
            "phases": by_phase,
            "sensing": per_date(units, l3, cc, ymd, phases),
            "blackout": blackout_windows(fid, blackout, ymd)})
        tot["have"] += len(l3); tot["want"] += want
        tot["cc_have"] += cc_docs; tot["cc_want"] += cc_want

    summary = {"batch_proc": proc_id, "label": proc.get("label"),
               "enabled": proc.get("enabled"), "k": k,
               "provenance": provenance(blackout),
               "products": tot["have"], "products_expected": tot["want"],
               "ccslc": tot["cc_have"], "ccslc_expected": tot["cc_want"],
               "stuck_frames": [r["frame"] for r in rows if r.get("stuck")],
               "frames_with_failures": [r["frame"] for r in rows if r.get("failed_units")],
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
    if summary["stuck_frames"]:
        print("  STUCK: frame(s) %s -- a historical k-set failed and gates everything behind it"
              % ", ".join(str(f) for f in summary["stuck_frames"]))
    other = [f for f in summary["frames_with_failures"] if f not in summary["stuck_frames"]]
    if other:
        print("  FAILURES (not blocking): frame(s) %s -- forward dates with no product; the "
              "frame keeps going" % ", ".join(str(f) for f in other))
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
    print("  legend: # done   > running   X FAILED   o MISSING (never ran)   . pending   ~ no_run")
    return summary


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
        return [(i, s) for i, s in procs if i == proc_id]
    chosen = [(i, s) for i, s in procs if s.get("frame_states")]
    chosen.sort(key=lambda ps: ps[1].get("last_run_date") or "", reverse=True)
    return chosen


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--id", help="batch proc id; default is every cslc_query_hist campaign")
    ap.add_argument("--failures", action="store_true",
                    help="only show frames with a failure, blocking or not")
    ap.add_argument("--all", action="store_true",
                    help="render completed campaigns in full instead of one line")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    r = es_post("/batch_proc/_search?size=1000", {"query": {"match_all": {}}})
    procs = [(h["_id"], h["_source"]) for h in truncated(r, "batch_proc")
             if h["_source"].get("job_type") == "cslc_query_hist"]
    procs = select_campaigns(procs, args.id)
    if not procs:
        print("no matching batch proc")
        return 1

    _, jobs_by_frame = campaign_jobs()
    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    try:
        blackout = localize_disp_blackout_dates()
    except Exception as e:
        print("WARNING: blackout dates could not be read (%s); windows omitted" % e,
              file=sys.stderr)
        blackout = None
    stuck = failures = False
    for pid, proc in procs:
        s = render(pid, proc, frame_to_bursts, jobs_by_frame, blackout, args)
        stuck = stuck or bool(s.get("stuck_frames"))
        failures = failures or bool(s.get("frames_with_failures"))
    return 2 if stuck else (1 if failures else 0)


if __name__ == "__main__":
    sys.exit(main())
