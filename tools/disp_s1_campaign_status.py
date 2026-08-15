#!/usr/bin/env python3
"""Live status of a phased DISP-S1 processing campaign, per frame and per phase.

Answers the questions an operator actually has mid-campaign: how far along is the
whole thing, which frame is where, which k-set is running right now, and what is left.

Status is derived from PRODUCTS, not just the batch proc cursor. The cursor advances
when a query job is submitted; a k-set is only really done when its compressed CSLC
boundary has published. Where the two disagree, the products win.

    python disp_s1_campaign_status.py                    # every enabled campaign
    python disp_s1_campaign_status.py --id <BATCH_PROC_ID>
    python disp_s1_campaign_status.py --json
"""

import argparse
import json
import re
import subprocess
import sys

from data_subscriber import cslc_utils

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200"
CCSLC_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")
L3_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_")

DONE, RUNNING, PENDING = "done", "running", "pending"
BAR = {DONE: "#", RUNNING: ">", PENDING: "."}


def es_post(path, body):
    out = subprocess.run(["curl", "-s", "-k", "--netrc-file", NETRC, "-XPOST", ES + path,
                          "-H", "Content-Type: application/json", "-d", json.dumps(body)],
                         capture_output=True, text=True, timeout=180)
    try:
        return json.loads(out.stdout)
    except ValueError:
        return {}


def frame_products(frame_id):
    """Return (l3_secondary_dates, ccslc_boundary_dates, ccslc_doc_count)."""
    l3, cc = set(), set()
    cc_docs = 0
    for ds, acc in (("L3_DISP_S1", l3), ("L2_CSLC_S1_COMPRESSED", cc)):
        r = es_post("/grq/_search?size=3000", {
            "query": {"bool": {"must": [{"term": {"dataset.keyword": ds}},
                                        {"term": {"metadata.frame_id": frame_id}}]}},
            "_source": ["id"]})
        for h in r.get("hits", {}).get("hits", []):
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


def phase_progress(frame, phases, cursor, k, l3_dates, ccslc_dates):
    """Per-phase, per-k-set / per-date status."""
    ymd = [d.strftime("%Y%m%d") for d in frame.sensing_datetimes]
    out = []
    for p in phases:
        entry = {"label": p.label, "start": p.start_pos, "length": p.length, "units": []}
        if p.label.startswith("historical_"):
            for n, start in enumerate(range(p.start_pos, p.start_pos + p.length, k), 1):
                boundary = ymd[start + k - 1]
                if boundary in ccslc_dates:
                    st = DONE
                elif cursor > start:
                    st = RUNNING
                else:
                    st = PENDING
                entry["units"].append({"name": "k-set %d" % n, "status": st,
                                       "boundary": boundary,
                                       "dates": "%s..%s" % (ymd[start], ymd[start + k - 1])})
        elif p.label.startswith("forward_"):
            for i in range(p.start_pos, p.start_pos + p.length):
                if ymd[i] in l3_dates:
                    st = DONE
                elif cursor > i:
                    st = RUNNING
                else:
                    st = PENDING
                entry["units"].append({"name": ymd[i], "status": st, "dates": ymd[i]})
        else:
            entry["units"].append({"name": "%d dates" % p.length, "status": "skipped",
                                   "dates": "%s..%s" % (ymd[p.start_pos],
                                                        ymd[p.start_pos + p.length - 1])})
        out.append(entry)
    return out


def expected(phases, k, bursts):
    prod = ccslc = 0
    for p in phases:
        if p.label.startswith("historical_"):
            prod += p.length - 1
            ccslc += (p.length // k) * bursts
        elif p.label.startswith("forward_"):
            prod += p.length
    return prod, ccslc


def render(proc_id, proc, frame_to_bursts, as_json):
    k = int(proc.get("k", 15))
    states = {int(f): int(c) for f, c in (proc.get("frame_states") or {}).items()}
    rows, tot_have, tot_want, tot_cc_have, tot_cc_want = [], 0, 0, 0, 0

    for fid in sorted(states):
        fr = frame_to_bursts.get(fid)
        if fr is None:
            continue
        phases = getattr(fr, "phases", None)
        l3, cc, cc_docs = frame_products(fid)
        bursts = len(fr.burst_ids)
        if phases:
            want, cc_want = expected(phases, k, bursts)
            prog = phase_progress(fr, phases, states[fid], k, l3, cc)
        else:
            want, cc_want, prog = 0, 0, []
        rows.append({"frame": fid, "cursor": states[fid], "bursts": bursts,
                     "products": len(l3), "expected": want,
                     "ccslc": cc_docs, "ccslc_expected": cc_want, "phases": prog})
        tot_have += len(l3); tot_want += want
        tot_cc_have += cc_docs; tot_cc_want += cc_want

    summary = {"batch_proc": proc_id, "label": proc.get("label"),
               "enabled": proc.get("enabled"), "k": k,
               "products": tot_have, "products_expected": tot_want,
               "ccslc": tot_cc_have, "ccslc_expected": tot_cc_want, "frames": rows}
    if as_json:
        print(json.dumps(summary, indent=2)); return

    pct = int(tot_have / tot_want * 100) if tot_want else 0
    print("=" * 78)
    print("%s   [%s]" % (proc.get("label"), "enabled" if proc.get("enabled") else "disabled"))
    print("  %d/%d products (%d%%)   %d/%d compressed CSLCs   k=%d"
          % (tot_have, tot_want, pct, tot_cc_have, tot_cc_want, k))
    print("=" * 78)
    for r in rows:
        fpct = int(r["products"] / r["expected"] * 100) if r["expected"] else 0
        bar = ""
        for ph in r["phases"]:
            bar += "".join(BAR.get(u["status"], "~") for u in ph["units"]) + " "
        print("\n  frame %-7s %3d/%-3d products (%3d%%)  %2d bursts   [%s]"
              % (r["frame"], r["products"], r["expected"], fpct, r["bursts"], bar.strip()))
        for ph in r["phases"]:
            counts = {}
            for u in ph["units"]:
                counts[u["status"]] = counts.get(u["status"], 0) + 1
            tally = "  ".join("%d %s" % (n, s) for s, n in
                              sorted(counts.items(), key=lambda x: x[0]))
            print("     %-16s @%-4d %-3d dates   %s" % (ph["label"], ph["start"],
                                                        ph["length"], tally))
            running = [u for u in ph["units"] if u["status"] == RUNNING]
            pending = [u for u in ph["units"] if u["status"] == PENDING]
            for u in running:
                print("        -> RUNNING  %-10s %s" % (u["name"], u["dates"]))
            if pending:
                nxt = pending[0]
                more = "  (+%d more)" % (len(pending) - 1) if len(pending) > 1 else ""
                print("           next     %-10s %s%s" % (nxt["name"], nxt["dates"], more))
    print("\n  legend: # done   > running   . pending   ~ skipped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", help="batch proc id; default is every enabled cslc_query_hist proc")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    r = es_post("/batch_proc/_search?size=200", {"query": {"match_all": {}}})
    procs = [(h["_id"], h["_source"]) for h in r.get("hits", {}).get("hits", [])
             if h["_source"].get("job_type") == "cslc_query_hist"]
    if args.id:
        procs = [(i, s) for i, s in procs if i == args.id]
    else:
        procs = [(i, s) for i, s in procs if s.get("enabled")]
    if not procs:
        print("no matching batch proc"); return 1

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    for pid, proc in procs:
        render(pid, proc, frame_to_bursts, args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
