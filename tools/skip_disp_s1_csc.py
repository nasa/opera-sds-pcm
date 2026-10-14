#!/usr/bin/env python3
"""Skip one sensing date of a DISP-S1 frame, then re-evaluate the k-cycles held behind it.

WHY THIS EXISTS

Forward processing stops for a frame at the first sensing date where only some of its
bursts have a CSLC, if that date falls after the end of the consistent burst database's
survey. Before the survey end the database leaves such a pass out and the k-cycle
evaluator steps over it (db_excluded). After it, nothing does: the partial cycle state
config (CSC) never fires the k-cycle rule, and every later k-cycle state config (KSC) of
the frame stays incomplete or gap_unresolved behind it. Nothing errors and nothing fails;
the frame just stops making products.

Skipping the date tells the evaluator to treat it exactly like a date the burst database
excluded: it takes no k-slot, does not count toward a k-boundary, is not a gap in the
lineage and is never projected as a pending boundary. The reason, who and when are
recorded on the CSC. The tool then re-evaluates the frame's KSCs after the date that have
not fired, one k-cycle evaluator job at a time in date order, so each boundary settles
before the KSCs that depend on it. KSCs that already fired are never touched.

TYPICAL USE

    python tools/skip_disp_s1_csc.py --frame-id 9154 --date 20260508 \\
        --reason "2 bursts not imaged (ASF SENTINEL-1_BURSTS)" --by gmanipon     # dry run
    python tools/skip_disp_s1_csc.py ... --apply

    python tools/skip_disp_s1_csc.py --frame-id 9154 --date 20260508 --unskip --apply
    python tools/skip_disp_s1_csc.py --frame-id 9154 --date 20260508 --reevaluate-only --apply
    python tools/skip_disp_s1_csc.py --list                  # every skipped date and why
    python tools/skip_disp_s1_csc.py --list --frame-id 9154 --csv > skipped_9154.csv

A skip is refused if a KSC that fires holds the date in its k-window: products were (or
will be) made with it. An unskip is refused once any later KSC has fired: products were
made without it. Both would leave published products and the frame's k-cycles disagreeing.

--reevaluate-only leaves the CSC alone and just re-evaluates the unfired KSCs after the
date: to finish a run that stopped at a failed or timed-out job, or to release KSCs still
held after the partial date's CSLCs arrived. After each job the tool re-reads the KSC and
prints what it now says. A KSC that is complete but waiting on an earlier boundary's CCSLC
set is not recomputed by a re-evaluation job (the evaluator only does that on a CCSLC-set
cascade); the tool says so rather than counting it.

Re-evaluation does not ingest anything. Dates after the skipped one that were never
cataloged (a forward walk that stopped at the date, say) still need their CSLCs; restart
that walk at the skipped date and it steps over it as a terminal no-fire.

The skip lives on the CSC document in GRQ. The cycle evaluator carries it over whenever
it re-creates the CSC, so a late burst or a re-ingest does not undo it. The CSC dataset in
S3 is not rewritten, so a GRQ rebuilt from the dataset bucket comes back without the skips
that were never carried over: `--list --csv` is the record to re-apply them from (GRQ
snapshots carry them). A cycle evaluator job that read the CSC just before the skip and
publishes just after it can replace the document without the skip; the tool re-reads the
CSC after marking it and again at the end, and says if the skip is gone.
"""

import argparse
import csv
import getpass
import json
import os
import socket
import sys
import time
from datetime import datetime, timezone

import requests
import urllib3

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import make_csc_id

urllib3.disable_warnings()

CSC_INDEX = "grq_*_cslc_s1-cycle-state-config*"
KSC_INDEX = "grq_*_disp_s1-kcycle-state-config*"
SDS_CONFIG = os.path.expanduser("~/.sds/config")
TERMINAL_JOB_STATUSES = ("job-completed", "job-failed", "job-revoked", "job-offline", "job-deduped")


def ksc_fired(meta):
    """Did this KSC fire its SCIFLO? Mirrors the trigger-SCIFLO_L3_DISP_S1 rule."""
    return bool(meta.get(c.IS_COMPLETE) and meta.get(c.COMPRESSED_CSLC_FINAL)
                and not meta.get(c.GAP_UNRESOLVED) and not meta.get(c.SUPERSEDED_BY))


def find_csc(es, frame_id, date):
    """The CSC hit for frame_id/date, or None."""
    hits = es.search(index=CSC_INDEX, body={
        "query": {"ids": {"values": [make_csc_id(frame_id, date)]}}, "size": 1})["hits"]["hits"]
    return hits[0] if hits else None


def kscs_from(es, frame_id, date):
    """KSC hits of the frame with sensing_date on or after date, oldest first."""
    try:
        return es.search(index=KSC_INDEX, body={
            "query": {"bool": {"must": [
                {"term": {"metadata.frame_id": frame_id}},
                {"range": {"metadata.sensing_date": {"gte": date}}}]}},
            "sort": [{"metadata.sensing_date": {"order": "asc"}}],
            "size": 1000})["hits"]["hits"]
    except Exception as e:
        if "index_not_found" in str(e) or "404" in str(e):
            return []
        raise


def plan(frame_id, date, csc_hit, ksc_hits, unskip, reevaluate_only=False):
    """What --apply would do. Returns (refusal, to_reevaluate).

    refusal is '' when the change is allowed. to_reevaluate lists the KSC hits to
    re-evaluate, in date order. reevaluate_only changes nothing on the CSC, so nothing
    is refused.
    """
    meta = csc_hit["_source"].get("metadata", {})
    later = [h for h in ksc_hits if h["_source"]["metadata"].get(c.SENSING_DATE) > date]
    fired_later = [h["_source"]["metadata"][c.SENSING_DATE] for h in later
                   if ksc_fired(h["_source"]["metadata"])]

    if reevaluate_only:
        pass
    elif unskip:
        if not meta.get(c.SKIPPED):
            return f"frame {frame_id} {date} is not skipped", []
        if fired_later:
            return (f"KSCs after {date} have already fired without it ({', '.join(fired_later[:5])}"
                    f"{' ...' if len(fired_later) > 5 else ''}); putting the date back would make the "
                    f"frame's k-cycles disagree with published products"), []
    else:
        if meta.get(c.SKIPPED):
            return (f"frame {frame_id} {date} is already skipped ({meta.get(c.SKIP_REASON)}, "
                    f"by {meta.get(c.SKIPPED_BY)} at {meta.get(c.SKIPPED_AT)})"), []
        holding = [h["_source"]["metadata"][c.SENSING_DATE] for h in ksc_hits
                   if ksc_fired(h["_source"]["metadata"])
                   and date in (h["_source"]["metadata"].get(c.WINDOW_SENSING_DATES) or [])]
        if holding:
            return (f"KSCs that fire hold {date} in their k-window ({', '.join(holding[:5])}"
                    f"{' ...' if len(holding) > 5 else ''}); products were or will be made with it"), []

    return "", [h for h in later if not ksc_fired(h["_source"]["metadata"])]


def describe(frame_id, date, csc_hit, ksc_hits, to_reevaluate):
    meta = csc_hit["_source"].get("metadata", {})
    expected = meta.get(c.EXPECTED_BURST_IDS) or []
    found = meta.get(c.FOUND_BURST_IDS) or []
    missing = sorted(set(expected) - set(found))
    print(f"frame {frame_id} {date}: {csc_hit['_id']}")
    print(f"   coverage {len(found)}/{len(expected)}, is_complete={meta.get(c.IS_COMPLETE)}, "
          f"blackout={meta.get(c.BLACKOUT)}, db_excluded={meta.get(c.DB_EXCLUDED)}, "
          f"skipped={meta.get(c.SKIPPED, False)}")
    if missing:
        print(f"   missing bursts: {', '.join(missing)}")
    if meta.get(c.SKIPPED):
        print(f"   skipped by {meta.get(c.SKIPPED_BY)} at {meta.get(c.SKIPPED_AT)}: {meta.get(c.SKIP_REASON)}")
    print(f"   KSCs on or after {date}: {len(ksc_hits)}; not fired after it: {len(to_reevaluate)}")
    for h in to_reevaluate:
        m = h["_source"]["metadata"]
        note = "  (not recomputed by a job: waits for the next CCSLC-set cascade)" if not recomputable(m) else ""
        print(f"      {m.get(c.SENSING_DATE)}  final={m.get(c.COMPRESSED_CSLC_FINAL)} "
              f"gap_unresolved={m.get(c.GAP_UNRESOLVED)}  {(m.get(c.COMPLETENESS_REASON) or '')[:110]}{note}")


def recomputable(meta):
    """Will a k-cycle evaluator re-evaluation job recompute this KSC?

    A job cannot pass force_publish, so the evaluator skips a KSC that is complete and not
    held by a gap; one that fired is rotation-locked anyway.
    """
    return not (meta.get(c.IS_COMPLETE) and not meta.get(c.GAP_UNRESOLVED))


def ksc_state(es, frame_id, date):
    """Short summary of the KSC for frame_id/date as it is now."""
    hits = es.search(index=KSC_INDEX, body={"query": {"bool": {"must": [
        {"term": {"metadata.frame_id": frame_id}}, {"term": {"metadata.sensing_date": date}}]}},
        "size": 1})["hits"]["hits"]
    if not hits:
        return "no KSC"
    m = hits[0]["_source"].get("metadata", {})
    if ksc_fired(m):
        return "fires" + (" (k-boundary)" if m.get(c.SAVE_COMPRESSED_CSLC) else "")
    if m.get(c.GAP_UNRESOLVED):
        return "still held by a gap"
    if m.get(c.IS_COMPLETE):
        return f"complete, awaiting CCSLC sets at {','.join(m.get(c.COMPRESSED_CSLC_PENDING) or [])}"
    return f"incomplete: {(m.get(c.COMPLETENESS_REASON) or '')[:90]}"


def skip_in_place(es, frame_id, date):
    """Is the CSC still skipped? (A cycle evaluator job can replace the document.)"""
    es.indices.refresh(index=CSC_INDEX, ignore_unavailable=True, allow_no_indices=True)
    hit = find_csc(es, frame_id, date)
    return bool(hit and hit["_source"].get("metadata", {}).get(c.SKIPPED))


def mark(es, csc_hit, skip_fields):
    """Set (or clear) the skip on the CSC document and make it visible to the next search."""
    es.update(index=csc_hit["_index"], id=csc_hit["_id"], body={"doc": {"metadata": skip_fields}},
              refresh="wait_for")


def submit_reevaluation(mozart_ip, job_release, queue, k, m, ksc_hit, stamp):
    """Submit one k-cycle evaluator job on a KSC (re-evaluation, no cascade), dedup off."""
    src = ksc_hit["_source"]
    meta = src.get("metadata", {})
    params = {
        "product_paths": next((u for u in (src.get("urls") or []) if u.startswith("s3://")), ""),
        "product_metadata": {"metadata": meta},
        "dataset_type": src.get("dataset", c.DISP_S1_KCYCLE_STATE_CONFIG),
        "input_dataset_id": ksc_hit["_id"],
        "k": k,
        "m": m,
    }
    form = {
        "queue": queue,
        "priority": 5,
        "tags": json.dumps(["disp_s1_skip_csc_reevaluate"]),
        "type": f"job-disp_s1_k_cycle_evaluator:{job_release}",
        "params": json.dumps(params),
        "name": f"skip-reevaluate-kce-f{meta.get(c.FRAME_ID)}-{meta.get(c.SENSING_DATE)}-{stamp}",
    }
    r = requests.post(f"https://{mozart_ip}/mozart/api/v0.1/job/submit?enable_dedup=false",
                      data=form, verify=False, timeout=120)
    r.raise_for_status()
    res = r.json()
    if not res.get("success"):
        raise RuntimeError(f"k-cycle evaluator submit failed: {res}")
    return res["result"]


def wait_for_job(mozart_ip, job_id, timeout_s, poll_s=10):
    """Poll Mozart until the job reaches a terminal status; returns it, or 'timeout'."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        time.sleep(poll_s)
        try:
            r = requests.get(f"https://{mozart_ip}/mozart/api/v0.1/job/status",
                             params={"id": job_id}, verify=False, timeout=60)
            status = r.json().get("status", "") if r.status_code == 200 else ""
        except (requests.RequestException, ValueError):
            status = ""
        if status in TERMINAL_JOB_STATUSES:
            return status
    return "timeout"


def list_skipped(es, frame_id, as_csv):
    must = [{"term": {f"metadata.{c.SKIPPED}": True}}]
    if frame_id is not None:
        must.append({"term": {"metadata.frame_id": frame_id}})
    try:
        hits = es.search(index=CSC_INDEX, body={
            "query": {"bool": {"must": must}},
            "sort": [{"metadata.frame_id": {"order": "asc"}}, {"metadata.sensing_date": {"order": "asc"}}],
            "size": 10000})["hits"]["hits"]
    except Exception as e:
        if "index_not_found" in str(e) or "404" in str(e):
            hits = []
        else:
            raise
    rows = []
    for h in hits:
        m = h["_source"].get("metadata", {})
        rows.append({"frame_id": m.get(c.FRAME_ID), "sensing_date": m.get(c.SENSING_DATE),
                     "coverage": f"{len(m.get(c.FOUND_BURST_IDS) or [])}/{len(m.get(c.EXPECTED_BURST_IDS) or [])}",
                     "skip_reason": m.get(c.SKIP_REASON), "skipped_by": m.get(c.SKIPPED_BY),
                     "skipped_at": m.get(c.SKIPPED_AT)})
    if as_csv:
        w = csv.DictWriter(sys.stdout, fieldnames=["frame_id", "sensing_date", "coverage", "skip_reason",
                                                   "skipped_by", "skipped_at"])
        w.writeheader()
        w.writerows(rows)
    else:
        for r in rows:
            print(f"{r['frame_id']:>6} {r['sensing_date']}  {r['coverage']:>6}  {r['skipped_at']}  "
                  f"{r['skipped_by']}  {r['skip_reason']}")
        print(f"{len(rows)} skipped date(s)")
    return 0


def sds_config():
    from util.conf_util import SettingsConf
    return SettingsConf(file=SDS_CONFIG).cfg


def main(argv=None, es=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int)
    ap.add_argument("--date", help="sensing date, YYYYMMDD")
    ap.add_argument("--reason", help="why the date is skipped (required to skip)")
    ap.add_argument("--by", default=f"{getpass.getuser()}@{socket.gethostname()}",
                    help="who is skipping it (default: user@host)")
    ap.add_argument("--unskip", action="store_true", help="put a skipped date back")
    ap.add_argument("--list", action="store_true", help="list skipped dates (all, or --frame-id)")
    ap.add_argument("--csv", action="store_true", help="--list as CSV")
    ap.add_argument("--apply", action="store_true", help="make the change; otherwise dry run")
    ap.add_argument("--no-reevaluate", action="store_true",
                    help="only mark the CSC; leave the KSCs after it for the next cascade")
    ap.add_argument("--reevaluate-only", action="store_true",
                    help="leave the CSC alone and re-evaluate the unfired KSCs after the date (resume a "
                         "run that stopped, or release KSCs still held after the gap was resolved)")
    ap.add_argument("--k", type=int, default=15)
    ap.add_argument("--m", type=int, default=6)
    ap.add_argument("--mozart-ip", help="defaults to MOZART_PVT_IP from ~/.sds/config")
    ap.add_argument("--job-release", help="defaults to STAGING_AREA.JOB_RELEASE from ~/.sds/config")
    ap.add_argument("--queue", default="opera-job_worker-evaluator")
    ap.add_argument("--job-timeout-mins", type=int, default=30,
                    help="max wait for each re-evaluation job")
    args = ap.parse_args(argv)

    if es is None:
        from opera_commons.es_connection import get_grq_es
        es = get_grq_es().es

    if args.list:
        return list_skipped(es, args.frame_id, args.csv)

    if args.frame_id is None or not args.date:
        ap.error("--frame-id and --date are required")
    if len(args.date) != 8 or not args.date.isdigit():
        ap.error("--date must be YYYYMMDD")
    if args.unskip and args.reevaluate_only:
        ap.error("--unskip and --reevaluate-only do not go together")
    if not args.unskip and not args.reevaluate_only and not (args.reason or "").strip():
        ap.error("--reason is required to skip a date")

    csc_hit = find_csc(es, args.frame_id, args.date)
    if csc_hit is None:
        print(f"no cycle state config for frame {args.frame_id} {args.date}; nothing to skip "
              f"(has the date been cataloged?)")
        return 1
    ksc_hits = kscs_from(es, args.frame_id, args.date)
    refusal, to_reevaluate = plan(args.frame_id, args.date, csc_hit, ksc_hits, args.unskip, args.reevaluate_only)
    describe(args.frame_id, args.date, csc_hit, ksc_hits, to_reevaluate)
    if refusal:
        print(f"\nREFUSED: {refusal}")
        return 1

    action = ("leave the CSC as it is" if args.reevaluate_only
              else "un-skip" if args.unskip else f"skip ({args.reason.strip()})")
    reeval = "none (--no-reevaluate)" if args.no_reevaluate else f"{len(to_reevaluate)} KSC(s), one at a time"
    if not args.apply:
        print(f"\nDRY RUN: would {action} frame {args.frame_id} {args.date} and re-evaluate {reeval}. "
              f"Re-run with --apply.")
        return 0

    if not args.reevaluate_only:
        if args.unskip:
            # A partial update merges, so the old values are overwritten with null rather than dropped.
            fields = {c.SKIPPED: False, c.SKIP_REASON: None, c.SKIPPED_BY: None, c.SKIPPED_AT: None}
        else:
            fields = {c.SKIPPED: True, c.SKIP_REASON: args.reason.strip(), c.SKIPPED_BY: args.by,
                      c.SKIPPED_AT: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        mark(es, csc_hit, fields)
        print(f"\n{'un-skipped' if args.unskip else 'skipped'} {csc_hit['_id']}")
        if skip_in_place(es, args.frame_id, args.date) == args.unskip:
            print("the CSC changed under the tool (a cycle evaluator job replaced it?); re-run the dry run")
            return 1

    if args.unskip and csc_hit["_source"].get("metadata", {}).get(c.IS_COMPLETE):
        # A complete date's own k-cycle comes back first, re-driven from its CSC like a fresh arrival. A
        # partial one gets no KSC, exactly as the k-cycle rule would leave it.
        to_reevaluate = [csc_hit] + to_reevaluate
    if args.no_reevaluate or not to_reevaluate:
        print("no KSCs re-evaluated")
        return 0

    cfg = sds_config() if not (args.mozart_ip and args.job_release) else {}
    mozart_ip = args.mozart_ip or cfg["MOZART_PVT_IP"]
    job_release = args.job_release or cfg["STAGING_AREA"]["JOB_RELEASE"]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    done, not_recomputed = 0, []
    for hit in to_reevaluate:
        sd = hit["_source"]["metadata"].get(c.SENSING_DATE)
        job_id = submit_reevaluation(mozart_ip, job_release, args.queue, args.k, args.m, hit, stamp)
        status = wait_for_job(mozart_ip, job_id, args.job_timeout_mins * 60)
        if status != "job-completed":
            print(f"   {sd}  {job_id}  {status}")
            print(f"stopping: the re-evaluation of {sd} ended '{status}'; later KSCs depend on it. "
                  f"Resume with --reevaluate-only once it is fixed")
            break
        done += 1
        if hit["_source"].get("dataset") == c.DISP_S1_KCYCLE_STATE_CONFIG and \
                not recomputable(hit["_source"].get("metadata", {})):
            not_recomputed.append(sd)
        print(f"   {sd}  {job_id}  {status}  -> {ksc_state(es, args.frame_id, sd)}")

    print(f"\nre-evaluation jobs completed: {done}/{len(to_reevaluate)}")
    if not_recomputed:
        print(f"not recomputed (complete, awaiting a CCSLC set; the next CCSLC-set cascade does it): "
              f"{', '.join(not_recomputed)}")
    if not args.reevaluate_only and skip_in_place(es, args.frame_id, args.date) == args.unskip:
        print("WARNING: the skip on the CSC changed while re-evaluating (a cycle evaluator job replaced it?); "
              "check with --list and re-apply")
        return 1
    return 0 if done == len(to_reevaluate) else 1


if __name__ == "__main__":
    sys.exit(main())
