#!/usr/bin/env python3
"""Serialized forward DISP-S1 simulation for the dev-e2e smoke test.

Two modes (--mode):

  full-serial (default) — ingest ONE sensing date at a time and fully drain it
    (wait for its L3, plus the CCSLC at a k-boundary) before the next.  Most
    faithful to real forward ops (the ~12-day acquisition spacing is >> the
    pipeline runtime, so in reality nothing ever overlaps), and flicker-safe by
    construction — but slow: every firing date costs a full SCIFLO_L3_DISP_S1.

  boundary-serial — ingest dates in order but only SERIALIZE AT k-BOUNDARIES.
    The only pipeline-generated artifact a later date depends on is the CCSLC,
    written by the boundary SCIFLO (save_compressed_cslc).  Within an
    inter-boundary window every date references the SAME already-published
    CCSLCs, so those dates are mutually independent: advance as soon as each
    KSC reaches a terminal disposition — their L3 SCIFLOs run in the background,
    in parallel.  Only at a boundary do we wait for the new CCSLC to publish
    before crossing.  Same flicker-safety (a post-boundary date never sees a
    missing CCSLC), far fewer blocking waits -> hours instead of ~a day.  This
    is the mode for Stage B/C scale.

Per date D the cascade is:
    cslc_catalog_ingest (metadata-only L2_CSLC_S1) -> cycle_evaluator -> CSC
      -> k_cycle_evaluator -> KSC -> (if KSC fires) SCIFLO_L3_DISP_S1 -> L3
      [+ CCSLC at a k-boundary].

Robustness (learned from a de-serialization incident): the per-date wait keys on
a TERMINAL disposition of the KSC, not a fixed short timeout.  A KSC that is
merely slow to appear (its CSC exists, the k-cycle evaluator is still queued
behind other work) or transiently incomplete (e.g. a full window momentarily
"missing static layers", which the cascade re-evaluates once inputs settle) is
kept pending/incomplete and WAITED ON — not prematurely declared FAIL/no-fire,
which advances early and de-serializes the run.  A date is terminal only when it
fires, is superseded, is gap-blocked, or its still-filling window is STABLE
across the settle window.
"""
import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime

import requests
import urllib3

urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("forward_serial")

KSC_INDEX = "grq_1_disp_s1-kcycle-state-config*"
CSC_INDEX = "grq_1_cslc_s1-cycle-state-config*"
L3_INDEX = "grq_v1.0_l3_disp_s1*"
CCSLC_INDEX = "grq_1_l2_cslc_s1_compressed*"

# GranuleUR OPERA_L2_CSLC-S1_<burst>_<sensingYYYYMMDD>T..Z_.. -> (burst, date)
GRANULE_RE = re.compile(r"OPERA_L2_CSLC-S1_(T[0-9A-Za-z\-]+?)_(\d{8})T")

# Terminal dispositions of a date (the cascade has decided its fate).
FIRE = ("fire", "fire-boundary")
NOFIRE = ("no-fire-superseded", "no-fire-gap", "no-fire-incomplete")

WHITELIST = None


def isofmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def ksc_fires(meta):
    """Whether this KSC satisfies the real trigger-SCIFLO_L3_DISP_S1 user rule:

        is_complete AND compressed_cslc_final
        AND NOT gap_unresolved AND NOT (superseded_by exists)

    A KSC can be is_complete=True yet fire NOTHING — e.g. an early forward window
    superseded_by=existing_ccslc (already covered by bootstrap CCSLCs), or one
    that is gap_unresolved.  Keying on is_complete alone makes such a date hang
    waiting for an L3 that never comes.
    """
    return bool(meta
                and meta.get("is_complete")
                and meta.get("compressed_cslc_final")
                and not meta.get("gap_unresolved")
                and not meta.get("superseded_by")
                and meta.get("region", 'UNKNOWN') in WHITELIST if WHITELIST is not None else True)


def classify_ksc(meta):
    """Pure disposition of a date from one KSC metadata snapshot.

    Returns:
      'fire-boundary'      — fires a SCIFLO that ALSO writes a CCSLC
                             (save_compressed_cslc); downstream dates depend on it.
      'fire'               — fires a SCIFLO (L3 only, non-boundary); nothing in the
                             cascade depends on its L3.
      'no-fire-superseded' — complete window already covered by an existing CCSLC.
      'no-fire-gap'        — blocked by an unresolved acquisition gap.
      'incomplete'         — KSC exists, not firing, not superseded/gap: window
                             still resolving.  Transient (will re-eval to fire) OR
                             terminal (early seeded date) — the caller decides via
                             window-fullness + stability, NOT here.
      'pending'            — no KSC yet; cascade still working -> keep waiting.
    """
    if meta is None:
        return "pending"
    if ksc_fires(meta):
        return "fire-boundary" if meta.get("save_compressed_cslc") else "fire"
    if meta.get("superseded_by"):
        return "no-fire-superseded"
    if meta.get("gap_unresolved"):
        return "no-fire-gap"
    return "incomplete"


def window_full(meta):
    """True if the KSC's k-window has all its cycles (cycles_complete >=
    cycles_expected).  A full-but-not-firing window is waiting on ancillary
    inputs (static layers / ionosphere) that the cascade re-evaluates -> a
    TRANSIENT incomplete to keep waiting on, NOT an early seeded date."""
    try:
        ce = int(meta.get("cycles_expected") or 0)
        cc = int(meta.get("cycles_complete") or 0)
    except (TypeError, ValueError):
        return False
    return ce > 0 and cc >= ce


def submit_catalog_ingest(mozart_ip, job_release, queue, frame_id, s_date, e_date):
    """Submit one narrow-window cslc_catalog_ingest (enable_dedup=false)."""
    url = f"https://{mozart_ip}/mozart/api/v0.1/job/submit?enable_dedup=false"
    params = {
        "queue": queue,
        "priority": "0",
        "tags": json.dumps(["e2e-test", "forward-processing", "serial"]),
        "type": f"job-cslc_catalog_ingest:{job_release}",
        "params": json.dumps({
            "frame_ids": str(frame_id),
            "start_date": isofmt(s_date),
            "end_date": isofmt(e_date),
        }),
        "name": f"e2e-cslc_catalog_ingest-fwd-f{frame_id}-{s_date.strftime('%Y%m%d')}",
    }
    r = requests.post(url, data=params, verify=False, timeout=60)
    r.raise_for_status()
    res = r.json()
    if not res.get("success"):
        raise RuntimeError(f"catalog_ingest submit failed: {res}")
    return res["result"]


def es_count(es, index, body):
    try:
        return es.es.count(index=index, body=body).get("count", 0)
    except Exception as e:
        msg = str(e)
        if "index_not_found" in msg or "404" in msg:
            return 0
        raise


def l3_count(es):
    return es_count(es, L3_INDEX, {})


def ccslc_count(es, frame_id):
    return es_count(es, CCSLC_INDEX, {"query": {"term": {"metadata.frame_id": frame_id}}})


def get_ksc(es, frame_id, sensing_int):
    body = {"query": {"bool": {"must": [
        {"term": {"metadata.frame_id": frame_id}},
        {"term": {"metadata.sensing_date": sensing_int}}]}},
        "size": 1,
        "_source": ["metadata.is_complete", "metadata.compressed_cslc_final",
                    "metadata.save_compressed_cslc", "metadata.gap_unresolved",
                    "metadata.superseded_by", "metadata.cycles_complete",
                    "metadata.cycles_expected", "metadata.completeness_reason"]}
    try:
        hits = es.es.search(index=KSC_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        if "index_not_found" in str(e) or "404" in str(e):
            return None
        raise
    return hits[0]["_source"].get("metadata", {}) if hits else None


def csc_exists(es, frame_id, sensing_int):
    """Does the CSC (cycle-state-config) for this date exist yet?  CSC is created
    by the cycle_evaluator, upstream of the KSC.  CSC present + KSC absent means
    the k-cycle evaluator is still pending — keep waiting, don't declare a miss."""
    body = {"query": {"bool": {"must": [
        {"term": {"metadata.frame_id": frame_id}},
        {"match": {"metadata.sensing_date": sensing_int}}]}}}
    return es_count(es, CSC_INDEX, body) > 0


def wait_for_disposition(es, frame_id, sensing_int, ksc_timeout_s, poll_s,
                         incomplete_settle_s):
    """Wait until the date reaches a TERMINAL disposition, robust to cascade lag.

    Returns (disp, meta) where disp is one of FIRE, NOFIRE, or 'fail-no-ksc'.

      - 'pending' (no KSC): keep waiting up to ksc_timeout_s.  Logs once whether
        the CSC exists (CSC present => the kce is merely queued, expected to
        resolve; CSC absent => still in cycle_evaluator / ingest).
      - 'incomplete' with a FULL window: keep waiting (transient — the cascade
        re-evaluates when static-layer/ionosphere inputs settle).
      - 'incomplete' with a still-filling window: terminal no-fire once the
        (cycles_complete, reason) signature is STABLE for incomplete_settle_s
        (an early seeded date whose window will never fill).
    """
    deadline = time.monotonic() + ksc_timeout_s
    stable_sig = None
    stable_since = None
    logged_pending = False
    while time.monotonic() < deadline:
        meta = get_ksc(es, frame_id, sensing_int)
        disp = classify_ksc(meta)
        if disp in FIRE or disp in NOFIRE:
            return disp, meta
        if disp == "pending":
            if not logged_pending:
                logged_pending = True
                has_csc = csc_exists(es, frame_id, sensing_int)
                logger.info(f"    KSC not yet created (CSC exists={has_csc}); "
                            f"cascade still working — waiting")
        else:  # 'incomplete'
            if window_full(meta):
                pass  # transient: full window awaiting ancillary inputs — keep waiting
            else:
                sig = (meta.get("cycles_complete"), meta.get("completeness_reason"))
                now = time.monotonic()
                if sig != stable_sig:
                    stable_sig, stable_since = sig, now  # progress -> reset clock
                elif now - stable_since >= incomplete_settle_s:
                    return "no-fire-incomplete", meta    # stable, still filling -> terminal
        time.sleep(poll_s)

    # Timed out at the cap.  Decide from the final snapshot rather than the clock.
    meta = get_ksc(es, frame_id, sensing_int)
    if meta is None:
        return "fail-no-ksc", None
    if ksc_fires(meta):
        return ("fire-boundary" if meta.get("save_compressed_cslc") else "fire"), meta
    if meta.get("superseded_by"):
        return "no-fire-superseded", meta
    if meta.get("gap_unresolved"):
        return "no-fire-gap", meta
    return "no-fire-incomplete", meta


def wait_for_date(es, frame_id, sensing_dt, base_l3, base_ccslc, mode,
                  ksc_timeout_s, l3_timeout_s, ccslc_timeout_s, poll_s,
                  incomplete_settle_s):
    """Resolve one date: wait for its terminal disposition, then —

      - fire-boundary -> ALWAYS wait for the new CCSLC (+ its L3): later dates
        depend on it.  This is the serialization point.
      - fire (non-boundary): full-serial waits for the L3; boundary-serial
        advances immediately (its SCIFLO runs in the background).
      - no-fire* -> advance immediately, no product.
    """
    sensing_int = int(sensing_dt.strftime("%Y%m%d"))
    disp, meta = wait_for_disposition(es, frame_id, sensing_int,
                                      ksc_timeout_s, poll_s, incomplete_settle_s)
    is_boundary = bool(meta and meta.get("save_compressed_cslc"))

    if disp == "fail-no-ksc":
        return {"ok": False, "reason": "KSC never created", "boundary": None, "disp": disp}

    if disp in NOFIRE:
        return {"ok": True, "fired": False, "boundary": is_boundary, "disp": disp,
                "superseded": disp == "no-fire-superseded",
                "l3": l3_count(es)}

    if disp == "fire-boundary":
        # Wait for the boundary's CCSLC (and its L3) before crossing — every mode.
        deadline = time.monotonic() + ccslc_timeout_s
        while time.monotonic() < deadline:
            l3_now, ccslc_now = l3_count(es), ccslc_count(es, frame_id)
            if ccslc_now > base_ccslc and l3_now > base_l3:
                return {"ok": True, "fired": True, "boundary": True, "disp": disp,
                        "l3": l3_now, "ccslc": ccslc_now}
            time.sleep(poll_s)
        return {"ok": False, "fired": True, "timeout": True, "boundary": True, "disp": disp,
                "l3": l3_count(es), "ccslc": ccslc_count(es, frame_id)}

    # disp == 'fire' (non-boundary).
    if mode == "boundary-serial":
        # Nothing in the cascade depends on this L3 -> advance now; its SCIFLO
        # runs in the background, in parallel with later dates in this window.
        return {"ok": True, "fired": True, "boundary": False, "disp": disp,
                "async": True, "l3": base_l3}
    deadline = time.monotonic() + l3_timeout_s
    while time.monotonic() < deadline:
        l3_now = l3_count(es)
        if l3_now > base_l3:
            return {"ok": True, "fired": True, "boundary": False, "disp": disp, "l3": l3_now}
        time.sleep(poll_s)
    return {"ok": False, "fired": True, "timeout": True, "boundary": False, "disp": disp,
            "l3": l3_count(es)}


def drain_inflight(es, frame_id, poll_s, stable_checks, max_wait_s):
    """boundary-serial leaves non-boundary SCIFLOs running in the background.
    After the last date, wait for the L3 count to stop rising (all drained)
    before declaring done, so the final composition is complete."""
    logger.info("draining in-flight background SCIFLOs (L3 count must settle)...")
    deadline = time.monotonic() + max_wait_s
    last, stable = l3_count(es), 0
    while time.monotonic() < deadline:
        time.sleep(poll_s)
        now = l3_count(es)
        if now == last:
            stable += 1
            if stable >= stable_checks:
                logger.info(f"    L3 settled at {now} (CCSLC={ccslc_count(es, frame_id)})")
                return now
        else:
            stable = 0
            logger.info(f"    L3 {last} -> {now}, still draining")
            last = now
    logger.warning(f"    drain timed out; L3={last}")
    return last


def filter_to_complete_coverage(cci, frame_id, start_date, end_date, settings, discovered_dts):
    """Drop discovered sensing dates lacking the frame's FULL burst set in CMR.

    DISP-S1 cycles are full-frame-only, so a partial-coverage acquisition (e.g. a
    post-gap S1A pass that images only a subset of the frame's bursts) mints an
    INCOMPLETE CSC that can never complete a k-window and trips the
    lineage-gap-unresolved guard.  This opt-in gate keeps only dates whose CMR
    coverage is the full frame, so the forward cascade sees a clean all-complete
    window.  The consistent burst DB already excludes such partials; the burst[0]-
    based discovery does not, which this restores.

    Reuses the deployed CslcCatalogIngest.frame_to_bursts + _query_cmr_for_frame
    (read-only; cslc_catalog_ingest is NOT modified).  _query_cmr_for_frame queries
    CMR for exactly the frame's burst_ids, so per-date found bursts are a subset of
    expected; a date is complete iff its distinct-burst count equals expected.
    """
    from data_subscriber.cmr import get_cmr_token
    expected_n = len(cci.frame_to_bursts[frame_id].burst_ids)
    cmr_hostname, token, _, _, _ = get_cmr_token("OPS", settings)
    items = cci._query_cmr_for_frame(frame_id, start_date, end_date, cmr_hostname, token)
    found_by_date = {}
    for it in items:
        gid = it.get("umm", {}).get("GranuleUR", "") or it.get("meta", {}).get("native-id", "")
        m = GRANULE_RE.search(gid)
        if m:
            found_by_date.setdefault(m.group(2), set()).add(m.group(1))
    complete = {d for d, bursts in found_by_date.items() if len(bursts) >= expected_n}
    kept = [dt for dt in discovered_dts if dt.strftime("%Y%m%d") in complete]
    dropped = [dt.strftime("%Y%m%d") for dt in discovered_dts
               if dt.strftime("%Y%m%d") not in complete]
    logger.info(f"require-full-coverage: kept {len(kept)} full-coverage date(s), "
                f"dropped {len(dropped)} partial (need {expected_n} bursts/date)")
    if dropped:
        logger.info(f"    dropped (partial-coverage): {dropped}")
    return kept


def run(es, args):
    from data_subscriber.cslc.cslc_catalog_ingest import CslcCatalogIngest
    from util.conf_util import SettingsConf
    global WHITELIST

    # Set WHITELIST to value in args
    WHITELIST = args.region_whitelist
    logger.info(f'Set {WHITELIST=}')

    # Enumerate the ACTUAL forward sensing dates via the bulk ingest's own
    # discovery in DRY-RUN mode (gap check + seeded start_date + CMR query +
    # ccslc-lineage filter, live es_conn) — exactly the CSLCs the real ingest
    # would produce, NOT the consistent-burst DB (blackout-filtered, historical).
    settings = SettingsConf().cfg
    cci = CslcCatalogIngest(settings, es_conn=es)
    discovered = cci.ingest([args.frame_id], args.start_date, args.end_date, dry_run=True)
    date_strs = discovered.get(args.frame_id, [])
    fwd_dts = [datetime.strptime(d, "%Y-%m-%d") for d in date_strs]
    if getattr(args, "require_full_coverage", False):
        fwd_dts = filter_to_complete_coverage(
            cci, args.frame_id, args.start_date, args.end_date, settings, fwd_dts)
    logger.info(f"frame {args.frame_id}: {len(fwd_dts)} forward sensing dates "
                f"(dry-run discovery) in [{args.start_date}, {args.end_date}]; "
                f"mode={args.mode}")
    if not fwd_dts:
        logger.error("no forward sensing dates discovered in range")
        return 2
    gaps = [(fwd_dts[i] - fwd_dts[i - 1]).days for i in range(1, len(fwd_dts))]
    logger.info(f"sensing-date gaps (days): {sorted(set(gaps))} "
                f"(6/12 expected; larger = real acquisition gap)")

    ksc_timeout_s = args.ksc_timeout_mins * 60
    l3_timeout_s = args.l3_timeout_mins * 60
    ccslc_timeout_s = args.ccslc_timeout_mins * 60
    incomplete_settle_s = args.incomplete_settle_mins * 60
    if args.start_index > 1:
        logger.info(f"resuming at index {args.start_index}/{len(fwd_dts)} "
                    f"({fwd_dts[args.start_index - 1].strftime('%Y%m%d')}); "
                    f"skipping {args.start_index - 1} already-processed dates")

    results = []
    for i, dt in enumerate(fwd_dts, 1):
        if i < args.start_index:
            continue
        d = dt.strftime("%Y%m%d")
        s_date = dt.replace(hour=0, minute=0, second=0)
        e_date = dt.replace(hour=23, minute=59, second=59)
        base_l3 = l3_count(es)
        base_ccslc = ccslc_count(es, args.frame_id)
        logger.info(f"[{i}/{len(fwd_dts)}] ingest {d} (L3 base={base_l3}, CCSLC base={base_ccslc})")
        job_id = submit_catalog_ingest(args.mozart_ip, args.job_release, args.queue,
                                       args.frame_id, s_date, e_date)
        logger.info(f"    submitted catalog_ingest {job_id}; waiting...")
        r = wait_for_date(es, args.frame_id, dt, base_l3, base_ccslc, args.mode,
                          ksc_timeout_s, l3_timeout_s, ccslc_timeout_s,
                          args.poll_secs, incomplete_settle_s)
        r["date"] = d
        results.append(r)
        if r["ok"] and r.get("fired"):
            if r.get("async"):
                logger.info(f"    FIRED {d}: non-boundary, SCIFLO running in background — advancing")
            else:
                logger.info(f"    FIRED {d}: L3 produced "
                            f"(boundary={r['boundary']}, L3={r['l3']}, CCSLC={r.get('ccslc')})")
        elif r["ok"] and not r.get("fired"):
            reason = {"no-fire-superseded": "superseded by existing CCSLC (early window already covered)",
                      "no-fire-gap": "gap_unresolved (acquisition gap)",
                      "no-fire-incomplete": "window incomplete/stable (seeded/window-building)"}.get(
                          r.get("disp"), r.get("disp"))
            logger.info(f"    no-fire {d}: {reason} — no product, advancing")
        else:
            logger.error(f"    FAIL {d}: {r.get('reason') or 'L3/CCSLC timeout'} "
                         f"(disp={r.get('disp')}, boundary={r.get('boundary')}, "
                         f"L3={r.get('l3')}, CCSLC={r.get('ccslc')})")
            if not args.continue_on_timeout:
                logger.error("stopping (use --continue-on-timeout to keep going)")
                break

    if args.mode == "boundary-serial":
        drain_inflight(es, args.frame_id, args.poll_secs, stable_checks=4,
                       max_wait_s=ccslc_timeout_s)

    fired = sum(1 for r in results if r.get("ok") and r.get("fired"))
    nofire = sum(1 for r in results if r.get("ok") and not r.get("fired"))
    superseded = sum(1 for r in results if r.get("disp") == "no-fire-superseded")
    failed = sum(1 for r in results if not r.get("ok"))
    logger.info(f"SERIAL FORWARD DONE (mode={args.mode}): {fired} fired, "
                f"{nofire} no-fire ({superseded} superseded), {failed} failed; "
                f"{len(results)} dates processed (of {len(fwd_dts)} discovered); "
                f"final L3={l3_count(es)} CCSLC={ccslc_count(es, args.frame_id)}")
    return 0 if failed == 0 else 1


def selftest():
    """Unit tests for the pure decision logic — run with --selftest (no cluster)."""
    fire_b = {"is_complete": True, "compressed_cslc_final": True, "save_compressed_cslc": True}
    fire = {"is_complete": True, "compressed_cslc_final": True, "save_compressed_cslc": False}
    sup = {"is_complete": True, "compressed_cslc_final": True, "superseded_by": "existing_ccslc"}
    gap = {"is_complete": True, "compressed_cslc_final": True, "gap_unresolved": True}
    seeding = {"is_complete": False, "cycles_complete": 3, "cycles_expected": 15,
               "completeness_reason": "K-window incomplete: 3/15 CSCs complete"}
    static = {"is_complete": False, "cycles_complete": 15, "cycles_expected": 15,
              "completeness_reason": "15/15 CSCs complete, missing static layers"}
    cases = [
        ("None->pending", classify_ksc(None), "pending"),
        ("boundary fire", classify_ksc(fire_b), "fire-boundary"),
        ("plain fire", classify_ksc(fire), "fire"),
        ("superseded", classify_ksc(sup), "no-fire-superseded"),
        ("gap blocks fire", classify_ksc(gap), "no-fire-gap"),
        ("seeding incomplete", classify_ksc(seeding), "incomplete"),
        ("static-layer incomplete", classify_ksc(static), "incomplete"),
    ]
    ok = True
    for name, got, want in cases:
        flag = "ok " if got == want else "FAIL"
        if got != want:
            ok = False
        print(f"  [{flag}] {name}: {got} (want {want})")
    # ksc_fires + window_full
    checks = [
        ("fires boundary", ksc_fires(fire_b), True),
        ("fires plain", ksc_fires(fire), True),
        ("superseded !fires", ksc_fires(sup), False),
        ("gap !fires", ksc_fires(gap), False),
        ("seeding !fires", ksc_fires(seeding), False),
        ("static full window", window_full(static), True),
        ("seeding window not full", window_full(seeding), False),
        ("fire-meta window not tracked", window_full(fire), False),
    ]
    for name, got, want in checks:
        flag = "ok " if got == want else "FAIL"
        if got != want:
            ok = False
        print(f"  [{flag}] {name}: {got} (want {want})")
    print("SELFTEST:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    ap = argparse.ArgumentParser(description="Serialized forward DISP-S1 driver")
    ap.add_argument("--frame-id", type=int, default=31241)
    ap.add_argument("--start-date", required=True, help="forward start, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--end-date", required=True, help="forward end, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--mozart-ip", required=True)
    ap.add_argument("--job-release", required=True)
    ap.add_argument("--queue", default="opera-job_worker-cslc_data_download")
    ap.add_argument("--mode", choices=["full-serial", "boundary-serial"], default="full-serial",
                    help="full-serial: drain every date's L3 (most faithful, slow). "
                         "boundary-serial: only block at CCSLC boundaries (Stage B/C scale).")
    ap.add_argument("--ksc-timeout-mins", type=int, default=60,
                    help="max wait for a date's KSC to reach a terminal disposition")
    ap.add_argument("--l3-timeout-mins", type=int, default=120,
                    help="full-serial: max wait for a firing non-boundary date's L3")
    ap.add_argument("--ccslc-timeout-mins", type=int, default=180,
                    help="max wait for a boundary's CCSLC (may queue behind in-flight SCIFLOs)")
    ap.add_argument("--incomplete-settle-mins", type=int, default=5,
                    help="a still-filling window unchanged this long is terminal no-fire")
    ap.add_argument("--poll-secs", type=int, default=30)
    ap.add_argument("--continue-on-timeout", action="store_true",
                    help="advance past a date that times out (default: stop)")
    ap.add_argument("--require-full-coverage", action="store_true",
                    help="drop discovered sensing dates lacking the frame's FULL burst "
                         "set in CMR (e.g. post-gap S1A partial passes that image only a "
                         "subset of the frame); for gap-restart of partial-coverage "
                         "frames. Default off.")
    ap.add_argument("--start-index", type=int, default=1,
                    help="1-based index into discovered dates to resume from "
                         "(skips already-processed earlier dates; their state stays in place)")
    ap.add_argument('--region-whitelist', type=str, default=None, nargs='+',
                    help='List of regions to whitelist, None to disable')
    args = ap.parse_args()

    from opera_commons.es_connection import get_grq_es
    es = get_grq_es()
    sys.exit(run(es, args))


if __name__ == "__main__":
    main()
