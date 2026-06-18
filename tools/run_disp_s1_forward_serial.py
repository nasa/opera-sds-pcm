#!/usr/bin/env python3
"""Serialized forward DISP-S1 simulation for the dev-e2e smoke test.

Ingests CSLCs ONE sensing date at a time and waits for that date's products
to publish before ingesting the next date — mirroring real forward operations,
where each acquisition is fully processed before the next arrives (~12 days
later).  Per date D:

    1. submit a cslc_catalog_ingest over a narrow window [D-30m, D+30m]
       (metadata-only L2_CSLC_S1 -> cycle_evaluator -> CSC ->
        k_cycle_evaluator -> KSC -> SCIFLO_L3_DISP_S1)
    2. wait until KSC(D) is is_complete AND compressed_cslc_final
    3. wait until the L3_DISP_S1 for D publishes (and, at a k-boundary where
       KSC(D).save_compressed_cslc is true, until its CCSLC publishes)
    4. only then advance to D+1

The bulk cslc_catalog_ingest (whole range at once) instead floods the system
with out-of-order CSLCs, so KSCs evaluate before their in-window CCSLC boundary
has been generated and finalize on a stale CCSLC -> rotation flicker.  Draining
each date first removes that out-of-order condition entirely.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta

import requests
import urllib3

from opera_commons.es_connection import get_grq_es
from data_subscriber.cslc.cslc_catalog_ingest import CslcCatalogIngest
from data_subscriber.cmr import get_cmr_token
from util.conf_util import SettingsConf

urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("forward_serial")

KSC_INDEX = "grq_1_disp_s1-kcycle-state-config*"
L3_INDEX = "grq_v1.0_l3_disp_s1*"
CCSLC_INDEX = "grq_1_l2_cslc_s1_compressed*"


def isofmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


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


def get_ksc(es, frame_id, sensing_int):
    body = {"query": {"bool": {"must": [
        {"term": {"metadata.frame_id": frame_id}},
        {"term": {"metadata.sensing_date": sensing_int}}]}},
        "size": 1,
        "_source": ["metadata.is_complete", "metadata.compressed_cslc_final",
                    "metadata.save_compressed_cslc", "metadata.gap_unresolved",
                    "metadata.superseded_by"]}
    try:
        hits = es.es.search(index=KSC_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        if "index_not_found" in str(e) or "404" in str(e):
            return None
        raise
    return hits[0]["_source"].get("metadata", {}) if hits else None


def ksc_fires(meta):
    """Whether this KSC will actually trigger a SCIFLO_L3_DISP_S1.

    Mirrors the real trigger-SCIFLO_L3_DISP_S1 user-rule condition exactly:
        is_complete AND compressed_cslc_final
        AND NOT gap_unresolved AND NOT (superseded_by exists)
    A KSC can be is_complete=True yet fire NOTHING — e.g. an early forward window
    that is superseded_by=existing_ccslc (already covered by bootstrap CCSLCs).
    Keying on is_complete alone makes such a date hang Phase 3 until l3-timeout.
    """
    return bool(meta
                and meta.get("is_complete")
                and meta.get("compressed_cslc_final")
                and not meta.get("gap_unresolved")
                and not meta.get("superseded_by"))


def wait_for_date(es, frame_id, sensing_dt, base_l3, base_ccslc,
                  ksc_timeout_s, l3_timeout_s, poll_s, settle_checks=3):
    """Forward model: ingest date D, wait for KSC(D) to be evaluated, then —

      - if KSC(D) satisfies the SCIFLO trigger (ksc_fires) it fires a
        SCIFLO_L3_DISP_S1 -> wait for the L3 [+ the CCSLC at a k-boundary],
        then advance;
      - otherwise it fires NOTHING -> move on immediately, no product.  This
        covers BOTH a seeded/early date whose k-window never fills (is_complete
        stays False) AND a complete window that is superseded_by=existing_ccslc
        (already covered by bootstrap CCSLCs) or gap-blocked.

    This is what makes the per-date drain faithful AND fast: window-setup and
    superseded dates don't block on a product that will never come.
    """
    sensing_int = int(sensing_dt.strftime("%Y%m%d"))

    # Phase 1: wait for KSC(D) to exist (the cascade CSLC->CSC->kce->KSC).
    deadline = time.monotonic() + ksc_timeout_s
    meta = None
    while time.monotonic() < deadline:
        meta = get_ksc(es, frame_id, sensing_int)
        if meta:
            break
        time.sleep(poll_s)
    if not meta:
        return {"ok": False, "reason": "KSC never created", "boundary": None}
    is_boundary = bool(meta.get("save_compressed_cslc"))

    # Phase 2: will this KSC actually fire a SCIFLO?  Use the real trigger
    # condition (ksc_fires), NOT bare is_complete.  Re-check across the settle
    # window in case the KSC is still mid-evaluation; a KSC that stays non-firing
    # (never-completes OR complete-but-superseded/gap-blocked) is a no-fire.
    if not ksc_fires(meta):
        for _ in range(settle_checks):
            time.sleep(poll_s)
            m = get_ksc(es, frame_id, sensing_int)
            if m:
                meta = m
                if ksc_fires(m):
                    break
        if not ksc_fires(meta):
            return {"ok": True, "fired": False, "boundary": is_boundary,
                    "superseded": bool(meta.get("superseded_by")),
                    "complete": bool(meta.get("is_complete")),
                    "l3": es_count(es, L3_INDEX, {})}

    # Phase 3: KSC satisfies the trigger -> a SCIFLO fires -> wait for L3 [+ CCSLC].
    deadline = time.monotonic() + l3_timeout_s
    while time.monotonic() < deadline:
        l3_now = es_count(es, L3_INDEX, {})
        ccslc_now = es_count(es, CCSLC_INDEX,
                             {"query": {"term": {"metadata.frame_id": frame_id}}})
        if l3_now > base_l3 and (ccslc_now > base_ccslc if is_boundary else True):
            return {"ok": True, "fired": True, "boundary": is_boundary,
                    "l3": l3_now, "ccslc": ccslc_now}
        time.sleep(poll_s)
    return {"ok": False, "fired": True, "timeout": True, "boundary": is_boundary,
            "l3": es_count(es, L3_INDEX, {}),
            "ccslc": es_count(es, CCSLC_INDEX,
                              {"query": {"term": {"metadata.frame_id": frame_id}}})}


def main():
    ap = argparse.ArgumentParser(description="Serialized forward DISP-S1 smoke driver")
    ap.add_argument("--frame-id", type=int, default=31241)
    ap.add_argument("--start-date", required=True, help="forward start, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--end-date", required=True, help="forward end, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--mozart-ip", required=True)
    ap.add_argument("--job-release", required=True)
    ap.add_argument("--queue", default="opera-job_worker-cslc_data_download")
    ap.add_argument("--ksc-timeout-mins", type=int, default=30,
                    help="max wait for KSC(D) to be evaluated after ingest")
    ap.add_argument("--l3-timeout-mins", type=int, default=90,
                    help="max wait for a firing date's L3 [+CCSLC] to publish")
    ap.add_argument("--poll-secs", type=int, default=30)
    ap.add_argument("--continue-on-timeout", action="store_true",
                    help="advance to the next date even if a date times out (default: stop)")
    ap.add_argument("--start-index", type=int, default=1,
                    help="1-based index into the discovered dates to resume from "
                         "(skips already-processed earlier dates; their KSCs/CSLCs "
                         "stay in place and remain part of later windows)")
    args = ap.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%dT%H:%M:%SZ")
    end = datetime.strptime(args.end_date, "%Y-%m-%dT%H:%M:%SZ")

    # Enumerate the ACTUAL forward sensing dates via the bulk ingest's own
    # discovery in DRY-RUN mode (gap check + seeded start_date + CMR query +
    # ccslc-lineage filter, with a live es_conn) — faithful to exactly the CSLCs
    # the real ingest would produce.  NOT the consistent-burst DB's
    # sensing_datetimes (blackout-filtered, historical-only).  Cadence is 6 or
    # 12 days per frame; winter dates ARE included (forward does not blackout).
    settings = SettingsConf().cfg
    es = get_grq_es()
    cci = CslcCatalogIngest(settings, es_conn=es)
    discovered = cci.ingest([args.frame_id], args.start_date, args.end_date, dry_run=True)
    date_strs = discovered.get(args.frame_id, [])
    fwd_dts = [datetime.strptime(d, "%Y-%m-%d") for d in date_strs]
    logger.info(f"frame {args.frame_id}: {len(fwd_dts)} forward sensing dates "
                f"(dry-run discovery) in [{args.start_date}, {args.end_date}]")
    if not fwd_dts:
        logger.error("no forward sensing dates discovered in range")
        sys.exit(2)
    gaps = [(fwd_dts[i] - fwd_dts[i - 1]).days for i in range(1, len(fwd_dts))]
    logger.info(f"sensing-date gaps (days): {sorted(set(gaps))} "
                f"(6/12 expected; larger = real acquisition gap)")

    ksc_timeout_s = args.ksc_timeout_mins * 60
    l3_timeout_s = args.l3_timeout_mins * 60
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
        base_l3 = es_count(es, L3_INDEX, {})
        base_ccslc = es_count(es, CCSLC_INDEX,
                              {"query": {"term": {"metadata.frame_id": args.frame_id}}})
        logger.info(f"[{i}/{len(fwd_dts)}] ingest {d} "
                    f"(L3 base={base_l3}, CCSLC base={base_ccslc})")
        job_id = submit_catalog_ingest(args.mozart_ip, args.job_release, args.queue,
                                       args.frame_id, s_date, e_date)
        logger.info(f"    submitted catalog_ingest {job_id}; waiting...")
        r = wait_for_date(es, args.frame_id, dt, base_l3, base_ccslc,
                          ksc_timeout_s, l3_timeout_s, args.poll_secs)
        r["date"] = d
        results.append(r)
        if r["ok"] and r.get("fired"):
            logger.info(f"    FIRED {d}: L3 produced "
                        f"(boundary={r['boundary']}, L3={r['l3']}, CCSLC={r['ccslc']})")
        elif r["ok"] and not r.get("fired"):
            why = ("superseded by existing CCSLC (early window already covered)"
                   if r.get("superseded")
                   else "is_complete=False (seeded/window-building)")
            logger.info(f"    no-fire {d}: {why} — no product, advancing")
        else:
            logger.error(f"    FAIL {d}: {r.get('reason') or 'L3 timeout'} "
                         f"(boundary={r.get('boundary')}, L3={r.get('l3')}, CCSLC={r.get('ccslc')})")
            if not args.continue_on_timeout:
                logger.error("stopping (use --continue-on-timeout to keep going)")
                break

    fired = sum(1 for r in results if r.get("ok") and r.get("fired"))
    nofire = sum(1 for r in results if r.get("ok") and not r.get("fired"))
    superseded = sum(1 for r in results
                     if r.get("ok") and not r.get("fired") and r.get("superseded"))
    failed = sum(1 for r in results if not r.get("ok"))
    logger.info(f"SERIAL FORWARD DONE: {fired} fired (L3 produced), "
                f"{nofire} no-fire ({superseded} superseded by existing CCSLC), "
                f"{failed} failed; {len(results)} dates processed "
                f"(of {len(fwd_dts)} discovered)")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
