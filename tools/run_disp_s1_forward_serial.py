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
                    "metadata.save_compressed_cslc"]}
    try:
        hits = es.es.search(index=KSC_INDEX, body=body)["hits"]["hits"]
    except Exception as e:
        if "index_not_found" in str(e) or "404" in str(e):
            return None
        raise
    return hits[0]["_source"].get("metadata", {}) if hits else None


def wait_for_date(es, frame_id, sensing_dt, base_l3, base_ccslc, timeout_s, poll_s):
    """Block until KSC(D) final + L3(D) published [+ CCSLC if boundary]. Returns dict."""
    sensing_int = int(sensing_dt.strftime("%Y%m%d"))
    deadline = time.monotonic() + timeout_s
    ksc_final = False
    is_boundary = None
    while time.monotonic() < deadline:
        meta = get_ksc(es, frame_id, sensing_int)
        if meta:
            if is_boundary is None:
                is_boundary = bool(meta.get("save_compressed_cslc"))
            if meta.get("is_complete") and meta.get("compressed_cslc_final"):
                ksc_final = True
        if ksc_final:
            l3_now = es_count(es, L3_INDEX, {"match_all": {}})
            ccslc_now = es_count(es, CCSLC_INDEX,
                                 {"query": {"term": {"metadata.frame_id": frame_id}}})
            l3_ok = l3_now > base_l3
            ccslc_ok = (ccslc_now > base_ccslc) if is_boundary else True
            if l3_ok and ccslc_ok:
                return {"ok": True, "boundary": is_boundary,
                        "l3": l3_now, "ccslc": ccslc_now}
        time.sleep(poll_s)
    # timed out — report what we saw
    return {"ok": False, "boundary": is_boundary, "ksc_final": ksc_final,
            "l3": es_count(es, L3_INDEX, {"match_all": {}}),
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
    ap.add_argument("--per-date-timeout-mins", type=int, default=120,
                    help="max wait for one date's products before moving on")
    ap.add_argument("--poll-secs", type=int, default=30)
    ap.add_argument("--continue-on-timeout", action="store_true",
                    help="advance to the next date even if a date times out (default: stop)")
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

    timeout_s = args.per_date_timeout_mins * 60
    results = []
    for i, dt in enumerate(fwd_dts, 1):
        s_date = dt.replace(hour=0, minute=0, second=0)
        e_date = dt.replace(hour=23, minute=59, second=59)
        base_l3 = es_count(es, L3_INDEX, {"match_all": {}})
        base_ccslc = es_count(es, CCSLC_INDEX,
                              {"query": {"term": {"metadata.frame_id": args.frame_id}}})
        logger.info(f"[{i}/{len(fwd_dts)}] ingest {dt.strftime('%Y%m%d')} "
                    f"(L3 base={base_l3}, CCSLC base={base_ccslc})")
        job_id = submit_catalog_ingest(args.mozart_ip, args.job_release, args.queue,
                                       args.frame_id, s_date, e_date)
        logger.info(f"    submitted catalog_ingest {job_id}; waiting for products...")
        r = wait_for_date(es, args.frame_id, dt, base_l3, base_ccslc, timeout_s, args.poll_secs)
        r["date"] = dt.strftime("%Y%m%d")
        results.append(r)
        if r["ok"]:
            logger.info(f"    OK {dt.strftime('%Y%m%d')} "
                        f"(boundary={r['boundary']}, L3={r['l3']}, CCSLC={r['ccslc']})")
        else:
            logger.error(f"    TIMEOUT {dt.strftime('%Y%m%d')} after "
                         f"{args.per_date_timeout_mins}m (ksc_final={r.get('ksc_final')}, "
                         f"boundary={r['boundary']}, L3={r['l3']}, CCSLC={r['ccslc']})")
            if not args.continue_on_timeout:
                logger.error("stopping (use --continue-on-timeout to keep going)")
                break

    ok = sum(1 for r in results if r["ok"])
    logger.info(f"SERIAL FORWARD DONE: {ok}/{len(results)} dates produced products "
                f"({len(fwd_dts)} total in range)")
    sys.exit(0 if ok == len(fwd_dts) else 1)


if __name__ == "__main__":
    main()
