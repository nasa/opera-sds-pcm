#!/usr/bin/env python3
"""Re-evaluate DISP-S1 cycle state configs that are not complete.

A cycle state config (CSC) records how many of a frame's bursts have been found for one
sensing date. The cycle evaluator recomputes it whenever a CSLC of that frame and date
is published. So when the way coverage is counted changes -- for example after a release
that lets the evaluator see CSLC datasets published in a shape it previously missed --
existing CSCs keep their old coverage until another CSLC for that date arrives, which
for past dates never happens.

This tool submits one disp_s1_cycle_evaluator job per incomplete CSC, triggered on the
CSC itself (the evaluator's re-evaluation input), with dedup disabled. The evaluator
recounts coverage from the CSLC documents in GRQ and republishes the CSC; it publishes
nothing else. A CSC that becomes complete then triggers the k-cycle evaluator through
the normal trigger rule, exactly as if its last burst had just arrived, so expect one
k-cycle evaluator job for each CSC that completes. Start with --frame-id or --limit on
a venue with many CSCs.

Run it on mozart from the PCM checkout:

    cd ~/mozart/ops/opera-pcm
    PYTHONPATH=$PWD python tools/reevaluate_disp_s1_cscs.py                        # dry run
    PYTHONPATH=$PWD python tools/reevaluate_disp_s1_cscs.py --frame-id 15422 --apply
    PYTHONPATH=$PWD python tools/reevaluate_disp_s1_cscs.py --since 20260825 --limit 100 --apply
"""

import argparse
import json
import os
import subprocess
import sys
import time

ES = "https://localhost:9200"
NETRC = os.path.expanduser("~/.netrc-os")
CSC_INDEX = "grq_*_cslc_s1-cycle-state-config*"
CSC_DATASET_TYPE = "cslc_s1-cycle-state-config"
DEFAULT_QUEUE = "opera-job_worker-evaluator_verdi"
TAG = "disp_s1_csc_reevaluate"


def es(method, path, body=None):
    cmd = ["curl", "-s", "-k", "--netrc-file", NETRC, "-X" + method, ES + path]
    if body is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(body)]
    out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, timeout=300).stdout
    try:
        response = json.loads(out)
    except ValueError:
        raise RuntimeError(f"{method} {path}: not a JSON response: {out[:300]}")
    if isinstance(response, dict) and response.get("error"):
        raise RuntimeError(f"{method} {path}: {json.dumps(response['error'])[:500]}")
    return response


def build_query(frame_ids=None, since=None, region=None):
    """Incomplete CSCs, optionally for some frames, from a sensing date, or in a region."""
    must = [{"term": {"metadata.is_complete": False}}]
    if frame_ids:
        must.append({"terms": {"metadata.frame_id": [int(f) for f in frame_ids]}})
    if region is not None:
        must.append({"term": {"metadata.region_id": str(region)}})
    query = {"bool": {"must": must}}
    if since:
        # metadata.sensing_date is mapped as a date with format yyyyMMdd.
        query["bool"]["filter"] = [{"range": {"metadata.sensing_date": {"gte": since}}}]
    return query


def scan(query, page_size=1000):
    response = es("POST", f"/{CSC_INDEX}/_search?scroll=5m",
                  {"size": page_size, "query": query,
                   "_source": ["dataset", "urls", "metadata"]})
    scroll_id = response.get("_scroll_id")
    try:
        while True:
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                return
            for hit in hits:
                yield hit
            response = es("POST", "/_search/scroll", {"scroll": "5m", "scroll_id": scroll_id})
            scroll_id = response.get("_scroll_id", scroll_id)
    finally:
        if scroll_id:
            try:
                es("DELETE", "/_search/scroll", {"scroll_id": scroll_id})
            except RuntimeError:
                pass


def build_reeval_params(hit):
    """Job params matching what the trigger rule's hysds-io would produce for this CSC."""
    source = hit["_source"]
    return {
        "product_paths": next((u for u in (source.get("urls") or []) if u.startswith("s3://")), ""),
        "product_metadata": {"metadata": source.get("metadata", {})},
        "dataset_type": source.get("dataset") or CSC_DATASET_TYPE,
        "input_dataset_id": hit["_id"],
    }


def build_submit_form(hit, job_release, queue, stamp):
    return {
        "queue": queue,
        "priority": 5,
        "tags": json.dumps([TAG]),
        "type": f"job-disp_s1_cycle_evaluator:{job_release}",
        "params": json.dumps(build_reeval_params(hit)),
        "name": f"reevaluate-csc-{hit['_id']}-{stamp}",
    }


def describe(hit):
    md = hit["_source"].get("metadata", {})
    return (f"{hit['_id']}  found={md.get('coverage_actual')}/{md.get('coverage_expected')}"
            f"  region={md.get('region_id')}  blackout={md.get('blackout')}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int, nargs="+", help="restrict to these frames")
    ap.add_argument("--since", help="only sensing dates on or after YYYYMMDD")
    ap.add_argument("--region", help="only CSCs stamped with this region_id")
    ap.add_argument("--limit", type=int, help="submit at most this many jobs")
    ap.add_argument("--job-release", help="defaults to STAGING_AREA.JOB_RELEASE from ~/.sds/config")
    ap.add_argument("--queue", default=DEFAULT_QUEUE)
    ap.add_argument("--sleep-secs", type=float, default=0.2, help="pause between submissions")
    ap.add_argument("--apply", action="store_true", help="submit; otherwise dry run")
    args = ap.parse_args()

    if args.since and (len(args.since) != 8 or not args.since.isdigit()):
        ap.error("--since takes YYYYMMDD")

    hits = list(scan(build_query(args.frame_id, args.since, args.region)))
    hits.sort(key=lambda h: h["_id"])
    if args.limit is not None:
        hits = hits[:args.limit]
    print(f"{len(hits)} incomplete cycle state config(s) selected")
    for hit in hits[:20]:
        print("   " + describe(hit))
    if len(hits) > 20:
        print(f"   ... and {len(hits) - 20} more")

    if not hits:
        return 0
    if not args.apply:
        print("\nDRY RUN -- nothing submitted. Re-run with --apply.")
        return 0

    from util.conf_util import SettingsConf
    import requests
    import urllib3
    urllib3.disable_warnings()

    cfg = SettingsConf(file=os.path.expanduser("~/.sds/config")).cfg
    release = args.job_release or cfg["STAGING_AREA"]["JOB_RELEASE"]
    url = f"https://{cfg['MOZART_PVT_IP']}/mozart/api/v0.1/job/submit?enable_dedup=false"
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    submitted = 0
    for hit in hits:
        response = requests.post(url, data=build_submit_form(hit, release, args.queue, stamp),
                                 verify=False, timeout=120)
        ok = response.status_code == 200 and response.json().get("success")
        submitted += 1 if ok else 0
        if not ok:
            print(f"   FAILED {hit['_id']}: {response.text[:200]}")
        time.sleep(args.sleep_secs)

    print(f"\nsubmitted {submitted}/{len(hits)} disp_s1_cycle_evaluator job(s) tagged {TAG}")
    return 0 if submitted == len(hits) else 1


if __name__ == "__main__":
    sys.exit(main())
