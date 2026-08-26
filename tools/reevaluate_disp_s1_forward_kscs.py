#!/usr/bin/env python3
"""Re-evaluate the k-cycle state configs of forward dates the walk has already passed.

WHY THIS EXISTS

A forward date leaves the phased walk as soon as its k-cycle state config reaches a
TERMINAL disposition, and a no-fire is terminal. So the walk can run a whole forward
block to the end, advance the cursor, self-disable at 100%, and owe every one of those
products -- with `stalled_frames` empty and no failed job anywhere.

Once that has happened the walk cannot recover on its own. Re-enabling it and rewinding
the cursor does not help: it re-submits `cslc_catalog_ingest`, that job is a no-op
because the dataset already exists, so nothing re-triggers the evaluator cascade and the
walk reads the same stale state config and advances past the date again.

This tool re-drives those dates directly, one `disp_s1_k_cycle_evaluator` job per date,
triggered on that date's own cycle state config with dedup disabled. It only causes the
state config to be recomputed; it publishes nothing itself.

TYPICAL USE

Something upstream was fixed -- a partial acquisition was flagged, a missing burst
arrived, a blackout window was corrected -- and the frame needs to reconsider dates it
has already written off:

    python tools/reevaluate_disp_s1_forward_kscs.py --frame-id 24726            # dry run
    python tools/reevaluate_disp_s1_forward_kscs.py --frame-id 24726 --apply

Confirm the outcome with:

    python conf/sds/files/test/check_disp_s1_phases.py --frame-id 24726 --k 15

whose forward-product assertion is the check this tool exists to satisfy.

Delete the stale KSCs first if they carry a terminal reason you want cleared -- this
tool recomputes them, but an existing KSC at compressed_cslc_final=True is rotation
locked and will not be revisited.
"""

import argparse
import json
import subprocess
import sys
import time

from data_subscriber import cslc_utils
from util.conf_util import SettingsConf

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200"
CSC_INDEX = "grq_*_cslc_s1-cycle-state-config*"


def es_post(path, body):
    out = subprocess.run(
        ["curl", "-s", "-k", "--netrc-file", NETRC, "-XPOST", ES + path,
         "-H", "Content-Type: application/json", "-d", json.dumps(body)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=300)
    try:
        return json.loads(out.stdout)
    except ValueError:
        return {}


def forward_dates(frame):
    ymd = [d.strftime("%Y%m%d") for d in frame.sensing_datetimes]
    out = []
    for phase in frame.phases:
        if phase.label.startswith("forward_"):
            out += [ymd[i] for i in range(phase.start_pos, phase.end_pos)]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int, required=True)
    ap.add_argument("--k", type=int, default=15)
    ap.add_argument("--m", type=int, default=6)
    ap.add_argument("--dates", help="comma-separated subset; default is every forward date")
    ap.add_argument("--job-release", help="defaults to STAGING_AREA.JOB_RELEASE from ~/.sds/config")
    ap.add_argument("--queue", default="opera-job_worker-evaluator")
    ap.add_argument("--apply", action="store_true", help="submit; otherwise dry run")
    args = ap.parse_args()

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    frame = frame_to_bursts.get(args.frame_id)
    if frame is None:
        print("frame %d is not in the deployed burst database" % args.frame_id)
        return 1
    if not getattr(frame, "phases", None):
        print("frame %d has no phases -- is DISP_S1_PROCESSING_MODE_ENABLED on and the "
              "database annotated?" % args.frame_id)
        return 1

    dates = args.dates.split(",") if args.dates else forward_dates(frame)
    if not dates:
        print("frame %d has no forward dates" % args.frame_id)
        return 0
    print("frame %d: %d forward date(s) to re-evaluate" % (args.frame_id, len(dates)))

    r = es_post("/%s/_search" % CSC_INDEX, {
        "query": {"bool": {"must": [{"term": {"metadata.frame_id": args.frame_id}},
                                    {"terms": {"metadata.sensing_date": dates}}]}},
        "size": 500})
    by_date = {h["_source"]["metadata"]["sensing_date"]: h
               for h in r.get("hits", {}).get("hits", [])}
    missing = [d for d in dates if d not in by_date]
    if missing:
        print("no cycle state config for %d date(s): %s" % (len(missing), missing))
        print("those cannot be re-driven from here -- the cascade never reached them.")

    for date in dates:
        hit = by_date.get(date)
        if not hit:
            continue
        md = hit["_source"].get("metadata", {})
        print("   %s  csc is_complete=%-5s found=%d/%d  blackout=%s"
              % (date, md.get("is_complete"),
                 len(md.get("found_burst_ids") or []),
                 len(md.get("expected_burst_ids") or []), md.get("blackout")))

    if not args.apply:
        print("\nDRY RUN -- nothing submitted. Re-run with --apply.")
        return 0

    cfg = SettingsConf(file="/export/home/hysdsops/.sds/config").cfg
    release = args.job_release or cfg["STAGING_AREA"]["JOB_RELEASE"]
    url = "https://%s/mozart/api/v0.1/job/submit?enable_dedup=false" % cfg["MOZART_PVT_IP"]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    import requests
    submitted = 0
    for date in dates:
        hit = by_date.get(date)
        if not hit:
            continue
        src = hit["_source"]
        params = {
            "product_paths": next((u for u in (src.get("urls") or [])
                                   if u.startswith("s3://")), ""),
            "product_metadata": {"metadata": src.get("metadata", {})},
            "dataset_type": src.get("dataset"),
            "input_dataset_id": hit["_id"],
            "k": args.k,
            "m": args.m,
        }
        form = {
            "queue": args.queue,
            "priority": 5,
            "tags": json.dumps(["disp_s1_forward_ksc_reevaluate"]),
            "type": "job-disp_s1_k_cycle_evaluator:%s" % release,
            "params": json.dumps(params),
            "name": "reevaluate-kce-f%d-%s-%s" % (args.frame_id, date, stamp),
        }
        resp = requests.post(url, data=form, verify=False, timeout=120)
        ok = resp.status_code == 200 and resp.json().get("success")
        submitted += 1 if ok else 0
        print("   %s %s" % (date, "submitted" if ok else "FAILED %s" % resp.text[:140]))

    print("\nsubmitted %d/%d" % (submitted, len(by_date)))
    print("Confirm with: python conf/sds/files/test/check_disp_s1_phases.py "
          "--frame-id %d --k %d" % (args.frame_id, args.k))
    return 0


if __name__ == "__main__":
    sys.exit(main())
