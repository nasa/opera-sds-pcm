#!/usr/bin/env python
"""Assert that CSLCs produced by the local CSLC-S1 PGE drive the DISP-S1 forward cascade.

Every other forward date in the DISP-S1 smoke test enters the cascade as a metadata-only
cslc_catalog_ingest dataset. Operational forward processing instead ingests SLCs and
runs the CSLC-S1 PGE, whose datasets product2dataset publishes. This script ingests one
SLC through the production SLC query job, in reprocessing mode because that is the mode
the CSLC-S1 trigger rule matches, then follows the cascade for one frame and date:

  1. the CSLC-S1 PGE publishes an L2_CSLC_S1 dataset for every burst of the frame, each
     carrying metadata.burst_id and a dataset starttime at the top level;
  2. the frame's cycle state config for the date is complete and records exactly those
     datasets' .h5 products;
  3. the frame's k-cycle state config for the date is complete and lists them;
  4. an L3_DISP_S1 product for the date is published, built from them.

Writes SUCCESS/ERROR lines in the same format check_datasets_file.py uses, so
check_pcm.py can assert on the result file. Each step has its own time budget; a step
that runs out is an ERROR, reported with the jobs that failed since the SLC was
submitted, and the steps after it are not attempted.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone

from opera_commons.es_connection import get_grq_es, get_mozart_es

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("pge_cslc_feed")

CSLC_INDEX = "grq_*_l2_cslc_s1-*"
CSC_INDEX = "grq_*_cslc_s1-cycle-state-config*"
KSC_INDEX = "grq_*_disp_s1-kcycle*"
L3_INDEX = "grq_*_l3_disp_s1*"
JOB_STATUS_INDEX = "job_status-current"

SLC_QUERY_JOB_TYPES = {
    "SENTINEL-1A_SLC": "slcs1a_query",
    "SENTINEL-1B_SLC": "slcs1b_query",
    "SENTINEL-1C_SLC": "slcs1c_query",
    "SENTINEL-1D_SLC": "slcs1d_query",
}
SLC_QUERY_QUEUE = "opera-job_worker-slc_data_query"
SLC_DOWNLOAD_QUEUE = "opera-job_worker-slc_data_download"
TAG = "disp_s1_pge_cslc_feed"

# ..._<burst>_<sensing>T..Z_<processed>T..Z_...
CSLC_ID_RE = re.compile(r"OPERA_L2_CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8})T\d{6}Z_(\d{8}T\d{6}Z)_")


def build_slc_query_params(slc_start, slc_end, job_release):
    """Params for the SLC query job, formatted as the query-timer and batch lambdas do."""
    return {
        "endpoint": "--endpoint=OPS",
        "start_datetime": f"--start-date={slc_start}",
        "end_datetime": f"--end-date={slc_end}",
        "bounding_box": "",
        "no_schedule_download": "",
        "download_job_release": f"--release-version={job_release}",
        "download_job_queue": f"--job-queue={SLC_DOWNLOAD_QUEUE}",
        "chunk_size": "--chunk-size=1",
        "smoke_run": "",
        "dry_run": "",
        "use_temporal": "--use-temporal",
        "max_revision": "--max-revision=1000",
        "temporal_start_datetime": "",
        "processing_mode": "--processing-mode=reprocessing",
        "include_regions": "",
        "exclude_regions": "",
        "transfer_protocol": "--transfer-protocol=auto",
        "provider": "--provider=ASF",
    }


def submit_slc_query(mozart_ip, job_release, collection, slc_start, slc_end):
    import requests
    import urllib3
    urllib3.disable_warnings()

    form = {
        "queue": SLC_QUERY_QUEUE,
        "priority": "0",
        "tags": json.dumps([TAG]),
        "type": f"job-{SLC_QUERY_JOB_TYPES[collection]}:{job_release}",
        "params": json.dumps(build_slc_query_params(slc_start, slc_end, job_release)),
        "name": f"{TAG}-{collection}-{slc_start}",
    }
    url = f"https://{mozart_ip}/mozart/api/v0.1/job/submit?enable_dedup=false"
    response = requests.post(url, data=form, verify=False, timeout=60)
    response.raise_for_status()
    result = response.json()
    if not result.get("success"):
        raise RuntimeError(f"SLC query submission failed: {result}")
    return result["result"]


def h5_path(meta):
    return next((p for p in (meta.get("product_s3_paths") or []) if p.endswith(".h5")), "")


def select_pge_cslcs(hits, burst_ids, sensing_date):
    """The newest PGE-produced VV CSLC per burst of the frame on the sensing date."""
    newest = {}
    for hit in hits:
        source = hit.get("_source", {})
        meta = source.get("metadata", {})
        if "PGE" not in (meta.get("tags") or []):
            continue
        match = CSLC_ID_RE.search(hit.get("_id", ""))
        if not match or "_VV_" not in hit["_id"]:
            continue
        burst_id, date, processed = match.groups()
        if burst_id not in burst_ids or date != sensing_date:
            continue
        if burst_id not in newest or processed > newest[burst_id][0]:
            newest[burst_id] = (processed, hit)
    return {burst_id: hit for burst_id, (_, hit) in newest.items()}


def check_cslcs(selected, burst_ids):
    """Every burst has a PGE CSLC, and each carries the promoted top-level fields."""
    errors = []
    missing = sorted(set(burst_ids) - set(selected))
    if missing:
        errors.append(f"no PGE-produced CSLC for bursts {missing}")
    for burst_id, hit in sorted(selected.items()):
        source = hit["_source"]
        meta = source.get("metadata", {})
        if meta.get("burst_id") != burst_id:
            errors.append(f"{hit['_id']} has metadata.burst_id={meta.get('burst_id')!r}")
        if not source.get("starttime"):
            errors.append(f"{hit['_id']} has no dataset starttime")
        if not h5_path(meta):
            errors.append(f"{hit['_id']} lists no .h5 in metadata.product_s3_paths")
    return errors


def check_csc(meta, expected_paths):
    errors = []
    if not meta.get("is_complete"):
        errors.append(f"cycle state config incomplete: {meta.get('completeness_reason')}")
    paths = set(meta.get("cslc_product_paths") or [])
    if paths != set(expected_paths):
        errors.append(f"cycle state config records {sorted(paths)}, "
                      f"expected the PGE products {sorted(expected_paths)}")
    return errors


def check_ksc(meta, expected_paths):
    errors = []
    if not meta.get("is_complete"):
        errors.append(f"k-cycle state config incomplete: {meta.get('completeness_reason')}")
    if meta.get("compressed_cslc_final") is False:
        errors.append(f"k-cycle state config waiting on compressed CSLCs "
                      f"{meta.get('compressed_cslc_pending')}")
    listed = set((meta.get("product_paths") or {}).get("L2_CSLC_S1") or [])
    absent = sorted(set(expected_paths) - listed)
    if absent:
        errors.append(f"k-cycle state config does not list the PGE products {absent}")
    return errors


def find_l3(hits, frame_id, sensing_date):
    """The L3_DISP_S1 products whose secondary date is the sensing date."""
    pattern = re.compile(rf"_F{int(frame_id):05d}_VV_\d{{8}}T\d{{6}}Z_{sensing_date}T")
    return [hit for hit in hits if pattern.search(hit.get("_id", ""))]


def lineage_uses(hit, cslc_ids):
    lineage = (hit.get("_source", {}).get("metadata", {}) or {}).get("lineage") or []
    return [cslc_id for cslc_id in cslc_ids if any(cslc_id in str(entry) for entry in lineage)]


class Report:
    def __init__(self, path):
        self.path = path
        self.lines = []

    def success(self, message):
        logger.info(message)
        self.lines.append(f"SUCCESS: {message}")

    def error(self, message):
        logger.error(message)
        self.lines.append(f"ERROR: {message}")

    def write(self):
        with open(self.path, "w") as f:
            f.write("\n".join(self.lines) + "\n")


def failed_jobs_since(start_iso):
    """Best-effort summary of jobs that failed after the SLC was submitted."""
    try:
        body = {
            "query": {"bool": {"must": [
                {"term": {"status": "job-failed"}},
                {"range": {"@timestamp": {"gte": start_iso}}},
            ]}},
            "_source": ["job.type", "job.name", "short_error", "error"],
            "size": 50,
        }
        hits = get_mozart_es(logger).query(index=JOB_STATUS_INDEX, body=body) or []
        return [f"{h['_source'].get('job', {}).get('type')} {h['_source'].get('job', {}).get('name')}: "
                f"{str(h['_source'].get('short_error') or h['_source'].get('error'))[:200]}"
                for h in hits]
    except Exception as e:
        return [f"(could not list failed jobs: {e})"]


def poll(what, fn, timeout_mins, poll_secs):
    """Call fn until it returns (True, value) or the budget runs out; returns the last value."""
    deadline = time.time() + timeout_mins * 60
    value = None
    while True:
        done, value = fn()
        if done:
            return True, value
        if time.time() >= deadline:
            logger.warning(f"{what}: gave up after {timeout_mins} min")
            return False, value
        logger.info(f"{what}: waiting")
        time.sleep(poll_secs)


def run(args, eu, report):
    burst_ids = sorted(args.burst_ids)
    date = args.sensing_date
    day = f"{date[:4]}-{date[4:6]}-{date[6:]}"

    def l3_hits():
        body = {"query": {"term": {"metadata.frame_id": args.frame_id}},
                "_source": ["id", "metadata.lineage"], "size": 1000}
        return find_l3(eu.query(index=L3_INDEX, body=body) or [], args.frame_id, date)

    if l3_hits():
        report.error(f"an L3_DISP_S1 for frame {args.frame_id} on {date} existed before the SLC "
                     f"was ingested, so this stage cannot attribute it to the PGE CSLCs")
        return

    start_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not args.no_submit:
        job_id = submit_slc_query(args.mozart_ip, args.job_release, args.slc_collection,
                                  args.slc_start, args.slc_end)
        logger.info(f"submitted SLC query job {job_id}")

    cslc_body = {
        "query": {"bool": {
            "must": [
                {"term": {"dataset_type.keyword": "L2_CSLC_S1"}},
                {"terms": {"metadata.Files.burst_id.keyword": burst_ids}},
            ],
            "filter": [{"range": {"metadata.Files.acquisition_ts": {
                "gte": f"{day}T00:00:00", "lt": f"{day}T23:59:59"}}}],
        }},
        "size": 200,
    }

    def cslcs_ready():
        selected = select_pge_cslcs(eu.query(index=CSLC_INDEX, body=cslc_body) or [], burst_ids, date)
        return set(selected) == set(burst_ids), selected

    ok, selected = poll("PGE CSLCs", cslcs_ready, args.cslc_timeout_mins, args.poll_secs)
    errors = check_cslcs(selected or {}, burst_ids)
    if not ok or errors:
        for e in errors or ["PGE CSLCs did not appear"]:
            report.error(e)
        for line in failed_jobs_since(start_iso):
            report.error(f"failed job: {line}")
        return
    expected_paths = sorted(h5_path(hit["_source"]["metadata"]) for hit in selected.values())
    cslc_ids = sorted(hit["_id"] for hit in selected.values())
    report.success(f"CSLC-S1 PGE published {len(selected)} CSLCs for frame {args.frame_id} on "
                   f"{date}, each with metadata.burst_id and starttime: {cslc_ids}")

    csc_id = f"cslc_s1-cycle-f{args.frame_id}-{date}-state-config"

    def csc_ready():
        hits = eu.query(index=CSC_INDEX, body={"query": {"ids": {"values": [csc_id]}}}) or []
        meta = hits[0]["_source"]["metadata"] if hits else {}
        return bool(meta) and not check_csc(meta, expected_paths), meta

    ok, meta = poll("cycle state config", csc_ready, args.csc_timeout_mins, args.poll_secs)
    if not ok:
        for e in (check_csc(meta, expected_paths) if meta else [f"{csc_id} was never published"]):
            report.error(e)
        return
    report.success(f"{csc_id} complete ({meta.get('coverage_actual')}/{meta.get('coverage_expected')}) "
                   f"from the PGE products")

    def ksc_ready():
        body = {"query": {"term": {"metadata.frame_id": args.frame_id}}, "size": 1000}
        kscs = [h for h in (eu.query(index=KSC_INDEX, body=body) or [])
                if str(h["_source"]["metadata"].get("sensing_date")) == date]
        good = [h for h in kscs if not check_ksc(h["_source"]["metadata"], expected_paths)]
        if good:
            return True, good[0]
        return False, kscs[0] if kscs else None

    ok, ksc = poll("k-cycle state config", ksc_ready, args.ksc_timeout_mins, args.poll_secs)
    if not ok:
        if ksc is None:
            report.error(f"no k-cycle state config for frame {args.frame_id} on {date}")
        else:
            for e in check_ksc(ksc["_source"]["metadata"], expected_paths):
                report.error(f"{ksc['_id']}: {e}")
        return
    report.success(f"{ksc['_id']} complete and lists the PGE products")

    ok, l3 = poll("L3_DISP_S1", lambda: (bool(l3_hits()), l3_hits()),
                  args.l3_timeout_mins, args.poll_secs)
    if not ok:
        report.error(f"no L3_DISP_S1 for frame {args.frame_id} on {date}")
        for line in failed_jobs_since(start_iso):
            report.error(f"failed job: {line}")
        return
    used = lineage_uses(l3[0], cslc_ids)
    report.success(f"{l3[0]['_id']} published")
    if len(used) == len(cslc_ids):
        report.success("its lineage includes every PGE-produced CSLC of the date")
    else:
        report.error(f"its lineage includes {len(used)}/{len(cslc_ids)} of the PGE-produced CSLCs")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mozart-ip", required=True)
    ap.add_argument("--job-release", required=True)
    ap.add_argument("--frame-id", type=int, required=True)
    ap.add_argument("--burst-ids", nargs="+", required=True, help="the frame's bursts, e.g. T117-249921-IW3")
    ap.add_argument("--sensing-date", required=True, help="YYYYMMDD")
    ap.add_argument("--slc-collection", choices=sorted(SLC_QUERY_JOB_TYPES), required=True)
    ap.add_argument("--slc-start", required=True, help="temporal window start, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--slc-end", required=True, help="temporal window end, YYYY-MM-DDTHH:MM:SSZ")
    ap.add_argument("--no-submit", action="store_true", help="only check; the SLC was already ingested")
    ap.add_argument("--poll-secs", type=int, default=60)
    ap.add_argument("--cslc-timeout-mins", type=int, default=240)
    ap.add_argument("--csc-timeout-mins", type=int, default=60)
    ap.add_argument("--ksc-timeout-mins", type=int, default=90)
    ap.add_argument("--l3-timeout-mins", type=int, default=240)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    report = Report(args.out)
    try:
        run(args, get_grq_es(logger), report)
    except Exception as e:
        report.error(f"stage aborted: {e!r}")
    report.write()
    print(open(args.out).read())
    return 1 if any(line.startswith("ERROR") for line in report.lines) else 0


if __name__ == "__main__":
    sys.exit(main())
