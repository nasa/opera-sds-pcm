#!/usr/bin/env python3
"""Find duplicate DISP-S1 forward outputs and the SCIFLO runs that made them.

A k-cycle state config (KSC) that is finalized more than once fires
trigger-SCIFLO_L3_DISP_S1 more than once, and every run publishes its own copies,
which differ only in their creation timestamp. This tool reports, from GRQ and
Mozart:

    L3_DISP_S1               more than one product per (frame, pol, reference date,
                             secondary date, version)
    L2_CSLC_S1_COMPRESSED    more than one product per (frame, burst, reference date,
                             first date, last date, pol, version)
    disp_s1-ccslc-set        more than one CCSLC-set marker per (frame, last date),
                             i.e. a k-boundary SCIFLO that ran twice
    SCIFLO_L3_DISP_S1 jobs   more than one run per KSC that is queued, running, or
                             completed without bailing as a duplicate

Run it on mozart after a forward campaign or acceptance test. It exits 1 when it
finds anything, so it can gate a test.

    python tools/disp_s1_duplicate_audit.py
    python tools/disp_s1_duplicate_audit.py --frame-id 46288 46290 --since 2026-10-05T00:00:00Z
    python tools/disp_s1_duplicate_audit.py --json > duplicates.json
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict

logger = logging.getLogger(__name__)

L3_INDEX = "grq_*_l3_disp_s1-*"
CCSLC_INDEX = "grq_*_l2_cslc_s1_compressed-*"
CCSLC_SET_INDEX = "grq_*_disp_s1-ccslc-set-*"
JOB_STATUS_INDEX = "job_status-*"
SCIFLO_JOB_TYPE_PREFIX = "job-SCIFLO_L3_DISP_S1:"

# OPERA_L3_DISP-S1_IW_F36541_VV_20250703T140200Z_20260627T140210Z_v1.0_20261006T101400Z
L3_RE = re.compile(
    r"^OPERA_L3_DISP-S1_IW_F(?P<frame>\d{5})_(?P<pol>[A-Z+]+)_(?P<ref>\d{8})T\d{6}Z_"
    r"(?P<sec>\d{8})T\d{6}Z_(?P<version>v\d+\.\d+)_\d{8}T\d{6}Z$")
# OPERA_L2_COMPRESSED-CSLC-S1_F36541_T064-135520-IW1_20250703T000000Z_20250703T000000Z_
#   20260627T000000Z_20261006T101400Z_VV_v1.0
CCSLC_RE = re.compile(
    r"^OPERA_L2_COMPRESSED-CSLC-S1_F(?P<frame>\d{5})_(?P<burst>\w{4}-\w{6}-\w{3})_"
    r"(?P<ref>\d{8})T000000Z_(?P<first>\d{8})T000000Z_(?P<last>\d{8})T000000Z_\d{8}T\d{6}Z_"
    r"(?P<pol>[A-Z+]+)_(?P<version>v\d+\.\d+)$")
# disp_s1-ccslc-set-f36541-20250703-20250703-20260627-20261006T101500Z
CCSLC_SET_RE = re.compile(
    r"^disp_s1-ccslc-set-f(?P<frame>\d+)-(?P<ref>\d{8})-(?P<first>\d{8})-(?P<last>\d{8})-"
    r"\d{8}T\d{6}Z$")
# SCIFLO_L3_DISP_S1__<release>-disp_s1-kcycle-k15-m6-f36541-20260627-state-config-<ts>
KSC_IN_JOB_RE = re.compile(r"(disp_s1-kcycle-k\d+-m\d+-f\d+-\d{8}-state-config)")

# A SCIFLO that found its product already published completes with this message
# and publishes nothing.
DUP_SKIP_MSG_PREFIX = "dup skip"
COUNTED_JOB_STATUSES = ("job-queued", "job-started", "job-completed")


def _group(ids, pattern, key_fields):
    groups = defaultdict(list)
    for dataset_id in ids:
        m = pattern.match(dataset_id)
        if m:
            groups[tuple(m.group(f) for f in key_fields)].append(dataset_id)
    return {key: sorted(members) for key, members in groups.items() if len(members) > 1}


def duplicate_l3s(ids):
    """{(frame, pol, ref, sec, version): [ids]} for keys carrying more than one L3."""
    return _group(ids, L3_RE, ("frame", "pol", "ref", "sec", "version"))


def duplicate_ccslcs(ids):
    """{(frame, burst, ref, first, last, pol, version): [ids]} for keys carrying more than one CCSLC."""
    return _group(ids, CCSLC_RE, ("frame", "burst", "ref", "first", "last", "pol", "version"))


def duplicate_ccslc_sets(ids):
    """{(frame, last): [ids]} for k-boundaries with more than one CCSLC-set marker."""
    return _group(ids, CCSLC_SET_RE, ("frame", "last"))


def duplicate_sciflo_runs(jobs):
    """{ksc_id: [job ids]} for KSCs with more than one SCIFLO run that produced or may produce output.

    :param jobs: job_status sources with job_id, status and msg.
    """
    runs = defaultdict(list)
    for job in jobs:
        if job.get("status") not in COUNTED_JOB_STATUSES:
            continue
        if str(job.get("msg") or "").startswith(DUP_SKIP_MSG_PREFIX):
            continue
        m = KSC_IN_JOB_RE.search(job.get("job_id") or "")
        if m:
            runs[m.group(1)].append(job["job_id"])
    return {ksc: sorted(job_ids) for ksc, job_ids in runs.items() if len(job_ids) > 1}


def frame_matches(key_frame, frame_ids):
    return not frame_ids or int(key_frame) in frame_ids


def _scroll(es, index, query, source):
    """Yield every hit of a query, a page at a time."""
    resp = es.search(index=index, body={"query": query, "_source": source, "size": 5000, "sort": ["_doc"]},
                     scroll="5m", ignore_unavailable=True, allow_no_indices=True)
    scroll_id = resp.get("_scroll_id")
    try:
        while True:
            hits = resp.get("hits", {}).get("hits", [])
            if not hits:
                break
            yield from hits
            resp = es.scroll(scroll_id=scroll_id, scroll="5m")
            scroll_id = resp.get("_scroll_id", scroll_id)
    finally:
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception as e:
                logger.debug(f"clear_scroll failed: {e}")


def _dataset_query(dataset_type, frame_ids, since):
    must = [{"term": {"dataset_type.keyword": dataset_type}}]
    if frame_ids:
        must.append({"terms": {"metadata.frame_id": sorted(frame_ids)}})
    if since:
        must.append({"range": {"creation_timestamp": {"gte": since}}})
    return {"bool": {"must": must}}


def dataset_ids(grq, index, dataset_type, frame_ids, since):
    return [h["_id"] for h in _scroll(grq, index, _dataset_query(dataset_type, frame_ids, since), False)]


def sciflo_jobs(mozart, since):
    must = [{"prefix": {"type": SCIFLO_JOB_TYPE_PREFIX}},
            {"terms": {"status": list(COUNTED_JOB_STATUSES)}}]
    if since:
        must.append({"range": {"job.job_info.time_queued": {"gte": since}}})
    return [h["_source"] for h in _scroll(mozart, JOB_STATUS_INDEX, {"bool": {"must": must}},
                                          ["job_id", "status", "msg"])]


def audit(grq, mozart, frame_ids=None, since=None):
    frame_ids = set(frame_ids or [])
    report = {
        "L3_DISP_S1": duplicate_l3s(dataset_ids(grq, L3_INDEX, "L3_DISP_S1", frame_ids, since)),
        "L2_CSLC_S1_COMPRESSED": duplicate_ccslcs(
            dataset_ids(grq, CCSLC_INDEX, "L2_CSLC_S1_COMPRESSED", frame_ids, since)),
        "disp_s1-ccslc-set": duplicate_ccslc_sets(
            dataset_ids(grq, CCSLC_SET_INDEX, "disp_s1-ccslc-set", frame_ids, since)),
        "SCIFLO_L3_DISP_S1": {
            ksc: job_ids for ksc, job_ids in duplicate_sciflo_runs(sciflo_jobs(mozart, since)).items()
            if frame_matches(re.search(r"-f(\d+)-", ksc).group(1), frame_ids)
        },
    }
    return report


def print_report(report, limit):
    clean = True
    for what, groups in report.items():
        extras = sum(len(v) - 1 for v in groups.values())
        print(f"{what}: {len(groups)} duplicated key(s), {extras} extra item(s)")
        for key, members in sorted(groups.items())[:limit]:
            label = key if isinstance(key, str) else "/".join(key)
            print(f"  {label}: {len(members)}")
            for member in members:
                print(f"    {member}")
        if len(groups) > limit:
            print(f"  ... {len(groups) - limit} more")
        clean = clean and not groups
    print("CLEAN" if clean else "DUPLICATES FOUND")
    return clean


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int, nargs="+", help="only these frames")
    ap.add_argument("--since", help="only products created / jobs queued at or after this time "
                                    "(ISO 8601, e.g. 2026-10-05T00:00:00Z)")
    ap.add_argument("--limit", type=int, default=20, help="duplicated keys listed per category")
    ap.add_argument("--json", action="store_true", help="print the report as JSON")
    args = ap.parse_args(argv)

    from opera_commons.es_connection import get_grq_es, get_mozart_es
    report = audit(get_grq_es(logger).es, get_mozart_es(logger).es, args.frame_id, args.since)

    if args.json:
        print(json.dumps({what: {k if isinstance(k, str) else "/".join(k): v for k, v in groups.items()}
                          for what, groups in report.items()}, indent=2))
        clean = not any(report.values())
    else:
        clean = print_report(report, args.limit)
    return 0 if clean else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
