#!/usr/bin/env python3
"""Tag each SLC scene a burst coverage audit reports missing with the fix it needs, from OPS FWD state.

cmr_audit_burst_coverage.py reports which CSLC-S1 bursts are missing from CMR, not why. The fix depends
on where the scene stopped in the pipeline, which only the forward cluster knows: whether the SLC was ever
ingested, whether the query job flagged it as intersecting North America, whether the ionosphere download
job (which submits the forward CSLC job) ever got to it, and what happened to its CSLC job. This tool reads
that state (read-only searches against GRQ and the job OpenSearch) and writes one row per scene with a class
and the fix, plus one SLC list per class for OPS.

Run it where the celery config reaches the cluster: the FWD mozart, as hysdsops. The opera-monitoring
launchpad host has no route to FWD, so copy the audit report there first.

    python3 classify_missing_slcs.py cslc_s1_burst_coverage.jsonl -o classified.csv --lists-dir lists/

The checks, in order (the first that applies wins):

  produced_not_in_cmr   every missing burst has a CSLC on FWD: the products never reached CMR (delivery)
  partial_output        the scene has CSLCs on FWD, but not for every missing burst (SAS dropped bursts)
  cslc_job_in_progress  the newest CSLC job is queued, started or offline: wait for it
  cslc_job_failed       the newest CSLC job failed: triage it, fix, resubmit
  cslc_job_<status>     the newest CSLC job ended otherwise (e.g. completed without publishing)
  outside_gate          FWD has the SLC but not flagged intersects_north_america: no CSLC is expected
  iono_stranded         forward SLC, NA flag set, no ionosphere metadata, no CSLC job: the ionosphere download
                        job never picked it up (late IONEX, 52 h lookback); rerun it over the creation minute
                        (iono_window). Use --check-s3-inputs: a rerun over a scene whose inputs aged out fails
  inputs_aged_out       as iono_stranded, but the SLC's S3 inputs are gone (--check-s3-inputs): a rerun would
                        fail; re-download by native id in historical mode, then submit the historical CSLC job
  trigger_lost          NA scene with an SLC doc and no CSLC job otherwise: submit the CSLC job on demand
  download_not_ingested in slc_catalog but no SLC doc on FWD: re-download by native id (reprocessing mode)
  not_ingested          not in slc_catalog: the query never saw it; re-query by native id or sensing time

Job state comes from the job_status-current alias, so jobs older than the alias span are not seen and such a
scene falls through to the SLC checks. Job docs do not index their input dataset, so CSLC jobs are scanned
once (type, status, input id and error only) and matched to scenes locally.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Optional

CSLC_PRODUCT = "CSLC-S1"
SLC_INDEX = "grq_*_l1_s1_slc-*"
CSLC_INDEX = "grq_*_l2_cslc_s1-*"   # not the compressed or static-layer indices
CATALOG_INDEX = "slc_catalog*"
JOB_INDEX = "job_status-current"
CSLC_JOB_TYPE_PREFIX = "job-SCIFLO_L2_CSLC_S1"   # forward/reprocessing and _hist
ACTIVE_JOB_STATUSES = {"job-queued", "job-started", "job-offline"}

FIXES = {
    "produced_not_in_cmr": "CSLCs are on FWD but not in CMR: check the CNM delivery and ingest of those products",
    "partial_output": "some bursts were made, the missing ones were not: check the CSLC job's burst list and SAS "
                      "log; resubmit the scene (native id with --burst-ids for the missing bursts)",
    "cslc_job_in_progress": "the CSLC job is still queued or running: wait for it",
    "cslc_job_failed": "triage the failed CSLC job (error below), fix the cause and resubmit",
    "outside_gate": "none: the PCM makes no CSLC for scenes outside the North America polygon",
    "iono_stranded": "rerun 'Download SLC ionosphere correction data' over the creation window below",
    "inputs_aged_out": "SLC inputs are gone from S3: re-download by native id in historical mode, then submit the "
                       "historical CSLC job on demand against the new -rN doc",
    "trigger_lost": "submit the CSLC job on demand against the SLC doc below",
    "download_not_ingested": "in slc_catalog but never ingested: re-download by native id in reprocessing mode",
    "not_ingested": "never ingested: re-query by native id or by sensing time in reprocessing mode",
}

CSV_COLUMNS = [
    "slc_id", "platform", "sensing_date", "polarizations", "missing_bursts", "cbdb_frames", "class", "fix",
    "slc_doc_id", "slc_doc_created", "processing_mode", "intersects_north_america", "ionosphere",
    "iono_window", "in_slc_catalog", "cslc_on_fwd", "cslc_job", "cslc_job_status", "cslc_job_error",
    "inputs_in_s3",
]


# =============================================================================
# Audit report
# =============================================================================

@dataclass
class MissingScene:
    slc_id: str                   # CMR native id without the -SLC suffix
    platform: str
    sensing_date: str
    bursts: set = field(default_factory=set)        # {(burst_pattern, polarization)}
    cbdb_frames: set = field(default_factory=set)


def load_missing_scenes(paths: Iterable[str], product: str = CSLC_PRODUCT) -> tuple[dict, dict]:
    """Group the report's missing bursts by SLC. Returns ({slc_id: MissingScene}, notes).

    Reads the chunked JSONL (chunk_result lines) and the whole-run JSON report. notes counts what a reader
    must not mistake for gaps: unverified bursts and skipped chunks, by day.
    """
    scenes: dict[str, MissingScene] = {}
    notes = {"unverified_days": set(), "unverified": 0, "failed_days": set()}

    def add(item):
        sid = item["slc_native_id"].removesuffix("-SLC")
        scene = scenes.get(sid)
        if scene is None:
            scene = scenes[sid] = MissingScene(sid, item.get("platform", sid[:3]), item["acquisition_time"][:10])
        scene.bursts.add((item["burst_pattern"], item.get("polarization", "")))
        scene.cbdb_frames.update(item.get("cbdb_frames") or [])

    for path in paths:
        with open(path) as f:
            if path.endswith(".jsonl"):
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    kind = rec.get("_type")
                    if kind == "chunk_result" and rec.get("product_type") == product:
                        for item in rec.get("missing", []):
                            add(item)
                        notes["unverified"] += len(rec.get("unverified", []))
                        notes["unverified_days"].update(u["acquisition_time"][:10] for u in rec.get("unverified", []))
                    elif kind == "chunk_error":
                        notes["failed_days"].add(rec["chunk_start"][:10])
            else:
                results = json.load(f)
                cov = results.get("products", {}).get(product, {})
                for item in cov.get("missing", []):
                    add(item)
                notes["unverified"] += len(cov.get("unverified", []))
                notes["unverified_days"].update(u["acquisition_time"][:10] for u in cov.get("unverified", []))
                notes["failed_days"].update(fc["chunk_start"][:10]
                                            for fc in results.get("metadata", {}).get("failed_chunks", []))
    return scenes, notes


# =============================================================================
# FWD state
# =============================================================================

@dataclass
class FwdFacts:
    in_catalog: bool = False
    catalog_na: Optional[bool] = None
    slc_docs: list = field(default_factory=list)     # dicts: id, created, processing_mode, na, iono, s3_prefix
    cslc_bursts: set = field(default_factory=set)    # {(burst_pattern, polarization)} with a CSLC doc on FWD
    cslc_jobs: list = field(default_factory=list)    # dicts: type, status, queued, error, input
    inputs_in_s3: Optional[bool] = None

    @property
    def newest_doc(self) -> Optional[dict]:
        return max(self.slc_docs, key=lambda d: d.get("created") or "") if self.slc_docs else None

    @property
    def newest_job(self) -> Optional[dict]:
        return max(self.cslc_jobs, key=lambda j: j.get("queued") or "") if self.cslc_jobs else None


def _batches(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _hits(es, index: str, body: dict) -> list[dict]:
    res = es.search(index=index, body=body)
    hits = res["hits"]["hits"]
    total = res["hits"].get("total")
    total = total.get("value") if isinstance(total, dict) else total
    if total is not None and total > len(hits):
        raise RuntimeError(f"{index}: {total} hits but only {len(hits)} returned; lower --batch-size")
    return hits


def s3_prefix_from_urls(urls: list[str]) -> Optional[tuple[str, str]]:
    """(bucket, key prefix) of an SLC dataset from its GRQ urls, e.g. s3://s3-us-west-2.amazonaws.com:80/bucket/key."""
    for url in urls or []:
        if url.startswith("s3://"):
            parts = url[len("s3://"):].split("/", 2)
            if len(parts) == 3 and "amazonaws.com" in parts[0]:
                return parts[1], parts[2].rstrip("/") + "/"
            bucket, _, key = url[len("s3://"):].partition("/")
            return bucket, key.rstrip("/") + "/"
    return None


def gather_fwd_facts(scene_ids: list[str], grq, mozart, batch_size: int = 100,
                     jobs_since: Optional[str] = None) -> dict[str, FwdFacts]:
    """Read FWD state for the scenes: slc_catalog, SLC and CSLC datasets on GRQ, CSLC jobs. Read-only."""
    facts = {sid: FwdFacts() for sid in scene_ids}

    for batch in _batches(scene_ids, batch_size):
        for h in _hits(grq, CATALOG_INDEX, {
                "size": len(batch) * 5, "_source": ["granule_id", "intersects_north_america"],
                "query": {"terms": {"granule_id": [sid + "-SLC" for sid in batch]}}}):
            src = h["_source"]
            f = facts.get(src["granule_id"].removesuffix("-SLC"))
            if f is not None:
                f.in_catalog = True
                if src.get("intersects_north_america") is not None:
                    f.catalog_na = bool(src["intersects_north_america"])

        for h in _hits(grq, SLC_INDEX, {
                "size": len(batch) * 10,
                "_source": ["id", "creation_timestamp", "urls", "metadata.id", "metadata.processing_mode",
                            "metadata.intersects_north_america", "metadata.ionosphere.FileName"],
                "query": {"terms": {"metadata.id.keyword": batch}}}):
            src = h["_source"]
            meta = src.get("metadata", {})
            f = facts.get(meta.get("id"))
            if f is not None:
                f.slc_docs.append({
                    "id": src.get("id", h["_id"]),
                    "created": src.get("creation_timestamp", ""),
                    "processing_mode": meta.get("processing_mode"),
                    "na": meta.get("intersects_north_america"),
                    "iono": bool(meta.get("ionosphere")),
                    "s3_prefix": s3_prefix_from_urls(src.get("urls")),
                })

        for h in _hits(grq, CSLC_INDEX, {
                "size": 10000,
                "_source": ["metadata.input_granule_id", "metadata.burst_id", "metadata.polarization"],
                "query": {"terms": {"metadata.input_granule_id.keyword": batch}}}):
            meta = h["_source"].get("metadata", {})
            f = facts.get(meta.get("input_granule_id"))
            if f is not None:
                f.cslc_bursts.add((meta.get("burst_id"), meta.get("polarization")))

    if jobs_since is None:
        created = [d["created"] for f in facts.values() for d in f.slc_docs if d.get("created")]
        jobs_since = min(created)[:10] if created else None
    for job in scan_cslc_jobs(mozart, jobs_since):
        f = facts.get(job["input"].rsplit("-r", 1)[0])
        if f is not None:
            f.cslc_jobs.append(job)
    return facts


def scan_cslc_jobs(mozart, since: Optional[str], page: int = 5000) -> Iterable[dict]:
    """Every CSLC job doc queued since `since` (YYYY-MM-DD), with only the fields the classes need.

    Job docs copy no input dataset id into an indexed field until the job localizes its inputs, so a
    queued job or one that failed before localizing cannot be searched for by scene; scanning the CSLC
    jobs once and matching locally finds all of them.
    """
    if since is None:
        return
    body = {
        "size": page,
        "_source": ["type", "status", "error", "short_error", "job.params.input_dataset_id",
                    "job.job_info.time_queued"],
        "query": {"bool": {"filter": [
            {"prefix": {"type": CSLC_JOB_TYPE_PREFIX}},
            {"range": {"job.job_info.time_queued": {"gte": since}}},
        ]}},
    }
    res = mozart.search(index=JOB_INDEX, body=body, scroll="2m")
    scroll_id = res.get("_scroll_id")
    try:
        while True:
            hits = res["hits"]["hits"]
            if not hits:
                break
            for h in hits:
                src = h["_source"]
                job = src.get("job", {})
                input_id = job.get("params", {}).get("input_dataset_id", "")
                if not input_id:
                    continue
                yield {
                    "type": src.get("type", ""),
                    "status": src.get("status", ""),
                    "queued": job.get("job_info", {}).get("time_queued", ""),
                    "error": _first_line(src.get("short_error") or src.get("error")),
                    "input": input_id,
                }
            if not scroll_id:
                break
            res = mozart.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = res.get("_scroll_id", scroll_id)
    finally:
        if scroll_id:
            try:
                mozart.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _first_line(text: Optional[str], limit: int = 200) -> str:
    text = (text or "").strip()
    return text.splitlines()[0][:limit] if text else ""


def _stranded_by_ionosphere_job(f: FwdFacts) -> bool:
    """A forward NA scene the ionosphere download job never picked up: no CSLC, no job, no iono metadata."""
    doc = f.newest_doc
    return bool(doc and not f.cslc_bursts and not f.cslc_jobs and doc.get("na") is True
                and doc.get("processing_mode") == "forward" and not doc.get("iono"))


def check_s3_inputs(facts: dict[str, FwdFacts], s3_client=None) -> None:
    """For scenes that would get an ionosphere rerun, record whether the SLC .zip is still in S3.

    The forward inputs prefix ages out; a rerun over a scene whose SAFE is gone submits a CSLC job that
    fails with a 404 on the SAFE download.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    for f in facts.values():
        doc = f.newest_doc
        if not _stranded_by_ionosphere_job(f) or not doc.get("s3_prefix"):
            continue
        bucket, prefix = doc["s3_prefix"]
        res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100)
        f.inputs_in_s3 = any(o["Key"].endswith(".zip") for o in res.get("Contents", []))


# =============================================================================
# Classification
# =============================================================================

def iono_window(created: str) -> str:
    """The ionosphere job's start/end arguments for the minute a doc was created in.

    The job picks every forward NA SLC doc created in [start, end) that has no ionosphere metadata and
    submits a CSLC job for each, so a wide window also resubmits neighbours that got their CSLC another
    way. One minute around the creation time keeps it to this scene (and any created the same minute).
    """
    t = datetime.fromisoformat(created.replace("Z", "+00:00")).replace(tzinfo=None, second=0, microsecond=0)
    return f"--start-date={t:%Y-%m-%dT%H:%M:%SZ} --end-date={t + timedelta(minutes=1):%Y-%m-%dT%H:%M:%SZ}"


def classify(scene: MissingScene, f: FwdFacts) -> str:
    if f.cslc_bursts:
        return "produced_not_in_cmr" if scene.bursts <= f.cslc_bursts else "partial_output"
    job = f.newest_job
    if job:
        if job["status"] in ACTIVE_JOB_STATUSES:
            return "cslc_job_in_progress"
        if job["status"] == "job-failed":
            return "cslc_job_failed"
        return "cslc_job_" + job["status"].removeprefix("job-")
    doc = f.newest_doc
    if doc:
        if doc.get("na") is not True:
            return "outside_gate"
        if _stranded_by_ionosphere_job(f):
            return "inputs_aged_out" if f.inputs_in_s3 is False else "iono_stranded"
        return "trigger_lost"
    if f.in_catalog:
        return "outside_gate" if f.catalog_na is False else "download_not_ingested"
    return "not_ingested"


def row_for(scene: MissingScene, f: FwdFacts) -> dict:
    cls = classify(scene, f)
    doc, job = f.newest_doc, f.newest_job
    fix = FIXES.get(cls) or f"the newest CSLC job is {job['status']} with no CSLC on FWD: check its products_staged and triage dataset"
    return {
        "slc_id": scene.slc_id,
        "platform": scene.platform,
        "sensing_date": scene.sensing_date,
        "polarizations": "|".join(sorted({pol for _, pol in scene.bursts})),
        "missing_bursts": len(scene.bursts),
        "cbdb_frames": "|".join(map(str, sorted(scene.cbdb_frames))),
        "class": cls,
        "fix": fix,
        "slc_doc_id": doc["id"] if doc else "",
        "slc_doc_created": doc["created"] if doc else "",
        "processing_mode": (doc.get("processing_mode") or "") if doc else "",
        "intersects_north_america": "" if not doc or doc.get("na") is None else str(bool(doc["na"])).lower(),
        "ionosphere": "" if not doc else ("yes" if doc.get("iono") else "no"),
        "iono_window": iono_window(doc["created"]) if cls == "iono_stranded" and doc.get("created") else "",
        "in_slc_catalog": "yes" if f.in_catalog else "no",
        "cslc_on_fwd": len(f.cslc_bursts & scene.bursts),
        "cslc_job": f"{job['type']} {job['input']} queued {job['queued']}" if job else "",
        "cslc_job_status": job["status"] if job else "",
        "cslc_job_error": job["error"] if job else "",
        "inputs_in_s3": "" if f.inputs_in_s3 is None else ("yes" if f.inputs_in_s3 else "no"),
    }


# =============================================================================
# CLI
# =============================================================================

def connect():
    """GRQ and job OpenSearch clients from this host's celery config (the FWD mozart)."""
    import logging
    from opera_commons.es_connection import get_grq_es, get_mozart_es
    grq, mozart = get_grq_es().es, get_mozart_es(None).es
    # The search clients log every request at INFO under the PCM's logging setup; the summary is what matters.
    for name in ("elasticsearch", "opensearch", "urllib3", "botocore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return grq, mozart


def write_outputs(rows: list[dict], output: str, lists_dir: Optional[str]) -> None:
    with open(output, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    if lists_dir:
        os.makedirs(lists_dir, exist_ok=True)
        by_class = defaultdict(list)
        for r in rows:
            by_class[r["class"]].append(r)
        for cls, members in by_class.items():
            with open(os.path.join(lists_dir, f"slcs-{cls}.txt"), "w") as fh:
                fh.write("".join(f"{r['slc_id']}-SLC\n" for r in sorted(members, key=lambda r: r["slc_id"])))
        windows = Counter(r["iono_window"] for r in rows if r["iono_window"])
        if windows:
            with open(os.path.join(lists_dir, "iono-windows.txt"), "w") as fh:
                fh.write("".join(f"{w}  # {n} scene(s)\n" for w, n in sorted(windows.items())))


def print_summary(rows: list[dict], notes: dict) -> None:
    by_class = defaultdict(lambda: [0, 0, 0])
    for r in rows:
        c = by_class[r["class"]]
        c[0] += 1
        c[1] += r["missing_bursts"]
        c[2] += bool(r["cbdb_frames"])
    print(f"{'class':<24}{'scenes':>8}{'bursts':>8}{'in CBDB':>9}  fix")
    for cls, (n, bursts, cbdb) in sorted(by_class.items(), key=lambda kv: -kv[1][1]):
        print(f"{cls:<24}{n:>8}{bursts:>8}{cbdb:>9}  {FIXES.get(cls, 'see cslc_job_status')}")
    if notes["unverified"]:
        print(f"{notes['unverified']} bursts were UNVERIFIED in the report (no complete CMR answer) on "
              f"{','.join(sorted(notes['unverified_days']))}: not classified; re-run those days with --recheck-dates")
    if notes["failed_days"]:
        print(f"{len(notes['failed_days'])} day(s) were SKIPPED by the audit (no complete SLC listing): "
              f"{','.join(sorted(notes['failed_days']))}")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("reports", nargs="+", help="cmr_audit_burst_coverage.py report(s), .jsonl or .json")
    parser.add_argument("-o", "--output", default="classified_missing_cslc.csv", help="CSV, one row per SLC")
    parser.add_argument("--lists-dir", help="also write slcs-<class>.txt (native ids with -SLC) and iono-windows.txt here")
    parser.add_argument("--check-s3-inputs", action="store_true",
                        help="for scenes that would get an ionosphere rerun, check the SLC .zip is still in S3")
    parser.add_argument("--jobs-since", metavar="YYYY-MM-DD",
                        help="scan CSLC jobs queued since this date (default: the oldest SLC doc's creation day)")
    parser.add_argument("--batch-size", type=int, default=100, help="scenes per GRQ query (default 100)")
    return parser


def main(argv=None) -> int:
    args = create_parser().parse_args(argv)
    scenes, notes = load_missing_scenes(args.reports)
    print(f"{len(scenes)} SLC scenes with missing {CSLC_PRODUCT} bursts "
          f"({sum(len(s.bursts) for s in scenes.values())} bursts)")
    if not scenes:
        print_summary([], notes)
        return 0
    grq, mozart = connect()
    facts = gather_fwd_facts(sorted(scenes), grq, mozart, args.batch_size, args.jobs_since)
    if args.check_s3_inputs:
        check_s3_inputs(facts)
    rows = [row_for(scenes[sid], facts[sid]) for sid in sorted(scenes)]
    write_outputs(rows, args.output, args.lists_dir)
    print_summary(rows, notes)
    print(f"wrote {args.output}" + (f" and lists in {args.lists_dir}" if args.lists_dir else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
