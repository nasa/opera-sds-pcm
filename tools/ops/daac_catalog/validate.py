"""Validation: counts, field-level comparison with GRQ, and the consumer query pack."""
from __future__ import annotations

import json
import logging
import random
import re
from collections import Counter
from pathlib import Path

from tools.ops.daac_catalog import store
from tools.ops.daac_catalog.bundle import BundleReader
from tools.ops.daac_catalog.compare import compare_docs
from tools.ops.daac_catalog.registry import DatasetSpec
from tools.ops.daac_catalog.runs import RunDir, is_done, read_done, read_jsonl_gz

logger = logging.getLogger(__name__)

# Query shapes the GRQ consumers use (DISP-S1 evaluators and ledger, preconditions, DIST state, audits, bach).
QUERY_PACK = {
    "frame_by_cycle": {"query": {"bool": {"must": [{"term": {"metadata.frame_id": 16655}}]}},
                       "sort": [{"metadata.acquisition_cycle": {"order": "desc", "unmapped_type": "long"}}],
                       "size": 5},
    "burst_window": {"query": {"bool": {
        "should": [{"terms": {"metadata.burst_id.keyword": ["T113-241595-IW1"]}},
                   {"terms": {"metadata.Files.burst_id.keyword": ["T113-241595-IW1"]}}],
        "minimum_should_match": 1,
        "filter": [{"range": {"metadata.Files.acquisition_ts": {"gte": "2026-05-31T00:00:00",
                                                                 "lte": "2026-06-01T00:00:00"}}}]}}, "size": 5},
    "tile": {"query": {"term": {"metadata.tile_id.keyword": "T36VUM"}}, "size": 5},
    "creation_window": {"query": {"range": {"creation_timestamp": {"gte": "2026-06-01T00:00:00",
                                                                     "lte": "2026-06-02T00:00:00"}}},
                        "sort": [{"creation_timestamp": "desc"}], "size": 5},
    "delivered_volume": {"query": {"term": {"daac_delivery_status.keyword": "SUCCESS"}},
                         "aggs": {"ds": {"terms": {"field": "dataset.keyword"}},
                                  "size": {"sum": {"field": "metadata.FileSize"}}}, "size": 0},
    "sensors": {"query": {"term": {"dataset.keyword": "L3_DSWx_S1"}},
                "aggs": {"sensor": {"terms": {"field": "metadata.sensor.keyword"}}}, "size": 0},
}


def manifest_counts(run: RunDir, dataset: str) -> dict:
    total, complete, incomplete = 0, 0, []
    for shard in run.listing_shards(dataset):
        done = read_done(run.listing(dataset, shard))
        if not done:
            incomplete.append(shard)
            continue
        total += done["granules"]
        complete += 1
    return {"granules": total, "shards_complete": complete, "shards_incomplete": incomplete}


def docs_counts(run: RunDir, dataset: str) -> dict:
    docs = rejects = 0
    for shard in run.docs_shards(dataset):
        done = read_done(run.docs(dataset, shard))
        if done:
            docs += done["docs"]
            rejects += done["rejects"]
    return {"docs": docs, "rejects": rejects}


def validate_counts(spec: DatasetSpec, run: RunDir | None, client, prefix: str) -> dict:
    dataset = spec.dataset.value
    pattern = store.dataset_pattern(prefix, dataset)
    out = {"dataset": dataset, "store": store.count(client, pattern)}
    if run is not None:
        out["listing"] = manifest_counts(run, dataset)
        out["built"] = docs_counts(run, dataset)
        out["ok"] = out["store"] == out["built"]["docs"] and \
            out["listing"]["granules"] == out["built"]["docs"] + out["built"]["rejects"]
    return out


def validate_bundle_counts(reader: BundleReader, client, prefix: str) -> dict:
    expected = {prefix + suffix: entry["count"] for suffix, entry in reader.manifest["indices"].items()}
    actual = store.counts_by_index(client, ",".join(expected)) if expected else {}
    mismatches = {index: {"bundle": n, "store": actual.get(index, 0)} for index, n in expected.items()
                  if actual.get(index, 0) < n}
    return {"bundle_id": reader.manifest["bundle_id"], "kind": reader.kind, "indices": len(expected),
            "bundle_docs": reader.manifest["doc_count"], "store_docs": sum(actual.get(i, 0) for i in expected),
            "short_indices": mismatches, "ok": not mismatches}


def sample_catalog_docs(spec: DatasetSpec, client, prefix: str, size: int, seed: int = 0) -> list[dict]:
    pattern = store.dataset_pattern(prefix, spec.dataset.value)
    body = {"size": size, "query": {"function_score": {"query": {"term": {"dataset.keyword": spec.dataset.value}},
                                                       "random_score": {"seed": seed, "field": "_seq_no"}}}}
    hits = client.search(index=pattern, body=body, ignore_unavailable=True)["hits"]["hits"]
    return [h["_source"] for h in hits]


def sample_run_docs(spec: DatasetSpec, run: RunDir, size: int, seed: int = 0) -> list[dict]:
    """Reservoir sample of built documents from a run directory."""
    rng = random.Random(seed)
    sample, seen = [], 0
    for shard in run.docs_shards(spec.dataset.value):
        path = run.docs(spec.dataset.value, shard)
        if not is_done(path):
            continue
        for doc in read_jsonl_gz(path):
            seen += 1
            if len(sample) < size:
                sample.append(doc)
            else:
                j = rng.randrange(seen)
                if j < size:
                    sample[j] = doc
    return sample


def _newer(index_a: str, index_b: str) -> bool:
    """True when index_a's YYYY.MM suffix is later than index_b's (ties broken by name)."""
    def key(index):
        return index.rsplit("-", 1)[-1] if "-" in index else "", index
    return key(index_a) > key(index_b)


def fetch_grq_docs(grq_client, dataset: str, ids: list[str], grq_pattern: str | None = None) -> dict[str, dict]:
    pattern = grq_pattern or f"grq_*_{dataset.lower()}-*"
    out = {}
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        body = {"size": len(chunk), "query": {"terms": {"id.keyword": chunk}}}
        for hit in grq_client.search(index=pattern, body=body, ignore_unavailable=True)["hits"]["hits"]:
            # several GRQ generations may hold the same id; keep the newest index
            if hit["_id"] not in out or _newer(hit["_index"], out[hit["_id"]]["_index"]):
                out[hit["_id"]] = hit
    return {k: v["_source"] for k, v in out.items()}


def load_grq_dump(path: Path) -> dict[str, dict]:
    """GRQ documents from a JSON-lines dump of search hits (or bare sources), newest index winning per id.

    Lets the comparison run where the reference cluster's credentials are not: dump the hits read-only on the
    cluster's own host, copy the file, compare here."""
    out, index_of = {}, {}
    for p in sorted(Path(path).glob("*.jsonl")) if Path(path).is_dir() else [Path(path)]:
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            hit = json.loads(line)
            source, index = (hit["_source"], hit.get("_index", "")) if "_source" in hit else (hit, "")
            gid = source.get("id")
            if gid and (gid not in out or _newer(index, index_of[gid])):
                out[gid], index_of[gid] = source, index
    return out


def _delta_stats(results) -> dict:
    """Distribution of (ours - GRQ) in seconds for each approximated timestamp."""
    by_field: dict[str, list[float]] = {}
    for r in results:
        for name, seconds in r.deltas.items():
            by_field.setdefault(name, []).append(seconds)
    out = {}
    for name, values in sorted(by_field.items()):
        values.sort()
        pick = lambda q: values[min(len(values) - 1, int(q * (len(values) - 1)))]
        out[name] = {"n": len(values), "min": values[0], "p50": pick(0.5), "p99": pick(0.99), "max": values[-1]}
    return out


def validate_against_grq(spec: DatasetSpec, ours: list[dict], grq_client=None, grq_pattern: str | None = None,
                         grq_docs: dict[str, dict] | None = None) -> dict:
    by_id = {d["id"]: d for d in ours}
    if grq_docs is not None:
        grq = {gid: doc for gid, doc in grq_docs.items() if gid in by_id}
    else:
        grq = fetch_grq_docs(grq_client, spec.dataset.value, sorted(by_id), grq_pattern)
    results = [compare_docs(by_id[gid], grq[gid]) for gid in sorted(grq)]
    problems = Counter()
    for r in results:
        for line in r.exact_mismatches + r.file_mismatches + r.approx_out_of_tolerance + r.missing + \
                r.unexpected_extra:
            problems[re.sub(r"Files\[[^\]]*\]", "Files[*]", line.split(":")[0])] += 1
    n = len(results)
    return {
        "dataset": spec.dataset.value, "sampled": len(ours), "found_in_grq": n,
        "exact_ok": sum(r.exact_ok for r in results), "approx_ok": sum(r.approx_ok for r in results),
        "no_missing": sum(not r.missing for r in results), "no_extra": sum(not r.unexpected_extra for r in results),
        "exact_rate": round(sum(r.exact_ok for r in results) / n, 4) if n else None,
        "approx_rate": round(sum(r.approx_ok for r in results) / n, 4) if n else None,
        "problem_fields": problems.most_common(30),
        "examples": [{"id": r.id, "lines": r.lines()[:12]} for r in results if not r.ok][:10],
        "omitted_counts": Counter(x for r in results for x in r.omitted).most_common(),
        "approximation_seconds": _delta_stats(results),
        "known_counts": Counter(re.sub(r"Files\[[^\]]*\]", "Files[*]", x.split(":")[0])
                                for r in results for x in r.known_artifacts).most_common(10),
        "grq_without_cnm_r": [r.id for r in results
                              if "GRQ has no CNM-R record for this delivered product" in r.known_artifacts][:200],
    }


def run_query_pack(client, targets: dict[str, str]) -> dict:
    """{query name: {target: {total, ids or aggs}}} for each target index pattern or alias."""
    out = {}
    for name, body in QUERY_PACK.items():
        out[name] = {}
        for label, index in targets.items():
            try:
                resp = client.search(index=index, body=body, ignore_unavailable=True, request_timeout=120)
            except Exception as err:
                out[name][label] = {"error": str(err)[:300]}
                continue
            total = resp["hits"]["total"]
            entry = {"total": total["value"] if isinstance(total, dict) else total,
                     "ids": [h["_id"] for h in resp["hits"]["hits"]]}
            if "aggregations" in resp:
                entry["aggs"] = resp["aggregations"]
            out[name][label] = entry
    return out


def markdown_report(title: str, sections: dict) -> str:
    lines = [f"# {title}", ""]
    for heading, value in sections.items():
        lines += [f"## {heading}", "", "```json", json.dumps(value, indent=1, default=str), "```", ""]
    return "\n".join(lines)


def write_report(run: RunDir | None, workdir: Path, name: str, text: str) -> Path:
    target = (run.reports() if run else Path(workdir) / "reports")
    target.mkdir(parents=True, exist_ok=True)
    path = target / name
    path.write_text(text)
    return path
