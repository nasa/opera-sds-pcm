"""Build, index and enrich stages over run directories and the store."""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from fnmatch import fnmatch
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator

from tools.ops.daac_catalog.build import BuildContext, build_doc, index_name
from tools.ops.daac_catalog.fetch import SourceFetcher
from tools.ops.daac_catalog.parse import GranuleListing, ListedObject, RejectedGranule, parse_granule
from tools.ops.daac_catalog.registry import DatasetSpec
from tools.ops.daac_catalog.runs import (
    JsonlGzWriter, RunDir, append_jsonl, is_done, mark_done, read_done, read_jsonl_gz, write_json)
from tools.ops.daac_catalog.sources import fmt_seconds_z, normalize_ts
from tools.ops.daac_catalog import store

logger = logging.getLogger(__name__)

LIFECYCLE_FIELDS = ("first_seen", "last_seen", "deleted_at", "lifecycle")


def chunked(iterable: Iterable, size: int) -> Iterator[list]:
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


# ---- Parquet ----------------------------------------------------------------------------------------------------

PARQUET_COLUMNS = ("id", "index", "dataset", "tier", "natural_key", "creation_timestamp", "doc")


def parquet_row(doc: dict) -> dict:
    return {"id": doc["id"], "index": index_name(doc), "dataset": doc["dataset"], "tier": doc["catalog"]["tier"],
            "natural_key": doc["catalog"]["natural_key"], "creation_timestamp": doc.get("creation_timestamp"),
            "doc": json.dumps(doc, separators=(",", ":"))}


class ParquetSink:
    """Row-group-at-a-time Parquet writer: pyarrow when installed, else fastparquet (the mozart runtime)."""

    def __init__(self, path: Path, batch: int = 5000):
        self.path = Path(path)
        self._part = self.path.with_name(self.path.name + ".part")
        self._batch = batch
        self._rows: list[dict] = []
        self._writer = None
        self._fastparquet_started = False
        self.count = 0
        try:
            import pyarrow  # noqa: F401
            self._engine = "pyarrow"
        except ImportError:
            self._engine = "fastparquet"

    def write(self, doc: dict):
        self._rows.append(parquet_row(doc))
        self.count += 1
        if len(self._rows) >= self._batch:
            self._flush()

    def _flush(self):
        if not self._rows:
            return
        if self._engine == "pyarrow":
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pylist(self._rows)
            if self._writer is None:
                self._writer = pq.ParquetWriter(self._part, table.schema, compression="zstd")
            self._writer.write_table(table)
        else:
            import fastparquet
            import pandas as pd
            frame = pd.DataFrame(self._rows, columns=list(PARQUET_COLUMNS))
            fastparquet.write(str(self._part), frame, compression="GZIP", append=self._fastparquet_started)
            self._fastparquet_started = True
        self._rows = []

    def close(self):
        self._flush()
        if self._writer is not None:
            self._writer.close()
        if self._part.exists():
            self._part.replace(self.path)


# ---- build ------------------------------------------------------------------------------------------------------

@dataclass
class BuildOptions:
    tier: int = 1
    workers: int = 32
    shard_filter: str | None = None
    ids: set | None = None
    parquet: bool = False
    force: bool = False


def build_one(spec: DatasetSpec, record: dict, fetcher: SourceFetcher | None, ctx: BuildContext,
              tier: int) -> tuple[dict | None, dict | None]:
    """(document, None) or (None, reject record) for one manifest line."""
    listing = GranuleListing.from_record(record)
    try:
        parsed = parse_granule(spec, listing)
    except RejectedGranule as err:
        return None, {"id": listing.id, "reason": str(err), "files": [f.key for f in listing.files][:50]}
    problems = []
    t1 = t2 = None
    try:
        if tier >= 1 and fetcher is not None:
            t1, more = fetcher.tier1(parsed)
            problems += more
        if tier >= 2 and fetcher is not None:
            t2, more = fetcher.tier2(parsed)
            problems += more
    except Exception as err:  # a source read failure degrades to a lower tier for this granule only
        problems.append(f"source read failed: {type(err).__name__}: {err}")
    doc = build_doc(parsed, ctx, t1, t2)
    if problems:
        doc["catalog"].setdefault("problems", []).extend(problems)
    return doc, None


def run_build(spec: DatasetSpec, run: RunDir, fetcher: SourceFetcher | None, ctx: BuildContext,
              options: BuildOptions) -> dict:
    dataset = spec.dataset.value
    summary = {"dataset": dataset, "tier": options.tier, "shards": 0, "skipped": 0, "docs": 0, "rejects": 0,
               "incomplete_listing_shards": []}
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, options.workers)) as pool:
        for shard in run.listing_shards(dataset):
            if options.shard_filter and not fnmatch(shard, options.shard_filter):
                continue
            listing_path = run.listing(dataset, shard)
            if not is_done(listing_path):
                summary["incomplete_listing_shards"].append(shard)
                continue
            docs_path = run.docs(dataset, shard)
            if is_done(docs_path) and not options.force:
                summary["skipped"] += 1
                continue
            rejects_path = run.rejects(dataset, shard)
            if rejects_path.exists():
                rejects_path.unlink()
            shard_started = time.monotonic()
            sink = ParquetSink(docs_path.with_name(f"{shard}.parquet")) if options.parquet else None
            n_docs = n_rejects = 0
            with JsonlGzWriter(docs_path) as writer:
                records = (r for r in read_jsonl_gz(listing_path) if not options.ids or r["id"] in options.ids)
                for chunk in chunked(records, 256):
                    results = pool.map(lambda r: build_one(spec, r, fetcher, ctx, options.tier), chunk)
                    rejects = []
                    for doc, reject in results:
                        if doc is not None:
                            writer.write(doc)
                            if sink:
                                sink.write(doc)
                            n_docs += 1
                        else:
                            rejects.append(reject)
                    if rejects:
                        append_jsonl(rejects_path, rejects)
                        n_rejects += len(rejects)
            if sink:
                sink.close()
            mark_done(docs_path, {"docs": n_docs, "rejects": n_rejects, "tier": options.tier,
                                  "seconds": round(time.monotonic() - shard_started, 2),
                                  "built_at": fmt_seconds_z(ctx.built_at)})
            summary["shards"] += 1
            summary["docs"] += n_docs
            summary["rejects"] += n_rejects
            logger.info(f"{dataset} {shard}: {n_docs} docs, {n_rejects} rejects")
    summary["seconds"] = round(time.monotonic() - started, 1)
    if fetcher is not None:
        summary["fetch"] = fetcher.stats.to_dict()
    return summary


# ---- index ------------------------------------------------------------------------------------------------------

@dataclass
class IndexOptions:
    mode: str = "catalog"
    prefix: str | None = None
    replace: bool = False
    chunk_size: int = 2000
    threads: int = 3
    shard_filter: str | None = None
    ids: set | None = None
    force: bool = False


def run_index(spec: DatasetSpec, run: RunDir, client, options: IndexOptions) -> dict:
    dataset = spec.dataset.value
    prefix = store.index_prefix(options.mode, options.prefix)
    op_type = "index" if options.replace else "create"
    total = store.BulkResult()
    summary = {"dataset": dataset, "prefix": prefix, "op_type": op_type, "shards": 0, "skipped": 0}
    for shard in run.docs_shards(dataset):
        if options.shard_filter and not fnmatch(shard, options.shard_filter):
            continue
        docs_path = run.docs(dataset, shard)
        if not is_done(docs_path):
            continue
        result_path = run.index_result(dataset, shard)
        if is_done(result_path) and not options.force:
            summary["skipped"] += 1
            continue
        docs = (d for d in read_jsonl_gz(docs_path) if not options.ids or d["id"] in options.ids)
        result = store.bulk_load(client, store.bulk_actions(docs, prefix, op_type), chunk_size=options.chunk_size,
                                 threads=options.threads)
        write_json(result_path, result.to_dict())
        mark_done(result_path, {"ok": result.ok, "conflicts": result.conflicts, "errors": result.errors})
        total.add(result)
        summary["shards"] += 1
        logger.info(f"{dataset} {shard}: indexed {result.ok}, conflicts {result.conflicts}, errors {result.errors}")
    summary.update(total.to_dict())
    summary["alias"] = store.ensure_alias(client, prefix, dataset)
    return summary


# ---- enrich -----------------------------------------------------------------------------------------------------

def listing_from_doc(doc: dict) -> GranuleListing:
    files = [ListedObject(f["src"], f["bucket"], f["key"], int(f["size"]), f["last_modified"], f.get("etag"))
             for f in doc["catalog"]["files"]]
    return GranuleListing(doc["id"], doc["dataset"], files)


@dataclass
class EnrichOptions:
    tier: int = 1
    since: str | None = None        # YYYY-MM: creation month lower bound
    until: str | None = None
    only_missing: bool = True
    workers: int = 32
    limit: int | None = None
    prefix: str = store.CATALOG_PREFIX


def enrich_query(dataset: str, options: EnrichOptions) -> dict:
    must = [{"term": {"dataset.keyword": dataset}}]
    if options.only_missing:
        must.append({"range": {"catalog.tier": {"lt": options.tier}}})
    if options.since or options.until:
        rng = {}
        if options.since:
            rng["gte"] = f"{options.since}-01T00:00:00"
        if options.until:
            rng["lt"] = f"{options.until}-01T00:00:00"
        must.append({"range": {"creation_timestamp": rng}})
    return {"bool": {"filter": must}}


def enrich_doc(spec: DatasetSpec, old: dict, fetcher: SourceFetcher, ctx: BuildContext, tier: int,
               prefix: str) -> tuple[dict | None, str | None]:
    """Rebuilt document (lifecycle fields preserved) and the index the old version lives in."""
    parsed = parse_granule(spec, listing_from_doc(old))
    t1, problems = fetcher.tier1(parsed) if tier >= 1 else (None, [])
    t2 = None
    if tier >= 2:
        t2, more = fetcher.tier2(parsed)
        problems += more
    doc = build_doc(parsed, ctx, t1, t2)
    for key in LIFECYCLE_FIELDS:
        if key in old["catalog"]:
            doc["catalog"][key] = old["catalog"][key]
    if problems:
        doc["catalog"]["problems"] = problems
    return doc, index_name(old, prefix)


def run_enrich(spec: DatasetSpec, client, fetcher: SourceFetcher, ctx: BuildContext, options: EnrichOptions,
               report_path: Path | None = None) -> dict:
    dataset = spec.dataset.value
    pattern = store.dataset_pattern(options.prefix, dataset)
    query = enrich_query(dataset, options)
    summary = {"dataset": dataset, "tier": options.tier, "scanned": 0, "updated": 0, "moved": 0, "failed": 0}
    total = store.BulkResult()

    def work(hit):
        old = hit["_source"]
        try:
            doc, _ = enrich_doc(spec, old, fetcher, ctx, options.tier, options.prefix)
        except Exception as err:  # keep going; the old document stays as it was
            return hit, None, f"{type(err).__name__}: {err}"
        return hit, doc, None

    hits = store.scan(client, pattern, query)
    if options.limit:
        hits = islice(hits, options.limit)
    with ThreadPoolExecutor(max_workers=max(1, options.workers)) as pool:
        for chunk in chunked(hits, 500):
            actions = []
            for hit, doc, error in pool.map(work, chunk):
                summary["scanned"] += 1
                if error:
                    summary["failed"] += 1
                    continue
                new_index = index_name(doc, options.prefix)
                store.stamp(doc)
                actions.append({"_op_type": "index", "_index": new_index, "_id": doc["id"], "_source": doc})
                if new_index != hit["_index"]:
                    actions.append({"_op_type": "delete", "_index": hit["_index"], "_id": doc["id"]})
                    summary["moved"] += 1
            total.add(store.bulk_load(client, actions))
    summary.update(total.to_dict())
    summary["updated"] = summary["scanned"] - summary["failed"]
    if report_path:
        write_json(report_path, summary)
    return summary
