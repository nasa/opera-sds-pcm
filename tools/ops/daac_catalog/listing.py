"""Tier-0 listing of the DAAC buckets into per-shard granule manifests.

A shard is an id prefix (`OPERA_L2_RTC-S1_T001-`). For each source of a dataset (products bucket, browse or
public bucket, ASF UMMG/ tree) and each bucket prefix of the source, the shard's keys are listed recursively and
grouped by granule. S3 returns keys in byte order and no granule id is a prefix of another, so every stream yields
granules in the same id order and the streams are merge-joined without holding a shard in memory.
"""
from __future__ import annotations

import heapq
import itertools
import logging
import time
from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Iterable, Iterator

from tools.ops.daac_catalog.parallel import fork_map, split, state, thread_map
from tools.ops.daac_catalog.parse import GranuleListing, ListedObject
from tools.ops.daac_catalog.registry import DatasetSpec, Layout, Source, granule_id_of
from tools.ops.daac_catalog.runs import JsonlGzWriter, RunDir, is_done, mark_done, shard_name, write_json

logger = logging.getLogger(__name__)


@dataclass
class ShardStats:
    granules: int = 0
    objects: dict = field(default_factory=dict)
    requests: dict = field(default_factory=dict)
    unparsed_keys: list = field(default_factory=list)
    unparsed_count: int = 0
    seconds: float = 0.0

    def to_dict(self) -> dict:
        return {"granules": self.granules, "objects": self.objects, "requests": self.requests,
                "unparsed_count": self.unparsed_count, "unparsed_sample": self.unparsed_keys[:20],
                "seconds": round(self.seconds, 2)}


def iter_objects(s3, bucket: str, prefix: str, stats: ShardStats | None = None, source: str = "",
                 page_size: int = 1000) -> Iterator[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": page_size}):
        if stats is not None:
            stats.requests[source] = stats.requests.get(source, 0) + 1
        for obj in page.get("Contents", []):
            yield obj


def _iso(dt) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(dt, "strftime") else str(dt)


def group_by_granule(spec: DatasetSpec, source: Source, bucket: str, prefix: str, objects: Iterable[dict],
                     stats: ShardStats) -> Iterator[tuple[str, list[ListedObject]]]:
    """(granule id, objects) in key order for one prefix of one source."""
    current, group = None, []
    for obj in objects:
        key = obj["Key"]
        rest = key[len(prefix):]
        if spec.layout == Layout.FOLDER or source == Source.UMMG:
            gid = rest.split("/", 1)[0] if "/" in rest else None
        else:
            gid = granule_id_of(spec.dataset, rest) if "/" not in rest else None
        stats.objects[source.value] = stats.objects.get(source.value, 0) + 1
        if not gid:
            stats.unparsed_count += 1
            if len(stats.unparsed_keys) < 20:
                stats.unparsed_keys.append(f"s3://{bucket}/{key}")
            continue
        if gid != current:
            if current is not None:
                if gid < current:
                    raise RuntimeError(f"keys out of order in s3://{bucket}/{prefix}: {gid} after {current}")
                yield current, group
            current, group = gid, []
        group.append(ListedObject(source.value, bucket, key, int(obj["Size"]), _iso(obj["LastModified"]),
                                  (obj.get("ETag") or "").strip('"') or None))
    if current is not None:
        yield current, group


def list_shard(spec: DatasetSpec, shard_prefix: str, clients, sources: list[Source], stats: ShardStats,
               prefixes: list[str] | None = None) -> Iterator[GranuleListing]:
    """`prefixes` limits the product and browse bucket prefixes listed (default all of the dataset's)."""
    streams = []
    for source in sources:
        bucket, _ = spec.bucket_prefix(source)
        for prefix in spec.prefixes(source):
            if prefixes and source != Source.UMMG and prefix not in prefixes:
                continue
            objects = iter_objects(clients.for_source(spec, source), bucket, prefix + shard_prefix, stats,
                                   source.value)
            streams.append(group_by_granule(spec, source, bucket, prefix, objects, stats))
    # heapq.merge is stable: a granule's files come in stream order
    merged = heapq.merge(*streams, key=lambda item: item[0])
    for gid, items in itertools.groupby(merged, key=lambda item: item[0]):
        files = [obj for _, objs in items for obj in objs]
        stats.granules += 1
        yield GranuleListing(gid, spec.dataset.value, files)


def listed_prefixes(spec: DatasetSpec, requested: list[str] | None = None) -> list[str]:
    """The product and browse bucket prefixes to list, in the dataset's order."""
    available = spec.prefixes(Source.PRODUCTS)
    unknown = sorted(set(requested or []) - set(available))
    if unknown:
        raise ValueError(f"{spec.dataset} has no bucket prefix {unknown}; choose from {available}")
    return [p for p in available if not requested or p in requested]


def default_sources(spec: DatasetSpec, requested: list[str] | None = None) -> list[Source]:
    available = spec.sources()
    if not requested:
        return available
    chosen = [Source(s) for s in requested]
    return [s for s in available if s in chosen]


def select_shards(spec: DatasetSpec, shard_filter: str | None) -> list[str]:
    shards = spec.shards()
    if shard_filter:
        shards = [s for s in shards if fnmatch(shard_name(spec, s), shard_filter)]
    return shards


def _list_one(shard_prefix: str) -> dict:
    """List one shard into its manifest (runs in a worker; reads the run state from parallel.state())."""
    s = state()
    spec, run, clients, sources, prefixes = s["spec"], s["run"], s["clients"], s["sources"], s["prefixes"]
    name = shard_name(spec, shard_prefix)
    path = run.listing(spec.dataset.value, name)
    if is_done(path) and not s["force"]:
        return {"shard": name, "status": "skipped"}
    try:
        stats = ShardStats()
        started = time.monotonic()
        with JsonlGzWriter(path) as writer:
            for granule in list_shard(spec, shard_prefix, clients, sources, stats, prefixes):
                writer.write(granule.to_record())
                if s["limit"] and writer.count >= s["limit"]:
                    break
        stats.seconds = time.monotonic() - started
        # A listing of some of the bucket prefixes is not complete: reconcile skips it, and export from its build
        # writes a delta bundle.
        record = stats.to_dict() | {"shard_prefix": shard_prefix, "sources": [x.value for x in sources],
                                    "prefixes": prefixes,
                                    "complete": not s["limit"] and prefixes == spec.prefixes(Source.PRODUCTS),
                                    "listed_at": run.run_id}
        mark_done(path, record)
    except Exception as err:  # one shard's failure must not stop the others; it is retried on rerun
        logger.error(f"listing {shard_prefix} failed: {type(err).__name__}: {err}")
        return {"shard": name, "status": "failed", "error": f"{shard_prefix}: {type(err).__name__}: {err}"}
    logger.info(f"{spec.dataset} {name}: {record['granules']} granules, {record['objects']} objects, "
                f"{record['seconds']} s")
    return {"shard": name, "status": "listed", "granules": record["granules"]}


def _list_group(shards: list[str]) -> list[dict]:
    return thread_map(_list_one, shards, state()["threads"])


def run_listing(spec: DatasetSpec, run: RunDir, clients, sources: list[Source], workers: int = 16,
                shard_filter: str | None = None, limit: int | None = None, force: bool = False,
                procs: int = 1, prefixes: list[str] | None = None) -> dict:
    """List every selected shard of a dataset into the run's manifests. Returns a summary."""
    shards = select_shards(spec, shard_filter)
    prefixes = listed_prefixes(spec, prefixes)
    write_json(run.plan(spec.dataset.value), {"dataset": spec.dataset.value, "shards": shards,
                                              "sources": [s.value for s in sources], "prefixes": prefixes})
    shard_groups = split(shards, procs)
    shared = {"spec": spec, "run": run, "clients": clients, "sources": sources, "prefixes": prefixes,
              "limit": limit, "force": force, "threads": max(1, workers // max(1, len(shard_groups)))}
    results = [r for group in fork_map(_list_group, shard_groups, shared) for r in group]
    summary = {"dataset": spec.dataset.value, "shards": len(shards), "procs": procs, "prefixes": prefixes,
               "skipped": sum(r["status"] == "skipped" for r in results),
               "listed": sum(r["status"] == "listed" for r in results),
               "failed": sum(r["status"] == "failed" for r in results),
               "granules": sum(r.get("granules", 0) for r in results),
               "errors": [r["error"] for r in results if r["status"] == "failed"]}
    return summary
