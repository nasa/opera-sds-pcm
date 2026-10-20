"""Tier-0 listing of the DAAC buckets into per-shard granule manifests.

A shard is an id prefix (`OPERA_L2_RTC-S1_T001-`). For each source of a dataset (products bucket, browse or
public bucket, ASF UMMG/ tree) the shard's keys are listed recursively and grouped by granule. S3 returns keys in
byte order and no granule id is a prefix of another, so every source yields granules in the same id order and the
sources are merge-joined without holding a shard in memory.
"""
from __future__ import annotations

import heapq
import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Iterable, Iterator

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


def group_by_granule(spec: DatasetSpec, source: Source, bucket: str, objects: Iterable[dict],
                     stats: ShardStats) -> Iterator[tuple[str, list[ListedObject]]]:
    """(granule id, objects) in key order for one source."""
    _, prefix = spec.bucket_prefix(source)
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


def list_shard(spec: DatasetSpec, shard_prefix: str, clients, sources: list[Source],
               stats: ShardStats) -> Iterator[GranuleListing]:
    streams = []
    for order, source in enumerate(sources):
        bucket, prefix = spec.bucket_prefix(source)
        objects = iter_objects(clients.for_source(spec, source), bucket, prefix + shard_prefix, stats, source.value)
        streams.append(((gid, order, objs) for gid, objs in group_by_granule(spec, source, bucket, objects, stats)))
    merged = heapq.merge(*streams, key=lambda item: (item[0], item[1]))
    for gid, items in itertools.groupby(merged, key=lambda item: item[0]):
        files = [obj for _, _, objs in items for obj in objs]
        stats.granules += 1
        yield GranuleListing(gid, spec.dataset.value, files)


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


def run_listing(spec: DatasetSpec, run: RunDir, clients, sources: list[Source], workers: int = 16,
                shard_filter: str | None = None, limit: int | None = None, force: bool = False) -> dict:
    """List every selected shard of a dataset into the run's manifests. Returns a summary."""
    shards = select_shards(spec, shard_filter)
    write_json(run.plan(spec.dataset.value), {"dataset": spec.dataset.value, "shards": shards,
                                              "sources": [s.value for s in sources]})
    summary = {"dataset": spec.dataset.value, "shards": len(shards), "skipped": 0, "listed": 0, "failed": 0,
               "granules": 0, "errors": []}

    def one(shard_prefix: str):
        name = shard_name(spec, shard_prefix)
        path = run.listing(spec.dataset.value, name)
        if is_done(path) and not force:
            return name, "skipped", None
        stats = ShardStats()
        started = time.monotonic()
        with JsonlGzWriter(path) as writer:
            for granule in list_shard(spec, shard_prefix, clients, sources, stats):
                writer.write(granule.to_record())
                if limit and writer.count >= limit:
                    break
        stats.seconds = time.monotonic() - started
        record = stats.to_dict() | {"shard_prefix": shard_prefix, "sources": [s.value for s in sources],
                                    "complete": not limit, "listed_at": run.run_id}
        mark_done(path, record)
        return name, "listed", record

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(one, shard): shard for shard in shards}
        for future in as_completed(futures):
            shard = futures[future]
            try:
                name, status, record = future.result()
            except Exception as err:  # one shard's failure must not stop the others; it is retried on rerun
                summary["failed"] += 1
                summary["errors"].append(f"{shard}: {type(err).__name__}: {err}")
                logger.error(f"listing {shard} failed: {err}")
                continue
            summary[status] += 1
            if record:
                summary["granules"] += record["granules"]
                logger.info(f"{spec.dataset} {name}: {record['granules']} granules, {record['objects']} objects, "
                            f"{record['seconds']} s")
    return summary
