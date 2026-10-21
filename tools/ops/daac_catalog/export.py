"""Write a dataset's catalog to a bundle, from a run's built documents or from the live store."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch
from pathlib import Path

from tools.ops.daac_catalog import store
from tools.ops.daac_catalog.bundle import BundleWriter, bundle_id_for
from tools.ops.daac_catalog.registry import DatasetSpec
from tools.ops.daac_catalog.runs import RunDir, is_done, read_jsonl_gz
from tools.ops.daac_catalog.sources import normalize_ts, fmt_seconds_z

logger = logging.getLogger(__name__)


def export_query(dataset: str, since: str | None = None, ids: set | None = None) -> dict:
    filters = [{"term": {"dataset.keyword": dataset}}]
    if since:
        filters.append({"range": {"catalog.updated_at": {"gte": fmt_seconds_z(normalize_ts(since))}}})
    if ids is not None:
        filters.append({"terms": {"id.keyword": sorted(ids)}})
    return {"bool": {"filter": filters}}


def export_from_store(spec: DatasetSpec, client, writer: BundleWriter, prefix: str = store.CATALOG_PREFIX,
                      since: str | None = None, ids: set | None = None, slices: int = 1) -> int:
    """Scroll the dataset's indices into the bundle. `slices` > 1 runs sliced scrolls in parallel."""
    pattern = store.dataset_pattern(prefix, spec.dataset.value)
    query = export_query(spec.dataset.value, since, ids)
    if slices <= 1:
        for hit in store.scan(client, pattern, query):
            writer.write(hit["_source"])
        return writer.doc_count

    import threading
    lock = threading.Lock()

    def one(slice_id: int):
        from opensearchpy import helpers
        body = {"query": query, "slice": {"id": slice_id, "max": slices}}
        for hit in helpers.scan(client, index=pattern, query=body, size=2000, scroll="10m",
                                ignore_unavailable=True, request_timeout=300):
            with lock:
                writer.write(hit["_source"])

    with ThreadPoolExecutor(max_workers=slices) as pool:
        list(pool.map(one, range(slices)))
    return writer.doc_count


def export_from_run(spec: DatasetSpec, run: RunDir, writer: BundleWriter, shard_filter: str | None = None,
                    ids: set | None = None) -> int:
    for shard in run.docs_shards(spec.dataset.value):
        if shard_filter and not fnmatch(shard, shard_filter):
            continue
        path = run.docs(spec.dataset.value, shard)
        if not is_done(path):
            logger.warning(f"skipping unfinished docs shard {shard}")
            continue
        for doc in read_jsonl_gz(path):
            if ids is None or doc["id"] in ids:
                writer.write(doc)
    return writer.doc_count


def listing_files(run: RunDir, dataset: str) -> list[Path]:
    return [run.listing(dataset, shard) for shard in run.listing_shards(dataset)
            if is_done(run.listing(dataset, shard))]


def new_bundle_dir(bundles_dir: Path, dataset: str, from_build: bool, run_id: str | None) -> Path:
    bundle_id = bundle_id_for(dataset, "build" if from_build else "store", run_id)
    return Path(bundles_dir) / bundle_id
