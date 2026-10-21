"""OpenSearch side: template, bulk loads, lifecycle updates, aliases and scans.

Writes never silently overwrite: loads use op `create` (an existing id is a 409 conflict, counted and listed) unless
`replace` is asked for or the documents come from a delta bundle. Every write stamps `catalog.updated_at`, which is
what `export --since` selects on.
"""
from __future__ import annotations

import json
import logging
import netrc
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urlparse

from tools.ops.daac_catalog.build import index_name
from tools.ops.daac_catalog.sources import fmt_seconds_z

logger = logging.getLogger(__name__)

TEMPLATE_PATH = Path(__file__).with_name("index_template.json")
TEMPLATE_NAME = "catalog"
CATALOG_PREFIX = "catalog_"
GRQ_PREFIX = "grq_"


def make_client(es_url: str, netrc_path: str | None = None, timeout: int = 120, pool_maxsize: int = 16):
    from opensearchpy import OpenSearch

    auth = None
    if netrc_path:
        host = urlparse(es_url).hostname
        entry = netrc.netrc(os.path.expanduser(netrc_path)).authenticators(host)
        if entry:
            auth = (entry[0], entry[2])
    return OpenSearch(hosts=[es_url], http_auth=auth, verify_certs=False, ssl_show_warn=False, timeout=timeout,
                      max_retries=5, retry_on_timeout=True, http_compress=True, pool_maxsize=pool_maxsize)


def load_template(path: Path = TEMPLATE_PATH) -> dict:
    return json.loads(Path(path).read_text())


def install_template(client, path: Path = TEMPLATE_PATH, name: str = TEMPLATE_NAME) -> dict:
    body = load_template(path)
    return client.transport.perform_request("PUT", f"/_index_template/{name}", body=body)


def check_restore_template(client) -> bool:
    """Restore mode relies on the GRQ legacy `grq` template (or a composable one) covering grq_*."""
    for path in ("/_template/grq", "/_index_template/grq"):
        try:
            client.transport.perform_request("GET", path)
            return True
        except Exception:
            continue
    return False


def index_prefix(mode: str, override: str | None = None) -> str:
    if override:
        return override
    return GRQ_PREFIX if mode == "grq" else CATALOG_PREFIX


def stamp(doc: dict, now: datetime | None = None) -> dict:
    doc.setdefault("catalog", {})["updated_at"] = fmt_seconds_z(now or datetime.now(timezone.utc))
    return doc


def bulk_actions(docs: Iterable[dict], prefix: str, op_type: str = "create", now: datetime | None = None,
                 index_for=None, restamp: bool = True) -> Iterator[dict]:
    """Bulk actions with the document under `_source` (the field-level form drops keys that collide with
    action metadata). `index_for(doc)` overrides the computed index. Bundle loads pass `restamp=False`: a copy
    keeps its source's `catalog.updated_at`, so the destination mirrors the source and later deltas line up."""
    for doc in docs:
        if restamp or not (doc.get("catalog") or {}).get("updated_at"):
            stamp(doc, now)
        yield {"_op_type": op_type, "_index": index_for(doc) if index_for else index_name(doc, prefix),
               "_id": doc["id"], "_source": doc}


@dataclass
class BulkResult:
    ok: int = 0
    conflicts: int = 0
    errors: int = 0
    conflict_ids: list = field(default_factory=list)
    error_samples: list = field(default_factory=list)
    indices: dict = field(default_factory=dict)

    def add(self, other: "BulkResult"):
        self.ok += other.ok
        self.conflicts += other.conflicts
        self.errors += other.errors
        self.conflict_ids.extend(other.conflict_ids[:1000 - len(self.conflict_ids)])
        self.error_samples.extend(other.error_samples[:20 - len(self.error_samples)])
        for k, v in other.indices.items():
            self.indices[k] = self.indices.get(k, 0) + v

    def to_dict(self) -> dict:
        return {"ok": self.ok, "conflicts": self.conflicts, "errors": self.errors,
                "conflict_ids": self.conflict_ids[:1000], "error_samples": self.error_samples[:20],
                "indices": self.indices}


RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


def bulk_load(client, actions: Iterable[dict], chunk_size: int = 2000, threads: int = 3,
              max_chunk_bytes: int = 50 * 1024 * 1024, retries: int = 3, backoff_seconds: float = 5.0) -> BulkResult:
    """Bulk-load actions; items rejected with a retryable status (queue full, cluster-event timeouts while new
    indices and mappings are created) are resent up to `retries` times. Conflicts (409) are counted, not retried."""
    import time
    from opensearchpy import helpers

    result = BulkResult()
    pending = actions
    for attempt in range(retries + 1):
        sent: dict[tuple, dict] = {}

        def remember(stream):
            for action in stream:
                sent[(action["_index"], action.get("_id"))] = action
                yield action

        retry = []
        iterator = helpers.parallel_bulk(client, remember(pending), thread_count=max(1, threads),
                                         chunk_size=chunk_size, max_chunk_bytes=max_chunk_bytes,
                                         raise_on_error=False, raise_on_exception=False, request_timeout=300)
        for ok, item in iterator:
            op, info = next(iter(item.items()))
            key = (info.get("_index"), info.get("_id"))
            action = sent.pop(key, None)
            if ok:
                result.ok += 1
                result.indices[info.get("_index")] = result.indices.get(info.get("_index"), 0) + 1
            elif info.get("status") == 409:
                result.conflicts += 1
                if len(result.conflict_ids) < 1000:
                    result.conflict_ids.append(info.get("_id"))
            elif info.get("status") in RETRYABLE_STATUSES and action is not None and attempt < retries:
                retry.append(action)
            else:
                result.errors += 1
                if len(result.error_samples) < 20:
                    result.error_samples.append({"id": info.get("_id"), "status": info.get("status"),
                                                 "error": info.get("error")})
        if not retry:
            break
        logger.warning(f"retrying {len(retry)} bulk items after retryable errors (attempt {attempt + 1})")
        time.sleep(backoff_seconds * (attempt + 1))
        pending = retry
    return result


def refresh(client, indices) -> None:
    """Make writes visible to the next stage (the template refreshes every 5 s; exports and reconciles that
    follow a load in the same pipeline must not miss its last seconds)."""
    names = sorted({i for i in indices if i})
    for start in range(0, len(names), 50):
        try:
            client.indices.refresh(index=",".join(names[start:start + 50]), ignore_unavailable=True)
        except Exception as err:
            logger.warning(f"refresh failed: {err}")


def dataset_pattern(prefix: str, dataset: str) -> str:
    return f"{prefix}*_{dataset.lower()}-*"


def ensure_alias(client, prefix: str, dataset: str) -> str | None:
    """`catalog_<dataset>` alias over the dataset's catalog indices (the `catalog` alias comes from the template)."""
    if prefix != CATALOG_PREFIX:
        return None
    alias = f"catalog_{dataset.lower()}"
    pattern = dataset_pattern(prefix, dataset)
    try:
        client.indices.put_alias(index=pattern, name=alias)
    except Exception as err:  # no index yet for this dataset
        logger.warning(f"alias {alias} on {pattern} not set: {err}")
        return None
    return alias


def scan(client, index: str, query: dict | None = None, source=None, size: int = 2000,
         scroll: str = "10m") -> Iterator[dict]:
    """Scroll through hits (no point-in-time: opensearch-py 3.x breaks it on these clusters)."""
    from opensearchpy import helpers

    body = {"query": query or {"match_all": {}}}
    if source is not None:
        body["_source"] = source
    yield from helpers.scan(client, index=index, query=body, size=size, scroll=scroll, ignore_unavailable=True,
                            preserve_order=False, request_timeout=300)


def find(client, index: str, ids: list[str], source=None) -> dict[str, dict]:
    """Stored hits by id over an index pattern (a document can be in any month's index). Keep `ids` to a few
    hundred per call."""
    if not ids:
        return {}
    body = {"query": {"ids": {"values": list(ids)}}, "size": len(ids)}
    if source is not None:
        body["_source"] = source
    resp = client.search(index=index, body=body, ignore_unavailable=True, request_timeout=300)
    return {hit["_id"]: hit for hit in resp["hits"]["hits"]}


def count(client, index: str, query: dict | None = None) -> int:
    body = {"query": query} if query else None
    try:
        return int(client.count(index=index, body=body, ignore_unavailable=True)["count"])
    except Exception as err:
        logger.warning(f"count on {index} failed: {err}")
        return 0


def counts_by_index(client, index: str) -> dict[str, int]:
    rows = client.cat.indices(index=index, format="json", h="index,docs.count")
    return {r["index"]: int(r["docs.count"] or 0) for r in rows}


def partial_updates(client, updates: Iterable[tuple[str, str, dict]], threads: int = 2) -> BulkResult:
    """(index, id, partial catalog fields) -> update actions; only the given fields are sent."""
    actions = ({"_op_type": "update", "_index": index, "_id": doc_id, "doc": {"catalog": fields}}
               for index, doc_id, fields in updates)
    result = bulk_load(client, actions, threads=threads)
    refresh(client, result.indices)
    return result
