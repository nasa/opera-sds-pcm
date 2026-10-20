"""Compare a complete listing with the store: new granules, deletions at the DAAC, and reappearances.

Work is per listing shard (an id prefix), so memory stays bounded by the largest shard. A stored granule is only
marked deleted when its shard's listing completed in this run. Deletions and reappearances are recorded in
`catalog.lifecycle`; `catalog.last_seen` is advanced only with `touch_last_seen`, because rewriting every document
of a 76-million-granule collection each week costs more than it tells anyone.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from tools.ops.daac_catalog import store
from tools.ops.daac_catalog.registry import DatasetSpec
from tools.ops.daac_catalog.runs import RunDir, read_done, read_jsonl_gz, write_json
from tools.ops.daac_catalog.sources import fmt_seconds_z

logger = logging.getLogger(__name__)


def stored_ids(client, prefix: str, dataset: str, id_prefix: str) -> dict[str, dict]:
    pattern = store.dataset_pattern(prefix, dataset)
    query = {"bool": {"filter": [{"prefix": {"id.keyword": id_prefix}}]}}
    out = {}
    for hit in store.scan(client, pattern, query, source=["catalog.deleted_at", "catalog.lifecycle"]):
        catalog = (hit.get("_source") or {}).get("catalog") or {}
        out[hit["_id"]] = {"index": hit["_index"], "deleted_at": catalog.get("deleted_at"),
                           "lifecycle": catalog.get("lifecycle") or []}
    return out


def reconcile_dataset(spec: DatasetSpec, run: RunDir, client, prefix: str = store.CATALOG_PREFIX,
                      touch_last_seen: bool = False, apply: bool = True,
                      now: datetime | None = None) -> dict:
    dataset = spec.dataset.value
    now = now or datetime.now(timezone.utc)
    run_ts = fmt_seconds_z(run.listed_at)
    stamp = fmt_seconds_z(now)
    out_dir = run.path("reconcile")
    new_ids, deleted_ids, reappeared_ids = [], [], []
    summary = {"dataset": dataset, "run_id": run.run_id, "shards": 0, "incomplete_shards": [], "listed": 0,
               "stored": 0, "new": 0, "deleted": 0, "reappeared": 0, "touched": 0, "applied": apply}
    updates = []
    for shard in run.listing_shards(dataset):
        path = run.listing(dataset, shard)
        done = read_done(path)
        if not done or not done.get("complete", False):
            summary["incomplete_shards"].append(shard)
            continue
        listed = {record["id"] for record in read_jsonl_gz(path)}
        known = stored_ids(client, prefix, dataset, done["shard_prefix"])
        summary["shards"] += 1
        summary["listed"] += len(listed)
        summary["stored"] += len(known)
        new_ids.extend(sorted(listed - set(known)))
        for gid, state in known.items():
            if gid in listed:
                if state["deleted_at"]:
                    reappeared_ids.append(gid)
                    updates.append((state["index"], gid, {
                        "deleted_at": None, "last_seen": run_ts, "updated_at": stamp,
                        "lifecycle": state["lifecycle"] + [{"event": "reappeared", "at": run_ts,
                                                            "run_id": run.run_id}]}))
                elif touch_last_seen:
                    updates.append((state["index"], gid, {"last_seen": run_ts, "updated_at": stamp}))
                    summary["touched"] += 1
            elif not state["deleted_at"]:
                deleted_ids.append(gid)
                updates.append((state["index"], gid, {
                    "deleted_at": run_ts, "updated_at": stamp,
                    "lifecycle": state["lifecycle"] + [{"event": "deleted", "at": run_ts, "run_id": run.run_id}]}))
    summary.update(new=len(new_ids), deleted=len(deleted_ids), reappeared=len(reappeared_ids))
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, ids in (("new_ids", new_ids), ("deleted_ids", deleted_ids), ("reappeared_ids", reappeared_ids)):
        (out_dir / f"{dataset}.{name}.txt").write_text("".join(f"{i}\n" for i in ids))
    if apply and updates:
        summary["update_result"] = store.partial_updates(client, updates).to_dict()
    write_json(out_dir / f"{dataset}.summary.json", summary)
    return summary
