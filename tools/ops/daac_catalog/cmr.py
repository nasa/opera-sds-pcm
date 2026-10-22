"""Optional CMR check: which catalogued granules CMR publishes, recorded in `catalog.cmr`.

The catalog counts a granule as delivered when the DAAC archive holds it. At ASF every archived granule also has its
UMM-G record, but PO.DAAC keeps none where the catalog can read it, so its archive cannot show whether a product
reached CMR: in October 2026, 4,873 DSWx-HLS products were in PO.DAAC's archive after "Rate Exceeded" CNM-R errors
but not in CMR. This stage lists the collection in CMR one listing shard (granule UR prefix) at a time, joins the
result to that shard's documents and records:

* in CMR: `catalog.cmr = {concept_id, revision_id, checked_at}`, plus `daac_catalog_id` and `daac_catalog_url` as
  the CNM-R response gives them (they leave `catalog.omitted`);
* not in CMR: `catalog.cmr = {checked_at}`, without a concept id;
* not checked: no `catalog.cmr`. A granule missing from CMR whose newest archived file is younger than the grace
  period keeps its previous state, since CMR ingest follows the archive.

`checked_at` is when the CMR listing that set the current state started. A document is rewritten, and gets a new
`catalog.updated_at`, only when its state changes, so later checks add only the changes to a delta bundle.

The listing uses CMR's references format (`granules.xml`: granule UR, concept id and revision id, a few hundred
bytes per granule). A shard's listing is kept only when its items add up to CMR-Hits.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Iterator
from xml.etree import ElementTree

import requests

from tools.ops.daac_catalog import store
from tools.ops.daac_catalog.parallel import fork_map, split, state
from tools.ops.daac_catalog.parse import natural_key
from tools.ops.daac_catalog.registry import Daac, DatasetSpec, id_pattern
from tools.ops.daac_catalog.runs import (
    JsonlGzWriter, RunDir, is_done, mark_done, read_done, read_jsonl_gz, shard_name, write_json)
from tools.ops.daac_catalog.sources import fmt_seconds_z, normalize_ts

logger = logging.getLogger(__name__)

CMR_URL = "https://cmr.earthdata.nasa.gov"
PROVIDERS = {Daac.ASF: "ASF", Daac.PODAAC: "POCLOUD"}
PAGE_SIZE = 2000
RETRY_STATUSES = {429, 500, 502, 503, 504}
CMR_FIELDS = ("daac_catalog_id", "daac_catalog_url")
CLIENT_ID = "opera-sds-pcm-daac-catalog"


class CmrError(Exception):
    pass


@dataclass
class CmrOptions:
    source: str = "store"             # "store": update stored documents; "build": rewrite the run's built documents
    url: str = CMR_URL
    grace: timedelta = timedelta(hours=24)
    procs: int = 1
    shard_filter: str | None = None
    relist: bool = False              # list CMR again for shards this run listed before
    force: bool = False               # check shards this run checked before again
    prefix: str = store.CATALOG_PREFIX
    attempts: int = 3                 # listings of a shard whose pages do not add up to CMR-Hits


def catalog_url(base: str, concept_id: str) -> str:
    return f"{base.rstrip('/')}/search/concepts/{concept_id}.umm_json"


# ---- documents --------------------------------------------------------------------------------------------------

def set_cmr(doc: dict, entry: dict | None, checked_at: str, base_url: str = CMR_URL) -> bool:
    """Record a granule's CMR state (`entry` is its CMR listing row, None when CMR does not have it). True when the
    document changed; the same concept and revision as before leave it untouched, `checked_at` included."""
    catalog = doc["catalog"]
    new = {"concept_id": entry["concept_id"], "revision_id": entry["revision_id"]} if entry else {}
    old = catalog.get("cmr")
    if old is not None and {k: old[k] for k in ("concept_id", "revision_id") if k in old} == new:
        return False
    catalog["cmr"] = new | {"checked_at": checked_at}
    omitted = set(catalog.get("omitted") or [])
    if entry:
        doc["daac_catalog_id"] = entry["concept_id"]
        doc["daac_catalog_url"] = catalog_url(base_url, entry["concept_id"])
        omitted -= set(CMR_FIELDS)
    else:
        for key in CMR_FIELDS:
            doc.pop(key, None)
        omitted |= set(CMR_FIELDS)
    catalog["omitted"] = sorted(omitted)
    return True


def carry_cmr(old: dict, doc: dict):
    """Keep a stored document's CMR state on its rebuild (`enrich`, a changed granule's replacement)."""
    cmr = (old.get("catalog") or {}).get("cmr")
    if not cmr:
        return
    entry = {"concept_id": cmr["concept_id"], "revision_id": cmr.get("revision_id")} if cmr.get("concept_id") else None
    set_cmr(doc, entry, cmr.get("checked_at"))
    if entry and old.get("daac_catalog_url"):
        doc["daac_catalog_url"] = old["daac_catalog_url"]


def newest_file(doc: dict) -> datetime | None:
    times = [normalize_ts(f.get("last_modified")) for f in (doc.get("catalog") or {}).get("files") or []]
    times = [t for t in times if t]
    return max(times) if times else None


def check_doc(doc: dict, entry: dict | None, listed_at: datetime, grace: timedelta,
              base_url: str = CMR_URL) -> tuple[str, bool]:
    """(state, changed) for one document: state is `found`, `missing`, or `recent` (missing from CMR but archived
    within the grace period before the listing, left as it was)."""
    if entry is None:
        newest = newest_file(doc)
        if newest and newest > listed_at - grace:
            return "recent", False
    return ("found" if entry else "missing"), set_cmr(doc, entry, fmt_seconds_z(listed_at), base_url)


# ---- CMR listing ------------------------------------------------------------------------------------------------

def parse_references(body: bytes) -> tuple[int, list[dict]]:
    root = ElementTree.fromstring(body)
    hits = int(root.findtext("hits"))
    items = [{"id": ref.findtext("name"), "concept_id": ref.findtext("id"),
              "revision_id": int(ref.findtext("revision-id"))} for ref in root.iter("reference")]
    return hits, items


def _get(session, url: str, params: dict, headers: dict, retries: int = 5):
    reason = ""
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=120)
            if resp.status_code not in RETRY_STATUSES:
                resp.raise_for_status()
                return resp
            reason = f"HTTP {resp.status_code}"
        except (requests.ConnectionError, requests.Timeout) as err:
            reason = f"{type(err).__name__}: {err}"
        if attempt < retries:
            time.sleep(min(60, 2 * 2 ** attempt))
    raise CmrError(f"CMR request failed after {retries + 1} attempts: {reason}")


def list_prefix(session, spec: DatasetSpec, prefix: str, url: str = CMR_URL, page_size: int = PAGE_SIZE) -> list[dict]:
    """Every granule of the collection in CMR whose granule UR starts with `prefix`. Raises CmrError when CMR-Hits
    moves while paging or the pages do not add up to it."""
    params = {"short_name": spec.short_name, "provider": PROVIDERS[spec.daac], "granule_ur[]": f"{prefix}*",
              "options[granule_ur][pattern]": "true", "page_size": page_size}
    items, hits, search_after = [], None, None
    while True:
        headers = {"Client-Id": CLIENT_ID} | ({"CMR-Search-After": search_after} if search_after else {})
        resp = _get(session, f"{url.rstrip('/')}/search/granules.xml", params, headers)
        page_hits, page = parse_references(resp.content)
        if hits is not None and page_hits != hits:
            raise CmrError(f"{prefix}: CMR-Hits moved from {hits} to {page_hits} while paging")
        hits = page_hits
        items.extend(page)
        search_after = resp.headers.get("CMR-Search-After")
        if not page or len(items) >= hits or not search_after:
            break
    if len(items) != hits or len({i["id"] for i in items}) != len(items):
        raise CmrError(f"{prefix}: {len(items)} items ({len({i['id'] for i in items})} distinct) for CMR-Hits {hits}")
    return items


def shard_listing(spec: DatasetSpec, run: RunDir, shard: str, prefix: str, session,
                  options: CmrOptions) -> tuple[list[dict], datetime]:
    """The shard's CMR listing and when it started: this run's earlier listing unless `relist`."""
    path = run.cmr_listing(spec.dataset.value, shard)
    done = read_done(path)
    if done and not options.relist:
        return list(read_jsonl_gz(path)), normalize_ts(done["listed_at"])
    for attempt in range(1, options.attempts + 1):
        listed_at = datetime.now(timezone.utc).replace(microsecond=0)
        try:
            items = list_prefix(session, spec, prefix, options.url)
            break
        except CmrError as err:
            if attempt == options.attempts:
                raise
            logger.warning(f"{spec.dataset} {shard}: {err}; listing again")
    with JsonlGzWriter(path) as writer:
        for item in items:
            writer.write(item)
    mark_done(path, {"items": len(items), "listed_at": fmt_seconds_z(listed_at), "prefix": prefix})
    return items, listed_at


# ---- shards -----------------------------------------------------------------------------------------------------

def _stored_docs(client, spec: DatasetSpec, prefix: str, options: CmrOptions) -> Iterator[tuple[dict, str]]:
    query = {"bool": {"filter": [{"term": {"dataset.keyword": spec.dataset.value}},
                                 {"prefix": {"id.keyword": prefix}}]}}
    for hit in store.scan(client, store.dataset_pattern(options.prefix, spec.dataset.value), query):
        yield hit["_source"], hit["_index"]


def check_shard(spec: DatasetSpec, run: RunDir, shard: str, prefix: str, session, client,
                options: CmrOptions) -> dict:
    dataset = spec.dataset.value
    result_path = run.cmr_result(dataset, shard, options.source)
    if is_done(result_path) and not options.force:
        return read_done(result_path) | {"status": "skipped"}
    docs_path = run.docs(dataset, shard)
    if options.source == "build" and not is_done(docs_path):
        raise CmrError(f"{shard}: no finished build output in {run.run_id}")
    started = time.monotonic()
    items, listed_at = shard_listing(spec, run, shard, prefix, session, options)
    by_id = {item["id"]: item for item in items}
    pattern = id_pattern(spec.dataset)
    cmr_keys, versions = set(), {}
    for item in items:
        match = pattern.fullmatch(item["id"])
        if match:
            groups = {k: v for k, v in match.groupdict().items() if k != "id" and v}
            cmr_keys.add(natural_key(spec, groups))
            versions[item["id"]] = groups.get("product_version") or "none"
    counts = {"docs": 0, "found": 0, "missing": 0, "recent": 0, "changed": 0}
    missing, seen = [], set()

    def checked(docs):
        for doc, where in docs:
            state_, changed = check_doc(doc, by_id.get(doc["id"]), listed_at, options.grace, options.url)
            counts["docs"] += 1
            counts[state_] += 1
            counts["changed"] += changed
            seen.add(doc["id"])
            if state_ == "missing":
                missing.append({"id": doc["id"], "other_version_in_cmr": doc["catalog"]["natural_key"] in cmr_keys})
            yield doc, where, changed

    if options.source == "build":
        with JsonlGzWriter(docs_path) as writer:
            for doc, _, _ in checked((d, None) for d in read_jsonl_gz(docs_path)):
                writer.write(doc)
        bulk = None
    else:
        actions = ({"_op_type": "index", "_index": where, "_id": doc["id"], "_source": store.stamp(doc)}
                   for doc, where, changed in checked(_stored_docs(client, spec, prefix, options)) if changed)
        bulk = store.bulk_load(client, actions, threads=2)
        store.refresh(client, bulk.indices)
        if bulk.errors:
            raise CmrError(f"{shard}: {bulk.errors} documents not updated: {bulk.error_samples[:3]}")
    cmr_only = sorted(set(by_id) - seen)
    by_version = {}
    for gid in cmr_only:
        by_version[versions.get(gid, "unparsed")] = by_version.get(versions.get(gid, "unparsed"), 0) + 1
    result = counts | {"shard": shard, "listed_at": fmt_seconds_z(listed_at), "cmr_items": len(items),
                       "cmr_only": len(cmr_only), "cmr_only_by_version": by_version, "cmr_only_sample": cmr_only[:20],
                       "missing_ids": missing, "seconds": round(time.monotonic() - started, 2)}
    write_json(result_path, result)
    mark_done(result_path, result)
    logger.info(f"{dataset} {shard}: {counts['docs']} docs, {counts['found']} in CMR, {counts['missing']} not, "
                f"{counts['recent']} recent, {counts['changed']} changed, {len(cmr_only)} only in CMR")
    return result | {"status": "checked"}


def _check_group(shards: list[tuple[str, str]]) -> list[dict]:
    s = state()
    spec, run, options = s["spec"], s["run"], s["options"]
    client = s["client_factory"]() if options.source == "store" else None
    out = []
    with requests.Session() as session:
        for shard, prefix in shards:
            try:
                out.append(check_shard(spec, run, shard, prefix, session, client, options))
            except Exception as err:  # the shard stays unchecked and is retried on rerun
                logger.error(f"cmr {shard} failed: {type(err).__name__}: {err}")
                out.append({"shard": shard, "status": "failed", "error": f"{shard}: {type(err).__name__}: {err}"})
    return out


def run_cmr(spec: DatasetSpec, run: RunDir, options: CmrOptions, client=None, client_factory=None) -> dict:
    dataset = spec.dataset.value
    started = time.monotonic()
    shards = [(shard_name(spec, p), p) for p in spec.shards()]
    shards = [(s, p) for s, p in shards if not options.shard_filter or fnmatch(s, options.shard_filter)]
    procs = max(1, options.procs)
    shared = {"spec": spec, "run": run, "options": options,
              "client_factory": client_factory if procs > 1 and client_factory else (lambda: client)}
    results = [r for group in fork_map(_check_group, split(shards, procs), shared) for r in group]
    done = [r for r in results if r["status"] != "failed"]
    missing = [m for r in done for m in r["missing_ids"]]
    summary = {"dataset": dataset, "source": options.source, "shards": sum(r["status"] == "checked" for r in results),
               "skipped": sum(r["status"] == "skipped" for r in results),
               "failed": sum(r["status"] == "failed" for r in results),
               "errors": [r["error"] for r in results if r["status"] == "failed"]}
    for key in ("docs", "found", "missing", "recent", "changed", "cmr_items", "cmr_only"):
        summary[key] = sum(r[key] for r in done)
    summary["missing_other_version_in_cmr"] = sum(m["other_version_in_cmr"] for m in missing)
    summary["cmr_only_by_version"] = {}
    for r in done:
        for version, n in (r.get("cmr_only_by_version") or {}).items():
            summary["cmr_only_by_version"][version] = summary["cmr_only_by_version"].get(version, 0) + n
    listed = sorted(r["listed_at"] for r in done)
    summary["listed"] = [listed[0], listed[-1]] if listed else None
    summary["seconds"] = round(time.monotonic() - started, 1)
    write_json(run.cmr_summary(dataset, options.source), summary)
    with open(run.cmr_missing(dataset, options.source), "w") as fh:
        fh.writelines(f"{m['id']}\t{'other_version_in_cmr' if m['other_version_in_cmr'] else 'no_other_version'}\n"
                      for m in sorted(missing, key=lambda m: m["id"]))
    return summary
