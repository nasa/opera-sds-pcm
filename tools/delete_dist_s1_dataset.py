#!/usr/bin/env python3
"""
delete_dataset.py

Delete DIST-S1 datasets from GRQ (OpenSearch) and S3 given a tile-list file.

For each tile listed (one MGRS tile per line, e.g. ``01WCN``), the tool finds:

* state-config docs in ``grq_*dist_s1*state-config*`` whose
  ``metadata.mgrs_tile_id`` matches the tile (these have no S3 backing); and
* product docs in ``grq_*_l3_dist_s1*`` whose ``metadata.mgrs_tile_id`` matches
  the tile — the S3 prefix is read out of each doc's ``urls`` / ``browse_urls``.

It then deletes the matching S3 prefixes and the ES docs.

Safety:

* dry-run by default; pass ``--execute`` to actually delete
* typed ``DELETE`` confirmation required unless ``--yes``
* a JSONL manifest of every resolved target is always written before any
  destructive call
* hard cap (``--max``, default 10000) on the number of docs to delete unless
  ``--force`` is passed

Connection settings are read from ``~/.sds/config`` when present (the file is
template-rendered on Mozart). They can also be supplied or overridden via
``--es-url``, ``--es-user``, ``--es-password``, and ``--verify-certs``.
S3 deletes rely on standard boto3 credential resolution (instance role on
Mozart; ``AWS_PROFILE`` / ``~/.aws/credentials`` locally).
"""

from __future__ import annotations

import argparse
import json
import logging
import netrc
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urlparse

import boto3
import yaml
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, helpers as os_helpers

LOGGER = logging.getLogger("delete_dataset")

STATE_CONFIG_INDEX = "grq_*dist_s1*state-config*"
PRODUCT_INDEX = "grq_*_l3_dist_s1*"

DEFAULT_HARD_CAP = 10_000
ES_BULK_CHUNK = 1000
S3_DELETE_CHUNK = 1000
ES_SCROLL_TTL = "2m"
ES_SCROLL_PAGE = 1000


@dataclass
class Target:
    index: str
    doc_id: str
    s3_uris: list[str] = field(default_factory=list)

    def to_manifest_row(self) -> dict:
        return {"index": self.index, "_id": self.doc_id, "s3_uris": self.s3_uris}


# ---------------------------------------------------------------------------
# config loading
# ---------------------------------------------------------------------------

def load_sds_config(path: Path) -> dict:
    """Parse ``~/.sds/config`` permissively (skipping un-rendered templates)."""
    if not path.exists():
        return {}
    text = path.read_text()
    # The template form on the dev side has ``${VAR}`` placeholders; on Mozart
    # the file is rendered. Treat any un-rendered ${...} or {{...}} as empty.
    text = re.sub(r"\$\{[^}]+\}", "", text)
    text = re.sub(r"\{\{[^}]+\}\}", "", text)
    try:
        return yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        LOGGER.warning("could not parse %s as YAML: %s", path, e)
        return {}


def _netrc_auth(netrc_path: Path) -> tuple[str, str] | None:
    if not netrc_path.exists():
        return None
    try:
        creds = netrc.netrc(str(netrc_path)).authenticators("default")
    except (netrc.NetrcParseError, OSError) as e:
        LOGGER.warning("could not parse %s: %s", netrc_path, e)
        return None
    if not creds:
        return None
    return creds[0], creds[2]


def _first_scalar(val):
    """Return the first element if ``val`` is a list/tuple, else val itself."""
    if isinstance(val, (list, tuple)):
        return val[0] if val else None
    return val


def build_opensearch_client(args, cfg: dict) -> OpenSearch:
    es_url = args.es_url
    if not es_url:
        protocol = _first_scalar(cfg.get("GRQ_ES_PROTOCOL")) or "http"
        host = (
            _first_scalar(cfg.get("GRQ_ES_PVT_IP"))
            or _first_scalar(cfg.get("GRQ_PVT_IP"))
            or _first_scalar(cfg.get("GRQ_FQDN"))
        )
        port = _first_scalar(cfg.get("GRQ_ES_PORT")) or 9200
        if not host:
            sys.exit(
                "error: ES host not resolved — pass --es-url or ensure "
                "GRQ_ES_PVT_IP / GRQ_PVT_IP is set in ~/.sds/config"
            )
        es_url = f"{protocol}://{host}:{port}"

    parsed = urlparse(es_url)
    use_ssl = parsed.scheme == "https"

    http_auth: tuple[str, str] | None = None
    if args.es_user and args.es_password:
        http_auth = (args.es_user, args.es_password)
    elif use_ssl:
        http_auth = _netrc_auth(Path("~/.netrc-os").expanduser())

    client = OpenSearch(
        hosts=[es_url],
        http_compress=True,
        http_auth=http_auth,
        use_ssl=use_ssl,
        verify_certs=args.verify_certs,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        timeout=60,
    )
    client.info()  # fail fast on bad config
    LOGGER.info("connected to OpenSearch at %s (ssl=%s)", es_url, use_ssl)
    return client


# ---------------------------------------------------------------------------
# tile input
# ---------------------------------------------------------------------------

TILE_RE = re.compile(r"^[0-9]{1,2}[A-Za-z]{3}$")


def read_tiles(path: Path) -> list[str]:
    tiles: list[str] = []
    seen: set[str] = set()
    bad: list[str] = []
    for line_no, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        tile = line.upper().removeprefix("T")
        if not TILE_RE.match(tile):
            bad.append(f"{path}:{line_no}: {raw!r}")
            continue
        if tile in seen:
            continue
        seen.add(tile)
        tiles.append(tile)
    if bad:
        for msg in bad[:5]:
            LOGGER.warning("ignoring non-MGRS line: %s", msg)
        if len(bad) > 5:
            LOGGER.warning("...and %d more", len(bad) - 5)
    LOGGER.info("loaded %d unique tiles from %s", len(tiles), path)
    return tiles


# ---------------------------------------------------------------------------
# ES search
# ---------------------------------------------------------------------------

def _chunked(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _scroll_search(es: OpenSearch, index: str, body: dict) -> Iterator[dict]:
    body = {**body, "size": ES_SCROLL_PAGE}
    page = es.search(index=index, body=body, scroll=ES_SCROLL_TTL, ignore_unavailable=True)
    scroll_id = page.get("_scroll_id")
    try:
        while True:
            hits = page.get("hits", {}).get("hits", [])
            if not hits:
                return
            yield from hits
            page = es.scroll(scroll_id=scroll_id, scroll=ES_SCROLL_TTL)
            new_id = page.get("_scroll_id")
            if new_id and new_id != scroll_id:
                # clear superseded scroll context to avoid leaks
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass
                scroll_id = new_id
    finally:
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


# Older docs only carry ``metadata.tile_id``; newer ones add ``metadata.mgrs_tile_id``.
# Match either so we don't miss historical docs.
TILE_FIELDS = ("metadata.mgrs_tile_id.keyword", "metadata.tile_id.keyword")


def _tile_terms_query(tiles: list[str]) -> dict:
    return {
        "bool": {
            "should": [{"terms": {f: tiles}} for f in TILE_FIELDS],
            "minimum_should_match": 1,
        }
    }


def find_state_config_targets(es: OpenSearch, tiles: list[str]) -> list[Target]:
    """state-configs: ES-only; key off any of ``TILE_FIELDS``."""
    targets: list[Target] = []
    seen: set[tuple[str, str]] = set()
    for tile_chunk in _chunked(tiles, 1024):
        body = {"query": _tile_terms_query(tile_chunk), "_source": False}
        for hit in _scroll_search(es, STATE_CONFIG_INDEX, body):
            key = (hit["_index"], hit["_id"])
            if key in seen:
                continue
            seen.add(key)
            targets.append(Target(index=hit["_index"], doc_id=hit["_id"]))
    LOGGER.info("found %d state-config docs", len(targets))
    return targets


def find_product_targets(es: OpenSearch, tiles: list[str]) -> list[Target]:
    """products: key off any of ``TILE_FIELDS``; pull S3 prefixes from
    ``urls`` / ``browse_urls``."""
    targets: list[Target] = []
    seen: set[tuple[str, str]] = set()
    for tile_chunk in _chunked(tiles, 1024):
        body = {
            "query": _tile_terms_query(tile_chunk),
            "_source": ["urls", "browse_urls"],
        }
        for hit in _scroll_search(es, PRODUCT_INDEX, body):
            key = (hit["_index"], hit["_id"])
            if key in seen:
                continue
            seen.add(key)
            src = hit.get("_source", {}) or {}
            uris = _extract_s3_uris(src.get("urls", [])) + _extract_s3_uris(
                src.get("browse_urls", [])
            )
            targets.append(
                Target(index=hit["_index"], doc_id=hit["_id"], s3_uris=uris)
            )
    LOGGER.info("found %d product docs", len(targets))
    return targets


# ---------------------------------------------------------------------------
# S3 URL normalization + delete
# ---------------------------------------------------------------------------

_S3_ENDPOINT_HOST_RE = re.compile(r"^s3[.-][a-z0-9-]+\.amazonaws\.com$")


def normalize_s3_uri(url: str) -> tuple[str, str] | None:
    """Convert an OPERA-style URL to ``(bucket, key_prefix)`` or return None.

    Handles:
      * ``s3://<bucket>/<key>``
      * ``s3://s3-us-west-2.amazonaws.com:80/<bucket>/<key>``  (endpoint-style)
      * ``http(s)://<bucket>.s3-website-...amazonaws.com/<key>``
    """
    if not url or not isinstance(url, str):
        return None
    parsed = urlparse(url)
    if parsed.scheme == "s3":
        host = parsed.hostname or ""
        path = parsed.path.lstrip("/")
        if _S3_ENDPOINT_HOST_RE.match(host):
            # endpoint-style: first path segment is the bucket
            if "/" not in path:
                return None
            bucket, _, key = path.partition("/")
            return bucket, key
        # plain s3://bucket/key
        if not host:
            return None
        return host, path
    if parsed.scheme in {"http", "https"}:
        host = parsed.hostname or ""
        # website endpoint: <bucket>.s3-website-<region>.amazonaws.com or
        # <bucket>.s3.<region>.amazonaws.com
        m = re.match(r"^(?P<bucket>[^.]+)\.s3[.-][a-z0-9.-]+amazonaws\.com$", host)
        if m:
            return m.group("bucket"), parsed.path.lstrip("/")
    return None


def _extract_s3_uris(urls: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for u in urls or []:
        norm = normalize_s3_uri(u)
        if not norm:
            continue
        canonical = f"s3://{norm[0]}/{norm[1]}"
        if canonical in seen:
            continue
        seen.add(canonical)
        out.append(canonical)
    return out


def delete_s3_prefix(s3_client, bucket: str, key_prefix: str, *, dry_run: bool) -> tuple[int, int]:
    """Delete every object under ``key_prefix``. Returns (deleted, errors)."""
    if not key_prefix:
        LOGGER.warning("refusing to delete empty key prefix in bucket %s", bucket)
        return 0, 1

    deleted = 0
    errors = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=key_prefix)
    batch: list[dict] = []
    try:
        for page in pages:
            for obj in page.get("Contents", []) or []:
                batch.append({"Key": obj["Key"]})
                if len(batch) >= S3_DELETE_CHUNK:
                    deleted_n, errors_n = _flush_s3_batch(
                        s3_client, bucket, batch, dry_run=dry_run
                    )
                    deleted += deleted_n
                    errors += errors_n
                    batch = []
        if batch:
            deleted_n, errors_n = _flush_s3_batch(s3_client, bucket, batch, dry_run=dry_run)
            deleted += deleted_n
            errors += errors_n
    except ClientError as e:
        LOGGER.error("S3 list failed for s3://%s/%s: %s", bucket, key_prefix, e)
        errors += 1
    return deleted, errors


def _flush_s3_batch(s3_client, bucket: str, batch: list[dict], *, dry_run: bool) -> tuple[int, int]:
    if dry_run:
        return len(batch), 0
    try:
        resp = s3_client.delete_objects(
            Bucket=bucket, Delete={"Objects": batch, "Quiet": True}
        )
    except ClientError as e:
        LOGGER.error("delete_objects failed on s3://%s (n=%d): %s", bucket, len(batch), e)
        return 0, len(batch)
    errs = resp.get("Errors", []) or []
    for err in errs[:5]:
        LOGGER.error(
            "s3 delete error: bucket=%s key=%s code=%s message=%s",
            bucket, err.get("Key"), err.get("Code"), err.get("Message"),
        )
    if len(errs) > 5:
        LOGGER.error("...and %d more s3 delete errors", len(errs) - 5)
    return len(batch) - len(errs), len(errs)


# ---------------------------------------------------------------------------
# ES delete
# ---------------------------------------------------------------------------

def delete_es_docs(es: OpenSearch, targets: list[Target], *, dry_run: bool) -> tuple[int, int]:
    """Delete docs in chunks per index using delete_by_query terms ``_id``."""
    if dry_run:
        return len(targets), 0
    deleted = 0
    errors = 0
    # group by physical index so each delete_by_query is scoped narrowly
    by_index: dict[str, list[str]] = {}
    for t in targets:
        by_index.setdefault(t.index, []).append(t.doc_id)

    for index, ids in by_index.items():
        for chunk in _chunked(ids, ES_BULK_CHUNK):
            body = {"query": {"terms": {"_id": chunk}}}
            try:
                resp = es.delete_by_query(
                    index=index,
                    body=body,
                    refresh=True,
                    wait_for_completion=True,
                    conflicts="proceed",
                )
            except Exception as e:
                LOGGER.error("delete_by_query failed on %s (n=%d): %s", index, len(chunk), e)
                errors += len(chunk)
                continue
            deleted += int(resp.get("deleted", 0))
            failures = resp.get("failures", []) or []
            for f in failures[:5]:
                LOGGER.error("ES delete failure in %s: %s", index, f)
            if len(failures) > 5:
                LOGGER.error("...and %d more ES delete failures in %s", len(failures) - 5, index)
            errors += len(failures)
    return deleted, errors


# ---------------------------------------------------------------------------
# logging / manifest
# ---------------------------------------------------------------------------

def setup_logging(log_file: Path | None, verbose: bool) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    LOGGER.setLevel(logging.DEBUG if verbose else logging.INFO)
    # console
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    LOGGER.addHandler(sh)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = TimedRotatingFileHandler(log_file, when="midnight", backupCount=14)
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        LOGGER.addHandler(fh)


def write_manifest(path: Path, targets: list[Target]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for t in targets:
            f.write(json.dumps(t.to_manifest_row()) + "\n")
    LOGGER.info("wrote manifest of %d targets to %s", len(targets), path)


def confirm(prompt: str, expected: str) -> bool:
    try:
        answer = input(prompt).strip()
    except EOFError:
        return False
    return answer == expected


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--tile-file", required=True, type=Path,
        help="text file with one MGRS tile per line (no leading T), comments with # ignored",
    )
    p.add_argument(
        "--execute", action="store_true",
        help="actually delete (otherwise dry-run: list + manifest only)",
    )
    p.add_argument(
        "--yes", action="store_true",
        help="skip the typed-DELETE confirmation prompt (still subject to --max)",
    )
    p.add_argument(
        "--max", dest="max_targets", type=int, default=DEFAULT_HARD_CAP,
        help=f"refuse to delete more than this many docs unless --force (default: {DEFAULT_HARD_CAP})",
    )
    p.add_argument("--force", action="store_true", help="override --max hard cap")
    default_manifest = (
        Path.cwd()
        / f"delete_dataset-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.jsonl"
    )
    p.add_argument(
        "--manifest", type=Path, default=default_manifest,
        help="path to JSONL manifest (default: ./delete_dataset-<ts>.jsonl in cwd)",
    )
    p.add_argument("--log-file", type=Path, help="write a rolling audit log here")
    p.add_argument("--workers", type=int, default=8, help="parallel S3 prefix deletes")

    p.add_argument("--config", type=Path, default=Path("~/.sds/config").expanduser(),
                   help="path to .sds/config to source ES + bucket settings from")
    p.add_argument("--es-url", help="OpenSearch URL (overrides config)")
    p.add_argument("--es-user", help="OpenSearch basic-auth user")
    p.add_argument("--es-password", help="OpenSearch basic-auth password")
    p.add_argument(
        "--verify-certs", action="store_true",
        help="verify TLS certs (off by default to match existing OPERA tools that use self-signed certs)",
    )
    p.add_argument(
        "--state-config-only", action="store_true",
        help="skip product index — only purge state-config docs",
    )
    p.add_argument(
        "--products-only", action="store_true",
        help="skip state-config index — only purge product docs (and their S3)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    setup_logging(args.log_file, args.verbose)

    if args.state_config_only and args.products_only:
        LOGGER.error("--state-config-only and --products-only are mutually exclusive")
        return 2

    cfg = load_sds_config(args.config) if args.config else {}
    es = build_opensearch_client(args, cfg)

    tiles = read_tiles(args.tile_file)
    if not tiles:
        LOGGER.error("no valid tiles in %s; aborting", args.tile_file)
        return 2

    targets: list[Target] = []
    if not args.products_only:
        targets.extend(find_state_config_targets(es, tiles))
    if not args.state_config_only:
        targets.extend(find_product_targets(es, tiles))

    if not targets:
        LOGGER.info("no matching docs found — nothing to do")
        # still write an (empty) manifest so the run is auditable
        write_manifest(args.manifest, targets)
        return 0

    # summary
    n_state = sum(1 for t in targets if not t.s3_uris)
    n_prod = len(targets) - n_state
    n_s3 = sum(len(t.s3_uris) for t in targets)
    LOGGER.info(
        "resolved %d targets across %d tiles (%d state-config, %d product, %d s3 prefixes)",
        len(targets), len(tiles), n_state, n_prod, n_s3,
    )
    sample = targets[:5]
    for t in sample:
        LOGGER.info("  sample: %s/%s  s3=%s", t.index, t.doc_id, t.s3_uris[:1])

    # always write the manifest before any destructive call
    write_manifest(args.manifest, targets)

    # hard cap
    if len(targets) > args.max_targets and not args.force:
        LOGGER.error(
            "target count %d exceeds --max %d; re-run with --force to override",
            len(targets), args.max_targets,
        )
        return 2

    if not args.execute:
        LOGGER.info("dry-run complete; pass --execute to perform deletes")
        return 0

    if not args.yes:
        prompt = (
            f"\nAbout to delete {len(targets)} docs ({n_s3} S3 prefixes) across "
            f"{len(tiles)} tiles.\nType 'DELETE' to proceed (anything else aborts): "
        )
        if not confirm(prompt, "DELETE"):
            LOGGER.error("confirmation not received; aborting")
            return 2

    t0 = time.time()
    # 1. S3 prefixes first — if this fails partway, we still have the manifest
    s3_client = boto3.client("s3")
    s3_deleted = 0
    s3_errors = 0
    prefix_jobs: list[tuple[str, str, str]] = []  # (doc_id, bucket, key_prefix)
    for t in targets:
        for uri in t.s3_uris:
            norm = normalize_s3_uri(uri)
            if not norm:
                LOGGER.warning("could not normalize %s for %s; skipping", uri, t.doc_id)
                s3_errors += 1
                continue
            prefix_jobs.append((t.doc_id, norm[0], norm[1]))

    if prefix_jobs:
        LOGGER.info("deleting %d S3 prefixes with %d workers", len(prefix_jobs), args.workers)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futs = {
                pool.submit(delete_s3_prefix, s3_client, bucket, key, dry_run=False): (doc_id, bucket, key)
                for doc_id, bucket, key in prefix_jobs
            }
            for fut in as_completed(futs):
                doc_id, bucket, key = futs[fut]
                try:
                    deleted, errors = fut.result()
                except Exception as e:  # pragma: no cover - defensive
                    LOGGER.error("s3 delete crashed for %s s3://%s/%s: %s", doc_id, bucket, key, e)
                    s3_errors += 1
                    continue
                s3_deleted += deleted
                s3_errors += errors

    # 2. ES docs after S3 — keeps ES from claiming we still have files we deleted
    es_deleted, es_errors = delete_es_docs(es, targets, dry_run=False)

    elapsed = time.time() - t0
    LOGGER.info(
        "done in %.1fs: s3 objects deleted=%d errors=%d; es docs deleted=%d errors=%d",
        elapsed, s3_deleted, s3_errors, es_deleted, es_errors,
    )
    return 0 if (s3_errors == 0 and es_errors == 0) else 2


if __name__ == "__main__":
    sys.exit(main())
