"""Run directories and the file hand-off between stages.

```
<workdir>/<run-id>/
  plan/<dataset>.json                   shard list and estimates
  listing/<dataset>/<shard>.jsonl.gz    tier-0 manifest, one granule per line, + <shard>._DONE
  docs/<dataset>/<shard>.jsonl.gz       core documents, + <shard>._DONE, <shard>.rejects.jsonl
  index/<family>/<dataset>/<shard>.result.json  bulk results per index family (catalog, grq, ...)
  reconcile/<dataset>.*                 reconcile outputs: summary.json, new_ids.txt, changed_ids.txt, ...
  cmr/listing/<dataset>/<shard>.jsonl.gz  CMR granules per shard (optional check), + <shard>._DONE
  cmr/<store|build>/<dataset>/<shard>.result.json  CMR check per shard; cmr/<dataset>.<store|build>.summary.json
  bundles/<bundle-id>/                  transfer bundles
  reports/*.md
  logs/<command>-<dataset>.log
```
Every stage writes `<name>.part` first and renames on success, then writes the `_DONE` marker, so a rerun skips
finished shards and never reads a half-written file.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from tools.ops.daac_catalog.registry import DatasetSpec

RUN_ID_RE = re.compile(r"^inv-\d{8}T\d{4}Z$")


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"inv-{now:%Y%m%dT%H%MZ}"


def run_time(run_id: str) -> datetime:
    return datetime.strptime(run_id, "inv-%Y%m%dT%H%MZ").replace(tzinfo=timezone.utc)


def latest_run_id(workdir: Path) -> str | None:
    runs = sorted(p.name for p in Path(workdir).glob("inv-*") if p.is_dir() and RUN_ID_RE.match(p.name))
    return runs[-1] if runs else None


def shard_name(spec: DatasetSpec, shard_prefix: str) -> str:
    name = shard_prefix[len(spec.id_stem):] if shard_prefix.startswith(spec.id_stem) else shard_prefix
    return re.sub(r"[^A-Za-z0-9._-]", "_", name) or "all"


@dataclass(frozen=True)
class RunDir:
    workdir: Path
    run_id: str

    @property
    def root(self) -> Path:
        return Path(self.workdir) / self.run_id

    @property
    def listed_at(self) -> datetime:
        return run_time(self.run_id)

    def path(self, *parts: str) -> Path:
        return self.root.joinpath(*parts)

    def plan(self, dataset: str) -> Path:
        return self.path("plan", f"{dataset}.json")

    def listing(self, dataset: str, shard: str) -> Path:
        return self.path("listing", dataset, f"{shard}.jsonl.gz")

    def docs(self, dataset: str, shard: str) -> Path:
        return self.path("docs", dataset, f"{shard}.jsonl.gz")

    def rejects(self, dataset: str, shard: str) -> Path:
        return self.path("docs", dataset, f"{shard}.rejects.jsonl")

    def index_result(self, dataset: str, shard: str, prefix: str = "catalog_") -> Path:
        """Per index family, so a restore into grq_* is tracked apart from the catalog_* load."""
        family = prefix.rstrip("_") or "default"
        return self.path("index", family, dataset, f"{shard}.result.json")

    def reports(self) -> Path:
        return self.path("reports")

    def logs(self) -> Path:
        return self.path("logs")

    def bundles(self) -> Path:
        return self.path("bundles")

    def reconcile_ids(self, dataset: str, kind: str) -> Path:
        """`kind` is new, changed, deleted or reappeared."""
        return self.path("reconcile", f"{dataset}.{kind}_ids.txt")

    def reconcile_summary(self, dataset: str) -> Path:
        return self.path("reconcile", f"{dataset}.summary.json")

    def cmr_listing(self, dataset: str, shard: str) -> Path:
        return self.path("cmr", "listing", dataset, f"{shard}.jsonl.gz")

    def cmr_result(self, dataset: str, shard: str, source: str) -> Path:
        """`source` is store or build: checking the stored documents is tracked apart from rewriting a build."""
        return self.path("cmr", source, dataset, f"{shard}.result.json")

    def cmr_summary(self, dataset: str, source: str) -> Path:
        return self.path("cmr", f"{dataset}.{source}.summary.json")

    def cmr_missing(self, dataset: str, source: str) -> Path:
        return self.path("cmr", f"{dataset}.{source}.missing.tsv")

    def listing_shards(self, dataset: str) -> list[str]:
        return sorted(p.name[:-len(".jsonl.gz")] for p in self.path("listing", dataset).glob("*.jsonl.gz"))

    def docs_shards(self, dataset: str) -> list[str]:
        return sorted(p.name[:-len(".jsonl.gz")] for p in self.path("docs", dataset).glob("*.jsonl.gz"))


def done_marker(data_path: Path) -> Path:
    name = data_path.name
    for suffix in (".jsonl.gz", ".result.json", ".json"):
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    return data_path.with_name(f"{name}._DONE")


def is_done(data_path: Path) -> bool:
    return done_marker(data_path).exists()


def mark_done(data_path: Path, stats: dict):
    write_json(done_marker(data_path), stats)


def read_done(data_path: Path) -> dict | None:
    marker = done_marker(data_path)
    return json.loads(marker.read_text()) if marker.exists() else None


def write_json(path: Path, value) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    part.write_text(json.dumps(value, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(part, path)
    return path


class JsonlGzWriter:
    """Atomic gzipped JSON-lines writer: `<path>.part` while open, renamed on a clean close."""

    def __init__(self, path: Path, compresslevel: int = 6):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._part = self.path.with_name(self.path.name + ".part")
        self._fh = gzip.open(self._part, "wt", encoding="utf-8", compresslevel=compresslevel)
        self.count = 0

    def write(self, record: dict):
        self._fh.write(json.dumps(record, separators=(",", ":"), default=str))
        self._fh.write("\n")
        self.count += 1

    def close(self):
        self._fh.close()
        os.replace(self._part, self.path)

    def abort(self):
        try:
            self._fh.close()
        finally:
            if self._part.exists():
                self._part.unlink()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.close()
        else:
            self.abort()


def read_jsonl_gz(path: Path) -> Iterator[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def iter_jsonl_bytes(data: bytes) -> Iterator[dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        for line in io.TextIOWrapper(gz, encoding="utf-8"):
            if line.strip():
                yield json.loads(line)


def append_jsonl(path: Path, records: Iterable[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")


def read_ids_file(path: Path | None) -> set[str] | None:
    """None without a file (no selection); otherwise the ids it lists, and an empty file selects nothing."""
    if not path:
        return None
    return {line.strip() for line in Path(path).read_text().splitlines() if line.strip()}
