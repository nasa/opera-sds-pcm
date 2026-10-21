"""Bundles: the format a dataset's catalog travels in between clusters, and its S3 archive form.

```
<bundle-id>/
  bundle.json                    manifest, written (and uploaded) last; its presence means the bundle is complete
  docs/<index>.<nnnn>.jsonl.gz   documents grouped by target index without the family prefix
  docs/<index>.<nnnn>.parquet    optional, same rows
  listing/<shard>.jsonl.gz       optional tier-0 manifests, so the destination can reconcile without DAAC access
```
A document line is the stored `_source` and nothing else. The destination recomputes `_index` and `_id` and its own
templates supply mappings, so a bundle is independent of the OpenSearch version and of the index family it lands in.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import socket
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from tools.ops.daac_catalog import SCHEMA_VERSION, __version__
from tools.ops.daac_catalog.build import index_name
from tools.ops.daac_catalog.runs import JsonlGzWriter, iter_jsonl_bytes, write_json
from tools.ops.daac_catalog.sources import fmt_seconds_z

logger = logging.getLogger(__name__)

FORMAT_VERSION = 1
MANIFEST = "bundle.json"
DEFAULT_SHARD_DOCS = 50_000


class BundleError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def bundle_id_for(dataset: str, kind: str, run_id: str | None = None, now: datetime | None = None) -> str:
    if run_id and kind == "build":
        return f"{dataset}-{run_id}"
    now = now or datetime.now(timezone.utc)
    return f"{dataset}-exp-{now:%Y%m%dT%H%MZ}"


class BundleWriter:
    def __init__(self, directory: Path, dataset: str, kind: str = "full", source: dict | None = None,
                 parquet: bool = False, shard_docs: int = DEFAULT_SHARD_DOCS):
        if kind not in ("full", "delta"):
            raise ValueError(kind)
        self.directory = Path(directory)
        if (self.directory / MANIFEST).exists():
            raise BundleError(f"{self.directory} already holds a complete bundle")
        (self.directory / "docs").mkdir(parents=True, exist_ok=True)
        self.dataset = dataset
        self.kind = kind
        self.source = source or {}
        self.parquet = parquet
        self.shard_docs = shard_docs
        self._open: dict[str, tuple[JsonlGzWriter, object, int]] = {}
        self._seq: dict[str, int] = {}
        self._closed_files: list[tuple[str, str]] = []   # (index suffix, relative path)
        self._listing: list[str] = []
        self.doc_count = 0
        self.tiers: dict[str, int] = {}

    def _rotate(self, suffix: str):
        writer, sink, _ = self._open.pop(suffix)
        writer.close()
        if sink:
            sink.close()
        self._closed_files.append((suffix, str(writer.path.relative_to(self.directory))))
        if sink:
            self._closed_files.append((suffix, str(sink.path.relative_to(self.directory))))

    def write(self, doc: dict):
        suffix = index_name(doc, prefix="")
        entry = self._open.get(suffix)
        if entry and entry[2] >= self.shard_docs:
            self._rotate(suffix)
            entry = None
        if entry is None:
            seq = self._seq.get(suffix, 0)
            self._seq[suffix] = seq + 1
            writer = JsonlGzWriter(self.directory / "docs" / f"{suffix}.{seq:04d}.jsonl.gz")
            sink = None
            if self.parquet:
                from tools.ops.daac_catalog.stages import ParquetSink
                sink = ParquetSink(self.directory / "docs" / f"{suffix}.{seq:04d}.parquet")
            entry = (writer, sink, 0)
        writer, sink, n = entry
        writer.write(doc)
        if sink:
            sink.write(doc)
        self._open[suffix] = (writer, sink, n + 1)
        self.doc_count += 1
        tier = str((doc.get("catalog") or {}).get("tier"))
        self.tiers[tier] = self.tiers.get(tier, 0) + 1

    def add_listing(self, files: list[Path]):
        target = self.directory / "listing"
        target.mkdir(parents=True, exist_ok=True)
        for path in files:
            shutil.copy2(path, target / path.name)
            self._listing.append(f"listing/{path.name}")

    def close(self) -> dict:
        for suffix in list(self._open):
            self._rotate(suffix)
        files, indices = [], {}
        for suffix, rel in sorted(self._closed_files):
            path = self.directory / rel
            count = None
            if rel.endswith(".jsonl.gz"):
                count = sum(1 for _ in iter_jsonl_bytes(path.read_bytes()))
                entry = indices.setdefault(suffix, {"count": 0, "files": []})
                entry["count"] += count
                entry["files"].append(rel)
            files.append({"path": rel, "bytes": path.stat().st_size, "sha256": sha256_file(path), "count": count})
        for rel in self._listing:
            path = self.directory / rel
            files.append({"path": rel, "bytes": path.stat().st_size, "sha256": sha256_file(path), "count": None})
        manifest = {
            "bundle_id": self.directory.name,
            "format_version": FORMAT_VERSION,
            "kind": self.kind,
            "dataset": self.dataset,
            "schema_version": SCHEMA_VERSION,
            "tool_version": __version__,
            "created": fmt_seconds_z(datetime.now(timezone.utc)),
            "source": {"host": socket.gethostname(), **self.source},
            "doc_count": self.doc_count,
            "tiers": self.tiers,
            "indices": indices,
            "files": files,
            "complete": True,
        }
        write_json(self.directory / MANIFEST, manifest)
        return manifest


@dataclass
class BundleLocation:
    url: str
    is_s3: bool
    bucket: str | None = None
    prefix: str | None = None
    path: Path | None = None

    @classmethod
    def parse(cls, url: str) -> "BundleLocation":
        if url.startswith("s3://"):
            bucket, _, prefix = url[5:].partition("/")
            return cls(url, True, bucket=bucket, prefix=prefix.rstrip("/"))
        return cls(url, False, path=Path(url))


class BundleReader:
    def __init__(self, location: BundleLocation, manifest: dict, s3_client=None):
        self.location = location
        self.manifest = manifest
        self._s3 = s3_client

    @classmethod
    def open(cls, url: str, s3_client=None) -> "BundleReader":
        location = BundleLocation.parse(url)
        if location.is_s3:
            if s3_client is None:
                import boto3
                s3_client = boto3.client("s3")
            try:
                body = s3_client.get_object(Bucket=location.bucket,
                                            Key=f"{location.prefix}/{MANIFEST}")["Body"].read()
            except Exception as err:
                raise BundleError(f"{url} has no {MANIFEST}: incomplete or not a bundle ({err})") from None
            manifest = json.loads(body)
        else:
            path = location.path / MANIFEST
            if not path.exists():
                raise BundleError(f"{url} has no {MANIFEST}: incomplete or not a bundle")
            manifest = json.loads(path.read_text())
        if manifest.get("format_version") != FORMAT_VERSION or not manifest.get("complete"):
            raise BundleError(f"{url}: unsupported or incomplete bundle manifest")
        return cls(location, manifest, s3_client)

    def check_schema(self, allow_mismatch: bool = False):
        theirs = str(self.manifest.get("schema_version", "")).split(".")[0]
        ours = SCHEMA_VERSION.split(".")[0]
        if theirs != ours and not allow_mismatch:
            raise BundleError(f"bundle schema {self.manifest.get('schema_version')} does not match tool schema "
                              f"{SCHEMA_VERSION} (use --allow-schema-mismatch to load anyway)")

    @property
    def kind(self) -> str:
        return self.manifest["kind"]

    def _read(self, rel: str) -> bytes:
        if self.location.is_s3:
            return self._s3.get_object(Bucket=self.location.bucket,
                                       Key=f"{self.location.prefix}/{rel}")["Body"].read()
        return (self.location.path / rel).read_bytes()

    def _entry(self, rel: str) -> dict:
        for entry in self.manifest["files"]:
            if entry["path"] == rel:
                return entry
        raise BundleError(f"{rel} is not in the manifest")

    def verified_bytes(self, rel: str) -> bytes:
        data = self._read(rel)
        entry = self._entry(rel)
        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise BundleError(f"{rel}: size or sha256 does not match the manifest")
        return data

    def doc_files(self) -> list[str]:
        return [f["path"] for f in self.manifest["files"] if f["path"].endswith(".jsonl.gz")
                and f["path"].startswith("docs/")]

    def iter_docs(self) -> Iterator[dict]:
        for rel in self.doc_files():
            data = self.verified_bytes(rel)
            count = 0
            for doc in iter_jsonl_bytes(data):
                count += 1
                yield doc
            expected = self._entry(rel)["count"]
            if expected is not None and count != expected:
                raise BundleError(f"{rel}: {count} documents, manifest says {expected}")

    def verify(self) -> int:
        """Check every file against the manifest; returns the number of documents."""
        total = 0
        for entry in self.manifest["files"]:
            data = self.verified_bytes(entry["path"])
            if entry["path"].endswith(".jsonl.gz") and entry["path"].startswith("docs/"):
                n = sum(1 for _ in iter_jsonl_bytes(data))
                if n != entry["count"]:
                    raise BundleError(f"{entry['path']}: {n} documents, manifest says {entry['count']}")
                total += n
        if total != self.manifest["doc_count"]:
            raise BundleError(f"bundle holds {total} documents, manifest says {self.manifest['doc_count']}")
        return total


def push(directory: Path, s3_url: str, s3_client, threads: int = 8) -> str:
    """Upload a bundle directory to `<s3_url>/<dataset>/<bundle-id>/`: the files in parallel, then a check that
    every uploaded object has the manifest's size, then bundle.json last, so a bundle in S3 is complete or absent."""
    directory = Path(directory)
    manifest = json.loads((directory / MANIFEST).read_text())
    location = BundleLocation.parse(s3_url.rstrip("/"))
    if not location.is_s3:
        raise BundleError("push needs an s3:// destination")
    base = "/".join(p for p in (location.prefix, manifest["dataset"], manifest["bundle_id"]) if p)

    def upload(entry):
        s3_client.upload_file(str(directory / entry["path"]), location.bucket, f"{base}/{entry['path']}")

    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        list(pool.map(upload, manifest["files"]))
    uploaded = {}
    for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=location.bucket, Prefix=f"{base}/"):
        for obj in page.get("Contents", []):
            uploaded[obj["Key"][len(base) + 1:]] = obj["Size"]
    wrong = [e["path"] for e in manifest["files"] if uploaded.get(e["path"]) != e["bytes"]]
    if wrong:
        raise BundleError(f"{len(wrong)} uploaded files are missing or have the wrong size, e.g. {wrong[:3]}")
    s3_client.upload_file(str(directory / MANIFEST), location.bucket, f"{base}/{MANIFEST}")
    return f"s3://{location.bucket}/{base}"


def pull(s3_url: str, bundles_dir: Path, s3_client) -> Path:
    """Download a bundle and verify every file before anything is loaded."""
    reader = BundleReader.open(s3_url, s3_client)
    target = Path(bundles_dir) / reader.manifest["bundle_id"]
    part = target.with_name(target.name + ".part")
    if target.exists():
        raise BundleError(f"{target} already exists")
    if part.exists():
        shutil.rmtree(part)
    for entry in reader.manifest["files"]:
        data = reader.verified_bytes(entry["path"])
        out = part / entry["path"]
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(data)
    write_json(part / MANIFEST, reader.manifest)
    os.replace(part, target)
    BundleReader.open(str(target)).verify()
    return target
