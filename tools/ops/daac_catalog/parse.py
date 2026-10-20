"""Tier 0: turn one granule's S3 listing into classified files and GRQ `metadata.Files` entries.

`metadata.Files` is built exactly the way HySDS builds it at product creation: every file matching a
`pge_outputs.yaml` Primary regex goes through `extractor.FilenameRegexMetExtractor` with the dataset's
`settings.yaml` PRODUCT_TYPES pattern and configuration, then `product2dataset.merge_dataset_met_json` moves the
`Product*` keys and `dataset_version` up to the dataset level. The only difference is that sizes come from the
S3 listing instead of a local `stat`, and the extractor sees the file name instead of a worker path (GRQ's TROPO
`model` group swallowed part of the job path because its regex is unanchored on the left).
"""
from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from enum import StrEnum

from extractor.CoreMetExtractor import CoreMetExtractor
from extractor.FilenameRegexMetExtractor import FilenameRegexMetExtractor

from tools.ops.daac_catalog.registry import (
    DatasetSpec, Source, id_pattern, is_granule_id, primary_patterns, product_type_config)


class Role(StrEnum):
    PRIMARY = "primary"      # data files
    METADATA = "metadata"    # .iso.xml
    BROWSE = "browse"        # browse images delivered by the SDS
    CHECKSUM = "checksum"    # .md5 twins
    SIDECAR = "sidecar"      # ASF UMM-G .cmr.json
    DERIVED = "derived"      # made by the DAAC: _low-res.png, _thumbnail.png, .zarr.json.gz
    OTHER = "other"


_DERIVED_SUFFIXES = ("_low-res.png", "_thumbnail.png", ".zarr.json.gz")


@dataclass
class ListedObject:
    source: str
    bucket: str
    key: str
    size: int
    last_modified: str
    etag: str | None = None

    @property
    def name(self) -> str:
        return posixpath.basename(self.key)

    @property
    def s3_url(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @property
    def https_url(self) -> str:
        return f"https://{self.bucket}.s3.us-west-2.amazonaws.com/{self.key}"

    def to_record(self) -> dict:
        record = {"src": self.source, "bucket": self.bucket, "key": self.key, "size": self.size,
                  "last_modified": self.last_modified}
        if self.etag:
            record["etag"] = self.etag
        return record

    @classmethod
    def from_record(cls, record: dict) -> "ListedObject":
        return cls(source=record["src"], bucket=record["bucket"], key=record["key"], size=int(record["size"]),
                   last_modified=record["last_modified"], etag=record.get("etag"))


@dataclass
class GranuleListing:
    """One manifest line: everything the DAAC holds for one granule."""
    id: str
    dataset: str
    files: list[ListedObject]

    def to_record(self) -> dict:
        return {"id": self.id, "dataset": self.dataset, "files": [f.to_record() for f in self.files]}

    @classmethod
    def from_record(cls, record: dict) -> "GranuleListing":
        return cls(id=record["id"], dataset=record["dataset"],
                   files=[ListedObject.from_record(f) for f in record["files"]])


@dataclass
class ClassifiedFile:
    obj: ListedObject
    role: Role
    in_grq_files: bool


@dataclass
class ParsedGranule:
    spec: DatasetSpec
    id: str
    id_groups: dict
    files: list[ClassifiedFile]
    grq_files: list[dict]
    dataset_version: str | None
    natural_key: str
    problems: list[str] = field(default_factory=list)

    @property
    def product_version(self) -> str | None:
        """`1.1` (no v), from the file name."""
        version = self.id_groups.get("product_version") or self.dataset_version
        return version[1:] if version and version.startswith("v") else version

    def objects(self, *roles: Role) -> list[ListedObject]:
        return [f.obj for f in self.files if not roles or f.role in roles]

    def by_name(self, name: str) -> ListedObject | None:
        for f in self.files:
            if f.obj.name == name:
                return f.obj
        return None

    def grq_objects(self) -> list[ListedObject]:
        return [f.obj for f in self.files if f.in_grq_files]


class RejectedGranule(Exception):
    pass


class _ListingCoreMet(CoreMetExtractor):
    """Core metadata from listing values instead of a local file."""

    def __init__(self, location: str, size: int):
        self._location = location
        self._size = size

    def get_core_metadata(self, product):
        return {"FileLocation": self._location, "FileSize": self._size, "FileName": posixpath.basename(product)}


class _ListingMetExtractor(FilenameRegexMetExtractor, _ListingCoreMet):
    """The HySDS filename extractor with its file-system probe replaced by listing values.

    FilenameRegexMetExtractor calls `super(FilenameRegexMetExtractor, self).get_core_metadata`, which resolves to
    the next class in this MRO, `_ListingCoreMet`, rather than to `CoreMetExtractor`.
    """


def classify(spec: DatasetSpec, obj: ListedObject) -> tuple[Role, bool]:
    """Role of a listed object and whether HySDS would have listed it in metadata.Files."""
    name = obj.name
    if obj.source == Source.UMMG:
        return Role.SIDECAR, False
    if name.endswith(".md5"):
        return Role.CHECKSUM, False
    if name.endswith(_DERIVED_SUFFIXES):
        return Role.DERIVED, False
    in_files = any(p.search(name) for p in primary_patterns(spec.dataset))
    if name.endswith(".iso.xml"):
        return Role.METADATA, in_files
    if name.endswith(".png") or "_BROWSE" in name:
        return Role.BROWSE, in_files
    if in_files:
        return Role.PRIMARY, True
    return Role.OTHER, False


def file_entry(spec: DatasetSpec, granule_id: str, obj: ListedObject) -> tuple[dict, dict]:
    """The metadata.Files entry for one file and the dataset-level keys the merge step hoists out of it."""
    conf = product_type_config(spec.dataset)
    extractor = _ListingMetExtractor(f"/datasets/{granule_id}", obj.size)
    met = extractor.extract(obj.name, conf["Pattern"].pattern, conf.get("Configuration") or {})
    hoisted = {k: met.pop(k) for k in list(met) if k.startswith("Product") or k == "dataset_version"}
    met["id"] = granule_id
    met["tags"] = ["PGE"]
    return met, hoisted


def natural_key(spec: DatasetSpec, id_groups: dict) -> str:
    return "|".join([spec.dataset.value] + [str(id_groups.get(f) or "") for f in spec.natural_key_fields])


def parse_granule(spec: DatasetSpec, listing: GranuleListing) -> ParsedGranule:
    granule_id = listing.id
    match = id_pattern(spec.dataset).fullmatch(granule_id)
    if not match or not is_granule_id(spec.dataset, granule_id):
        raise RejectedGranule(f"granule id does not match the {spec.dataset} pattern")
    id_groups = {k: v for k, v in match.groupdict().items() if k != "id" and v is not None}

    problems = []
    classified = []
    seen = set()
    for obj in sorted(listing.files, key=lambda o: (o.name, o.source)):
        if not obj.name.startswith(granule_id):
            problems.append(f"foreign file {obj.key}")
            continue
        if (obj.source, obj.name) in seen:
            continue
        seen.add((obj.source, obj.name))
        role, in_files = classify(spec, obj)
        classified.append(ClassifiedFile(obj, role, in_files))

    entries = []
    dataset_version = None
    for cf in classified:
        if not cf.in_grq_files:
            continue
        try:
            entry, hoisted = file_entry(spec, granule_id, cf.obj)
        except ValueError as err:
            problems.append(f"unparseable file {cf.obj.name}: {err}")
            cf.in_grq_files = False
            continue
        dataset_version = dataset_version or hoisted.get("dataset_version")
        entries.append(entry)
    if not entries or not any(cf.role == Role.PRIMARY for cf in classified):
        raise RejectedGranule("no data file matches the dataset's Primary output patterns")
    entries.sort(key=lambda e: e["FileName"])
    return ParsedGranule(spec=spec, id=granule_id, id_groups=id_groups, files=classified, grq_files=entries,
                         dataset_version=dataset_version, natural_key=natural_key(spec, id_groups),
                         problems=problems)
