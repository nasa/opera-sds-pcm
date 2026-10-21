"""Assemble the GRQ-compatible core document (schema v1.0) for one granule.

Inputs are the parsed tier-0 listing and, when available, the tier-1 sources (UMM-G sidecar, ISO XML) and tier-2
facts (DISP-S1 RunConfig, burst database). `build_doc` is a pure function of those inputs plus the run context,
so `enrich` can rebuild a stored document from its own `catalog.files` and new sources.
"""
from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from dateutil.parser import parse as parse_date
from shapely import centroid

from tools.ops.daac_catalog import SCHEMA_VERSION
from tools.ops.daac_catalog.parse import ListedObject, ParsedGranule, Role, files_digest
from tools.ops.daac_catalog.registry import ASF_PRODUCTS_BUCKET, DATASETS, Daac, Dataset, DatasetSpec, Source
from tools.ops.daac_catalog.sources import (
    IsoInfo, UmmgInfo, fmt_creation, fmt_micro_z, fmt_seconds_z, normalize_ts, parse_list_attribute,
    to_grq_geojson, valid_geometry)

# Timestamps whose GRQ value only existed inside the HySDS job; the catalog carries a DAAC-side proxy.
ALWAYS_APPROXIMATED = (
    "creation_timestamp", "@timestamp", "metadata.ProductReceivedTime", "daac_CNM_S_timestamp",
    "daac_submission_timestamp", "daac_received_timestamp", "daac_process_complete_timestamp")

# GRQ fields that only the worker that ran the PGE ever knew.
ALWAYS_OMITTED = (
    "metadata.pcm_version", "metadata.accountability", "metadata.runconfig", "metadata.lineage_basepath",
    "metadata.InputProductReceivedTime", "prov.wasGeneratedBy", "daac_catalog_id", "daac_catalog_url")

# Promoted fields product2dataset.py emits that need trigger state the DAAC never saw (decision 5).
DEFERRED = {
    Dataset.L3_DSWX_S1: ("metadata.mgrs_set_id", "metadata.input_granule_id"),
    Dataset.L3_DIST_S1: ("metadata.input_granule_id",),
}


@dataclass
class BuildContext:
    run_id: str
    listed_at: datetime                       # when the tier-0 listing ran; first_seen/last_seen
    built_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    frame_to_bursts: Any = None               # DISP-S1 consistent burst DB (cslc_utils), optional


@dataclass
class SourceRef:
    kind: str                                 # ummg | iso_xml | product_file | md5
    url: str
    etag: str | None = None
    last_modified: str | None = None

    def to_dict(self) -> dict:
        return {k: v for k, v in {"kind": self.kind, "url": self.url, "etag": self.etag,
                                  "last_modified": self.last_modified}.items() if v is not None}


@dataclass
class Tier1:
    ummg: UmmgInfo | None = None
    iso: IsoInfo | None = None
    md5: dict[str, str] = field(default_factory=dict)       # file name -> md5 from the .md5 twins
    refs: list[SourceRef] = field(default_factory=list)


@dataclass
class Tier2:
    pge_runconfig: str | None = None
    cslc_inputs: list[str] = field(default_factory=list)    # DISP-S1 RunConfig cslc_file_list basenames
    ccslc_inputs: list[str] = field(default_factory=list)
    refs: list[SourceRef] = field(default_factory=list)


class _Derivation:
    """Records where each approximated or optional value came from (catalog.derivation)."""

    def __init__(self):
        self.used: dict[str, str] = {}

    def pick(self, field_name: str, candidates: list[tuple[str, Any]]):
        for label, value in candidates:
            if value not in (None, "", []):
                self.used[field_name] = label
                return value
        return None


def _delivered(parsed: ParsedGranule) -> list[ListedObject]:
    """Objects the SDS delivered (excludes ASF derivatives and the UMM-G sidecar)."""
    return [f.obj for f in parsed.files if f.role not in (Role.DERIVED, Role.SIDECAR)]


def _earliest(objs: list[ListedObject]) -> datetime | None:
    stamps = [normalize_ts(o.last_modified) for o in objs if o.last_modified]
    return min(stamps) if stamps else None


def _latest(objs: list[ListedObject]) -> datetime | None:
    stamps = [normalize_ts(o.last_modified) for o in objs if o.last_modified]
    return max(stamps) if stamps else None


def _promote(metadata: dict, keys: tuple[str, ...]):
    """product2dataset.promote_file_metadata: copy per-file values that agree across Files to the top level."""
    files = metadata.get("Files") or []
    for key in keys:
        if key in metadata:
            continue
        values = [f[key] for f in files if key in f]
        if values and all(v == values[0] for v in values[1:]):
            metadata[key] = values[0]


def _iso_offset(value: str) -> str:
    """`parse(ts).replace(tzinfo=utc).isoformat()`, as product2dataset writes RTC acquisition/revision times."""
    return parse_date(value).replace(tzinfo=timezone.utc).isoformat()


def daac_url_for_input(name: str) -> str:
    """`url:` reference for an input file or granule: the ASF S3 location when the input is itself an OPERA
    product archived at ASF, else the bare name."""
    for spec in DATASETS.values():
        if spec.daac != Daac.ASF or not name.startswith(spec.id_stem):
            continue
        from tools.ops.daac_catalog.registry import granule_id_of
        gid = granule_id_of(spec.dataset, name)
        if not gid:
            continue
        if name == gid:
            return f"url:s3://{ASF_PRODUCTS_BUCKET}/{spec.product_prefix}{gid}"
        return f"url:s3://{ASF_PRODUCTS_BUCKET}/{spec.product_prefix}{gid}/{name}"
    return f"url:{name}"


# ---- per-dataset promoted fields ----------------------------------------------------------------------------------

def _l2_inputs(parsed, t1, metadata, omitted, lineage):
    ummg, iso = t1.ummg, t1.iso
    slc = None
    if ummg and ummg.input_granules:
        slc = ummg.input_granules[0]
    elif iso:
        slcs = parse_list_attribute(iso.attribute("L1SlcFiles", "L1SlcGranules"))
        slc = slcs[0] if slcs else None
    if slc:
        slc = slc.removesuffix(".zip")
        metadata["input_granule_id"] = slc
        lineage.append(f"{slc}.zip")
    else:
        omitted.append("metadata.input_granule_id")
    orbits = parse_list_attribute(iso.attribute("OrbitFiles")) if iso else []
    if orbits:
        metadata["orbit_file"] = posixpath.basename(orbits[0])
        lineage.extend(posixpath.basename(o) for o in orbits)
    else:
        omitted.append("metadata.orbit_file")


def _polarization_list(t1) -> list[str]:
    if t1.iso and t1.iso.attribute("ListOfPolarizations"):
        return parse_list_attribute(t1.iso.attribute("ListOfPolarizations"))
    if t1.ummg and t1.ummg.attributes.get("POLARIZATION"):
        return list(t1.ummg.attributes["POLARIZATION"])
    return []


def _cslc(parsed, t1, t2, ctx, metadata, omitted, lineage):
    _promote(metadata, ("burst_id", "acquisition_ts", "sensor", "pol"))
    _l2_inputs(parsed, t1, metadata, omitted, lineage)
    polarization = (t1.iso.attribute("Polarization") if t1.iso else None) or (
        t1.ummg.attribute("POLARIZATION") if t1.ummg else None)
    if polarization:
        metadata["polarization"] = polarization
    else:
        omitted.append("metadata.polarization")


def _cslc_static(parsed, t1, t2, ctx, metadata, omitted, lineage):
    _promote(metadata, ("burst_id", "validity_ts", "sensor"))
    _l2_inputs(parsed, t1, metadata, omitted, lineage)


def _rtc(parsed, t1, t2, ctx, metadata, omitted, lineage):
    from rtc_utils import determine_acquisition_cycle_for_rtc_granule
    _l2_inputs(parsed, t1, metadata, omitted, lineage)
    _promote(metadata, ("acquisition_ts",))
    sample = parsed.grq_files[0]
    polarizations = _polarization_list(t1)
    if polarizations:
        metadata["polarizations"] = polarizations
    else:
        omitted.append("metadata.polarizations")
    metadata["granule_id"] = parsed.id
    metadata["burst_id"] = sample["burst_id"]
    metadata["acquisition_timestamp"] = _iso_offset(sample["acquisition_ts"])
    metadata["revision_timestamp"] = _iso_offset(sample["creation_ts"])
    metadata["sensor"] = sample["sensor"]
    metadata["acquisition_cycle"] = determine_acquisition_cycle_for_rtc_granule(parsed.id)


def _rtc_static(parsed, t1, t2, ctx, metadata, omitted, lineage):
    _l2_inputs(parsed, t1, metadata, omitted, lineage)


def _dswx_hls(parsed, t1, t2, ctx, metadata, omitted, lineage):
    hls = t1.iso.attribute("HlsDataset") if t1.iso else None
    if hls:
        metadata["input_granule_id"] = hls
        lineage.append(hls)
    else:
        omitted.append("metadata.input_granule_id")


def _dswx_s1(parsed, t1, t2, ctx, metadata, omitted, lineage):
    metadata["tile_id"] = parsed.id_groups["tile_id"]
    iso = t1.iso
    if iso:
        for key, attr in (("rtc_sensing_start_time", "RTCSensingStartTime"),
                          ("rtc_sensing_end_time", "RTCSensingEndTime")):
            if iso.attribute(attr):
                metadata[key] = iso.attribute(attr)
            else:
                omitted.append(f"metadata.{key}")
        rtc_inputs = sorted(parse_list_attribute(iso.attribute("RTCInputList")))
        if rtc_inputs:
            metadata["rtc_input_list"] = rtc_inputs
            lineage.extend(rtc_inputs)
        else:
            omitted.append("metadata.rtc_input_list")
    else:
        omitted.extend(["metadata.rtc_sensing_start_time", "metadata.rtc_sensing_end_time",
                        "metadata.rtc_input_list"])


def _disp(parsed, t1, t2, ctx, metadata, omitted, lineage):
    from tools.ops.daac_catalog import tier2 as tier2_mod
    frame_id = int(parsed.id_groups["frame_id"].lstrip("F"))
    metadata["frame_id"] = frame_id
    cslc_inputs = list(t2.cslc_inputs) if t2 and t2.cslc_inputs else []
    ccslc_inputs = list(t2.ccslc_inputs) if t2 and t2.ccslc_inputs else []
    if not cslc_inputs and t1.iso:
        for name in parse_list_attribute(t1.iso.attribute("SourceDataFileList")):
            (ccslc_inputs if "COMPRESSED-CSLC" in name else cslc_inputs).append(name)
    lineage.extend(cslc_inputs + ccslc_inputs)
    derived = None
    if ctx.frame_to_bursts is not None:
        derived = tier2_mod.disp_s1_cycles(frame_id, cslc_inputs, ctx.frame_to_bursts)
    if derived and derived.get("acquisition_cycle") is not None:
        metadata["acquisition_cycle"] = derived["acquisition_cycle"]
    else:
        omitted.append("metadata.acquisition_cycle")
    if derived and derived.get("ksc_id"):
        metadata["input_granule_id"] = derived["ksc_id"]
    else:
        omitted.append("metadata.input_granule_id")


def _disp_static(parsed, t1, t2, ctx, metadata, omitted, lineage):
    metadata["input_granule_id"] = str(int(parsed.id_groups["frame_id"].lstrip("F")))


def _dist(parsed, t1, t2, ctx, metadata, omitted, lineage):
    metadata["mgrs_tile_id"] = parsed.id_groups["tile_id"].removeprefix("T")
    if t1.iso:
        for attr in ("PreRtcOperaIds", "PostRtcOperaIds"):
            lineage.extend(parse_list_attribute(t1.iso.attribute(attr)))


def _tropo(parsed, t1, t2, ctx, metadata, omitted, lineage):
    ecmwf = t1.ummg.input_granules[0] if t1.ummg and t1.ummg.input_granules else None
    if ecmwf:
        ecmwf = ecmwf if ecmwf.endswith(".nc") else f"{ecmwf}.nc"
        metadata["input_granule_id"] = f"{parsed.id_groups['acquisition_ts'][:8]}/{ecmwf}"
        lineage.append(ecmwf)
    else:
        omitted.append("metadata.input_granule_id")


PROMOTERS: dict[Dataset, Callable] = {
    Dataset.L2_CSLC_S1: _cslc,
    Dataset.L2_CSLC_S1_STATIC: _cslc_static,
    Dataset.L2_RTC_S1: _rtc,
    Dataset.L2_RTC_S1_STATIC: _rtc_static,
    Dataset.L3_DSWX_HLS: _dswx_hls,
    Dataset.L3_DSWX_S1: _dswx_s1,
    Dataset.L3_DISP_S1: _disp,
    Dataset.L3_DISP_S1_STATIC: _disp_static,
    Dataset.L3_DIST_S1: _dist,
    Dataset.L4_TROPO: _tropo,
}


# ---- temporal fallbacks from the file name ----------------------------------------------------------------------

def _filename_temporal(parsed: ParsedGranule) -> tuple[datetime | None, datetime | None]:
    g = parsed.id_groups
    if "ref_datetime" in g:
        return normalize_ts(g["ref_datetime"]), normalize_ts(g["sec_datetime"])
    for key in ("acquisition_ts", "validity_ts", "validity_start_date"):
        if g.get(key):
            ts = normalize_ts(g[key])
            return ts, ts
    return None, None


def _iso_temporal(spec: DatasetSpec, iso: IsoInfo | None) -> tuple[datetime | None, datetime | None]:
    if iso is None:
        return None, None
    if spec.dataset == Dataset.L3_DISP_S1:
        start = iso.attribute("ReferenceZeroDopplerStartTime", "ReferenceDatetime")
        end = iso.attribute("SecondaryZeroDopplerEndTime", "SecondaryDatetime")
        if start and end:
            return normalize_ts(start), normalize_ts(end)
    return iso.begin, iso.end


# ---- the document -----------------------------------------------------------------------------------------------

def tier_of(t1: Tier1 | None, t2: Tier2 | None) -> int:
    if t2 and (t2.pge_runconfig or t2.cslc_inputs):
        return 2
    if t1 and (t1.ummg or t1.iso):
        return 1
    return 0


def build_doc(parsed: ParsedGranule, ctx: BuildContext, tier1: Tier1 | None = None,
              tier2: Tier2 | None = None) -> dict:
    spec = parsed.spec
    t1 = tier1 or Tier1()
    gid = parsed.id
    dataset = spec.dataset.value
    version = parsed.dataset_version or f"v{parsed.product_version}"
    derivation = _Derivation()
    approximated = list(ALWAYS_APPROXIMATED)
    omitted = list(ALWAYS_OMITTED) + list(DEFERRED.get(spec.dataset, ()))
    delivered = _delivered(parsed)

    creation = derivation.pick("creation_timestamp", [
        ("iso:ProcessingDateTime", t1.iso.processing_time if t1.iso else None),
        ("ummg:DataGranule.ProductionDateTime", t1.ummg.production_time if t1.ummg else None),
        ("filename:creation_ts", normalize_ts(parsed.id_groups.get("creation_ts"))),
        ("s3:earliest_last_modified", _earliest(delivered)),
    ])
    inserted = derivation.pick("@timestamp", [
        ("ummg:ProviderDates.Insert", t1.ummg.insert_time if t1.ummg else None),
        ("s3:latest_last_modified", _latest(delivered)),
    ])
    first_delivery = derivation.pick("daac_CNM_S_timestamp", [("s3:earliest_last_modified", _earliest(delivered))])

    start, end = None, None
    for label, (s, e) in (("ummg:TemporalExtent", (t1.ummg.begin, t1.ummg.end) if t1.ummg else (None, None)),
                          ("iso:temporal", _iso_temporal(spec, t1.iso)),
                          ("filename", _filename_temporal(parsed))):
        if s:
            start, end = s, e or s
            derivation.used["starttime"] = label
            if label == "filename":
                approximated.extend(["starttime", "endtime"])
            break

    geometry = None
    for label, geom in (("ummg:SpatialExtent", t1.ummg.geometry if t1.ummg else None),
                        ("iso:extent", t1.iso.geometry() if t1.iso else None)):
        geom = valid_geometry(geom)
        if geom is not None:
            geometry = geom
            derivation.used["location"] = label
            break

    pge_version = derivation.pick("pge_version", [
        ("ummg:PGEVersionClass", t1.ummg.pge_version if t1.ummg else None),
        ("iso:PGEVersionId", t1.iso.pge_version if t1.iso else None)])
    sas_version = derivation.pick("sas_version", [
        ("ummg:SASVersionId", t1.ummg.sas_version if t1.ummg else None),
        ("iso:SASVersionId", t1.iso.sas_version if t1.iso else None)])

    grq_objs = parsed.grq_objects()
    metadata: dict[str, Any] = {
        "Files": parsed.grq_files,
        "FileSize": sum(e["FileSize"] for e in parsed.grq_files),
        "FileName": gid,
        "id": gid,
        "product_urls": [o.https_url for o in grq_objs],
        "product_s3_paths": [o.s3_url for o in grq_objs],
        "ProductType": dataset,
        "CollectionName": spec.short_name,
        "ProductVersion": parsed.product_version,
        "dataset_version": version,
        "tags": ["PGE", "daac_delivered"],
    }
    if creation:
        metadata.update({
            "ProductReceivedTime": fmt_micro_z(creation),
            "ProductReceivedYear": creation.year,
            "ProductReceivedMonth": f"{creation.month:02d}",
            "ProductReceivedDay": f"{creation.day:02d}",
        })
    for key, value in (("pge_version", pge_version), ("sas_version", sas_version)):
        if value:
            metadata[key] = value
        else:
            omitted.append(f"metadata.{key}")
    if spec.files_not_in_grq:
        approximated.append("metadata.FileSize")

    lineage: list[str] = []
    PROMOTERS[spec.dataset](parsed, t1, tier2, ctx, metadata, omitted, lineage)
    lineage = sorted(set(lineage))
    if lineage:
        metadata["lineage"] = lineage
    else:
        omitted.append("metadata.lineage")

    products_bucket, _ = spec.bucket_prefix(Source.PRODUCTS)
    browse_bucket, _ = spec.bucket_prefix(Source.BROWSE)
    product_prefix = spec.granule_prefix(Source.PRODUCTS, gid)
    browse_prefix = spec.granule_prefix(Source.BROWSE, gid)
    granule_url = f"{products_bucket}/{product_prefix.rstrip('/')}"
    browse_url = f"{browse_bucket}/{browse_prefix.rstrip('/')}"

    doc: dict[str, Any] = {
        "id": gid,
        "objectid": gid,
        "dataset": dataset,
        "dataset_type": dataset,
        "dataset_level": spec.level,
        "ipath": f"hysds::data/{dataset}",
        "version": version,
        "system_version": version,
        "urls": [f"s3://{granule_url}", f"https://{products_bucket}.s3.us-west-2.amazonaws.com/"
                                        f"{product_prefix.rstrip('/')}"],
        "browse_urls": [f"s3://{browse_url}", f"https://{browse_bucket}.s3.us-west-2.amazonaws.com/"
                                              f"{browse_prefix.rstrip('/')}"],
        "images": [],
        "prov": {"wasDerivedFrom": [daac_url_for_input(name) for name in lineage]},
        "grq_index_result": {"index": None},
        "metadata": metadata,
    }
    if creation:
        doc["creation_timestamp"] = fmt_creation(creation)
    if inserted:
        doc["@timestamp"] = fmt_micro_z(inserted)
    if start:
        doc["starttime"] = fmt_seconds_z(start)
        doc["endtime"] = fmt_seconds_z(end)
    else:
        omitted.extend(["starttime", "endtime"])
    if geometry is not None:
        doc["location"] = to_grq_geojson(geometry)
        doc["center"] = to_grq_geojson(centroid(geometry))
    else:
        omitted.extend(["location", "center"])

    doc.update(_daac_block(spec, parsed, delivered, first_delivery, inserted))

    md5s = dict(t1.ummg.checksums) if t1.ummg else {}
    md5s.update(t1.md5)
    refs = list(t1.refs) + (list(tier2.refs) if tier2 else [])
    catalog = {
        "schema_version": SCHEMA_VERSION,
        "source": "daac-s3",
        "daac": spec.daac.value,
        "tier": tier_of(tier1, tier2),
        "build_run_id": ctx.run_id,
        "built_at": fmt_seconds_z(ctx.built_at),
        "product_prefix": f"s3://{products_bucket}/{product_prefix}",
        "browse_prefix": f"s3://{browse_bucket}/{browse_prefix}",
        "files": [_catalog_file(cf.obj, cf.role, md5s.get(cf.obj.name)) for cf in parsed.files],
        "files_digest": files_digest((cf.obj.bucket, cf.obj.key, cf.obj.size, cf.obj.etag) for cf in parsed.files),
        "sources": [r.to_dict() for r in refs],
        "derivation": derivation.used,
        "natural_key": parsed.natural_key,
        "approximated": sorted(set(approximated)),
        "omitted": sorted(set(omitted)),
        "first_seen": fmt_seconds_z(ctx.listed_at),
        "last_seen": fmt_seconds_z(ctx.listed_at),
        "updated_at": fmt_seconds_z(ctx.built_at),
    }
    if parsed.problems:
        catalog["problems"] = parsed.problems
    if tier2 and tier2.pge_runconfig:
        catalog["pge_runconfig"] = tier2.pge_runconfig
    doc["catalog"] = catalog
    return doc


def _catalog_file(obj: ListedObject, role: Role, md5: str | None) -> dict:
    record = obj.to_record()
    record["role"] = role.value
    if md5:
        record["md5"] = md5
    return record


def _daac_block(spec: DatasetSpec, parsed: ParsedGranule, delivered: list[ListedObject],
                first_delivery: datetime | None, inserted: datetime | None) -> dict:
    archived = [f.obj.s3_url for f in parsed.files if f.role in (Role.PRIMARY, Role.METADATA, Role.BROWSE)]
    block = {
        "daac_identifier": parsed.id,
        "daac_collection": spec.short_name,
        "daac_delivery_status": "SUCCESS",
        "daac_CNM_S_status": "SUCCESS",
        "daac_delivery_error_message": None,
        "daac_product_file_urls": [f.obj.s3_url for f in parsed.files],
        "archive_product_urls": archived,
    }
    if first_delivery:
        block["daac_CNM_S_timestamp"] = fmt_micro_z(first_delivery)
        block["daac_submission_timestamp"] = fmt_micro_z(first_delivery)
        block["daac_received_timestamp"] = _daac_text_ts(spec, first_delivery)
    if inserted:
        block["daac_process_complete_timestamp"] = _daac_text_ts(spec, inserted)
    return block


def _daac_text_ts(spec: DatasetSpec, dt: datetime) -> str:
    """CNM-R text timestamps differ by DAAC: ASF `2026-06-01 08:45:32Z`, PO.DAAC `2026-07-02T01:28:34.033Z`."""
    if spec.daac == Daac.PODAAC:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return dt.strftime("%Y-%m-%d %H:%M:%SZ")


def index_name(doc: dict, prefix: str = "catalog_") -> str:
    """`catalog_{version}_{dataset}-{YYYY.MM}` (or `grq_...` in restore mode), month of creation_timestamp."""
    created = normalize_ts(doc.get("creation_timestamp")) or normalize_ts(doc["catalog"]["first_seen"])
    return f"{prefix}{doc['version']}_{doc['dataset']}-{created:%Y.%m}".lower()
