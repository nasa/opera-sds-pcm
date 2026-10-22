"""Field-level comparison of a catalog document with the GRQ document HySDS wrote for the same granule.

Used by the golden unit test (fixtures) and by `validate --against-grq` (live documents, read-only). Fields fall
in four classes:

* EXACT: must be equal whenever GRQ has them.
* APPROXIMATED: the catalog carries a DAAC-side proxy; checked against a tolerance.
* NOT_COMPARED: job context and venue paths the DAAC never saw, URL fields that point elsewhere by design, and the
  fields grq2's dataset API adds at ingest (city, continent, temporal_span), which bulk loads do not go through.
* Everything else present on one side only is reported as missing or extra, except the catalog's known
  additions (fields newer product2dataset versions emit, time and space for every type, the catalog block).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta

from tools.ops.daac_catalog.registry import DATASETS, Dataset
from tools.ops.daac_catalog.sources import normalize_ts

EXACT_TOP = ("id", "objectid", "dataset", "dataset_type", "dataset_level", "ipath", "version", "system_version",
             "daac_identifier", "daac_collection", "daac_delivery_status", "daac_CNM_S_status")
EXACT_METADATA = ("FileName", "id", "ProductType", "CollectionName", "ProductVersion", "dataset_version",
                  "pge_version", "sas_version", "input_granule_id", "orbit_file", "burst_id", "acquisition_ts",
                  "sensor", "pol", "polarization", "polarizations", "frame_id", "acquisition_cycle", "tile_id",
                  "mgrs_tile_id", "rtc_input_list", "rtc_sensing_start_time", "rtc_sensing_end_time",
                  "validity_ts", "granule_id", "acquisition_timestamp", "revision_timestamp", "FileSize",
                  "ProductReceivedYear")
SET_METADATA = ("tags",)

NOT_COMPARED_TOP = ("urls", "browse_urls", "images", "prov", "daac_product_file_urls", "archive_product_urls",
                    "daac_catalog_id", "daac_catalog_url", "grq_index_result", "daac_delivery_error_message",
                    "is_backfilled", "catalog", "city", "continent", "temporal_span")
NOT_COMPARED_METADATA = ("Files", "accountability", "runconfig", "lineage", "lineage_basepath", "pcm_version",
                         "InputProductReceivedTime", "product_urls", "product_s3_paths", "context",
                         "ProductReceivedMonth", "ProductReceivedDay", "mgrs_set_id", "updated_product")
FILE_IGNORED = ("FileLocation", "runconfig", "lineage")

# Fields the catalog adds that older GRQ documents lack.
KNOWN_EXTRAS_TOP = ("starttime", "endtime", "location", "center", "catalog", "creation_timestamp", "@timestamp")
KNOWN_EXTRAS_METADATA = ("burst_id", "acquisition_ts", "sensor", "pol", "polarization", "polarizations",
                         "granule_id", "acquisition_timestamp", "revision_timestamp", "acquisition_cycle",
                         "validity_ts", "lineage", "tile_id", "mgrs_tile_id", "input_granule_id", "orbit_file",
                         "ProductReceivedTime", "ProductReceivedYear", "ProductReceivedMonth", "ProductReceivedDay")

# Known GRQ-side artifacts: GRQ ran the TROPO extractor on a worker path, so its unanchored `model` group
# swallowed part of the path, and catalog.json reported the TROPO PGE version with an extra `.0`.
KNOWN_GRQ_ARTIFACTS = {
    Dataset.L4_TROPO: {"Files.model", "metadata.pge_version"},
}


@dataclass
class Tolerance:
    creation_max_early: timedelta = timedelta(hours=24)   # GRQ creation is after the PGE finished
    creation_max_late: timedelta = timedelta(minutes=5)
    # DAAC-side times (daac_* and @timestamp) are what the archive shows: ingest can lag the SDS's records by the
    # DAAC's backlog, and PO.DAAC rewrites archive objects days later (S3 LastModified moves), so lateness is DAAC
    # behavior, reported as a lag count. A DAAC time well before the SDS's own record would be a bug.
    daac_max_early: timedelta = timedelta(hours=6)
    daac_lag_report: timedelta = timedelta(hours=6)
    time_window: timedelta = timedelta(seconds=60)          # starttime/endtime when GRQ has them
    center_degrees: float = 0.5


@dataclass
class Comparison:
    id: str
    dataset: str
    exact_mismatches: list[str] = field(default_factory=list)
    approx_out_of_tolerance: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    unexpected_extra: list[str] = field(default_factory=list)
    file_mismatches: list[str] = field(default_factory=list)
    known_artifacts: list[str] = field(default_factory=list)
    omitted: list[str] = field(default_factory=list)      # GRQ fields the catalog declares it cannot supply
    deltas: dict = field(default_factory=dict)            # approximated field -> seconds (ours - GRQ)
    daac_lagged: list[str] = field(default_factory=list)  # DAAC-side times later than GRQ by more than the report lag

    @property
    def exact_ok(self) -> bool:
        return not (self.exact_mismatches or self.file_mismatches)

    @property
    def approx_ok(self) -> bool:
        return not self.approx_out_of_tolerance

    @property
    def ok(self) -> bool:
        return self.exact_ok and self.approx_ok and not self.missing and not self.unexpected_extra

    def lines(self) -> list[str]:
        out = []
        for label, items in (("exact", self.exact_mismatches), ("files", self.file_mismatches),
                             ("approx", self.approx_out_of_tolerance), ("missing", self.missing),
                             ("extra", self.unexpected_extra), ("known", self.known_artifacts),
                             ("omitted", self.omitted)):
            out.extend(f"{label}: {item}" for item in items)
        return out


def _files_by_name(files):
    return {f["FileName"]: f for f in files or []}


def compare_files(ours: list[dict], grq: list[dict], dataset: Dataset, result: Comparison):
    spec = DATASETS[dataset]
    artifacts = KNOWN_GRQ_ARTIFACTS.get(dataset, set())
    ours_by, grq_by = _files_by_name(ours), _files_by_name(grq)
    allowed_extra = [rule.split(":", 1)[1] for rule in spec.files_not_in_grq if rule.startswith("extra:")]
    allowed_missing = [rule.split(":", 1)[1] for rule in spec.files_not_in_grq if rule.startswith("missing:")]
    for name in sorted(set(ours_by) | set(grq_by)):
        if name not in ours_by:
            if any(name.endswith(s) for s in allowed_missing):
                result.known_artifacts.append(f"file not delivered to the DAAC: {name}")
            else:
                result.file_mismatches.append(f"missing file {name}")
            continue
        if name not in grq_by:
            if any(name.endswith(s) for s in allowed_extra):
                result.known_artifacts.append(f"file delivered but absent from GRQ Files: {name}")
            else:
                result.file_mismatches.append(f"extra file {name}")
            continue
        a = {k: v for k, v in ours_by[name].items() if k not in FILE_IGNORED}
        b = {k: v for k, v in grq_by[name].items() if k not in FILE_IGNORED}
        for key in sorted(set(a) | set(b)):
            if a.get(key) == b.get(key):
                continue
            if f"Files.{key}" in artifacts:
                result.known_artifacts.append(f"Files[{name}].{key}: grq={b.get(key)!r}")
            else:
                result.file_mismatches.append(f"Files[{name}].{key}: ours={a.get(key)!r} grq={b.get(key)!r}")


def _ts_check(result, name, ours, grq, early: timedelta, late: timedelta):
    """ours may precede GRQ by up to `early` and follow it by up to `late`."""
    a, b = normalize_ts(ours), normalize_ts(grq)
    if a is None or b is None:
        return
    result.deltas[name] = (a - b).total_seconds()
    if a < b - early or a > b + late:
        result.approx_out_of_tolerance.append(f"{name}: ours={ours} grq={grq} (delta {a - b})")


def _center_distance(a: dict, b: dict) -> float | None:
    try:
        (x1, y1), (x2, y2) = a["coordinates"][:2], b["coordinates"][:2]
    except (KeyError, TypeError, ValueError):
        return None
    return math.hypot(x1 - x2, y1 - y2)


def compare_docs(ours: dict, grq: dict, tol: Tolerance | None = None) -> Comparison:
    tol = tol or Tolerance()
    dataset = Dataset(grq.get("dataset") or ours["dataset"])
    result = Comparison(id=grq.get("id") or ours.get("id"), dataset=dataset.value)
    artifacts = KNOWN_GRQ_ARTIFACTS.get(dataset, set())
    spec = DATASETS[dataset]
    om, gm = ours.get("metadata", {}), grq.get("metadata", {})
    omitted = set((ours.get("catalog") or {}).get("omitted") or [])

    for key in EXACT_TOP:
        if key in grq and ours.get(key) != grq[key]:
            result.exact_mismatches.append(f"{key}: ours={ours.get(key)!r} grq={grq[key]!r}")
    for key in EXACT_METADATA:
        if key not in gm:
            continue
        if key not in om and f"metadata.{key}" in omitted:
            result.omitted.append(f"metadata.{key}")
            continue
        if key == "FileSize" and spec.files_not_in_grq:
            result.known_artifacts.append(f"metadata.FileSize differs with the file set: grq={gm[key]}")
            continue
        if om.get(key) != gm[key]:
            if f"metadata.{key}" in artifacts:
                result.known_artifacts.append(f"metadata.{key}: ours={om.get(key)!r} grq={gm[key]!r}")
            else:
                result.exact_mismatches.append(f"metadata.{key}: ours={om.get(key)!r} grq={gm[key]!r}")
    # A GRQ document without CNM-R fields was delivered (the DAAC holds it) but its delivery response never
    # reached GRQ; the catalog's daac_* fields and daac_delivered tag are then expected additions.
    no_cnm_r = "daac_delivery_status" not in grq
    if no_cnm_r:
        result.known_artifacts.append("GRQ has no CNM-R record for this delivered product")
    for key in SET_METADATA:
        if key in gm and set(om.get(key) or []) != set(gm[key] or []):
            if no_cnm_r and key == "tags" and set(om.get(key) or []) == set(gm[key] or []) | {"daac_delivered"}:
                continue
            result.exact_mismatches.append(f"metadata.{key}: ours={om.get(key)!r} grq={gm[key]!r}")
    compare_files(om.get("Files"), gm.get("Files"), dataset, result)

    _ts_check(result, "creation_timestamp", ours.get("creation_timestamp"), grq.get("creation_timestamp"),
              tol.creation_max_early, tol.creation_max_late)
    _ts_check(result, "metadata.ProductReceivedTime", om.get("ProductReceivedTime"), gm.get("ProductReceivedTime"),
              tol.creation_max_early, tol.creation_max_late)
    unbounded = timedelta(days=36500)
    # GRQ's @timestamp is its document's last update, which can be any time after delivery.
    _ts_check(result, "@timestamp", ours.get("@timestamp"), grq.get("@timestamp"), unbounded, unbounded)
    for key in ("daac_CNM_S_timestamp", "daac_submission_timestamp", "daac_received_timestamp",
                "daac_process_complete_timestamp"):
        _ts_check(result, key, ours.get(key), grq.get(key), tol.daac_max_early, unbounded)
        if result.deltas.get(key, 0) > tol.daac_lag_report.total_seconds():
            result.daac_lagged.append(key)
    for key in ("starttime", "endtime"):
        _ts_check(result, key, ours.get(key), grq.get(key), tol.time_window, tol.time_window)
    if grq.get("center") and ours.get("center"):
        distance = _center_distance(ours["center"], grq["center"])
        if distance is not None and distance > tol.center_degrees:
            result.approx_out_of_tolerance.append(f"center: {distance:.3f} degrees apart")

    for key in sorted(set(grq) - set(ours)):
        if key not in NOT_COMPARED_TOP and key not in omitted:
            result.missing.append(key)
    for key in sorted(set(ours) - set(grq)):
        if key not in KNOWN_EXTRAS_TOP and key not in NOT_COMPARED_TOP and not (no_cnm_r and key.startswith("daac_")):
            result.unexpected_extra.append(key)
    for key in sorted(set(gm) - set(om)):
        if key not in NOT_COMPARED_METADATA and f"metadata.{key}" not in omitted:
            result.missing.append(f"metadata.{key}")
    for key in sorted(set(om) - set(gm)):
        if key not in KNOWN_EXTRAS_METADATA and key not in NOT_COMPARED_METADATA:
            result.unexpected_extra.append(f"metadata.{key}")
    return result
