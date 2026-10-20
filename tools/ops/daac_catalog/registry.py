"""Registry of the OPERA collections the SDS delivered to the DAACs.

One `DatasetSpec` per collection says where its granules live at the DAAC, how the bucket is laid out, how
granule ids are sharded for listing, and which filename regexes (from `conf/settings.yaml` PRODUCT_TYPES and
`conf/pge_outputs.yaml`) describe its files. Everything that is a property of a collection rather than of a
granule lives here.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from functools import cache
from typing import Callable

ASF_PRODUCTS_BUCKET = "asf-cumulus-prod-opera-products"
ASF_BROWSE_BUCKET = "asf-cumulus-prod-opera-browse"
PODAAC_PROTECTED_BUCKET = "podaac-ops-cumulus-protected"
PODAAC_PUBLIC_BUCKET = "podaac-ops-cumulus-public"

S3_REGION = "us-west-2"


class Dataset(StrEnum):
    L2_CSLC_S1 = "L2_CSLC_S1"
    L2_CSLC_S1_STATIC = "L2_CSLC_S1_STATIC"
    L2_RTC_S1 = "L2_RTC_S1"
    L2_RTC_S1_STATIC = "L2_RTC_S1_STATIC"
    L3_DISP_S1 = "L3_DISP_S1"
    L3_DISP_S1_STATIC = "L3_DISP_S1_STATIC"
    L3_DIST_S1 = "L3_DIST_S1"
    L4_TROPO = "L4_TROPO"
    L3_DSWX_HLS = "L3_DSWx_HLS"
    L3_DSWX_S1 = "L3_DSWx_S1"


class Daac(StrEnum):
    ASF = "ASF"
    PODAAC = "PODAAC"


class Layout(StrEnum):
    FOLDER = "folder"  # <prefix><granule id>/<files>  (ASF)
    FLAT = "flat"      # <prefix><granule id><suffix>  (PO.DAAC)


class Source(StrEnum):
    """The bucket a listed object came from, relative to its dataset."""
    PRODUCTS = "products"  # ASF products bucket, PO.DAAC protected bucket
    BROWSE = "browse"      # ASF browse bucket, PO.DAAC public bucket (browse, ISO XML, md5)
    UMMG = "ummg"          # ASF UMM-G sidecar under UMMG/<short name>/


def _track_shards() -> list[str]:
    return [f"T{track:03d}-" for track in range(1, 176)]


def _two_digit_shards(start: int = 0, stop: int = 100) -> list[str]:
    return [f"{n:02d}" for n in range(start, stop)]


def _utm_zone_shards() -> list[str]:
    return [f"{zone:02d}" for zone in range(1, 61)]


def _utm_zone_band_shards() -> list[str]:
    return [f"{zone:02d}{band}" for zone in range(1, 61) for band in "CDEFGHJKLMNPQRSTUVWX"]


def _year_shards() -> list[str]:
    return [str(year) for year in range(2014, datetime.now(timezone.utc).year + 2)]


@dataclass(frozen=True)
class DatasetSpec:
    dataset: Dataset
    daac: Daac
    short_name: str          # DAAC collection short name; GRQ metadata.CollectionName and daac_collection
    level: str               # GRQ dataset_level
    layout: Layout
    product_bucket: str
    product_prefix: str      # with trailing slash
    browse_bucket: str
    browse_prefix: str       # with trailing slash
    id_stem: str             # every granule id of the collection starts with this
    shard_suffixes: Callable[[], list[str]]
    natural_key_fields: tuple[str, ...]
    ummg_prefix: str | None = None
    # Files the DAAC holds that GRQ never listed in metadata.Files, or the reverse. See golden test notes.
    files_not_in_grq: tuple[str, ...] = field(default_factory=tuple)

    @property
    def products_role_readable(self) -> bool:
        """ASF products (and UMMG/) are listable with the SDS cluster instance roles; everything else needs
        DAAC s3credentials."""
        return self.daac == Daac.ASF

    def shards(self) -> list[str]:
        """Listing shards as id prefixes, e.g. `OPERA_L2_CSLC-S1_T001-`."""
        return [self.id_stem + suffix for suffix in self.shard_suffixes()]

    def bucket_prefix(self, source: Source) -> tuple[str, str]:
        if source == Source.PRODUCTS:
            return self.product_bucket, self.product_prefix
        if source == Source.BROWSE:
            return self.browse_bucket, self.browse_prefix
        if source == Source.UMMG:
            if not self.ummg_prefix:
                raise ValueError(f"{self.dataset} has no UMM-G sidecars")
            return self.product_bucket, self.ummg_prefix
        raise ValueError(source)

    def sources(self) -> list[Source]:
        return [Source.PRODUCTS, Source.BROWSE] + ([Source.UMMG] if self.ummg_prefix else [])

    def granule_prefix(self, source: Source, granule_id: str) -> str:
        """Key prefix under which every object of a granule lives in a source bucket."""
        bucket, prefix = self.bucket_prefix(source)
        if source == Source.UMMG or self.layout == Layout.FOLDER:
            return f"{prefix}{granule_id}/"
        return f"{prefix}{granule_id}"


def _asf(dataset, short_name, level, prefix, id_stem, shard_suffixes, natural_key_fields, **kw) -> DatasetSpec:
    return DatasetSpec(
        dataset=dataset, daac=Daac.ASF, short_name=short_name, level=level, layout=Layout.FOLDER,
        product_bucket=ASF_PRODUCTS_BUCKET, product_prefix=prefix,
        browse_bucket=ASF_BROWSE_BUCKET, browse_prefix=prefix,
        id_stem=id_stem, shard_suffixes=shard_suffixes, natural_key_fields=natural_key_fields,
        ummg_prefix=f"UMMG/{short_name}/", **kw)


def _podaac(dataset, short_name, level, id_stem, shard_suffixes, natural_key_fields, **kw) -> DatasetSpec:
    prefix = f"{short_name}/"
    return DatasetSpec(
        dataset=dataset, daac=Daac.PODAAC, short_name=short_name, level=level, layout=Layout.FLAT,
        product_bucket=PODAAC_PROTECTED_BUCKET, product_prefix=prefix,
        browse_bucket=PODAAC_PUBLIC_BUCKET, browse_prefix=prefix,
        id_stem=id_stem, shard_suffixes=shard_suffixes, natural_key_fields=natural_key_fields, **kw)


DATASETS: dict[Dataset, DatasetSpec] = {spec.dataset: spec for spec in (
    _asf(Dataset.L2_CSLC_S1, "OPERA_L2_CSLC-S1_V1", "L2", "OPERA_L2_CSLC-S1/", "OPERA_L2_CSLC-S1_",
         _track_shards, ("burst_id", "acquisition_ts", "sensor", "pol", "product_version")),
    _asf(Dataset.L2_CSLC_S1_STATIC, "OPERA_L2_CSLC-S1-STATIC_V1", "L2", "OPERA_L2_CSLC-S1_STATIC/",
         "OPERA_L2_CSLC-S1-STATIC_", _track_shards, ("burst_id", "validity_ts", "sensor", "product_version")),
    _asf(Dataset.L2_RTC_S1, "OPERA_L2_RTC-S1_V1", "L2", "OPERA_L2_RTC-S1/", "OPERA_L2_RTC-S1_",
         _track_shards, ("burst_id", "acquisition_ts", "sensor", "spacing", "product_version")),
    _asf(Dataset.L2_RTC_S1_STATIC, "OPERA_L2_RTC-S1-STATIC_V1", "L2", "OPERA_L2_RTC-S1_STATIC/",
         "OPERA_L2_RTC-S1-STATIC_", _track_shards,
         ("burst_id", "validity_ts", "sensor", "spacing", "product_version"),
         files_not_in_grq=("missing:.h5",)),
    _asf(Dataset.L3_DISP_S1, "OPERA_L3_DISP-S1_V1", "L3", "OPERA_L3_DISP-S1_V1/", "OPERA_L3_DISP-S1_IW_F",
         lambda: _two_digit_shards(0, 50),
         ("frame_id", "pol", "ref_datetime", "sec_datetime", "product_version")),
    _asf(Dataset.L3_DISP_S1_STATIC, "OPERA_L3_DISP-S1-STATIC_V1", "L3", "OPERA_L3_DISP-S1-STATIC_V1/",
         "OPERA_L3_DISP-S1-STATIC_F", lambda: _two_digit_shards(0, 50),
         ("frame_id", "validity_start_date", "sensor", "product_version")),
    _asf(Dataset.L3_DIST_S1, "OPERA_L3_DIST-ALERT-S1_V1", "L3", "OPERA_L3_DIST-ALERT-S1_V1/",
         "OPERA_L3_DIST-ALERT-S1_T", _utm_zone_shards,
         ("tile_id", "acquisition_ts", "sensor", "spacing", "product_version")),
    _asf(Dataset.L4_TROPO, "OPERA_L4_TROPO-ZENITH_V1", "L4", "OPERA_L4_TROPO-ZENITH_V1/",
         "OPERA_L4_TROPO-ZENITH_", _year_shards, ("acquisition_ts", "model", "product_version"),
         files_not_in_grq=("extra:.nc",)),
    _podaac(Dataset.L3_DSWX_HLS, "OPERA_L3_DSWX-HLS_V1", "L3", "OPERA_L3_DSWx-HLS_T", _utm_zone_band_shards,
            ("tile_id", "acquisition_ts", "sensor", "spacing", "product_version")),
    _podaac(Dataset.L3_DSWX_S1, "OPERA_L3_DSWX-S1_V1", "L3", "OPERA_L3_DSWx-S1_T", _utm_zone_band_shards,
            ("tile_id", "acquisition_ts", "sensor", "spacing", "product_version"),
            files_not_in_grq=("extra:_BROWSE.tif",)),
)}


def get_spec(dataset: str | Dataset) -> DatasetSpec:
    try:
        return DATASETS[Dataset(dataset)]
    except ValueError:
        raise ValueError(f"unknown dataset {dataset!r}; choose from {[d.value for d in Dataset]}") from None


def resolve_datasets(names: list[str] | str) -> list[DatasetSpec]:
    """`all`, one name, or a comma-separated list."""
    if isinstance(names, str):
        names = [n for n in names.split(",") if n]
    if not names or names == ["all"]:
        return list(DATASETS.values())
    return [get_spec(n) for n in names]


# ---- regexes from the PCM configuration -------------------------------------------------------------------------

@cache
def _settings_product_types() -> dict:
    from util.conf_util import SettingsConf
    return SettingsConf().cfg["PRODUCT_TYPES"]


@cache
def _pge_outputs() -> dict:
    from util.conf_util import PGEOutputsConf
    return PGEOutputsConf().cfg


def product_type_config(dataset: Dataset) -> dict:
    """The settings.yaml PRODUCT_TYPES entry the HySDS extractor applies to every file of the dataset."""
    return _settings_product_types()[dataset.value]


def primary_patterns(dataset: Dataset) -> list[re.Pattern]:
    """pge_outputs.yaml Primary regexes: the files product2dataset turns into metadata.Files entries."""
    return [entry["regex"] for entry in _pge_outputs()[dataset.value]["Outputs"]["Primary"]]


def named_group_source(pattern: str, name: str) -> str:
    """Source text of the named group `(?P<name>...)` inside a regex, found by balancing parentheses."""
    start = pattern.find(f"(?P<{name}>")
    if start < 0:
        raise ValueError(f"no group {name!r} in {pattern!r}")
    depth = 0
    i = start
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            i += 2
            continue
        if c == "[":
            # skip a character class, which may contain parentheses
            j = i + 1
            if j < len(pattern) and pattern[j] == "^":
                j += 1
            if j < len(pattern) and pattern[j] == "]":
                j += 1
            while j < len(pattern) and pattern[j] != "]":
                j += 2 if pattern[j] == "\\" else 1
            i = j + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return pattern[start:i + 1]
        i += 1
    raise ValueError(f"unbalanced group {name!r} in {pattern!r}")


@cache
def id_pattern(dataset: Dataset) -> re.Pattern:
    """Matches a granule id at the start of any file name of the dataset (data, ISO XML, browse, md5)."""
    source = named_group_source(product_type_config(dataset)["Pattern"].pattern, "id")
    return re.compile(source)


def granule_id_of(dataset: Dataset, file_name: str) -> str | None:
    """Granule id of a file name. Works for every file the DAAC holds for a granule, including `.md5` twins and
    ASF-made derivatives, because the id is always a prefix of the file name."""
    match = id_pattern(dataset).match(file_name)
    return match.group("id") if match else None


def is_granule_id(dataset: Dataset, text: str) -> bool:
    match = id_pattern(dataset).fullmatch(text)
    return match is not None
