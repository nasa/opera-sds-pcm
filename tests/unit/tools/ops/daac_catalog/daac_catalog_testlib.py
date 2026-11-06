"""Shared fixture loaders for the daac_catalog tests.

The fixtures are one delivered granule per dataset type: the live GRQ `_source` (job context removed), the S3
listings of the DAAC buckets, the ASF UMM-G sidecar and the gzipped ISO XML.
"""
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from tools.ops.daac_catalog.build import BuildContext, Tier1
from tools.ops.daac_catalog.parse import GranuleListing, ListedObject
from tools.ops.daac_catalog.registry import DATASETS, Source
from tools.ops.daac_catalog.sources import parse_iso, parse_ummg

FIXTURES = Path(__file__).parent / "fixtures"
BURST_DB = FIXTURES / "disp_s1_burst_db_f16655.json"
UMMG_LAST_MODIFIED = "2026-01-01T00:00:00+00:00"


def granule_ids() -> list[str]:
    return sorted(p.name for p in FIXTURES.iterdir() if p.is_dir())


def grq_hit(gid: str) -> dict:
    return json.loads((FIXTURES / gid / "grq.json").read_text())


def grq_doc(gid: str) -> dict:
    return grq_hit(gid)["_source"]


def spec_of(gid: str):
    return DATASETS[grq_doc(gid)["dataset"]]


def listing(gid: str, with_ummg: bool = True) -> GranuleListing:
    spec = spec_of(gid)
    objs = []
    for name, source in (("s3_products.json", Source.PRODUCTS), ("s3_browse.json", Source.BROWSE)):
        bucket = spec.bucket_prefix(source)[0]
        for row in json.loads((FIXTURES / gid / name).read_text()) or []:
            etag = row[3].strip('"') if len(row) > 3 else None
            objs.append(ListedObject(source.value, bucket, row[0], int(row[1]), row[2], etag))
    sidecar = FIXTURES / gid / f"{gid}.cmr.json"
    if with_ummg and sidecar.exists():
        objs.append(ListedObject(Source.UMMG.value, spec.product_bucket, f"{spec.ummg_prefix}{gid}/{gid}.cmr.json",
                                 sidecar.stat().st_size, UMMG_LAST_MODIFIED))
    return GranuleListing(gid, spec.dataset.value, objs)


def ummg_bytes(gid: str) -> bytes | None:
    sidecar = FIXTURES / gid / f"{gid}.cmr.json"
    return sidecar.read_bytes() if sidecar.exists() else None


def iso_bytes(gid: str) -> bytes:
    return gzip.decompress((FIXTURES / gid / f"{gid}.iso.xml.gz").read_bytes())


def tier1(gid: str, ummg: bool = True, iso: bool = True) -> Tier1:
    t1 = Tier1()
    if ummg and ummg_bytes(gid):
        t1.ummg = parse_ummg(ummg_bytes(gid))
    if iso:
        t1.iso = parse_iso(iso_bytes(gid))
    return t1


def context(frame_to_bursts=None) -> BuildContext:
    return BuildContext(run_id="inv-20261020T2100Z", listed_at=datetime(2026, 10, 20, 21, 0, tzinfo=timezone.utc),
                        built_at=datetime(2026, 10, 20, 21, 5, tzinfo=timezone.utc), frame_to_bursts=frame_to_bursts)


def gid_for(dataset: str) -> str:
    for gid in granule_ids():
        if grq_doc(gid)["dataset"] == dataset:
            return gid
    raise KeyError(dataset)
