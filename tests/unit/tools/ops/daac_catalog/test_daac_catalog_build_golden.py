"""Golden test: rebuild each live GRQ document from DAAC-side fixtures only and compare field by field."""
import pytest

from daac_catalog_testlib import BURST_DB, context, gid_for, granule_ids, grq_doc, grq_hit, listing, spec_of, tier1
from tools.ops.daac_catalog.build import ALWAYS_OMITTED, build_doc, index_name
from tools.ops.daac_catalog.compare import compare_docs
from tools.ops.daac_catalog.parse import parse_granule
from tools.ops.daac_catalog.tier2 import load_burst_db


def _build(gid, frame_to_bursts=None, **tier1_kwargs):
    return build_doc(parse_granule(spec_of(gid), listing(gid)), context(frame_to_bursts), tier1(gid, **tier1_kwargs))


@pytest.fixture(scope="module")
def frame_to_bursts():
    return load_burst_db(str(BURST_DB))


@pytest.mark.parametrize("gid", granule_ids())
def test_golden_document_matches_grq(gid, frame_to_bursts):
    ours = _build(gid, frame_to_bursts)
    result = compare_docs(ours, grq_doc(gid))
    assert result.exact_ok, result.lines()
    assert result.approx_ok, result.lines()
    assert not result.missing, result.lines()
    assert not result.unexpected_extra, result.lines()


@pytest.mark.parametrize("gid", granule_ids())
def test_golden_index_name_matches_grq_month(gid):
    ours = _build(gid)
    assert index_name(ours, prefix="grq_") == grq_hit(gid)["_index"]


@pytest.mark.parametrize("gid", granule_ids())
def test_golden_catalog_block(gid):
    doc = _build(gid)
    catalog = doc["catalog"]
    assert catalog["schema_version"] == "1.0"
    assert catalog["source"] == "daac-s3"
    assert catalog["tier"] == 1
    assert catalog["natural_key"].startswith(doc["dataset"] + "|")
    assert set(ALWAYS_OMITTED) <= set(catalog["omitted"])
    assert "creation_timestamp" in catalog["approximated"]
    assert catalog["files"] and all("role" in f and "key" in f for f in catalog["files"])
    for field in catalog["omitted"]:
        top, _, sub = field.partition(".")
        container = doc.get(top) if sub else doc
        assert not (isinstance(container, dict) and (sub or top) in container), f"{field} is omitted but present"
    for key in ("starttime", "endtime", "location", "center"):
        assert key in doc, key
    assert doc["location"]["type"] in ("polygon", "multipolygon")
    assert doc["center"]["type"] == "point"
    assert "is_backfilled" not in doc


def test_disp_s1_cycle_fields_need_the_burst_db():
    gid = gid_for("L3_DISP_S1")
    doc = _build(gid)
    assert "acquisition_cycle" not in doc["metadata"]
    assert "input_granule_id" not in doc["metadata"]
    assert {"metadata.acquisition_cycle", "metadata.input_granule_id"} <= set(doc["catalog"]["omitted"])


def test_disp_s1_ksc_id_and_cycle_from_iso_inputs(frame_to_bursts):
    gid = gid_for("L3_DISP_S1")
    doc = _build(gid, frame_to_bursts)
    expected = grq_doc(gid)["metadata"]
    assert doc["metadata"]["input_granule_id"] == expected["input_granule_id"]
    assert doc["metadata"]["acquisition_cycle"] == expected["acquisition_cycle"] == 984
    assert doc["metadata"]["frame_id"] == 16655
    assert len(doc["metadata"]["lineage"]) == 51


def test_tier0_document_from_listing_only():
    gid = gid_for("L2_CSLC_S1")
    doc = build_doc(parse_granule(spec_of(gid), listing(gid)), context())
    assert doc["catalog"]["tier"] == 0
    assert doc["creation_timestamp"] == "2026-06-01T07:46:26.000"   # file name creation_ts
    assert doc["catalog"]["derivation"]["creation_timestamp"] == "filename:creation_ts"
    assert doc["starttime"] == doc["endtime"] == "2026-05-31T11:06:54Z"
    assert {"starttime", "endtime"} <= set(doc["catalog"]["approximated"])
    assert {"location", "center", "metadata.pge_version", "metadata.sas_version"} <= set(doc["catalog"]["omitted"])
    assert "location" not in doc
    result = compare_docs(doc, grq_doc(gid))
    assert result.exact_ok, result.lines()


def test_ummg_only_tier1_keeps_versions_and_geometry():
    gid = gid_for("L2_RTC_S1")
    doc = _build(gid, iso=False)
    assert doc["metadata"]["pge_version"] == "2.1.4"
    assert doc["metadata"]["sas_version"] == "1.0.4"
    assert doc["metadata"]["polarizations"] == ["HH", "HV"]
    assert "orbit_file" not in doc["metadata"]
    assert "metadata.orbit_file" in doc["catalog"]["omitted"]
    assert doc["catalog"]["derivation"]["location"] == "ummg:SpatialExtent"


def test_daac_urls_point_at_the_daac():
    gid = gid_for("L3_DSWx_HLS")
    doc = _build(gid)
    assert doc["urls"][0] == f"s3://podaac-ops-cumulus-protected/OPERA_L3_DSWX-HLS_V1/{gid}"
    assert doc["browse_urls"][0] == f"s3://podaac-ops-cumulus-public/OPERA_L3_DSWX-HLS_V1/{gid}"
    assert all(u.startswith("s3://podaac-ops-cumulus-protected/") for u in doc["metadata"]["product_s3_paths"])
    assert doc["daac_received_timestamp"].endswith("Z") and "T" in doc["daac_received_timestamp"]
    gid = gid_for("L2_CSLC_S1")
    doc = _build(gid)
    assert doc["urls"][0] == f"s3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1/{gid}"
    assert f"s3://asf-cumulus-prod-opera-browse/OPERA_L2_CSLC-S1/{gid}/{gid}_BROWSE.png" in \
        doc["metadata"]["product_s3_paths"]
    assert doc["daac_received_timestamp"].endswith("Z") and " " in doc["daac_received_timestamp"]
    assert any(u.endswith(".cmr.json") for u in doc["daac_product_file_urls"])
    assert not any(u.endswith(".md5") for u in doc["archive_product_urls"])
    assert not any("_low-res" in u for u in doc["archive_product_urls"])


def test_lineage_inputs_resolve_to_asf_locations():
    gid = gid_for("L3_DIST_S1")
    doc = _build(gid)
    derived = doc["prov"]["wasDerivedFrom"]
    assert derived and all(u.startswith("url:s3://asf-cumulus-prod-opera-products/OPERA_L2_RTC-S1/") for u in derived)
    gid = gid_for("L3_DSWx_S1")
    doc = _build(gid)
    assert all(u.endswith("_VV.tif") for u in doc["prov"]["wasDerivedFrom"])
    assert doc["metadata"]["rtc_input_list"] == grq_doc(gid)["metadata"]["rtc_input_list"]
