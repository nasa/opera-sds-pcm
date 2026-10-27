from datetime import datetime, timezone

import pytest

from daac_catalog_testlib import gid_for, granule_ids, grq_doc, iso_bytes, ummg_bytes
from tools.ops.daac_catalog.sources import (
    fmt_creation, fmt_micro_z, md5_from_twin, normalize_ts, parse_iso, parse_list_attribute, parse_ummg,
    polygon_from_text, to_grq_geojson)

UTC = timezone.utc


@pytest.mark.parametrize("text,expected", [
    ("2026-06-01 08:05:02.165540", datetime(2026, 6, 1, 8, 5, 2, 165540, UTC)),
    ("2026-08-02T20:14:59.367871Z", datetime(2026, 8, 2, 20, 14, 59, 367871, UTC)),
    ("2026-06-27T22:44:23.1023200Z", datetime(2026, 6, 27, 22, 44, 23, 102320, UTC)),
    ("2026-06-11T02:05:30.6123290000Z", datetime(2026, 6, 11, 2, 5, 30, 612329, UTC)),
    ("2014-04-03", datetime(2014, 4, 3, tzinfo=UTC)),
    ("2025-09-20T04:25:02Z", datetime(2025, 9, 20, 4, 25, 2, tzinfo=UTC)),
    ("2026-10-20 22:11:31+00:00", datetime(2026, 10, 20, 22, 11, 31, tzinfo=UTC)),
    ("2026-10-20T15:11:31-07:00", datetime(2026, 10, 20, 22, 11, 31, tzinfo=UTC)),
    ("20260531T110654Z", datetime(2026, 5, 31, 11, 6, 54, tzinfo=UTC)),
    ("20140403", datetime(2014, 4, 3, tzinfo=UTC)),
    ("2026-07-28 00:00:00", datetime(2026, 7, 28, tzinfo=UTC)),
])
def test_normalize_ts(text, expected):
    assert normalize_ts(text) == expected


def test_normalize_ts_edge_cases():
    assert normalize_ts(None) is None and normalize_ts("") is None
    assert normalize_ts(datetime(2026, 1, 1)).tzinfo == UTC
    with pytest.raises(ValueError):
        normalize_ts("yesterday")


def test_formats_match_grq():
    dt = datetime(2026, 6, 1, 8, 5, 2, 165540, UTC)
    assert fmt_creation(dt) == "2026-06-01T08:05:02.165"
    assert fmt_micro_z(dt) == "2026-06-01T08:05:02.165540Z"


@pytest.mark.parametrize("gid", granule_ids())
def test_iso_versions_match_grq(gid):
    iso = parse_iso(iso_bytes(gid))
    grq = grq_doc(gid)["metadata"]
    assert iso.sas_version == grq["sas_version"]
    if grq["ProductType"] == "L4_TROPO":
        assert grq["pge_version"].startswith(iso.pge_version)   # catalog.json added a ".0"
    else:
        assert iso.pge_version == grq["pge_version"]


@pytest.mark.parametrize("gid", granule_ids())
def test_every_fixture_has_time_and_space(gid):
    iso = parse_iso(iso_bytes(gid))
    sidecar = ummg_bytes(gid)
    geometry = parse_ummg(sidecar).geometry if sidecar else iso.geometry()
    assert geometry is not None and geometry.is_valid and geometry.area > 0
    begin = parse_ummg(sidecar).begin if sidecar else iso.begin
    assert begin is not None


@pytest.mark.parametrize("dataset,kind", [
    ("L2_CSLC_S1", "Polygon"), ("L2_RTC_S1", "Polygon"), ("L3_DISP_S1", "Polygon"),
    ("L3_DISP_S1_STATIC", "Polygon"), ("L3_DSWx_HLS", "Polygon"), ("L3_DIST_S1", "Polygon"),
])
def test_iso_geometry_variants(dataset, kind):
    geometry = parse_iso(iso_bytes(gid_for(dataset))).geometry()
    assert geometry.geom_type == kind
    assert -180 <= geometry.bounds[0] <= geometry.bounds[2] <= 180


def test_iso_without_polygon_or_bbox_has_no_geometry():
    iso = parse_iso(iso_bytes(gid_for("L2_RTC_S1_STATIC")))
    assert iso.geometry() is None


def test_polygon_text_variants():
    assert polygon_from_text("(1 2 3 2 3 4 1 2)").geom_type == "Polygon"
    assert polygon_from_text("((1 2 3 2 3 4 1 2))").geom_type == "Polygon"
    assert polygon_from_text("POLYGON ((1 2, 3 2, 3 4, 1 2))").geom_type == "Polygon"
    assert polygon_from_text("MULTIPOLYGON (((1 2, 3 2, 3 4, 1 2)))").geom_type == "MultiPolygon"
    assert polygon_from_text("") is None and polygon_from_text("(1 2 3)") is None
    gj = to_grq_geojson(polygon_from_text("(1 2 3 2 3 4 1 2)"))
    assert gj["type"] == "polygon" and gj["coordinates"][0][0] == [1.0, 2.0]


def test_ummg_fields():
    info = parse_ummg(ummg_bytes(gid_for("L2_CSLC_S1")))
    assert info.insert_time == datetime(2026, 6, 1, 8, 47, 52, tzinfo=UTC)
    assert info.production_time == datetime(2026, 6, 1, 8, 5, 2, tzinfo=UTC)
    assert info.pge_version == "2.1.3" and info.sas_version == "0.5.6"
    assert info.input_granules == ["S1A_IW_SLC__1SDV_20260531T110629_20260531T110659_064760_0828CC_379B"]
    assert info.attribute("OPERA_BURST_ID") == "T113_241595_IW1"
    assert len(info.checksums) == 2 and all(len(v) == 32 for v in info.checksums.values())
    tropo = parse_ummg(ummg_bytes(gid_for("L4_TROPO")))
    assert tropo.geometry.bounds == (-180.0, -89.94619, 179.92969, 89.94619)
    static = parse_ummg(ummg_bytes(gid_for("L2_CSLC_S1_STATIC")))
    assert static.begin == static.end == datetime(2014, 4, 3, tzinfo=UTC)
    wrapped = parse_ummg({"meta": {}, "umm": {"GranuleUR": "x"}})
    assert wrapped.granule_ur == "x"


def test_list_attributes():
    assert parse_list_attribute('["a", "b"]') == ["a", "b"]
    assert parse_list_attribute("['a', 'b']") == ["a", "b"]
    assert parse_list_attribute("a,b, c") == ["a", "b", "c"]
    assert parse_list_attribute("single") == ["single"]
    assert parse_list_attribute(None) == []


def test_md5_twin():
    assert md5_from_twin(b"4071E49055C6C28DA55FBFC05573C5CC\n") == "4071e49055c6c28da55fbfc05573c5cc"
    assert md5_from_twin(b"4071e49055c6c28da55fbfc05573c5cc  file.h5\n") == "4071e49055c6c28da55fbfc05573c5cc"
    assert md5_from_twin(b"not a checksum") is None
