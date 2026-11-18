"""What the burst coverage audit expects: polarizations per scene, production start dates, CBDB membership.

The audit used to expand every burst into every requested polarization, so a run with VV and HH reported
each VV scene's HH CSLCs (and each HH scene's VV CSLCs) missing; it was run VV-only and HH scenes went
unaudited. It also had no notion of when OPERA started producing for a platform, and no way to line its
output up with the DISP-S1 consistent burst database that PST's products tracker scopes by.
"""
import asyncio
import json
from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from tools.ops.cmr_audit import cmr_audit_burst_coverage as cov

UTC = timezone.utc
VV_SCENE = "S1C_IW_SLC__1SDV_20261021T004700_20261021T004731_009976_013E4D_0CCC-SLC"
HH_SCENE = "S1C_IW_SLC__1SDH_20260807T104726_20260807T104757_008888_011A18_F21F-SLC"
OLD_S1D_SCENE = "S1D_IW_SLC__1SDV_20260410T140818_20260410T140843_001133_0071F0_4EA8-SLC"


def _slc(native_id, bursts):
    slc = cov.SLCGranule.from_native_id(native_id)
    slc.bursts = [cov.BurstInfo.from_asf_id(b) for b in bursts]
    return slc


@pytest.mark.parametrize("native_id, mode, owed", [
    (VV_SCENE, "SDV", ["VV"]),
    (HH_SCENE, "SDH", ["HH"]),
    ("S1A_IW_SLC__1SSV_20200101T000000_20200101T000027_030000_037000_ABCD-SLC", "SSV", ["VV"]),
    ("S1A_IW_SLC__1SSH_20200101T000000_20200101T000027_030000_037000_ABCD-SLC", "SSH", ["HH"]),
])
def test_a_scene_owes_only_the_requested_polarizations_its_mode_holds(native_id, mode, owed):
    slc = cov.SLCGranule.from_native_id(native_id)
    assert slc.mode == mode
    assert slc.owed_polarizations(["VV", "HH"]) == owed


def test_cross_polarizations_follow_the_mode_too():
    slc = cov.SLCGranule.from_native_id(VV_SCENE)
    assert slc.owed_polarizations(["HH", "VH", "VV"]) == ["VH", "VV"]
    assert slc.owed_polarizations(["HH"]) == []


def test_an_unrecognised_mode_keeps_every_requested_polarization():
    slc = cov.SLCGranule.from_native_id(VV_SCENE)
    slc.mode = ""
    assert slc.owed_polarizations(["VV", "HH"]) == ["VV", "HH"]


def test_vv_and_hh_together_expect_each_scene_in_its_own_co_polarization():
    slcs = [_slc(VV_SCENE, ["078_165489_IW1", "078_165489_IW2"]), _slc(HH_SCENE, ["040_084571_IW1"])]

    async def bursts_already_derived(slc, session, sem, polarization=None):
        return slc.bursts

    with patch.object(cov, "fetch_bursts_for_slc", bursts_already_derived):
        raw, expected = asyncio.run(cov.process_slcs_to_expected_bursts(slcs, ["VV", "HH"]))

    assert raw == 3
    assert sorted((e.burst.filename_pattern, e.polarization) for e in expected) == [
        ("T040-084571-IW1", "HH"), ("T078-165489-IW1", "VV"), ("T078-165489-IW2", "VV")]


def test_default_polarizations_are_vv_and_hh():
    args = cov.create_parser().parse_args(["--start-datetime", "x", "--end-datetime", "y", "-g", "z"])
    assert args.polarizations == ["VV", "HH"]


# --- production start ------------------------------------------------------------------------------

def test_production_start_defaults_follow_the_agreed_dates():
    starts = cov.resolve_production_start(["CSLC-S1", "RTC-S1"])
    assert starts["CSLC-S1"] == {"S1C": date(2025, 5, 20), "S1D": date(2026, 4, 17)}
    assert starts["RTC-S1"] == {"S1C": date(2025, 5, 19), "S1D": date(2026, 4, 17)}


def test_production_start_overrides():
    starts = cov.resolve_production_start(["CSLC-S1", "RTC-S1"], ["S1D=2026-06-13", "s1c=none"])
    assert starts["CSLC-S1"] == {"S1D": date(2026, 6, 13)} and starts["RTC-S1"] == {"S1D": date(2026, 6, 13)}
    assert cov.resolve_production_start(["CSLC-S1"], ["none"]) == {"CSLC-S1": {}}
    assert cov.resolve_production_start(["CSLC-S1"], ["none", "S1A=2016-01-01"]) == {"CSLC-S1": {"S1A": date(2016, 1, 1)}}
    for bad in ("S1D", "S1E=2026-01-01", "S1D=2026-13-01"):
        with pytest.raises(ValueError):
            cov.resolve_production_start(["CSLC-S1"], [bad])


def test_bursts_sensed_before_their_platforms_start_are_split_off():
    def exp(platform, day):
        return cov.ExpectedBurst(cov.BurstInfo(42, 88937, "IW1"), datetime(2026, 4, day, 14, 8, tzinfo=UTC),
                                 platform, "VV", "x")
    starts = {"S1D": date(2026, 4, 17)}
    owed, excluded = cov.split_by_production_start([exp("S1D", 16), exp("S1D", 17), exp("S1A", 1)], starts)
    assert excluded == 1
    assert [(e.platform, e.acquisition_time.day) for e in owed] == [("S1D", 17), ("S1A", 1)]


# --- CBDB membership -------------------------------------------------------------------------------

def _cbdb(tmp_path):
    db = {"metadata": {}, "data": {
        "10572": {"burst_id_list": ["t040_084571_iw1", "t040_084571_iw2"], "sensing_time_list": []},
        "10573": {"burst_id_list": ["t040_084571_iw2"], "sensing_time_list": []},
    }}
    path = tmp_path / "cbdb.json"
    path.write_text(json.dumps(db))
    return path


def test_cbdb_bursts_map_to_their_frames_in_the_audits_burst_form(tmp_path):
    frames = cov.load_cbdb_burst_frames(str(_cbdb(tmp_path)))
    assert frames == {"T040-084571-IW1": [10572], "T040-084571-IW2": [10572, 10573]}


def test_annotation_adds_frames_to_every_item_and_counts_the_ones_in_the_cbdb():
    items = [{"burst_pattern": "T040-084571-IW2"}, {"burst_pattern": "T078-165489-IW1"}]
    assert cov.annotate_cbdb_frames(items, {"T040-084571-IW2": [10572, 10573]}) == 1
    assert [i["cbdb_frames"] for i in items] == [[10572, 10573], []]


# --- end to end over one chunk ---------------------------------------------------------------------

def test_audit_checks_owed_bursts_only_and_reports_scope(tmp_path):
    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "Feature", "properties": {},
                                   "geometry": {"type": "Polygon", "coordinates": [[[-180, -80], [180, -80], [180, 85], [-180, 85], [-180, -80]]]}}))
    points = {"umm": {"SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": {"GPolygons": [{"Boundary": {"Points": [
        {"Longitude": -70.0, "Latitude": 44.0}, {"Longitude": -69.0, "Latitude": 44.0},
        {"Longitude": -69.0, "Latitude": 45.0}, {"Longitude": -70.0, "Latitude": 45.0}]}}]}}}}}
    bursts = {VV_SCENE: ["078_165489_IW1"], HH_SCENE: ["040_084571_IW1"], OLD_S1D_SCENE: ["042_088937_IW1"]}
    checked = []

    async def listing(start, end, bbox):
        return set(bursts), {nid: points for nid in bursts}

    async def derived(slc, session, sem, polarization=None):
        return [cov.BurstInfo.from_asf_id(b) for b in bursts[slc.native_id]]

    async def coverage(expected_bursts, product_type, use_grq=False):
        checked.extend((e.burst.filename_pattern, e.polarization) for e in expected_bursts)
        return [], [e.to_dict() for e in expected_bursts], []

    out = tmp_path / "audit.jsonl"
    with patch.object(cov, "fetch_slc_granules", listing), patch.object(cov, "fetch_bursts_for_slc", derived), \
            patch.object(cov, "check_coverage_for_bursts", coverage):
        results = asyncio.run(cov.audit_burst_coverage(
            start_datetime=datetime(2026, 4, 1, tzinfo=UTC), end_datetime=datetime(2026, 11, 1, tzinfo=UTC),
            geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV", "HH"],
            low_memory=True, output_path=str(out), chunk_days=400, buffer_deg=0.0,
            production_start=cov.resolve_production_start(["CSLC-S1"]),
            cbdb_frames={"T040-084571-IW1": [10572]}))

    # The VV scene in VV, the HH scene in HH, and the April S1D scene (before 2026-04-17) not at all.
    assert sorted(checked) == [("T040-084571-IW1", "HH"), ("T078-165489-IW1", "VV")]
    product = results["products"]["CSLC-S1"]
    assert (product["expected_count"], product["missing_count"], product["excluded_before_start_count"]) == (2, 2, 1)
    assert product["missing_in_cbdb_count"] == 1
    assert results["metadata"]["production_start"]["CSLC-S1"] == {"S1C": "2025-05-20", "S1D": "2026-04-17"}

    records = [json.loads(line) for line in out.read_text().splitlines()]
    meta = next(r for r in records if r["_type"] == "metadata")
    assert meta["polarizations"] == ["VV", "HH"] and meta["cbdb"] is True and meta["buffer_deg"] == 0.0
    chunk = next(r for r in records if r["_type"] == "chunk_result")
    assert chunk["excluded_before_start_count"] == 1
    assert {m["burst_pattern"]: m["cbdb_frames"] for m in chunk["missing"]} == {
        "T040-084571-IW1": [10572], "T078-165489-IW1": []}


# --- scenes whose bursts cannot be derived -----------------------------------------------------------

def test_concurrent_derivations_ask_edl_for_one_token(monkeypatch):
    calls = []

    def token():
        calls.append(1)
        return "tok"

    monkeypatch.setattr(cov, "_edl_token", None)
    monkeypatch.setattr(cov, "_edl_lock", None)
    monkeypatch.setattr(cov, "get_edl_token", token)

    async def many():
        return await asyncio.gather(*[cov._get_edl_token_once() for _ in range(20)])

    assert asyncio.run(many()) == ["tok"] * 20
    assert len(calls) == 1


def test_a_refused_token_is_retried_then_fails_every_derivation_loudly(monkeypatch):
    attempts = []

    def refused():
        attempts.append(1)
        raise RuntimeError("403 Client Error: Forbidden")

    async def no_sleep(_):
        return None

    monkeypatch.setattr(cov, "_edl_token", None)
    monkeypatch.setattr(cov, "_edl_lock", None)
    monkeypatch.setattr(cov, "get_edl_token", refused)
    monkeypatch.setattr(cov.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(cov, "get_grq", lambda: None)
    empty_cache = cov.RequestCache(enabled=False)
    monkeypatch.setattr(cov, "get_cache", lambda: empty_cache)

    slc = cov.SLCGranule.from_native_id(VV_SCENE)
    with pytest.raises(cov.BurstDerivationError, match="no EDL token"):
        asyncio.run(cov.fetch_bursts_for_slc(slc, None, None))
    assert len(attempts) == cov.EDL_TOKEN_ATTEMPTS


def test_underived_scenes_are_reported_and_make_the_run_incomplete(tmp_path):
    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "Feature", "properties": {},
                                   "geometry": {"type": "Polygon", "coordinates": [[[-180, -80], [180, -80], [180, 85], [-180, 85], [-180, -80]]]}}))
    points = {"umm": {"SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": {"GPolygons": [{"Boundary": {"Points": [
        {"Longitude": -70.0, "Latitude": 44.0}, {"Longitude": -69.0, "Latitude": 44.0},
        {"Longitude": -69.0, "Latitude": 45.0}, {"Longitude": -70.0, "Latitude": 45.0}]}}]}}}}}

    async def listing(start, end, bbox):
        return {VV_SCENE, HH_SCENE}, {VV_SCENE: points, HH_SCENE: points}

    async def derived(slc, session, sem, polarization=None):
        if slc.native_id == HH_SCENE:
            raise cov.BurstDerivationError("annotation derivation failed: timeout")
        return [cov.BurstInfo.from_asf_id("078_165489_IW1")]

    async def coverage(expected_bursts, product_type, use_grq=False):
        return [{**e.to_dict(), "opera_product_id": "x"} for e in expected_bursts], [], []

    out = tmp_path / "audit.jsonl"
    with patch.object(cov, "fetch_slc_granules", listing), patch.object(cov, "fetch_bursts_for_slc", derived), \
            patch.object(cov, "check_coverage_for_bursts", coverage):
        results = asyncio.run(cov.audit_burst_coverage(
            start_datetime=datetime(2026, 8, 1, tzinfo=UTC), end_datetime=datetime(2026, 11, 1, tzinfo=UTC),
            geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV", "HH"],
            low_memory=True, output_path=str(out), chunk_days=400, buffer_deg=0.0))

    # Everything checked was found, but one scene was never checked: that is not a clean run.
    assert results["products"]["CSLC-S1"]["missing_count"] == 0
    assert [u["slc_native_id"] for u in results["metadata"]["underived_slcs"]] == [HH_SCENE]
    assert cov.exit_code_for(results) == 2
    records = [json.loads(line) for line in out.read_text().splitlines()]
    assert next(r for r in records if r["_type"] == "chunk_underived")["slcs"][0]["reason"].startswith("annotation")
    assert next(r for r in records if r["_type"] == "summary")["underived_slcs"][0]["slc_native_id"] == HH_SCENE
