"""The DISP-S1 smoke test's PGE-fed forward stage judges each step correctly.

The stage can only run on a cluster; these tests cover the decisions it makes about the
documents it reads back.
"""
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

CHECKER = (Path(__file__).parents[3] / "conf" / "sds" / "files" / "test"
           / "check_disp_s1_pge_cslc_feed.py")


def _load_checker():
    commons = types.ModuleType("opera_commons")
    es = types.ModuleType("opera_commons.es_connection")
    es.get_grq_es = lambda *a, **k: None
    es.get_mozart_es = lambda *a, **k: None
    commons.es_connection = es
    with mock.patch.dict(sys.modules, {"opera_commons": commons, "opera_commons.es_connection": es}):
        spec = importlib.util.spec_from_file_location("_check_disp_s1_pge_cslc_feed", CHECKER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


feed = _load_checker()

BURSTS = ["T117-249921-IW3", "T117-249922-IW3"]
DATE = "20190613"
BUCKET = "s3://opera-dev-rs-fwd-gmanipon/products/CSLC_S1/2019/06/13"


def _cslc(burst_id, processed="20260911T010203Z", pol="VV", tags=("PGE",), promoted=True,
          sensing=f"{DATE}T172935Z"):
    stem = f"OPERA_L2_CSLC-S1_{burst_id}_{sensing}_{processed}_S1A_{pol}_v1.1"
    meta = {"tags": list(tags),
            "product_s3_paths": [f"{BUCKET}/{stem}/{stem}_BROWSE.png", f"{BUCKET}/{stem}/{stem}.h5"]}
    source = {"metadata": meta}
    if promoted:
        meta["burst_id"] = burst_id
        source["starttime"] = "2019-06-13T17:29:35.000000Z"
    return {"_id": stem, "_source": source}


class SelectPgeCslcsTest(unittest.TestCase):

    def test_keeps_the_newest_pge_vv_product_per_burst(self):
        hits = [_cslc(BURSTS[0], processed="20260911T010203Z"),
                _cslc(BURSTS[0], processed="20260912T010203Z"),
                _cslc(BURSTS[1]),
                _cslc(BURSTS[1], pol="VH", processed="20260913T000000Z"),
                _cslc(BURSTS[1], tags=(), processed="20260914T000000Z"),
                _cslc("T001-000001-IW1"),
                _cslc(BURSTS[0], sensing="20190601T172935Z", processed="20260915T000000Z")]

        selected = feed.select_pge_cslcs(hits, BURSTS, DATE)

        self.assertEqual(sorted(selected), BURSTS)
        self.assertIn("20260912T010203Z", selected[BURSTS[0]]["_id"])
        self.assertIn("20260911T010203Z", selected[BURSTS[1]]["_id"])


class CheckStepsTest(unittest.TestCase):

    def test_cslcs_pass_when_every_burst_carries_the_promoted_fields(self):
        selected = {b: _cslc(b) for b in BURSTS}

        self.assertEqual(feed.check_cslcs(selected, BURSTS), [])

    def test_cslcs_fail_on_a_missing_burst_or_the_old_shape(self):
        errors = feed.check_cslcs({BURSTS[0]: _cslc(BURSTS[0], promoted=False)}, BURSTS)

        self.assertEqual(len(errors), 3)
        self.assertTrue(any("no PGE-produced CSLC" in e for e in errors))
        self.assertTrue(any("metadata.burst_id" in e for e in errors))
        self.assertTrue(any("starttime" in e for e in errors))

    def test_csc_must_record_exactly_the_pge_products(self):
        paths = [feed.h5_path(_cslc(b)["_source"]["metadata"]) for b in BURSTS]

        self.assertEqual(feed.check_csc({"is_complete": True, "cslc_product_paths": paths}, paths), [])
        daac = paths[:1] + ["s3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1/x/x.h5"]
        self.assertEqual(len(feed.check_csc({"is_complete": True, "cslc_product_paths": daac}, paths)), 1)
        self.assertEqual(len(feed.check_csc({"is_complete": False, "cslc_product_paths": paths}, paths)), 1)

    def test_ksc_must_be_complete_final_and_list_the_pge_products(self):
        paths = [feed.h5_path(_cslc(b)["_source"]["metadata"]) for b in BURSTS]
        good = {"is_complete": True, "compressed_cslc_final": True,
                "product_paths": {"L2_CSLC_S1": paths + ["s3://other/older.h5"]}}

        self.assertEqual(feed.check_ksc(good, paths), [])
        self.assertEqual(len(feed.check_ksc({**good, "compressed_cslc_final": False}, paths)), 1)
        self.assertEqual(len(feed.check_ksc({**good, "product_paths": {"L2_CSLC_S1": paths[:1]}}, paths)), 1)

    def test_find_l3_matches_the_secondary_date_only(self):
        hits = [{"_id": "OPERA_L3_DISP-S1_IW_F31241_VV_20171021T172929Z_20190613T172935Z_v1.0_20260911T100000Z"},
                {"_id": "OPERA_L3_DISP-S1_IW_F31241_VV_20190613T172935Z_20190625T172936Z_v1.0_20260911T110000Z"},
                {"_id": "OPERA_L3_DISP-S1_IW_F31242_VV_20171021T172929Z_20190613T172935Z_v1.0_20260911T100000Z"}]

        found = feed.find_l3(hits, 31241, DATE)

        self.assertEqual([h["_id"] for h in found], [hits[0]["_id"]])

    def test_lineage_uses(self):
        ids = [_cslc(b)["_id"] for b in BURSTS]
        l3 = {"_source": {"metadata": {"lineage": [f"{ids[0]}/{ids[0]}.h5", "OPERA_L2_COMPRESSED-CSLC-S1_x.h5"]}}}

        self.assertEqual(feed.lineage_uses(l3, ids), ids[:1])

    def test_slc_query_params_run_in_reprocessing_mode_over_a_temporal_window(self):
        params = feed.build_slc_query_params("2019-06-13T17:29:30Z", "2019-06-13T17:29:35Z", "6.0.6")

        self.assertEqual(params["processing_mode"], "--processing-mode=reprocessing")
        self.assertEqual(params["use_temporal"], "--use-temporal")
        self.assertEqual(params["start_datetime"], "--start-date=2019-06-13T17:29:30Z")
        self.assertEqual(params["end_datetime"], "--end-date=2019-06-13T17:29:35Z")
        self.assertEqual(params["download_job_release"], "--release-version=6.0.6")


if __name__ == "__main__":
    unittest.main()
