"""Pure parts of the operator tools for CSLC datasets and DISP-S1 cycle state configs.

The tools talk to OpenSearch and Mozart only inside main(); what is tested here is the
query, script and job-parameter construction they send.
"""
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

TOOLS = Path(__file__).parents[3] / "tools"
TEST_DATA = Path(__file__).parents[1] / "data_subscriber" / "test_data"


def _load(name):
    spec = importlib.util.spec_from_file_location(f"_{name}", TOOLS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backfill = _load("backfill_cslc_dataset_metadata")
reevaluate = _load("reevaluate_disp_s1_cscs")
region_db = _load("check_disp_s1_region_db")


class BackfillQueryAndScriptTest(unittest.TestCase):

    def test_only_documents_without_a_top_level_burst_id_are_selected(self):
        query = backfill.build_query("L2_CSLC_S1")

        self.assertEqual(query["bool"]["must"], [{"term": {"dataset_type.keyword": "L2_CSLC_S1"}}])
        self.assertEqual(query["bool"]["must_not"], [{"exists": {"field": "metadata.burst_id"}}])

    def test_a_frame_restricts_by_the_per_file_burst_id(self):
        query = backfill.build_query("L2_CSLC_S1", ["T117-249922-IW3", "T117-249921-IW3"])

        self.assertIn({"terms": {"metadata.Files.burst_id.keyword":
                                 ["T117-249921-IW3", "T117-249922-IW3"]}},
                      query["bool"]["must"])

    def test_cslc_script_promotes_the_keys_and_sets_the_time_range(self):
        script = backfill.build_script("L2_CSLC_S1")

        self.assertEqual(script["lang"], "painless")
        self.assertEqual(script["params"],
                         {"keys": ["burst_id", "acquisition_ts", "sensor", "pol"],
                          "set_time_range": True})
        # Never overwrites: every write is guarded by an absence check, and an unchanged
        # document is a noop rather than a reindex.
        self.assertIn("md.get(key) == null", script["source"])
        self.assertIn("ctx._source.get('starttime') == null", script["source"])
        self.assertIn("ctx.op = 'noop'", script["source"])

    def test_static_script_sets_no_time_range(self):
        script = backfill.build_script("L2_CSLC_S1_STATIC")

        self.assertEqual(script["params"],
                         {"keys": ["burst_id", "validity_ts", "sensor"], "set_time_range": False})

    def test_keys_match_what_product2dataset_promotes(self):
        try:
            import hysds.utils  # noqa: F401
            stubs = {}
        except ImportError:
            stubs = {"hysds": MagicMock(), "hysds.utils": MagicMock()}
        with patch.dict(sys.modules, stubs):
            from product2dataset import product2dataset

        self.assertEqual(backfill.PROMOTED_KEYS["L2_CSLC_S1"], list(product2dataset.CSLC_PROMOTED_KEYS))
        self.assertEqual(backfill.PROMOTED_KEYS["L2_CSLC_S1_STATIC"],
                         list(product2dataset.CSLC_STATIC_PROMOTED_KEYS))


class ReevaluateCscsTest(unittest.TestCase):

    HIT = {
        "_id": "cslc_s1-cycle-f15422-20260826-state-config",
        "_source": {
            "dataset": "cslc_s1-cycle-state-config",
            "urls": ["http://bucket.s3-website/state-config", "s3://bucket/state-config"],
            "metadata": {"frame_id": 15422, "sensing_date": "20260826", "is_complete": False},
        },
    }

    def test_query_selects_incomplete_cscs(self):
        query = reevaluate.build_query()

        self.assertEqual(query, {"bool": {"must": [{"term": {"metadata.is_complete": False}}]}})

    def test_query_filters(self):
        query = reevaluate.build_query(frame_ids=["15422", 42810], since="20260825", region=2)

        must = query["bool"]["must"]
        self.assertIn({"terms": {"metadata.frame_id": [15422, 42810]}}, must)
        self.assertIn({"term": {"metadata.region_id": "2"}}, must)
        self.assertEqual(query["bool"]["filter"],
                         [{"range": {"metadata.sensing_date": {"gte": "20260825"}}}])

    def test_params_match_the_trigger_rule_wiring(self):
        params = reevaluate.build_reeval_params(self.HIT)

        self.assertEqual(params, {
            "product_paths": "s3://bucket/state-config",
            "product_metadata": {"metadata": self.HIT["_source"]["metadata"]},
            "dataset_type": "cslc_s1-cycle-state-config",
            "input_dataset_id": "cslc_s1-cycle-f15422-20260826-state-config",
        })

    def test_submit_form(self):
        form = reevaluate.build_submit_form(self.HIT, "6.0.6", "opera-job_worker-evaluator_verdi",
                                            "20260910T000000Z")

        self.assertEqual(form["type"], "job-disp_s1_cycle_evaluator:6.0.6")
        self.assertEqual(form["queue"], "opera-job_worker-evaluator_verdi")
        self.assertEqual(json.loads(form["params"]), reevaluate.build_reeval_params(self.HIT))
        self.assertEqual(json.loads(form["tags"]), [reevaluate.TAG])
        self.assertIn(self.HIT["_id"], form["name"])


class RegionDbAuditTest(unittest.TestCase):

    def setUp(self):
        with open(TEST_DATA / "example_region_db.json") as f:
            regions = json.load(f)
        self.frame_region_map = {int(frame): region
                                 for region, frames in regions.items() for frame in frames}

    def test_compare_frame_sets(self):
        missing, extra = region_db.compare_frame_sets({7098, 31241, "24718"},
                                                      {**self.frame_region_map, 99999: "4"})

        self.assertEqual(missing, [24718, 31241])
        self.assertEqual(extra, [99999])

    def test_resolve_frames_marks_unlisted_frames_unknown(self):
        resolved = region_db.resolve_frames([7098, 31241], self.frame_region_map)

        self.assertEqual(resolved, {7098: "region_a", 31241: "UNKNOWN"})

    def test_region_counts(self):
        self.assertEqual(region_db.region_counts({1: "4", 2: "4", 3: "3a"}), {"3a": 1, "4": 2})

    def test_parse_frames(self):
        self.assertEqual(region_db.parse_frames("15422, 42810,"), [15422, 42810])


if __name__ == "__main__":
    unittest.main()
