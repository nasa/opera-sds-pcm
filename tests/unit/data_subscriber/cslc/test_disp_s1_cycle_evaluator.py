"""Tests for DISP-S1 Per-Cycle Evaluator."""

import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

from data_subscriber.cslc import disp_s1_constants as c

# Mock heavy imports to avoid numpy/elasticsearch version issues in local dev.
# The elasticsearch serializer references np.float_ which was removed in NumPy 2.0.
_mock_cslc_utils = MagicMock()
_mock_es_conn_util = MagicMock()

with patch.dict(sys.modules, {
    "data_subscriber.cslc_utils": _mock_cslc_utils,
    "util.exec_util": MagicMock(),
    "util.ctx_util": MagicMock(),
    "data_subscriber.es_conn_util": _mock_es_conn_util,
    "hysds.celery": MagicMock(),
    "elasticsearch": MagicMock(),
    "elasticsearch.client": MagicMock(),
    "elasticsearch.transport": MagicMock(),
    "elasticsearch.serializer": MagicMock(),
}):
    from data_subscriber.cslc import disp_s1_cycle_evaluator as evaluator_mod
    from data_subscriber.cslc.disp_s1_cycle_evaluator import DispS1CycleEvaluator


class _FakeHistBursts:
    def __init__(self, frame_number, burst_ids, day_indices):
        self.frame_number = frame_number
        self.burst_ids = set(burst_ids)
        self.sensing_datetimes = []
        self.sensing_seconds_since_first = []
        self.sensing_datetime_days_index = day_indices


def _make_evaluator(frame_to_bursts, burst_to_frames, es_conn):
    _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
        frame_to_bursts, burst_to_frames, {}
    )
    return DispS1CycleEvaluator(es_conn)


class TestCycleEvaluatorSingleFrame(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["T074-157286-IW3", "T074-157287-IW1", "T074-157288-IW2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_first_burst_creates_state_config(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        # Mock find to return not found
        with patch.object(evaluator_mod, "find_cycle_state_config", return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={"product_s3_paths": ["s3://bucket/products/file.h5"]},
            )

        expected_dir = "disp-s1_f7098_a0_state-config"
        self.assertTrue(os.path.isdir(expected_dir))

        met_path = os.path.join(expected_dir, f"{expected_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertFalse(met[c.CYCLE_COMPLETE])
        self.assertEqual(met[c.COVERAGE_ACTUAL], 1)

    def test_duplicate_burst_is_idempotent(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        existing = {
            c.EXPECTED_BURST_IDS: self.burst_ids,
            c.FOUND_BURST_IDS: ["T074-157286-IW3"],
            c.FOUND_CSLC_GRANULE_IDS: ["g1"],
            c.CSLC_PRODUCT_PATHS: ["s3://p1"],
            c.CYCLE_COMPLETE: False,
        }

        with patch.object(evaluator_mod, "find_cycle_state_config",
                          return_value=(existing, "idx")):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={"product_s3_paths": ["s3://p1"]},
            )

        expected_dir = "disp-s1_f7098_a0_state-config"
        met_path = os.path.join(expected_dir, f"{expected_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met[c.COVERAGE_ACTUAL], 1)

    def test_adding_new_burst_updates_coverage(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157287-IW1", mock_dts, {7098: 0}, [7098]
        )

        existing = {
            c.EXPECTED_BURST_IDS: self.burst_ids,
            c.FOUND_BURST_IDS: ["T074-157286-IW3"],
            c.FOUND_CSLC_GRANULE_IDS: ["g1"],
            c.CSLC_PRODUCT_PATHS: ["s3://p1"],
            c.CYCLE_COMPLETE: False,
        }

        with patch.object(evaluator_mod, "find_cycle_state_config",
                          return_value=(existing, "idx")):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157287-IW1_20240801T000000Z",
                metadata={"product_s3_paths": ["s3://p2"]},
            )

        expected_dir = "disp-s1_f7098_a0_state-config"
        met_path = os.path.join(expected_dir, f"{expected_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met[c.COVERAGE_ACTUAL], 2)
        self.assertIn("T074-157287-IW1", met[c.FOUND_BURST_IDS])


class TestCycleEvaluatorMultiFrame(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.shared_burst = "T074-157286-IW3"
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(
            7098, [self.shared_burst, "T074-157287-IW1"], [0, 6]
        )
        self.frame_to_bursts[7099] = _FakeHistBursts(
            7099, [self.shared_burst, "T074-157289-IW1"], [0, 6]
        )
        self.burst_to_frames = {
            self.shared_burst: [7098, 7099],
            "T074-157287-IW1": [7098],
            "T074-157289-IW1": [7099],
        }
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_shared_burst_updates_two_frames(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            self.shared_burst, mock_dts,
            {7098: 0, 7099: 0},
            [7098, 7099],
        )

        with patch.object(evaluator_mod, "find_cycle_state_config",
                          return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801",
                metadata={"product_s3_paths": ["s3://bucket/file.h5"]},
            )

        self.assertTrue(os.path.isdir("disp-s1_f7098_a0_state-config"))
        self.assertTrue(os.path.isdir("disp-s1_f7099_a0_state-config"))


if __name__ == "__main__":
    unittest.main()
