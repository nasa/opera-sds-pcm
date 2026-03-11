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


class TestCycleEvaluatorL2Input(unittest.TestCase):
    """Test Input A: triggered by L2_CSLC_S1."""

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

    def test_creates_csc_from_l2_cslc(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        # Mock find_csc to return not found (no skip)
        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            # Mock ES query for CSLCs - returns 2 of 3 bursts
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                ["T074-157286-IW3", "T074-157287-IW1"],
                ["s3://p1", "s3://p2"],
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        expected_dir = "cslc_s1-cycle-f7098-20240801-state-config"
        self.assertTrue(os.path.isdir(expected_dir))

        met_path = os.path.join(expected_dir, f"{expected_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertFalse(met[c.IS_COMPLETE])
        self.assertEqual(met[c.COVERAGE_ACTUAL], 2)
        self.assertEqual(met[c.SENSING_DATE], "20240801")

    def test_complete_when_all_bursts_found(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids,
                ["s3://p1", "s3://p2", "s3://p3"],
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        expected_dir = "cslc_s1-cycle-f7098-20240801-state-config"
        met_path = os.path.join(expected_dir, f"{expected_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertIn("complete", met[c.COMPLETENESS_REASON])

    def test_skips_when_already_complete(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        # Mock find_csc to return already complete
        existing = {c.IS_COMPLETE: True}
        with patch.object(evaluator_mod, "find_csc", return_value=(existing, "idx")):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator._query_cslcs_for_cycle = MagicMock()
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        # Should not have queried ES for CSLCs
        evaluator._query_cslcs_for_cycle.assert_not_called()
        # No dataset dir created
        self.assertFalse(os.path.isdir("cslc_s1-cycle-f7098-20240801-state-config"))


class TestCycleEvaluatorCSCInput(unittest.TestCase):
    """Test Input B: on-demand re-evaluation from existing CSC."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["T074-157286-IW3", "T074-157287-IW1"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_re_evaluates_from_csc(self):
        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="cslc_s1-cycle-f7098-20240801-state-config",
                metadata={
                    c.FRAME_ID: 7098,
                    c.SENSING_DATE: "20240801",
                    c.ACQUISITION_CYCLE: 0,
                },
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        expected_dir = "cslc_s1-cycle-f7098-20240801-state-config"
        self.assertTrue(os.path.isdir(expected_dir))

    def test_force_publish_bypasses_skip(self):
        existing = {c.IS_COMPLETE: True}
        with patch.object(evaluator_mod, "find_csc", return_value=(existing, "idx")):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="cslc_s1-cycle-f7098-20240801-state-config",
                metadata={
                    c.FRAME_ID: 7098,
                    c.SENSING_DATE: "20240801",
                    c.ACQUISITION_CYCLE: 0,
                },
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
                force_publish=True,
            )

        # Should have created despite existing complete CSC
        evaluator._query_cslcs_for_cycle.assert_called_once()


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

    def test_shared_burst_creates_two_cscs(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            self.shared_burst, mock_dts,
            {7098: 0, 7099: 0},
            [7098, 7099],
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(self.frame_to_bursts, self.burst_to_frames, self.es_conn)
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                [self.shared_burst], ["s3://p1"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        self.assertTrue(os.path.isdir("cslc_s1-cycle-f7098-20240801-state-config"))
        self.assertTrue(os.path.isdir("cslc_s1-cycle-f7099-20240801-state-config"))


if __name__ == "__main__":
    unittest.main()
