"""Tests for DISP-S1 K-Cycle Evaluator."""

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
    from data_subscriber.cslc import disp_s1_k_cycle_evaluator as k_evaluator_mod
    from data_subscriber.cslc.disp_s1_k_cycle_evaluator import DispS1KCycleEvaluator


class _FakeHistBursts:
    def __init__(self, frame_number, burst_ids, day_indices):
        self.frame_number = frame_number
        self.burst_ids = set(burst_ids)
        self.sensing_datetimes = []
        self.sensing_seconds_since_first = []
        self.sensing_datetime_days_index = day_indices


def _make_evaluator(frame_to_bursts, burst_to_frames, es_conn, k=3, m=2):
    _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
        frame_to_bursts, burst_to_frames, {}
    )
    return DispS1KCycleEvaluator(es_conn, k=k, m=m)


class TestDetermineKGroup(unittest.TestCase):

    def setUp(self):
        self.burst_ids = ["b1", "b2", "b3"]
        self.day_indices = [0, 6, 12, 18, 24, 30, 36, 42, 48]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, self.day_indices)
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def test_first_group(self):
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3
        )
        k_group_index, cycles = evaluator._determine_k_group(7098, 0)
        self.assertEqual(k_group_index, 0)
        self.assertEqual(cycles, [0, 6, 12])

    def test_second_group(self):
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3
        )
        k_group_index, cycles = evaluator._determine_k_group(7098, 18)
        self.assertEqual(k_group_index, 1)
        self.assertEqual(cycles, [18, 24, 30])

    def test_third_group(self):
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3
        )
        k_group_index, cycles = evaluator._determine_k_group(7098, 36)
        self.assertEqual(k_group_index, 2)
        self.assertEqual(cycles, [36, 42, 48])

    def test_unknown_cycle_returns_none(self):
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3
        )
        k_group_index, cycles = evaluator._determine_k_group(7098, 999)
        self.assertIsNone(k_group_index)

    def test_unknown_frame_returns_none(self):
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3
        )
        k_group_index, cycles = evaluator._determine_k_group(99999, 0)
        self.assertIsNone(k_group_index)


class TestEvaluateKGroup(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.day_indices = [0, 6, 12]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, self.day_indices)
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_creates_k_group_when_not_exists(self):
        with patch.object(k_evaluator_mod, "find_k_group_state_config",
                          return_value=({}, None)), \
             patch.object(k_evaluator_mod, "find_cycle_state_config",
                          return_value=({c.CYCLE_COMPLETE: True,
                                         c.COVERAGE_ACTUAL: 2,
                                         c.COVERAGE_EXPECTED: 2}, "idx")):

            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
            )
            evaluator._check_compressed_cslcs = MagicMock(return_value=(True, ["cc1", "cc2"]))
            evaluator._evaluate_k_group(7098, 0, [0, 6, 12])

        met_path = "disp-s1_f7098_k0_state-config/disp-s1_f7098_k0_state-config.met.json"
        self.assertTrue(os.path.exists(met_path))
        with open(met_path) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertTrue(met[c.ALL_CYCLES_COMPLETE])

    def test_incomplete_when_not_all_cycles_done(self):
        def cycle_side_effect(es_conn, sc_id):
            if "a12" in sc_id:
                return ({}, None)
            return (
                {c.CYCLE_COMPLETE: True, c.COVERAGE_ACTUAL: 2, c.COVERAGE_EXPECTED: 2},
                "idx",
            )

        with patch.object(k_evaluator_mod, "find_k_group_state_config",
                          return_value=({}, None)), \
             patch.object(k_evaluator_mod, "find_cycle_state_config",
                          side_effect=cycle_side_effect):

            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
            )
            evaluator._evaluate_k_group(7098, 0, [0, 6, 12])

        met_path = "disp-s1_f7098_k0_state-config/disp-s1_f7098_k0_state-config.met.json"
        with open(met_path) as f:
            met = json.load(f)
        self.assertFalse(met[c.IS_COMPLETE])
        self.assertFalse(met[c.ALL_CYCLES_COMPLETE])
        self.assertEqual(met[c.CYCLES_COMPLETE], 2)

    def test_saves_blocked_job_when_ccslc_not_ready(self):
        _mock_cslc_utils.save_blocked_download_job.reset_mock()

        with patch.object(k_evaluator_mod, "find_k_group_state_config",
                          return_value=({}, None)), \
             patch.object(k_evaluator_mod, "find_cycle_state_config",
                          return_value=({c.CYCLE_COMPLETE: True,
                                         c.COVERAGE_ACTUAL: 2,
                                         c.COVERAGE_EXPECTED: 2}, "idx")):

            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
            )
            evaluator._check_compressed_cslcs = MagicMock(return_value=(False, []))
            evaluator._evaluate_k_group(7098, 0, [0, 6, 12])

        _mock_cslc_utils.save_blocked_download_job.assert_called_once()

    def test_updates_existing_k_group(self):
        existing = {
            c.K: 3, c.M: 2,
            c.ACQUISITION_CYCLES: [0, 6, 12],
            c.CYCLE_STATE_CONFIG_IDS: ["sc1", "sc2", "sc3"],
            c.CYCLES_COMPLETE: 1, c.IS_COMPLETE: False,
        }

        with patch.object(k_evaluator_mod, "find_k_group_state_config",
                          return_value=(existing, "grq_idx")), \
             patch.object(k_evaluator_mod, "find_cycle_state_config",
                          return_value=({c.CYCLE_COMPLETE: True,
                                         c.COVERAGE_ACTUAL: 2,
                                         c.COVERAGE_EXPECTED: 2}, "idx")):

            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
            )
            evaluator._check_compressed_cslcs = MagicMock(return_value=(True, ["cc1", "cc2"]))
            evaluator._evaluate_k_group(7098, 0, [0, 6, 12])

        met_path = "disp-s1_f7098_k0_state-config/disp-s1_f7098_k0_state-config.met.json"
        with open(met_path) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])


class TestEvaluateEntryPoint(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.day_indices = [0, 6, 12]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, self.day_indices)
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_evaluate_routes_to_correct_k_group(self):
        with patch.object(k_evaluator_mod, "find_k_group_state_config",
                          return_value=({}, None)), \
             patch.object(k_evaluator_mod, "find_cycle_state_config",
                          return_value=({}, None)):

            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
            )
            evaluator.evaluate({c.FRAME_ID: 7098, c.ACQUISITION_CYCLE: 6})

        # Cycle 6 is at position 1 in [0,6,12], group = 1//3 = 0
        self.assertTrue(os.path.isdir("disp-s1_f7098_k0_state-config"))


if __name__ == "__main__":
    unittest.main()
