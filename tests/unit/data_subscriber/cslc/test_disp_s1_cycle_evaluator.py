"""Tests for DISP-S1 Per-Cycle Evaluator."""

import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch
from pathlib import Path

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc_utils import _localize_region_db

TEST_DATA = Path(__file__).parents[1] / "test_data"
TEST_REGION_DB = str(TEST_DATA / "example_region_db.json")


# Mock heavy imports to avoid numpy/elasticsearch version issues in local dev.
_mock_cslc_utils = MagicMock()
_mock_es_conn_util = MagicMock()
_mock_cslc_blackout = MagicMock()

# The evaluators import latest_cslc_per_burst by name, so the module mock must supply a
# working one or every product-path list becomes a MagicMock. These tests predate the
# deduplication and assert the old sorted-unique semantics, which is exactly what this
# preserves; the selection rule itself is covered by test_latest_cslc_per_burst.py.
_mock_cslc_utils.latest_cslc_per_burst = lambda paths: sorted(set(paths or []))


def _mock_get_region_from_frame(frame_id):
    return _localize_region_db(TEST_REGION_DB).get(frame_id, 'UNKNOWN')


_mock_cslc_utils.get_region_from_frame = _mock_get_region_from_frame


with patch.dict(sys.modules, {
    "data_subscriber.cslc_utils": _mock_cslc_utils,
    "data_subscriber.cslc.cslc_blackout": _mock_cslc_blackout,
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
    _mock_cslc_utils.localize_frame_geojson_map.return_value = {}
    _mock_cslc_utils.get_geojson_for_frame.return_value = None
    evaluator = DispS1CycleEvaluator(es_conn)
    # DispS1BlackoutDates is a mocked class; default the instance to
    # "not in blackout" so coverage-focused tests are unaffected.
    evaluator.blackout_dates.is_in_blackout.return_value = (False, None)
    return evaluator


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


class TestCycleEvaluatorBlackout(unittest.TestCase):
    """Blackout is an orthogonal flag on the CSC: is_complete stays a pure
    burst-coverage fact; the blackout flag drives DISP-S1 exclusion
    downstream."""

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

    def _met(self, frame_id=7098, sensing_date="20241201"):
        d = f"cslc_s1-cycle-f{frame_id}-{sensing_date}-state-config"
        with open(os.path.join(d, f"{d}.met.json")) as f:
            return json.load(f)

    def test_blackout_flag_true_with_truthful_is_complete(self):
        import datetime
        mock_dts = datetime.datetime(2024, 12, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator.blackout_dates.is_in_blackout.return_value = (
                True,
                (datetime.datetime(2024, 11, 1), datetime.datetime(2025, 4, 1)),
            )
            # Full coverage: is_complete must stay truthfully True.
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20241201T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        met = self._met()
        self.assertTrue(met[c.BLACKOUT])
        self.assertTrue(met[c.IS_COMPLETE])
        # Day-precision datetime drives the acquisition-index math.
        evaluator.blackout_dates.is_in_blackout.assert_called_with(
            7098, datetime.datetime(2024, 12, 1)
        )

    def test_no_blackout_flag_false(self):
        import datetime
        mock_dts = datetime.datetime(2024, 8, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        met = self._met(sensing_date="20240801")
        self.assertFalse(met[c.BLACKOUT])
        self.assertTrue(met[c.IS_COMPLETE])

    def test_blackout_incomplete_coverage_stays_truthful(self):
        import datetime
        mock_dts = datetime.datetime(2024, 12, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator.blackout_dates.is_in_blackout.return_value = (
                True,
                (datetime.datetime(2024, 11, 1), datetime.datetime(2025, 4, 1)),
            )
            # Partial coverage: 1 of 2 bursts.
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                [self.burst_ids[0]], ["s3://p1"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20241201T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        met = self._met()
        self.assertTrue(met[c.BLACKOUT])
        self.assertFalse(met[c.IS_COMPLETE])

    def test_blackout_on_reeval_path(self):
        """Input B (CSC re-evaluation) has only sensing_date; the blackout
        decision must still be made, reconstructing full precision."""
        import datetime

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator.blackout_dates.is_in_blackout.return_value = (
                True,
                (datetime.datetime(2024, 11, 1), datetime.datetime(2025, 4, 1)),
            )
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="cslc_s1-cycle-f7098-20241201-state-config",
                metadata={
                    c.FRAME_ID: 7098,
                    c.SENSING_DATE: "20241201",
                    c.ACQUISITION_CYCLE: 0,
                },
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        met = self._met()
        self.assertTrue(met[c.BLACKOUT])
        # Fake frame has no sensing_datetimes -> last-resort midnight fallback
        evaluator.blackout_dates.is_in_blackout.assert_called_with(
            7098, datetime.datetime(2024, 12, 1)
        )

    def test_l2_trigger_uses_full_precision_acquisition_dts(self):
        """The L2 path must pass the trigger's exact acquisition datetime —
        blackout windows carry the frame's time-of-day, and a midnight
        stand-in would miss the first date of every window."""
        import datetime
        mock_dts = datetime.datetime(2024, 12, 1, 17, 30, 22)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            "T074-157286-IW3", mock_dts, {7098: 0}, [7098]
        )

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20241201T173022Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        evaluator.blackout_dates.is_in_blackout.assert_called_with(
            7098, mock_dts
        )

    def test_reeval_reconstructs_time_of_day_from_frame_history(self):
        """Re-eval path: prefer the frame's recorded sensing datetime on the
        calendar date; else combine the date with the frame's acquisition
        time-of-day."""
        import datetime
        exact = datetime.datetime(2024, 12, 1, 17, 30, 22)
        self.frame_to_bursts[7098].sensing_datetimes = [
            datetime.datetime(2024, 11, 25, 17, 30, 21),
            exact,
        ]

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            # Exact-date match returns the recorded entry.
            self.assertEqual(
                evaluator._sensing_datetime_for_blackout(7098, "20241201"),
                exact,
            )
            # No recorded entry for the date -> date + frame time-of-day.
            self.assertEqual(
                evaluator._sensing_datetime_for_blackout(7098, "20241213"),
                datetime.datetime(2024, 12, 13, 17, 30, 21),
            )

    def test_two_frames_get_independent_blackout_decisions(self):
        """Blackout windows are per-frame: a shared burst's two frames can
        differ."""
        import datetime
        shared = "T074-157286-IW3"
        self.frame_to_bursts[7099] = _FakeHistBursts(
            7099, [shared, "T074-157289-IW1"], [0, 6]
        )
        self.burst_to_frames = {
            shared: [7098, 7099],
            "T074-157287-IW1": [7098],
            "T074-157289-IW1": [7099],
        }
        mock_dts = datetime.datetime(2024, 12, 1)
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            shared, mock_dts, {7098: 0, 7099: 0}, [7098, 7099]
        )
        window = (datetime.datetime(2024, 11, 1), datetime.datetime(2025, 4, 1))

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator.blackout_dates.is_in_blackout.side_effect = (
                lambda fid, dt: (True, window) if fid == 7098 else (False, None)
            )
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                [shared], ["s3://p1"]
            ))
            evaluator.evaluate(
                input_dataset_id="OPERA_L2_CSLC-S1_T074-157286-IW3_20241201T000000Z",
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        self.assertTrue(self._met(frame_id=7098)[c.BLACKOUT])
        self.assertFalse(self._met(frame_id=7099)[c.BLACKOUT])

    def test_reeval_idempotency_on_existing_blackout_csc(self):
        """An existing complete+blackout CSC: skip without force (orthogonal
        skip check ignores blackout); recompute + re-persist with force."""
        import datetime
        existing = {c.IS_COMPLETE: True, c.BLACKOUT: True}
        window = (datetime.datetime(2024, 11, 1), datetime.datetime(2025, 4, 1))

        with patch.object(evaluator_mod, "find_csc",
                          return_value=(existing, "idx")):
            evaluator = _make_evaluator(
                self.frame_to_bursts, self.burst_to_frames, self.es_conn
            )
            evaluator.blackout_dates.is_in_blackout.return_value = (True, window)
            evaluator._query_cslcs_for_cycle = MagicMock(return_value=(
                self.burst_ids, ["s3://p1", "s3://p2"]
            ))
            reeval_kwargs = dict(
                input_dataset_id="cslc_s1-cycle-f7098-20241201-state-config",
                metadata={
                    c.FRAME_ID: 7098,
                    c.SENSING_DATE: "20241201",
                    c.ACQUISITION_CYCLE: 0,
                },
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )
            # Without force: the is_complete skip fires; nothing recreated.
            evaluator.evaluate(**reeval_kwargs)
            evaluator._query_cslcs_for_cycle.assert_not_called()
            self.assertFalse(
                os.path.isdir("cslc_s1-cycle-f7098-20241201-state-config")
            )
            # With force: recomputed, blackout re-stamped true.
            evaluator.evaluate(**reeval_kwargs, force_publish=True)

        met = self._met()
        self.assertTrue(met[c.BLACKOUT])
        self.assertTrue(met[c.IS_COMPLETE])


if __name__ == "__main__":
    unittest.main()


class DbExcludedStampTest(unittest.TestCase):
    """The CSC records WHY an acquisition is out, and blackout is resolved first.

    A blacked-out date is also absent from sensing_time_list, so testing absence alone
    would relabel every snow-season acquisition as a partial-coverage exclusion and put
    a false reason into the record used to reconcile with ADT.
    """

    FRAME = 831
    LISTED = ["20240105", "20240117"]
    EXCLUDED = "20240111"
    ASSESSED_END = "20241231"
    UNASSESSED = "20250115"

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        from datetime import datetime as _dt
        frame = _FakeHistBursts(self.FRAME, ["b1", "b2"], [0, 12])
        frame.sensing_datetimes = [_dt.strptime(d, "%Y%m%d") for d in self.LISTED]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[self.FRAME] = frame

        self.es_conn = MagicMock()
        self.evaluator = _make_evaluator(self.frame_to_bursts, {"b1": [self.FRAME]},
                                         self.es_conn)
        self.evaluator._query_cslcs_for_cycle = MagicMock(return_value=(["b1"], ["s3://p"]))
        self.evaluator.es_conn = self.es_conn

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    _UNSET = object()

    def _run(self, sensing_date, enabled=True, assessed_end=_UNSET, in_blackout=False):
        if assessed_end is self._UNSET:
            assessed_end = self.ASSESSED_END
        # Own the blackout decision outright rather than reaching through the module-level
        # mock: whether DispS1BlackoutDates is mocked at all depends on which test module
        # imported the evaluator first, and that is not this test's business.
        self.evaluator.blackout_dates = MagicMock()
        self.evaluator.blackout_dates.is_in_blackout.return_value = (
            (True, (MagicMock(), MagicMock())) if in_blackout else (False, None))
        with patch.object(evaluator_mod, "burst_db_exclusion_enabled", return_value=enabled), \
             patch.object(evaluator_mod, "localize_disp_burst_db_assessed_end",
                          return_value=assessed_end), \
             patch.object(evaluator_mod, "find_csc", return_value=({}, None)), \
             patch.object(evaluator_mod, "create_csc") as create:
            self.evaluator._evaluate_cycle(self.FRAME, 0, sensing_date)
        return create.call_args.kwargs

    def test_absent_inside_the_range_is_stamped(self):
        kwargs = self._run(self.EXCLUDED)
        self.assertTrue(kwargs["db_excluded"])
        self.assertIn("absent from the consistent burst database", kwargs["db_excluded_reason"])
        self.assertIn(self.ASSESSED_END, kwargs["db_excluded_reason"])

    def test_listed_date_is_not_stamped(self):
        kwargs = self._run(self.LISTED[0])
        self.assertFalse(kwargs["db_excluded"])
        self.assertEqual(kwargs["db_excluded_reason"], "")

    def test_absent_past_the_range_is_not_stamped(self):
        kwargs = self._run(self.UNASSESSED)
        self.assertFalse(kwargs["db_excluded"])

    def test_blackout_wins_over_absence(self):
        """Blacked-out dates are stripped from sensing_time_list too -- they must be
        recorded as blackout, never as a burst-database exclusion."""
        kwargs = self._run(self.EXCLUDED, in_blackout=True)
        self.assertTrue(kwargs["blackout"])
        self.assertFalse(kwargs["db_excluded"])
        self.assertEqual(kwargs["db_excluded_reason"], "")

    def test_switch_off_stamps_nothing(self):
        kwargs = self._run(self.EXCLUDED, enabled=False)
        self.assertFalse(kwargs["db_excluded"])

    def test_no_assessed_range_stamps_nothing(self):
        kwargs = self._run(self.EXCLUDED, assessed_end=None)
        self.assertFalse(kwargs["db_excluded"])


def _acquisition_iso(sensing_ts):
    """'20240801T010203Z' -> '2024-08-01T01:02:03.000000Z', the extractor's format."""
    return (f"{sensing_ts[0:4]}-{sensing_ts[4:6]}-{sensing_ts[6:8]}T"
            f"{sensing_ts[9:11]}:{sensing_ts[11:13]}:{sensing_ts[13:15]}.000000Z")


def _pge_hit(burst_id, sensing_ts="20240801T010203Z", pol="VV", h5_position=0,
             promoted=False, with_h5=True):
    """An L2_CSLC_S1 document as a CSLC-S1 PGE job publishes it.

    Every published file (.h5, browse .png, .iso.xml) is listed, and the filename
    metadata sits on each file entry. ``promoted`` adds the top-level copies that
    product2dataset now writes; without it the document has the older shape.
    """
    stem = f"OPERA_L2_CSLC-S1_{burst_id}_{sensing_ts}_20260910T212729Z_S1D_{pol}_v1.1"
    prefix = f"s3://opera-dev-rs-fwd-test/products/CSLC_S1/2024/08/01/{stem}/{stem}"
    paths = [f"{prefix}_BROWSE.png", f"{prefix}.iso.xml"]
    if with_h5:
        paths.insert(h5_position, f"{prefix}.h5")
    acquisition_ts = _acquisition_iso(sensing_ts)
    files = [{"FileName": path.rsplit("/", 1)[1], "burst_id": burst_id,
              "acquisition_ts": acquisition_ts, "sensor": "S1D", "pol": pol}
             for path in paths]
    metadata = {"Files": files, "product_s3_paths": paths, "tags": ["PGE"]}
    if promoted:
        metadata.update(burst_id=burst_id, acquisition_ts=acquisition_ts, sensor="S1D", pol=pol)
    return {"_id": stem, "_source": {"dataset_type": "L2_CSLC_S1", "metadata": metadata}}


def _catalog_hit(burst_id, sensing_ts="20240801T010203Z"):
    """An L2_CSLC_S1 document as cslc_catalog_ingest publishes it."""
    stem = f"OPERA_L2_CSLC-S1_{burst_id}_{sensing_ts}_20240710T080810Z_S1A_VV_v1.1"
    return {"_id": stem, "_source": {
        "dataset_type": "L2_CSLC_S1",
        "starttime": _acquisition_iso(sensing_ts)[:19],
        "metadata": {
            "burst_id": burst_id,
            "catalog_ingest": True,
            "product_s3_paths": [
                f"s3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1/{stem}/{stem}.h5"],
        },
    }}


def _values_under(node, key):
    """Every value stored under ``key`` anywhere in a nested query body."""
    found = []
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key:
                found.append(v)
            found.extend(_values_under(v, key))
    elif isinstance(node, list):
        for v in node:
            found.extend(_values_under(v, key))
    return found


class TestQueryCslcsForCycleShapes(unittest.TestCase):
    """Coverage counts CSLCs from both producers: catalog ingest and the CSLC-S1 PGE."""

    def setUp(self):
        self.burst_ids = ["T074-157286-IW3", "T074-157287-IW1", "T074-157288-IW2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.es_conn = MagicMock()
        self.evaluator = _make_evaluator(
            self.frame_to_bursts, {b: [7098] for b in self.burst_ids}, self.es_conn)

    def _query(self, hits):
        self.es_conn.query.return_value = hits
        return self.evaluator._query_cslcs_for_cycle(7098, self.burst_ids, "20240801")

    def test_query_matches_both_metadata_shapes(self):
        self._query([])

        kwargs = self.es_conn.query.call_args.kwargs
        body = kwargs["body"]
        self.assertEqual(kwargs["index"], "grq_*_l2_cslc_s1-*")
        terms_fields = {field for clause in _values_under(body, "terms") for field in clause}
        self.assertEqual(terms_fields,
                         {"metadata.burst_id.keyword", "metadata.Files.burst_id.keyword"})
        for clause in _values_under(body, "terms"):
            self.assertEqual(sorted(next(iter(clause.values()))), sorted(self.burst_ids))
        range_fields = {field for clause in _values_under(body, "range") for field in clause}
        self.assertEqual(range_fields, {"starttime", "metadata.Files.acquisition_ts"})
        for clause in _values_under(body, "range"):
            self.assertEqual(next(iter(clause.values())),
                             {"gte": "2024-08-01T00:00:00", "lt": "2024-08-01T23:59:59"})
        # Each alternative is an OR of exactly the two shapes.
        self.assertEqual([len(s) for s in _values_under(body, "should")], [2, 2])
        self.assertEqual(_values_under(body, "minimum_should_match"), [1, 1])
        self.assertGreaterEqual(body["size"], 200)

    def test_pge_shaped_documents_count_and_select_the_h5(self):
        hits = [_pge_hit(b, h5_position=i) for i, b in enumerate(self.burst_ids)]

        found, paths = self._query(hits)

        self.assertEqual(sorted(found), sorted(self.burst_ids))
        self.assertEqual(len(paths), 3)
        self.assertTrue(all(p.endswith(".h5") for p in paths), paths)

    def test_promoted_pge_documents_count(self):
        found, paths = self._query([_pge_hit(b, promoted=True) for b in self.burst_ids])

        self.assertEqual(sorted(found), sorted(self.burst_ids))
        self.assertTrue(all(p.endswith(".h5") for p in paths), paths)

    def test_catalog_ingest_documents_still_count(self):
        found, paths = self._query([_catalog_hit(b) for b in self.burst_ids])

        self.assertEqual(sorted(found), sorted(self.burst_ids))
        self.assertTrue(all(p.startswith("s3://asf-cumulus-prod-opera-products/") for p in paths))

    def test_a_burst_published_by_both_producers_counts_once(self):
        burst_id = self.burst_ids[0]

        found, paths = self._query([_pge_hit(burst_id), _catalog_hit(burst_id)])

        self.assertEqual(found, [burst_id])
        # Choosing between the two granules is latest_cslc_per_burst's job.
        self.assertEqual(len(paths), 2)

    def test_non_vv_documents_do_not_count(self):
        found, paths = self._query([
            _pge_hit(self.burst_ids[0], pol="VH"),
            _pge_hit(self.burst_ids[1], pol="HH", promoted=True),
        ])

        self.assertEqual(found, [])
        self.assertEqual(paths, [])

    def test_bursts_outside_the_frame_do_not_count(self):
        found, paths = self._query([_pge_hit("T001-000001-IW1")])

        self.assertEqual(found, [])
        self.assertEqual(paths, [])

    def test_a_document_without_an_h5_product_does_not_count(self):
        found, paths = self._query([_pge_hit(self.burst_ids[0], with_h5=False)])

        self.assertEqual(found, [])
        self.assertEqual(paths, [])

    def test_reaching_the_size_limit_is_logged(self):
        with self.assertLogs(evaluator_mod.logger, level="WARNING") as logs:
            self._query([_catalog_hit(self.burst_ids[0])] * 200)

        self.assertTrue(any("size limit" in line for line in logs.output), logs.output)


class TestCycleEvaluatorPgeShapedInput(unittest.TestCase):
    """A frame whose bursts were produced by the local CSLC-S1 PGE reaches full coverage."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_creates_complete_csc_from_pge_shaped_documents(self):
        import datetime
        burst_ids = ["T074-157286-IW3", "T074-157287-IW1", "T074-157288-IW2"]
        frame_to_bursts = defaultdict(lambda: None)
        frame_to_bursts[7098] = _FakeHistBursts(7098, burst_ids, [0, 6, 12])
        _mock_cslc_utils.parse_cslc_native_id.return_value = (
            burst_ids[0], datetime.datetime(2024, 8, 1, 1, 2, 3), {7098: 0}, [7098]
        )
        es_conn = MagicMock()
        # The older PGE shape: no top-level burst id, no starttime, h5 listed last.
        es_conn.query.return_value = [_pge_hit(b, h5_position=2) for b in burst_ids]

        with patch.object(evaluator_mod, "find_csc", return_value=({}, None)):
            evaluator = _make_evaluator(frame_to_bursts, {b: [7098] for b in burst_ids}, es_conn)
            evaluator.evaluate(
                input_dataset_id=_pge_hit(burst_ids[0])["_id"],
                metadata={},
                dataset_type="L2_CSLC_S1",
            )

        csc_dir = "cslc_s1-cycle-f7098-20240801-state-config"
        with open(os.path.join(csc_dir, f"{csc_dir}.met.json")) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertEqual(met[c.COVERAGE_ACTUAL], 3)
        self.assertEqual(met[c.FOUND_BURST_IDS], sorted(burst_ids))
        self.assertEqual(len(met[c.CSLC_PRODUCT_PATHS]), 3)
        for path in met[c.CSLC_PRODUCT_PATHS]:
            self.assertTrue(path.startswith("s3://opera-dev-rs-fwd-test/products/CSLC_S1/"), path)
            self.assertTrue(path.endswith(".h5"), path)
