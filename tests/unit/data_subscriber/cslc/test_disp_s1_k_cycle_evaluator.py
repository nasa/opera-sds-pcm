"""Tests for DISP-S1 K-Cycle Evaluator."""

import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch, call

from data_subscriber.cslc import disp_s1_constants as c

# Mock heavy imports to avoid numpy/elasticsearch version issues in local dev.
_mock_cslc_utils = MagicMock()
_mock_es_conn_util = MagicMock()

import re as _re_module

_CCSLC_DOC_ID_DATE_RE = _re_module.compile(
    r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_"
)


def _mock_parse_ccslc_dates(doc_id):
    """Real implementation of cslc_utils.parse_ccslc_doc_id_dates, so the
    evaluator's lineage-bound lookup is exercised end-to-end in tests."""
    m = _CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None


_mock_cslc_utils.parse_ccslc_doc_id_dates = _mock_parse_ccslc_dates

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


def _make_evaluator(frame_to_bursts, burst_to_frames, es_conn, k=3, m=2,
                    frame_geo_map=None):
    _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
        frame_to_bursts, burst_to_frames, {}
    )
    _mock_cslc_utils.localize_frame_geo_json.return_value = (
        frame_geo_map or {7098: [-118.5, 33.5, -117.0, 35.0]}
    )
    _mock_cslc_utils.get_bounding_box_for_frame.side_effect = (
        lambda fid, geo: geo.get(fid, [])
    )
    _mock_cslc_utils.localize_frame_geojson_map.return_value = {}
    _mock_cslc_utils.get_geojson_for_frame.return_value = None
    return DispS1KCycleEvaluator(es_conn, k=k, m=m)


def _make_csc_hit(sensing_date, is_complete=True, burst_ids=None,
                  product_paths=None, acquisition_cycle=0):
    """Create a mock ES hit for a CSC."""
    return {
        "_source": {
            "metadata": {
                c.SENSING_DATE: sensing_date,
                c.ACQUISITION_CYCLE: acquisition_cycle,
                c.IS_COMPLETE: is_complete,
                c.EXPECTED_BURST_IDS: burst_ids or ["b1", "b2"],
                c.FOUND_BURST_IDS: burst_ids or ["b1", "b2"],
                c.CSLC_PRODUCT_PATHS: product_paths or [f"s3://p_{sensing_date}"],
            }
        }
    }


class TestKCycleEvaluatorWindow(unittest.TestCase):
    """Test nearest-neighbor window query."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12, 18, 24])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_full_k_window(self):
        """When enough CSCs exist, window has exactly k entries."""
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://static"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://iono"]))
            evaluator.evaluate(
                input_dataset_id="cslc_s1-cycle-f7098-20240129-state-config",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        self.assertTrue(os.path.isdir(ksc_dir))

        met_path = os.path.join(ksc_dir, f"{ksc_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertEqual(len(met[c.WINDOW_SENSING_DATES]), 3)
        self.assertEqual(len(met[c.WINDOW_ENTRIES]), 3)

    def test_partial_window_early_series(self):
        """When fewer than k CSCs exist, window has whatever is available."""
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, [], [], "no CCSLCs required (early window)"))
            evaluator.evaluate(
                input_dataset_id="cslc_s1-cycle-f7098-20240117-state-config",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240117"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240117-state-config"
        self.assertTrue(os.path.isdir(ksc_dir))

        met_path = os.path.join(ksc_dir, f"{ksc_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        # Not complete because only 2 of 3 CSCs
        self.assertFalse(met[c.IS_COMPLETE])
        self.assertEqual(len(met[c.WINDOW_ENTRIES]), 2)


class TestKCycleEvaluatorCCSLC(unittest.TestCase):
    """Test compressed CSLC satisfaction."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_complete_with_ccsls(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(
                return_value=(True, ["ccslc1"], ["s3://cc1"], "1 CCSLCs")
            )
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://static"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://iono"]))
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        met_path = os.path.join(ksc_dir, f"{ksc_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertTrue(met[c.COMPRESSED_CSLC_SATISFIED])

    def test_incomplete_without_ccsls(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(
                return_value=(False, [], [], "CCSLCs 0/1")
            )
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        met_path = os.path.join(ksc_dir, f"{ksc_dir}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertFalse(met[c.IS_COMPLETE])
        self.assertFalse(met[c.COMPRESSED_CSLC_SATISFIED])


class TestKCycleEvaluatorSkipLogic(unittest.TestCase):
    """Test skip logic for already-complete KSCs."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_skips_when_already_complete(self):
        existing = {c.IS_COMPLETE: True}

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc",
                          return_value=(existing, "idx")), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame") as mock_query:
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        # Should not have queried for CSCs
        mock_query.assert_not_called()

    def test_force_publish_bypasses_skip(self):
        existing = {c.IS_COMPLETE: True}

        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        with patch.object(k_evaluator_mod, "find_ksc",
                          return_value=(existing, "idx")), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame",
                          return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(
                return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs")
            )
            evaluator.evaluate(
                input_dataset_id="ksc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.DISP_S1_KCYCLE_STATE_CONFIG,
                force_publish=True,
            )

        # Should have created KSC despite existing complete one
        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        self.assertTrue(os.path.isdir(ksc_dir))


class TestKCycleEvaluatorCascade(unittest.TestCase):
    """Test cascade re-evaluation of affected incomplete KSCs."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_cascade_re_evaluates_affected_kscs(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        # An affected incomplete KSC for a different reference date
        affected_ksc = {
            "_source": {
                "metadata": {
                    c.SENSING_DATE: "20240117",
                    c.FRAME_ID: 7098,
                }
            }
        }

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )

        call_count = [0]
        original_evaluate = evaluator._evaluate_k_cycle

        def tracking_evaluate(fid, sd, force_publish=False, cascade=True):
            call_count[0] += 1
            return original_evaluate(fid, sd, force_publish=force_publish, cascade=cascade)

        evaluator._evaluate_k_cycle = tracking_evaluate

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame",
                          return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[affected_ksc]), \
             patch.object(k_evaluator_mod, "query_stale_window_kscs",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(
                return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs")
            )
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        # Should have evaluated twice: once for 20240129, once for 20240117
        self.assertEqual(call_count[0], 2)


def _partial_csc_hit(sensing_date, found, expected):
    """Mock ES hit for an incomplete CSC (partial bursts)."""
    return {"_source": {"metadata": {
        c.SENSING_DATE: sensing_date,
        c.EXPECTED_BURST_IDS: [f"b{i}" for i in range(expected)],
        c.FOUND_BURST_IDS: [f"b{i}" for i in range(found)],
    }}}


def _ccslc_hit(frame_id, ref, first, last, creation, burst="T042-088905-IW1"):
    """Mock ES hit for a CCSLC (date pattern carries the boundary info)."""
    doc_id = (
        f"OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id}_{burst}_"
        f"{ref}T000000Z_{first}T000000Z_{last}T000000Z_{creation}T010150Z_VV_v1.0"
    )
    return {"_id": doc_id}


class TestCheckLineageGapUnresolved(unittest.TestCase):
    """lineage gap detection — partial CSC in (CCSLC.last_date, sensing_date]."""

    def setUp(self):
        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}

    def _make_evaluator_with_es(self, csc_hits=None, ccslc_hits=None):
        es_conn = MagicMock()
        # es_conn.query is called twice in _check_lineage_gap_unresolved:
        # first from _get_lineage_lower_bound (ccslcs), then from the main
        # method (cscs). Use side_effect to return different results per call.
        es_conn.query.side_effect = [ccslc_hits or [], csc_hits or []]
        return _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, es_conn, k=3, m=2
        )

    def test_no_partial_returns_false(self):
        evaluator = self._make_evaluator_with_es(csc_hits=[], ccslc_hits=[])
        gap, detail = evaluator._check_lineage_gap_unresolved(7098, "20240129")
        self.assertFalse(gap)
        self.assertEqual(detail, "")

    def test_partial_csc_in_lineage_returns_true(self):
        evaluator = self._make_evaluator_with_es(
            csc_hits=[_partial_csc_hit("20240117", found=1, expected=2)],
            ccslc_hits=[],
        )
        gap, detail = evaluator._check_lineage_gap_unresolved(7098, "20240129")
        self.assertTrue(gap)
        self.assertIn("20240117", detail)
        self.assertIn("1/2", detail)

    def test_ignores_partial_before_most_recent_ccslc(self):
        # CCSLC at last_date=20240105 bounds the lineage. Partial CSC at
        # 20240101 (before CCSLC) should NOT be queried because the ES range
        # clause excludes it via gt=20240105.
        # We simulate this by having ES return ONLY the partial that's after
        # the bound — i.e., the ES range query already filters. The test
        # validates that _check_lineage_gap_unresolved passes the right
        # range to ES.
        es_conn = MagicMock()
        # First call: CCSLC lookup
        es_conn.query.side_effect = [
            [_ccslc_hit(7098, "20240105", "20231201", "20240105", "20240106")],
            [],  # CSC lookup: no incompletes in the post-CCSLC range
        ]
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, es_conn, k=3, m=2
        )
        gap, detail = evaluator._check_lineage_gap_unresolved(7098, "20240129")
        self.assertFalse(gap)
        # Verify the CSC query used the CCSLC boundary as a lower bound.
        csc_call = es_conn.query.call_args_list[1]
        range_clause = csc_call.kwargs["body"]["query"]["bool"]["must"][3]["range"]["metadata.sensing_date"]
        self.assertEqual(range_clause.get("gt"), "20240105")

    def test_es_error_returns_false(self):
        es_conn = MagicMock()
        # First call (lineage lower bound) succeeds, second call (CSC) raises.
        es_conn.query.side_effect = [[], RuntimeError("ES down")]
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, es_conn, k=3, m=2
        )
        gap, detail = evaluator._check_lineage_gap_unresolved(7098, "20240129")
        self.assertFalse(gap)
        self.assertEqual(detail, "")


class TestGetLineageLowerBound(unittest.TestCase):
    """CCSLC last_date lookup for lineage bounding."""

    def setUp(self):
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, ["b1"], [0])

    def test_returns_max_last_date_strictly_before_sensing(self):
        es_conn = MagicMock()
        es_conn.query.return_value = [
            _ccslc_hit(7098, "20221002", "20220417", "20221002", "20250903"),
            _ccslc_hit(7098, "20230424", "20221014", "20230424", "20250903"),
            _ccslc_hit(7098, "20241202", "20240605", "20241202", "20250904"),
        ]
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        # sensing_date=20250101: all three CCSLCs are before this — should pick 20241202
        self.assertEqual(
            evaluator._get_lineage_lower_bound(7098, "20250101"),
            "20241202",
        )

    def test_excludes_ccslc_at_or_after_sensing_date(self):
        es_conn = MagicMock()
        es_conn.query.return_value = [
            _ccslc_hit(7098, "20221002", "20220417", "20221002", "20250903"),
            _ccslc_hit(7098, "20241202", "20240605", "20241202", "20250904"),
        ]
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        # sensing_date=20240101: only 20221002 < 20240101
        self.assertEqual(
            evaluator._get_lineage_lower_bound(7098, "20240101"),
            "20221002",
        )

    def test_returns_empty_when_no_ccslcs(self):
        es_conn = MagicMock()
        es_conn.query.return_value = []
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        self.assertEqual(
            evaluator._get_lineage_lower_bound(7098, "20240101"), ""
        )


class TestCcslcExistsAtBoundary(unittest.TestCase):
    """detect CCSLC already at exact k-boundary."""

    def setUp(self):
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, ["b1"], [0])

    def _make(self, ccslc_hits):
        es_conn = MagicMock()
        es_conn.query.return_value = ccslc_hits
        return _make_evaluator(self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2)

    def test_returns_true_when_ccslc_at_exact_last_date(self):
        evaluator = self._make(ccslc_hits=[
            _ccslc_hit(7098, "20171021", "20161020", "20171021", "20260513"),
        ])
        self.assertTrue(evaluator._ccslc_exists_at_boundary(7098, "20171021"))

    def test_returns_false_when_ccslc_at_different_last_date(self):
        evaluator = self._make(ccslc_hits=[
            _ccslc_hit(7098, "20171021", "20161020", "20171021", "20260513"),
        ])
        # CCSLC's last_date is 20171021; ask for 20180101 → no exact match
        self.assertFalse(evaluator._ccslc_exists_at_boundary(7098, "20180101"))

    def test_returns_false_when_no_ccslcs(self):
        evaluator = self._make(ccslc_hits=[])
        self.assertFalse(evaluator._ccslc_exists_at_boundary(7098, "20171021"))

    def test_returns_false_on_es_error(self):
        es_conn = MagicMock()
        es_conn.query.side_effect = RuntimeError("ES down")
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        # ES error falls through as not-exists (caller may regenerate; safer
        # than blocking)
        self.assertFalse(evaluator._ccslc_exists_at_boundary(7098, "20171021"))

    def test_picks_match_among_multiple_ccslcs(self):
        evaluator = self._make(ccslc_hits=[
            _ccslc_hit(7098, "20220101", "20210101", "20220101", "20260513"),
            _ccslc_hit(7098, "20230101", "20220101", "20230101", "20260513"),
            _ccslc_hit(7098, "20240101", "20230101", "20240101", "20260513"),
        ])
        self.assertTrue(evaluator._ccslc_exists_at_boundary(7098, "20230101"))
        self.assertFalse(evaluator._ccslc_exists_at_boundary(7098, "20250101"))


class TestKCycleEvaluatorSupersededByExistingCcslc(unittest.TestCase):
    """Evaluator marks KSC superseded_by=existing_ccslc when boundary already
    has a CCSLC, so the trigger-disp_s1_job user_rule excludes it via
    must_not exists. is_complete retains its structural meaning."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_superseded_by_set_when_ccslc_exists_at_boundary(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )
        # Pretend the math says "yes, save" and a CCSLC already exists.
        evaluator._determine_save_compressed = MagicMock(return_value=True)
        evaluator._check_lineage_gap_unresolved = MagicMock(return_value=(False, ""))
        evaluator._ccslc_exists_at_boundary = MagicMock(return_value=True)

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://s"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://i"]))
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            met = json.load(f)
        # superseded_by present with existing_ccslc value
        self.assertEqual(met.get(c.SUPERSEDED_BY), c.SUPERSEDED_BY_EXISTING_CCSLC)
        self.assertIn(c.SUPERSEDED_AT, met)
        # save_compressed_cslc also suppressed (defense-in-depth)
        self.assertFalse(met[c.SAVE_COMPRESSED_CSLC])
        # is_complete still TRUE — structural readiness is preserved.
        # The trigger-disp_s1_job rule excludes via must_not exists
        # superseded_by, not via is_complete=false.
        self.assertTrue(met[c.IS_COMPLETE])
        # completeness_reason mentions supersession for operator visibility.
        self.assertIn("superseded_by=existing_ccslc", met[c.COMPLETENESS_REASON])
        evaluator._ccslc_exists_at_boundary.assert_called_once_with(7098, "20240129")

    def test_no_superseded_by_when_no_ccslc_at_boundary(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]
        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )
        evaluator._determine_save_compressed = MagicMock(return_value=True)
        evaluator._check_lineage_gap_unresolved = MagicMock(return_value=(False, ""))
        evaluator._ccslc_exists_at_boundary = MagicMock(return_value=False)

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://s"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://i"]))
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            met = json.load(f)
        # No supersession fields written at all
        self.assertNotIn(c.SUPERSEDED_BY, met)
        self.assertNotIn(c.SUPERSEDED_AT, met)
        self.assertTrue(met[c.SAVE_COMPRESSED_CSLC])
        self.assertTrue(met[c.IS_COMPLETE])


class TestKCycleEvaluatorGapUnresolved(unittest.TestCase):
    """integration — gap_unresolved propagates from evaluator to KSC."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6, 12])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}
        self.es_conn = MagicMock()

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_partial_csc_sets_gap_unresolved_true(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )
        # Force _check_lineage_gap_unresolved to report a gap.
        evaluator._check_lineage_gap_unresolved = MagicMock(
            return_value=(True, "partial CSC(s): 20240126 (1/2)")
        )

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://s"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://i"]))
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            met = json.load(f)
        self.assertTrue(met[c.GAP_UNRESOLVED])
        self.assertIn("partial CSC", met[c.COMPLETENESS_REASON])

    def test_no_partial_keeps_gap_unresolved_false(self):
        csc_hits = [
            _make_csc_hit("20240105"),
            _make_csc_hit("20240117"),
            _make_csc_hit("20240129"),
        ]

        evaluator = _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, self.es_conn, k=3, m=2
        )
        evaluator._check_lineage_gap_unresolved = MagicMock(return_value=(False, ""))

        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=csc_hits), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, ["cc1"], ["s3://cc1"], "1 CCSLCs"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://s"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://i"]))
            evaluator.evaluate(
                input_dataset_id="csc_trigger",
                metadata={c.FRAME_ID: 7098, c.SENSING_DATE: "20240129"},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20240129-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            met = json.load(f)
        self.assertFalse(met[c.GAP_UNRESOLVED])


def _ksc_hit(frame_id, sensing_date, save_compressed_cslc=True,
             superseded_by=None):
    """Build a fake KSC ES hit for _get_pending_ccslc_boundaries tests."""
    metadata = {
        c.FRAME_ID: frame_id,
        c.SENSING_DATE: sensing_date,
        c.SAVE_COMPRESSED_CSLC: save_compressed_cslc,
    }
    if superseded_by:
        metadata[c.SUPERSEDED_BY] = superseded_by
    return {"_source": {"metadata": metadata}}


class TestGetPendingCcslcBoundaries(unittest.TestCase):
    """Pending-CCSLC list: earlier k-boundary KSCs whose CCSLC is missing.

    Drives compressed_cslc_final and gates the SCIFLO trigger so KSC and
    L3 always reference the same compressed-CSLC list (opera-handel audit
    invariant).
    """

    def setUp(self):
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, ["b1"], [0])

    def _make(self, ksc_hits, ccslc_hits):
        """ES connection that returns different results per index pattern."""
        def query_side_effect(body, index):
            if "disp_s1-kcycle" in index:
                return ksc_hits
            if "l2_cslc_s1_compressed" in index:
                return ccslc_hits
            return []

        es_conn = MagicMock()
        es_conn.query.side_effect = query_side_effect
        return _make_evaluator(self.frame_to_bursts, {"b1": [7098]}, es_conn,
                               k=3, m=2)

    def test_no_earlier_k_boundary_kscs_returns_empty(self):
        # Fresh forward bootstrap: no earlier KSCs marked as k-boundary.
        evaluator = self._make(ksc_hits=[], ccslc_hits=[])
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20220411"), []
        )

    def test_earlier_boundary_with_published_ccslc_not_pending(self):
        # KSC at 20221008 is a k-boundary; CCSLC at last_date=20221008
        # exists → not pending for downstream KSC at 20221020.
        evaluator = self._make(
            ksc_hits=[_ksc_hit(7098, "20221008")],
            ccslc_hits=[_ccslc_hit(7098, "20211106", "20211106",
                                   "20221008", "20260101")],
        )
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20221020"), []
        )

    def test_earlier_boundary_without_ccslc_is_pending(self):
        # KSC at 20221008 is a k-boundary but no CCSLC at that last_date
        # exists yet → pending until the SCIFLO publishes it.
        evaluator = self._make(
            ksc_hits=[_ksc_hit(7098, "20221008")],
            ccslc_hits=[],
        )
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20221020"),
            ["20221008"],
        )

    def test_superseded_boundary_kscs_excluded_by_es_query(self):
        # Superseded boundary KSCs don't generate CCSLCs themselves
        # (imported one is already in GRQ), so the production ES query in
        # _get_pending_ccslc_boundaries excludes them via
        # ``must_not exists superseded_by``. Verify the query body filters
        # so the helper never sees them in the first place.
        es_conn = MagicMock()

        def query_side_effect(body, index):
            if "disp_s1-kcycle" in index:
                # Verify the production query filters out superseded KSCs.
                must_not = body.get("query", {}).get("bool", {}).get(
                    "must_not", []
                )
                filters = [
                    m for m in must_not
                    if m.get("exists", {}).get("field")
                    == f"metadata.{c.SUPERSEDED_BY}"
                ]
                if not filters:
                    raise AssertionError(
                        "earlier-KSC query must filter superseded via "
                        "must_not exists metadata.superseded_by"
                    )
            return []

        es_conn.query.side_effect = query_side_effect
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20220411"), []
        )

    def test_multiple_pending_boundaries_returned_sorted(self):
        evaluator = self._make(
            ksc_hits=[
                _ksc_hit(7098, "20221008"),
                _ksc_hit(7098, "20230406"),
            ],
            ccslc_hits=[
                # Only the older boundary has its CCSLC published.
                _ccslc_hit(7098, "20211106", "20211106",
                           "20221008", "20260101"),
            ],
        )
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20230418"),
            ["20230406"],
        )

    def test_ksc_query_error_returns_empty(self):
        # ES error on earlier-KSC query → don't block trigger.
        es_conn = MagicMock()
        es_conn.query.side_effect = RuntimeError("ES down")
        evaluator = _make_evaluator(
            self.frame_to_bursts, {"b1": [7098]}, es_conn, k=3, m=2
        )
        self.assertEqual(
            evaluator._get_pending_ccslc_boundaries(7098, "20221020"), []
        )


class TestCompressedCslcFinalGate(unittest.TestCase):
    """End-to-end: pending list flows from _evaluate_k_cycle through create_ksc,
    so the trigger-disp_s1_job user_rule sees the correct
    compressed_cslc_final flag."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.burst_ids = ["b1", "b2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def _build(self, csc_hits, ksc_hits=None, ccslc_hits=None):
        def query_side_effect(body, index):
            if "cslc_s1-cycle" in index:
                return csc_hits
            if "disp_s1-kcycle" in index:
                return ksc_hits or []
            if "l2_cslc_s1_compressed" in index:
                return ccslc_hits or []
            return []

        es_conn = MagicMock()
        es_conn.query.side_effect = query_side_effect
        return _make_evaluator(
            self.frame_to_bursts, self.burst_to_frames, es_conn, k=3, m=2
        )

    def test_final_flag_set_when_no_pending_boundaries(self):
        # Window has 3 complete CSCs; no earlier k-boundary KSCs exist.
        cscs = [
            _make_csc_hit("20220411", burst_ids=self.burst_ids),
            _make_csc_hit("20220423", burst_ids=self.burst_ids),
            _make_csc_hit("20220505", burst_ids=self.burst_ids),
        ]
        evaluator = self._build(csc_hits=cscs, ksc_hits=[], ccslc_hits=[])
        evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s"]))
        evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["i"]))
        evaluator._determine_save_compressed = MagicMock(return_value=False)
        evaluator._ccslc_exists_at_boundary = MagicMock(return_value=False)

        evaluator._evaluate_k_cycle(7098, "20220505", force_publish=True)

        # KSC dir should exist and contain compressed_cslc_final=true.
        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20220505-state-config"
        self.assertTrue(os.path.isdir(ksc_dir))
        met = json.load(open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")))
        self.assertTrue(met[c.IS_COMPLETE])
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [])
        self.assertTrue(met[c.COMPRESSED_CSLC_FINAL])

    def test_final_flag_blocked_when_earlier_boundary_unpublished(self):
        # KSC at 20221008 is an earlier k-boundary; its CCSLC has not
        # been published yet → current KSC's compressed_cslc_final must
        # be False so the SCIFLO trigger waits.
        cscs = [
            _make_csc_hit("20221008", burst_ids=self.burst_ids),
            _make_csc_hit("20221020", burst_ids=self.burst_ids),
            _make_csc_hit("20221101", burst_ids=self.burst_ids),
        ]
        evaluator = self._build(
            csc_hits=cscs,
            ksc_hits=[_ksc_hit(7098, "20221008")],
            ccslc_hits=[],
        )
        evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s"]))
        evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["i"]))
        evaluator._determine_save_compressed = MagicMock(return_value=False)
        evaluator._ccslc_exists_at_boundary = MagicMock(return_value=False)

        evaluator._evaluate_k_cycle(7098, "20221101", force_publish=True)

        ksc_dir = "disp_s1-kcycle-k3-m2-f7098-20221101-state-config"
        met = json.load(open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")))
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], ["20221008"])
        self.assertFalse(met[c.COMPRESSED_CSLC_FINAL])
        self.assertIn("awaiting CCSLCs at 20221008",
                      met[c.COMPLETENESS_REASON])


if __name__ == "__main__":
    unittest.main()
