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
    """OPERA-2466: lineage gap detection — partial CSC in (CCSLC.last_date, sensing_date]."""

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
    """OPERA-2466: CCSLC last_date lookup for lineage bounding."""

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


class TestKCycleEvaluatorGapUnresolved(unittest.TestCase):
    """OPERA-2466: integration — gap_unresolved propagates from evaluator to KSC."""

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


if __name__ == "__main__":
    unittest.main()
