"""K-cycle evaluator guards for processing-mode-annotated frames.

Two things must hold once a frame's burst database carries phases: dates a
historical batch owns must not also fire a forward SCIFLO, and nothing from
before a lineage break may be selected, anchored on, or counted.

Every guard keys off the frame carrying phases at all, which the
DISP_S1_PROCESSING_MODE_ENABLED master switch controls, so an unannotated
frame exercises exactly the un-phased code paths.
"""

import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from data_subscriber.cslc import disp_s1_constants as c

CCSLC_DOC_ID_DATE_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")


def parse_ccslc_doc_id_dates(doc_id):
    """Real implementation of the cslc_utils helper, so the lineage bound is exercised."""
    m = CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None


_mock_cslc_utils = MagicMock()
_mock_cslc_utils.parse_ccslc_doc_id_dates = parse_ccslc_doc_id_dates

with patch.dict(sys.modules, {
    "data_subscriber.cslc_utils": _mock_cslc_utils,
    "util.exec_util": MagicMock(),
    "util.ctx_util": MagicMock(),
    "data_subscriber.es_conn_util": MagicMock(),
    "hysds.celery": MagicMock(),
    "elasticsearch": MagicMock(),
    "elasticsearch.client": MagicMock(),
    "elasticsearch.transport": MagicMock(),
    "elasticsearch.serializer": MagicMock(),
}):
    from data_subscriber.cslc import disp_s1_k_cycle_evaluator as k_evaluator_mod

# Take the class off the module object that gets patched below: patch.dict drops the module from
# sys.modules on exit, so importing the class by its dotted path can yield a second, unpatched
# copy of the module when another test module has already imported this one.
DispS1KCycleEvaluator = k_evaluator_mod.DispS1KCycleEvaluator

# Build the phases with the very functions the evaluator holds, for the same reason: enum members
# compare by identity, and a second copy of the phase module would make every comparison in the
# guards false.
segment_phases = k_evaluator_mod.lineage_start_pos.__globals__["segment_phases"]
PhaseKind = k_evaluator_mod.PhaseKind

K = 3
FRAME = 7098
BURSTS = ["b1", "b2"]

# Three dates per historical phase, a two-date forward tail, then a multi-year gap.
# historical_02 starts at position 5, which is not a multiple of k.
DATES = ["20240105", "20240117", "20240129",   # historical_01
         "20240210", "20240222",               # forward_01
         "20250301", "20250313", "20250325",   # historical_02
         "20250406"]                           # forward_02
LABELS = (["historical_01"] * 3 + ["forward_01"] * 2
          + ["historical_02"] * 3 + ["forward_02"])
H02_START_DATE = "20250301"


class FakeFrame:
    """Stand-in for cslc_utils._HistBursts carrying (or not carrying) phases."""

    def __init__(self, dates, labels=None, k=K):
        self.frame_number = FRAME
        self.burst_ids = set(BURSTS)
        self.sensing_datetimes = [datetime.strptime(d, "%Y%m%d") for d in dates]
        first = self.sensing_datetimes[0]
        self.sensing_datetime_days_index = [(dt - first).days for dt in self.sensing_datetimes]
        self.sensing_seconds_since_first = [
            int((dt - first).total_seconds()) for dt in self.sensing_datetimes]
        self.processing_modes = labels
        self.phases = segment_phases(labels, k) if labels else None
        self.phase_error = None


def make_frame_map(labels=LABELS, dates=DATES):
    frame_to_bursts = defaultdict(lambda: None)
    frame_to_bursts[FRAME] = FakeFrame(dates, labels)
    return frame_to_bursts


def make_evaluator(frame_to_bursts, es_conn, k=K, m=2):
    with patch.object(k_evaluator_mod, "localize_disp_frame_burst_hist",
                      return_value=(frame_to_bursts, {b: [FRAME] for b in BURSTS}, {})), \
         patch.object(k_evaluator_mod, "localize_frame_geo_json", return_value={}), \
         patch.object(k_evaluator_mod, "localize_frame_geojson_map", return_value={}):
        evaluator = DispS1KCycleEvaluator(es_conn, k=k, m=m)

    # Frame geometry is mocked out of the burst-DB module and plays no part in these guards
    evaluator._compute_bounding_box = MagicMock(return_value=[])
    return evaluator


def csc_hit(sensing_date):
    return {"_source": {"metadata": {
        c.SENSING_DATE: sensing_date,
        c.ACQUISITION_CYCLE: DATES.index(sensing_date) * 12,
        c.IS_COMPLETE: True,
        c.EXPECTED_BURST_IDS: BURSTS,
        c.FOUND_BURST_IDS: BURSTS,
        c.CSLC_PRODUCT_PATHS: [f"s3://p_{sensing_date}"],
    }}}


def ccslc_hits(last_date, first_date=None, bursts=BURSTS):
    """One CCSLC doc per burst at a k-boundary."""
    first_date = first_date or last_date
    creation = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=3)).strftime("%Y%m%d")
    return [
        {"_id": f"OPERA_L2_COMPRESSED-CSLC-S1_{b}_{first_date}T000000Z_{first_date}T000000Z_"
                f"{last_date}T000000Z_{creation}T000000Z_VV_v1.0",
         "_source": {"metadata": {"product_s3_paths": [f"s3://ccslc/{b}_{last_date}"]}}}
        for b in bursts
    ]


class PhaseSupersessionTest(unittest.TestCase):
    """A date historical processing owns must not fire the forward SCIFLO."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.es_conn = MagicMock()
        self.es_conn.search_by_id.return_value = {"found": False}
        self.es_conn.query.return_value = []

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def evaluate(self, sensing_date, labels=LABELS, k=K):
        evaluator = make_evaluator(make_frame_map(labels=labels), self.es_conn, k=k)
        window = [csc_hit(d) for d in DATES if d <= sensing_date]
        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=window), \
             patch.object(k_evaluator_mod, "get_geojson_for_frame", return_value=None), \
             patch.object(k_evaluator_mod, "query_incomplete_kscs_with_sensing_date",
                          return_value=[]):
            evaluator._get_compressed_cslcs = MagicMock(return_value=(True, [], [], "early"))
            evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://static"]))
            evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://iono"]))
            evaluator.evaluate(
                input_dataset_id=f"cslc_s1-cycle-f{FRAME}-{sensing_date}-state-config",
                metadata={c.FRAME_ID: FRAME, c.SENSING_DATE: sensing_date},
                dataset_type=c.CSLC_S1_CYCLE_STATE_CONFIG,
            )
        ksc_dir = f"disp_s1-kcycle-k{k}-m2-f{FRAME}-{sensing_date}-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            return json.load(f)

    def test_historical_phase_date_is_superseded(self):
        met = self.evaluate("20240129")

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertFalse(met[c.SAVE_COMPRESSED_CSLC])

    def test_post_gap_historical_phase_date_is_superseded(self):
        met = self.evaluate("20250325")

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)

    def test_forward_phase_date_is_untouched(self):
        met = self.evaluate("20240222")

        self.assertIsNone(met.get(c.SUPERSEDED_BY))

    def test_unannotated_frame_is_untouched(self):
        met = self.evaluate("20240129", labels=None)

        self.assertIsNone(met.get(c.SUPERSEDED_BY))

    def test_no_run_date_is_superseded(self):
        met = self.evaluate("20240129", labels=["no_run"] * len(DATES))

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)


class LineageBoundTest(unittest.TestCase):
    """Nothing from before a lineage break may feed the block after it."""

    def setUp(self):
        self.es_conn = MagicMock()
        self.es_conn.search_by_id.return_value = {"found": False}
        self.es_conn.query.return_value = []
        self.evaluator = make_evaluator(make_frame_map(), self.es_conn)
        self.unphased = make_evaluator(make_frame_map(labels=None), self.es_conn)

    def test_lineage_start_date(self):
        self.assertEqual(self.evaluator._lineage_start_date(FRAME, "20240117"), DATES[0])
        # A forward phase chains onto its own chunk's historical block
        self.assertEqual(self.evaluator._lineage_start_date(FRAME, "20240222"), DATES[0])
        self.assertEqual(self.evaluator._lineage_start_date(FRAME, "20250313"), H02_START_DATE)
        self.assertEqual(self.evaluator._lineage_start_date(FRAME, "20250406"), H02_START_DATE)
        # Unannotated frames bound nothing
        self.assertEqual(self.unphased._lineage_start_date(FRAME, "20250313"), "")

    def test_window_does_not_straddle_a_lineage_break(self):
        hits = [csc_hit(d) for d in DATES[:7]]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            window = self.evaluator._get_window_cscs(FRAME, "20250313")

        self.assertEqual([e[c.SENSING_DATE] for e in window], ["20250301", "20250313"])

    def test_window_without_annotations_spans_the_break(self):
        hits = [csc_hit(d) for d in DATES[:7]]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.unphased._query_cslc_catalog = MagicMock(return_value={})
            window = self.unphased._get_window_cscs(FRAME, "20250313")

        self.assertEqual([e[c.SENSING_DATE] for e in window],
                         ["20240222", "20250301", "20250313"])

    def test_date_position_is_phase_relative(self):
        hits = [csc_hit(d) for d in DATES]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            self.unphased._query_cslc_catalog = MagicMock(return_value={})

            self.assertEqual(self.evaluator._get_date_position(FRAME, H02_START_DATE), 0)
            self.assertEqual(self.unphased._get_date_position(FRAME, H02_START_DATE), 5)

    def test_pre_gap_ccslcs_are_not_selected_as_input(self):
        self.es_conn.query.return_value = ccslc_hits("20240129")

        satisfied, ids, paths, detail = self.evaluator._get_compressed_cslcs(FRAME, "20250313")

        self.assertTrue(satisfied)
        self.assertEqual(ids, [])
        self.assertIn("early window", detail)

    def test_pre_gap_ccslcs_are_selected_without_annotations(self):
        self.es_conn.query.return_value = ccslc_hits("20240129")

        satisfied, ids, _, _ = self.unphased._get_compressed_cslcs(FRAME, "20250313")

        self.assertTrue(satisfied)
        self.assertEqual(len(ids), len(BURSTS))

    def test_in_phase_ccslcs_are_selected(self):
        self.es_conn.query.return_value = (ccslc_hits("20240129")
                                           + ccslc_hits("20250301"))

        satisfied, ids, _, _ = self.evaluator._get_compressed_cslcs(FRAME, "20250325")

        self.assertTrue(satisfied)
        self.assertTrue(all("20250301T000000Z" in i for i in ids))

    def test_save_compressed_anchors_within_the_phase(self):
        """The post-gap block's third date closes its first ministack."""

        self.es_conn.query.return_value = ccslc_hits("20240129")
        hits = [csc_hit(d) for d in DATES]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            self.unphased._query_cslc_catalog = MagicMock(return_value={})

            self.assertTrue(self.evaluator._determine_save_compressed(FRAME, "20250325"))
            # Anchored on the pre-gap CCSLC, the same date is not a boundary at all
            self.assertFalse(self.unphased._determine_save_compressed(FRAME, "20250325"))

    def test_leading_edge_dates_belong_to_the_last_chunk(self):
        """Forward production running ahead of the burst database keeps the post-gap bound."""

        assert self.evaluator._lineage_start_date(FRAME, "20260101") == H02_START_DATE
        # A date inside the annotated range that the database does not list bounds nothing
        assert self.evaluator._lineage_start_date(FRAME, "20250307") == ""

    def test_pending_boundaries_are_projected_within_the_lineage(self):
        """The projection strides from the lineage's own boundary, not the absolute grid."""

        # Only a pre-gap compressed CSLC exists; the post-gap block has published nothing yet
        self.es_conn.query.side_effect = lambda index=None, body=None: (
            ccslc_hits("20240129") if "compressed" in index else [])
        hits = [csc_hit(d) for d in DATES]

        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            pending = self.evaluator._get_pending_ccslc_boundaries(FRAME, "20250325")

        # Nothing to wait for: the pre-gap boundary is not this lineage's input
        assert pending == []

    def test_pending_boundaries_still_wait_within_the_lineage(self):
        """An in-lineage boundary whose compressed CSLC has not published still blocks."""

        ksc_hit = {"_source": {"metadata": {c.SENSING_DATE: "20250313"}}}
        self.es_conn.query.side_effect = lambda index=None, body=None: (
            [] if "compressed" in index else [ksc_hit])
        hits = [csc_hit(d) for d in DATES]

        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=hits):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            pending = self.evaluator._get_pending_ccslc_boundaries(FRAME, "20250325")

        assert pending == ["20250313"]

    def test_lineage_lower_bound_ignores_pre_gap_ccslcs(self):
        self.es_conn.query.return_value = ccslc_hits("20240129")

        self.assertEqual(self.evaluator._get_lineage_lower_bound(FRAME, "20250313"), "")
        self.assertEqual(self.unphased._get_lineage_lower_bound(FRAME, "20250313"), "20240129")


if __name__ == "__main__":
    unittest.main()
