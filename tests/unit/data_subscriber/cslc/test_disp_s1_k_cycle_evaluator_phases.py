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
from data_subscriber.cslc_utils import batch_proc_covers_k_set

CCSLC_DOC_ID_DATE_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")


def parse_ccslc_doc_id_dates(doc_id):
    """Real implementation of the cslc_utils helper, so the lineage bound is exercised."""
    m = CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None


_mock_cslc_utils = MagicMock()
_mock_cslc_utils.parse_ccslc_doc_id_dates = parse_ccslc_doc_id_dates

# The evaluators import latest_cslc_per_burst by name, so the module mock must supply a
# working one or every product-path list becomes a MagicMock. These tests predate the
# deduplication and assert the old sorted-unique semantics, which is exactly what this
# preserves; the selection rule itself is covered by test_latest_cslc_per_burst.py.
_mock_cslc_utils.latest_cslc_per_burst = lambda paths: sorted(set(paths or []))


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

    def __init__(self, dates, labels=None, k=K, clock=timedelta()):
        self.frame_number = FRAME
        self.burst_ids = set(BURSTS)
        # clock: the time of day every acquisition is sensed at
        self.sensing_datetimes = [datetime.strptime(d, "%Y%m%d") + clock for d in dates]
        first = self.sensing_datetimes[0]
        self.sensing_datetime_days_index = [(dt - first).days for dt in self.sensing_datetimes]
        self.sensing_seconds_since_first = [
            int((dt - first).total_seconds()) for dt in self.sensing_datetimes]
        self.processing_modes = labels
        self.phases = segment_phases(labels, k) if labels else None
        self.phase_error = None


def make_frame_map(labels=LABELS, dates=DATES, clock=timedelta()):
    frame_to_bursts = defaultdict(lambda: None)
    frame_to_bursts[FRAME] = FakeFrame(dates, labels, clock=clock)
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

    def evaluate(self, sensing_date, labels=LABELS, k=K, clock=timedelta()):
        evaluator = make_evaluator(make_frame_map(labels=labels, clock=clock), self.es_conn, k=k)
        self.last_evaluator = evaluator
        window = [csc_hit(d) for d in DATES if d <= sensing_date]
        # The supersede warning judges coverage with the walk's own date rule. Patched here rather than
        # on the module mock: another test module may have loaded the evaluator with its own mock.
        with patch.object(k_evaluator_mod, "batch_proc_covers_k_set", batch_proc_covers_k_set), \
             patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
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

    def batch_procs(self, *sources, error=None):
        def query(index=None, body=None):
            if index == k_evaluator_mod.BATCH_PROC_INDEX:
                if error:
                    raise error
                return [{"_source": source} for source in sources]
            return []
        self.es_conn.query.side_effect = query

    def batch_proc_queries(self):
        return [call for call in self.es_conn.query.call_args_list
                if call.kwargs.get("index") == k_evaluator_mod.BATCH_PROC_INDEX]

    def test_supersede_on_a_venue_with_no_historical_batch_proc_warns(self):
        """No historical batch proc on this venue covers the superseded date; say so."""
        self.batch_procs({"job_type": "cslc_query_hist", "frames": [8000]},
                         {"job_type": "slc_query", "frames": [FRAME]})

        with self.assertLogs(k_evaluator_mod.logger, "WARNING") as logs:
            met = self.evaluate("20240129")

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        warnings = [r.getMessage() for r in logs.records if "historical batch proc" in r.getMessage()]
        self.assertEqual(len(warnings), 1)
        self.assertIn("DISP_S1_PROCESSING_MODE_ENABLED", warnings[0])
        self.assertIn("another venue", warnings[0])
        self.assertNotIn("DISP_S1_PROCESSING_MODE_ENABLED: false", warnings[0])  # never advise turning it off
        self.assertIn(f"f{FRAME} 20240129 NO HISTORICAL BATCH PROC", self.last_evaluator.msgs)

    @staticmethod
    def hist_proc(frames=(FRAME,), start="2025-01-01T00:00:00", end="2025-12-31T00:00:00", **extra):
        proc = {"job_type": "cslc_query_hist", "frames": list(frames), **extra}
        if start is not None:
            proc["data_start_date"] = start
        if end is not None:
            proc["data_end_date"] = end
        return proc

    def assert_quiet(self, sensing_date, **kw):
        met = self.evaluate(sensing_date, **kw)
        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertFalse([m for m in self.last_evaluator.msgs if "NO HISTORICAL BATCH PROC" in m])

    def assert_warns(self, sensing_date, **kw):
        with self.assertLogs(k_evaluator_mod.logger, "WARNING") as logs:
            met = self.evaluate(sensing_date, **kw)
        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertIn(f"f{FRAME} {sensing_date} NO HISTORICAL BATCH PROC", self.last_evaluator.msgs)
        warnings = [r.getMessage() for r in logs.records if "historical batch proc" in r.getMessage()]
        self.assertEqual(len(warnings), 1)
        return warnings[0]

    def test_supersede_with_a_batch_proc_covering_the_date_is_quiet(self):
        # A finished campaign's batch proc is disabled but still covers the frame, here via a range
        self.batch_procs(self.hist_proc(frames=[[FRAME - 1, FRAME + 1]], enabled=False))

        self.assert_quiet("20250325")

    def test_finished_campaign_before_a_gap_does_not_cover_the_post_gap_phase(self):
        # The case the warning exists for: the old campaign lists the frame, but a gap reset
        # batch proc for historical_02 was never created, so its dates would be lost silently.
        self.batch_procs(self.hist_proc(start="2024-01-01T00:00:00", end="2024-02-28T00:00:00",
                                        enabled=False))

        warning = self.assert_warns("20250325")

        self.assertIn("covers it", warning)
        self.assertIn("2025-03-01T00:00:00 to 2025-03-25T00:00:00", warning)

    def test_finished_campaign_still_covers_its_own_dates(self):
        # A CSLC re-ingested after the campaign finished: superseded, and rightly quiet
        self.batch_procs(self.hist_proc(start="2024-01-01T00:00:00", end="2024-02-28T00:00:00",
                                        enabled=False))

        self.assert_quiet("20240129")

    def test_batch_proc_range_exactly_on_the_k_set_is_quiet(self):
        # Both ends are inclusive, as in the walk
        self.batch_procs(self.hist_proc(start="2025-03-01T00:00:00", end="2025-03-25T00:00:00"))

        self.assert_quiet("20250313")

    def test_batch_proc_ending_inside_the_k_set_does_not_cover_it(self):
        # The date itself is inside the range, but the walk stops before a k-set that ends after
        # data_end_date, so no date of that k-set is produced.
        self.batch_procs(self.hist_proc(start="2025-01-01T00:00:00", end="2025-03-13T00:00:00"))

        self.assert_warns("20250301")

    def test_batch_proc_starting_inside_the_k_set_does_not_cover_it(self):
        # The walk skips a k-set that starts before data_start_date, later dates included
        self.batch_procs(self.hist_proc(start="2025-03-01T00:00:01", end="2025-12-31T00:00:00"))

        self.assert_warns("20250325")

    def test_coverage_compares_sensing_datetimes_not_days(self):
        # Acquisitions sensed at 01:32:39 are inside a range opening at 01:00 that day, as the walk sees it
        self.batch_procs(self.hist_proc(start="2025-03-01T01:00:00", end="2025-03-25T02:00:00"))

        self.assert_quiet("20250301", clock=timedelta(hours=1, minutes=32, seconds=39))

    def test_coverage_range_opening_after_the_acquisition_time_warns(self):
        self.batch_procs(self.hist_proc(start="2025-03-01T02:00:00", end="2025-03-25T02:00:00"))

        self.assert_warns("20250313", clock=timedelta(hours=1, minutes=32, seconds=39))

    def test_batch_proc_for_another_frame_does_not_cover(self):
        self.batch_procs(self.hist_proc(frames=[FRAME + 5]))

        self.assert_warns("20250325")

    def test_batch_proc_missing_a_date_covers_nothing(self):
        self.batch_procs(self.hist_proc(end=None), self.hist_proc(start=None))

        self.assert_warns("20250325")

    def test_no_run_date_does_not_look_for_batch_procs(self):
        # no_run dates get no product by design; there is nothing a batch proc could be missing
        self.batch_procs()

        met = self.evaluate("20240129", labels=["no_run"] * len(DATES))

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertEqual(self.batch_proc_queries(), [])
        self.assertFalse([m for m in self.last_evaluator.msgs if "NO HISTORICAL BATCH PROC" in m])

    def test_unreadable_batch_procs_still_supersede_and_warn(self):
        self.batch_procs(error=RuntimeError("index_not_found_exception"))

        with self.assertLogs(k_evaluator_mod.logger, "WARNING") as logs:
            met = self.evaluate("20240129")

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertTrue(any("could not be read" in r.getMessage() for r in logs.records))

    def test_forward_phase_date_does_not_look_for_batch_procs(self):
        self.batch_procs()

        self.evaluate("20240222")

        self.assertEqual(self.batch_proc_queries(), [])


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

    @staticmethod
    def ends(ministacks):
        return [f"{last:%Y%m%d}" for _, last in ministacks]

    def test_historical_ministacks_of_a_forward_date(self):
        # A forward date continues the historical phase of its own chunk
        self.assertEqual(self.ends(self.evaluator._historical_ministacks(FRAME, "20240210")), ["20240129"])
        self.assertEqual(self.ends(self.evaluator._historical_ministacks(FRAME, "20250406")), ["20250325"])
        # ... as does a date past the last annotated one
        self.assertEqual(self.ends(self.evaluator._historical_ministacks(FRAME, "20260101")), ["20250325"])
        # Historical processing owns a date in a historical phase
        self.assertEqual(self.evaluator._historical_ministacks(FRAME, "20250313"), [])
        # A date inside the annotated range that the database does not list
        self.assertEqual(self.evaluator._historical_ministacks(FRAME, "20250307"), [])
        self.assertEqual(self.unphased._historical_ministacks(FRAME, "20250406"), [])

    def test_historical_ministacks_of_a_lineage_with_two(self):
        dates = DATES[:3] + ["20240210", "20240222", "20240305"] + ["20240317"]
        labels = ["historical_01"] * 6 + ["forward_01"]
        evaluator = make_evaluator(make_frame_map(labels=labels, dates=dates), self.es_conn)

        ministacks = evaluator._historical_ministacks(FRAME, "20240317")

        self.assertEqual([(f"{first:%Y%m%d}", f"{last:%Y%m%d}") for first, last in ministacks],
                         [("20240105", "20240129"), ("20240210", "20240305")])

    def test_no_run_chunk_has_no_historical_ministack(self):
        evaluator = make_evaluator(make_frame_map(labels=["no_run"] * len(DATES)), self.es_conn)

        self.assertEqual(evaluator._historical_ministacks(FRAME, "20260101"), [])

    def test_forward_date_waits_for_its_historical_ministack(self):
        """Only the pre-gap compressed CSLC exists; the post-gap ministack has published nothing."""
        self.es_conn.query.return_value = ccslc_hits("20240129")

        satisfied, ids, paths, detail = self.evaluator._get_compressed_cslcs(FRAME, "20250406")

        self.assertFalse(satisfied)
        self.assertEqual((ids, paths), ([], []))
        self.assertNotIn("early window", detail)
        self.assertIn("historical ministack ending 20250325", detail)

    def test_forward_date_takes_the_ministack_once_it_has_published(self):
        self.es_conn.query.return_value = (ccslc_hits("20240129")
                                           + ccslc_hits("20250325", first_date="20250301"))

        satisfied, ids, paths, detail = self.evaluator._get_compressed_cslcs(FRAME, "20250406")

        self.assertTrue(satisfied)
        self.assertEqual(len(ids), len(BURSTS))
        self.assertTrue(all("_20250301T000000Z_20250325T000000Z_" in i for i in ids))
        self.assertEqual(len(paths), len(BURSTS))
        self.assertIn("20250301..20250325", detail)

    def test_forward_date_past_the_database_waits_too(self):
        """Daily forward processing runs ahead of the burst database and continues the last chunk."""
        self.es_conn.query.return_value = ccslc_hits("20240129")

        satisfied, _, _, detail = self.evaluator._get_compressed_cslcs(FRAME, "20260101")

        self.assertFalse(satisfied)
        self.assertIn("historical ministack ending 20250325", detail)

    def test_first_ministack_of_a_lineage_is_still_an_early_window(self):
        """A date inside the lineage's first ministack needs no compressed CSLCs."""
        self.es_conn.query.return_value = ccslc_hits("20240129")

        for sensing_date in ("20250301", "20250313", "20250325"):
            satisfied, ids, _, detail = self.evaluator._get_compressed_cslcs(FRAME, sensing_date)
            self.assertTrue(satisfied, sensing_date)
            self.assertEqual(ids, [])
            self.assertIn("early window", detail)

    def test_unannotated_frame_keeps_its_early_window(self):
        self.es_conn.query.return_value = []

        satisfied, ids, _, detail = self.unphased._get_compressed_cslcs(FRAME, "20250406")

        self.assertTrue(satisfied)
        self.assertEqual(ids, [])
        self.assertIn("early window", detail)


class ForwardDateBeforeItsHistoricalMinistackTest(unittest.TestCase):
    """A forward date evaluated before the historical ministack ahead of it has published.

    Observed on frame 16955, a one-burst frame whose burst a neighbouring frame also holds. Its
    post-gap lineage is one historical ministack, 2025-05-27 to 2026-05-22, followed by forward
    dates. The neighbour's ingest delivered the burst for 2026-06-03 and 2026-06-16 while the
    historical job was still running, so both dates were evaluated about half an hour before the
    ministack's compressed CSLC reached GRQ. They were marked ready with none, their jobs were
    submitted and the SAS refused them.
    """

    FRAME = 16955
    BURST = "T064-135633-IW1"
    K = 15
    M = 6
    PRE_GAP = ["20200610", "20200622", "20200704", "20200716", "20200728", "20200809", "20200821",
               "20200902", "20200914", "20200926", "20201008", "20201020", "20210430", "20210512",
               "20210524"]
    MINISTACK = ["20250527", "20250608", "20250620", "20250702", "20250714", "20250726",
                 "20250807", "20250819", "20250831", "20250912", "20250924", "20251006",
                 "20251018", "20260428", "20260522"]
    FORWARD = ["20260603", "20260616", "20260716"]
    DATES = PRE_GAP + MINISTACK + FORWARD
    LABELS = ["historical_01"] * 15 + ["historical_02"] * 15 + ["forward_02"] * 3
    PRE_GAP_CCSLC = ("OPERA_L2_COMPRESSED-CSLC-S1_F16955_T064-135633-IW1_20210524T000000Z_"
                     "20200610T000000Z_20210524T000000Z_20251024T231446Z_VV_v1.0")
    MINISTACK_CCSLC = ("OPERA_L2_COMPRESSED-CSLC-S1_F16955_T064-135633-IW1_20260522T000000Z_"
                       "20250527T000000Z_20260522T000000Z_20261121T024150Z_VV_v1.0")

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.published = [self.PRE_GAP_CCSLC]
        frame = FakeFrame(self.DATES, self.LABELS, k=self.K)
        frame.frame_number = self.FRAME
        frame.burst_ids = {self.BURST}
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[self.FRAME] = frame

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def csc(self, sensing_date):
        return {"_source": {"metadata": {
            c.SENSING_DATE: sensing_date,
            c.ACQUISITION_CYCLE: self.DATES.index(sensing_date),
            c.IS_COMPLETE: True,
            c.EXPECTED_BURST_IDS: [self.BURST],
            c.FOUND_BURST_IDS: [self.BURST],
            c.CSLC_PRODUCT_PATHS: [f"s3://cslc/{self.BURST}_{sensing_date}.h5"],
        }}}

    def evaluate(self, sensing_date, run):
        """Evaluate one date against what GRQ holds now; returns the state config written."""
        def query(index=None, body=None):
            if "compressed" in index:
                return [{"_id": i, "_source": {"metadata": {
                    "product_s3_paths": [f"s3://lts/{i}/{i}.h5"]}}} for i in self.published]
            if "cslc_s1-cycle" in index:
                return [self.csc(d) for d in self.DATES if d <= sensing_date]
            return []

        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {"found": False}
        es_conn.query.side_effect = query
        with patch.object(k_evaluator_mod, "localize_disp_frame_burst_hist",
                          return_value=(self.frame_to_bursts, {self.BURST: [self.FRAME]}, {})), \
             patch.object(k_evaluator_mod, "localize_frame_geo_json", return_value={}), \
             patch.object(k_evaluator_mod, "localize_frame_geojson_map", return_value={}):
            evaluator = DispS1KCycleEvaluator(es_conn, k=self.K, m=self.M)
        evaluator._compute_bounding_box = MagicMock(return_value=[])
        evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://static"]))
        evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://iono"]))

        run_dir = os.path.join(self.test_dir, run)
        os.makedirs(run_dir)
        os.chdir(run_dir)
        # The region and geometry lookups come from whichever mock the evaluator module was first
        # imported with; pinned here so the state config can be written whatever that was.
        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "get_region_from_frame", return_value="UNKNOWN"), \
             patch.object(k_evaluator_mod, "get_geojson_for_frame", return_value=None):
            evaluator._evaluate_k_cycle(self.FRAME, sensing_date, force_publish=True, cascade=False)
        ksc_dir = f"disp_s1-kcycle-k{self.K}-m{self.M}-f{self.FRAME}-{sensing_date}-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            return json.load(f)

    @staticmethod
    def fires(met):
        """The trigger rule's conditions for submitting the forward job."""
        return bool(met[c.IS_COMPLETE] and met[c.COMPRESSED_CSLC_FINAL]
                    and not met[c.GAP_UNRESOLVED] and not met.get(c.SUPERSEDED_BY))

    def test_the_date_does_not_fire_before_the_ministack_has_published(self):
        for sensing_date in ("20260603", "20260616"):
            met = self.evaluate(sensing_date, f"before-{sensing_date}")

            self.assertEqual(met[c.CYCLES_COMPLETE], self.K)
            self.assertFalse(self.fires(met), sensing_date)
            self.assertFalse(met[c.COMPRESSED_CSLC_SATISFIED])
            self.assertEqual(met[c.COMPRESSED_CSLC_IDS], [])
            self.assertNotIn("early window", met[c.COMPLETENESS_REASON])
            self.assertIn("historical ministack ending 20260522", met[c.COMPLETENESS_REASON])

    def test_the_date_fires_with_the_ministack_once_it_has_published(self):
        before = self.evaluate("20260603", "before")
        self.assertFalse(self.fires(before))

        self.published.append(self.MINISTACK_CCSLC)
        after = self.evaluate("20260603", "after")

        self.assertTrue(self.fires(after))
        self.assertEqual(after[c.COMPRESSED_CSLC_IDS], [self.MINISTACK_CCSLC])
        self.assertEqual(len(after[c.PRODUCT_PATHS]["L2_CSLC_S1_COMPRESSED"]), 1)
        self.assertIn("1 CCSLCs [20250527..20260522]", after[c.COMPLETENESS_REASON])
        # The window is the fifteen dates up to the forward date, all inside the lineage.
        self.assertEqual(after[c.WINDOW_SENSING_DATES][0], "20250608")

    def test_no_state_config_that_fires_has_an_empty_compressed_cslc_list(self):
        """Every forward date of the lineage, before and after the ministack publishes."""
        for published in (False, True):
            self.published = [self.PRE_GAP_CCSLC] + ([self.MINISTACK_CCSLC] if published else [])
            for sensing_date in self.FORWARD:
                met = self.evaluate(sensing_date, f"{published}-{sensing_date}")
                if self.fires(met):
                    self.assertTrue(met[c.COMPRESSED_CSLC_IDS], sensing_date)
                self.assertEqual(self.fires(met), published, sensing_date)

    def test_the_ministack_boundary_itself_is_left_to_historical_processing(self):
        """Unchanged: the boundary date is superseded, and needs no compressed CSLCs of its own."""
        met = self.evaluate("20260522", "boundary")

        self.assertEqual(met[c.SUPERSEDED_BY], c.SUPERSEDED_BY_HISTORICAL_PROCESSING)
        self.assertIn("early window", met[c.COMPLETENESS_REASON])


class ForwardDateBehindTwoHistoricalMinistacksTest(unittest.TestCase):
    """A forward date waits for every historical ministack of its lineage, not only the first.

    The lineage here is one historical phase of two ministacks, A and B, then forward dates. The
    forward date's CSLCs are in GRQ early, as on frame 16955. Once A has published there is a
    compressed CSLC in the lineage, so the date is no longer an early window, but B is still to
    come: firing then would give the job A's compressed CSLC only, and a window that skips B's
    dates whenever B's CSLCs are not catalogued yet. The SAS would accept both.
    """

    FRAME = 16669
    BURST = "T064-135633-IW1"
    K = 15
    DATES = [(datetime(2024, 1, 5) + timedelta(days=12 * i)).strftime("%Y%m%d") for i in range(32)]
    A, B, FORWARD = DATES[:15], DATES[15:30], DATES[30:]
    LABELS = ["historical_01"] * 30 + ["forward_01"] * 2

    @classmethod
    def ccslc(cls, first, last, made="20261121T024150"):
        return (f"OPERA_L2_COMPRESSED-CSLC-S1_F{cls.FRAME}_{cls.BURST}_{last}T000000Z_"
                f"{first}T000000Z_{last}T000000Z_{made}Z_VV_v1.0")

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.ccslc_a = self.ccslc(self.A[0], self.A[-1])
        self.ccslc_b = self.ccslc(self.B[0], self.B[-1], made="20261122T024150")
        self.runs = 0

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def evaluate(self, published, catalogued, m=6, batch_procs=None):
        """Evaluate the first forward date. published: compressed CSLC ids in GRQ. catalogued:
        the historical dates whose CSLCs the historical jobs have catalogued so far."""
        sensing_date = self.FORWARD[0]
        frame = FakeFrame(self.DATES, self.LABELS, k=self.K)
        frame.frame_number = self.FRAME
        frame.burst_ids = {self.BURST}
        frame_to_bursts = defaultdict(lambda: None)
        frame_to_bursts[self.FRAME] = frame

        def query(index=None, body=None):
            if "compressed" in index:
                return [{"_id": i, "_source": {"metadata": {
                    "product_s3_paths": [f"s3://lts/{i}/{i}.h5"]}}} for i in published]
            if "cslc_s1-cycle" in index:
                return [{"_source": {"metadata": {
                    c.SENSING_DATE: sensing_date, c.ACQUISITION_CYCLE: 372, c.IS_COMPLETE: True,
                    c.EXPECTED_BURST_IDS: [self.BURST], c.FOUND_BURST_IDS: [self.BURST],
                    c.CSLC_PRODUCT_PATHS: [f"s3://cslc/{self.BURST}_{sensing_date}.h5"]}}}]
            if "cslc_catalog" in index:
                return [{"_source": {
                    "frame_id": self.FRAME, "burst_id": self.BURST,
                    "acquisition_ts": f"{d[:4]}-{d[4:6]}-{d[6:]}T01:54:43",
                    "acquisition_cycle": self.DATES.index(d) * 12,
                    "download_batch_id": f"f{self.FRAME}_a{self.DATES.index(d) * 12}",
                    "s3_url": f"s3://hist/{self.BURST}_{d}.h5"}} for d in catalogued]
            if index == k_evaluator_mod.BATCH_PROC_INDEX:
                return [{"_source": proc} for proc in (batch_procs or [])]
            return []

        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {"found": False}
        es_conn.query.side_effect = query
        with patch.object(k_evaluator_mod, "localize_disp_frame_burst_hist",
                          return_value=(frame_to_bursts, {self.BURST: [self.FRAME]}, {})), \
             patch.object(k_evaluator_mod, "localize_frame_geo_json", return_value={}), \
             patch.object(k_evaluator_mod, "localize_frame_geojson_map", return_value={}):
            evaluator = DispS1KCycleEvaluator(es_conn, k=self.K, m=m)
        evaluator._compute_bounding_box = MagicMock(return_value=[])
        evaluator._resolve_static_layers = MagicMock(return_value=(True, ["s3://static"]))
        evaluator._resolve_ionosphere_files = MagicMock(return_value=(True, ["s3://iono"]))
        self.evaluator = evaluator

        self.runs += 1
        run_dir = os.path.join(self.test_dir, str(self.runs))
        os.makedirs(run_dir)
        os.chdir(run_dir)
        with patch.object(k_evaluator_mod, "find_ksc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "find_csc", return_value=({}, None)), \
             patch.object(k_evaluator_mod, "burst_db_exclusion_enabled", return_value=False), \
             patch.object(k_evaluator_mod, "batch_proc_covers_k_set", batch_proc_covers_k_set), \
             patch.object(k_evaluator_mod, "get_region_from_frame", return_value="UNKNOWN"), \
             patch.object(k_evaluator_mod, "get_geojson_for_frame", return_value=None):
            evaluator._evaluate_k_cycle(self.FRAME, sensing_date, force_publish=True, cascade=False)
        ksc_dir = f"disp_s1-kcycle-k{self.K}-m{m}-f{self.FRAME}-{sensing_date}-state-config"
        with open(os.path.join(ksc_dir, f"{ksc_dir}.met.json")) as f:
            return json.load(f)

    @staticmethod
    def fires(met):
        return bool(met[c.IS_COMPLETE] and met[c.COMPRESSED_CSLC_FINAL]
                    and not met[c.GAP_UNRESOLVED] and not met.get(c.SUPERSEDED_BY))

    def test_nothing_published_waits_for_both_ministacks(self):
        met = self.evaluate(published=[], catalogued=self.A)

        self.assertFalse(self.fires(met))
        self.assertFalse(met[c.COMPRESSED_CSLC_SATISFIED])
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [self.A[-1], self.B[-1]])
        self.assertIn(f"historical ministack ending {self.A[-1]}", met[c.COMPLETENESS_REASON])

    def test_first_ministack_published_still_waits_for_the_second(self):
        """What the first ministack's set marker re-evaluates, before B's CSLCs are catalogued."""
        met = self.evaluate(published=[self.ccslc_a], catalogued=self.A)

        self.assertFalse(self.fires(met))
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [self.B[-1]])
        self.assertFalse(met[c.COMPRESSED_CSLC_FINAL])
        self.assertIn(f"awaiting CCSLCs at {self.B[-1]}", met[c.COMPLETENESS_REASON])

    def test_second_ministack_catalogued_and_not_published_still_waits(self):
        met = self.evaluate(published=[self.ccslc_a], catalogued=self.A + self.B)

        self.assertFalse(self.fires(met))
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [self.B[-1]])

    def test_both_ministacks_published_fires_with_both(self):
        met = self.evaluate(published=[self.ccslc_a, self.ccslc_b], catalogued=self.A + self.B)

        self.assertTrue(self.fires(met))
        self.assertEqual(met[c.COMPRESSED_CSLC_IDS], sorted([self.ccslc_a, self.ccslc_b]))
        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [])
        # the window is B's last fourteen dates and the forward date
        self.assertEqual(met[c.WINDOW_SENSING_DATES], self.B[1:] + [self.FORWARD[0]])

    def test_a_ministack_made_on_another_date_grid_counts_as_published(self):
        """B's compressed CSLC ends one acquisition before the database's boundary; it is still
        B's, and the date must not wait for a boundary that will never be written."""
        shifted_b = self.ccslc(self.B[0], self.B[-2], made="20261122T024150")

        met = self.evaluate(published=[self.ccslc_a, shifted_b], catalogued=self.A + self.B)

        self.assertEqual(met[c.COMPRESSED_CSLC_PENDING], [])
        self.assertTrue(self.fires(met))

    def test_only_the_boundaries_the_job_takes_are_waited_for(self):
        """With m=2 the job takes one compressed CSLC, the newest: B's."""
        waits = self.evaluate(published=[self.ccslc_a], catalogued=self.A + self.B, m=2)
        self.assertEqual(waits[c.COMPRESSED_CSLC_PENDING], [self.B[-1]])

        fires = self.evaluate(published=[self.ccslc_b], catalogued=self.A + self.B, m=2)
        self.assertEqual(fires[c.COMPRESSED_CSLC_PENDING], [])
        self.assertEqual(fires[c.COMPRESSED_CSLC_IDS], [self.ccslc_b])

    def proc(self, **overrides):
        proc = {"job_type": "cslc_query_hist", "frames": [self.FRAME],
                "data_start_date": "2016-07-01T00:00:00", "data_end_date": "2026-11-01T00:00:00"}
        proc.update(overrides)
        return proc

    def test_waiting_with_a_batch_proc_that_makes_the_ministack_is_quiet(self):
        self.evaluate(published=[], catalogued=self.A, batch_procs=[self.proc(enabled=False)])

        self.assertIn("CCSLCs awaited", self.evaluator.msgs)
        self.assertFalse([m for m in self.evaluator.msgs if "NO HISTORICAL BATCH PROC" in m])

    def test_waiting_for_a_ministack_no_batch_proc_here_makes_says_so(self):
        """A venue that only runs forward processing: nothing on it will publish the ministack."""
        with self.assertLogs(k_evaluator_mod.logger, "WARNING") as logs:
            self.evaluate(published=[], catalogued=self.A, batch_procs=[self.proc(frames=[8000])])

        self.assertIn(f"f{self.FRAME} {self.FORWARD[0]} NO HISTORICAL BATCH PROC", self.evaluator.msgs)
        warnings = [r.getMessage() for r in logs.records if "historical ministack" in r.getMessage()]
        self.assertEqual(len(warnings), 1)
        self.assertIn("2024-01-05 to 2024-06-21", warnings[0])
        self.assertIn("another venue", warnings[0])


if __name__ == "__main__":
    unittest.main()
