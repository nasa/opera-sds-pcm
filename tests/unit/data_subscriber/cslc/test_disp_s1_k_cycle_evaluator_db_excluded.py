"""Acquisitions the consistent burst database deliberately left out.

A pass that covers only part of a frame is published to CMR anyway, and the cascade
writes a cycle state config for it. The burst database excludes such a date from
sensing_time_list, but nothing downstream used to know that: the partial state config
occupied a k-slot AND counted as an unresolved gap in the lineage, so every later
forward date of the frame stalled behind it -- silently, because a no-fire disposition
is terminal and the walk simply advanced past.

Absence only carries that meaning inside the range the database's CMR survey assessed.
Past it the survey never looked, so a date absent there is ordinary forward work and
must keep flowing. These tests pin both halves, and the fail-open behavior that keeps
an unparseable or absent range from silently suppressing anything.
"""

import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime
from unittest.mock import MagicMock, patch

from data_subscriber.cslc import disp_s1_constants as c

CCSLC_DOC_ID_DATE_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")


def parse_ccslc_doc_id_dates(doc_id):
    m = CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None


_mock_cslc_utils = MagicMock()
_mock_cslc_utils.parse_ccslc_doc_id_dates = parse_ccslc_doc_id_dates

# Import the evaluator the same way its sibling test modules do. Whichever of them runs
# first creates the module bound to ITS mock of cslc_utils, and the rest no-op the import
# and share that object -- so nothing here may depend on the mock above being the live
# one. Every stub below goes through patch.object on the module, which is safe either way,
# and this file is named to sort after test_disp_s1_k_cycle_evaluator.py so that module
# keeps ownership of the shared mock its own assertions rely on.
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

DispS1KCycleEvaluator = k_evaluator_mod.DispS1KCycleEvaluator

K = 3
FRAME = 7098
BURSTS = ["b1", "b2"]

# The database lists four full-coverage dates. 20240111 and 20240123 are the partial
# passes it left out; they interleave with the listed ones exactly as they do on a real
# frame whose track alternates full and partial coverage.
LISTED = ["20240105", "20240117", "20240129", "20240210"]
EXCLUDED = ["20240111", "20240123"]
ASSESSED_END = "20241231"
# past the surveyed range: absent, but the survey never looked
UNASSESSED = "20250115"


class FakeFrame:
    def __init__(self, dates):
        self.frame_number = FRAME
        self.burst_ids = set(BURSTS)
        self.sensing_datetimes = [datetime.strptime(d, "%Y%m%d") for d in dates]
        first = self.sensing_datetimes[0] if self.sensing_datetimes else None
        self.sensing_datetime_days_index = [(dt - first).days for dt in self.sensing_datetimes]
        self.sensing_seconds_since_first = [
            int((dt - first).total_seconds()) for dt in self.sensing_datetimes]
        self.processing_modes = None
        self.phases = None
        self.phase_error = None


def make_frame_map(dates=LISTED):
    frame_to_bursts = defaultdict(lambda: None)
    frame_to_bursts[FRAME] = FakeFrame(dates)
    return frame_to_bursts


def make_evaluator(es_conn, dates=LISTED, enabled=True, assessed_end=ASSESSED_END):
    frame_to_bursts = make_frame_map(dates)
    with patch.object(k_evaluator_mod, "localize_disp_frame_burst_hist",
                      return_value=(frame_to_bursts, {b: [FRAME] for b in BURSTS}, {})), \
         patch.object(k_evaluator_mod, "localize_frame_geo_json", return_value={}), \
         patch.object(k_evaluator_mod, "localize_frame_geojson_map", return_value={}):
        evaluator = DispS1KCycleEvaluator(es_conn, k=K, m=2)
    evaluator._compute_bounding_box = MagicMock(return_value=[])
    # patch the two accessors the evaluator imported by name
    patcher_enabled = patch.object(k_evaluator_mod, "burst_db_exclusion_enabled",
                                   return_value=enabled)
    patcher_end = patch.object(k_evaluator_mod, "localize_disp_burst_db_assessed_end",
                               return_value=assessed_end)
    patcher_enabled.start()
    patcher_end.start()
    evaluator._test_patchers = [patcher_enabled, patcher_end]
    return evaluator


def csc_hit(sensing_date, found=BURSTS):
    return {"_source": {"metadata": {
        c.SENSING_DATE: sensing_date,
        c.ACQUISITION_CYCLE: 0,
        c.IS_COMPLETE: len(found) == len(BURSTS),
        c.EXPECTED_BURST_IDS: BURSTS,
        c.FOUND_BURST_IDS: list(found),
        c.CSLC_PRODUCT_PATHS: [f"s3://p_{sensing_date}"],
    }}}


class ExclusionPredicateTest(unittest.TestCase):
    """When does absence from the database mean 'deliberately excluded'?"""

    def setUp(self):
        self.evaluator = make_evaluator(MagicMock())

    def tearDown(self):
        for p in getattr(self.evaluator, "_test_patchers", []):
            p.stop()

    def test_absent_inside_the_assessed_range_is_excluded(self):
        for date in EXCLUDED:
            self.assertTrue(self.evaluator._is_db_excluded(FRAME, date), date)

    def test_listed_date_is_never_excluded(self):
        for date in LISTED:
            self.assertFalse(self.evaluator._is_db_excluded(FRAME, date), date)

    def test_absent_past_the_assessed_range_is_not_excluded(self):
        """The survey never looked, so absence says nothing -- this is forward work."""
        self.assertFalse(self.evaluator._is_db_excluded(FRAME, UNASSESSED))

    def test_date_on_the_assessed_end_is_inside_the_range(self):
        self.assertTrue(self.evaluator._is_db_excluded(FRAME, ASSESSED_END))

    def test_unknown_frame_fails_open(self):
        self.assertFalse(self.evaluator._is_db_excluded(999999, EXCLUDED[0]))

    def test_frame_with_no_sensing_times_fails_open(self):
        ev = make_evaluator(MagicMock(), dates=[])
        try:
            self.assertFalse(ev._is_db_excluded(FRAME, EXCLUDED[0]))
        finally:
            for p in ev._test_patchers:
                p.stop()

    def test_unparseable_assessed_range_fails_open(self):
        ev = make_evaluator(MagicMock(), assessed_end=None)
        try:
            self.assertFalse(ev._is_db_excluded(FRAME, EXCLUDED[0]))
        finally:
            for p in ev._test_patchers:
                p.stop()

    def test_switch_off_disables_the_whole_mechanism(self):
        ev = make_evaluator(MagicMock(), enabled=False)
        try:
            self.assertFalse(ev._is_db_excluded(FRAME, EXCLUDED[0]))
        finally:
            for p in ev._test_patchers:
                p.stop()


class WindowCompositionTest(unittest.TestCase):
    """An excluded partial must not hold the k-window below k."""

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.es_conn = MagicMock()

    def tearDown(self):
        for p in getattr(self, "evaluator", None)._test_patchers if getattr(self, "evaluator", None) else []:
            p.stop()
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _window(self, evaluator, cscs, trigger):
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=cscs):
            evaluator._query_cslc_catalog = MagicMock(return_value={})
            return evaluator._get_window_cscs(FRAME, trigger)

    def test_excluded_partial_does_not_occupy_a_k_slot(self):
        """The k=3 window fills from listed dates instead of stalling at 2/3."""
        self.evaluator = make_evaluator(self.es_conn)
        cscs = [csc_hit(d) for d in LISTED[:3]]
        cscs.append(csc_hit(EXCLUDED[0], found=["b1"]))     # partial, excluded
        window = self._window(self.evaluator, cscs, LISTED[2])
        dates = [w[c.SENSING_DATE] for w in window]
        self.assertEqual(dates, LISTED[:3])
        self.assertNotIn(EXCLUDED[0], dates)

    def test_partial_on_a_listed_date_still_occupies_its_slot(self):
        """Only the database's own exclusions are filtered -- a real hole stays visible."""
        self.evaluator = make_evaluator(self.es_conn)
        cscs = [csc_hit(LISTED[0]), csc_hit(LISTED[1], found=["b1"]), csc_hit(LISTED[2])]
        window = self._window(self.evaluator, cscs, LISTED[2])
        dates = [w[c.SENSING_DATE] for w in window]
        self.assertEqual(dates, LISTED[:3])

    def test_excluded_date_is_not_resurrected_from_the_catalog(self):
        """A date with no CSC at all must still be kept out, or the catalog fallback
        readmits it as complete with whatever bursts happen to be present."""
        self.evaluator = make_evaluator(self.es_conn)
        cscs = [csc_hit(d) for d in LISTED[:3]]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=cscs):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={
                EXCLUDED[0]: {c.SENSING_DATE: EXCLUDED[0], c.IS_COMPLETE: True,
                              c.EXPECTED_BURST_IDS: ["b1"], c.FOUND_BURST_IDS: ["b1"],
                              c.CSLC_PRODUCT_PATHS: []},
            })
            window = self.evaluator._get_window_cscs(FRAME, LISTED[2])
        self.assertNotIn(EXCLUDED[0], [w[c.SENSING_DATE] for w in window])

    def test_switch_off_restores_the_previous_behavior(self):
        self.evaluator = make_evaluator(self.es_conn, enabled=False)
        cscs = [csc_hit(d) for d in LISTED[:3]]
        cscs.append(csc_hit(EXCLUDED[0], found=["b1"]))
        window = self._window(self.evaluator, cscs, LISTED[2])
        self.assertIn(EXCLUDED[0], [w[c.SENSING_DATE] for w in window])


class LineageGapTest(unittest.TestCase):
    """An excluded partial must not read as an unresolved gap in the lineage."""

    def setUp(self):
        self.es_conn = MagicMock()

    def tearDown(self):
        for p in getattr(self, "evaluator", None)._test_patchers if getattr(self, "evaluator", None) else []:
            p.stop()

    def _gap(self, evaluator, partial_dates, trigger):
        hits = [{"_source": {"metadata": {
            c.SENSING_DATE: d,
            c.EXPECTED_BURST_IDS: BURSTS,
            c.FOUND_BURST_IDS: ["b1"],
        }}} for d in partial_dates]
        evaluator._get_lineage_lower_bound = MagicMock(return_value="")
        evaluator._lineage_start_date = MagicMock(return_value="")
        with patch.object(k_evaluator_mod, "backoff_wrapper", return_value=hits):
            return evaluator._check_lineage_gap_unresolved(FRAME, trigger)

    def test_excluded_partial_is_not_a_gap(self):
        self.evaluator = make_evaluator(self.es_conn)
        gap, detail = self._gap(self.evaluator, EXCLUDED, LISTED[-1])
        self.assertFalse(gap, detail)

    def test_partial_on_a_listed_date_is_still_a_gap(self):
        self.evaluator = make_evaluator(self.es_conn)
        gap, detail = self._gap(self.evaluator, [LISTED[1]], LISTED[-1])
        self.assertTrue(gap)
        self.assertIn(LISTED[1], detail)

    def test_partial_past_the_assessed_range_is_still_a_gap(self):
        """Unassessed dates keep their old meaning -- the fix never silences new data."""
        self.evaluator = make_evaluator(self.es_conn)
        gap, detail = self._gap(self.evaluator, [UNASSESSED], UNASSESSED)
        self.assertTrue(gap)
        self.assertIn(UNASSESSED, detail)

    def test_mixed_partials_report_only_the_real_gap(self):
        self.evaluator = make_evaluator(self.es_conn)
        gap, detail = self._gap(self.evaluator, EXCLUDED + [LISTED[1]], LISTED[-1])
        self.assertTrue(gap)
        self.assertIn(LISTED[1], detail)
        for date in EXCLUDED:
            self.assertNotIn(date, detail)
        self.assertIn("ignored 2 db-excluded", detail)

    def test_switch_off_restores_the_previous_behavior(self):
        self.evaluator = make_evaluator(self.es_conn, enabled=False)
        gap, _ = self._gap(self.evaluator, EXCLUDED, LISTED[-1])
        self.assertTrue(gap)


class PreExistingStateConfigTest(unittest.TestCase):
    """Exclusion is computed from the deployed database, never read off the document.

    Cycle state configs written before this shipped carry no db_excluded field, and one
    written under an older database may carry a stale value. Neither may decide anything.
    """

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.es_conn = MagicMock()

    def tearDown(self):
        for p in getattr(self, "evaluator", None)._test_patchers if getattr(self, "evaluator", None) else []:
            p.stop()
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_unstamped_state_config_is_still_excluded(self):
        self.evaluator = make_evaluator(self.es_conn)
        cscs = [csc_hit(d) for d in LISTED[:3]]
        cscs.append(csc_hit(EXCLUDED[0], found=["b1"]))     # no db_excluded key at all
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=cscs):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            window = self.evaluator._get_window_cscs(FRAME, LISTED[2])
        self.assertNotIn(EXCLUDED[0], [w[c.SENSING_DATE] for w in window])

    def test_stale_stamp_does_not_suppress_a_listed_date(self):
        """A newer database that lists the date wins over an older stamp."""
        self.evaluator = make_evaluator(self.es_conn)
        stamped = csc_hit(LISTED[1])
        stamped["_source"]["metadata"][c.DB_EXCLUDED] = True
        cscs = [csc_hit(LISTED[0]), stamped, csc_hit(LISTED[2])]
        with patch.object(k_evaluator_mod, "query_cscs_for_frame", return_value=cscs):
            self.evaluator._query_cslc_catalog = MagicMock(return_value={})
            window = self.evaluator._get_window_cscs(FRAME, LISTED[2])
        self.assertIn(LISTED[1], [w[c.SENSING_DATE] for w in window])


if __name__ == "__main__":
    unittest.main()
