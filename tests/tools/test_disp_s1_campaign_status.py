#!/usr/bin/env python3
"""Unit tests for disp_s1_campaign_status.py.

The premise under test: the processing-mode burst database is the source of truth for
what a campaign owes, and product existence plus job status decide what state each of
those owed units is actually in. These tests drive the pure reconciliation functions
with a hand-built frame so the failure paths -- which a healthy cluster never
exercises -- are covered.
"""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

tools_dir = Path(__file__).parent.parent.parent / "tools"
sys.path.insert(0, str(tools_dir))

from disp_s1_campaign_status import (  # noqa: E402
    DONE, FAILED, PENDING, RUNNING, SKIPPED,
    JOB_ACQ_RE, JOB_INGEST_RE, JOB_QUERY_RE, JOB_RANGE_RE,
    attribute, blocking_failure, expected_units, job_state, per_date,
)
from data_subscriber.cslc.disp_s1_phases import segment_phases  # noqa: E402

K = 15


class Frame(object):
    """Just enough of a burst-database frame for the reconciliation functions."""

    def __init__(self, labels, start="2016-07-09"):
        base = datetime.strptime(start, "%Y-%m-%d")
        self.sensing_datetimes = [base + timedelta(days=12 * i) for i in range(len(labels))]
        self.burst_ids = ["t093_197801_iw%d" % i for i in range(1, 4)]
        self.phases = segment_phases(labels, K)


def labels(*spec):
    out = []
    for label, n in spec:
        out.extend([label] * n)
    return out


def build(spec, start="2016-07-09"):
    frame = Frame(labels(*spec), start)
    ymd = [d.strftime("%Y%m%d") for d in frame.sensing_datetimes]
    acq_of = list(range(len(ymd)))          # 1:1 stand-in for the day index
    units = expected_units(frame, frame.phases, K, ymd, acq_of)
    return frame, ymd, acq_of, units


def job(name, status, jtype="cslc_query_hist", job_id="job-x"):
    """Mimic what campaign_jobs() produces after parsing a real job name."""
    import re
    j = {"type": jtype, "status": status, "name": name, "id": job_id, "when": ""}
    m = JOB_QUERY_RE.search(name)
    if m:
        j["frame"] = int(m.group(1))
        j["span"] = (m.group(2).replace("-", ""), m.group(3).replace("-", ""))
        return j
    m = JOB_INGEST_RE.search(name)
    if m:
        j["frame"] = int(m.group(1)); j["date"] = m.group(2); return j
    m = JOB_ACQ_RE.search(name)
    if m:
        j["frame"] = int(m.group(1)); j["acq"] = int(m.group(2)); return j
    m = JOB_RANGE_RE.search(name)
    if m:
        j["frame"] = int(m.group(1))
        j["acq_range"] = (int(m.group(2)), int(m.group(3)))
        return j
    raise AssertionError("job name did not parse: %s" % name)


class TestExpectationFromBurstDatabase(unittest.TestCase):
    """The database alone decides what a campaign owes, before any cluster is queried."""

    def test_historical_phase_owes_one_unit_per_k_set(self):
        _, _, _, units = build([("historical_01", 45)])
        self.assertEqual([u["name"] for u in units], ["k-set 1", "k-set 2", "k-set 3"])
        self.assertTrue(all(len(u["positions"]) == K for u in units))

    def test_first_date_of_a_historical_phase_yields_no_product(self):
        _, _, _, units = build([("historical_01", 30)])
        self.assertEqual(len(units[0]["product_positions"]), K - 1)
        self.assertEqual(len(units[1]["product_positions"]), K)
        self.assertEqual(sum(u["want"] if "want" in u else len(u["product_positions"])
                             for u in units), 29)

    def test_forward_phase_owes_one_product_per_date(self):
        _, _, _, units = build([("historical_01", 15), ("forward_01", 4)])
        fwd = [u for u in units if u["kind"] == "forward"]
        self.assertEqual(len(fwd), 4)
        self.assertTrue(all(len(u["product_positions"]) == 1 for u in fwd))

    def test_no_run_owes_nothing(self):
        _, _, _, units = build([("historical_01", 15), ("no_run", 9)])
        nr = [u for u in units if u["kind"] == "no_run"][0]
        self.assertEqual(nr["product_positions"], [])
        self.assertEqual(len(nr["positions"]), 9)

    def test_post_gap_phase_boundaries_are_phase_relative(self):
        _, ymd, _, units = build([("historical_01", 30), ("forward_01", 8),
                                  ("historical_02", 15)])
        h2 = [u for u in units if u["phase"] == "historical_02"]
        self.assertEqual(len(h2), 1)
        # boundary is the last date of the phase, not of the absolute k-grid
        self.assertEqual(h2[0]["boundary"], ymd[52])


class TestJobNameAttribution(unittest.TestCase):
    """Every DISP-S1 job name has to land on exactly one unit."""

    def setUp(self):
        self.frame, self.ymd, self.acq, self.units = build(
            [("historical_01", 30), ("forward_01", 3)])

    def test_query_hist_lands_on_its_k_set(self):
        name = ("data-subscriber-query-timer-Region phased_f24726-%s-%sT01__02__36-"
                "20260815T003531.505926Z"
                % (fmt(self.ymd[15]) + "T01__33__16", fmt(self.ymd[29])))
        orphans = attribute(self.units, [job(name, "job-completed")], self.ymd)
        self.assertEqual(orphans, [])
        self.assertEqual(len(self.units[1]["jobs"]), 1)
        self.assertEqual(self.units[0]["jobs"], [])

    def test_sciflo_hist_lands_via_its_acquisition_index(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-20260815T00Z"
        orphans = attribute(self.units, [job(name, "job-started",
                                             "SCIFLO_L3_DISP_S1_hist")], self.ymd)
        self.assertEqual(orphans, [])
        self.assertEqual(len(self.units[1]["jobs"]), 1)

    def test_catalog_ingest_lands_on_its_forward_date(self):
        name = "cslc_catalog_ingest-Region phased_f24726-%s-20260814T171224Z" % self.ymd[31]
        orphans = attribute(self.units, [job(name, "job-completed",
                                             "cslc_catalog_ingest")], self.ymd)
        self.assertEqual(orphans, [])
        fwd = [u for u in self.units if u["name"] == self.ymd[31]][0]
        self.assertEqual(len(fwd["jobs"]), 1)

    def test_download_range_lands_on_the_unit_holding_its_last_index(self):
        name = "job-WF-cslc_download-frame-24726-acq_indices-15-to-29-20260815T00Z"
        orphans = attribute(self.units, [job(name, "job-completed", "cslc_download")],
                            self.ymd)
        self.assertEqual(orphans, [])
        self.assertEqual(len(self.units[1]["jobs"]), 1)

    def test_a_job_for_a_date_this_frame_does_not_have_is_reported_not_dropped(self):
        name = "cslc_catalog_ingest-Region phased_f24726-19990101-20260814T171224Z"
        orphans = attribute(self.units, [job(name, "job-failed", "cslc_catalog_ingest")],
                            self.ymd)
        self.assertEqual(len(orphans), 1)


def fmt(ymd_str):
    return "%s-%s-%s" % (ymd_str[:4], ymd_str[4:6], ymd_str[6:])


class TestStateReconciliation(unittest.TestCase):
    """Products decide 'done'. Job status explains everything products cannot."""

    def setUp(self):
        from disp_s1_campaign_status import settle
        self.settle = settle
        self.frame, self.ymd, self.acq, self.units = build(
            [("historical_01", 30), ("forward_01", 2)])

    def all_products_for(self, unit):
        return {self.ymd[i] for i in unit["product_positions"]}

    def test_k_set_needs_products_AND_its_boundary_to_be_done(self):
        attribute(self.units, [], self.ymd)
        u = self.units[0]
        # products complete but the compressed CSLC has not published
        self.settle(self.units, self.all_products_for(u), set(), self.ymd)
        self.assertNotEqual(u["status"], DONE)
        self.settle(self.units, self.all_products_for(u), {u["boundary"]}, self.ymd)
        self.assertEqual(u["status"], DONE)

    def test_a_failed_job_makes_the_unit_failed_not_pending(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-x"
        attribute(self.units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], FAILED)
        self.assertEqual(self.units[2]["status"], PENDING)

    def test_a_completed_job_with_no_products_is_a_failure_not_a_success(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-x"
        attribute(self.units, [job(name, "job-completed", "SCIFLO_L3_DISP_S1_hist")],
                  self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], FAILED)

    def test_products_win_over_a_stale_failed_job(self):
        u = self.units[0]
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        attribute(self.units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], self.ymd)
        self.settle(self.units, self.all_products_for(u), {u["boundary"]}, self.ymd)
        self.assertEqual(u["status"], DONE, "a retry that succeeded must clear the failure")

    def test_no_run_is_never_failed_or_pending(self):
        _, ymd, _, units = build([("historical_01", 15), ("no_run", 9)])
        attribute(units, [], ymd)
        from disp_s1_campaign_status import settle
        settle(units, set(), set(), ymd)
        self.assertEqual(units[-1]["status"], SKIPPED)

    def test_running_job_is_running(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-x"
        attribute(self.units, [job(name, "job-started", "SCIFLO_L3_DISP_S1_hist")], self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], RUNNING)

    def test_job_state_mapping(self):
        self.assertEqual(job_state("job-failed"), FAILED)
        self.assertEqual(job_state("job-offline"), FAILED)
        self.assertEqual(job_state("job-revoked"), FAILED)
        self.assertEqual(job_state("job-completed"), DONE)
        self.assertEqual(job_state("job-started"), RUNNING)


class TestStuckDetection(unittest.TestCase):
    """A frame is stuck when its EARLIEST outstanding unit failed -- everything after
    it gates on a compressed CSLC that will never publish."""

    def setUp(self):
        from disp_s1_campaign_status import settle
        self.settle = settle

    def test_failure_on_the_earliest_outstanding_unit_is_stuck(self):
        _, ymd, _, units = build([("historical_01", 45)])
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        attribute(units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], ymd)
        self.settle(units, set(), set(), ymd)
        self.assertIsNotNone(blocking_failure(units))
        self.assertEqual(blocking_failure(units)["name"], "k-set 1")

    def test_a_later_failure_behind_a_running_unit_is_not_yet_blocking(self):
        _, ymd, _, units = build([("historical_01", 45)])
        running = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        failed = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-44_hist-y"
        attribute(units, [job(running, "job-started", "SCIFLO_L3_DISP_S1_hist"),
                          job(failed, "job-failed", "SCIFLO_L3_DISP_S1_hist")], ymd)
        self.settle(units, set(), set(), ymd)
        self.assertIsNone(blocking_failure(units))

    def test_completed_frame_is_not_stuck(self):
        _, ymd, _, units = build([("historical_01", 30)])
        attribute(units, [], ymd)
        products = {ymd[i] for u in units for i in u["product_positions"]}
        self.settle(units, products, {u["boundary"] for u in units}, ymd)
        self.assertIsNone(blocking_failure(units))
        self.assertTrue(all(u["status"] == DONE for u in units))


class TestPerDateView(unittest.TestCase):
    """What the timeline plot consumes: one entry per sensing date."""

    def setUp(self):
        from disp_s1_campaign_status import settle
        self.settle = settle

    def test_every_sensing_date_appears_exactly_once_in_order(self):
        frame, ymd, _, units = build([("historical_01", 30), ("forward_01", 3),
                                      ("historical_02", 15), ("no_run", 4)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertEqual([e["date"] for e in entries], ymd)
        self.assertEqual([e["position"] for e in entries], list(range(len(ymd))))

    def test_every_historical_phase_starts_a_lineage(self):
        """A lineage BEGINS at the first date of every historical phase, including the
        first -- that is the date lineage_start_pos() returns and the one date the phase
        yields no product for. Only the post-gap ones additionally RESET a lineage."""
        frame, ymd, _, units = build([("historical_01", 30), ("forward_01", 3),
                                      ("historical_02", 15)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertEqual([e["position"] for e in entries if e["lineage_start"]], [0, 33])
        self.assertEqual([e["position"] for e in entries if e["lineage_reset"]], [33])

    def test_leading_no_run_still_marks_its_first_historical_phase(self):
        """is_new_lineage is False for the first PROCESSED phase even when it is labelled
        historical_02, so keying the marker off it would draw nothing at all here."""
        frame, ymd, _, units = build([("no_run", 9), ("historical_02", 15)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertEqual([e["position"] for e in entries if e["lineage_start"]], [9])
        self.assertEqual([e["position"] for e in entries if e["lineage_reset"]], [])

    def test_boundary_flagged_and_published_state_tracked(self):
        frame, ymd, _, units = build([("historical_01", 30)])
        attribute(units, [], ymd)
        self.settle(units, set(), {ymd[14]}, ymd)
        entries = per_date(units, set(), {ymd[14]}, ymd, frame.phases)
        bounds = [(e["position"], e["boundary_published"]) for e in entries if e["boundary"]]
        self.assertEqual(bounds, [(14, True), (29, False)])

    def test_a_failed_unit_paints_every_one_of_its_dates_failed(self):
        frame, ymd, _, units = build([("historical_01", 30)])
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        attribute(units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertTrue(all(e["status"] == FAILED for e in entries[:15]))
        self.assertTrue(all(e["status"] == PENDING for e in entries[15:]))

    def test_a_published_date_shows_done_even_inside_a_failed_unit(self):
        """Partial output is the normal shape of a failed ministack -- show it."""
        frame, ymd, _, units = build([("historical_01", 30)])
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        attribute(units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], ymd)
        published = {ymd[3], ymd[4]}
        self.settle(units, published, set(), ymd)
        entries = per_date(units, published, set(), ymd, frame.phases)
        self.assertEqual(entries[3]["status"], DONE)
        self.assertEqual(entries[4]["status"], DONE)
        self.assertEqual(entries[5]["status"], FAILED)

    def test_no_run_dates_are_skipped_not_pending(self):
        frame, ymd, _, units = build([("historical_01", 15), ("no_run", 9)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertTrue(all(e["status"] == SKIPPED for e in entries[15:]))


if __name__ == "__main__":
    unittest.main()
