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

# tests/unit/tools/<this file> -> parents[3] is the repository root
tools_dir = Path(__file__).parents[3] / "tools"
sys.path.insert(0, str(tools_dir))

from disp_s1_campaign_status import (  # noqa: E402
    DONE, FAILED, MISSING, PENDING, RUNNING, SKIPPED,
    JOB_ACQ_RE, JOB_FRAME_DATE_RE, JOB_QUERY_RE, JOB_RANGE_RE,
    attribute, blocking_failure, expected_units, job_state, per_date,
    select_campaigns,
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


def job(name, status, jtype="cslc_query_hist", job_id="job-x", queued=""):
    """Mimic what campaign_jobs() produces after parsing a real job name."""
    import re
    j = {"type": jtype, "status": status, "name": name, "id": job_id, "when": "", "queued": queued}
    m = JOB_QUERY_RE.search(name)
    if m:
        j["frame"] = int(m.group(1))
        j["span"] = (m.group(2).replace("-", ""), m.group(3).replace("-", ""))
        return j
    m = JOB_FRAME_DATE_RE.search(name)
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

    def test_forward_sciflo_lands_on_its_forward_date(self):
        """Its name is nothing like the historical SCIFLO's: no -frame- and no
        latest_acq_index, just f<frame>-<date>-state-config. Missing it made the live
        campaign report a running forward date as FAILED."""
        name = ("SCIFLO_L3_DISP_S1__OPERA-2561-disp_s1-kcycle-k15-m6-f33065-%s"
                "-state-config-20260815T163629.150Z" % self.ymd[31])
        orphans = attribute(self.units, [job(name, "job-started", "SCIFLO_L3_DISP_S1")],
                            self.ymd)
        self.assertEqual(orphans, [])
        fwd = [u for u in self.units if u["name"] == self.ymd[31]][0]
        self.assertEqual(len(fwd["jobs"]), 1)

    def test_query_hist_name_is_not_mistaken_for_a_frame_date_name(self):
        """Both carry _f<frame>-, but query_hist dates are hyphenated so \\d{8} cannot
        match them. Order of the checks must not be load-bearing."""
        name = ("data-subscriber-query-timer-Region_f33065-2016-07-09T01__33__16-"
                "2017-04-29T02__02__36-20260815T003531.505926Z")
        self.assertIsNone(JOB_FRAME_DATE_RE.search(name))

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

    def test_a_completed_PRODUCING_job_with_no_products_is_a_failure(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-x"
        attribute(self.units, [job(name, "job-completed", "SCIFLO_L3_DISP_S1_hist")],
                  self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], FAILED)

    def test_a_completed_UPSTREAM_job_with_no_products_is_not_a_failure(self):
        """A finished query or ingest only means the producer has not run yet. Calling
        that a failure cried wolf on a healthy live campaign."""
        name = ("data-subscriber-query-timer-Region_f24726-%s-%sT01__02__36-x"
                % (fmt(self.ymd[15]) + "T01__33__16", fmt(self.ymd[29])))
        attribute(self.units, [job(name, "job-completed", "cslc_query_hist")], self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], RUNNING)

    def test_a_revoked_job_that_was_submitted_again_does_not_fail_the_unit(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-%s"
        old = job(name % "a", "job-revoked", "SCIFLO_L3_DISP_S1_hist", "a", queued="2026-11-19T05:52:29Z")
        new = job(name % "b", "job-queued", "SCIFLO_L3_DISP_S1_hist", "b", queued="2026-11-19T12:54:17Z")
        attribute(self.units, [old, new], self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], RUNNING)

    def test_a_resubmit_that_failed_too_still_fails_the_unit(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-%s"
        old = job(name % "a", "job-revoked", "SCIFLO_L3_DISP_S1_hist", "a", queued="2026-11-19T05:52:29Z")
        new = job(name % "b", "job-failed", "SCIFLO_L3_DISP_S1_hist", "b", queued="2026-11-19T12:54:17Z")
        attribute(self.units, [old, new], self.ymd)
        self.settle(self.units, set(), set(), self.ymd)
        self.assertEqual(self.units[1]["status"], FAILED)

    def test_a_failure_after_an_earlier_run_of_the_same_type_fails_the_unit(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-%s"
        old = job(name % "a", "job-completed", "SCIFLO_L3_DISP_S1_hist", "a", queued="2026-11-19T05:52:29Z")
        new = job(name % "b", "job-failed", "SCIFLO_L3_DISP_S1_hist", "b", queued="2026-11-19T12:54:17Z")
        attribute(self.units, [old, new], self.ymd)
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

    def test_a_failed_FORWARD_date_is_a_failure_but_not_blocking(self):
        """Forward dates do not gate each other -- the walk advances on the state
        config's disposition -- so a failed one leaves a hole and the frame carries on."""
        _, ymd, _, units = build([("historical_01", 15), ("forward_01", 4)])
        name = ("SCIFLO_L3_DISP_S1__OPERA-2561-disp_s1-kcycle-k15-m6-f33065-%s"
                "-state-config-x" % ymd[15])
        attribute(units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1")], ymd)
        self.settle(units, set(), set(), ymd)
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertEqual(forward[0]["status"], FAILED)
        self.assertIsNone(blocking_failure(units), "a forward hole must not read as STUCK")

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

    def test_a_unit_the_cursor_passed_with_no_product_is_MISSING_not_pending(self):
        """A forward date whose state config returns a no-fire disposition is terminal:
        the walk advances over it without submitting anything and never comes back. It
        reported as pending forever, which is how a frame reached a 100% cursor,
        self-disabled, and still owed 25 products with nothing flagged."""
        frame, ymd, _, units = build([("historical_01", 15), ("forward_01", 4)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=len(ymd))
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertTrue(all(u["status"] == MISSING for u in forward))

    def test_a_unit_the_cursor_has_not_reached_is_still_pending(self):
        frame, ymd, _, units = build([("historical_01", 15), ("forward_01", 4)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=15)
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertTrue(all(u["status"] == PENDING for u in forward))

    def test_a_running_job_beats_the_cursor_having_passed(self):
        frame, ymd, _, units = build([("historical_01", 15), ("forward_01", 2)])
        name = ("SCIFLO_L3_DISP_S1__OPERA-2561-disp_s1-kcycle-k15-m6-f24726-%s"
                "-state-config-x" % ymd[15])
        attribute(units, [job(name, "job-started", "SCIFLO_L3_DISP_S1")], ymd)
        self.settle(units, set(), set(), ymd, cursor=len(ymd))
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertEqual(forward[0]["status"], RUNNING)
        self.assertEqual(forward[1]["status"], MISSING)

    def test_no_run_dates_are_skipped_not_pending(self):
        frame, ymd, _, units = build([("historical_01", 15), ("no_run", 9)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd)
        entries = per_date(units, set(), set(), ymd, frame.phases)
        self.assertTrue(all(e["status"] == SKIPPED for e in entries[15:]))


if __name__ == "__main__":
    unittest.main()


class TestCampaignSelection(unittest.TestCase):
    """A campaign self-disables when it completes.

    Selecting campaigns on the batch proc's `enabled` flag therefore blinds the report at
    exactly the moment the work is finished -- the point at which an operator is checking
    what was produced and whether anything was left owed. Observed in production: a
    completed regional campaign made the tool print "no matching batch proc" and exit.
    """

    RUNNING_PROC = ("p-running", {"enabled": True, "frame_states": {"24726": 40},
                                  "last_run_date": "2026-08-16T00:00:00"})
    DONE_PROC = ("p-done", {"enabled": False, "frame_states": {"24726": 115},
                            "last_run_date": "2026-08-17T15:25:44"})
    NEVER_RAN = ("p-empty", {"enabled": True, "frame_states": {}})

    def test_completed_campaign_is_still_reported(self):
        got = select_campaigns([self.DONE_PROC])
        self.assertEqual([i for i, _ in got], ["p-done"])

    def test_enabled_and_disabled_campaigns_both_selected(self):
        got = select_campaigns([self.RUNNING_PROC, self.DONE_PROC])
        self.assertEqual(sorted(i for i, _ in got), ["p-done", "p-running"])

    def test_most_recently_active_campaign_comes_first(self):
        got = select_campaigns([self.RUNNING_PROC, self.DONE_PROC])
        self.assertEqual(got[0][0], "p-done")

    def test_proc_that_was_never_given_frames_is_not_a_campaign(self):
        got = select_campaigns([self.NEVER_RAN, self.DONE_PROC])
        self.assertEqual([i for i, _ in got], ["p-done"])

    def test_explicit_id_wins_even_without_frame_states(self):
        """--id is an operator asking for one specific proc; never second-guess it."""
        got = select_campaigns([self.NEVER_RAN, self.DONE_PROC], proc_id="p-empty")
        self.assertEqual([i for i, _ in got], ["p-empty"])

    def test_missing_last_run_date_does_not_raise(self):
        undated = ("p-undated", {"enabled": False, "frame_states": {"1": 1}})
        got = select_campaigns([undated, self.DONE_PROC])
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0][0], "p-done")

    def test_no_campaigns_yields_empty_not_error(self):
        self.assertEqual(select_campaigns([]), [])


class TestStartingCursor(unittest.TestCase):
    """A catch-up round starts each frame part-way through, at the cursor its batch file
    gives. Nothing before that cursor is owed by the round. Counting from the frame's
    first date reported every earlier ministack as MISSING ("walk passed it, nothing ran,
    nothing will retry") on a healthy run that had only just started."""

    def setUp(self):
        from disp_s1_campaign_status import BEFORE, settle
        self.BEFORE, self.settle = BEFORE, settle

    def test_units_wholly_before_the_start_are_before_start_and_owe_nothing(self):
        _, ymd, _, units = build([("historical_01", 45), ("forward_01", 3)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=30, start=30)
        self.assertEqual([u["status"] for u in units[:2]], [self.BEFORE, self.BEFORE])
        self.assertEqual([u["want"] for u in units[:2]], [0, 0])
        self.assertEqual(units[2]["status"], PENDING)
        self.assertEqual(units[2]["want"], K)

    def test_a_unit_before_the_start_is_never_missing_however_far_the_walk_went(self):
        _, ymd, _, units = build([("historical_01", 30), ("forward_01", 4)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=len(ymd), start=30)
        self.assertEqual([u["status"] for u in units[:2]], [self.BEFORE, self.BEFORE])
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertTrue(all(u["status"] == MISSING for u in forward),
                        "owed dates the walk passed are still MISSING")

    def test_a_start_on_a_forward_date_owes_that_date_and_later_only(self):
        _, ymd, _, units = build([("historical_01", 15), ("forward_01", 4)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=17, start=17)
        forward = [u for u in units if u["kind"] == "forward"]
        self.assertEqual([u["status"] for u in forward],
                         [self.BEFORE, self.BEFORE, PENDING, PENDING])

    def test_no_start_keeps_the_old_behaviour(self):
        _, ymd, _, units = build([("historical_01", 30)])
        attribute(units, [], ymd)
        self.settle(units, set(), set(), ymd, cursor=30, start=None)
        self.assertTrue(all(u["status"] == MISSING for u in units))

    def test_before_start_dates_are_drawn_as_such_and_expect_no_product(self):
        frame, ymd, _, units = build([("historical_01", 30), ("forward_01", 2)])
        attribute(units, [], ymd)
        products = {ymd[3]}                       # an earlier round's product
        self.settle(units, products, set(), ymd, cursor=30, start=30)
        entries = per_date(units, products, set(), ymd, frame.phases)
        self.assertTrue(all(e["status"] == self.BEFORE for e in entries[:30]))
        self.assertFalse(any(e["expects_product"] for e in entries[:30]))
        self.assertTrue(all(e["status"] == PENDING for e in entries[30:]))

    def test_an_earlier_failure_before_the_start_does_not_make_the_frame_stuck(self):
        _, ymd, _, units = build([("historical_01", 45)])
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-14_hist-x"
        attribute(units, [job(name, "job-failed", "SCIFLO_L3_DISP_S1_hist")], ymd)
        self.settle(units, set(), set(), ymd, cursor=15, start=15)
        self.assertEqual(units[0]["status"], self.BEFORE)
        self.assertIsNone(blocking_failure(units))


class TestStartStateFiles(unittest.TestCase):
    """The starting cursors come from the batch files, matched to batch procs by label."""

    def write(self, tmp, name, spec):
        import json
        import os
        path = os.path.join(tmp, name)
        with open(path, "w") as f:
            json.dump(spec, f)
        return path

    def test_files_map_label_to_starting_cursors(self):
        import tempfile
        from disp_s1_campaign_status import load_start_states
        with tempfile.TemporaryDirectory() as tmp:
            a = self.write(tmp, "p0.json", {"label": "Prior0", "frame_states": {"9154": 330}})
            b = self.write(tmp, "p3b.json", {"label": "Prior3B", "frame_states": {"10599": 60}})
            got = load_start_states([a, b])
        self.assertEqual(got["Prior0"][1], {9154: 330})
        self.assertEqual(got["Prior3B"][1], {10599: 60})
        self.assertTrue(got["Prior0"][0].endswith("p0.json"))

    def test_a_file_without_label_or_frame_states_is_ignored(self):
        import tempfile
        from disp_s1_campaign_status import load_start_states
        with tempfile.TemporaryDirectory() as tmp:
            a = self.write(tmp, "unphased.json", {"label": "Old", "frames": [1, 2]})
            b = self.write(tmp, "nolabel.json", {"frame_states": {"1": 15}})
            self.assertEqual(load_start_states([a, b]), {})

    def test_no_files_is_no_starts(self):
        from disp_s1_campaign_status import load_start_states
        self.assertEqual(load_start_states([]), {})


class TestCompressedCslcGenerations(unittest.TestCase):
    """A ministack run twice leaves two compressed CSLCs per burst and boundary. Counting
    documents reported more compressed CSLCs than the campaign could ever owe."""

    def test_burst_and_boundary_parse_from_a_real_id(self):
        from disp_s1_campaign_status import CCSLC_BURST_RE, CCSLC_RE
        pid = ("OPERA_L2_COMPRESSED-CSLC-S1_F10601_T040-084801-IW1_20210627T000000Z_"
               "20210110T000000Z_20210627T000000Z_20250808T093249Z_VV_v1.0")
        self.assertEqual(CCSLC_BURST_RE.search(pid).group(1), "T040-084801-IW1")
        self.assertEqual(CCSLC_RE.search(pid).group(3), "20210627")


class TestPagedJobRead(unittest.TestCase):
    """Job records are read with a scroll. A single 10,000-hit page silently dropped the
    rest on a long campaign."""

    def test_every_page_is_returned(self):
        import disp_s1_campaign_status as cs
        from unittest import mock
        pages = [
            {"_scroll_id": "s1", "hits": {"hits": [{"_id": "a"}, {"_id": "b"}]}},
            {"_scroll_id": "s2", "hits": {"hits": [{"_id": "c"}]}},      # a short page ends the scroll
        ]
        with mock.patch.object(cs, "es_post", side_effect=pages) as post, \
                mock.patch.object(cs.subprocess, "run") as run:
            hits = cs.es_scroll("/job_status-*/_search", {"query": {"match_all": {}}}, page=2)
        self.assertEqual([h["_id"] for h in hits], ["a", "b", "c"])
        self.assertIn("scroll=1m&size=2", post.call_args_list[0][0][0])
        self.assertEqual(post.call_args_list[0][0][1]["sort"], ["_doc"])       # index order: no scoring
        run.assert_called_once()                      # the scroll is cleared afterwards


class TestQueryWindowAcrossMidnight(unittest.TestCase):
    """The query job is named by its window, which starts 30 minutes before the k-set's first acquisition.
    For a frame imaged at 00:10 UTC that is 23:40 the day before, so the name carries a date the frame
    does not have. The job went unattributed and a k-set whose query had just been submitted was MISSING."""

    def setUp(self):
        from disp_s1_campaign_status import settle
        self.settle = settle
        self.frame, self.ymd, _, self.units = build([("historical_01", 30)])

    def day(self, i, shift):
        d = datetime.strptime(self.ymd[i], "%Y%m%d") + timedelta(days=shift)
        return d.strftime("%Y-%m-%d")

    def test_a_window_starting_the_day_before_lands_on_its_k_set(self):
        name = ("data-subscriber-query-timer-P1_f16668-%sT23__40__24-%sT00__40__10-x"
                % (self.day(15, -1), fmt(self.ymd[29])))
        orphans = attribute(self.units, [job(name, "job-queued")], self.ymd)
        self.assertEqual(orphans, [])
        self.assertEqual(len(self.units[1]["jobs"]), 1)
        self.settle(self.units, set(), set(), self.ymd, cursor=30, start=15)
        self.assertEqual(self.units[1]["status"], RUNNING)

    def test_a_window_ending_the_day_after_lands_on_its_k_set(self):
        name = ("data-subscriber-query-timer-P1_f16668-%sT23__20__00-%sT00__20__00-x"
                % (self.day(15, -1), self.day(29, 1)))
        orphans = attribute(self.units, [job(name, "job-queued")], self.ymd)
        self.assertEqual(orphans, [])
        self.assertEqual(len(self.units[1]["jobs"]), 1)

    def test_the_first_k_set_is_not_mistaken_for_the_second(self):
        name = ("data-subscriber-query-timer-P1_f16668-%sT23__40__24-%sT00__40__10-x"
                % (self.day(0, -1), fmt(self.ymd[14])))
        attribute(self.units, [job(name, "job-queued")], self.ymd)
        self.assertEqual(len(self.units[0]["jobs"]), 1)
        self.assertEqual(self.units[1]["jobs"], [])


class TestJustSubmittedUnit(unittest.TestCase):
    """The cursor moves as the walk submits a unit's first job; the job's status record is indexed a few
    seconds later. A report built in that window saw the unit as MISSING."""

    def setUp(self):
        from disp_s1_campaign_status import just_submitted, settle
        self.just_submitted, self.settle = just_submitted, settle

    def row(self, cursor, jobs=()):
        frame, ymd, _, units = build([("historical_01", 45)])
        attribute(units, list(jobs), ymd)
        self.settle(units, set(), set(), ymd, cursor=cursor, start=15)
        return {"cursor": cursor, "phases": [{"units": units}]}

    def test_the_unit_right_behind_the_cursor_with_no_job_is_a_suspect(self):
        self.assertTrue(self.just_submitted(self.row(30)))

    def test_a_unit_further_back_is_not(self):
        row = self.row(45, [job("job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-44_hist-x",
                                "job-started", "SCIFLO_L3_DISP_S1_hist")])
        self.assertEqual(row["phases"][0]["units"][1]["status"], MISSING)
        self.assertFalse(self.just_submitted(row))

    def test_a_forward_date_whose_ingest_finished_but_whose_sciflo_is_not_queued_yet_is_a_suspect(self):
        frame, ymd, _, units = build([("historical_01", 15), ("forward_01", 3)])
        name = "cslc_catalog_ingest-P1_f30455-%s-20261118T233211Z" % ymd[15]
        attribute(units, [job(name, "job-completed", "cslc_catalog_ingest")], ymd)
        self.settle(units, set(), set(), ymd, cursor=16, start=15)
        self.assertEqual(units[1]["status"], MISSING)
        self.assertTrue(self.just_submitted({"cursor": 16, "phases": [{"units": units}]}))

    def test_a_unit_with_a_producing_job_is_not(self):
        name = "job-WF-SCIFLO_L3_DISP_S1-frame-24726-latest_acq_index-29_hist-x"
        self.assertFalse(self.just_submitted(self.row(30, [job(name, "job-started",
                                                                "SCIFLO_L3_DISP_S1_hist")])))


class TestProductsReadForManyFrames(unittest.TestCase):
    """Products are read with one scroll per dataset for all of a campaign's frames."""

    def test_hits_are_sorted_to_their_frames(self):
        import disp_s1_campaign_status as cs
        from unittest import mock

        def hit(pid, frame):
            return {"_source": {"id": pid, "metadata": {"frame_id": frame}}}

        l3 = [hit("OPERA_L3_DISP-S1_IW_F10599_VV_20210603T105752Z_20210615T105753Z_v1.0_20261118T211033Z", 10599),
              hit("OPERA_L3_DISP-S1_IW_F10601_VV_20210627T105819Z_20210709T105820Z_v1.0_20261118T210953Z", 10601)]
        cc = [hit("OPERA_L2_COMPRESSED-CSLC-S1_F10601_T040-084801-IW1_20210627T000000Z_20210110T000000Z_"
                  "20210627T000000Z_%s_VV_v1.0" % made, 10601) for made in ("20250808T093249Z", "20250901T010101Z")]
        with mock.patch.object(cs, "es_scroll", side_effect=[l3, cc]) as scroll:
            got = cs.frames_products([10599, 10600, 10601])
        self.assertEqual(scroll.call_count, 2)                      # not two per frame
        self.assertEqual(got[10599][0], {"20210615"})
        self.assertEqual(got[10600], (set(), set(), {}))
        self.assertEqual(got[10601][0], {"20210709"})
        self.assertEqual(got[10601][1], {"20210627"})
        self.assertEqual(got[10601][2], {("T040-084801-IW1", "20210627"): 2})


class TestLightProductReads(unittest.TestCase):
    """The product reads name their indices and can be bounded to a campaign's own products."""

    def test_reads_name_the_product_indices_and_bound_creation_time(self):
        import disp_s1_campaign_status as cs
        from unittest import mock
        with mock.patch.object(cs, "es_scroll", return_value=[]) as scroll:
            cs.frames_products([10599], created_since="2026-11-18T19:00:00Z")
        paths = [call[0][0] for call in scroll.call_args_list]
        self.assertTrue(paths[0].startswith("/grq_*_l3_disp_s1-*/"))
        self.assertTrue(paths[1].startswith("/grq_*_l2_cslc_s1_compressed-*/"))
        for call in scroll.call_args_list:
            self.assertIn({"range": {"creation_timestamp": {"gte": "2026-11-18T19:00:00Z"}}},
                          call[0][1]["query"]["bool"]["filter"])

    def test_without_a_bound_every_product_of_the_frames_is_read(self):
        import disp_s1_campaign_status as cs
        from unittest import mock
        with mock.patch.object(cs, "es_scroll", return_value=[]) as scroll:
            cs.frames_products([10599])
        self.assertEqual(len(scroll.call_args_list[0][0][1]["query"]["bool"]["filter"]), 2)

    def test_several_batch_procs_can_be_selected_in_one_run(self):
        procs = [("a", {"frame_states": {"1": 1}}), ("b", {"frame_states": {"2": 1}}), ("c", {})]
        self.assertEqual([i for i, _ in select_campaigns(procs, proc_id=["a", "c"])], ["a", "c"])
