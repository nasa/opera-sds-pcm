"""Frame selection for CSLC bursts that belong to two DISP-S1 frames.

A burst can belong to two frames, and each frame counts acquisition cycles from its own first
sensing datetime. Frames 18904 and 18905 share T071-151233-IW1..IW3, and 18905's series starts
12 days before 18904's, so one acquisition of a shared burst is cycle c of 18904 and cycle c+12
of 18905. Wherever the code picks a frame or a cycle for such a burst it has to use the frame
that was asked for and that frame's own cycle.

CMR is replaced by a fake that answers from every granule the two frames' burst database
entries imply, so the reprocessing paths run end to end without a network.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data_subscriber.cmr import CMR_TIME_FORMAT
from data_subscriber.cslc.cslc_blackout import DispS1BlackoutDates
from data_subscriber.cslc.cslc_query import CslcCmrQuery
from data_subscriber.cslc_utils import determine_acquisition_cycle_cslc, split_download_batch_id
from data_subscriber.parser import create_parser
from data_subscriber.query import DateTimeRange

TEST_DATA = Path(__file__).parents[1] / "test_data"
BURST_DB = str(TEST_DATA / "disp_s1_with_processing_mode_2026_10_27_excerpt.json")
BLACKOUT = str(TEST_DATA / "disp_s1_blackout_dates_f25278.json")
SETTINGS = {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60}

LOWER, UPPER = 18904, 18905
SHARED = {"T071-151233-IW1", "T071-151233-IW2", "T071-151233-IW3"}
NOW = datetime(2026, 11, 1)


def granule_id(burst_id, acquired):
    produced = acquired + timedelta(days=1)
    return f"OPERA_L2_CSLC-S1_{burst_id}_{acquired:%Y%m%dT%H%M%SZ}_{produced:%Y%m%dT%H%M%SZ}_S1A_VV_v1.1"


def make_args(*extra):
    return create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--k=1", "--m=1", *extra])


def make_query(*extra):
    return CslcCmrQuery(make_args(*extra), None, None, None, None, SETTINGS, BURST_DB, BLACKOUT)


def build_universe(frame_to_bursts):
    """Every granule CMR holds for the two frames: each burst of a frame on each of its sensing
    datetimes. A shared burst is acquired once per pass, so it appears once per calendar date."""

    universe = {}
    for frame_id in (LOWER, UPPER):
        frame = frame_to_bursts[frame_id]
        for acquired in frame.sensing_datetimes:
            for burst_id in frame.burst_ids:
                universe.setdefault((burst_id, acquired.date()), (burst_id, acquired))
    return list(universe.values())


def fake_cmr(universe):
    """Stand in for _get_query_func: answer native-id and temporal queries from the universe."""

    def factory(use_async=False, secondary=False, args=None, settings=None):
        bursts = None
        if args is not None and args.native_id:
            bursts = {pattern.replace("OPERA_L2_CSLC-S1_", "").rstrip("*")
                      for pattern in args.native_id.split("&native-id[]=")}

        async def query(timerange, now):
            start = datetime.strptime(timerange.start_date, CMR_TIME_FORMAT)
            end = datetime.strptime(timerange.end_date, CMR_TIME_FORMAT)
            return [{"granule_id": granule_id(burst_id, acquired)} for burst_id, acquired in universe
                    if (bursts is None or burst_id in bursts) and start <= acquired <= end]

        return query

    return factory


@pytest.fixture
def query_for():
    """Build a CslcCmrQuery wired to the fake CMR."""

    def build(*extra):
        q = make_query(*extra)
        q._get_query_func = fake_cmr(build_universe(q.disp_burst_map_hist))
        return q

    return build


def around(acquired, minutes=15):
    return DateTimeRange((acquired - timedelta(minutes=minutes)).strftime(CMR_TIME_FORMAT),
                         (acquired + timedelta(minutes=minutes)).strftime(CMR_TIME_FORMAT))


def date_args(acquired, minutes=15):
    timerange = around(acquired, minutes)
    return [f"--start-date={timerange.start_date}", f"--end-date={timerange.end_date}"]


def download_batches(q, granules):
    return {g["download_batch_id"] for g in q.determine_download_granules(q.eliminate_duplicate_granules(granules))}


def lower_date_with_a_lower_date_12_days_later(frame_to_bursts):
    """An acquisition of 18904 whose 12-days-later neighbour is also an 18904 acquisition."""

    dates = frame_to_bursts[LOWER].sensing_datetimes
    by_day = {d.date() for d in dates}
    return next(d for d in dates if d.year >= 2024 and (d + timedelta(days=12)).date() in by_day)


def test_the_fixture_frames_share_bursts_and_count_cycles_from_different_epochs():
    q = make_query()
    assert {b for b, frames in q.burst_to_frames.items() if sorted(frames) == [LOWER, UPPER]} == SHARED

    lower_epoch = q.disp_burst_map_hist[LOWER].sensing_datetimes[0]
    upper_epoch = q.disp_burst_map_hist[UPPER].sensing_datetimes[0]
    assert round((lower_epoch - upper_epoch).total_seconds() / 86400) == 12


# ---------------------------------------------------------------------------
# reprocessing by date range
# ---------------------------------------------------------------------------

def test_reprocessing_a_frame_by_date_queries_only_that_frames_acquisition(query_for):
    """The neighbour's cycle for a shared burst names another 18904 acquisition 12 days later."""

    probe = make_query()
    acquired = lower_date_with_a_lower_date_12_days_later(probe.disp_burst_map_hist)
    q = query_for("--processing-mode=reprocessing", f"--frame-id={LOWER}", *date_args(acquired))

    granules = q._run_query_func(around(acquired), NOW)

    assert {g["frame_id"] for g in granules} == {LOWER}
    assert {g["acquisition_ts"].date() for g in granules} == {acquired.date()}
    cycle = determine_acquisition_cycle_cslc(acquired, LOWER, q.disp_burst_map_hist)
    assert download_batches(q, granules) == {f"f{LOWER}_a{cycle}"}


def test_reprocessing_a_frame_at_its_first_date_survives_the_neighbours_negative_cycle(query_for):
    """At 18905's first date the shared bursts are cycle -12 of 18904."""

    probe = make_query()
    acquired = probe.disp_burst_map_hist[UPPER].sensing_datetimes[0]
    q = query_for("--processing-mode=reprocessing", f"--frame-id={UPPER}", *date_args(acquired))

    granules = q._run_query_func(around(acquired), NOW)

    assert {g["frame_id"] for g in granules} == {UPPER}
    assert {g["acquisition_ts"].date() for g in granules} == {acquired.date()}
    assert download_batches(q, granules) == {f"f{UPPER}_a0"}


def test_reprocessing_by_date_without_a_frame_handles_a_negative_cycle(query_for):
    """Without --frame-id both frames are queried, including 18904 at cycle -12."""

    probe = make_query()
    acquired = probe.disp_burst_map_hist[UPPER].sensing_datetimes[0]
    q = query_for("--processing-mode=reprocessing", *date_args(acquired))

    granules = q._run_query_func(around(acquired), NOW)

    assert (LOWER, -12) in {(g["frame_id"], g["acquisition_cycle"]) for g in granules}
    # 18904 holds only the three shared bursts that day, so only 18905 forms a complete batch
    assert download_batches(q, granules) == {f"f{UPPER}_a0"}


# ---------------------------------------------------------------------------
# reprocessing by native id
# ---------------------------------------------------------------------------

def shared_burst_acquisition_in_both_frames(frame_to_bursts):
    upper_days = {d.date() for d in frame_to_bursts[UPPER].sensing_datetimes}
    return next(d for d in frame_to_bursts[LOWER].sensing_datetimes if d.year >= 2024 and d.date() in upper_days)


def test_native_id_on_a_shared_burst_processes_the_requested_frame(query_for):
    probe = make_query()
    acquired = shared_burst_acquisition_in_both_frames(probe.disp_burst_map_hist)
    native_id = granule_id("T071-151233-IW2", acquired)
    q = query_for("--processing-mode=reprocessing", f"--native-id={native_id}", f"--frame-id={UPPER}")

    granules = q._run_query_func(None, NOW)

    assert {g["frame_id"] for g in granules} == {UPPER}
    assert {g["burst_id"] for g in granules} == q.disp_burst_map_hist[UPPER].burst_ids


def test_native_id_on_a_shared_burst_without_a_frame_says_which_frame_it_took(query_for):
    probe = make_query()
    acquired = shared_burst_acquisition_in_both_frames(probe.disp_burst_map_hist)
    native_id = granule_id("T071-151233-IW2", acquired)
    q = query_for("--processing-mode=reprocessing", f"--native-id={native_id}")
    q.logger = MagicMock()

    granules = q._run_query_func(None, NOW)

    assert {g["frame_id"] for g in granules} == {LOWER}
    warning = q.logger.warning.call_args
    assert warning is not None and [LOWER, UPPER] in warning.args


def test_native_id_with_a_frame_the_burst_is_not_in_is_rejected(query_for):
    probe = make_query()
    acquired = shared_burst_acquisition_in_both_frames(probe.disp_burst_map_hist)
    native_id = granule_id("T071-151233-IW2", acquired)
    q = query_for("--processing-mode=reprocessing", f"--native-id={native_id}", "--frame-id=24726")

    with pytest.raises(AssertionError, match="does not belong to frame 24726"):
        q._run_query_func(None, NOW)


# ---------------------------------------------------------------------------
# --frame-id itself
# ---------------------------------------------------------------------------

def test_frame_id_is_parsed_as_an_integer():
    assert make_args(f"--frame-id={LOWER}").frame_id == LOWER
    with pytest.raises(SystemExit):
        make_args("--frame-id=F18904")


def test_frame_id_outside_the_burst_database_is_rejected_up_front():
    with pytest.raises(AssertionError, match="Frame 99999 is not in the DISP-S1 Burst ID Database"):
        make_query("--processing-mode=forward", "--frame-id=99999")


@pytest.mark.parametrize("proc_mode", ["reprocessing", "historical"])
def test_download_batches_for_another_frame_fail_the_query(proc_mode):
    q = make_query(f"--processing-mode={proc_mode}", f"--frame-id={LOWER}",
                   "--start-date=2024-01-01T00:00:00Z", "--end-date=2024-02-01T00:00:00Z")

    q.assert_download_batches_match_frame([{"download_batch_id": f"f{LOWER}_a100"}])

    with pytest.raises(AssertionError, match=f"f{UPPER}_a112"):
        q.assert_download_batches_match_frame([{"download_batch_id": f"f{UPPER}_a112"}])


def test_a_batch_for_another_frame_fails_the_query_before_anything_is_catalogued(query_for):
    """determine_download_granules catalogues the k-1 granules it retrieves, so the frame check has
    to come first: a failed query must leave no catalog documents behind for a retry to pile onto."""

    probe = make_query()
    acquired = shared_burst_acquisition_in_both_frames(probe.disp_burst_map_hist)
    q = query_for("--processing-mode=reprocessing", f"--frame-id={LOWER}", *date_args(acquired))
    q.args.k = 15
    other_frame = [{"granule_id": granule_id(b, acquired), "frame_id": UPPER, "burst_id": b,
                    "acquisition_cycle": 112, "acquisition_ts": acquired}
                   for b in sorted(q.disp_burst_map_hist[UPPER].burst_ids)]
    q._run_query_func = MagicMock(return_value=other_frame)
    q.k_es_conn = MagicMock()
    q.es_conn = MagicMock()
    q.catalog_granules = MagicMock()
    q.determine_download_granules = MagicMock(wraps=q.determine_download_granules)

    with pytest.raises(AssertionError, match=f"f{UPPER}_a112"):
        q.run_query()

    q.determine_download_granules.assert_not_called()
    q.catalog_granules.assert_not_called()
    assert q.k_es_conn.method_calls == []


def test_forward_queries_with_a_frame_id_are_not_checked():
    """A forward query also completes batches of other frames left unsubmitted by earlier queries."""

    q = make_query("--processing-mode=forward", f"--frame-id={LOWER}")
    q.assert_download_batches_match_frame([{"download_batch_id": f"f{UPPER}_a112"}])


# ---------------------------------------------------------------------------
# forward mode copies a shared burst to its second frame
# ---------------------------------------------------------------------------

def test_forward_copy_for_the_second_frame_carries_that_frames_cycle():
    q = make_query()
    blackout = DispS1BlackoutDates({}, q.disp_burst_map_hist, q.burst_to_frames)
    acquired = shared_burst_acquisition_in_both_frames(q.disp_burst_map_hist)
    granules = [{"granule_id": granule_id("T071-151233-IW1", acquired)}]

    blackout.extend_additional_records(granules, "forward")

    assert sorted(g["frame_id"] for g in granules) == [LOWER, UPPER]
    for g in granules:
        cycle = determine_acquisition_cycle_cslc(acquired, g["frame_id"], q.disp_burst_map_hist)
        assert g["acquisition_cycle"] == cycle
        assert split_download_batch_id(g["download_batch_id"]) == (g["frame_id"], cycle)
    by_frame = {g["frame_id"]: g["acquisition_cycle"] for g in granules}
    assert by_frame[UPPER] - by_frame[LOWER] == 12
