"""Query-side handling of frames with little or no recorded acquisition history.

Retrieving the k-1 previous acquisitions walks CMR backwards in widening windows. It has
never consulted the burst database, so it cannot tell a frame that is missing history
from one that never had any: a frame in its first k cycles used to walk the window back
to the start of the mission and then fail the whole query job. That job covers every
frame in the run, and the failure lands before anything is catalogued, so granules for
unrelated frames discovered in the same window were lost with it.

The search now stops at the frame's own first possible acquisition -- its first recorded
sensing datetime, or the campaign start for a frame the database lists without any --
and reports the shortfall instead of failing. A DISP-S1 product is still only generated
once a full window exists; that gate lives in the k-cycle evaluator.
"""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_query import CslcCmrQuery, EARLIEST_POSSIBLE_CSLC_DATE

TEST_DATA = Path(__file__).parents[1] / "test_data"
EMPTY_FRAMES_DB = str(TEST_DATA / "disp_s1_consistent_db_empty_frames.json")

K = 15
EMPTY_FRAME = 40590
CONTROL_FRAME = 31241
CAMPAIGN_START = datetime(2016, 7, 1)


@pytest.fixture(scope="module")
def frame_to_bursts():
    return cslc_utils.process_disp_frame_burst_hist(EMPTY_FRAMES_DB, use_processing_modes=False)[0]


@pytest.fixture
def query(frame_to_bursts, monkeypatch):
    """A CslcCmrQuery with only what these methods touch, so no cluster or S3 is needed."""
    q = CslcCmrQuery.__new__(CslcCmrQuery)
    q.logger = MagicMock()
    q.disp_burst_map_hist = frame_to_bursts
    q.token = None
    q.cmr = None
    q.settings = {}
    q.blackout_dates_obj = MagicMock()
    q.es_conn = MagicMock()
    q.proc_mode = "forward"
    return q


def downloads_for(frame_id, acquisition_time, burst_ids):
    return [{"frame_id": frame_id, "acquisition_ts": acquisition_time, "burst_id": b} for b in burst_ids]


def patch_k_granules(monkeypatch, granules_by_call):
    """Stand in for the CMR round trip, returning a granule map per successive window."""
    calls = []
    responses = iter(granules_by_call)

    def fake(self, query_timerange, frame_number, verbose=True):
        calls.append(query_timerange)
        try:
            return {}, next(responses)
        except StopIteration:
            return {}, {}

    monkeypatch.setattr(
        "data_subscriber.cslc.cslc_dependency.CSLCDependency.get_k_granules_from_cmr", fake)
    return calls


# ---------------------------------------------------------------------------
# k-1 retrieval
# ---------------------------------------------------------------------------

def test_frame_with_no_history_returns_what_exists_instead_of_failing(query, frame_to_bursts, monkeypatch):
    calls = patch_k_granules(monkeypatch, [])
    downloads = downloads_for(EMPTY_FRAME, CAMPAIGN_START + timedelta(days=12),
                              sorted(frame_to_bursts[EMPTY_FRAME].burst_ids))

    k_granules = query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)

    assert k_granules == []
    assert calls, "the first window should still be searched"
    assert any("no more history" in str(c) for c in query.logger.warning.call_args_list)


def test_the_search_stops_at_the_campaign_start_for_a_frame_with_no_sensing_datetimes(
        query, frame_to_bursts, monkeypatch):
    """It must not keep widening the window back towards the start of the mission.

    The first window is a fixed span back from the acquisition and may itself begin before
    the epoch, which costs nothing -- CMR simply holds nothing there. What matters is that
    the loop stops instead of stepping back another span, and another, until it trips the
    mission-start assertion and fails the job.
    """
    calls = patch_k_granules(monkeypatch, [])
    acquisition = CAMPAIGN_START + timedelta(days=24)
    downloads = downloads_for(EMPTY_FRAME, acquisition, sorted(frame_to_bursts[EMPTY_FRAME].burst_ids))

    query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)

    assert len(calls) <= 2, f"kept widening the search: {len(calls)} windows"


def test_a_dated_frame_stops_at_its_own_first_acquisition(query, frame_to_bursts, monkeypatch):
    """Same containment for a newly activated frame whose burst database entry is correct but
    short: the shortfall is reported rather than escalated into a failed query job."""
    calls = patch_k_granules(monkeypatch, [])
    frame_epoch = frame_to_bursts[CONTROL_FRAME].sensing_datetimes[0]
    downloads = downloads_for(CONTROL_FRAME, frame_epoch + timedelta(days=12),
                              sorted(frame_to_bursts[CONTROL_FRAME].burst_ids))

    k_granules = query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)

    assert k_granules == []
    assert len(calls) <= 2, f"kept widening the search: {len(calls)} windows"
    assert any("no more history" in str(c) for c in query.logger.warning.call_args_list)


def test_the_mission_start_assertion_is_never_reached(query, frame_to_bursts, monkeypatch):
    """The epoch floor is always later than the mission-start backstop, so the failure mode
    that used to take down the whole query job is now unreachable."""
    patch_k_granules(monkeypatch, [])
    mission_start = datetime.strptime(EARLIEST_POSSIBLE_CSLC_DATE, "%Y-%m-%dT%H:%M:%SZ")

    for frame_id in (EMPTY_FRAME, CONTROL_FRAME):
        epoch = cslc_utils.frame_sensing_epoch(frame_to_bursts[frame_id], campaign_start=CAMPAIGN_START)
        assert epoch > mission_start

        downloads = downloads_for(frame_id, epoch + timedelta(days=12),
                                  sorted(frame_to_bursts[frame_id].burst_ids))
        query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)  # must not raise


def test_a_full_window_is_returned_without_a_warning(query, frame_to_bursts, monkeypatch):
    """The ordinary case is unchanged: enough previous acquisitions, no complaint."""
    found = {12 * i: [{"granule_id": f"g{i}"}] for i in range(1, K)}
    patch_k_granules(monkeypatch, [found])
    frame_epoch = frame_to_bursts[CONTROL_FRAME].sensing_datetimes[0]
    downloads = downloads_for(CONTROL_FRAME, frame_epoch + timedelta(days=12 * K),
                              sorted(frame_to_bursts[CONTROL_FRAME].burst_ids))

    k_granules = query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)

    assert len(k_granules) == K - 1
    assert not any("no more history" in str(c) for c in query.logger.warning.call_args_list)


def test_a_partial_window_keeps_the_granules_it_did_find(query, frame_to_bursts, monkeypatch):
    found = {12: [{"granule_id": "g1"}], 24: [{"granule_id": "g2"}]}
    patch_k_granules(monkeypatch, [found])
    downloads = downloads_for(EMPTY_FRAME, CAMPAIGN_START + timedelta(days=36),
                              sorted(frame_to_bursts[EMPTY_FRAME].burst_ids))

    k_granules = query.retrieve_k_granules(downloads, MagicMock(k=K, m=6), K - 1)

    assert [g["granule_id"] for g in k_granules] == ["g2", "g1"]


# ---------------------------------------------------------------------------
# reprocessing a single acquisition cycle
# ---------------------------------------------------------------------------

def capture_timerange(monkeypatch):
    """Record the window query_cmr_by_frame_and_acq_cycle builds, without querying."""
    captured = {}

    def fake(self, frame_id, args, token, cmr, settings, now, timerange, verbose=True):
        captured["timerange"] = timerange
        return []

    monkeypatch.setattr(CslcCmrQuery, "query_cmr_by_frame_and_dates", fake)
    return captured


def window_of(captured):
    timerange = captured["timerange"]
    return (datetime.strptime(timerange.start_date, "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime(timerange.end_date, "%Y-%m-%dT%H:%M:%SZ"))


def test_reprocessing_window_for_an_empty_frame_is_the_whole_day(query, monkeypatch):
    """Cycles count whole days from the campaign start and the frame's time of day is
    unknown, so the window cannot be a few minutes around a time that was never recorded."""
    captured = capture_timerange(monkeypatch)

    query.query_cmr_by_frame_and_acq_cycle(EMPTY_FRAME, 24, MagicMock(), None, None, {}, datetime.now())

    start, end = window_of(captured)
    assert start == CAMPAIGN_START + timedelta(days=24)
    assert end - start == timedelta(days=1)


def test_reprocessing_window_for_a_dated_frame_is_unchanged(query, frame_to_bursts, monkeypatch):
    captured = capture_timerange(monkeypatch)

    query.query_cmr_by_frame_and_acq_cycle(CONTROL_FRAME, 24, MagicMock(), None, None, {}, datetime.now())

    start, end = window_of(captured)
    expected = frame_to_bursts[CONTROL_FRAME].sensing_datetimes[0] + timedelta(days=24)
    assert start == expected - timedelta(minutes=15)
    assert end == expected + timedelta(minutes=15)


# ---------------------------------------------------------------------------
# historical processing
# ---------------------------------------------------------------------------

def test_historical_query_over_an_empty_frame_finds_nothing_and_says_why(query, monkeypatch):
    """There is no recorded series to walk. Previously this fell through to a query whose
    results were then filtered against an empty index list, which looked like a clean run."""
    query.proc_mode = "historical"
    query.args = MagicMock(frame_id=str(EMPTY_FRAME))
    monkeypatch.setattr(CslcCmrQuery, "query_cmr_by_frame_and_dates",
                        lambda *a, **kw: pytest.fail("historical mode must not query for a frame with no series"))

    assert query.query_cmr(MagicMock(), datetime.now()) == []
    assert any("no acquisition series" in str(c) for c in query.logger.warning.call_args_list)
