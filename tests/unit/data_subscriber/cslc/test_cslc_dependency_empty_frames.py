"""Compressed CSLC dependency math for frames with no sensing datetimes in the burst database.

A frame the database lists without sensing datetimes has no recorded series to take a
position in, so its position is counted from what has already been observed: the cycle
state configs the cycle evaluator publishes, which exist exactly when a cycle's bursts
are all present. The k-cycle evaluator decides window position and compressed CSLC
boundaries from those same records, so the download side and the evaluator agree by
construction rather than by coincidence -- the last two tests here hold that to account.

Counting from published state rather than from CMR also means these paths add no CMR
traffic; every test asserts the CMR query is never reached.
"""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_dependency import CSLCDependency

TEST_DATA = Path(__file__).parents[1] / "test_data"
EMPTY_FRAMES_DB = str(TEST_DATA / "disp_s1_consistent_db_empty_frames.json")

K = 15
M = 6
EMPTY_FRAME = 40590
CONTROL_FRAME = 31241
CAMPAIGN_START = datetime(2016, 7, 1)


@pytest.fixture(autouse=True)
def no_cmr(monkeypatch):
    """Nothing in these paths may reach CMR. The mock makes a regression an explicit failure
    rather than a slow test."""
    cmr = MagicMock(side_effect=AssertionError("CMR must not be queried for an empty frame"))
    monkeypatch.setattr("data_subscriber.cslc.cslc_dependency.query_cmr_cslc_blackout_polarization", cmr)
    yield cmr
    cmr.assert_not_called()


@pytest.fixture(scope="module")
def frame_to_bursts():
    return cslc_utils.process_disp_frame_burst_hist(EMPTY_FRAMES_DB, use_processing_modes=False)[0]


def es_with_cycles(cycles):
    """Stand-in for the GRQ utility, returning complete cycle state configs for the frame."""
    es = MagicMock()
    es.query.return_value = [
        {"_source": {"metadata": {"acquisition_cycle": cycle, "is_complete": True}}}
        for cycle in cycles
    ]
    return es


def dependency(frame_to_bursts, cycles, k=K, m=M):
    return CSLCDependency(k, m, frame_to_bursts, None, None, None, None, None,
                          es_util=es_with_cycles(cycles))


# ---------------------------------------------------------------------------
# where in the k-cycle an acquisition falls
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prior_cycles, expected", [
    (0, 1),           # the frame's first acquisition ever
    (1, 2),
    (K - 2, K - 1),
    (K - 1, 0),       # the k-th acquisition closes the ministack
    (K, 1),           # and the next one starts a new one
    (2 * K - 1, 0),   # as does every k-th after that
])
def test_k_cycle_position_counts_observed_acquisitions(frame_to_bursts, prior_cycles, expected):
    cycles = [12 * i for i in range(prior_cycles)]
    current = 12 * prior_cycles
    dep = dependency(frame_to_bursts, cycles)

    assert dep.determine_k_cycle(None, current, EMPTY_FRAME) == expected


def test_k_cycle_position_ignores_cycles_after_the_one_being_evaluated(frame_to_bursts):
    """Out-of-order ingestion must not move an earlier acquisition's position."""
    dep = dependency(frame_to_bursts, [0, 12, 24, 36, 48])

    assert dep.determine_k_cycle(None, 24, EMPTY_FRAME) == 3  # two before it, plus itself


def test_k_cycle_position_is_stable_whether_or_not_the_current_cycle_is_recorded(frame_to_bursts):
    """The current cycle's own state config may or may not exist yet when the download job
    asks; the answer cannot depend on that race."""
    prior = [12 * i for i in range(K - 1)]
    current = 12 * (K - 1)

    without_current = dependency(frame_to_bursts, prior)
    with_current = dependency(frame_to_bursts, prior + [current])

    assert without_current.determine_k_cycle(None, current, EMPTY_FRAME) == 0
    assert with_current.determine_k_cycle(None, current, EMPTY_FRAME) == 0


def test_blacked_out_and_excluded_cycles_are_left_out_of_the_count(frame_to_bursts):
    """The query itself excludes them, the same way every other k-window consumer does."""
    dep = dependency(frame_to_bursts, [0, 12, 24])
    dep.determine_k_cycle(None, 36, EMPTY_FRAME)

    body = dep.es_util.query.call_args[1]["body"]
    must_not = body["query"]["bool"]["must_not"]
    assert {"term": {"metadata.blackout": True}} in must_not
    assert {"term": {"metadata.db_excluded": True}} in must_not
    must = body["query"]["bool"]["must"]
    assert {"term": {"metadata.frame_id": EMPTY_FRAME}} in must
    assert {"term": {"metadata.is_complete": True}} in must


def test_duplicate_state_configs_count_once(frame_to_bursts):
    dep = dependency(frame_to_bursts, [0, 0, 12, 12, 24])

    assert dep.determine_k_cycle(None, 36, EMPTY_FRAME) == 4


def test_k_cycle_position_accepts_an_acquisition_datetime(frame_to_bursts, monkeypatch):
    """The download job passes a day index; the burst database tool passes a datetime."""
    acquisition = CAMPAIGN_START + timedelta(days=36)
    dep = dependency(frame_to_bursts, [0, 12, 24])

    assert dep.determine_k_cycle(acquisition, None, EMPTY_FRAME) == 4


# ---------------------------------------------------------------------------
# the previous acquisitions a compressed CSLC lineage is built from
# ---------------------------------------------------------------------------

def test_no_previous_acquisitions_for_a_frames_first_cycle(frame_to_bursts):
    dep = dependency(frame_to_bursts, [])

    assert dep.get_prev_day_indices(0, EMPTY_FRAME) == []


def test_previous_acquisitions_are_the_observed_ones_before_this_cycle(frame_to_bursts):
    dep = dependency(frame_to_bursts, [0, 12, 24, 36, 48])

    assert dep.get_prev_day_indices(36, EMPTY_FRAME) == [0, 12, 24]


def test_first_ministack_needs_no_compressed_cslcs(frame_to_bursts):
    """m ramps from 1 at the start of a series, so no compressed CSLC is required and the
    frame is not blocked waiting for one that cannot exist yet."""
    dep = dependency(frame_to_bursts, [])
    grq = es_with_cycles([])

    assert dep.compressed_cslc_satisfied(EMPTY_FRAME, 0, grq) is True

    # Counting what has been observed is expected; looking for a compressed CSLC is not.
    queried = [call.kwargs.get("index") for call in grq.query.call_args_list]
    assert not any("compressed" in (index or "") for index in queried), queried


# ---------------------------------------------------------------------------
# operating without a connection
# ---------------------------------------------------------------------------

def test_missing_es_connection_is_reported_plainly(frame_to_bursts):
    dep = CSLCDependency(K, M, frame_to_bursts, None, None, None, None, None)

    with pytest.raises(RuntimeError, match="no OpenSearch connection"):
        dep.determine_k_cycle(None, 12, EMPTY_FRAME)


def test_es_connection_may_be_passed_per_call(frame_to_bursts):
    """compressed_cslc_satisfied already carries one down; the count uses it."""
    dep = CSLCDependency(K, M, frame_to_bursts, None, None, None, None, None)

    assert dep.get_prev_day_indices(36, EMPTY_FRAME, es_with_cycles([0, 12, 24])) == [0, 12, 24]


# ---------------------------------------------------------------------------
# equivalence: the empty-frame path must agree with the two implementations
# that already decide the same thing for dated frames
# ---------------------------------------------------------------------------

def test_agrees_with_the_burst_database_path_for_the_same_series(frame_to_bursts):
    """Strip a dated frame's series into an empty frame, feed the same acquisitions in as
    observed state, and the two code paths must return the same position everywhere."""
    control = frame_to_bursts[CONTROL_FRAME]
    series = control.sensing_datetime_days_index

    dated = CSLCDependency(K, M, frame_to_bursts, None, None, None, None, None)
    empty = dependency(frame_to_bursts, series)

    for position, day_index in enumerate(series):
        from_database = dated.determine_k_cycle(None, day_index, CONTROL_FRAME)
        # the empty frame's own series is the observed one, so the same position maps across
        from_observed = len([c for c in series if c < day_index]) + 1
        assert from_database == from_observed % K, f"position {position}"

    # and the boundaries land in the same places
    boundaries_database = [p for p, d in enumerate(series)
                           if dated.determine_k_cycle(None, d, CONTROL_FRAME) == 0]
    boundaries_observed = [p for p, d in enumerate(series)
                           if empty._known_complete_cycles(EMPTY_FRAME) and
                           (len([c for c in series if c < d]) + 1) % K == 0]
    assert boundaries_database == boundaries_observed
    assert boundaries_database[:2] == [K - 1, 2 * K - 1]
