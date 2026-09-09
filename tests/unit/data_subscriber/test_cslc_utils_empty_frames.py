"""Acquisition cycles for frames the burst database lists without sensing datetimes.

Such a frame carries a burst pattern and is processed forward, but has no first
acquisition to count cycles from, so it counts from the campaign start instead. The
value only has to be a constant every component agrees on: a cycle labels a batch, a
state config and a compressed CSLC, and is never measured against anything.

The failure this prevents is not confined to the frame itself. A burst can belong to
two frames, and the cycle for every frame a granule belongs to is computed together, so
one empty frame used to take its dated neighbour down with it -- and, because the
computation happens during the CMR query and before anything is catalogued, the whole
forward query run with it.
"""

from datetime import datetime
from pathlib import Path

import dateutil.parser
import pytest

from data_subscriber import cslc_utils

TEST_DATA = Path(__file__).parent / "test_data"
EMPTY_FRAMES_DB = str(TEST_DATA / "disp_s1_consistent_db_empty_frames.json")

CAMPAIGN_START = datetime(2016, 7, 1)

EMPTY_FRAME = 40590          # 3 bursts, no sensing datetimes, shares a burst with 40589
EMPTY_FRAME_ISOLATED = 46800  # 9 bursts, no sensing datetimes, no shared bursts
DATED_NEIGHBOR = 40589        # 12 bursts, 2 sensing datetimes, shares a burst with 40590
CONTROL_FRAME = 31241         # 2 bursts, 45 sensing datetimes
SHARED_BURST = "T152-324713-IW1"


@pytest.fixture(scope="module")
def burst_db():
    return cslc_utils.process_disp_frame_burst_hist(EMPTY_FRAMES_DB, use_processing_modes=False)


@pytest.fixture(scope="module")
def frame_to_bursts(burst_db):
    return burst_db[0]


@pytest.fixture(scope="module")
def burst_to_frames(burst_db):
    return burst_db[1]


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------

def test_empty_frames_load_with_their_burst_pattern(frame_to_bursts):
    """A frame with no sensing datetimes still carries the bursts that define it."""
    for frame_id, burst_count in ((EMPTY_FRAME, 3), (EMPTY_FRAME_ISOLATED, 9)):
        frame = frame_to_bursts[frame_id]
        assert len(frame.burst_ids) == burst_count
        assert frame.sensing_datetimes == []
        assert frame.sensing_datetime_days_index == []
        assert frame.sensing_seconds_since_first == []


def test_loading_reports_which_frames_have_no_sensing_datetimes(caplog):
    """A database revision that adds datetimes to one of these frames moves its epoch and
    changes every cycle index already published for it, so the frames are named in the log."""
    with caplog.at_level("WARNING"):
        cslc_utils.process_disp_frame_burst_hist.cache_clear()
        cslc_utils.process_disp_frame_burst_hist(EMPTY_FRAMES_DB, use_processing_modes=False)

    assert any(str(EMPTY_FRAME) in r.message and str(EMPTY_FRAME_ISOLATED) in r.message
               for r in caplog.records if "no sensing datetimes" in r.message)


# ---------------------------------------------------------------------------
# the campaign epoch
# ---------------------------------------------------------------------------

def test_frames_carry_the_survey_start_of_the_database_they_came_from(frame_to_bursts):
    """The epoch travels with the data it belongs to, so it stays consistent with whichever
    database was loaded and costs no lookup at the point of use."""
    assert frame_to_bursts[EMPTY_FRAME].campaign_start == CAMPAIGN_START
    assert frame_to_bursts[CONTROL_FRAME].campaign_start == CAMPAIGN_START


def test_campaign_start_falls_back_when_the_survey_range_is_unreadable(tmp_path):
    """Never guess a range: an unparseable or missing one yields the campaign start."""
    no_metadata = tmp_path / "no_metadata.json"
    no_metadata.write_text(
        '{"data": {"1": {"burst_id_list": ["t001_000001_iw1"], "sensing_time_list": []}}}')

    frames, _, _ = cslc_utils.process_disp_frame_burst_hist(str(no_metadata), use_processing_modes=False)

    assert frames[1].campaign_start == cslc_utils.DISP_S1_CAMPAIGN_START
    assert cslc_utils.frame_sensing_epoch(frames[1]) == cslc_utils.DISP_S1_CAMPAIGN_START


def test_frame_epoch_is_the_frames_first_acquisition_when_it_has_one(frame_to_bursts):
    frame = frame_to_bursts[CONTROL_FRAME]
    assert cslc_utils.frame_sensing_epoch(frame) == frame.sensing_datetimes[0]


def test_frame_epoch_is_the_campaign_start_when_the_frame_has_none(frame_to_bursts):
    frame = frame_to_bursts[EMPTY_FRAME]
    assert cslc_utils.frame_sensing_epoch(frame, campaign_start=CAMPAIGN_START) == CAMPAIGN_START


# ---------------------------------------------------------------------------
# day indices
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sensing_time, expected_days", [
    ("2016-07-01T02:47:14", 0),
    ("2016-07-13T02:47:14", 12),
    ("2024-06-18T02:47:14", 2909),
])
def test_empty_frame_day_index_counts_calendar_days_from_the_campaign_start(
        frame_to_bursts, monkeypatch, sensing_time, expected_days):

    day_index, seconds = cslc_utils.sensing_time_day_index(
        dateutil.parser.isoparse(sensing_time), EMPTY_FRAME, frame_to_bursts)

    assert day_index == expected_days
    assert seconds == int((dateutil.parser.isoparse(sensing_time) - CAMPAIGN_START).total_seconds())


def test_empty_frame_day_indices_are_distinct_and_ordered(frame_to_bursts, monkeypatch):
    """Successive acquisitions are days apart, which is all a cycle index has to distinguish."""
    times = [dateutil.parser.isoparse(f"2024-06-{day:02d}T02:47:14") for day in (6, 18, 30)]
    indices = [cslc_utils.sensing_time_day_index(t, EMPTY_FRAME, frame_to_bursts)[0] for t in times]

    assert indices == sorted(indices)
    assert len(set(indices)) == len(indices)
    assert indices[1] - indices[0] == 12


def test_dated_frame_day_index_is_unchanged(frame_to_bursts):
    """The frame-anchored path must be untouched: a listed date keeps the index the
    database recorded for it."""
    frame = frame_to_bursts[CONTROL_FRAME]

    for position in (0, 1, 14, 44):
        day_index, _ = cslc_utils.sensing_time_day_index(
            frame.sensing_datetimes[position], CONTROL_FRAME, frame_to_bursts)
        assert day_index == frame.sensing_datetime_days_index[position]

    assert frame.sensing_datetime_days_index[0] == 0


# ---------------------------------------------------------------------------
# the cross-frame case, which is what made this a production failure
# ---------------------------------------------------------------------------

def test_shared_burst_yields_a_cycle_for_both_frames(frame_to_bursts, burst_to_frames, monkeypatch):
    """One burst, two frames, one of them empty: both get a cycle, each from its own epoch.

    Before, computing the empty frame's cycle raised IndexError, so the granule was lost for
    the dated frame as well -- and the exception surfaced during the CMR query, before any
    granule in that run had been catalogued.
    """
    assert sorted(burst_to_frames[SHARED_BURST]) == [DATED_NEIGHBOR, EMPTY_FRAME]

    native_id = f"OPERA_L2_CSLC-S1_{SHARED_BURST}_20240618T024714Z_20240619T120000Z_S1A_VV_v1.1"
    burst_id, acquisition_dts, acquisition_cycles, frame_ids = cslc_utils.parse_cslc_native_id(
        native_id, burst_to_frames, frame_to_bursts)

    assert burst_id == SHARED_BURST
    assert sorted(frame_ids) == [DATED_NEIGHBOR, EMPTY_FRAME]
    assert set(acquisition_cycles) == {DATED_NEIGHBOR, EMPTY_FRAME}

    # The empty frame counts from the campaign start; its neighbour from its own first
    # acquisition. Both are reached through the same call.
    assert acquisition_cycles[EMPTY_FRAME] == (acquisition_dts.date() - CAMPAIGN_START.date()).days
    neighbor_epoch = frame_to_bursts[DATED_NEIGHBOR].sensing_datetimes[0]
    assert acquisition_cycles[DATED_NEIGHBOR] == pytest.approx(
        round((acquisition_dts - neighbor_epoch).total_seconds() / 86400), abs=1)
    assert acquisition_cycles[EMPTY_FRAME] != acquisition_cycles[DATED_NEIGHBOR]


def test_isolated_empty_frame_parses(frame_to_bursts, burst_to_frames, monkeypatch):

    burst_id = sorted(frame_to_bursts[EMPTY_FRAME_ISOLATED].burst_ids)[0]
    native_id = f"OPERA_L2_CSLC-S1_{burst_id}_20240618T024714Z_20240619T120000Z_S1A_VV_v1.1"

    _, _, acquisition_cycles, frame_ids = cslc_utils.parse_cslc_native_id(
        native_id, burst_to_frames, frame_to_bursts)

    assert frame_ids == [EMPTY_FRAME_ISOLATED]
    assert acquisition_cycles[EMPTY_FRAME_ISOLATED] > 0


def test_download_batch_id_is_well_formed_for_an_empty_frame(frame_to_bursts, monkeypatch):
    """The cycle ends up in the batch id, which keys the download job and the state configs."""
    cycle = cslc_utils.determine_acquisition_cycle_cslc(
        dateutil.parser.isoparse("2024-06-18T02:47:14"), EMPTY_FRAME, frame_to_bursts)
    batch_id = cslc_utils.download_batch_id_forward_reproc(
        {"frame_id": EMPTY_FRAME, "acquisition_cycle": cycle})

    assert batch_id == f"f{EMPTY_FRAME}_a{cycle}"
    assert cslc_utils.split_download_batch_id(batch_id) == (EMPTY_FRAME, cycle)


# ---------------------------------------------------------------------------
# historical processing does not apply to these frames
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("phased", [True, False])
def test_batch_proc_over_an_empty_frame_is_rejected(frame_to_bursts, phased):
    """There is no recorded acquisition series for a historical walk to step through."""
    proc = {
        "k": 15,
        "phased": phased,
        "frames": [EMPTY_FRAME],
        "data_start_date": "2016-07-01T00:00:00",
        "data_end_date": "2025-12-31T00:00:00",
    }
    validate = (cslc_utils.validate_phased_batch_proc if phased
                else cslc_utils.validate_unphased_batch_proc)

    result = validate(proc, frame_to_bursts)

    assert result is not True
    assert "no sensing datetimes" in result
    assert str(EMPTY_FRAME) in result


def test_batch_proc_over_a_dated_frame_is_still_accepted(frame_to_bursts):
    proc = {
        "k": 15,
        "phased": False,
        "frames": [CONTROL_FRAME],
        "data_start_date": "2016-07-01T00:00:00",
        "data_end_date": "2025-12-31T00:00:00",
    }
    assert cslc_utils.validate_unphased_batch_proc(proc, frame_to_bursts) is True
