"""Parsing of processing-mode-annotated DISP-S1 consistent burst databases.

Covers the master switch (annotations are inert unless it is on), the
golden-master requirement that an annotated database with the switch off
parses identically to the un-annotated one, per-frame quarantine of bad
annotations, and phase-aware progress accounting.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import PhaseKind

TEST_DATA = Path(__file__).parent / "test_data"
ANNOTATED_DB = str(TEST_DATA / "disp_s1_consistent_db_with_modes.json")
MALFORMED_DB = str(TEST_DATA / "disp_s1_consistent_db_malformed_modes.json")
EXCERPT_DB = str(TEST_DATA / "disp_s1_with_processing_mode_excerpt.json")
# The un-annotated database the annotated fixture was built from, frame for frame
LEGACY_DB = str(Path(__file__).parents[2] / "tools" / "test_consistent_db.json")

K = 15
ANNOTATED_FRAMES = [16669, 18904, 18905, 44328, 46294]


def phase_shape(frame):
    return [(p.label, p.start_pos, p.end_pos) for p in frame.phases]


# ---------------------------------------------------------------------------
# feature gating
# ---------------------------------------------------------------------------

def test_legacy_db_carries_no_annotations_even_with_modes_enabled():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(LEGACY_DB, use_processing_modes=True)

    for frame in frame_to_bursts.values():
        assert frame.processing_modes is None
        assert frame.phases is None
        assert frame.phase_error is None


def test_annotated_db_with_modes_disabled_parses_like_the_legacy_db():
    """Golden master: deploying the annotated database with the switch off is a no-op."""

    annotated, annotated_burst_to_frames, _ = cslc_utils.process_disp_frame_burst_hist(
        ANNOTATED_DB, use_processing_modes=False)
    legacy, legacy_burst_to_frames, _ = cslc_utils.process_disp_frame_burst_hist(
        LEGACY_DB, use_processing_modes=False)

    for frame_id in ANNOTATED_FRAMES:
        a, l = annotated[frame_id], legacy[frame_id]
        assert a.burst_ids == l.burst_ids
        assert a.sensing_datetimes == l.sensing_datetimes
        assert a.sensing_seconds_since_first == l.sensing_seconds_since_first
        assert a.sensing_datetime_days_index == l.sensing_datetime_days_index
        assert a.processing_modes is None
        assert a.phases is None
        assert a.phase_error is None
        assert legacy_burst_to_frames[sorted(l.burst_ids)[0]] == annotated_burst_to_frames[sorted(a.burst_ids)[0]]


def test_annotated_db_with_modes_enabled_keeps_the_same_sensing_times():
    """Enabling the switch adds annotations without disturbing anything else."""

    enabled, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)
    disabled, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=False)

    for frame_id in ANNOTATED_FRAMES:
        assert enabled[frame_id].sensing_datetimes == disabled[frame_id].sensing_datetimes
        assert enabled[frame_id].sensing_datetime_days_index == disabled[frame_id].sensing_datetime_days_index
        assert enabled[frame_id].phases is not None


def test_processing_mode_enabled_defaults_to_off_in_settings():
    assert cslc_utils.processing_mode_enabled() is False


def test_processing_mode_enabled_reads_the_settings_field(tmp_path):
    on = tmp_path / "settings_on.yaml"
    on.write_text(f"{cslc_utils.PROCESSING_MODE_SETTINGS_FIELD}: true\n")
    off = tmp_path / "settings_off.yaml"
    off.write_text("SOME_OTHER_FIELD: 1\n")

    assert cslc_utils.processing_mode_enabled(str(on)) is True
    assert cslc_utils.processing_mode_enabled(str(off)) is False
    assert cslc_utils.processing_mode_enabled(str(tmp_path / "does_not_exist.yaml")) is False


# ---------------------------------------------------------------------------
# segmentation of the annotated fixtures
# ---------------------------------------------------------------------------

def test_annotated_fixture_segmentation():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)

    # Four phases with a post-gap historical block that does not start on the absolute k grid
    assert phase_shape(frame_to_bursts[16669]) == [
        ("historical_01", 0, 195), ("forward_01", 195, 206),
        ("historical_02", 206, 236), ("forward_02", 236, 239),
    ]
    assert [p.is_new_lineage for p in frame_to_bursts[16669].phases] == [False, False, True, False]
    assert frame_to_bursts[16669].processing_mode_batch_size == K

    # Adjacent historical phases: a new lineage without an intervening forward block
    assert phase_shape(frame_to_bursts[18904]) == [
        ("historical_01", 0, 150), ("historical_02", 150, 315),
    ]
    assert [p.is_new_lineage for p in frame_to_bursts[18904].phases] == [False, True]

    # Leading no_run chunk: the first processed phase is greenfield even at ordinal 2
    assert phase_shape(frame_to_bursts[18905]) == [
        ("no_run", 0, 4), ("historical_02", 4, 319), ("forward_02", 319, 324),
    ]
    assert [p.is_new_lineage for p in frame_to_bursts[18905].phases] == [False, False, False]

    # Trailing no_run chunk
    assert phase_shape(frame_to_bursts[44328]) == [
        ("historical_01", 0, 135), ("forward_01", 135, 143), ("no_run", 143, 152),
    ]

    # Nothing processable at all
    assert phase_shape(frame_to_bursts[46294]) == [("no_run", 0, 300)]
    assert all(p.kind is PhaseKind.NO_RUN for p in frame_to_bursts[46294].phases)


def test_labels_pair_by_timestamp_not_by_insertion_order():
    """Frame 16669 is written newest-first in the fixture, as one deployed frame is."""

    raw = json.load(open(ANNOTATED_DB))["data"]["16669"]["sensing_time_list"]
    assert list(raw) != sorted(raw), "fixture is expected to be stored out of chronological order"

    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)
    frame = frame_to_bursts[16669]

    assert frame.sensing_datetimes == sorted(frame.sensing_datetimes)
    assert frame.processing_modes[0] == "historical_01"
    assert frame.processing_modes[-1] == "forward_02"
    assert frame.processing_modes[195] == "forward_01"
    assert frame.processing_modes[206] == "historical_02"


def test_unannotated_frame_inside_an_annotated_db():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)

    frame = frame_to_bursts[99999]
    assert len(frame.sensing_datetimes) == 20
    assert frame.processing_modes is None
    assert frame.phases is None
    assert frame.phase_error is None


def test_real_excerpt_segmentation():
    """The shapes of the real annotated database for the frames this work targets."""

    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(EXCERPT_DB, use_processing_modes=True)

    assert phase_shape(frame_to_bursts[24726]) == [
        ("historical_01", 0, 75), ("forward_01", 75, 86),
        ("historical_02", 86, 101), ("forward_02", 101, 109),
    ]
    assert [p.is_new_lineage for p in frame_to_bursts[24726].phases] == [False, False, True, False]
    assert frame_to_bursts[24726].sensing_datetimes[86] == datetime(2025, 5, 29, 1, 32, 39)

    # The only ticket frame whose post-gap historical block is two ministacks
    assert phase_shape(frame_to_bursts[44325]) == [
        ("historical_01", 0, 150), ("forward_01", 150, 152),
        ("historical_02", 152, 182), ("forward_02", 182, 185),
    ]

    # Adjacent historical phases on real data
    assert phase_shape(frame_to_bursts[5127]) == [
        ("historical_01", 0, 135), ("historical_02", 135, 150), ("forward_02", 150, 161),
    ]
    assert frame_to_bursts[5127].phases[1].is_new_lineage is True

    # Too few full-coverage dates to bootstrap any lineage
    assert phase_shape(frame_to_bursts[6825]) == [("no_run", 0, 2)]


# ---------------------------------------------------------------------------
# quarantine
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("frame_id,message", [
    (1001, "unrecognized"),
    (1002, "recurs"),
    (1003, "not a multiple"),
    (1004, "must directly follow"),
    (1005, "forward phases hold"),
    (1006, "ordinal decreases"),
])
def test_malformed_frames_are_quarantined_individually(frame_id, message):
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(MALFORMED_DB, use_processing_modes=True)

    frame = frame_to_bursts[frame_id]
    assert frame.phases is None
    assert message in frame.phase_error
    # The frame is still usable for everything that does not depend on phases
    assert len(frame.sensing_datetimes) == len(frame.processing_modes)


def test_valid_frame_survives_alongside_malformed_ones():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(MALFORMED_DB, use_processing_modes=True)

    frame = frame_to_bursts[1007]
    assert frame.phase_error is None
    assert phase_shape(frame) == [("historical_01", 0, 15), ("forward_01", 15, 17)]


def test_annotations_without_batch_size_metadata_are_quarantined(tmp_path):
    db = tmp_path / "no_batch_size.json"
    db.write_text(json.dumps({
        "metadata": {"generation_time": "2026-04-25 13:03:21"},
        "data": {"7000": {
            "burst_id_list": ["t111_222222_iw1"],
            "sensing_time_list": {"2020-01-01T01:02:03": "historical_01",
                                  "2020-01-13T01:02:04": "historical_01"},
        }},
    }))

    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(str(db), use_processing_modes=True)

    frame = frame_to_bursts[7000]
    assert frame.phases is None
    assert "batch_size" in frame.phase_error
    assert frame.processing_modes == ["historical_01", "historical_01"]


# ---------------------------------------------------------------------------
# progress accounting
# ---------------------------------------------------------------------------

def test_calculate_historical_progress_unphased_is_unchanged():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(LEGACY_DB, use_processing_modes=False)
    end_date = datetime(2030, 1, 1)

    progress, completion, last_processed = cslc_utils.calculate_historical_progress(
        {"16669": 30, "18904": 0}, end_date, frame_to_bursts, K)

    # 239 and 315 sensing times, both rounded down to a multiple of k
    assert completion == {"16669": 13, "18904": 0}
    assert progress == round(30 / (225 + 315) * 100)
    assert last_processed["16669"] == frame_to_bursts[16669].sensing_datetimes[29]
    assert last_processed["18904"] is None


def test_calculate_historical_progress_phased_excludes_no_run_dates():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)
    end_date = datetime(2030, 1, 1)

    progress, completion, _ = cslc_utils.calculate_historical_progress(
        {"16669": 206, "44328": 143, "46294": 0}, end_date, frame_to_bursts, K, phased=True)

    # 44328's trailing 9 no_run dates are not work to be done, so the frame is complete;
    # 46294 has nothing processable at all
    assert completion == {"16669": 86, "44328": 100, "46294": 100}
    assert progress == round((206 + 143) / (239 + 143) * 100)


def test_calculate_historical_progress_phased_ignores_frames_without_phases():
    """A phased batch proc still accounts for an unannotated frame the old way."""

    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)
    end_date = datetime(2030, 1, 1)

    _, completion, _ = cslc_utils.calculate_historical_progress(
        {"99999": 15}, end_date, frame_to_bursts, K, phased=True)

    assert completion == {"99999": round(15 / 15 * 100)}


def test_calculate_historical_progress_phased_respects_the_end_date():
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=True)
    end_date = frame_to_bursts[16669].sensing_datetimes[99]

    progress, completion, _ = cslc_utils.calculate_historical_progress(
        {"16669": 90}, end_date, frame_to_bursts, K, phased=True)

    # 100 dates are in range, all inside historical_01, rounded down to 6 whole k-sets
    assert completion == {"16669": 100}
    assert progress == 100
