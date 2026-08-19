"""Compressed CSLC dependency math on phase-annotated frames.

Every dependency computation counts from the start of the lineage containing
the date being processed. Without annotations that is the start of the series,
which is the un-phased behavior; with them it is the start of the containing
chunk's historical phase, so a post-gap block bootstraps a fresh lineage even
though it does not begin on the absolute k grid.
"""

from pathlib import Path

import pytest

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_dependency import CSLCDependency, get_dependent_ccslc_index

TEST_DATA = Path(__file__).parents[1] / "test_data"
ANNOTATED_DB = str(TEST_DATA / "disp_s1_consistent_db_with_modes.json")
EXCERPT_DB = str(TEST_DATA / "disp_s1_with_processing_mode_excerpt.json")

K = 15
M = 3

# Frame 16669 of the annotated fixture:
#   historical_01 [0, 195) forward_01 [195, 206) historical_02 [206, 236) forward_02 [236, 239)
FRAME = 16669
H02_START = 206
H02_STACK_1_END = 221  # first date of the phase's second ministack


class RecordingEs:
    """Minimal stand-in for the GRQ ES utility used by the dependency lookups."""

    def __init__(self, hits=None):
        self.hits = hits if hits is not None else [{"_id": "ccslc"}]
        self.queried_indices = []

    def query(self, index=None, body=None):
        self.queried_indices.append(
            body["query"]["bool"]["must"][0]["term"]["metadata.ccslc_m_index.keyword"])
        return self.hits


def make_dependency(use_processing_modes=True, db=ANNOTATED_DB, k=K, m=M):
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(db, use_processing_modes=use_processing_modes)
    return CSLCDependency(k, m, frame_to_bursts, None, None, None, None, None), frame_to_bursts


def day_index_at(frame_to_bursts, pos, frame=FRAME):
    return frame_to_bursts[frame].sensing_datetime_days_index[pos]


# ---------------------------------------------------------------------------
# lineage bounds
# ---------------------------------------------------------------------------

def test_lineage_start_is_the_containing_phases_historical_start():
    dependency, frame_to_bursts = make_dependency()

    for pos, expected in [(0, 0), (194, 0), (195, 0), (205, 0),
                          (H02_START, H02_START), (235, H02_START), (238, H02_START)]:
        day_index = day_index_at(frame_to_bursts, pos)
        assert dependency.lineage_start_list_index(FRAME, day_index) == expected, pos


def test_lineage_start_is_zero_without_annotations():
    dependency, frame_to_bursts = make_dependency(use_processing_modes=False)

    for pos in (0, 195, H02_START, 238):
        day_index = day_index_at(frame_to_bursts, pos)
        assert dependency.lineage_start_list_index(FRAME, day_index) == 0


def test_lineage_start_past_the_database_belongs_to_the_last_chunk():
    dependency, frame_to_bursts = make_dependency()
    beyond = frame_to_bursts[FRAME].sensing_datetime_days_index[-1] + 12

    assert dependency.lineage_start_list_index(FRAME, beyond) == H02_START


def test_lineage_start_isolates_a_no_run_phase():
    dependency, frame_to_bursts = make_dependency()
    # Frame 46294 is one long no_run block; frame 18905 opens with one
    assert dependency.lineage_start_list_index(46294, day_index_at(frame_to_bursts, 10, frame=46294)) == 0
    assert dependency.lineage_start_list_index(18905, day_index_at(frame_to_bursts, 2, frame=18905)) == 0
    assert dependency.lineage_start_list_index(18905, day_index_at(frame_to_bursts, 4, frame=18905)) == 4


# ---------------------------------------------------------------------------
# get_prev_day_indices
# ---------------------------------------------------------------------------

def test_prev_day_indices_are_bounded_by_the_phase():
    dependency, frame_to_bursts = make_dependency()
    all_indices = frame_to_bursts[FRAME].sensing_datetime_days_index

    prev = dependency.get_prev_day_indices(day_index_at(frame_to_bursts, 210), FRAME)

    assert prev == all_indices[H02_START:210]
    assert len(prev) == 4


def test_prev_day_indices_are_empty_at_a_new_lineage_start():
    dependency, frame_to_bursts = make_dependency()

    assert dependency.get_prev_day_indices(day_index_at(frame_to_bursts, H02_START), FRAME) == []


def test_prev_day_indices_unbounded_without_annotations():
    dependency, frame_to_bursts = make_dependency(use_processing_modes=False)
    all_indices = frame_to_bursts[FRAME].sensing_datetime_days_index

    prev = dependency.get_prev_day_indices(day_index_at(frame_to_bursts, 210), FRAME)

    assert prev == all_indices[:210]


# ---------------------------------------------------------------------------
# determine_k_cycle -- the compressed CSLC boundary the download job keys off
# ---------------------------------------------------------------------------

def test_k_cycle_fires_at_the_phase_relative_boundary():
    """A post-gap block starting off the absolute k grid still closes ministacks."""

    phased, frame_to_bursts = make_dependency()
    unphased, _ = make_dependency(use_processing_modes=False)

    boundary_day_index = day_index_at(frame_to_bursts, H02_START + K - 1)

    assert phased.determine_k_cycle(None, boundary_day_index, FRAME) == 0
    # The same date on the absolute grid is nowhere near a boundary, which is why a naive
    # historical restart never publishes a compressed CSLC for the post-gap block
    assert unphased.determine_k_cycle(None, boundary_day_index, FRAME) != 0


@pytest.mark.parametrize("offset,expected", [(0, 1), (1, 2), (K - 2, K - 1), (K - 1, 0), (K, 1)])
def test_k_cycle_counts_from_the_phase_start(offset, expected):
    dependency, frame_to_bursts = make_dependency()

    day_index = day_index_at(frame_to_bursts, H02_START + offset)

    assert dependency.determine_k_cycle(None, day_index, FRAME) == expected


def test_k_cycle_without_annotations_counts_from_the_series_start():
    dependency, frame_to_bursts = make_dependency(use_processing_modes=False)

    for pos in (K - 1, 2 * K - 1, 194):
        day_index = day_index_at(frame_to_bursts, pos)
        assert dependency.determine_k_cycle(None, day_index, FRAME) == (pos + 1) % K


def test_k_cycle_on_the_real_post_gap_frame():
    """Frame 24726's historical_02 starts at position 86, which is not a multiple of k."""

    dependency, frame_to_bursts = make_dependency(db=EXCERPT_DB)
    indices = frame_to_bursts[24726].sensing_datetime_days_index

    assert dependency.lineage_start_list_index(24726, indices[86]) == 86
    assert dependency.determine_k_cycle(None, indices[86 + K - 1], 24726) == 0
    assert dependency.determine_k_cycle(None, indices[86], 24726) == 1


# ---------------------------------------------------------------------------
# compressed CSLC selection
# ---------------------------------------------------------------------------

def test_compressed_cslc_trivially_satisfied_at_every_new_lineage():
    dependency, frame_to_bursts = make_dependency()
    es = RecordingEs(hits=[])

    for pos in (0, H02_START):
        assert dependency.compressed_cslc_satisfied(FRAME, day_index_at(frame_to_bursts, pos), es) is True

    assert es.queried_indices == [], "a lineage-first ministack must not depend on any compressed CSLC"


def test_dependent_compressed_cslcs_resolve_inside_the_phase():
    dependency, frame_to_bursts = make_dependency()
    indices = frame_to_bursts[FRAME].sensing_datetime_days_index
    es = RecordingEs()

    # Second ministack of historical_02: m ramps to 2, so one prior boundary is required
    dependency.get_dependent_compressed_cslcs(FRAME, day_index_at(frame_to_bursts, H02_STACK_1_END), es)

    expected_boundary = indices[H02_START + K - 1]
    assert es.queried_indices, "expected a compressed CSLC lookup for the phase's first boundary"
    assert all(q.endswith(f"_{expected_boundary}") for q in es.queried_indices)
    # Nothing from before the lineage break may be referenced
    assert all(int(q.rsplit("_", 1)[1]) >= indices[H02_START] for q in es.queried_indices)


def test_m_ramps_from_one_within_each_phase():
    dependency, frame_to_bursts = make_dependency()
    indices = frame_to_bursts[FRAME].sensing_datetime_days_index
    bursts = len(frame_to_bursts[FRAME].burst_ids)

    for stacks_done, pos in enumerate([H02_START, H02_START + K, H02_START + 2 * K]):
        es = RecordingEs()
        dependency.get_dependent_compressed_cslcs(FRAME, indices[pos], es)
        # m - 1 prior compressed CSLC sets are required, capped by what the phase has produced
        expected_sets = min(stacks_done, M - 1)
        assert len(es.queried_indices) == expected_sets * bursts


def test_dependent_ccslc_index_is_the_previous_in_phase_boundary():
    dependency, frame_to_bursts = make_dependency()
    indices = frame_to_bursts[FRAME].sensing_datetime_days_index

    prev = dependency.get_prev_day_indices(day_index_at(frame_to_bursts, 236), FRAME)
    burst_id = "T042-088905-IW2"

    assert get_dependent_ccslc_index(prev, 0, K, burst_id).endswith(f"_{indices[H02_START + 2 * K - 1]}")
    assert get_dependent_ccslc_index(prev, 1, K, burst_id).endswith(f"_{indices[H02_START + K - 1]}")


def test_dependent_compressed_cslcs_report_missing_boundary():
    dependency, frame_to_bursts = make_dependency()
    es = RecordingEs(hits=[])

    satisfied = dependency.compressed_cslc_satisfied(
        FRAME, day_index_at(frame_to_bursts, H02_STACK_1_END), es)

    assert satisfied is False
