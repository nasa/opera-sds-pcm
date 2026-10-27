"""Selection of the GUNWs a DISP-NI k-set uses for its ionosphere correction.

The SAS needs an unbroken chain of consecutive GUNWs through every date of the k-set, or none at all:
given a chain with a gap it would not fall back to estimating the ionosphere from the GSLCs. The chain
can pass through a NISAR pass the k-set leaves out, as frame 151_A_011's chain passes through cycle 7,
which the DISP-NI smoke test's frame database excludes.
"""

from data_subscriber.gslc.gunw_utils import (build_gunw_native_id_patterns, gunw_chain, parse_gunw_granule_id,
                                             select_gunws)

FRAME = "151_A_011"


def gunw_id(reference_cycle, secondary_cycle, nisar_frame=FRAME, mode="4000_SH", crid="P05023", counter="001"):
    track, direction, frame = nisar_frame.split("_")
    return (f"NISAR_L2_PR_GUNW_{reference_cycle:03d}_{track}_{direction}_{frame}_{secondary_cycle:03d}_{mode}_"
            f"20251108T155041_20251108T155058_20251120T155041_20251120T155058_{crid}_N_P_J_{counter}")


def gunws(*pairs, **kwargs):
    return [parse_gunw_granule_id(gunw_id(reference, secondary, **kwargs)) for reference, secondary in pairs]


def cycle_pairs(chain):
    return [(g.reference_cycle, g.secondary_cycle) for g in chain]


def test_a_granule_id_is_parsed_into_its_cycles_frame_and_mode():
    gunw = parse_gunw_granule_id(
        "NISAR_L2_PR_GUNW_006_151_A_011_007_4000_SH_20251202T155042_20251202T155059_20251214T155042_"
        "20251214T155059_P05023_N_P_J_001.h5")

    assert (gunw.reference_cycle, gunw.secondary_cycle) == (6, 7)
    assert gunw.nisar_frame == FRAME
    assert gunw.mode_polarization == "4000_SH"
    assert (gunw.reference_start.isoformat(), gunw.secondary_start.isoformat()) == \
        ("2025-12-02T15:50:42", "2025-12-14T15:50:42")


def test_consecutive_dates_get_n_minus_1_gunws():
    available = gunws((4, 5), (5, 6), (6, 7), (7, 8))

    assert cycle_pairs(gunw_chain(available, FRAME, [4, 5, 6], "4000_SH")) == [(4, 5), (5, 6)]


def test_the_chain_passes_through_a_date_the_k_set_leaves_out():
    available = gunws((4, 5), (5, 6), (6, 7), (7, 8), (8, 9))

    assert cycle_pairs(gunw_chain(available, FRAME, [4, 5, 6, 8, 9], "4000_SH")) == \
        [(4, 5), (5, 6), (6, 7), (7, 8), (8, 9)]


def test_a_gunw_spanning_a_missed_pass_links_the_dates_on_either_side():
    # Frame 151_A_011 has no pass on cycles 16 and 17, so NISAR paired cycle 15 with 18
    available = gunws((14, 15), (15, 18), (18, 19))

    assert cycle_pairs(gunw_chain(available, FRAME, [14, 15, 18, 19], "4000_SH")) == [(14, 15), (15, 18), (18, 19)]


def test_a_missing_link_leaves_the_k_set_without_gunws():
    available = gunws((4, 5), (6, 7))

    assert gunw_chain(available, FRAME, [4, 5, 6, 7], "4000_SH") is None


def test_the_shortest_hop_is_taken_and_a_dead_end_is_backed_out_of():
    # 5->6 is the shorter hop but leads nowhere; 5->7 completes the chain
    available = gunws((4, 5), (5, 6), (5, 7), (7, 8))

    assert cycle_pairs(gunw_chain(available, FRAME, [4, 5, 8], "4000_SH")) == [(4, 5), (5, 7), (7, 8)]


def test_a_gunw_overshooting_the_next_k_set_date_is_not_used():
    available = gunws((4, 6))

    assert gunw_chain(available, FRAME, [4, 5, 6], "4000_SH") is None


def test_a_reprocessed_gunw_replaces_the_original():
    original = parse_gunw_granule_id(gunw_id(4, 5, counter="001"))
    reprocessed = parse_gunw_granule_id(gunw_id(4, 5, counter="002"))

    assert gunw_chain([original, reprocessed], FRAME, [4, 5], "4000_SH") == [reprocessed]


def test_other_modes_and_frames_are_not_mixed_into_the_chain():
    available = gunws((4, 5)) + gunws((5, 6), mode="2000_SH") + gunws((5, 6), nisar_frame="151_A_012")

    assert gunw_chain(available, FRAME, [4, 5, 6], "4000_SH") is None


def test_the_next_preferred_mode_is_used_when_the_first_has_no_chain():
    available = gunws((4, 5)) + gunws((4, 5), (5, 6), mode="2000_SH")

    mode_polarization, chain = select_gunws(available, {FRAME: [4, 5, 6]}, ["4000_SH", "2000_SH"])

    assert mode_polarization == "2000_SH"
    assert cycle_pairs(chain) == [(4, 5), (5, 6)]


def test_every_nisar_frame_of_the_disp_ni_frame_needs_a_chain_in_the_same_mode():
    available = gunws((4, 5), nisar_frame="071_D_100") + gunws((4, 5), nisar_frame="071_D_101", mode="2000_SH")
    cycles = {"071_D_100": [4, 5], "071_D_101": [4, 5]}

    assert select_gunws(available, cycles, ["4000_SH", "2000_SH"]) is None
    assert select_gunws(available + gunws((4, 5), nisar_frame="071_D_101"), cycles, ["4000_SH"])[0] == "4000_SH"


def test_no_preferred_mode_with_a_chain_selects_nothing():
    assert select_gunws([], {FRAME: [4, 5]}, ["4000_SH"]) is None


def test_the_query_asks_cmr_for_every_gunw_of_the_nisar_frames():
    assert build_gunw_native_id_patterns({"071_D_101", "071_D_100"}) == \
        ["NISAR_L2_*_GUNW_*_071_D_100_*", "NISAR_L2_*_GUNW_*_071_D_101_*"]
