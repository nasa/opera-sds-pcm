"""DISP-NI frame database loading and NISAR GSLC naming.

The frame database drives DISP-NI historical processing, and every GSLC the query finds is keyed
by fields parsed from its granule id. A pattern that does not match real NISAR names (the old
L2_GSLC_NI pattern expected an "F" before the frame number) would leave nothing to process.
"""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber.cslc_utils import split_download_batch_id
from data_subscriber.gslc.gslc_utils import (NisarFrame, build_gslc_native_id_patterns, gslc_download_batch_id,
                                             gslc_unique_id, parse_gslc_granule_id, process_disp_ni_frame_db)
from util.conf_util import SettingsConf

TEST_DATA = Path(__file__).parent / "test_data"
DISP_NI_DB = str(TEST_DATA / "disp_ni_consistent_db.json")

# The GSLC sample named in the DISP-NI ICS
ICS_SAMPLE_GSLC = "NISAR_L2_PR_GSLC_004_151_A_011_4005_DHDH_A_20251108T155041_20251108T155058_X05009_N_P_J_001"


def test_frame_database_maps_each_frame_to_its_nisar_frames_and_sorted_dates():
    frame_db = process_disp_ni_frame_db(DISP_NI_DB)

    frame = frame_db[26410]
    assert frame.frame_number == 26410
    assert frame.nisar_frames == {"151_A_011"}
    assert frame.sensing_datetimes[0] == datetime(2025, 11, 8, 15, 50, 41)
    assert frame.sensing_datetimes == sorted(frame.sensing_datetimes)
    assert len(frame.sensing_datetimes) == 36

    assert frame_db[12419].nisar_frames == {"071_D_100", "071_D_101"}
    assert frame_db[30000].sensing_datetimes == []


def test_frame_database_sorts_dates_listed_out_of_order(tmp_path):
    db_file = tmp_path / "db.json"
    db_file.write_text(json.dumps({"data": {"5": {"nisar_frame_list": ["001_A_005"], "sensing_time_list": [
        "2026-01-07T15:50:44", "2025-11-08T15:50:41", "2025-12-02T15:50:42"]}}}))

    frame = process_disp_ni_frame_db(str(db_file))[5]

    assert frame.sensing_datetimes == [datetime(2025, 11, 8, 15, 50, 41), datetime(2025, 12, 2, 15, 50, 42),
                                       datetime(2026, 1, 7, 15, 50, 44)]


@pytest.mark.parametrize("granule_id", [ICS_SAMPLE_GSLC, ICS_SAMPLE_GSLC + ".h5"], ids=["granule_id", "filename"])
def test_granule_id_parses_into_cycle_nisar_frame_mode_and_sensing_start(granule_id):
    gslc = parse_gslc_granule_id(granule_id)

    assert gslc.cycle == 4
    assert gslc.track == 151
    assert gslc.direction == "A"
    assert gslc.nisar_frame == "151_A_011"
    assert gslc.mode == "4005"
    assert gslc.polarization == "DHDH"
    assert gslc.mode_polarization == "4005_DHDH"
    assert gslc.sensing_start == datetime(2025, 11, 8, 15, 50, 41)


def test_granule_ids_in_quad_and_single_pol_modes_parse_too():
    """The North America survey has 2005_QPDH and 0005_NADV granules, so the query must not choke on them."""
    assert parse_gslc_granule_id(ICS_SAMPLE_GSLC.replace("4005_DHDH", "2005_QPDH")).polarization == "QPDH"
    assert parse_gslc_granule_id(ICS_SAMPLE_GSLC.replace("4005_DHDH", "0005_NADV")).mode_polarization == "0005_NADV"


def test_a_granule_id_that_is_not_a_gslc_is_rejected():
    with pytest.raises(ValueError):
        parse_gslc_granule_id(ICS_SAMPLE_GSLC.replace("_GSLC_", "_GCOV_"))


def test_settings_pattern_matches_the_ics_sample_filename():
    """Regression: the frame group used to be F\\d{3}, which no real NISAR filename matches."""
    pattern = SettingsConf().cfg["PRODUCT_TYPES"]["L2_GSLC_NI"]["Pattern"]
    pattern = re.compile(pattern) if isinstance(pattern, str) else pattern

    match = pattern.match(ICS_SAMPLE_GSLC + ".h5")

    assert match is not None
    assert match.group("nisar_frame") == "011"


def test_native_id_patterns_cover_every_nisar_frame_of_the_frame():
    frame = NisarFrame(frame_number=12419, nisar_frames={"071_D_101", "071_D_100"})

    assert build_gslc_native_id_patterns(frame) == ["NISAR_L2_*_GSLC_*_071_D_100_*", "NISAR_L2_*_GSLC_*_071_D_101_*"]


def test_native_id_pattern_matches_the_ics_sample():
    pattern = build_gslc_native_id_patterns(NisarFrame(frame_number=26410, nisar_frames={"151_A_011"}))[0]

    assert re.fullmatch(pattern.replace("*", ".*"), ICS_SAMPLE_GSLC)


def test_batch_ids_split_like_disp_s1_batch_ids():
    batch_id = gslc_download_batch_id(26410, 4)

    assert batch_id == "f26410_a4"
    assert split_download_batch_id(batch_id) == (26410, 4)
    assert gslc_unique_id(batch_id, "151_A_011") == "f26410_a4_151_A_011"
