import pytest
from unittest.mock import MagicMock

from data_subscriber.gcov.gcov_granule_util import (
    extract_frame_id,
    extract_track_id, 
    extract_cycle_number,
    extract_frames_and_track_ids_from_granules
)


class TestExtractFrameId:
    
    def test_extract_frame_id_valid_granule(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        result = extract_frame_id(granule)
        assert result == 11
     
    def test_extract_frame_id_missing_granule_id(self):
        granule = {}
        with pytest.raises(KeyError):
            extract_frame_id(granule)
    
    def test_extract_frame_id_invalid_format(self):
        granule = {"granule_id": "INVALID_FORMAT"}
        with pytest.raises(IndexError):
            extract_frame_id(granule)
    
    def test_extract_frame_id_non_numeric(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_ABC_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        with pytest.raises(ValueError):
            extract_frame_id(granule)


class TestExtractTrackId:
    
    def test_extract_track_id_valid_granule(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        result = extract_track_id(granule)
        assert result == 156
    
    def test_extract_track_id_different_format(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_001_001_A_000_2000_SHNA_A_20240609T045403_20240609T045413_T00777_M_F_J_777"}
        result = extract_track_id(granule)
        assert result == 1
    
    def test_extract_track_id_missing_granule_id(self):
        granule = {}
        with pytest.raises(KeyError):
            extract_track_id(granule)
    
    def test_extract_track_id_invalid_format(self):
        granule = {"granule_id": "INVALID_FORMAT"}
        with pytest.raises(IndexError):
            extract_track_id(granule)
    
    def test_extract_track_id_non_numeric(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_015_ABC_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        with pytest.raises(ValueError):
            extract_track_id(granule)


class TestExtractCycleNumber:
    
    def test_extract_cycle_number_valid_granule(self):
        granule = {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        result = extract_cycle_number(granule)
        assert result == 15
    
    def test_extract_cycle_number_missing_granule_id(self):
        granule = {}
        with pytest.raises(KeyError):
            extract_cycle_number(granule)
    
    def test_extract_cycle_number_invalid_format(self):
        granule = {"granule_id": "INVALID_FORMAT"}
        with pytest.raises(IndexError):
            extract_cycle_number(granule)

class TestExtractFramesAndTrackIdsFromGranules:
    
    def test_extract_frames_and_track_ids_from_granules_empty_list(self):
        granules = []
        result = extract_frames_and_track_ids_from_granules(granules)
        assert result == set()
    
    def test_extract_frames_and_track_ids_from_granules_single_granule(self):
        granules = [{"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}]
        result = extract_frames_and_track_ids_from_granules(granules)
        assert result == {(11, 156)}
    
    def test_extract_frames_and_track_ids_from_granules_multiple_granules(self):
        granules = [
            {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"},
            {"granule_id": "NISAR_L2_PR_GCOV_001_001_A_000_2000_SHNA_A_20240609T045403_20240609T045413_T00777_M_F_J_777"},
            {"granule_id": "NISAR_L2_PR_GCOV_020_200_A_015_2010_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"}
        ]
        result = extract_frames_and_track_ids_from_granules(granules)
        expected = {(11, 156), (0, 1), (15, 200)}
        assert result == expected
    
    def test_extract_frames_and_track_ids_from_granules_duplicate_frames_tracks(self):
        granules = [
            {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"},
            {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000818_20230619T000836_T00406_M_P_J_002"}
        ]
        result = extract_frames_and_track_ids_from_granules(granules)
        # Should deduplicate to single entry
        assert result == {(11, 156)}
        assert len(result) == 1
    
    def test_extract_frames_and_track_ids_from_granules_invalid_granule(self):
        granules = [
            {"granule_id": "NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001"},
            {"granule_id": "INVALID_FORMAT"}
        ]
        with pytest.raises(IndexError):
            extract_frames_and_track_ids_from_granules(granules)