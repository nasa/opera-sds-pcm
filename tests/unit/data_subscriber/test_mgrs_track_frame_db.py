import pytest
import os

from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB

@pytest.fixture(scope="module")
def mgrs_test_db_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data", "MGRS_collection_db_DSWx-NI_v0.1.sqlite")

@pytest.fixture(scope="module")
def mgrs_test_db(mgrs_test_db_path):
    return MGRSTrackFrameDB(mgrs_test_db_path)

@pytest.fixture
def frame_1_expected_sets():
    return [
        'MS_1_1', 'MS_2_1', 'MS_3_1', 'MS_4_1', 'MS_5_1', 'MS_6_1', 'MS_7_1', 'MS_8_1', 'MS_9_1', 'MS_10_1', 'MS_11_1', 'MS_12_1', 'MS_13_1', 'MS_14_1', 
        'MS_15_1', 'MS_16_1', 'MS_17_1', 'MS_18_1', 'MS_19_1', 'MS_20_1', 'MS_21_1', 'MS_22_1', 'MS_23_1', 'MS_24_1', 'MS_25_1', 'MS_26_1', 'MS_27_1', 'MS_28_1', 
        'MS_29_1', 'MS_30_1', 'MS_31_1', 'MS_32_1', 'MS_33_1', 'MS_34_1', 'MS_35_1', 'MS_36_1', 'MS_37_1', 'MS_38_1', 'MS_39_1', 'MS_40_1', 'MS_41_1', 'MS_42_1', 
        'MS_43_1', 'MS_44_1', 'MS_45_1', 'MS_46_1', 'MS_47_1', 'MS_48_1', 'MS_49_1', 'MS_50_1', 'MS_51_1', 'MS_52_1', 'MS_53_1', 'MS_54_1', 'MS_55_1', 'MS_56_1', 
        'MS_57_1', 'MS_58_1', 'MS_59_1', 'MS_60_1', 'MS_61_1', 'MS_62_1', 'MS_63_1', 'MS_64_1', 'MS_65_1', 'MS_66_1', 'MS_67_1', 'MS_68_1', 'MS_69_1', 'MS_70_1', 
        'MS_71_1', 'MS_72_1', 'MS_73_1', 'MS_74_1', 'MS_75_1', 'MS_76_1', 'MS_77_1', 'MS_78_1', 'MS_79_1', 'MS_80_1', 'MS_81_1', 'MS_82_1', 'MS_83_1', 'MS_84_1', 
        'MS_85_1', 'MS_86_1', 'MS_87_1', 'MS_88_1', 'MS_89_1', 'MS_90_1', 'MS_91_1', 'MS_92_1', 'MS_93_1', 'MS_94_1', 'MS_95_1', 'MS_96_1', 'MS_97_1', 'MS_98_1', 
        'MS_99_1', 'MS_100_1', 'MS_101_1', 'MS_102_1', 'MS_103_1', 'MS_104_1', 'MS_104_173', 'MS_104_174', 'MS_105_1', 'MS_106_1', 'MS_107_1', 'MS_108_1', 'MS_109_1', 
        'MS_110_1', 'MS_111_1', 'MS_112_1', 'MS_113_1', 'MS_114_1', 'MS_115_1', 'MS_116_1', 'MS_117_1', 'MS_118_1', 'MS_119_1', 'MS_120_1', 'MS_121_1', 'MS_122_1', 
        'MS_123_1', 'MS_124_1', 'MS_125_1', 'MS_126_1', 'MS_127_1', 'MS_128_1', 'MS_129_1', 'MS_130_1', 'MS_131_1', 'MS_132_1', 'MS_133_1', 'MS_134_1', 'MS_135_1', 
        'MS_136_1', 'MS_137_1', 'MS_138_1', 'MS_139_1', 'MS_140_1', 'MS_141_1', 'MS_142_1', 'MS_143_1', 'MS_144_1', 'MS_145_1', 'MS_146_1', 'MS_147_1', 'MS_148_1', 
        'MS_149_1', 'MS_150_1', 'MS_151_1', 'MS_152_1', 'MS_153_1', 'MS_154_1', 'MS_155_1', 'MS_156_1', 'MS_157_1', 'MS_158_1', 'MS_159_1', 'MS_160_1', 'MS_161_1', 
        'MS_162_1', 'MS_163_1', 'MS_164_1', 'MS_165_1', 'MS_166_1', 'MS_167_1', 'MS_168_1', 'MS_169_1', 'MS_170_1', 'MS_171_1', 'MS_172_1', 'MS_173_1'
    ]

@pytest.fixture
def frame_2_expected_sets():
    return [
        'MS_1_1', 'MS_1_2', 'MS_2_1', 'MS_2_2', 'MS_3_1', 'MS_3_2', 'MS_4_1', 'MS_4_2', 'MS_5_1', 'MS_5_2', 'MS_6_1', 'MS_6_2', 'MS_7_1', 'MS_7_2', 'MS_8_1', 
        'MS_8_2', 'MS_9_1', 'MS_9_2', 'MS_10_1', 'MS_10_2', 'MS_11_1', 'MS_11_2', 'MS_12_1', 'MS_12_2', 'MS_13_1', 'MS_13_2', 'MS_14_1', 'MS_14_2', 'MS_15_1', 
        'MS_15_2', 'MS_16_1', 'MS_16_2', 'MS_17_1', 'MS_17_2', 'MS_18_1', 'MS_18_2', 'MS_19_1', 'MS_19_2', 'MS_20_1', 'MS_20_2', 'MS_21_1', 'MS_21_2', 'MS_22_1', 
        'MS_22_2', 'MS_23_1', 'MS_23_2', 'MS_24_1', 'MS_24_2', 'MS_25_1', 'MS_25_2', 'MS_26_1', 'MS_26_2', 'MS_27_1', 'MS_27_2', 'MS_28_1', 'MS_28_2', 'MS_29_1', 
        'MS_29_2', 'MS_30_1', 'MS_30_2', 'MS_31_1', 'MS_31_2', 'MS_32_1', 'MS_32_2', 'MS_33_1', 'MS_33_2', 'MS_34_1', 'MS_34_2', 'MS_35_1', 'MS_35_2', 'MS_36_1', 
        'MS_36_2', 'MS_37_1', 'MS_37_2', 'MS_38_1', 'MS_38_2', 'MS_39_1', 'MS_39_2', 'MS_40_1', 'MS_40_2', 'MS_41_1', 'MS_41_2', 'MS_42_1', 'MS_42_2', 'MS_43_1', 
        'MS_43_2', 'MS_44_1', 'MS_44_2', 'MS_45_1', 'MS_45_2', 'MS_46_1', 'MS_46_2', 'MS_47_1', 'MS_47_2', 'MS_48_1', 'MS_48_2', 'MS_49_1', 'MS_49_2', 'MS_50_1', 
        'MS_50_2', 'MS_51_1', 'MS_51_2', 'MS_52_1', 'MS_52_2', 'MS_53_1', 'MS_53_2', 'MS_54_1', 'MS_54_2', 'MS_55_1', 'MS_55_2', 'MS_56_1', 'MS_56_2', 'MS_57_1', 
        'MS_57_2', 'MS_58_1', 'MS_58_2', 'MS_59_1', 'MS_59_2', 'MS_60_1', 'MS_60_2', 'MS_61_1', 'MS_61_2', 'MS_62_1', 'MS_62_2', 'MS_63_1', 'MS_63_2', 'MS_64_1', 
        'MS_64_2', 'MS_65_1', 'MS_65_2', 'MS_66_1', 'MS_66_2', 'MS_67_1', 'MS_67_2', 'MS_68_1', 'MS_68_2', 'MS_69_1', 'MS_69_2', 'MS_70_1', 'MS_70_2', 'MS_71_1', 
        'MS_71_2', 'MS_72_1', 'MS_72_2', 'MS_73_1', 'MS_73_2', 'MS_74_1', 'MS_74_2', 'MS_75_1', 'MS_75_2', 'MS_76_1', 'MS_76_2', 'MS_77_1', 'MS_77_2', 'MS_78_1', 
        'MS_78_2', 'MS_79_1', 'MS_79_2', 'MS_80_1', 'MS_80_2', 'MS_81_1', 'MS_81_2', 'MS_82_1', 'MS_82_2', 'MS_83_1', 'MS_83_2', 'MS_84_1', 'MS_84_2', 'MS_85_1', 
        'MS_85_2', 'MS_86_1', 'MS_86_2', 'MS_87_1', 'MS_87_2', 'MS_88_1', 'MS_88_2', 'MS_89_1', 'MS_89_2', 'MS_90_1', 'MS_90_2', 'MS_91_1', 'MS_91_2', 'MS_92_1', 
        'MS_92_2', 'MS_93_1', 'MS_93_2', 'MS_94_1', 'MS_94_2', 'MS_95_1', 'MS_95_2', 'MS_96_1', 'MS_96_2', 'MS_97_1', 'MS_97_2', 'MS_98_1', 'MS_98_2', 'MS_99_1', 
        'MS_99_2', 'MS_100_1', 'MS_100_2', 'MS_101_1', 'MS_101_2', 'MS_102_1', 'MS_102_2', 'MS_103_1', 'MS_103_2', 'MS_104_1', 'MS_104_2', 'MS_104_174', 'MS_105_1', 
        'MS_105_2', 'MS_106_1', 'MS_106_2', 'MS_107_1', 'MS_107_2', 'MS_108_1', 'MS_108_2', 'MS_109_1', 'MS_109_2', 'MS_110_1', 'MS_110_2', 'MS_111_1', 'MS_111_2', 
        'MS_112_1', 'MS_112_2', 'MS_113_1', 'MS_113_2', 'MS_114_1', 'MS_114_2', 'MS_115_1', 'MS_115_2', 'MS_116_1', 'MS_116_2', 'MS_117_1', 'MS_117_2', 'MS_118_1', 
        'MS_118_2', 'MS_119_1', 'MS_119_2', 'MS_120_1', 'MS_120_2', 'MS_121_1', 'MS_121_2', 'MS_122_1', 'MS_122_2', 'MS_123_1', 'MS_123_2', 'MS_124_1', 'MS_124_2', 
        'MS_125_1', 'MS_125_2', 'MS_126_1', 'MS_126_2', 'MS_127_1', 'MS_127_2', 'MS_128_1', 'MS_128_2', 'MS_129_1', 'MS_129_2', 'MS_130_1', 'MS_130_2', 'MS_131_1', 
        'MS_131_2', 'MS_132_1', 'MS_132_2', 'MS_133_1', 'MS_133_2', 'MS_134_1', 'MS_134_2', 'MS_135_1', 'MS_135_2', 'MS_136_1', 'MS_136_2', 'MS_137_1', 'MS_137_2', 
        'MS_138_1', 'MS_138_2', 'MS_139_1', 'MS_139_2', 'MS_140_1', 'MS_140_2', 'MS_141_1', 'MS_141_2', 'MS_142_1', 'MS_142_2', 'MS_143_1', 'MS_143_2', 'MS_144_1', 
        'MS_144_2', 'MS_145_1', 'MS_145_2', 'MS_146_1', 'MS_146_2', 'MS_147_1', 'MS_147_2', 'MS_148_1', 'MS_148_2', 'MS_149_1', 'MS_149_2', 'MS_150_1', 'MS_150_2', 
        'MS_151_1', 'MS_151_2', 'MS_152_1', 'MS_152_2', 'MS_153_1', 'MS_153_2', 'MS_154_1', 'MS_154_2', 'MS_155_1', 'MS_155_2', 'MS_156_1', 'MS_156_2', 'MS_157_1', 
        'MS_157_2', 'MS_158_1', 'MS_158_2', 'MS_159_1', 'MS_159_2', 'MS_160_1', 'MS_160_2', 'MS_161_1', 'MS_161_2', 'MS_162_1', 'MS_162_2', 'MS_163_1', 'MS_163_2', 
        'MS_164_1', 'MS_164_2', 'MS_165_1', 'MS_165_2', 'MS_166_1', 'MS_166_2', 'MS_167_1', 'MS_167_2', 'MS_168_1', 'MS_168_2', 'MS_169_1', 'MS_169_2', 'MS_170_1', 
        'MS_170_2', 'MS_171_1', 'MS_171_2', 'MS_172_1', 'MS_172_2', 'MS_173_1', 'MS_173_2'
    ]

def test_frame_number_to_mgrs_set_ids_frame_1(mgrs_test_db, frame_1_expected_sets):
    result = mgrs_test_db.frame_number_to_mgrs_set_ids(1)
    assert result == frame_1_expected_sets

def test_frame_number_to_mgrs_set_ids_frame_2(mgrs_test_db, frame_2_expected_sets):
    result = mgrs_test_db.frame_number_to_mgrs_set_ids(2)
    assert result == frame_2_expected_sets

def test_frame_number_to_mgrs_set_ids_frame_ms_1_1(mgrs_test_db):
    result = mgrs_test_db.mgrs_set_id_to_frames("MS_1_1")
    expected = set([1, 2, 3])
    assert result == expected

def test_frame_number_to_mgrs_set_ids_frame_ms_1_2(mgrs_test_db):
    result = mgrs_test_db.mgrs_set_id_to_frames("MS_1_2")
    expected = set([2, 3, 4])
    assert result == expected

def test_frame_number_to_mgrs_set_ids_frame_ms_104_174(mgrs_test_db):
    result = mgrs_test_db.mgrs_set_id_to_frames("MS_104_174")
    expected = set([1, 2, 176])
    assert result == expected

def test_frame_number_to_frame_set(mgrs_test_db):
    result = mgrs_test_db.frame_number_to_frame_set(1)
    expected = set([1, 2, 3, 176])
    assert result == expected

def test_frame_number_to_mgrs_sets_with_frames(mgrs_test_db, frame_1_expected_sets):
    result = mgrs_test_db.frame_number_to_mgrs_sets_with_frames(1)
    for mgrs_set in frame_1_expected_sets:
        if mgrs_set == "MS_104_174":
            assert result[mgrs_set] == set([1, 2, 176])
        elif mgrs_set == "MS_104_173":
            assert result[mgrs_set] == set([1, 176])
        else:
            assert result[mgrs_set] == set([1, 2, 3])