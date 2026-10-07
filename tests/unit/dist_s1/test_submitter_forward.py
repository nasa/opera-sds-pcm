import logging
from unittest.mock import patch

from pytest import fail

from dist_s1 import submitter_forward
from util.conf_util import SettingsConf
from tests.unit.dist_s1.gap_finder_test_input import lookup_unique
from dist_s1.gap_finder import is_pair_disjoint


@patch("dist_s1.submitter_forward.gap_finder")
@patch("dist_s1.submitter_forward.dao")
def test_evaluate_no_gap(mock_dao, mock_gf, caplog):
    # ARRANGE
    submitter_forward.logger = logging.getLogger(__file__)
    submitter_forward.logger.setLevel(logging.DEBUG)

    submitter_forward.settings = SettingsConf().cfg

    mock_gf.dist_s1_lookup_tile_to_agns = lookup_unique
    mock_gf.is_pair_disjoint = is_pair_disjoint

    mock_dao.query_submittable_null_state_configs.return_value = [
        {"metadata": {"tile_id": "60UXB"}}
    ]

    mock_dao.query_state_configs_by_tile.return_value = [  # intentionally reverse sorted
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 3, "batch_id": "60UXB_3_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 2, "batch_id": "60UXB_2_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 1, "batch_id": "60UXB_1_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 0, "batch_id": "60UXB_0_300"}},
    ]

    # ACT
    with caplog.at_level(logging.DEBUG):
        results = submitter_forward.evaluate(filter_tile_id="60UXB")

    # ASSERT
    assert "60UXB" in results["tiles_gapless"]
    for record in caplog.records:
        if "No errors while performing gap check." in record.message:
            break
    else:
        fail("Should not reach here. Gap likely found in no-gap scenario.")


@patch("dist_s1.submitter_forward.gap_finder")
@patch("dist_s1.submitter_forward.dao")
def test_evaluate_gap(mock_dao, mock_gf, caplog):
    # ARRANGE
    submitter_forward.logger = logging.getLogger(__file__)
    submitter_forward.logger.setLevel(logging.DEBUG)

    submitter_forward.settings = SettingsConf().cfg

    mock_gf.dist_s1_lookup_tile_to_agns = lookup_unique
    mock_gf.is_pair_disjoint = is_pair_disjoint

    mock_dao.query_submittable_null_state_configs.return_value = [
        {"metadata": {"tile_id": "60UXB"}}
    ]

    mock_dao.query_state_configs_by_tile.return_value = [  # intentionally reverse sorted
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 3, "batch_id": "60UXB_3_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 2, "batch_id": "60UXB_2_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 1, "batch_id": "60UXB_1_300"}},
        {"metadata": {"tile_id": "60UXB","aci": 300,"agn": 0, "batch_id": "60UXB_0_300"}},
    ]

    # induce gap
    del mock_dao.query_state_configs_by_tile.return_value[1]

    # ACT
    with caplog.at_level(logging.INFO):
        results = submitter_forward.evaluate(filter_tile_id="60UXB")

    # ASSERT
    assert "60UXB" in results["tiles_gapped"]
    for record in caplog.records:
        if "pair disjoint" in record.message:
            break
    else:
        fail("Should have raised an error. Expected gap no found in gap check.")
