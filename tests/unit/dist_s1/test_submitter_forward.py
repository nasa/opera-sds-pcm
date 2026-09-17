import logging
from collections import namedtuple
from unittest.mock import patch

from pytest import fail

from dist_s1 import submitter_forward


@patch("dist_s1.submitter_forward.dao")
def test_no_gap(mock_dao, caplog):
    # ARRANGE
    submitter_forward.logger = logging.getLogger(__file__)
    submitter_forward.logger.setLevel(logging.DEBUG)

    Args = namedtuple("Args", ["filter_tile_id"])
    args = Args(filter_tile_id=None)
    submitter_forward.args = args

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
        submitter_forward.run()

    # ASSERT
    for record in caplog.records:
        if "No errors while performing gap check." in record.message:
            break
    else:
        fail("Should not reach here. Gap likely found in no-gap scenario.")



@patch("dist_s1.submitter_forward.dao")
def test_gap(mock_dao, caplog):
    # ARRANGE
    submitter_forward.logger = logging.getLogger(__file__)
    submitter_forward.logger.setLevel(logging.DEBUG)

    Args = namedtuple("Args", ["filter_tile_id"])
    args = Args(filter_tile_id=None)
    submitter_forward.args = args

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
        submitter_forward.run()

    # ASSERT
    for record in caplog.records:
        if "pair disjoint" in record.message:
            break
    else:
        fail("Should have raised an error. Expected gap no found in gap check.")
