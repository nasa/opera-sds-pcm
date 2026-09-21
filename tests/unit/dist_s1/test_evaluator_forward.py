import logging
from collections import namedtuple
from unittest.mock import patch, MagicMock

from pytest import fail

from dist_s1 import evaluator_forward
from dist_s1.evaluator_forward import Evaluator
from util.conf_util import SettingsConf


@patch("dist_s1.evaluator_forward.dao")
def test_evaluate_single_batch__when_state_config_missing(mock_dao):
    # ARRANGE
    evaluator_forward.logger = logging.getLogger(__file__)
    evaluator_forward.logger.setLevel(logging.DEBUG)

    mock_dao.query_state_config.return_value = None

    # ACT
    evaluator = Evaluator()
    results = evaluator.evaluate_single_batch(batch_id="dummy_batch_id")

    # ASSERT
    assert "state_config" not in results


@patch("dist_s1.evaluator_forward.dao")
def test_evaluate_single_batch__when_state_config__and_status_not_null__then_skipped(mock_dao):
    # ARRANGE
    evaluator_forward.logger = logging.getLogger(__file__)
    evaluator_forward.logger.setLevel(logging.DEBUG)

    mock_dao.query_state_config.return_value = {"metadata": {"status": "DUMMY_NOT_NULL"}}

    # ACT
    evaluator = Evaluator()
    results = evaluator.evaluate_single_batch(batch_id="dummy_batch_id")

    # ASSERT
    assert "skipped" in results


@patch("dist_s1.evaluator_forward.get_grq_es")
@patch("dist_s1.evaluator_forward.dao")
def test_evaluate_single_batch__when_state_config__and_status_null__and_current_granules_missing(mock_dao, mock_get_grq_es):
    # ARRANGE
    evaluator_forward.logger = logging.getLogger(__file__)
    evaluator_forward.logger.setLevel(logging.DEBUG)

    evaluator_forward.settings = SettingsConf().cfg

    mock_dao.query_state_config.return_value = {"metadata": {"status": "NULL", "rtc_granule_ids": []}}
    mock_get_grq_es.search.return_value = {"hits":{"hits":[{"_source":{}}]}}

    # ACT
    evaluator = Evaluator()
    results = evaluator.evaluate_single_batch(batch_id="dummy_batch_id")

    # ASSERT
    assert "batches_covered" in results
    assert not results["batches_covered"]


@patch("dist_s1.evaluator_forward.RtcBatchEvaluator")
@patch("dist_s1.evaluator_forward.DistDependency")
@patch("dist_s1.evaluator_forward.BaselineGranuleRetriever")
@patch("dist_s1.evaluator_forward.get_grq_es")
@patch("dist_s1.evaluator_forward.dao")
def test_evaluate_single_batch__when_state_config__and_status_null__and_current_granules__but_no_baseline_granules(
        mock_dao,
        mock_get_grq_es,
        mock_BaselineGranuleRetriever,
        mock_DistDependency,
        mock_RtcBatchEvaluator,
        caplog
):
    # ARRANGE
    evaluator_forward.logger = logging.getLogger(__file__)
    evaluator_forward.logger.setLevel(logging.DEBUG)

    evaluator_forward.settings = SettingsConf().cfg

    mock_dao.query_state_config.return_value = {"metadata": {"status": "NULL", "rtc_granule_ids": [], "k_offsets_counts":"[(365, 4), (730, 3), (1095, 3)]"}}
    mock_grq_es = MagicMock()
    mock_grq_es.search.return_value = {"hits":{"hits":[
        {"_source":{"granule_id":"OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0"}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359428-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359429-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359430-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359430-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359431-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359431-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359432-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359432-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359433-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359433-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359434-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359434-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359435-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
        {"_source":{"granule_id":'OPERA_L2_RTC-S1_T168-359435-IW3_20231217T052415Z_20231220T055805Z_S1A_30_v1.0'}},
    ]}}
    mock_get_grq_es.return_value = mock_grq_es
    mock_BaselineGranuleRetriever.return_value.retrieve_baseline_granules_for_affected_batches.return_value = {
        "p33VUE_5_S1A_a302": []
    }

    Args = namedtuple("Args", ["batch_id"])
    args = Args(batch_id=1)
    evaluator_forward.args = args

    # ACT
    evaluator = Evaluator()
    with caplog.at_level(logging.DEBUG):
        results = evaluator.evaluate_single_batch(batch_id="33VUE_5_S1A_302")

    # ASSERT
    mock_BaselineGranuleRetriever.return_value.retrieve_baseline_granules_for_affected_batches.assert_called_once()
    mock_RtcBatchEvaluator.return_value.evaluate.assert_called_once()
    assert "batch_id_to_baseline" in results
    assert not results["batch_id_to_baseline"]["33VUE_5_S1A_302"]

    for record in caplog.records:
        if "No baseline granules found for " in record.message:
            break
    else:
        fail("Should have raised an error. Expected error regarding missing baseline granules.")


