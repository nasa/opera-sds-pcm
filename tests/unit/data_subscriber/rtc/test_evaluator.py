from datetime import datetime, timedelta

import dateutil.parser
import pytest
from mock import patch

from data_subscriber.rtc import evaluator


def setup_module():
    # load GeoDataFrame ahead of tests for more comparable execution times
    import data_subscriber.rtc.mgrs_bursts_collection_db_client as client
    client.cached_load_mgrs_burst_db(filter_land=True)


def setup_function():
    pass


@pytest.mark.parametrize(
    "test_granule_id, expected_sets", [
        ("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0", {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
@patch("data_subscriber.rtc.evaluator.current_evaluation_datetime")
def test_grace_period__when_bursts_out_of_grace_period__then_kept_in_evaluator_results(
        evaluation_dt, es_conn_util, test_granule_id, expected_sets):
    # ARRANGE
    evaluation_dt.return_value = dateutil.parser.parse("2024-01-01T00:00:00")
    grace_period_mins = 2
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": test_granule_id,
                # 3-minute-old result, created BEFORE grace period window
                "creation_timestamp": (dateutil.parser.parse("2024-01-01T00:00:00") - timedelta(minutes=3)).isoformat(timespec="seconds"),
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]


    # ACT
    dummy_min_num_bursts = 0
    dummy_mgrs_set_id_acquisition_ts_cycle_indexes = None

    evaluator_results = evaluator.main(
        required_min_age_minutes_for_partial_burstsets=grace_period_mins,
        min_num_bursts=dummy_min_num_bursts,
        mgrs_set_id_acquisition_ts_cycle_indexes=dummy_mgrs_set_id_acquisition_ts_cycle_indexes
    )

    # ASSERT
    assert evaluator_results["mgrs_sets"] == expected_sets or evaluator_results["mgrs_sets"].keys() == expected_sets


@pytest.mark.parametrize(
    "test_granule_id, expected_sets", [
        ("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0", {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
@patch("data_subscriber.rtc.evaluator.current_evaluation_datetime")
def test_grace_period__when_bursts_in_grace_period__then_omitted_from_evaluator_results(
        evaluation_dt, es_conn_util, test_granule_id, expected_sets):
    # ARRANGE
    evaluation_dt.return_value = dateutil.parser.parse("2024-01-01T00:00:00")
    grace_period_mins = 2
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": test_granule_id,
                # 1-minute-old result, created WITHIN grace period window
                "creation_timestamp": (dateutil.parser.parse("2024-01-01T00:00:00") - timedelta(minutes=1)).isoformat(timespec="seconds"),
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]

    # ACT
    dummy_min_num_bursts = 0
    dummy_mgrs_set_id_acquisition_ts_cycle_indexes = None

    evaluator_results = evaluator.main(
        required_min_age_minutes_for_partial_burstsets=grace_period_mins,
        min_num_bursts=dummy_min_num_bursts,
        mgrs_set_id_acquisition_ts_cycle_indexes=dummy_mgrs_set_id_acquisition_ts_cycle_indexes
    )

    # ASSERT
    assert evaluator_results["mgrs_sets"] == {}


@pytest.mark.parametrize(
    "test_granule_id, expected_sets", [
        ("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0", {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
@patch("data_subscriber.rtc.evaluator.current_evaluation_datetime")
def test_coverage_target__when_bursts_out_of_grace_period__then_kept_in_evaluator_results(
        evaluation_dt, es_conn_util, test_granule_id, expected_sets):
    # ARRANGE
    evaluation_dt.return_value = dateutil.parser.parse("2024-01-01T00:00:00")
    grace_period_mins = 2
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": test_granule_id,
                # 3-minute-old result, created BEFORE grace period window
                "creation_timestamp": (dateutil.parser.parse("2024-01-01T00:00:00") - timedelta(minutes=3)).isoformat(timespec="seconds"),
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]


    # ACT
    dummy_coverage_target = 0
    dummy_mgrs_set_id_acquisition_ts_cycle_indexes = None

    evaluator_results = evaluator.main(
        required_min_age_minutes_for_partial_burstsets=grace_period_mins,
        coverage_target=dummy_coverage_target,
        mgrs_set_id_acquisition_ts_cycle_indexes=dummy_mgrs_set_id_acquisition_ts_cycle_indexes
    )

    # ASSERT
    assert evaluator_results["mgrs_sets"] == expected_sets or evaluator_results["mgrs_sets"].keys() == expected_sets


@pytest.mark.parametrize(
    "test_granule_id, expected_sets", [
        ("OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0", {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
@patch("data_subscriber.rtc.evaluator.current_evaluation_datetime")
def test_coverage_target__when_bursts_in_grace_period__then_omitted_from_evaluator_results(
        evaluation_dt, es_conn_util, test_granule_id, expected_sets):
    # ARRANGE
    evaluation_dt.return_value = dateutil.parser.parse("2024-01-01T00:00:00")
    grace_period_mins = 2
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": test_granule_id,
                # 1-minute-old result, created WITHIN grace period window
                "creation_timestamp": (dateutil.parser.parse("2024-01-01T00:00:00") - timedelta(minutes=1)).isoformat(timespec="seconds"),
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]

    # ACT
    dummy_coverage_target = 0
    dummy_mgrs_set_id_acquisition_ts_cycle_indexes = None
    evaluator_results = evaluator.main(
        required_min_age_minutes_for_partial_burstsets=grace_period_mins,
        coverage_target=dummy_coverage_target,
        mgrs_set_id_acquisition_ts_cycle_indexes=dummy_mgrs_set_id_acquisition_ts_cycle_indexes
    )

    # ASSERT
    assert evaluator_results["mgrs_sets"] == {}


@pytest.mark.parametrize(
    "test_coverage_target, expected_sets", [
        (100, set()),
        (1, {"MS_20_29", "MS_20_30"}),
        (0, {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
def test_native_id__when_coverage_target_A__and_bursts_found_B(es_conn_util, test_coverage_target, expected_sets):
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": "OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0",
                "creation_timestamp": "2024-01-01T00:00:00",
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]
    evaluator_results = evaluator.main(coverage_target=test_coverage_target, mgrs_set_id_acquisition_ts_cycle_indexes={"dummy"})
    assert evaluator_results["mgrs_sets"] == expected_sets or evaluator_results["mgrs_sets"].keys() == expected_sets


@pytest.mark.parametrize(
    "test_min_num_bursts, expected_sets", [
        (2, set()),
        (1, {"MS_20_29", "MS_20_30"}),
        (0, {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
def test_native_id__when_min_bursts_A__and__bursts_found_B(es_conn_util, test_min_num_bursts, expected_sets):
    es_conn_util.get_es_connection().query.return_value = [
        {
            "_source": {
                "granule_id": "OPERA_L2_RTC-S1_T020-041121-IW1_20231101T013115Z_20231104T041913Z_S1A_30_v1.0",
                "creation_timestamp": "2024-01-01T00:00:00",
                "mgrs_set_id_acquisition_ts_cycle_index": "dummy"
            }
        }
    ]
    evaluator_results = evaluator.main(min_num_bursts=test_min_num_bursts, coverage_target=None)
    assert evaluator_results["mgrs_sets"] == expected_sets or evaluator_results["mgrs_sets"].keys() == expected_sets
