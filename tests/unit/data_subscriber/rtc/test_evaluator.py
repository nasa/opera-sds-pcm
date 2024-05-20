import pytest
from mock import patch


def setup_module():
    # load GeoDataFrame ahead of tests for more comparable execution times
    import data_subscriber.rtc.mgrs_bursts_collection_db_client as client
    client.cached_load_mgrs_burst_db(filter_land=True)


def setup_function():
    pass


@pytest.mark.parametrize(
    "test_coverage_target, expected_sets", [
        (100, set()),
        (1, {"MS_20_29", "MS_20_30"}),
        (0, {"MS_20_29", "MS_20_30"}),
    ]
)
@patch("data_subscriber.rtc.evaluator.es_conn_util")
def test_native_id__when_coverage_target_A__and_bursts_found_B(es_conn_util, test_coverage_target, expected_sets):
    import data_subscriber.rtc.evaluator as evaluator
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
    import data_subscriber.rtc.evaluator as evaluator
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
