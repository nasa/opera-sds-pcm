"""Exact-second dedup of RTC granules (unique_latest_granules_by_second) and its use for baseline granules."""
from datetime import datetime

from data_subscriber.rtc_for_dist.baseline_granule_retriever import unique_latest_granules_by_second


def _decorated_granule(burst_id, acquisition_ts, production_ts):
    granule_id = f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_ts:%Y%m%dT%H%M%S}Z_{production_ts:%Y%m%dT%H%M%S}Z_S1A_30_v1.0"
    return {"granule_id": granule_id, "burst_id": burst_id,
            "acquisition_ts": acquisition_ts, "production_ts": production_ts}


def test_unique_latest_granules_by_second__when_same_second__then_keeps_latest_production():
    older = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    newer = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))

    assert unique_latest_granules_by_second([older, newer]) == [newer]
    assert unique_latest_granules_by_second([newer, older]) == [newer]


def test_unique_latest_granules_by_second__when_same_minute_but_different_second__then_keeps_both():
    # the minute rule (unique_latest_granules) would keep only the first one
    a = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))
    b = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 57), datetime(2026, 1, 2))

    assert unique_latest_granules_by_second([a, b]) == [a, b]


def test_unique_latest_granules_by_second__when_different_bursts__then_keeps_both():
    a = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    b = _decorated_granule("T123-123456-IW2", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))

    assert unique_latest_granules_by_second([a, b]) == [a, b]


def test_unique_latest_granules_by_second__when_production_ts_is_str():
    older = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    newer = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))
    older["production_ts"] = "2026-01-02T00:00:00"
    newer["production_ts"] = "2026-01-03T00:00:00"

    assert unique_latest_granules_by_second([older, newer]) == [newer]
