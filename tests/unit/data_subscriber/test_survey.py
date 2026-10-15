from datetime import datetime

from data_subscriber.survey import _parse_cmr_datetime


def test__parse_cmr_datetime__with_microseconds():
    result = _parse_cmr_datetime("2026-08-06T16:13:16.927Z")

    assert result == datetime(2026, 8, 6, 16, 13, 16, 927000)


def test__parse_cmr_datetime__without_microseconds():
    result = _parse_cmr_datetime("2025-11-03T23:23:39Z")

    assert result == datetime(2025, 11, 3, 23, 23, 39)
