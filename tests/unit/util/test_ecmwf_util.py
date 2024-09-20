
from collections import namedtuple
from datetime import datetime
from unittest.mock import patch, MagicMock

import botocore.client
import botocore.exceptions
import pytest

from util.ecmwf_util import (check_s3_for_ecmwf,
                             ecmwf_key_for_datetime,
                             find_ecmwf_for_datetime)


def test_check_s3_for_ecmwf(caplog):
    # Test with valid result from s3_client.head_object()
    mock_head_object = MagicMock()

    with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
        assert check_s3_for_ecmwf("s3://opera-ancillaries/ecmwf/20230202/D02020000020200001.subset.zz.nc")
        mock_head_object.assert_called()

    # Test with 404 not found result from s3_client.head_object()
    mock_head_object = MagicMock(
        side_effect=botocore.exceptions.ClientError({"Error": {"Code": "404"}}, "head_object")
    )

    with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
        assert check_s3_for_ecmwf("s3://opera-ancillaries/ecmwf/20230202/D02020000020200001.subset.zz.nc") == False
        mock_head_object.assert_called()
        assert "ECMWF file ecmwf/20230202/D02020000020200001.subset.zz.nc does not exist in bucket opera-ancillaries" in caplog.text

    # Test with an unexpected error result from s3_client.head_object()
    mock_head_object = MagicMock(side_effect=ValueError("Test Value Error"))

    with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
        with pytest.raises(ValueError):
            check_s3_for_ecmwf("s3://opera-ancillaries/ecmwf/20230202/D02020000020200001.subset.zz.nc")

        mock_head_object.assert_called()


def test_ecmwf_key_for_datetime():
    TestCase = namedtuple('TestCase', ['datetime', 'expected_ecmwf_key'])

    # Setup test cases that hit each 6 hour time quadrant
    test_cases = [
        TestCase("20230202T000000", "20230202/D02020000020200001.subset.zz.nc"),
        TestCase("20230202T031035", "20230202/D02020000020200001.subset.zz.nc"),
        TestCase("20240101T060000", "20240101/D01010600010106001.subset.zz.nc"),
        TestCase("20240101T103022", "20240101/D01010600010106001.subset.zz.nc"),
        TestCase("20220822T120000", "20220822/D08221200082212001.subset.zz.nc"),
        TestCase("20220822T151515", "20220822/D08221200082212001.subset.zz.nc"),
        TestCase("20210615T180000", "20210615/D06151800061518001.subset.zz.nc"),
        TestCase("20211109T212121", "20211109/D11091800110918001.subset.zz.nc")
    ]

    for test_case in test_cases:
        dt = datetime.strptime(test_case.datetime, "%Y%m%dT%H%M%S")

        ecmwf_key = ecmwf_key_for_datetime(dt)

        assert ecmwf_key == test_case.expected_ecmwf_key


def test_find_ecmwf_for_datetime():
    TestCase = namedtuple('TestCase', ['datetime', 'expected_ecmwf_uri'])

    # Setup test cases that hit each 6 hour time quadrant
    test_cases = [
        TestCase("20230202T000000", "s3://opera-ancillaries/ecmwf/20230202/D02020000020200001.subset.zz.nc"),
        TestCase("20240101T060000", "s3://opera-ancillaries/ecmwf/20240101/D01010600010106001.subset.zz.nc"),
        TestCase("20220822T120000", "s3://opera-ancillaries/ecmwf/20220822/D08221200082212001.subset.zz.nc"),
        TestCase("20210615T180000", "s3://opera-ancillaries/ecmwf/20210615/D06151800061518001.subset.zz.nc"),
    ]

    # Mock a valid response from s3_client.head_object()
    mock_head_object = MagicMock()

    with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
        for test_case in test_cases:
            dt = datetime.strptime(test_case.datetime, "%Y%m%dT%H%M%S")

            ecmwf_uri = find_ecmwf_for_datetime(dt)

            assert ecmwf_uri == test_case.expected_ecmwf_uri

    # Mock a 404 not found response from s3_client.head_object()
    mock_head_object = MagicMock(
        side_effect=botocore.exceptions.ClientError({"Error": {"Code": "404"}}, "head_object")
    )

    with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
        for test_case in test_cases:
            dt = datetime.strptime(test_case.datetime, "%Y%m%dT%H%M%S")

            ecmwf_uri = find_ecmwf_for_datetime(dt)

            assert ecmwf_uri is None
