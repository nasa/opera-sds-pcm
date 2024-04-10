#!/usr/bin/env python3

import json
import os.path
import tempfile
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from requests import Response, Session

import tools.stage_orbit_file
from tools.stage_orbit_file import (ORBIT_TYPE_POE,
                                    ORBIT_TYPE_RES,
                                    NoSuitableOrbitFileException)


class TestStageOrbitFile(unittest.TestCase):
    """Unit tests for the stage_orbit_file.py script"""

    def setUp(self) -> None:
        # Create a temporary working directory
        self.working_dir = tempfile.TemporaryDirectory(suffix="_temp", prefix="test_stage_orbit_file_")

    def tearDown(self) -> None:
        self.working_dir.cleanup()

    def test_parse_orbit_range_from_safe(self):
        """Tests for the parse_orbit_range_from_safe() function"""
        # Typical case: name of a valid input SLC file
        test_safe_file_name = "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F.zip"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1B")
        self.assertEquals(safe_start_time, "20180504T104507")
        self.assertEquals(safe_stop_time, "20180504T104535")

        # Test with a full path to a SAFE file
        test_safe_file_name = "/tmp/S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC.zip"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1A")
        self.assertEquals(safe_start_time, "20220501T015035")
        self.assertEquals(safe_stop_time, "20220501T015102")

        # Test with no file extension
        test_safe_file_name = "/tmp/S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1A")
        self.assertEquals(safe_start_time, "20220501T015035")
        self.assertEquals(safe_stop_time, "20220501T015102")

        # Test with invalid file name
        test_safe_file_name = "S2A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC"

        with self.assertRaises(RuntimeError):
            tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

    def test_construct_orbit_file_query(self):
        """Tests for the construct_orbit_file_query() function"""
        mission_id = "S1A"
        search_start_time = "20220501T000000"
        search_stop_time = "20220501T000030"
        orbit_type = ORBIT_TYPE_POE

        query = tools.stage_orbit_file.construct_orbit_file_query(
            mission_id, orbit_type, search_start_time, search_stop_time
        )

        # Check that all portions of the query were constructed with the inputs
        # as expected
        self.assertIn("startswith(Name,'S1A')", query)
        self.assertIn("contains(Name,'AUX_POEORB')", query)
        self.assertIn("ContentDate/Start lt '2022-05-01T00:00:00.000000Z'", query)
        self.assertIn("ContentDate/End gt '2022-05-01T00:00:30.000000Z", query)

        # Test again for restituted orbit type
        orbit_type = ORBIT_TYPE_RES

        query = tools.stage_orbit_file.construct_orbit_file_query(
            mission_id, orbit_type, search_start_time, search_stop_time
        )
        "startswith(Name,'S1A') and contains(Name,'AUX_RESORB') and ContentDate/Start lt '2022-05-01T00:00:00.000000Z' and ContentDate/End gt '2022-05-01T00:00:30.000000Z'"

        self.assertIn("startswith(Name,'S1A')", query)
        self.assertIn("contains(Name,'AUX_RESORB')", query)
        self.assertIn("ContentDate/Start lt '2022-05-01T00:00:00.000000Z'", query)
        self.assertIn("ContentDate/End gt '2022-05-01T00:00:30.000000Z'", query)

    def test_select_orbit_file(self):
        """Tests for the select_orbit_file() function"""
        # Sample (fictional) partial JSON response derived for SLC granule
        # S1A_IW_SLC__1SDV_20230825T185042_20230825T185110_050036_060529_C8A2
        json_response = """
        {
          "value": [
            {
              "Id": "5ae6bb3b-0f19-41c0-ba1e-28988ed93842",
              "Name": "S1A_OPER_AUX_RESORB_OPOD_20230825T223851_V20230825T185010_20230825T220740.EOF"
            },
            {
              "Id": "b36a07ec-fc8a-444d-9270-dfd2b9463200",
              "Name": "S1A_OPER_AUX_RESORB_OPOD_20230825T205555_V20230825T171126_20230825T202856.EOF"
            },
            {
              "Id": "6c264b9c-6646-4e3d-92d6-862ef96ee2e8",
              "Name": "S1A_OPER_AUX_RESORB_OPOD_20230825T190955_V20230825T153241_20230825T185011.EOF"
            }
          ]
        }
        """

        input_granule = "S1A_IW_SLC__1SDV_20230825T185042_20230825T185110_050036_060529_C8A2"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(input_granule)

        query_results = json.loads(json_response)['value']

        # Start with the default time range for finding a single RESORB file:
        # [sensing_start - T_orbit - 1 min, sensing_end + 1 min]
        sensing_start_range = "20230825T171057"
        sensing_stop_range =  "20230825T185210"

        # This should result in no suitable orbit file selected
        with self.assertRaises(NoSuitableOrbitFileException):
            tools.stage_orbit_file.select_orbit_file(
                query_results, sensing_start_range, sensing_stop_range
            )

        # Now use the two ranges used to find consecutive RESORB files that
        # the SAS will concatenate together
        # The first range is [sensing_start - 1 min, sensing_end + 1 min]
        sensing_start_range = "20230825T184942"
        sensing_stop_range = "20230825T185210"

        orbit_file_name, orbit_file_request_id = tools.stage_orbit_file.select_orbit_file(
            query_results, sensing_start_range, sensing_stop_range
        )

        expected_orbit_file = "S1A_OPER_AUX_RESORB_OPOD_20230825T205555_V20230825T171126_20230825T202856.EOF"
        expected_orbit_file_request_id = "b36a07ec-fc8a-444d-9270-dfd2b9463200"

        self.assertEqual(orbit_file_name, expected_orbit_file)
        self.assertEqual(orbit_file_request_id, expected_orbit_file_request_id)

        # The second range is [sensing_start – T_orb – 1 min, sensing_start – T_orb + 1 min]
        sensing_start_range = "20230825T171057"
        sensing_stop_range = "20230825T171257"

        orbit_file_name, orbit_file_request_id = tools.stage_orbit_file.select_orbit_file(
            query_results, sensing_start_range, sensing_stop_range
        )

        expected_orbit_file = "S1A_OPER_AUX_RESORB_OPOD_20230825T190955_V20230825T153241_20230825T185011.EOF"
        expected_orbit_file_request_id = "6c264b9c-6646-4e3d-92d6-862ef96ee2e8"

        self.assertEqual(orbit_file_name, expected_orbit_file)
        self.assertEqual(orbit_file_request_id, expected_orbit_file_request_id)

    def test_download_orbit_file_retry(self):
        """Tests the backoff/retry logic assigned to all HTTP request functions in stage_orbit_file.py"""
        # Set up some canned HTTP responses for the transient error codes we retry for
        response_401 = Response()
        response_401.status_code = 401
        response_429 = Response()
        response_429.status_code = 429
        response_500 = Response()
        response_500.status_code = 500
        response_503 = Response()
        response_503.status_code = 503
        response_504 = Response()
        response_504.status_code = 504
        response_200 = Response()

        # Create the successful response
        response_200.status_code = 200
        response_200.raw = BytesIO(b'orbit file contents')

        responses = [response_401, response_429, response_500, response_503, response_504, response_200]

        # Set up a Mock function for session.get which will cycle through all
        # transient error codes before finally returning success (200)
        mock_requests_get = MagicMock(side_effect=responses)

        with patch.object(Session, "get", mock_requests_get):
            tools.stage_orbit_file.download_orbit_file(
                'http://fakeurl.com', self.working_dir.name, 'orbit_file.EOF', 'token'
            )

        # Ensure we retired for each of the failed responses
        self.assertEqual(mock_requests_get.call_count, len(responses))

        # Ensure the orbit file was "downloaded" to disk after the successful response
        self.assertTrue(os.path.exists(os.path.join(self.working_dir.name, 'orbit_file.EOF')))

        # Ensure the "downloaded" contents match what is expected
        with open(os.path.join(self.working_dir.name, 'orbit_file.EOF'), 'rb') as infile:
            orbit_file_contents = infile.read()

        self.assertEqual(b'orbit file contents', orbit_file_contents)
