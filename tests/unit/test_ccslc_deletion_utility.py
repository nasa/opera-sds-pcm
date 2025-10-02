#!/usr/bin/env python3
"""
Unit tests for CCSLC Deletion Utility

Tests the core functionality of the CCSLC deletion utility including:
- Input validation
- Object discovery
- Deletion operations
- Error handling
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import tempfile
import os
import sys

# Add the tools directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tools.ccslc_deletion_utility import CCSLCDeletionUtility


class TestCCSLCDeletionUtility(unittest.TestCase):
    """Test cases for CCSLCDeletionUtility class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the settings and dependencies
        with patch("tools.ccslc_deletion_utility.SettingsConf") as mock_settings:
            mock_settings.return_value.cfg = {"LTS_BUCKET": "test-lts-bucket"}

            with patch(
                "tools.ccslc_deletion_utility.localize_disp_frame_burst_hist"
            ) as mock_hist:
                mock_hist.return_value = (
                    {10859: Mock(), 10860: Mock()},  # disp_burst_map
                    {
                        "T175-374393-IW1": [10859],
                        "T175-374394-IW1": [10860],
                    },  # burst_to_frames
                    {10859: Mock(), 10860: Mock()},  # frame_to_bursts
                )

                with patch("tools.ccslc_deletion_utility.boto3"):
                    self.utility = CCSLCDeletionUtility(dry_run=True, verbose=False)

    def test_validate_frame_id_valid(self):
        """Test frame ID validation with valid frame ID."""
        self.assertTrue(self.utility.validate_frame_id(10859))
        self.assertTrue(self.utility.validate_frame_id(10860))

    def test_validate_frame_id_invalid(self):
        """Test frame ID validation with invalid frame ID."""
        self.assertFalse(self.utility.validate_frame_id(99999))
        self.assertFalse(self.utility.validate_frame_id(0))

    def test_validate_burst_id_valid(self):
        """Test burst ID validation with valid burst ID."""
        self.assertTrue(self.utility.validate_burst_id("T175-374393-IW1"))
        self.assertTrue(self.utility.validate_burst_id("T175-374394-IW1"))

    def test_validate_burst_id_invalid(self):
        """Test burst ID validation with invalid burst ID."""
        self.assertFalse(self.utility.validate_burst_id("INVALID-BURST-ID"))
        self.assertFalse(self.utility.validate_burst_id(""))

    def test_validate_granule_id_valid(self):
        """Test granule ID validation with valid granule ID."""
        valid_granule = "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
        self.assertTrue(self.utility.validate_granule_id(valid_granule))

    def test_validate_granule_id_invalid(self):
        """Test granule ID validation with invalid granule ID."""
        invalid_granules = [
            "INVALID_GRANULE_ID",
            "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0.h5",
            "",
            "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0.extra",
        ]

        for invalid_granule in invalid_granules:
            with self.subTest(granule=invalid_granule):
                self.assertFalse(self.utility.validate_granule_id(invalid_granule))

    def test_parse_granule_id_valid(self):
        """Test granule ID parsing with valid granule ID."""
        valid_granule = "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
        parsed = self.utility.parse_granule_id(valid_granule)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["project"], "OPERA")
        self.assertEqual(parsed["level"], "L2")
        self.assertEqual(parsed["product_type"], "COMPRESSED-CSLC")
        self.assertEqual(parsed["source"], "S1")
        self.assertEqual(parsed["disp_frame_id"], "F10859")
        self.assertEqual(parsed["burst_id"], "T175-374393-IW1")
        self.assertEqual(parsed["pol"], "VV")
        self.assertEqual(parsed["product_version"], "v1.0")

    def test_parse_granule_id_invalid(self):
        """Test granule ID parsing with invalid granule ID."""
        invalid_granule = "INVALID_GRANULE_ID"
        parsed = self.utility.parse_granule_id(invalid_granule)
        self.assertIsNone(parsed)

    def test_get_ccslc_objects_by_frame(self):
        """Test getting CCSLC objects by frame ID with optimized prefix."""
        # Mock S3 response - now using optimized prefix with frame ID
        with patch.object(self.utility, "s3_client") as mock_s3_client:
            # Set up the mock chain correctly
            mock_paginator = MagicMock()
            mock_page_iterator = MagicMock()

            mock_s3_client.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = mock_page_iterator
            mock_page_iterator.__iter__ = lambda x: iter(
                [
                    {
                        "Contents": [
                            {
                                "Key": "products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0",
                                "Size": 1024,
                                "LastModified": datetime.now(),
                            }
                        ]
                    }
                ]
            )

            objects = self.utility.get_ccslc_objects_by_frame(10859)

            self.assertEqual(len(objects), 1)
            self.assertIn("key", objects[0])
            self.assertIn("filename", objects[0])
            self.assertIn("metadata", objects[0])
            self.assertEqual(objects[0]["metadata"]["disp_frame_id"], "F10859")

            # Verify that the optimized prefix was used
            mock_s3_client.get_paginator.assert_called_once()
            # The paginate call should use the optimized prefix
            call_args = mock_paginator.paginate.call_args
            self.assertEqual(
                call_args[1]["Prefix"],
                "products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F10859_",
            )

    def test_get_ccslc_objects_by_date_range(self):
        """Test getting CCSLC objects by date range."""
        with patch.object(self.utility, "s3_client") as mock_s3_client:
            # Mock S3 response
            mock_s3_client.get_paginator.return_value.paginate.return_value = [
                {
                    "Contents": [
                        {
                            "Key": "products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0.h5",
                            "Size": 1024,
                            "LastModified": datetime.now(),
                        }
                    ]
                }
            ]

            start_date = datetime(2023, 1, 1)
            end_date = datetime(2023, 1, 31)

            objects = self.utility.get_ccslc_objects_by_date_range(start_date, end_date)

            self.assertEqual(len(objects), 1)
            self.assertIn("metadata", objects[0])
            self.assertEqual(objects[0]["metadata"]["creation_ts"], "20230201T120000Z")

    @patch(
        "tools.ccslc_deletion_utility.CCSLCDeletionUtility.get_ccslc_objects_by_frame"
    )
    def test_get_ccslc_objects_by_burst_id(self, mock_get_objects_by_frame):
        """Test getting CCSLC objects by burst ID with optimized frame-based search."""
        # Mock the frame-based search to return objects with the burst ID
        mock_get_objects_by_frame.return_value = [
            {
                "key": "test-key",
                "filename": "test.h5",
                "size": 1024,
                "last_modified": datetime.now(),
                "metadata": {"burst_id": "T175-374393-IW1", "disp_frame_id": "F10859"},
            }
        ]

        objects = self.utility.get_ccslc_objects_by_burst_id("T175-374393-IW1")

        self.assertEqual(len(objects), 1)
        self.assertIn("metadata", objects[0])
        self.assertEqual(objects[0]["metadata"]["burst_id"], "T175-374393-IW1")

        # Verify that get_ccslc_objects_by_frame was called with the correct frame ID
        mock_get_objects_by_frame.assert_called_once_with(10859)

    def test_get_ccslc_objects_by_burst_id_fallback(self):
        """Test getting CCSLC objects by burst ID when burst not found in mapping."""
        with patch.object(self.utility, "s3_client") as mock_s3_client:
            # Mock S3 response for fallback search
            mock_s3_client.get_paginator.return_value.paginate.return_value = [
                {
                    "Contents": [
                        {
                            "Key": "products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0/OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0.h5",
                            "Size": 1024,
                            "LastModified": datetime.now(),
                        }
                    ]
                }
            ]

            # Test with a burst ID not in the mapping
            objects = self.utility.get_ccslc_objects_by_burst_id("T999-999999-IW1")

            self.assertEqual(len(objects), 1)
            self.assertIn("metadata", objects[0])
            self.assertEqual(objects[0]["metadata"]["burst_id"], "T175-374393-IW1")

            # Verify that the fallback prefix was used
            call_args = mock_s3_client.get_paginator.return_value.paginate.call_args
            self.assertEqual(call_args[1]["Prefix"], "products/CSLC_S1_COMPRESSED/")

    def test_get_ccslc_objects_by_granule_ids(self):
        """Test getting CCSLC objects by granule IDs."""
        with patch.object(self.utility, "s3_client") as mock_s3_client:
            # Mock S3 head_object response
            mock_s3_client.head_object.return_value = {
                "ContentLength": 1024,
                "LastModified": datetime.now(),
            }

            granule_ids = [
                "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
            ]

            objects = self.utility.get_ccslc_objects_by_granule_ids(granule_ids)

            self.assertEqual(len(objects), 1)
            self.assertIn("key", objects[0])
            self.assertIn("filename", objects[0])
            self.assertIn("metadata", objects[0])

    def test_get_ccslc_objects_by_granule_ids_not_found(self):
        """Test getting CCSLC objects by granule IDs when object not found."""
        with patch.object(self.utility, "s3_client") as mock_s3_client:
            # Mock S3 NoSuchKey exception
            from botocore.exceptions import ClientError

            mock_s3_client.head_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey"}}, "HeadObject"
            )

            granule_ids = [
                "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
            ]

            objects = self.utility.get_ccslc_objects_by_granule_ids(granule_ids)

            self.assertEqual(len(objects), 0)

    @patch("builtins.input", return_value="yes")
    @patch("tools.ccslc_deletion_utility.CCSLCDeletionUtility.s3_client")
    def test_delete_objects_success(self, mock_s3_client, mock_input):
        """Test successful deletion of objects."""
        # Mock S3 delete_objects response
        mock_s3_client.delete_objects.return_value = {
            "Deleted": [{"Key": "test-key"}],
            "Errors": [],
        }

        objects = [{"key": "test-key", "filename": "test.h5", "size": 1024}]

        # Create utility without dry_run
        with patch("tools.ccslc_deletion_utility.SettingsConf") as mock_settings:
            mock_settings.return_value.cfg = {"LTS_BUCKET": "test-bucket"}
            with patch("tools.ccslc_deletion_utility.localize_disp_frame_burst_hist"):
                with patch("tools.ccslc_deletion_utility.boto3"):
                    utility = CCSLCDeletionUtility(dry_run=False, verbose=False)

        successful, failed = utility.delete_objects(objects)

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)

    @patch("builtins.input", return_value="no")
    def test_delete_objects_cancelled(self, mock_input):
        """Test deletion cancellation by user."""
        objects = [{"key": "test-key", "filename": "test.h5", "size": 1024}]

        # Create utility without dry_run
        with patch("tools.ccslc_deletion_utility.SettingsConf") as mock_settings:
            mock_settings.return_value.cfg = {"LTS_BUCKET": "test-bucket"}
            with patch("tools.ccslc_deletion_utility.localize_disp_frame_burst_hist"):
                with patch("tools.ccslc_deletion_utility.boto3"):
                    utility = CCSLCDeletionUtility(dry_run=False, verbose=False)

        successful, failed = utility.delete_objects(objects)

        self.assertEqual(successful, 0)
        self.assertEqual(failed, 0)

    def test_delete_objects_dry_run(self):
        """Test dry run mode for deletion."""
        objects = [{"key": "test-key", "filename": "test.h5", "size": 1024}]

        successful, failed = self.utility.delete_objects(objects)

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)

    @patch(
        "tools.ccslc_deletion_utility.CCSLCDeletionUtility.get_ccslc_objects_by_frame"
    )
    def test_delete_by_frames(self, mock_get_objects):
        """Test deletion by frame IDs."""
        mock_get_objects.return_value = [
            {"key": "test-key", "filename": "test.h5", "size": 1024}
        ]

        with patch.object(self.utility, "delete_objects", return_value=(1, 0)):
            successful, failed = self.utility.delete_by_frames([10859])

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)
        mock_get_objects.assert_called_once_with(10859)

    @patch(
        "tools.ccslc_deletion_utility.CCSLCDeletionUtility.get_ccslc_objects_by_date_range"
    )
    def test_delete_by_date_range(self, mock_get_objects):
        """Test deletion by date range."""
        mock_get_objects.return_value = [
            {"key": "test-key", "filename": "test.h5", "size": 1024}
        ]

        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 31)

        with patch.object(self.utility, "delete_objects", return_value=(1, 0)):
            successful, failed = self.utility.delete_by_date_range(start_date, end_date)

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)
        mock_get_objects.assert_called_once_with(start_date, end_date)

    @patch(
        "tools.ccslc_deletion_utility.CCSLCDeletionUtility.get_ccslc_objects_by_burst_id"
    )
    def test_delete_by_burst_ids(self, mock_get_objects):
        """Test deletion by burst IDs."""
        mock_get_objects.return_value = [
            {"key": "test-key", "filename": "test.h5", "size": 1024}
        ]

        with patch.object(self.utility, "delete_objects", return_value=(1, 0)):
            successful, failed = self.utility.delete_by_burst_ids(["T175-374393-IW1"])

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)
        mock_get_objects.assert_called_once_with("T175-374393-IW1")

    @patch(
        "tools.ccslc_deletion_utility.CCSLCDeletionUtility.get_ccslc_objects_by_granule_ids"
    )
    def test_delete_by_granule_ids(self, mock_get_objects):
        """Test deletion by granule IDs."""
        mock_get_objects.return_value = [
            {"key": "test-key", "filename": "test.h5", "size": 1024}
        ]

        granule_ids = [
            "OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0"
        ]

        with patch.object(self.utility, "delete_objects", return_value=(1, 0)):
            successful, failed = self.utility.delete_by_granule_ids(granule_ids)

        self.assertEqual(successful, 1)
        self.assertEqual(failed, 0)
        mock_get_objects.assert_called_once_with(granule_ids)


class TestCCSLCDeletionUtilityIntegration(unittest.TestCase):
    """Integration tests for CCSLCDeletionUtility."""

    def test_initialization_with_invalid_bucket(self):
        """Test initialization fails with invalid bucket configuration."""
        with patch("tools.ccslc_deletion_utility.SettingsConf") as mock_settings:
            mock_settings.return_value.cfg = {}  # No LTS_BUCKET

            with patch("tools.ccslc_deletion_utility.localize_disp_frame_burst_hist"):
                with patch("tools.ccslc_deletion_utility.boto3"):
                    with self.assertRaises(ValueError):
                        CCSLCDeletionUtility(dry_run=True, verbose=False)

    def test_initialization_success(self):
        """Test successful initialization."""
        with patch("tools.ccslc_deletion_utility.SettingsConf") as mock_settings:
            mock_settings.return_value.cfg = {"LTS_BUCKET": "test-bucket"}

            with patch("tools.ccslc_deletion_utility.localize_disp_frame_burst_hist"):
                with patch("tools.ccslc_deletion_utility.boto3"):
                    utility = CCSLCDeletionUtility(dry_run=True, verbose=False)

                    self.assertTrue(utility.dry_run)
                    self.assertFalse(utility.verbose)
                    self.assertEqual(utility.lts_bucket, "test-bucket")


if __name__ == "__main__":
    # Create a test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestCCSLCDeletionUtility))
    suite.addTests(loader.loadTestsFromTestCase(TestCCSLCDeletionUtilityIntegration))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
