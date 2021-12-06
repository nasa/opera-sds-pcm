#!/usr/bin/env python
import os
import re
import unittest
import logging


class TestPCM(unittest.TestCase):
    success_re = re.compile(r"^SUCCESS", re.MULTILINE)
    error_re = re.compile(r"^ERROR", re.MULTILINE)

    def setUp(self):
        pass

    def check_expected(self, check_file, logger):
        """Utility function to check for 'SUCCESS' in check file."""

        assert os.path.exists(check_file)
        with open(check_file) as f:
            res = f.read()
        logger.debug("res: {}".format(res))
        assert self.success_re.search(res) is not None
        assert self.error_re.search(res) is None

    def check_exist(self, check_file, logger):
        """Utility function to check for file existence"""

        assert os.path.exists(check_file)
        logger.debug("File exist: {}".format(check_file))

    def test_expected_datasets(self):
        """Test that the expected number of datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets.txt", logger)

    def test_expected_cop_catalog(self):
        """Test that the expected number of COP catalog records were created."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/cop_catalog.txt", logger)

    def test_expected_tiurdrop_catalog(self):
        """Test that the expected number of COP catalog records were created."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/tiurdrop_catalog.txt", logger)

    def test_expected_rost_catalog(self):
        """Test that the expected number of ROST catalog records were created."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/rost_catalog.txt", logger)

    def test_expected_radar_mode_catalog(self):
        """Test that the expected number of Radar Mode catalog records were created."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/radar_mode_catalog.txt", logger)

    def test_empty_isl(self):
        """Test that the ISL was emptied out by the trigger rules."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/check_empty_isl_result.txt", logger)

    def test_isl_report(self):
        """Test that the ISL report exist"""

        logger = logging.getLogger(__name__)
        self.check_exist("/tmp/isl_report.csv", logger)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
