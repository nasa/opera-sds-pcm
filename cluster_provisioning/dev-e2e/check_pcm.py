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

    def test_stamped_datasets(self):
        """Test that the expected number of datasets were stamped."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/check_stamped_dataset_result.txt", logger)

    def test_empty_isl(self):
        """Test that the ISL was emptied out by the trigger rules."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/check_empty_isl_result.txt", logger)

    def test_isl_report(self):
        """Test that the ISL report exist"""

        logger = logging.getLogger(__name__)
        self.check_exist("/tmp/isl_report.csv", logger)

    def test_expected_force_submits(self):
        """Test that the expected number of datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/check_expected_force_submits.txt", logger)

    def test_expected_report_datasets(self):
        """Test that the expected number of report-related datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/report_datasets.txt", logger)

    def test_expected_bach_services(self):
        """Test that the bach_ui and bach_api v2 are up"""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/opera_bach_ui_status_code.txt", logger)
        self.check_expected("/tmp/opera_bach_api_status_code.txt", logger)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
