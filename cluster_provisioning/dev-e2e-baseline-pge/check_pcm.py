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

    def test_expected_datasets(self):
        """Test that the expected number of datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets.txt", logger)

    def test_stamped_datasets(self):
        """Test that the expected number of datasets were stamped."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/check_stamped_dataset_result.txt", logger)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
