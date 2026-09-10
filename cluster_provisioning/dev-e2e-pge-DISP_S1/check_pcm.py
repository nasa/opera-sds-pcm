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

    def test_historical_expected_datasets(self):
        """Test that the expected number of historical datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_hist.txt", logger)

    def test_phased_historical_expected_datasets(self):
        """Test that the phased historical walk generated the expected datasets."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_hist_phased.txt", logger)

    def test_phased_historical_structure(self):
        """Test that the phased walk took the phased path, not the absolute grid."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/phases_hist_phased.txt", logger)

    def test_forward_expected_datasets(self):
        """Test that the expected number of forward datasets were generated."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_fwd.txt", logger)

    def test_pge_cslc_feed(self):
        """Test that CSLCs produced by the local CSLC-S1 PGE drove a forward DISP-S1 product."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/pge_cslc_feed.txt", logger)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
