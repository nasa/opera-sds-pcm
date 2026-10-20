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

    def test_first_k_set_queried_and_second_blocked(self):
        """Test that the first k-set's GSLCs were cataloged and the second k-set waits on its compressed GSLC."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/disp_ni_hist_blocked.txt", logger)

    def test_first_k_set_expected_datasets(self):
        """Test that the first k-set's PGE job published its DISP-NI products and compressed GSLC."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_disp_ni_k_set_1.txt", logger)

    def test_compressed_gslc_unblocked_second_k_set(self):
        """Test that the published compressed GSLC unblocked the second k-set and the batch proc finished."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/disp_ni_hist_finished.txt", logger)

    def test_historical_expected_datasets(self):
        """Test that both k-sets' PGE jobs published the expected DISP-NI products and compressed GSLCs."""

        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_disp_ni_hist.txt", logger)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
