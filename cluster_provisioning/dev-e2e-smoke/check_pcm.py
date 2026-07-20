#!/usr/bin/env python
import os
import re
import unittest
import logging


class TestSmokeResults(unittest.TestCase):
    success_re = re.compile(r"^SUCCESS", re.MULTILINE)
    error_re = re.compile(r"^ERROR", re.MULTILINE)

    def check_expected(self, check_file, logger):
        """Utility function to check for 'SUCCESS' in check file."""
        assert os.path.exists(check_file), f"Result file not found: {check_file}"
        with open(check_file) as f:
            res = f.read()
        logger.debug("res: {}".format(res))
        assert self.success_re.search(res) is not None, f"No SUCCESS in {check_file}"
        assert self.error_re.search(res) is None, f"ERROR found in {check_file}"

    def test_dswx_hls_expected_datasets(self):
        """Test that the expected DSWx-HLS datasets were generated."""
        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_smoke.txt", logger)

    def test_dswx_hls_cnm_verification(self):
        """Test that CNM-S/R completed successfully for DSWx-HLS."""
        logger = logging.getLogger(__name__)
        self.check_expected("/tmp/datasets_cnm.txt", logger)


if __name__ == "__main__":
    unittest.main()