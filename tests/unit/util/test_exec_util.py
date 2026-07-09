"""Tests for util.exec_util PGE/SAS failure surfacing."""

import json
import os
import tempfile
import unittest
from subprocess import CalledProcessError
from unittest.mock import patch

from util import exec_util


class TestCallNoerrExitCodes(unittest.TestCase):
    """SAS exit codes surface as distinct, facetable short errors."""

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()

    def _run_with_exit(self, returncode):
        with patch.object(
            exec_util,
            "check_output",
            side_effect=CalledProcessError(returncode, "cmd", output=b"boom"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                exec_util.call_noerr("some-pge-cmd", self.work_dir)
        return ctx.exception

    def test_exit_1000_maps_to_large_gap_error(self):
        err = self._run_with_exit(1000)
        self.assertEqual(str(err), "large data gap (SAS exit 1000)")
        # Figaro elides short errors at 35 chars — the marker must survive.
        self.assertLessEqual(len(str(err)), 35)

    def test_other_exit_codes_keep_generic_error(self):
        err = self._run_with_exit(1)
        self.assertEqual(str(err), "PGE/SAS failure")

    def test_pge_info_records_exit_code(self):
        self._run_with_exit(1000)
        with open(os.path.join(self.work_dir, "_pge_info.json")) as f:
            info = json.load(f)
        self.assertEqual(info["status"], 1000)
        self.assertEqual(info["stderr"], "boom")


if __name__ == "__main__":
    unittest.main()
