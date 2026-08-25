"""Tests for util.exec_util PGE/SAS failure surfacing."""

import json
import os
import shutil
import tempfile
import unittest
from subprocess import CalledProcessError
from unittest.mock import patch

from util import exec_util


class TestCallNoerrExitCodes(unittest.TestCase):
    """SAS exit codes surface as distinct, facetable short errors."""

    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.starting_dir = os.curdir
        os.chdir(self.work_dir)
        self.addCleanup(shutil.rmtree, self.work_dir)

    def tearDown(self):
        os.chdir(self.starting_dir)

    def _pge_info(self):
        with open(os.path.join(self.work_dir, "_pge_info.json")) as f:
            return json.load(f)

    def test_real_exit_1000_truncates_to_232_and_maps(self):
        """No mocking: POSIX wait statuses are 8-bit, so a real child
        exiting 1000 arrives as returncode 232 (1000 % 256) — the mapping
        must catch the truncated code, not just the literal 1000."""
        with self.assertRaises(RuntimeError) as ctx:
            exec_util.call_noerr("exit 1000", self.work_dir)
        self.assertEqual(str(ctx.exception), "large data gap (SAS exit 1000)")
        self.assertEqual(self._pge_info()["status"], 232)

    def test_real_generic_failure_keeps_generic_error(self):
        with self.assertRaises(RuntimeError) as ctx:
            exec_util.call_noerr("exit 3", self.work_dir)
        self.assertEqual(str(ctx.exception), "PGE/SAS failure")
        self.assertEqual(self._pge_info()["status"], 3)

    def test_untruncated_1000_also_maps(self):
        """Future-proofing: if an invocation path ever preserves the full
        exit code, 1000 itself must still map."""
        with patch.object(
            exec_util,
            "check_output",
            side_effect=CalledProcessError(1000, "cmd", output=b"boom"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                exec_util.call_noerr("some-pge-cmd", self.work_dir)
        self.assertEqual(str(ctx.exception), "large data gap (SAS exit 1000)")
        info = self._pge_info()
        self.assertEqual(info["status"], 1000)
        self.assertEqual(info["stderr"], "boom")

    def test_short_error_fits_figaro_elision(self):
        # Figaro elides short errors at 35 chars — the marker must survive.
        self.assertLessEqual(len("large data gap (SAS exit 1000)"), 35)


if __name__ == "__main__":
    unittest.main()
