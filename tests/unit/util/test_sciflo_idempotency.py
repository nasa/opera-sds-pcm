"""Tests for util.sciflo_idempotency."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from util import sciflo_idempotency


class TestIsIdempotencyCheckEnabled(unittest.TestCase):

    def test_enabled_when_pge_true(self):
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        self.assertTrue(
            sciflo_idempotency.is_idempotency_check_enabled("L3_DISP_S1", settings)
        )

    def test_disabled_when_pge_false(self):
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": False}}
        self.assertFalse(
            sciflo_idempotency.is_idempotency_check_enabled("L3_DISP_S1", settings)
        )

    def test_disabled_when_pge_missing(self):
        # Other PGEs not listed -> opt-in model -> disabled by default
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        self.assertFalse(
            sciflo_idempotency.is_idempotency_check_enabled("L2_CSLC_S1", settings)
        )

    def test_disabled_when_section_missing(self):
        self.assertFalse(
            sciflo_idempotency.is_idempotency_check_enabled("L3_DISP_S1", {})
        )

    def test_disabled_when_settings_none(self):
        self.assertFalse(
            sciflo_idempotency.is_idempotency_check_enabled("L3_DISP_S1", None)
        )

    def test_disabled_when_section_is_none(self):
        # YAML "SCIFLO_IDEMPOTENCY_CHECK:" (no body) parses as None
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": None}
        self.assertFalse(
            sciflo_idempotency.is_idempotency_check_enabled("L3_DISP_S1", settings)
        )


class TestFindExistingProduct(unittest.TestCase):

    def _mk_es(self, hits):
        es = MagicMock()
        es.search.return_value = {"hits": {"hits": hits}}
        conn = MagicMock()
        conn.es = es
        return conn, es

    def test_returns_id_when_found(self):
        conn, es = self._mk_es([{"_id": "OPERA_L3_DISP-S1_..."}])
        with patch("data_subscriber.es_conn_util.get_es_connection",
                   return_value=conn):
            result = sciflo_idempotency.find_existing_product(
                "grq_*_l3_disp_s1*", {"match_all": {}}
            )
        self.assertEqual(result, "OPERA_L3_DISP-S1_...")

    def test_returns_id_from_source_when_id_missing(self):
        conn, es = self._mk_es(
            [{"_source": {"id": "OPERA_L3_DISP-S1_via_source"}}]
        )
        with patch("data_subscriber.es_conn_util.get_es_connection",
                   return_value=conn):
            result = sciflo_idempotency.find_existing_product(
                "grq_*_l3_disp_s1*", {"match_all": {}}
            )
        self.assertEqual(result, "OPERA_L3_DISP-S1_via_source")

    def test_returns_none_when_no_hits(self):
        conn, es = self._mk_es([])
        with patch("data_subscriber.es_conn_util.get_es_connection",
                   return_value=conn):
            result = sciflo_idempotency.find_existing_product(
                "grq_*_l3_disp_s1*", {"match_all": {}}
            )
        self.assertIsNone(result)

    def test_search_call_uses_index_pattern_and_query(self):
        conn, es = self._mk_es([])
        query = {"wildcard": {"id.keyword": "OPERA_L3_DISP-S1_*"}}
        with patch("data_subscriber.es_conn_util.get_es_connection",
                   return_value=conn):
            sciflo_idempotency.find_existing_product(
                "grq_*_l3_disp_s1*", query
            )
        es.search.assert_called_once()
        kwargs = es.search.call_args.kwargs
        self.assertEqual(kwargs["index"], "grq_*_l3_disp_s1*")
        self.assertEqual(kwargs["body"]["query"], query)
        self.assertEqual(kwargs["body"]["size"], 1)


class _TempCwdTestCase(unittest.TestCase):
    """Base class that defensively isolates each test in a fresh tempdir.

    The bail path writes _alt_msg.txt / _alt_msg_details.txt to CWD, so any
    test that exercises it (or asserts file presence/absence) must run in
    a known-writable directory. Other tests in the suite that os.chdir() to
    a tempdir then rmtree without restoring CWD can leave a broken CWD that
    makes os.getcwd() raise -- recover from that by falling back to the
    system tempdir.
    """

    def setUp(self):
        try:
            self.orig_dir = os.getcwd()
        except (FileNotFoundError, OSError):
            self.orig_dir = tempfile.gettempdir()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        target = self.orig_dir if os.path.isdir(self.orig_dir) else tempfile.gettempdir()
        try:
            os.chdir(target)
        except Exception:
            os.chdir(tempfile.gettempdir())
        shutil.rmtree(self.test_dir, ignore_errors=True)


class TestExitIfExistingProduct(_TempCwdTestCase):

    def test_noop_when_disabled(self):
        # Disabled -> no ES query, no exit
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": False}}
        with patch.object(sciflo_idempotency,
                          "find_existing_product") as mock_find:
            sciflo_idempotency.exit_if_existing_product(
                "L3_DISP_S1", settings, "grq_*_l3_disp_s1*", {"match_all": {}}
            )
        mock_find.assert_not_called()

    def test_noop_when_no_match(self):
        # Enabled, ES returns no match -> no exit, returns normally
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        with patch.object(sciflo_idempotency,
                          "find_existing_product", return_value=None):
            # Should not raise SystemExit
            sciflo_idempotency.exit_if_existing_product(
                "L3_DISP_S1", settings, "grq_*_l3_disp_s1*", {"match_all": {}}
            )

    def test_exits_clean_when_match_found(self):
        # Enabled, ES returns existing -> sys.exit(0). The helper writes
        # _alt_msg.txt to CWD on the bail path, so this test class uses
        # _TempCwdTestCase to ensure a writable tempdir is the CWD.
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        with patch.object(sciflo_idempotency,
                          "find_existing_product",
                          return_value="OPERA_L3_DISP-S1_existing"):
            with self.assertRaises(SystemExit) as ctx:
                sciflo_idempotency.exit_if_existing_product(
                    "L3_DISP_S1", settings, "grq_*_l3_disp_s1*",
                    {"match_all": {}}
                )
            self.assertEqual(ctx.exception.code, 0)


class TestOperatorVisibility(_TempCwdTestCase):
    """The bail must surface to the operator -- writing the standard
    _alt_msg.txt / _alt_msg_details.txt so the job appears as a
    distinguishable 'dup skip' in Figaro instead of an indistinguishable
    job-completed.
    """

    def test_writes_alt_msg_files_on_bail(self):
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        with patch.object(sciflo_idempotency,
                          "find_existing_product",
                          return_value="OPERA_L3_DISP-S1_existing_id"):
            with self.assertRaises(SystemExit):
                sciflo_idempotency.exit_if_existing_product(
                    "L3_DISP_S1", settings, "grq_*_l3_disp_s1*",
                    {"match_all": {}}
                )

        self.assertTrue(os.path.isfile("_alt_msg.txt"))
        self.assertTrue(os.path.isfile("_alt_msg_details.txt"))

        with open("_alt_msg.txt") as f:
            short = f.read().strip()
        self.assertEqual(short, "dup skip: L3_DISP_S1")
        # Figaro truncates short messages > 35 chars; enforce the limit.
        self.assertLessEqual(len(short), 35)

        with open("_alt_msg_details.txt") as f:
            details = f.read()
        self.assertIn("OPERA_L3_DISP-S1_existing_id", details)
        self.assertIn("L3_DISP_S1", details)
        self.assertIn("SCIFLO_IDEMPOTENCY_CHECK", details)

    def test_no_message_files_when_check_disabled(self):
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": False}}
        sciflo_idempotency.exit_if_existing_product(
            "L3_DISP_S1", settings, "grq_*_l3_disp_s1*", {"match_all": {}}
        )
        self.assertFalse(os.path.isfile("_alt_msg.txt"))
        self.assertFalse(os.path.isfile("_alt_msg_details.txt"))

    def test_no_message_files_when_no_match(self):
        settings = {"SCIFLO_IDEMPOTENCY_CHECK": {"L3_DISP_S1": True}}
        with patch.object(sciflo_idempotency,
                          "find_existing_product", return_value=None):
            sciflo_idempotency.exit_if_existing_product(
                "L3_DISP_S1", settings, "grq_*_l3_disp_s1*",
                {"match_all": {}}
            )
        self.assertFalse(os.path.isfile("_alt_msg.txt"))
        self.assertFalse(os.path.isfile("_alt_msg_details.txt"))


if __name__ == "__main__":
    unittest.main()
