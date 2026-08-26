"""Reading the range the consistent burst database's CMR survey assessed.

Absence from sensing_time_list only means "deliberately excluded" inside this range, so
getting it wrong in either direction is costly: too late an end suppresses acquisitions
the survey never examined, too early an end lets partial passes keep blocking. The range
lives in metadata.input_cmr_csv and nowhere else that survives -- both databases the
campaign ran on were renamed on disk, so the filename cannot be trusted.
"""

import json
import os
import tempfile
import unittest

from data_subscriber import cslc_utils

SURVEY = "cmr_survey_2016-07-01_to_2026-06-23.csv"


def write_db(metadata, path):
    with open(path, "w") as f:
        json.dump({"metadata": metadata, "data": {}} if metadata is not None else {"data": {}}, f)
    return path


class AssessedRangeTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        # the accessor is cached on the file path, so give every case its own
        self.n = 0

    def _path(self):
        self.n += 1
        return os.path.join(self.dir, f"db_{self.n}.json")

    def test_reads_the_end_date_from_input_cmr_csv(self):
        p = write_db({"input_cmr_csv": SURVEY}, self._path())
        self.assertEqual(cslc_utils.disp_burst_db_assessed_end(p), "20260623")

    def test_ignores_the_generation_time(self):
        """generation_time runs weeks past the survey end; using it would suppress
        acquisitions the survey never looked at."""
        p = write_db({"input_cmr_csv": SURVEY,
                      "generation_time": "2026-08-13 18:15:12.280999"}, self._path())
        self.assertEqual(cslc_utils.disp_burst_db_assessed_end(p), "20260623")

    def test_missing_metadata_block_yields_none(self):
        p = self._path()
        with open(p, "w") as f:
            json.dump({"data": {}}, f)
        self.assertIsNone(cslc_utils.disp_burst_db_assessed_end(p))

    def test_missing_survey_key_yields_none(self):
        p = write_db({"generation_time": "2026-08-13 18:15:12"}, self._path())
        self.assertIsNone(cslc_utils.disp_burst_db_assessed_end(p))

    def test_unparseable_survey_name_yields_none(self):
        p = write_db({"input_cmr_csv": "cmr_survey_latest.csv"}, self._path())
        self.assertIsNone(cslc_utils.disp_burst_db_assessed_end(p))

    def test_unreadable_file_yields_none(self):
        self.assertIsNone(cslc_utils.disp_burst_db_assessed_end(
            os.path.join(self.dir, "does_not_exist.json")))

    def test_list_shaped_json_yields_none(self):
        p = self._path()
        with open(p, "w") as f:
            json.dump([1, 2, 3], f)
        self.assertIsNone(cslc_utils.disp_burst_db_assessed_end(p))

    def test_shipping_fixture_parses(self):
        """The annotated fixture used across the suite carries a real survey name."""
        fixture = os.path.join(os.path.dirname(__file__), "test_data",
                               "disp_s1_consistent_db_with_modes.json")
        self.assertEqual(cslc_utils.disp_burst_db_assessed_end(fixture), "20241231")


class ExclusionSwitchTest(unittest.TestCase):
    """On unless a venue explicitly turns it off: this is a correctness repair, so a
    venue running new code against an un-updated settings.yaml still gets the fix."""

    def test_absent_key_reads_as_enabled(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write("SOME_OTHER_SETTING: 1\n")
            path = f.name
        try:
            self.assertTrue(cslc_utils.burst_db_exclusion_enabled(path))
        finally:
            os.unlink(path)

    def test_explicit_false_disables(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write("DISP_S1_BURST_DB_EXCLUSION_ENABLED: false\n")
            path = f.name
        try:
            self.assertFalse(cslc_utils.burst_db_exclusion_enabled(path))
        finally:
            os.unlink(path)

    def test_explicit_true_enables(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write("DISP_S1_BURST_DB_EXCLUSION_ENABLED: true\n")
            path = f.name
        try:
            self.assertTrue(cslc_utils.burst_db_exclusion_enabled(path))
        finally:
            os.unlink(path)

    def test_shipping_default_is_on(self):
        repo_settings = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                                     "conf", "settings.yaml")
        self.assertTrue(cslc_utils.burst_db_exclusion_enabled(os.path.abspath(repo_settings)))


if __name__ == "__main__":
    unittest.main()
