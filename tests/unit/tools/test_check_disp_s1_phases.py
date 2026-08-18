"""A campaign only owes a forward product for a date its walk actually reached.

check_disp_s1_phases.py asserts that every forward date produced a product. That assertion
exists because a no-fire is terminal, so a walk can pass a whole forward block, self-disable
at a 100% cursor, and owe every one of those products with nothing else in the system saying
so -- observed on frame 24726.

But it demanded products for the entire forward block regardless of how far the walk got. A
batch proc whose data_end_date stops short of the forward block never processes those dates,
and the check called a correct run broken. Seen on frame 24718 in the DISP-S1 smoke test,
whose forward_03 dates all fall after the proc's 2026-01-01 window.
"""

import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path

CHECKER_DIR = Path(__file__).parents[3] / "conf" / "sds" / "files" / "test"
sys.path.insert(0, str(CHECKER_DIR))

# the checker imports cluster-only modules at import time
for mod in ("data_subscriber", "data_subscriber.cslc_utils",
            "opera_commons", "opera_commons.es_connection"):
    sys.modules.setdefault(mod, types.ModuleType(mod))
sys.modules["data_subscriber"].cslc_utils = sys.modules["data_subscriber.cslc_utils"]
sys.modules["data_subscriber.cslc_utils"].localize_disp_frame_burst_hist = lambda: ({}, {}, {})
sys.modules["opera_commons.es_connection"].get_grq_es = lambda: None

from check_disp_s1_phases import forward_dates  # noqa: E402


class Phase(object):
    def __init__(self, label, start_pos, end_pos):
        self.label = label
        self.start_pos = start_pos
        self.end_pos = end_pos


class Frame(object):
    """Frame 24718's shape: no_run[11], historical_03[15], forward_03[10]."""

    def __init__(self):
        base = datetime(2025, 1, 1)
        self.sensing_datetimes = [base + timedelta(days=12 * i) for i in range(36)]
        self.phases = [Phase("no_run", 0, 11),
                       Phase("historical_03", 11, 26),
                       Phase("forward_03", 26, 36)]


class ForwardDatesRespectTheCursorTest(unittest.TestCase):

    def setUp(self):
        self.frame = Frame()

    def test_cursor_short_of_the_forward_block_owes_nothing(self):
        """The window ended before forward_03, so none of it was walked."""
        self.assertEqual(forward_dates(self.frame, cursor=26), set())

    def test_cursor_part_way_through_owes_only_what_was_walked(self):
        self.assertEqual(len(forward_dates(self.frame, cursor=30)), 4)

    def test_completed_walk_owes_the_whole_block(self):
        self.assertEqual(len(forward_dates(self.frame, cursor=36)), 10)

    def test_no_cursor_owes_the_whole_block(self):
        """Default stays strict, so a finished campaign is still fully checked."""
        self.assertEqual(len(forward_dates(self.frame, None)), 10)

    def test_cursor_beyond_the_end_is_harmless(self):
        self.assertEqual(len(forward_dates(self.frame, cursor=99)), 10)

    def test_the_dates_returned_are_the_walked_ones(self):
        walked = forward_dates(self.frame, cursor=28)
        expected = {self.frame.sensing_datetimes[p].strftime("%Y%m%d") for p in (26, 27)}
        self.assertEqual(walked, expected)

    def test_a_frame_with_no_forward_block_owes_nothing(self):
        self.frame.phases = [Phase("historical_01", 0, 15)]
        self.assertEqual(forward_dates(self.frame, cursor=15), set())


if __name__ == "__main__":
    unittest.main()
