"""One CSLC per burst per sensing time, newest processing date wins.

ASF republishes a burst under a new processing date when it reprocesses, and both
granules stay in the catalog. The SAS does not choose between them -- disp_s1's
_assert_no_duplicate_dates refuses the stack outright -- so a single reprocessing event
silently blocks every affected sensing date until PCM resolves it.

Observed on frame 24726: 14 of 27 bursts on 2026-06-05 carried both a 2026-06-06 and a
2026-07-18 granule, and the forward SCIFLO for that date failed while the other 24 ran.
"""

import unittest

from data_subscriber.cslc_utils import latest_cslc_per_burst

BASE = "s3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1"


def path(burst, sensing, processed):
    name = f"OPERA_L2_CSLC-S1_{burst}_{sensing}Z_{processed}Z_S1C_VV_v1.1"
    return f"{BASE}/{name}/{name}.h5"


ORIG = path("T093-197805-IW1", "20260605T013256", "20260606T190534")
REPROC = path("T093-197805-IW1", "20260605T013256", "20260718T021629")
OTHER_BURST = path("T093-197806-IW1", "20260605T013259", "20260606T190534")
OTHER_DATE = path("T093-197805-IW1", "20260524T013256", "20260606T190534")


class LatestCslcPerBurstTest(unittest.TestCase):

    def test_reprocessed_granule_wins(self):
        self.assertEqual(latest_cslc_per_burst([ORIG, REPROC]), [REPROC])

    def test_order_of_input_does_not_matter(self):
        self.assertEqual(latest_cslc_per_burst([REPROC, ORIG]), [REPROC])

    def test_different_bursts_are_both_kept(self):
        got = latest_cslc_per_burst([ORIG, OTHER_BURST])
        self.assertEqual(got, sorted([ORIG, OTHER_BURST]))

    def test_same_burst_different_sensing_times_are_both_kept(self):
        """Deduplication is per burst AND per acquisition -- never across dates."""
        got = latest_cslc_per_burst([ORIG, OTHER_DATE])
        self.assertEqual(got, sorted([ORIG, OTHER_DATE]))

    def test_the_observed_frame_24726_case(self):
        """14 bursts duplicated on one date collapse to 14 paths, all reprocessed."""
        paths = []
        for n in range(14):
            burst = f"T093-1978{10 + n:02d}-IW1"
            paths.append(path(burst, "20260605T013256", "20260606T190534"))
            paths.append(path(burst, "20260605T013256", "20260718T021629"))
        got = latest_cslc_per_burst(paths)
        self.assertEqual(len(got), 14)
        self.assertTrue(all("20260718" in p for p in got))

    def test_unparseable_paths_pass_through_rather_than_vanish(self):
        """Never silently drop an input we cannot reason about."""
        odd = "s3://bucket/some/other/thing.h5"
        got = latest_cslc_per_burst([ORIG, REPROC, odd])
        self.assertIn(odd, got)
        self.assertIn(REPROC, got)
        self.assertNotIn(ORIG, got)

    def test_empty_and_none_are_safe(self):
        self.assertEqual(latest_cslc_per_burst([]), [])
        self.assertEqual(latest_cslc_per_burst(None), [])

    def test_accepts_a_set_and_returns_sorted(self):
        got = latest_cslc_per_burst({OTHER_BURST, REPROC, ORIG})
        self.assertEqual(got, sorted(got))
        self.assertEqual(len(got), 2)

    def test_no_duplicates_is_a_no_op(self):
        paths = [REPROC, OTHER_BURST, OTHER_DATE]
        self.assertEqual(latest_cslc_per_burst(paths), sorted(paths))


if __name__ == "__main__":
    unittest.main()
