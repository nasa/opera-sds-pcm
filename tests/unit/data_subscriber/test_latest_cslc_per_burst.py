"""One CSLC per burst per sensing date, newest processing date wins.

ASF republishes a burst under a new processing date when it reprocesses, and both
granules stay in the catalog. The SAS does not choose between them -- disp_s1's
_assert_no_duplicate_dates refuses the stack outright -- so a single reprocessing event
silently blocks every affected sensing date until PCM resolves it.

Observed on frame 24726: 14 of 27 bursts on 2026-06-05 carried both a 2026-06-06 and a
2026-07-18 granule, and the forward SCIFLO for that date failed while the other 24 ran.

ASF also holds pairs of granules for one burst on one pass whose sensing times are one
second apart. Observed on frame 18904 (burst T071-151230-IW3, 2026-08-15) and on frames
16660 and 16661, which share burst T063-133281-IW3 (2026-06-28): every forward job whose
window held the date failed until the state configs were corrected by hand.
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


def s1d_path(burst, sensing, processed):
    name = f"OPERA_L2_CSLC-S1_{burst}_{sensing}Z_{processed}Z_S1D_VV_v1.1"
    return f"{BASE}/{name}/{name}.h5"


# Frame 18904: the granule with the later sensing time is the newer product.
F18904_FIRST = s1d_path("T071-151230-IW3", "20260815T135205", "20261010T122235")
F18904_SECOND = s1d_path("T071-151230-IW3", "20260815T135206", "20261010T122353")
# Frames 16660 and 16661: the granule with the later sensing time is the older product.
F16660_FIRST = s1d_path("T063-133281-IW3", "20260628T000654", "20261009T195831")
F16660_SECOND = s1d_path("T063-133281-IW3", "20260628T000655", "20261009T195701")


def sensing_dates_per_burst(paths):
    """What the SAS checks: the (burst, sensing date) of every CSLC it is given."""
    return [(p.split("_")[3], p.split("_")[4][:8]) for p in (path.rsplit("/", 1)[1] for path in paths)]


class SensingTimesOneSecondApartTest(unittest.TestCase):
    """Two granules of one burst on one pass are one acquisition, whatever their sensing seconds."""

    def test_one_granule_is_kept_for_the_date(self):
        got = latest_cslc_per_burst([F18904_FIRST, F18904_SECOND])
        self.assertEqual(got, [F18904_SECOND])

    def test_processing_time_decides_not_sensing_time(self):
        """Here the earlier sensing time is the newer product, and it is the one kept."""
        self.assertEqual(latest_cslc_per_burst([F16660_FIRST, F16660_SECOND]), [F16660_FIRST])
        self.assertEqual(latest_cslc_per_burst([F16660_SECOND, F16660_FIRST]), [F16660_FIRST])

    def test_a_window_holding_the_date_passes_the_duplicate_date_check(self):
        """A 15-date window with the pair on one date ends with one CSLC per burst and date."""
        window = [s1d_path("T071-151230-IW3", f"202608{day:02d}T135205", "20261010T120000")
                  for day in range(1, 15)]
        got = latest_cslc_per_burst(window + [F18904_FIRST, F18904_SECOND])
        self.assertEqual(len(got), 15)
        keys = sensing_dates_per_burst(got)
        self.assertEqual(len(keys), len(set(keys)))

    def test_the_other_bursts_of_the_date_are_untouched(self):
        neighbour = s1d_path("T071-151231-IW1", "20260815T135207", "20261010T122353")
        got = latest_cslc_per_burst([F18904_FIRST, F18904_SECOND, neighbour])
        self.assertEqual(got, sorted([F18904_SECOND, neighbour]))

    def test_the_same_processing_second_still_gives_one_stable_answer(self):
        first = s1d_path("T071-151230-IW3", "20260815T135205", "20261010T122235")
        second = s1d_path("T071-151230-IW3", "20260815T135206", "20261010T122235")
        self.assertEqual(latest_cslc_per_burst([first, second]), latest_cslc_per_burst([second, first]))
        self.assertEqual(len(latest_cslc_per_burst([first, second])), 1)

    def test_a_reprocessed_granule_still_wins(self):
        """The reprocessing case is unchanged by keying on the date."""
        self.assertEqual(latest_cslc_per_burst([ORIG, REPROC]), [REPROC])


if __name__ == "__main__":
    unittest.main()
