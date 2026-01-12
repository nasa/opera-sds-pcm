#!/usr/bin/env python3
"""
Unit and CMR integration tests for DIST-S1 lookback window selection logic.

This module tests the lookback window calculation and file selection logic
for the DIST-S1 algorithm. For a given time t0, we need to select files from
three lookback windows:
- w1: centered at t0 - 1 year (e.g., +/- 60 days)
- w2: centered at t0 - 2 years (e.g., +/- 60 days)
- w3: centered at t0 - 3 years (e.g., +/- 60 days)

Files are selected as the n closest files to the center of each window.

Test markers:
- Unit tests: No markers required - test lookback logic with mock data (run by default)
- @pytest.mark.cmr: CMR integration tests - test lookback logic with real CMR data (requires network)
  To run only unit tests: pytest tests/unit/test_dist_s1.py -m "not cmr"
  To run only CMR integration tests: pytest tests/unit/test_dist_s1.py -m cmr

Note: These tests do not exercise the full DIST-S1 pipeline, only the standalone lookback window
selection logic defined within the test along with CMR querying.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import dateutil.parser
import pytest

from data_subscriber.cmr import DateTimeRange, async_query_cmr_v2


@dataclass
class RtcGranule:
    """Represents an RTC granule file with its acquisition time."""

    granule_id: str
    acquisition_time: datetime

    def __repr__(self) -> str:
        return f"RtcGranule({self.granule_id}, {self.acquisition_time.isoformat()})"


@dataclass
class LookbackWindow:
    window_start: datetime
    window_center: datetime
    window_end: datetime


# Type alias for granule lists
GranuleList = List[RtcGranule]


# ============================================================================
# Test Helper Functions
# ============================================================================


def generate_rtc_granule_id(
    tile_id: str, burst_id: str, subswath: str, acquisition_time: datetime, satellite: str = "S1A"
) -> str:
    """
    Generate a realistic RTC granule ID.

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR")
        burst_id: Burst ID (e.g., "123456")
        subswath: Subswath identifier (e.g., "IW1", "IW2", "IW3")
        acquisition_time: Acquisition time
        satellite: Satellite identifier (default "S1A")

    Returns:
        RTC granule ID string
    """
    acq_str = acquisition_time.strftime("%Y%m%dT%H%M%SZ")
    # Production time is typically a day or so after acquisition
    prod_time = acquisition_time + timedelta(days=1)
    prod_str = prod_time.strftime("%Y%m%dT%H%M%SZ")

    return f"OPERA_L2_RTC-S1_{tile_id}-{burst_id}-{subswath}_" f"{acq_str}_{prod_str}_{satellite}_30_v1.0"


# ============================================================================
# Helper Functions for Lookback Window Logic
# ============================================================================


def calculate_lookback_window(t0: datetime, years_back: int, window_size_days: int) -> LookbackWindow:
    """
    Calculate a lookback window centered at t0 - years_back years.

    Args:
        t0: Reference time
        years_back: Number of years to look back (1, 2, or 3)
        window_size_days: Half-width of the window in days (e.g., 60 means +/- 60 days)

    Returns:
        Tuple of (window_start, window_center, window_end)
    """
    # Calculate the center of the window using timedelta to handle leap years properly
    # This avoids issues with Feb 29 in leap years
    days_back = years_back * 365
    window_center = t0 - timedelta(days=days_back)

    # Calculate window boundaries
    window_start = window_center - timedelta(days=window_size_days)
    window_end = window_center + timedelta(days=window_size_days)

    return LookbackWindow(window_start, window_center, window_end)


def select_files_in_window(
    available_files: GranuleList, lookback_window: LookbackWindow, max_files: int
) -> GranuleList:
    """
    Select files within a window, choosing those closest to the window center.

    Args:
        available_files: GranuleList of available files
        window_start: Start of the window
        window_center: Center of the window
        window_end: End of the window
        max_files: Maximum number of files to select

    Returns:
        GranuleList of selected files, sorted by proximity to center
    """
    # Filter files within the window
    files_in_window = [
        file
        for file in available_files
        if lookback_window.window_start <= file.acquisition_time <= lookback_window.window_end
    ]

    # Sort by distance from window center
    files_in_window.sort(key=lambda f: abs((f.acquisition_time - lookback_window.window_center).total_seconds()))

    # Return up to max_files
    return files_in_window[:max_files]


def select_dist_s1_input_files(
    t0: datetime, available_files: GranuleList, window_configs: List[Tuple[int, int, int]]
) -> Tuple[GranuleList, GranuleList, GranuleList]:
    """
    Select input files for DIST-S1 algorithm across three lookback windows.

    Args:
        t0: Reference time
        available_files: GranuleList of available files
        window_configs: List of (years_back, window_size_days, max_files) tuples
                       for w1, w2, w3 respectively

    Returns:
        Tuple of (w1_files, w2_files, w3_files) as GranuleLists

    Raises:
        ValueError: If no files are found in any window
    """
    results = []

    for years_back, window_size_days, max_files in window_configs:
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        selected_files = select_files_in_window(available_files, lookback_window, max_files)

        if len(selected_files) == 0:
            # Alert: no files found in this window
            print(
                f"WARNING: No files found in window w{years_back} "
                f"(center: {lookback_window.window_center.isoformat()}, "
                f"range: {lookback_window.window_start.isoformat()} to {lookback_window.window_end.isoformat()})"
            )

        results.append(selected_files)

    return tuple(results)


# ============================================================================
# Unit Tests
# ============================================================================


class TestLookbackWindowCalculation:
    """Tests for lookback window calculation."""

    def test_window_1_year_back(self):
        """Test calculating window centered at t0 - 1 year."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=window_size_days)

        # 365 days back from 2025-09-25 = 2024-09-25
        assert lookback_window.window_center == datetime(2024, 9, 25, 12, 0, 0)
        assert lookback_window.window_start == datetime(2024, 7, 27, 12, 0, 0)
        assert lookback_window.window_end == datetime(2024, 11, 24, 12, 0, 0)

    def test_window_2_years_back(self):
        """Test calculating window centered at t0 - 2 years."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=2, window_size_days=window_size_days)

        # 730 days back from 2025-09-25 = 2023-09-26
        assert lookback_window.window_center == datetime(2023, 9, 26, 12, 0, 0)
        assert lookback_window.window_start == datetime(2023, 7, 28, 12, 0, 0)
        assert lookback_window.window_end == datetime(2023, 11, 25, 12, 0, 0)

    def test_window_3_years_back(self):
        """Test calculating window centered at t0 - 3 years."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=3, window_size_days=window_size_days)

        # 1095 days back from 2025-09-25 = 2022-09-26
        assert lookback_window.window_center == datetime(2022, 9, 26, 12, 0, 0)
        assert lookback_window.window_start == datetime(2022, 7, 28, 12, 0, 0)
        assert lookback_window.window_end == datetime(2022, 11, 25, 12, 0, 0)

    def test_different_window_sizes(self):
        """Test windows with different sizes."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Test with 15-day window (center = 2024-09-25)
        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=15)
        assert lookback_window.window_start == datetime(2024, 9, 10, 12, 0, 0)
        assert lookback_window.window_end == datetime(2024, 10, 10, 12, 0, 0)

        # Test with 45-day window
        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=45)
        assert lookback_window.window_start == datetime(2024, 8, 11, 12, 0, 0)
        assert lookback_window.window_end == datetime(2024, 11, 9, 12, 0, 0)

    def test_leap_year_handling(self):
        """Test window calculation across leap years."""
        # Feb 29, 2024 (leap year) - 365 days = Mar 1, 2023
        t0 = datetime(2024, 2, 29, 12, 0, 0)
        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=60)

        # 365 days back from Feb 29, 2024 = Mar 1, 2023
        assert lookback_window.window_center == datetime(2023, 3, 1, 12, 0, 0)


class TestFileSelectionInWindow:
    """Tests for file selection within a window."""

    def test_select_files_closest_to_center(self):
        """Test that files closest to center are selected."""
        lookback_window = calculate_lookback_window(datetime(2023, 6, 15, 12, 0, 0), years_back=0, window_size_days=30)

        # Create mock RTC granules at various distances from center
        available_files = [
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center),
                lookback_window.window_center,
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center + timedelta(days=1)),
                lookback_window.window_center + timedelta(days=1),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center - timedelta(days=1)),
                lookback_window.window_center - timedelta(days=1),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center + timedelta(days=5)),
                lookback_window.window_center + timedelta(days=5),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center - timedelta(days=5)),
                lookback_window.window_center - timedelta(days=5),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center + timedelta(days=10)),
                lookback_window.window_center + timedelta(days=10),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center - timedelta(days=10)),
                lookback_window.window_center - timedelta(days=10),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center + timedelta(days=20)),
                lookback_window.window_center + timedelta(days=20),
            ),
        ]

        selected = select_files_in_window(available_files, lookback_window, max_files=4)

        assert len(selected) == 4
        # Should select the 4 closest to center - verify they're all from correct timeframe
        for file in selected:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            # Should be within 5 days of center (the 4 closest)
            assert abs((file.acquisition_time - lookback_window.window_center).days) <= 5

    def test_select_fewer_files_than_max(self):
        """Test when fewer files are available than requested."""
        lookback_window = calculate_lookback_window(datetime(2023, 6, 15, 12, 0, 0), 0, 30)

        available_files = [
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center),
                lookback_window.window_center,
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW2", lookback_window.window_center + timedelta(days=1)),
                lookback_window.window_center + timedelta(days=1),
            ),
        ]

        selected = select_files_in_window(available_files, lookback_window, max_files=10)

        assert len(selected) == 2

    def test_select_no_files_in_window(self):
        """Test when no files fall within the window."""
        lookback_window = calculate_lookback_window(datetime(2023, 6, 15, 12, 0, 0), years_back=0, window_size_days=30)

        # Files outside the window
        jan_time = datetime(2023, 1, 1, 12, 0, 0)
        dec_time = datetime(2023, 12, 31, 12, 0, 0)
        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", jan_time), jan_time),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", dec_time), dec_time),
        ]

        selected = select_files_in_window(available_files, lookback_window, max_files=10)

        assert len(selected) == 0

    def test_select_files_at_window_boundaries(self):
        """Test file selection at exact window boundaries."""
        lookback_window = calculate_lookback_window(datetime(2023, 6, 15, 12, 0, 0), years_back=0, window_size_days=30)

        available_files = [
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_start),
                lookback_window.window_start,
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW2", lookback_window.window_end),
                lookback_window.window_end,
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW3", lookback_window.window_center),
                lookback_window.window_center,
            ),
        ]

        selected = select_files_in_window(available_files, lookback_window, max_files=10)

        assert len(selected) == 3

    def test_files_sorted_by_proximity_to_center(self):
        """Test that returned files are sorted by distance from center."""
        lookback_window = calculate_lookback_window(datetime(2023, 6, 15, 12, 0, 0), years_back=0, window_size_days=30)

        available_files = [
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_center + timedelta(days=10)),
                lookback_window.window_center + timedelta(days=10),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW2", lookback_window.window_center),
                lookback_window.window_center,
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123456", "IW3", lookback_window.window_center + timedelta(days=5)),
                lookback_window.window_center + timedelta(days=5),
            ),
            RtcGranule(
                generate_rtc_granule_id("T031SGR", "123457", "IW1", lookback_window.window_center - timedelta(days=3)),
                lookback_window.window_center - timedelta(days=3),
            ),
        ]

        selected = select_files_in_window(available_files, lookback_window, max_files=10)

        # Check that files are ordered by proximity to center
        distances = [abs((f.acquisition_time - lookback_window.window_center).total_seconds()) for f in selected]
        assert distances == sorted(distances)


class TestCompleteFileSelection:
    """Integration tests for complete file selection across all three windows."""

    def test_standard_configuration(self):
        """Test with standard configuration: w1=8 files, w2=6 files, w3=6 files."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Create mock RTC granules spanning 4 years
        available_files = []
        burst_counter = 123456
        for year in range(2021, 2026):
            for month in [7, 8, 9, 10, 11]:  # Files around September
                for day in [1, 10, 20]:
                    subswath = ["IW1", "IW2", "IW3"][day % 3]
                    acq_time = datetime(year, month, day, 12, 0, 0)
                    granule = RtcGranule(
                        generate_rtc_granule_id("T031SGR", str(burst_counter), subswath, acq_time), acq_time
                    )
                    available_files.append(granule)
                    burst_counter += 1

        # Standard configuration: +/- 60 days, select 8, 6, 6 files
        window_configs = [
            (1, 60, 8),  # w1: 1 year back, +/- 60 days, 8 files
            (2, 60, 6),  # w2: 2 years back, +/- 60 days, 6 files
            (3, 60, 6),  # w3: 3 years back, +/- 60 days, 6 files
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        # Check that we got files from each window
        assert len(w1_files) > 0
        assert len(w2_files) > 0
        assert len(w3_files) > 0

        # Check that we don't exceed max files
        assert len(w1_files) <= 8
        assert len(w2_files) <= 6
        assert len(w3_files) <= 6

        # Check that w1 files are from 2024 and have valid RTC format
        for file in w1_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2024

        # Check that w2 files are from 2023 and have valid RTC format
        for file in w2_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2023

        # Check that w3 files are from 2022 and have valid RTC format
        for file in w3_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2022

    def test_with_sparse_data(self):
        """Test when data is sparse and not all windows can be filled."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Only files in 2024 and 2023, none in 2022
        time_2024_09_25 = datetime(2024, 9, 25, 12, 0, 0)
        time_2024_09_30 = datetime(2024, 9, 30, 12, 0, 0)
        time_2023_09_26 = datetime(2023, 9, 26, 12, 0, 0)

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2024_09_25), time_2024_09_25),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2024_09_30), time_2024_09_30),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2023_09_26), time_2023_09_26),
        ]

        window_configs = [
            (1, 60, 8),
            (2, 60, 6),
            (3, 60, 6),
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        # w1 and w2 should have files, w3 should be empty
        assert len(w1_files) == 2
        assert len(w2_files) == 1
        assert len(w3_files) == 0

    def test_configurable_window_sizes(self):
        """Test with different window sizes for each lookback period."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Create dense mock RTC data
        available_files = []
        burst_counter = 123456
        for year in range(2021, 2026):
            for day_offset in range(-60, 61, 5):  # Every 5 days for +/- 60 days
                subswath = ["IW1", "IW2", "IW3"][burst_counter % 3]

                base_date = datetime(year, 9, 25, 12, 0, 0)
                acq_time = base_date + timedelta(days=day_offset)
                granule = RtcGranule(
                    generate_rtc_granule_id("T031SGR", str(burst_counter), subswath, acq_time), acq_time
                )
                available_files.append(granule)
                burst_counter += 1

        # Different window sizes: w1=15 days, w2=30 days, w3=45 days
        window_configs = [
            (1, 15, 5),  # w1: narrower window
            (2, 30, 5),  # w2: medium window
            (3, 45, 5),  # w3: wider window
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        assert len(w1_files) <= 5
        assert len(w2_files) <= 5
        assert len(w3_files) <= 5

    def test_minimum_files_threshold(self):
        """Test that we can proceed with between 1 and n files."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Very sparse data
        time_2024 = datetime(2024, 9, 20, 12, 0, 0)
        time_2023 = datetime(2023, 9, 26, 12, 0, 0)
        time_2022 = datetime(2022, 10, 1, 12, 0, 0)

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2024), time_2024),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2023), time_2023),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2022), time_2022),
        ]

        window_configs = [
            (1, 60, 8),
            (2, 60, 6),
            (3, 60, 6),
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        # Each window should have 1 file (between 1 and n)
        assert 1 <= len(w1_files) <= 8
        assert 1 <= len(w2_files) <= 6
        assert 1 <= len(w3_files) <= 6


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_empty_file_list(self):
        """Test with no available files."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)
        available_files = []

        window_configs = [
            (1, 60, 8),
            (2, 60, 6),
            (3, 60, 6),
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        # All windows should be empty
        assert len(w1_files) == 0
        assert len(w2_files) == 0
        assert len(w3_files) == 0

    def test_files_exactly_at_center(self):
        """Test when files are exactly at window centers."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Note: 365 days back from 2025-09-25 = 2024-09-25
        time_2024 = datetime(2024, 9, 25, 12, 0, 0)  # w1 center
        time_2023 = datetime(2023, 9, 26, 12, 0, 0)  # w2 center
        time_2022 = datetime(2022, 9, 26, 12, 0, 0)  # w3 center

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2024), time_2024),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2023), time_2023),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2022), time_2022),
        ]

        window_configs = [
            (1, 60, 8),
            (2, 60, 6),
            (3, 60, 6),
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        # Each should select the file at the center
        assert len(w1_files) == 1
        assert len(w2_files) == 1
        assert len(w3_files) == 1
        assert "OPERA_L2_RTC-S1" in w1_files[0].granule_id
        assert "OPERA_L2_RTC-S1" in w2_files[0].granule_id
        assert "OPERA_L2_RTC-S1" in w3_files[0].granule_id

    def test_max_files_zero(self):
        """Test with max_files=0 (should return no files)."""
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        time_2024 = datetime(2024, 9, 25, 12, 0, 0)
        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2024), time_2024),
        ]

        window_configs = [
            (1, 60, 0),  # Request 0 files
            (2, 60, 0),
            (3, 60, 0),
        ]

        w1_files, w2_files, w3_files = select_dist_s1_input_files(t0, available_files, window_configs)

        assert len(w1_files) == 0
        assert len(w2_files) == 0
        assert len(w3_files) == 0


# ============================================================================
# CMR Query Helper Functions (for integration tests)
# ============================================================================


async def query_rtc_granules_for_windows(
    tile_id: str,
    t0: datetime,
    window_configs: List[Tuple[int, int, int]],
    provider: str = "ASF",
    collection: str = "OPERA_L2_RTC-S1_V1",
    bbox: Optional[str] = None,
) -> GranuleList:
    """
    Query CMR for RTC granules within specific lookback windows.

    This function queries only the time ranges needed for the lookback windows,
    making it much more efficient than querying years of data.

    Args:
        tile_id: MGRS tile ID (e.g., "T031SGR" or "T168")
        t0: Reference time for lookback calculation
        window_configs: List of (years_back, window_size_days, max_files) tuples
        provider: CMR provider (default "ASF")
        collection: Collection shortname (default "OPERA_L2_RTC-S1_V1")
        bbox: Bounding box in format "west,south,east,north" (optional but recommended for performance)

    Returns:
        Combined list of RtcGranule objects from all windows
    """
    all_granules = []

    # Query each window separately
    for years_back, window_size_days, max_files in window_configs:
        lookback_window = calculate_lookback_window(t0, years_back, window_size_days)

        # Create time range for this specific window
        timerange = DateTimeRange(
            start_date=lookback_window.window_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date=lookback_window.window_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        print(f"  Querying window w{years_back}: {timerange.start_date} to {timerange.end_date}")

        # Query CMR without token (RTC-S1 is public data)
        cmr_results = await async_query_cmr_v2(
            timerange=timerange, provider=provider, collection=collection, token=None, bbox=bbox
        )

        print(f"    Found {len(cmr_results)} CMR results")

        # Debug: print first few granule IDs
        if cmr_results and len(all_granules) == 0:
            print("    Sample granule IDs from CMR:")
            for result in cmr_results[:3]:
                umm = result.get("umm", {})
                granule_id = umm.get("GranuleUR")
                print(f"      {granule_id}")

        # Convert CMR results to RtcGranule objects
        # CMR returns raw UMM-JSON format when convert_results=False
        for result in cmr_results:
            # Extract granule ID from UMM-JSON
            umm = result.get("umm", {})
            granule_id = umm.get("GranuleUR")
            if not granule_id:
                continue

            # Filter by tile ID
            if not _granule_matches_tile(granule_id, tile_id):
                continue

            # Extract acquisition time from UMM-JSON
            acquisition_time = _extract_acquisition_time_from_umm(umm)
            if not acquisition_time:
                continue

            all_granules.append(RtcGranule(granule_id, acquisition_time))

    print(f"  Total granules after filtering: {len(all_granules)}")
    return all_granules


def _granule_matches_tile(granule_id: str, tile_id: str) -> bool:
    """
    Check if a granule ID matches the given tile ID.

    Args:
        granule_id: RTC granule ID (e.g., "OPERA_L2_RTC-S1_T168-359429-IW2_...")
        tile_id: MGRS tile ID to match (e.g., "T168" or "168")

    Returns:
        True if the granule belongs to the tile
    """
    # Simple approach: just check if the tile_id appears in the granule_id
    # RTC granules have format: OPERA_L2_RTC-S1_T{tile}-{burst}-{subswath}_...
    # So for tile "T168" or "168", we look for "T168-" or "_T168-" in the granule ID

    # Normalize tile_id to have T prefix
    if not tile_id.startswith("T"):
        tile_id = f"T{tile_id}"

    # Check if tile appears in the granule ID (after the product name and before burst)
    # Looking for pattern like "_T168-" or "T168-"
    return f"_{tile_id}-" in granule_id or f"S1_{tile_id}-" in granule_id


def _extract_acquisition_time_from_umm(umm: dict) -> Optional[datetime]:
    """
    Extract acquisition time from UMM-JSON metadata.

    Args:
        umm: UMM section of CMR response

    Returns:
        Acquisition time as naive datetime (UTC), or None if not found
    """
    # Try TemporalExtent for acquisition time
    temporal_extent = umm.get("TemporalExtent", {})

    # Check RangeDateTime first
    range_datetime = temporal_extent.get("RangeDateTime")
    if range_datetime:
        time_str = range_datetime.get("BeginningDateTime")
        if time_str:
            try:
                dt = dateutil.parser.isoparse(time_str)
                # Convert to naive UTC (remove timezone info)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except (ValueError, TypeError):
                pass

    # Fallback to SingleDateTime
    time_str = temporal_extent.get("SingleDateTime")
    if time_str:
        try:
            dt = dateutil.parser.isoparse(time_str)
            # Convert to naive UTC (remove timezone info)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            pass

    # Last resort: try ProductionDateTime
    data_granule = umm.get("DataGranule", {})
    time_str = data_granule.get("ProductionDateTime")
    if time_str:
        try:
            dt = dateutil.parser.isoparse(time_str)
            # Convert to naive UTC (remove timezone info)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            pass

    return None


# ============================================================================
# CMR Integration Tests (with real CMR data)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.cmr
class TestDistS1WithCMRData:
    """CMR integration tests - verify lookback window selection with real CMR data."""

    async def test_select_files_real_data_single_tile(self):
        """Test file selection with real CMR data for a single tile."""
        # Select tile with good data coverage
        tile_id = "T102"
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Standard DIST-S1 configuration
        window_configs = [(1, 60, 8), (2, 60, 6), (3, 60, 6)]

        # Use bounding box for small region in Alaska to speed up CMR query
        bbox = "-156,62,-155,62.5"

        print(f"\nQuerying CMR for tile {tile_id} with t0={t0.isoformat()}")
        print(f"Using bounding box: {bbox}")
        available_granules = await query_rtc_granules_for_windows(tile_id, t0, window_configs, bbox=bbox)
        print(f"Found {len(available_granules)} granules for tile {tile_id}")

        # Verify we got some data
        assert len(available_granules) > 0, f"No granules found for tile {tile_id} in date range"

        # Print sample of granules for debugging
        if len(available_granules) > 0:
            print("\nSample granules:")
            for g in available_granules[:5]:
                print(f"  {g.granule_id}")
                print(f"    Acquisition: {g.acquisition_time.isoformat()}")

        w1, w2, w3 = select_dist_s1_input_files(t0, available_granules, window_configs)

        # Print results
        print("\nSelection results:")
        print(f"  w1 (t0-1yr, ±60d, max 8): {len(w1)} files")
        print(f"  w2 (t0-2yr, ±60d, max 6): {len(w2)} files")
        print(f"  w3 (t0-3yr, ±60d, max 6): {len(w3)} files")

        # Assertions - at least one window should have data
        total_files = len(w1) + len(w2) + len(w3)
        assert total_files > 0, "Should find at least some files across all windows"

        # Verify all selected files are RTC granules and match tile
        for window_name, granules in [("w1", w1), ("w2", w2), ("w3", w3)]:
            for g in granules:
                assert g.granule_id.startswith(
                    "OPERA_L2_RTC-S1"
                ), f"Granule {g.granule_id} in {window_name} should be RTC granule"
                assert tile_id in g.granule_id, f"Granule {g.granule_id} in {window_name} should contain tile {tile_id}"

        # Verify files are sorted by proximity to window center
        for window_name, granules, years_back, window_size_days in [
            ("w1", w1, 1, 60),
            ("w2", w2, 2, 60),
            ("w3", w3, 3, 60),
        ]:
            if len(granules) > 1:
                lookback_window = calculate_lookback_window(t0, years_back, window_size_days)
                distances = [
                    abs((g.acquisition_time - lookback_window.window_center).total_seconds()) for g in granules
                ]
                assert distances == sorted(distances), f"Files in {window_name} should be sorted by proximity to center"

        print("\n✓ Test passed: File selection algorithm works correctly with real CMR data")

    async def test_leap_year_with_real_data(self):
        """Test leap year handling with actual data."""
        # Use t0 = Feb 29, 2024 (leap year)
        tile_id = "T102"
        t0 = datetime(2024, 2, 29, 12, 0, 0)

        # Standard DIST-S1 configuration
        window_configs = [(1, 60, 8), (2, 60, 6), (3, 60, 6)]

        # Use bounding box for small region in Alaska
        bbox = "-156,62,-155,62.5"

        print(f"\nTesting leap year handling with t0 = {t0.isoformat()}...")
        available_granules = await query_rtc_granules_for_windows(tile_id, t0, window_configs, bbox=bbox)
        print(f"Found {len(available_granules)} granules")

        if len(available_granules) == 0:
            pytest.skip(f"No data found for tile {tile_id}")

        # Calculate lookback windows
        w1_window = calculate_lookback_window(t0, years_back=1, window_size_days=60)
        w2_window = calculate_lookback_window(t0, years_back=2, window_size_days=60)
        w3_window = calculate_lookback_window(t0, years_back=3, window_size_days=60)

        print("\nLookback window centers:")
        print(f"  w1: {w1_window.window_center.isoformat()}")
        print(f"  w2: {w2_window.window_center.isoformat()}")
        print(f"  w3: {w3_window.window_center.isoformat()}")

        # Verify centers are calculated correctly (365 days back, not trying to match Feb 29)
        # From 2024-02-29, 365 days back = 2023-03-01
        assert w1_window.window_center == datetime(2023, 3, 1, 12, 0, 0)

        # Run selection
        w1, w2, w3 = select_dist_s1_input_files(t0, available_granules, window_configs)

        print("\nSelection results:")
        print(f"  w1: {len(w1)} files")
        print(f"  w2: {len(w2)} files")
        print(f"  w3: {len(w3)} files")

        print("\n✓ Leap year handling works correctly with real data")

    async def test_max_files_limit_enforced(self):
        """Test that max_files limit is respected with real data."""
        tile_id = "T102"
        t0 = datetime(2025, 9, 25, 12, 0, 0)

        # Use strict limits
        window_configs = [(1, 60, 3), (2, 60, 2), (3, 60, 1)]

        # Use bounding box for small region in Alaska
        bbox = "-156,62,-155,62.5"

        print("\nTesting max_files enforcement...")
        available_granules = await query_rtc_granules_for_windows(tile_id, t0, window_configs, bbox=bbox)
        print(f"Found {len(available_granules)} granules")

        if len(available_granules) == 0:
            pytest.skip(f"No data found for tile {tile_id}")

        w1, w2, w3 = select_dist_s1_input_files(t0, available_granules, window_configs)

        print("\nSelection results with limits [3, 2, 1]:")
        print(f"  w1: {len(w1)} files (max 3)")
        print(f"  w2: {len(w2)} files (max 2)")
        print(f"  w3: {len(w3)} files (max 1)")

        # Verify limits are enforced
        assert len(w1) <= 3, "w1 should have at most 3 files"
        assert len(w2) <= 2, "w2 should have at most 2 files"
        assert len(w3) <= 1, "w3 should have at most 1 file"

        print("\n✓ Max files limit is correctly enforced")
