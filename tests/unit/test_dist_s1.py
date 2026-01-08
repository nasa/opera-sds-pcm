#!/usr/bin/env python3
"""
Unit tests for DIST-S1 input file selection logic.

This module tests the lookback window calculation and file selection logic
for the DIST-S1 algorithm. For a given time t0, we need to select files from
three lookback windows:
- w1: centered at t0 - 1 year (e.g., +/- 60 days)
- w2: centered at t0 - 2 years (e.g., +/- 60 days)
- w3: centered at t0 - 3 years (e.g., +/- 60 days)

Files are selected as the n closest files to the center of each window.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple


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


# Type alias for file lists
FileList = List[RtcGranule]


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


def select_files_in_window(available_files: FileList, lookback_window: LookbackWindow, max_files: int) -> FileList:
    """
    Select files within a window, choosing those closest to the window center.

    Args:
        available_files: FileList of available files
        window_start: Start of the window
        window_center: Center of the window
        window_end: End of the window
        max_files: Maximum number of files to select

    Returns:
        FileList of selected files, sorted by proximity to center
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
    t0: datetime, available_files: FileList, window_configs: List[Tuple[int, int, int]]
) -> Tuple[FileList, FileList, FileList]:
    """
    Select input files for DIST-S1 algorithm across three lookback windows.

    Args:
        t0: Reference time
        available_files: FileList of available files
        window_configs: List of (years_back, window_size_days, max_files) tuples
                       for w1, w2, w3 respectively

    Returns:
        Tuple of (w1_files, w2_files, w3_files) as FileLists

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
        t0 = datetime(2024, 6, 15, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=window_size_days)

        # 365 days back from 2024-06-15 (leap year) = 2023-06-16
        assert lookback_window.window_center == datetime(2023, 6, 16, 12, 0, 0)
        assert lookback_window.window_start == datetime(2023, 4, 17, 12, 0, 0)
        assert lookback_window.window_end == datetime(2023, 8, 15, 12, 0, 0)

    def test_window_2_years_back(self):
        """Test calculating window centered at t0 - 2 years."""
        t0 = datetime(2024, 6, 15, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=2, window_size_days=window_size_days)

        # 730 days back from 2024-06-15 (leap year) = 2022-06-16
        assert lookback_window.window_center == datetime(2022, 6, 16, 12, 0, 0)
        assert lookback_window.window_start == datetime(2022, 4, 17, 12, 0, 0)
        assert lookback_window.window_end == datetime(2022, 8, 15, 12, 0, 0)

    def test_window_3_years_back(self):
        """Test calculating window centered at t0 - 3 years."""
        t0 = datetime(2024, 6, 15, 12, 0, 0)
        window_size_days = 60

        lookback_window = calculate_lookback_window(t0, years_back=3, window_size_days=window_size_days)

        # 1095 days back from 2024-06-15 = 2021-06-16
        assert lookback_window.window_center == datetime(2021, 6, 16, 12, 0, 0)
        assert lookback_window.window_start == datetime(2021, 4, 17, 12, 0, 0)
        assert lookback_window.window_end == datetime(2021, 8, 15, 12, 0, 0)

    def test_different_window_sizes(self):
        """Test windows with different sizes."""
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Test with 15-day window (center = 2023-06-16)
        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=15)
        assert lookback_window.window_start == datetime(2023, 6, 1, 12, 0, 0)
        assert lookback_window.window_end == datetime(2023, 7, 1, 12, 0, 0)

        # Test with 45-day window
        lookback_window = calculate_lookback_window(t0, years_back=1, window_size_days=45)
        assert lookback_window.window_start == datetime(2023, 5, 2, 12, 0, 0)
        assert lookback_window.window_end == datetime(2023, 7, 31, 12, 0, 0)

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
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", lookback_window.window_start), lookback_window.window_start),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW2", lookback_window.window_end), lookback_window.window_end),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW3", lookback_window.window_center), lookback_window.window_center),
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
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW2", lookback_window.window_center), lookback_window.window_center),
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Create mock RTC granules spanning 4 years
        available_files = []
        burst_counter = 123456
        for year in range(2020, 2025):
            for month in [4, 5, 6, 7, 8]:  # Files around June
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

        # Check that w1 files are from 2023 and have valid RTC format
        for file in w1_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2023

        # Check that w2 files are from 2022 and have valid RTC format
        for file in w2_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2022

        # Check that w3 files are from 2021 and have valid RTC format
        for file in w3_files:
            assert "OPERA_L2_RTC-S1" in file.granule_id
            assert file.acquisition_time.year == 2021

    def test_with_sparse_data(self):
        """Test when data is sparse and not all windows can be filled."""
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Only files in 2023 and 2022, none in 2021
        time_2023_06_15 = datetime(2023, 6, 15, 12, 0, 0)
        time_2023_06_20 = datetime(2023, 6, 20, 12, 0, 0)
        time_2022_06_15 = datetime(2022, 6, 15, 12, 0, 0)

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2023_06_15), time_2023_06_15),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2023_06_20), time_2023_06_20),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2022_06_15), time_2022_06_15),
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Create dense mock RTC data
        available_files = []
        burst_counter = 123456
        for year in range(2020, 2025):
            for day_offset in range(-60, 61, 5):  # Every 5 days for +/- 60 days
                subswath = ["IW1", "IW2", "IW3"][burst_counter % 3]

                base_date = datetime(year, 6, 15, 12, 0, 0)
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Very sparse data
        time_2023 = datetime(2023, 6, 10, 12, 0, 0)
        time_2022 = datetime(2022, 6, 15, 12, 0, 0)
        time_2021 = datetime(2021, 6, 20, 12, 0, 0)

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2023), time_2023),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2022), time_2022),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2021), time_2021),
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        # Note: 365 days back from 2024-06-15 = 2023-06-16 (due to leap year)
        time_2023 = datetime(2023, 6, 16, 12, 0, 0)  # w1 center
        time_2022 = datetime(2022, 6, 16, 12, 0, 0)  # w2 center
        time_2021 = datetime(2021, 6, 16, 12, 0, 0)  # w3 center

        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2023), time_2023),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123457", "IW1", time_2022), time_2022),
            RtcGranule(generate_rtc_granule_id("T031SGR", "123458", "IW1", time_2021), time_2021),
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
        t0 = datetime(2024, 6, 15, 12, 0, 0)

        time_2023 = datetime(2023, 6, 15, 12, 0, 0)
        available_files = [
            RtcGranule(generate_rtc_granule_id("T031SGR", "123456", "IW1", time_2023), time_2023),
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
