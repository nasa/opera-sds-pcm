#!/usr/bin/env python3
"""
Unit tests for disp_s1_k_cycle_date_analyzer.py

This test suite validates the K-cycle date analysis functionality
using a smaller test database and parameters from batch_proc.json.

Test Files:
- batch_proc.json: Contains test parameters (K=15, frames=[18904, 18905, 44328])
- test_consistent_db.json: Smaller version of the consistent burst database
  containing only the test frames (26KB vs 7.4MB original)
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
import sys

# tests/unit/tools/<this file> -> parents[3] is the repository root
tools_dir = Path(__file__).parents[3] / "tools"
sys.path.insert(0, str(tools_dir))

from disp_s1_k_cycle_date_analyzer import (
    analyze_frame_k_cycles,
    find_k_cycles,
    find_phased_k_cycles,
    load_burst_database,
    main,
)
from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import PhaseKind
from datetime import datetime


class TestKCycleDateAnalyzer(unittest.TestCase):
    """Test cases for K-cycle date analyzer."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are used by multiple test methods."""
        # Get the test directory
        # the json fixtures stay under tests/tools/, shared with the historical
        # processing tests; only this module moved into the CI test path
        cls.test_dir = Path(__file__).parents[2] / "tools"

        # Load batch processing parameters from local file
        batch_proc_path = cls.test_dir / "batch_proc.json"
        with open(batch_proc_path, "r") as f:
            cls.batch_params = json.load(f)

        # Expected values for the test database
        cls.expected_values = {
            "16669": {"burst_ids": 27, "sensing_datetimes": 239},
            "18904": {"burst_ids": 26, "sensing_datetimes": 315},
            "18905": {"burst_ids": 14, "sensing_datetimes": 324},
            "44328": {"burst_ids": 27, "sensing_datetimes": 152},
            "46294": {"burst_ids": 1, "sensing_datetimes": 300},
        }

        # Use pre-created test database file
        cls.test_db_path = cls.test_dir / "test_consistent_db.json"

        # Load the test database
        cls.disp_burst_map, cls.burst_to_frames, cls.day_indices_to_frames = (
            cslc_utils.process_disp_frame_burst_hist(str(cls.test_db_path))
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures."""
        # No cleanup needed since we use static test files
        pass

    def test_load_burst_database(self):
        """Test that the burst database loads correctly."""
        disp_burst_map, burst_to_frames, day_indices_to_frames = load_burst_database(
            str(self.test_db_path)
        )

        # Verify we have the expected frames
        expected_frames = {16669, 18904, 18905, 44328, 46294}
        actual_frames = set(disp_burst_map.keys())
        self.assertEqual(actual_frames, expected_frames)

        # Verify each frame has the expected number of burst_ids and sensing_datetimes
        for frame_id in expected_frames:
            frame_data = disp_burst_map[frame_id]
            self.assertEqual(
                len(frame_data.burst_ids),
                self.expected_values[str(frame_id)]["burst_ids"],
            )
            self.assertEqual(
                len(frame_data.sensing_datetimes),
                self.expected_values[str(frame_id)]["sensing_datetimes"],
            )

    def test_find_k_cycles_basic(self):
        """Test basic K-cycle date detection."""
        frame_data = self.disp_burst_map[18904]
        sensing_times = frame_data.sensing_datetimes

        # Test that the frame should have multiple cycles
        end_date = datetime(2017, 12, 31)
        k = 15

        cycles = find_k_cycles(sensing_times, end_date, k)

        self.assertGreater(len(cycles), 0)

        # Verify each cycle has the expected structure
        for cycle_num, cycle_dates in cycles:
            self.assertIsInstance(cycle_num, int)
            self.assertIsInstance(cycle_dates, list)
            self.assertGreater(len(cycle_dates), 0)
            self.assertLessEqual(len(cycle_dates), k)

    def test_analyze_frame_k_cycles_different_k_values(self):
        """Test frame analysis with different K values."""
        frame_id = 18904
        end_date = datetime(2017, 12, 31)

        k_values = [5, 10, 15, 20]
        expected_frame_states = [30, 30, 30, 20]
        results = {}

        for k in k_values:
            frame_state = analyze_frame_k_cycles(
                frame_id, self.disp_burst_map, end_date, k, verbose=False
            )
            results[k] = frame_state

            # Each K value should give the expected frame state
            self.assertEqual(frame_state, expected_frame_states[k_values.index(k)])

    def test_analyze_nonexistent_frame(self):
        """Test analysis of a frame that doesn't exist."""
        nonexistent_frame = 99999
        end_date = datetime(2017, 12, 31)
        k = 15

        frame_state = analyze_frame_k_cycles(
            nonexistent_frame, self.disp_burst_map, end_date, k, verbose=False
        )

        # Should return 0 for nonexistent frame
        self.assertEqual(frame_state, 0)

    def test_batch_proc_expected_values(self):
        """Test that results match expected values from batch_proc.json context."""
        k = self.batch_params["k"]

        results = {}
        for frame_id in self.batch_params["frames"]:
            # Use individual last_processed_datetimes for each frame as end_date
            end_date_str = self.batch_params["last_processed_datetimes"][str(frame_id)]
            end_date = datetime.fromisoformat(end_date_str)

            frame_state = analyze_frame_k_cycles(
                frame_id, self.disp_burst_map, end_date, k, verbose=False
            )
            results[str(frame_id)] = frame_state

        # Verify we have results for all frames
        expected_frame_ids = {str(f) for f in self.batch_params["frames"]}
        actual_frame_ids = set(results.keys())
        self.assertEqual(actual_frame_ids, expected_frame_ids)

        # Verify all results are the same as the frame_states in batch_proc.json
        for frame_id, frame_state in results.items():
            with self.subTest(frame_id=frame_id):
                self.assertEqual(
                    frame_state, self.batch_params["frame_states"][frame_id]
                )

        # Log the results for reference
        print(f"\nTest results using batch_proc.json parameters:")
        print(f"K value: {k}")
        for frame_id, frame_state in results.items():
            end_date_str = self.batch_params["last_processed_datetimes"][frame_id]
            print(f"Frame {frame_id} (end: {end_date_str}): {frame_state} frame state")

    def test_main_function_integration(self):
        """Test the main function with command line arguments."""
        import sys
        from unittest.mock import patch

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp_output:
            output_path = tmp_output.name

        try:
            # Mock command line arguments
            test_args = [
                "disp_s1_k_cycle_date_analyzer.py",
                "--k",
                "15",
                "--end-date",
                "2025-12-31T23:59:59",
                "--frames",
                "16669,18904,18905,44328,46294",
                "--output",
                output_path,
                "--db-file",
                str(self.test_db_path),
            ]

            with patch.object(sys, "argv", test_args):
                # Import and run main (need to import here to avoid issues with sys.argv)
                import disp_s1_k_cycle_date_analyzer

                result = disp_s1_k_cycle_date_analyzer.main()

            # Check that main returned success
            self.assertEqual(result, 0)

            # Verify output file was created and has expected structure
            self.assertTrue(os.path.exists(output_path))

            with open(output_path, "r") as f:
                output_data = json.load(f)

            # Should be a simple dict with frame IDs as keys
            self.assertIsInstance(output_data, dict)
            self.assertIn("16669", output_data)
            self.assertIn("18904", output_data)
            self.assertIn("18905", output_data)
            self.assertIn("44328", output_data)
            self.assertIn("46294", output_data)

            # Values should be the expected frame states for end date 2025-12-31T23:59:59
            expected_frame_states = {
                "16669": 225,
                "18904": 315,
                "18905": 315,
                "44328": 150,
                "46294": 300,
            }
            for frame_id, frame_state in output_data.items():
                self.assertEqual(frame_state, expected_frame_states[frame_id])

        finally:
            # Clean up
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_main_function_integration_with_end_date_2017_04_01(self):
        """Test the main function with command line arguments and end date 2017-04-01."""
        import sys
        from unittest.mock import patch

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp_output:
            output_path = tmp_output.name

        try:
            # Mock command line arguments
            test_args = [
                "disp_s1_k_cycle_date_analyzer.py",
                "--k",
                "15",
                "--end-date",
                "2017-04-01T00:00:00",
                "--frames",
                "16669",
                "--output",
                output_path,
                "--db-file",
                str(self.test_db_path),
            ]

            with patch.object(sys, "argv", test_args):
                # Import and run main (need to import here to avoid issues with sys.argv)
                import disp_s1_k_cycle_date_analyzer

                result = disp_s1_k_cycle_date_analyzer.main()

            # Check that main returned success
            self.assertEqual(result, 0)

            # Verify output file was created and has expected structure
            self.assertTrue(os.path.exists(output_path))

            with open(output_path, "r") as f:
                output_data = json.load(f)

            # Should be a simple dict with frame IDs as keys
            self.assertIsInstance(output_data, dict)
            self.assertIn("16669", output_data)

            # Values should be the expected frame states for end date 2017-04-01T00:00:00
            expected_frame_states = {"16669": 15}
            for frame_id, frame_state in output_data.items():
                self.assertEqual(frame_state, expected_frame_states[frame_id])

        finally:
            # Clean up
            if os.path.exists(output_path):
                os.unlink(output_path)


class TestPhasedKCycleDateAnalyzer(unittest.TestCase):
    """K-cycle analysis of a processing-mode-annotated database.

    k-sets restart at every historical phase, so the groups reported have to sit at
    phase-relative positions -- range(phase.start_pos, phase.end_pos, k) -- and not on the
    absolute grid from position 0, which is what the phased walk would actually submit.
    """

    K = 15

    @classmethod
    def setUpClass(cls):
        db = (Path(__file__).parents[1] / "data_subscriber" / "test_data"
              / "disp_s1_consistent_db_with_modes.json")
        # explicitly on, so the test does not depend on DISP_S1_PROCESSING_MODE_ENABLED
        cls.disp_burst_map, _, _ = load_burst_database(str(db), True)
        cls.end_date = datetime(2030, 1, 1)

    def groups_for(self, frame_id, end_date=None):
        frame = self.disp_burst_map[frame_id]
        return find_phased_k_cycles(frame.sensing_datetimes, frame.phases,
                                    end_date or self.end_date, self.K)

    def test_ksets_restart_at_each_historical_phase(self):
        """frame 16669: historical_02 starts at 206, so its k-sets are 206 and 221."""
        groups, _ = self.groups_for(16669)

        starts = [g.start_pos for g in groups if g.label == "historical_02"]
        self.assertEqual(starts, [206, 221])
        # the absolute grid would have put them at 195 and 210
        self.assertNotIn(195, starts)

        for group in groups:
            if group.kind is PhaseKind.HISTORICAL:
                self.assertEqual(len(group.dates), self.K)

    def test_forward_dates_are_reported_individually(self):
        """forward_NN dates are driven one at a time and are not k-sets."""
        groups, _ = self.groups_for(16669)

        forward = [g for g in groups if g.label == "forward_01"]
        self.assertEqual(len(forward), 11)
        self.assertEqual([g.start_pos for g in forward], list(range(195, 206)))
        for group in forward:
            self.assertEqual(len(group.dates), 1)
            self.assertFalse(group.skipped)

    def test_no_run_block_is_skipped_but_stepped_over(self):
        """frame 44328 ends in a no_run block: reported as skipped, cursor steps past it."""
        groups, cursor = self.groups_for(44328)

        no_run = [g for g in groups if g.skipped]
        self.assertEqual(len(no_run), 1)
        self.assertEqual((no_run[0].label, no_run[0].start_pos, len(no_run[0].dates)),
                         ("no_run", 143, 9))
        self.assertEqual(cursor, 152)

    def test_leading_no_run_shifts_the_kset_grid(self):
        """frame 18905 opens with 4 no_run dates, so k-sets start at 4, not 0."""
        end_date = datetime(2019, 1, 1)
        groups, cursor = self.groups_for(18905, end_date)

        self.assertEqual([g.start_pos for g in groups if not g.skipped], [4, 19, 34, 49])
        self.assertEqual(cursor, 64)

        # what the absolute grid would have reported instead
        absolute = find_k_cycles(self.disp_burst_map[18905].sensing_datetimes, end_date, self.K)
        self.assertEqual(sum(len(dates) for _, dates in absolute), 60)

    def test_frame_of_only_no_run_processes_nothing(self):
        groups, cursor = self.groups_for(46294)

        self.assertTrue(all(g.skipped for g in groups))
        self.assertEqual(cursor, 300)
        self.assertEqual(analyze_frame_k_cycles(46294, self.disp_burst_map, self.end_date, self.K), 300)

    def test_end_date_stops_the_walk(self):
        """A group whose last date is past the end date is not reported."""
        groups, cursor = self.groups_for(18904, datetime(2017, 12, 31))

        self.assertEqual(cursor, 30)
        self.assertEqual([g.start_pos for g in groups], [0, 15])
        self.assertTrue(all(g.dates[-1] <= datetime(2017, 12, 31) for g in groups))

    def test_unannotated_frame_in_an_annotated_db_uses_the_absolute_grid(self):
        """frame 99999 carries a plain sensing_time_list, so nothing changes for it."""
        frame = self.disp_burst_map[99999]
        self.assertFalse(frame.phases)

        state = analyze_frame_k_cycles(99999, self.disp_burst_map, self.end_date, self.K)
        absolute = find_k_cycles(frame.sensing_datetimes, self.end_date, self.K)
        self.assertEqual(state, sum(len(dates) for _, dates in absolute))
        self.assertEqual(state, 15)

    def test_rejected_annotations_fall_back_to_the_absolute_grid(self):
        """A frame quarantined by the phase validator keeps the un-phased behaviour."""
        db = (Path(__file__).parents[1] / "data_subscriber" / "test_data"
              / "disp_s1_consistent_db_malformed_modes.json")
        malformed, _, _ = load_burst_database(str(db), True)

        frame = malformed[1002]
        self.assertIsNone(frame.phases)
        self.assertTrue(frame.phase_error)

        state = analyze_frame_k_cycles(1002, malformed, self.end_date, self.K)
        absolute = find_k_cycles(frame.sensing_datetimes, self.end_date, self.K)
        self.assertEqual(state, sum(len(dates) for _, dates in absolute))


if __name__ == "__main__":
    unittest.main()
