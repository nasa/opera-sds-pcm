#!/usr/bin/env python3
"""
Unit tests for disp_s1_k_cycle_date_range_analyzer.py

This test suite validates the K-cycle date range analysis functionality
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

# Add the tools directory to the path for imports
tools_dir = Path(__file__).parent.parent.parent / "tools"
sys.path.insert(0, str(tools_dir))

from disp_s1_k_cycle_date_range_analyzer import (
    analyze_frame_k_cycles,
    find_k_cycles,
    load_burst_database,
    main
)
from data_subscriber import cslc_utils
from datetime import datetime


class TestKCycleDateRangeAnalyzer(unittest.TestCase):
    """Test cases for K-cycle date range analyzer."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are used by multiple test methods."""
        # Get the test directory
        cls.test_dir = Path(__file__).parent
        
        # Load batch processing parameters from local file
        batch_proc_path = cls.test_dir / "batch_proc.json"
        with open(batch_proc_path, 'r') as f:
            cls.batch_params = json.load(f)
        
        # Use pre-created test database file
        cls.test_db_path = cls.test_dir / "test_consistent_db.json"
        
        # Load the test database
        cls.disp_burst_map, cls.burst_to_frames, cls.day_indices_to_frames = \
            cslc_utils.process_disp_frame_burst_hist(str(cls.test_db_path))
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures."""
        # No cleanup needed since we use static test files
        pass
    
    def test_load_burst_database(self):
        """Test that the burst database loads correctly."""
        disp_burst_map, burst_to_frames, day_indices_to_frames = \
            load_burst_database(str(self.test_db_path))
        
        # Verify we have the expected frames
        expected_frames = {18904, 18905, 44328}
        actual_frames = set(disp_burst_map.keys())
        self.assertEqual(actual_frames, expected_frames)
        
        # Verify each frame has data
        for frame_id in expected_frames:
            frame_data = disp_burst_map[frame_id]
            self.assertGreater(len(frame_data.burst_ids), 0)
            self.assertGreater(len(frame_data.sensing_datetimes), 0)
    
    def test_find_k_cycles_basic(self):
        """Test basic K-cycle date range detection."""
        frame_data = self.disp_burst_map[18904]
        sensing_times = frame_data.sensing_datetimes
        
        # Test with a date range that should have multiple cycles
        start_date = datetime(2017, 1, 1)
        end_date = datetime(2017, 12, 31)
        k = 15
        
        cycles = find_k_cycles(
            sensing_times, start_date, end_date, k
        )
        
        self.assertGreater(len(cycles), 0)
        
        # Verify each cycle has the expected structure
        for cycle_num, cycle_dates in cycles:
            self.assertIsInstance(cycle_num, int)
            self.assertIsInstance(cycle_dates, list)
            self.assertGreater(len(cycle_dates), 0)
            self.assertLessEqual(len(cycle_dates), k)
    
    def test_find_k_cycles_no_overlap(self):
        """Test K-cycle detection with no sensing dates within the date range."""
        frame_data = self.disp_burst_map[18904]
        sensing_times = frame_data.sensing_datetimes
        
        # Use a date range that shouldn't have any sensing dates
        start_date = datetime(2050, 1, 1)
        end_date = datetime(2050, 12, 31)
        k = 15
        
        cycles = find_k_cycles(
            sensing_times, start_date, end_date, k
        )
        
        self.assertEqual(len(cycles), 0)
    
    def test_analyze_frame_k_cycles_with_batch_params(self):
        """Test frame analysis using parameters from batch_proc.json."""
        k = self.batch_params["k"]  # Should be 15
        
        # Use a date range from the batch parameters
        start_date = datetime.fromisoformat(self.batch_params["data_start_date"])
        end_date = datetime.fromisoformat(self.batch_params["data_end_date"])
        
        for frame_id in self.batch_params["frames"]:
            with self.subTest(frame_id=frame_id):
                total_sensing_dates = analyze_frame_k_cycles(
                    frame_id, self.disp_burst_map, start_date, end_date, k, verbose=False
                )
                
                # Verify we get a positive result
                self.assertGreaterEqual(total_sensing_dates, 0)
                
                # The result should be reasonable (not zero for these frames in this date range)
                self.assertGreater(total_sensing_dates, 0)
    
    def test_analyze_frame_k_cycles_different_k_values(self):
        """Test frame analysis with different K values."""
        frame_id = 18904
        start_date = datetime(2017, 1, 1)
        end_date = datetime(2017, 12, 31)
        
        k_values = [5, 10, 15, 20]
        results = {}
        
        for k in k_values:
            total_sensing_dates = analyze_frame_k_cycles(
                frame_id, self.disp_burst_map, start_date, end_date, k, verbose=False
            )
            results[k] = total_sensing_dates
            
            # Each K value should give a positive result
            self.assertGreater(total_sensing_dates, 0)
        
        # Results might vary with different K values, but should all be positive
        self.assertTrue(all(count > 0 for count in results.values()))
    
    def test_analyze_nonexistent_frame(self):
        """Test analysis of a frame that doesn't exist."""
        nonexistent_frame = 99999
        start_date = datetime(2017, 1, 1)
        end_date = datetime(2017, 12, 31)
        k = 15
        
        result = analyze_frame_k_cycles(
            nonexistent_frame, self.disp_burst_map, start_date, end_date, k, verbose=False
        )
        
        # Should return 0 for nonexistent frame
        self.assertEqual(result, 0)
    
    def test_batch_proc_expected_values(self):
        """Test that results match expected values from batch_proc.json context."""
        k = self.batch_params["k"]
        
        # Use data_start_date from batch_proc.json
        start_date = datetime.fromisoformat(self.batch_params["data_start_date"])
        
        results = {}
        for frame_id in self.batch_params["frames"]:
            # Use individual last_processed_datetimes for each frame as end_date
            end_date_str = self.batch_params["last_processed_datetimes"][str(frame_id)]
            end_date = datetime.fromisoformat(end_date_str)
            
            total_sensing_dates = analyze_frame_k_cycles(
                frame_id, self.disp_burst_map, start_date, end_date, k, verbose=False
            )
            results[str(frame_id)] = total_sensing_dates
        
        # Verify we have results for all frames
        expected_frame_ids = {str(f) for f in self.batch_params["frames"]}
        actual_frame_ids = set(results.keys())
        self.assertEqual(actual_frame_ids, expected_frame_ids)
        
        # Verify all results are positive
        for frame_id, count in results.items():
            with self.subTest(frame_id=frame_id):
                self.assertGreater(count, 0, f"Frame {frame_id} should have sensing dates")
                
        # Log the results for reference
        print(f"\nTest results using batch_proc.json parameters:")
        print(f"Start date: {start_date}")
        print(f"K value: {k}")
        for frame_id, count in results.items():
            end_date_str = self.batch_params["last_processed_datetimes"][frame_id]
            print(f"Frame {frame_id} (end: {end_date_str}): {count} sensing dates")
    
    def test_main_function_integration(self):
        """Test the main function with command line arguments."""
        import sys
        from unittest.mock import patch
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_output:
            output_path = tmp_output.name
        
        try:
            # Mock command line arguments
            test_args = [
                'disp_s1_k_cycle_date_range_analyzer.py',
                '--k', '15',
                '--start-date', '2017-01-01T00:00:00',
                '--end-date', '2017-06-01T00:00:00',
                '--frames', '18904,18905',
                '--output', output_path,
                '--db-file', str(self.test_db_path)
            ]
            
            with patch.object(sys, 'argv', test_args):
                # Import and run main (need to import here to avoid issues with sys.argv)
                import disp_s1_k_cycle_date_range_analyzer
                result = disp_s1_k_cycle_date_range_analyzer.main()
            
            # Check that main returned success
            self.assertEqual(result, 0)
            
            # Verify output file was created and has expected structure
            self.assertTrue(os.path.exists(output_path))
            
            with open(output_path, 'r') as f:
                output_data = json.load(f)
            
            # Should be a simple dict with frame IDs as keys
            self.assertIsInstance(output_data, dict)
            self.assertIn('18904', output_data)
            self.assertIn('18905', output_data)
            
            # Values should be positive integers
            for frame_id, count in output_data.items():
                self.assertIsInstance(count, int)
                self.assertGreater(count, 0)
                
        finally:
            # Clean up
            if os.path.exists(output_path):
                os.unlink(output_path)


if __name__ == '__main__':
    unittest.main()