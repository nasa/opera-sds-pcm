#!/usr/bin/env python3
"""
Example usage script for CCSLC Deletion Utility

This script demonstrates various ways to use the CCSLC deletion utility
for different scenarios in DISP-S1 reprocessing workflows.
"""

import subprocess
import sys
from datetime import datetime, timedelta

def run_command(cmd, description):
    """Run a command and display the result."""
    print(f"\n{'='*60}")
    print(f"Example: {description}")
    print(f"{'='*60}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Return code: {result.returncode}")
    except Exception as e:
        print(f"Error running command: {e}")

def main():
    """Run example commands for CCSLC deletion utility."""
    
    print("CCSLC Deletion Utility - Usage Examples")
    print("=" * 60)
    print("This script demonstrates various usage patterns for the CCSLC deletion utility.")
    print("All examples use --dry-run to prevent actual deletions.")
    print("=" * 60)
    
    # Example 1: Delete by frame IDs
    run_command(
        "python tools/ccslc_deletion_utility.py frames --frame-ids 10859,10860 --dry-run --verbose",
        "Delete CCSLC data for specific frame IDs"
    )
    
    # Example 2: Delete by date range
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    run_command(
        f"python tools/ccslc_deletion_utility.py date-range --start-date {start_date} --end-date {end_date} --dry-run --verbose",
        "Delete CCSLC data within a date range (last 30 days)"
    )
    
    # Example 3: Delete by burst IDs
    run_command(
        "python tools/ccslc_deletion_utility.py bursts --burst-ids T175-374393-IW1,T175-374394-IW1 --dry-run --verbose",
        "Delete CCSLC data for specific burst IDs"
    )
    
    # Example 4: Delete by granule IDs
    run_command(
        "python tools/ccslc_deletion_utility.py granules --granule-ids 'OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0' --dry-run --verbose",
        "Delete CCSLC data for specific granule IDs"
    )
    
    # Example 5: Help command
    run_command(
        "python tools/ccslc_deletion_utility.py --help",
        "Display help information"
    )
    
    print(f"\n{'='*60}")
    print("Examples completed!")
    print("=" * 60)
    print("Note: All examples used --dry-run to prevent actual deletions.")
    print("To perform actual deletions, remove the --dry-run flag and")
    print("ensure you have proper permissions and confirmation.")
    print("=" * 60)

if __name__ == "__main__":
    main()
