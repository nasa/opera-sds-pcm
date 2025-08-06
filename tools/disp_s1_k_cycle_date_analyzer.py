#!/usr/bin/env python3
"""
K-Cycle Date Analyzer

This script analyzes K-cycle groups prior to a specified end date for given frames
using the OPERA DISP-S1 consistent burst database. It determines which K-cycle
groups have sensing dates before the specified end date and calculates the total
number of sensing dates within those groups.

Usage:
    disp_s1_k_cycle_date_analyzer.py --k 15 --end-date 2020-12-31T23:59:59 --frames 831,832,833 --output results.json

Requirements:
    - PATH should include the directory containing this script
    - PYTHONPATH should include the opera-sds-pcm directory
"""

import argparse
import json
import logging
import math
from datetime import datetime
from typing import Dict, List, Tuple

# Reuse imports from disp_s1_burst_db_tool.py
from data_subscriber import cslc_utils

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyze K-cycle groups prior to a specified end date for specified frames"
    )

    parser.add_argument(
        "--k", type=int, required=True, help="Number of K acquisitions per grouping"
    )

    parser.add_argument(
        "--end-date",
        dest="end_date",
        required=True,
        help="Sensing end date (ISO format: YYYY-MM-DDTHH:MM:SS)",
    )

    parser.add_argument(
        "--frames",
        required=True,
        help="Comma-separated list of frame numbers to analyze (e.g., 831,832,833)",
    )

    parser.add_argument("--output", required=True, help="Output JSON file path")

    parser.add_argument(
        "--db-file",
        dest="db_file",
        help="Specify the DISP-S1 database json file on the local file system instead of using the standard one in S3 ancillary",
        required=False,
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    return parser.parse_args()


def load_burst_database(db_file=None):
    """Load the DISP-S1 burst database."""
    if db_file:
        logger.info(f"Using local DISP-S1 database json file: {db_file}")
        disp_burst_map, burst_to_frames, day_indices_to_frames = (
            cslc_utils.process_disp_frame_burst_hist(db_file)
        )
    else:
        disp_burst_map, burst_to_frames, day_indices_to_frames = (
            cslc_utils.localize_disp_frame_burst_hist()
        )

    return disp_burst_map, burst_to_frames, day_indices_to_frames


def find_k_cycles(
    sensing_datetimes: List[datetime], end_date: datetime, k: int
) -> List[Tuple[int, List[datetime]]]:
    """
    Find K-cycle groups with sensing dates prior to the specified end date.

    Args:
        sensing_datetimes: Sorted list of sensing datetimes for the frame
        end_date: End of the date range
        k: Number of acquisitions per K-cycle group

    Returns:
        List of tuples containing (k_cycle_number, sensing_dates_in_cycle)
    """
    cycles = []

    # Group sensing times into K-cycles
    for i in range(0, len(sensing_datetimes), k):
        end_idx = min(i + k, len(sensing_datetimes))
        k_cycle_dates = sensing_datetimes[i:end_idx]
        k_cycle_number = math.ceil((i + 1) / k)

        # ensure the cycle has k sensing dates
        if len(k_cycle_dates) != k:
            logger.warning(
                f"K-cycle {k_cycle_number} has {len(k_cycle_dates)} sensing dates, expected {k}"
            )
            continue

        # Check if this K-cycle has sensing dates before the specified end date
        cycle_start = k_cycle_dates[0]
        cycle_end = k_cycle_dates[-1]
        if cycle_end <= end_date:
            cycles.append((k_cycle_number, k_cycle_dates))

    return cycles


def analyze_frame_k_cycles(
    frame_number: int,
    disp_burst_map: Dict,
    end_date: datetime,
    k: int,
    verbose: bool = False,
) -> int:
    """
    Analyze K-cycles for a specific frame prior to the specified end date.

    Args:
        frame_number: Frame number to analyze
        disp_burst_map: Frame to burst mapping from the database
        end_date: End of the date range
        k: Number of acquisitions per K-cycle group
        verbose: Enable verbose logging

    Returns:
        The frame_state, which is the sum of the length of each K-cycle group's sensing dates that are prior to the specified end date
    """
    if frame_number not in disp_burst_map:
        logger.warning(f"Frame {frame_number} not found in database")
        return 0

    frame_data = disp_burst_map[frame_number]
    sensing_datetimes = frame_data.sensing_datetimes

    if not sensing_datetimes:
        logger.warning(f"No sensing datetimes found for frame {frame_number}")
        return 0

    # Find K-cycles prior to the specified end date
    cycles = find_k_cycles(sensing_datetimes, end_date, k)

    # Calculate total sensing dates in cycles
    total_sensing_dates = sum(len(cycle_dates) for _, cycle_dates in cycles)

    if verbose:
        logger.info(f"Frame {frame_number}:")
        logger.info(f"  Total sensing datetimes: {len(sensing_datetimes)}")
        logger.info(f"  K-cycles: {len(cycles)}")
        logger.info(f"  Total sensing dates in cycles: {total_sensing_dates}")

        for k_cycle_num, cycle_dates in cycles:
            logger.info(
                f"  K-cycle {k_cycle_num}: {len(cycle_dates)} dates "
                f"({cycle_dates[0].isoformat()} to {cycle_dates[-1].isoformat()})"
            )

    return total_sensing_dates


def main():
    """Main function."""
    args = parse_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse dates
    try:
        end_date = datetime.fromisoformat(args.end_date)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return 1

    # Parse frame numbers
    try:
        frame_numbers = [int(frame.strip()) for frame in args.frames.split(",")]
    except ValueError as e:
        logger.error(f"Invalid frame numbers: {e}")
        return 1

    logger.info(f"Analyzing {len(frame_numbers)} frames with K={args.k}")
    logger.info(f"End date: {end_date.isoformat()}")

    # Load the burst database
    try:
        disp_burst_map, burst_to_frames, day_indices_to_frames = load_burst_database(
            args.db_file
        )
        logger.info(f"Loaded database with {len(disp_burst_map)} frames")
    except Exception as e:
        logger.error(f"Failed to load burst database: {e}")
        return 1

    # Analyze each frame and store the frame_state in the results dictionary
    results = {}
    for frame_number in frame_numbers:
        logger.info(f"Processing frame {frame_number}...")

        frame_state = analyze_frame_k_cycles(
            frame_number, disp_burst_map, end_date, args.k, args.verbose
        )

        results[str(frame_number)] = frame_state

    # Write output JSON (simplified format: frame_id -> value)
    try:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results written to {args.output}")
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        return 1

    # Print summary
    logger.info("Analysis Summary:")
    for frame, frame_state in results.items():
        logger.info(f"  Frame {frame}: frame_state {frame_state}")

    return 0


if __name__ == "__main__":
    exit(main())
