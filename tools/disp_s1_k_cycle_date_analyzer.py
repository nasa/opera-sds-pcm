#!/usr/bin/env python3
"""
K-Cycle Date Analyzer

This script analyzes K-cycle groups prior to a specified end date for given frames
using the OPERA DISP-S1 consistent burst database. It determines which K-cycle
groups have sensing dates before the specified end date and calculates the total
number of sensing dates within those groups.

A frame carrying processing-mode annotations is walked phase by phase instead, the
way a phased batch proc walks it: k-sets restart at every historical phase, so their
boundaries sit at phase-relative positions range(phase.start_pos, phase.end_pos, k)
rather than on the absolute grid from position 0. forward_NN dates are driven one at
a time and are not k-sets, and no_run blocks are stepped over without ever being
processed. A frame without annotations -- an un-annotated database, or the
DISP_S1_PROCESSING_MODE_ENABLED switch off -- is analyzed on the absolute grid
exactly as before.

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
from typing import Dict, List, NamedTuple, Optional, Tuple

# Reuse imports from disp_s1_burst_db_tool.py
from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import PhaseKind

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

    parser.add_argument(
        "--use-processing-modes",
        dest="use_processing_modes",
        action="store_true",
        help="Parse the processing-mode annotations of the --db-file even when "
        f"{cslc_utils.PROCESSING_MODE_SETTINGS_FIELD} is off in settings.yaml",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    return parser.parse_args()


def load_burst_database(db_file=None, use_processing_modes=None):
    """Load the DISP-S1 burst database.

    use_processing_modes forces a local database file's processing-mode annotations to be parsed.
    Left as None the DISP_S1_PROCESSING_MODE_ENABLED setting decides, which is also the only thing
    that governs the standard database in S3 ancillary.
    """
    if db_file:
        logger.info(f"Using local DISP-S1 database json file: {db_file}")
        disp_burst_map, burst_to_frames, day_indices_to_frames = (
            cslc_utils.process_disp_frame_burst_hist(db_file, use_processing_modes)
        )
    else:
        if use_processing_modes:
            logger.warning(
                "--use-processing-modes applies to --db-file only; the standard database in S3 "
                f"ancillary is parsed according to {cslc_utils.PROCESSING_MODE_SETTINGS_FIELD} "
                "in settings.yaml"
            )
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


class PhaseGroup(NamedTuple):
    """One unit of work of a phase-annotated frame's walk.

    A historical phase contributes one group per k-set, a forward phase one group per date (they
    are driven one at a time and are not k-sets), and a no_run block one skipped group.
    """

    label: str  # phase label, e.g. "historical_02"
    kind: PhaseKind
    number: Optional[int]  # 1-based position of the group within its phase; None for no_run
    total: Optional[int]  # number of groups in the phase; None for no_run
    start_pos: int  # absolute index of the group's first sensing date
    dates: List[datetime]
    skipped: bool  # a no_run block: stepped over, never processed


def find_phased_k_cycles(
    sensing_datetimes: List[datetime], phases: List, end_date: datetime, k: int
) -> Tuple[List[PhaseGroup], int]:
    """
    Walk a phase-annotated frame the way a phased batch proc walks it.

    k-sets restart at every historical phase, so their boundaries are phase-relative:
    range(phase.start_pos, phase.end_pos, k). forward_NN dates advance one at a time, and a no_run
    block is stepped over whole -- its dates are never processed, whatever the end date is.

    Args:
        sensing_datetimes: Sorted list of sensing datetimes for the frame
        phases: The frame's ProcessingPhase list (end_pos is exclusive)
        end_date: End of the date range
        k: Number of acquisitions per K-cycle group

    Returns:
        (groups, cursor), where cursor is the ABSOLUTE sensing-list position the walk has reached,
        which is what a phased batch proc's frame_states holds. It is not the number of dates
        processed: the dates of a no_run block sit under the cursor without ever being processed.
    """
    groups = []
    cursor = 0

    for phase in phases:
        if phase.kind is PhaseKind.NO_RUN:
            groups.append(
                PhaseGroup(
                    label=phase.label,
                    kind=phase.kind,
                    number=None,
                    total=None,
                    start_pos=phase.start_pos,
                    dates=sensing_datetimes[phase.start_pos : phase.end_pos],
                    skipped=True,
                )
            )
            cursor = phase.end_pos
            continue

        # Historical phases are submitted a whole k-set at a time, forward phases a date at a time
        step = k if phase.kind is PhaseKind.HISTORICAL else 1
        starts = list(range(phase.start_pos, phase.end_pos, step))
        halted = False

        for number, position in enumerate(starts, start=1):
            group_dates = sensing_datetimes[position : min(position + step, phase.end_pos)]

            # ensure the group has its full complement of sensing dates
            if len(group_dates) != step:
                logger.warning(
                    f"Phase {phase.label} group {number} has {len(group_dates)} sensing dates, "
                    f"expected {step}"
                )
                halted = True
                break

            # Later phases are later in time, so the first group past the end date ends the walk
            if group_dates[-1] > end_date:
                halted = True
                break

            groups.append(
                PhaseGroup(
                    label=phase.label,
                    kind=phase.kind,
                    number=number,
                    total=len(starts),
                    start_pos=position,
                    dates=group_dates,
                    skipped=False,
                )
            )
            cursor = position + step

        if halted:
            break

    return groups, cursor


def _log_phased_groups(
    frame_number: int,
    sensing_datetimes: List[datetime],
    phases: List,
    groups: List[PhaseGroup],
    cursor: int,
):
    """Report a phase-annotated frame's groupings and the cursor they leave behind."""

    layout = " ".join(f"{phase.label}[{phase.length}]@{phase.start_pos}" for phase in phases)
    processed = sum(len(group.dates) for group in groups if not group.skipped)

    logger.info(f"Frame {frame_number} (phase-annotated: {layout}):")
    logger.info(f"  Total sensing datetimes: {len(sensing_datetimes)}")
    logger.info(f"  Groups: {len([g for g in groups if not g.skipped])}")
    logger.info(f"  Total sensing dates in groups: {processed}")

    for group in groups:
        last_pos = group.start_pos + len(group.dates) - 1
        span = (
            f"positions {group.start_pos}-{last_pos}, {len(group.dates)} dates "
            f"({group.dates[0].isoformat()} to {group.dates[-1].isoformat()})"
        )
        if group.skipped:
            logger.info(f"  {group.label}: SKIPPED, never processed - {span}")
        elif group.kind is PhaseKind.HISTORICAL:
            logger.info(f"  {group.label} k-set {group.number}/{group.total}: {span}")
        else:
            logger.info(
                f"  {group.label} date {group.number}/{group.total}: position "
                f"{group.start_pos} ({group.dates[0].isoformat()})"
            )

    logger.info(f"  Frame state (absolute cursor): {cursor}")


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

        For a phase-annotated frame this is instead the absolute sensing-list position the phased
        walk reaches, which is what a phased batch proc's frame_states holds. The two agree except
        across a no_run block, whose dates the walk steps over without processing them.
    """
    if frame_number not in disp_burst_map:
        logger.warning(f"Frame {frame_number} not found in database")
        return 0

    frame_data = disp_burst_map[frame_number]
    sensing_datetimes = frame_data.sensing_datetimes

    if not sensing_datetimes:
        logger.warning(f"No sensing datetimes found for frame {frame_number}")
        return 0

    # An annotated frame's k-sets restart at every historical phase, so walking the absolute grid
    # from position 0 would report groups the phased walk never submits
    phases = getattr(frame_data, "phases", None)
    if phases:
        groups, cursor = find_phased_k_cycles(sensing_datetimes, phases, end_date, k)
        if verbose:
            _log_phased_groups(frame_number, sensing_datetimes, phases, groups, cursor)
        else:
            logger.info(
                f"Frame {frame_number} is phase-annotated; walked "
                f"{len([g for g in groups if not g.skipped])} group(s) phase-relatively"
            )
        return cursor

    phase_error = getattr(frame_data, "phase_error", None)
    if phase_error:
        logger.warning(
            f"Frame {frame_number} has processing-mode annotations that were rejected "
            f"({phase_error}); analyzing on the absolute grid"
        )

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
            args.db_file, True if args.use_processing_modes else None
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
