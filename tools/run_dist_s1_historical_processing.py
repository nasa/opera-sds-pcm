#!/usr/bin/env python3
"""Submit chunked DIST-S1 historical processing jobs via daac_data_subscriber.

Splits a date range into configurable chunks (default: 1 day), submits each
chunk as an RTC data subscriber query, and waits a configurable duration
between chunks so that DIST-S1 dependencies from prior dates can complete.
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DATA_SUBSCRIBER_PATH = "~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py"

DEFAULT_COLLECTION = "OPERA_L2_RTC-S1_V1"
DEFAULT_ENDPOINT = "OPS"
DEFAULT_JOB_QUEUE = "opera-job_worker-rtc_for_dist_data_download"
DEFAULT_CHUNK_SIZE = 1
DEFAULT_CHUNK_DAYS = 1
DEFAULT_WAIT_HOURS = 1.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DIST-S1-HISTORICAL")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit chunked DIST-S1 historical processing jobs."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help=f"Start date in format {DATETIME_FORMAT} (e.g. 2024-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help=f"End date in format {DATETIME_FORMAT} (e.g. 2024-02-01T00:00:00Z)",
    )
    chunk_group = parser.add_mutually_exclusive_group()
    chunk_group.add_argument(
        "--chunk-days",
        type=int,
        default=None,
        help=f"Number of days per chunk (default: {DEFAULT_CHUNK_DAYS}). "
             "Cannot be used with --chunk-hours.",
    )
    chunk_group.add_argument(
        "--chunk-hours",
        type=float,
        default=None,
        help="Number of hours per chunk. Cannot be used with --chunk-days.",
    )
    parser.add_argument(
        "--wait-hours",
        type=float,
        default=DEFAULT_WAIT_HOURS,
        help=f"Hours to wait between chunk submissions (default: {DEFAULT_WAIT_HOURS})",
    )
    parser.add_argument(
        "--collection-shortname",
        default=DEFAULT_COLLECTION,
        help=f"CMR collection shortname (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"DAAC endpoint (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--job-queue",
        default=DEFAULT_JOB_QUEUE,
        help=f"Job queue name (default: {DEFAULT_JOB_QUEUE})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Data subscriber chunk-size parameter (default: {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print commands without executing them",
    )
    return parser.parse_args()


def build_command(chunk_start, chunk_end, args):
    """Build the daac_data_subscriber.py query command for a single chunk."""
    return [
        "python",
        DATA_SUBSCRIBER_PATH,
        "query",
        f"--collection-shortname={args.collection_shortname}",
        f"--endpoint={args.endpoint}",
        f"--job-queue={args.job_queue}",
        f"--chunk-size={args.chunk_size}",
        "--use-temporal",
        "--transfer-protocol=auto",
        "--processing-mode=historical",
        f"--start-date={chunk_start.strftime(DATETIME_FORMAT)}",
        f"--end-date={chunk_end.strftime(DATETIME_FORMAT)}",
    ]


def generate_chunks(start_date, end_date, chunk_duration):
    """Yield (chunk_start, chunk_end) tuples covering the full date range."""
    current = start_date
    while current < end_date:
        chunk_end = min(current + chunk_duration, end_date)
        yield current, chunk_end
        current = chunk_end


def main():
    args = parse_args()

    start_date = datetime.strptime(args.start_date, DATETIME_FORMAT)
    end_date = datetime.strptime(args.end_date, DATETIME_FORMAT)

    if start_date >= end_date:
        logger.error("start-date must be before end-date")
        sys.exit(1)

    if args.chunk_hours is not None:
        chunk_duration = timedelta(hours=args.chunk_hours)
        chunk_label = f"{args.chunk_hours} hour(s)"
    else:
        chunk_days = args.chunk_days if args.chunk_days is not None else DEFAULT_CHUNK_DAYS
        chunk_duration = timedelta(days=chunk_days)
        chunk_label = f"{chunk_days} day(s)"

    chunks = list(generate_chunks(start_date, end_date, chunk_duration))
    total_chunks = len(chunks)

    logger.info(
        "Processing %d chunk(s) of %s each from %s to %s",
        total_chunks,
        chunk_label,
        args.start_date,
        args.end_date,
    )
    logger.info("Wait between chunks: %.1f hours", args.wait_hours)

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        cmd = build_command(chunk_start, chunk_end, args)
        cmd_str = " ".join(cmd)

        logger.info("Chunk %d/%d: %s to %s", i, total_chunks,
                     chunk_start.strftime(DATETIME_FORMAT),
                     chunk_end.strftime(DATETIME_FORMAT))
        logger.info("Command: %s", cmd_str)

        if args.dry_run:
            logger.info("[DRY RUN] Skipping execution")
        else:
            try:
                result = subprocess.run(cmd_str, shell=True, check=True,
                                        capture_output=True, text=True)
                logger.info("stdout: %s", result.stdout)
                if result.stderr:
                    logger.warning("stderr: %s", result.stderr)
            except subprocess.CalledProcessError as e:
                logger.error("Command failed with return code %d", e.returncode)
                logger.error("stdout: %s", e.stdout)
                logger.error("stderr: %s", e.stderr)
                sys.exit(1)

        # Wait between chunks, but not after the last one
        if i < total_chunks:
            wait_secs = args.wait_hours * 3600
            logger.info("Waiting %.1f hours before next chunk...", args.wait_hours)
            time.sleep(wait_secs)

    logger.info("All %d chunks submitted successfully", total_chunks)


if __name__ == "__main__":
    main()
