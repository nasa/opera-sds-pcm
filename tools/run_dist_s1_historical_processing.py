#!/usr/bin/env python3
"""Create a temporary dist_s1 historical ingest/run directory and execute the ingest/query workflow."""

import argparse
import concurrent.futures
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_INGEST_SCRIPT = "~/mozart/ops/hysds/scripts/ingest_dataset.py"
DEFAULT_DATASETS_JSON = "~/mozart/etc/datasets.json"
DEFAULT_DATA_SUBSCRIBER = "~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py"
DEFAULT_COLLECTION = "OPERA_L2_RTC-S1_V1"
DEFAULT_PRODUCT = "DIST_S1"
DEFAULT_ENDPOINT = "OPS"
DEFAULT_JOB_QUEUE = "opera-job_worker-rtc_for_dist_data_download"
DEFAULT_CHUNK_SIZE = 1
DEFAULT_FILTER_TILES = ["18NUF", "18FWH", "44SQA"]
DEFAULT_START_DATE = "2026-01-01T00:11:00Z"
DEFAULT_END_DATE = "2026-01-02T00:00:00Z"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_dist_s1_hist")


def validate_datetime_arg(value):
    if "--" in value:
        raise argparse.ArgumentTypeError(
            "Invalid datetime value: found option-like text in the date string. "
            "Make sure you use a space before flags, e.g. --end-date 2026-02-01T00:00:00Z --dry-run"
        )
    try:
        if value.endswith("Z"):
            datetime.fromisoformat(value[:-1] + "+00:00")
        else:
            datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid ISO datetime: {value}")
    return value


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a temporary DIST_S1 historical run directory, ingest state config files, and submit a data subscriber query."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=validate_datetime_arg,
        help="Start date for the data subscriber query in format YYYY-MM-DDTHH:MM:SSZ",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=validate_datetime_arg,
        help="End date for the data subscriber query in format YYYY-MM-DDTHH:MM:SSZ",
    )
    parser.add_argument(
        "--collection-shortname",
        default=DEFAULT_COLLECTION,
        help=f"Collection shortname (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--product",
        default=DEFAULT_PRODUCT,
        help=f"Product for the data subscriber query (default: {DEFAULT_PRODUCT})",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"Endpoint for the data subscriber query (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--job-queue",
        default=DEFAULT_JOB_QUEUE,
        help=f"Job queue for the data subscriber query (default: {DEFAULT_JOB_QUEUE})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Chunk size for the data subscriber query (default: {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--filter-tiles",
        nargs="+",
        default=None,
        help="Filter tiles for the data subscriber query (optional)",
    )
    parser.add_argument(
        "--tile-list-file",
        type=str,
        default=None,
        help="Path to a newline-delimited tile list file. Each line should contain one tile code.",
    )
    parser.add_argument(
        "--bounds",
        default=None,
        help="Optional bound argument for the data subscriber query, e.g. -119.0,31.67,-114.02,36.05",
    )
    parser.add_argument(
        "--ingest-script",
        default=DEFAULT_INGEST_SCRIPT,
        help=f"Path to ingest_dataset.py (default: {DEFAULT_INGEST_SCRIPT})",
    )
    parser.add_argument(
        "--datasets-json",
        default=DEFAULT_DATASETS_JSON,
        help=f"Path to datasets.json (default: {DEFAULT_DATASETS_JSON})",
    )
    parser.add_argument(
        "--data-subscriber",
        default=DEFAULT_DATA_SUBSCRIBER,
        help=f"Path to daac_data_subscriber.py (default: {DEFAULT_DATA_SUBSCRIBER})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    parser.add_argument(
        "--keep-dir",
        action="store_true",
        help="Do not remove the temporary run directory after completion",
    )
    parser.add_argument(
        "--tile-list-chunk-size",
        type=int,
        default=10,
        help="Maximum number of filter tiles to send per CMR query when using a tile list file.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel ingest tasks to run when processing config files.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional path to a file where logs will be written",
    )
    return parser.parse_args()


def build_ingest_command(ingest_script, dataset_file, datasets_json):
    return [
        sys.executable,
        ingest_script,
        dataset_file,
        datasets_json,
        "--force",
    ]


def load_tile_list_file(tile_list_file):
    # .expanduser() handles paths starting with "~"
    # .resolve() converts relative paths (just filenames) to full paths using the terminal's current directory, 
    # but leaves already-full paths alone.
    path = Path(tile_list_file).expanduser().resolve()
    
    if not path.is_file():
        # Printing 'path' here is highly useful because it shows the absolute path Python checked
        raise FileNotFoundError(f"Tile list file not found. Looked exactly here: {path}")
        
    tiles = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            tile = line.strip()
            if tile:
                tiles.append(tile)
    return tiles


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_data_subscriber_command(args):
    cmd = [
        sys.executable,
        args.data_subscriber,
        "query",
        f"--collection-shortname={args.collection_shortname}",
        f"--product={args.product}",
        f"--endpoint={args.endpoint}",
        f"--start-date={args.start_date}",
        f"--end-date={args.end_date}",
        f"--job-queue={args.job_queue}",
        f"--chunk-size={args.chunk_size}",
        "--use-temporal",
        "--transfer-protocol=auto",
        "--processing-mode=historical",
        "--grace-min=1",
    ]
    if getattr(args, "filter_tiles", None):
        cmd.append("--filter-tiles")
        cmd.extend(args.filter_tiles)
    if getattr(args, "bounds", None):
        cmd.append(f"--bound={args.bounds}")
    return cmd


def run_command(cmd, dry_run=False, cwd=None):
    cmd_str = " ".join(shlex_quote(str(p)) for p in cmd)
    logger.info("Running: %s", cmd_str)

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        logger.error("Command failed (%d): %s", result.returncode, cmd_str)
        logger.error("stdout: %s", result.stdout.strip())
        logger.error("stderr: %s", result.stderr.strip())
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    logger.info("Command succeeded")
    if result.stdout:
        logger.info("stdout: %s", result.stdout.strip())
    if result.stderr:
        logger.warning("stderr: %s", result.stderr.strip())


def process_config_file(config_file, ingest_script, datasets_json, run_dir, dry_run):
    logger.info("Processing config file: %s", config_file.name)
    cmd = build_ingest_command(str(ingest_script), config_file.name, str(datasets_json))
    logger.info("Ingest command: %s", " ".join(shlex_quote(str(p)) for p in cmd))
    if not dry_run:
        run_command(cmd, dry_run=dry_run, cwd=run_dir)


def shlex_quote(text):
    if isinstance(text, str):
        return subprocess.list2cmdline([text])
    return str(text)


def main():
    args = parse_args()
    args.tile_list_file = str(Path(args.tile_list_file).expanduser().resolve())

    run_dir_name = f"dist_s1_hist_run_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    run_dir = Path.cwd() / run_dir_name
    run_dir.mkdir(parents=True, exist_ok=False)

    if args.log_file:
        log_path = Path(args.log_file).expanduser()
    else:
        log_path = Path.cwd() / run_dir_name / f"{run_dir_name}.log"
    if log_path.parent:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, mode="a")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)

    logger.info("Created temporary directory: %s", run_dir)
    logger.info("Logging to file: %s", log_path)
    try:
        ingest_script = Path(args.ingest_script).expanduser().resolve()
        datasets_json = Path(args.datasets_json).expanduser().resolve()
        data_subscriber = Path(args.data_subscriber).expanduser().resolve()

        if not ingest_script.exists():
            logger.error("Ingest script not found: %s", ingest_script)
            sys.exit(1)
        if not datasets_json.exists():
            logger.error("Datasets JSON not found: %s", datasets_json)
            sys.exit(1)
        if not data_subscriber.exists():
            logger.error("Data subscriber script not found: %s", data_subscriber)
            sys.exit(1)

        os.chdir(run_dir)

        logger.info("Building data subscriber query command")
        args.data_subscriber = str(data_subscriber)

        filter_tiles = args.filter_tiles or []
        if args.tile_list_file:
            filter_tiles = filter_tiles + load_tile_list_file(args.tile_list_file)

        if filter_tiles and len(filter_tiles) > args.tile_list_chunk_size:
            logger.info("Splitting %d tiles into chunks of %d for CMR queries", len(filter_tiles), args.tile_list_chunk_size)
            tile_chunks = list(chunk_list(filter_tiles, args.tile_list_chunk_size))
        else:
            tile_chunks = [filter_tiles]

        for i, tile_chunk in enumerate(tile_chunks, start=1):
            logger.info("Running CMR query chunk %d/%d with %d tiles", i, len(tile_chunks), len(tile_chunk))
            tile_args = argparse.Namespace(**vars(args))
            tile_args.filter_tiles = tile_chunk
            data_subscriber_cmd = build_data_subscriber_command(tile_args)
            run_command(data_subscriber_cmd, dry_run=args.dry_run, cwd=run_dir)

        if args.dry_run:
            logger.info("Dry-run enabled; skipping state-config ingestion")
            return

        config_files = sorted(Path.cwd().glob("DIST_S1_state-config*"))
        if not config_files:
            logger.warning("No DIST_S1_state-config* files found in %s", run_dir)
        else:
            logger.info("Processing %d config files using %d workers", len(config_files), args.max_workers)
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                futures = {
                    executor.submit(process_config_file, config_file, ingest_script, datasets_json, run_dir, args.dry_run): config_file
                    for config_file in config_files
                }
                for future in concurrent.futures.as_completed(futures):
                    config_file = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error("Config file %s failed: %s", config_file.name, exc)
                        raise

    finally:
        if args.keep_dir:
            logger.info("Keeping temporary directory: %s", run_dir)
        else:
            logger.info("Removing temporary directory: %s", run_dir)
            shutil.rmtree(run_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
