"""DIST-S1 forward-mode submitter job.

Queries for runnable state-configs, enforces ordering per (tile_id, agi, agn),
retrieves baseline granules, and submits RTC-for-DIST download jobs.
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, UTC
from functools import partial
from logging import Logger
from pathlib import Path

from more_itertools import first

from data_subscriber.dist_s1_utils import localize_dist_burst_db
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency
from data_subscriber.rtc_for_dist.rtc_batch_evaluator import DownloadJobSubmitter
from data_subscriber.rtc_for_dist.rtc_for_dist_catalog import RTCForDistProductCatalog
from dist_s1 import forward_state_config_dao as dao
from opera_commons.logger import get_logger, configure_library_loggers
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id

logger: Logger = None
args: argparse.Namespace = None

settings: dict = None

to_json = partial(json.dumps, indent=2)
"""json.dumps with default params"""

@exec_wrapper
def main():
    global logger
    global args

    parser = create_arg_parser()
    args = parser.parse_args(sys.argv[1:])
    logger = init_opera_pcm_logger()
    logger.info(f"{__file__} invoked with {sys.argv=}")
    logger.info(f"{args=}")

    run()

def run():
    global settings

    logger.info("BEGIN")

    settings = SettingsConf().cfg

    filter_tile_id = args.filter_tile_id

    job_id = supply_job_id()
    logger.info(f"{job_id=}")

    evaluate(filter_tile_id)

    logger.info("END")


def evaluate(filter_tile_id):
    """Main submitter logic."""

    # find all state-config chains with at least 1 pending state-config, then evaluate affected tiles
    submittable_state_configs = [sc["metadata"] for sc in dao.query_submittable_null_state_configs(filter_tile_id)]
    logger.info(f"{len(submittable_state_configs)=}")

    submittable_tile_ids = {sc["tile_id"] for sc in submittable_state_configs}
    logger.info(f"{len(submittable_tile_ids)=}")

    # Group by tile_id. sort chronologically using agi + agn
    tile_to_state_configs = defaultdict(list)
    for tile_id in submittable_tile_ids:
        logger.info(f"{tile_id=}")
        tile_state_configs = [sc["metadata"] for sc in dao.query_state_configs_by_tile(tile_id)]
        tile_to_state_configs[tile_id].extend(tile_state_configs)
    for t in tile_to_state_configs:
        tile_to_state_configs[t].sort(key=lambda sc: (sc["aci"], sc["agn"]))

    # For each group, submit ONLY the oldest (lowest aci) NULL batch

    tile_to_unique_oldest_state_configs = {}
    for tile_id, state_configs in tile_to_state_configs.items():
        sc = first((sc for sc in state_configs if sc.get("status") == "NULL"), None)
        if not sc:
            logger.info(f"Skipping {tile_id}: no NULL state config found in the chain. Chain is considered complete at this time.")
            continue
        tile_to_unique_oldest_state_configs[tile_id] = sc

    tile_to_filtered_unique_oldest_state_configs = {}
    for tile_id, sc in tile_to_unique_oldest_state_configs.items():
        now = datetime.now(UTC)
        grace_expired = now.isoformat() >= sc["grace_period_expiry"]
        if not grace_expired:
            logger.info(f"Skipping {tile_id}: grace period not expired")
            continue

        if not sc.get("is_usable"):
            logger.info(f"Skipping {tile_id}: is_usable flag not set")
            continue
        if not sc.get("is_submittable"):
            logger.info(f"Skipping {tile_id}: is_submittable flag not set")
            continue

        tile_to_filtered_unique_oldest_state_configs[tile_id] = sc

    for _, sc in tile_to_filtered_unique_oldest_state_configs.items():
        batch_id = sc["batch_id"]
        agn = sc["agn"]
        aci = sc["aci"]

        tile_id = sc["tile_id"]
        logger.info(f"Submitting download job for oldest NULL batch in {tile_id}: {batch_id}")

        # read DIST-S1 lookup DB
        dist_products, bursts_to_products, product_to_bursts, _ = localize_dist_burst_db()
        download_job_submitter = DownloadJobSubmitter(logger=logger, dist_dependency=DistDependency(logger, dist_products, bursts_to_products, product_to_bursts, settings), es_conn=RTCForDistProductCatalog(logger), settings=settings)
        batch_id_to_current_urls_map = {sc["download_batch_id"]: sc["current_urls"]}
        batch_id_to_baseline_urls_map = {sc["download_batch_id"]: sc["baseline_urls"]}
        download_job_ids = download_job_submitter.submit_download_jobs(
            submittable_batch_id_to_current_urls_map=batch_id_to_current_urls_map,
            batch_id_to_baseline_urls=batch_id_to_baseline_urls_map,
            query_timerange=None
        )  # TODO chrisjrd: test no query timerange as param appears redundant
        download_job_id = first(download_job_ids)
        logger.info(f"Submitted download job: {download_job_id}")
        dao.update_state_config_fields(batch_id, status="PENDING", download_job_id=download_job_id)


def load_job_context() -> dict:
    logger.info("Loading job context")
    jc = JobContext(str(Path("_context.json").absolute()))
    job_context = jc.ctx
    logger.info(f"job_context={to_json(job_context)}")
    return job_context


def init_opera_pcm_logger():
    global logger
    logger = get_logger(log_format_override="%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s")
    configure_library_loggers()
    return logger


def create_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filter-tile-id", help="Perform evaluation for a specific tile. If not specified, default behavior is to perform global state config evaluation.")
    return parser


if __name__ == "__main__":
    main()
