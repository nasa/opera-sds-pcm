"""DIST-S1 forward-mode evaluator job.

Determines whether a state-config batch is runnable based on:
- Completeness percentage vs threshold
- Grace period expiry
- Baseline granule availability (via cmr_rtc_cache)

Supports two trigger modes:
- Event-triggered: evaluates a specific batch_id
- Timer-triggered: scans for expired/stale state-configs
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, UTC
from functools import partial
from itertools import chain
from logging import Logger
from pathlib import Path

import dist_s1.forward_state_config_dao as dao
from data_subscriber.dist_s1_utils import extend_rtc_for_dist_records, localize_dist_burst_db, rtc_granule_dict_add, \
    compute_dist_s1_triggering, get_unique_rtc_id_for_dist, parse_k_parameter, basic_decorate_granule
from data_subscriber.rtc_for_dist.baseline_granule_retriever import BaselineGranuleRetriever
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency
from data_subscriber.rtc_for_dist.rtc_batch_evaluator import RtcBatchEvaluator
from data_subscriber.rtc_for_dist.rtc_for_dist_catalog import RTCForDistProductCatalog
from dist_s1.forward_state_config_dao import fix_batch_id
from opera_commons.es_connection import get_grq_es
from opera_commons.logger import get_logger, configure_library_loggers
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.grq_client import get_body
from util.pge_util import get_product_metadata

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

    evaluator = Evaluator()

    batch_id = args.batch_id
    if not batch_id:
        context_dict = load_job_context()
        product_metadata = get_product_metadata(context_dict)
        batch_id = product_metadata.get("batch_id")

    if batch_id:
        evaluator.evaluate_single_batch(batch_id)
    else:
        evaluator.evaluate_expired_batches()

    logger.info("END")


def create_body(rtc_granule_ids):
    body = get_body(match_all=False)
    body["size"] = 1
    body["sort"] = [{"granule_id.keyword": {"order": "desc", "unmapped_type" : "string"}}]
    body["query"]["bool"]["should"] = [{"match": {"granule_id": id_}} for id_ in rtc_granule_ids]
    return body


class Evaluator:
    def __init__(self):
        self.download_batch_id_to_k_granules = {}
        self.download_batch_id_to_job_submittable = {}

    def evaluate_single_batch(self, batch_id: str):
        """Event-triggered evaluation of a specific batch."""
        logger.info(f"Evaluating batch: {batch_id}")
        state_config = dao.query_state_config(batch_id)
        if state_config is not None:
            state_config = state_config["metadata"]
        if state_config is None:
            logger.warning(f"State-config not found: {batch_id}")
            return

        logger.info(f"Batch {batch_id}: {state_config=}")

        if state_config["status"] != "NULL":
            logger.info(f"Batch {batch_id} already has status={state_config['status']}. Skipping.")
            return

        rtc_granule_ids = state_config["rtc_granule_ids"]
        grq_es = get_grq_es(logger)
        rtc_granule_docs = grq_es.search(index=RTCForDistProductCatalog.ES_INDEX_PATTERNS, body=create_body(rtc_granule_ids))
        granules = [h["_source"] for h in rtc_granule_docs["hits"]["hits"]]

        # read DIST-S1 lookup DB
        dist_products, bursts_to_products, product_to_bursts, _ = localize_dist_burst_db()

        # TODO chrisjrd: filter should be handled upstream
        # filter and decorate granules
        filtered_granules = []
        for granule in granules:
            basic_decorate_granule(granule)
            burst_id = granule["burst_id"]
            if burst_id in bursts_to_products:
                filtered_granules.append(granule)
            else:
                logger.error(f"{granule} not in DIST-S1 lookup DB. This granule should not be present in the RTC catalog for DIST-S1. Skipping.")

        # TODO chrisjrd: dedupe should be handled upstream
        # dedupe granules
        # If there are multiple granules with the same burst_id and acquisition_ts, we only want to keep the latest one
        filtered_granules = BaselineGranuleRetriever.unique_latest_granules(filtered_granules)

        granules = filtered_granules

        extend_rtc_for_dist_records(bursts_to_products, granules, no_duplicate=True, force_product_id="_".join(batch_id.split("_")[:2]))
        granules_dict = {}
        rtc_granule_dict_add(granules_dict, granules)

        grace_mins = settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_QUERY_GRACE_PERIOD_MINUTES"]
        candidate_dist_s1_input_infos, _, __, ___ = compute_dist_s1_triggering(product_to_bursts, granules_dict, grace_mins, datetime.now(), complete_bursts_only=False)


        batch_id_to_current_granules = defaultdict(list)
        for batch_id, dist_s1_input_info in candidate_dist_s1_input_infos.items():  # batch ID for current granules
            if dist_s1_input_info.used_bursts != dist_s1_input_info.possible_bursts:
                logger.info("Incomplete burst set. To be handled by state-config expiry checker job. Skipping.")
                continue

            for rtc_granule in dist_s1_input_info.rtc_granules:
                unique_rtc_id = get_unique_rtc_id_for_dist(rtc_granule)
                batch_id_to_current_granules[batch_id].append(granules_dict[(unique_rtc_id, batch_id)])  # current granules
        if not batch_id_to_current_granules:
            logger.info("Nothing to do.")
            return

        baseline_granule_retriever = BaselineGranuleRetriever(
            logger=logger,
            k_offsets_counts=parse_k_parameter(state_config["k_offsets_counts"]),
            product_to_bursts=product_to_bursts,
            window_delta_days=args.window_delta if "window_delta" in args and args.window_delta else settings["DIST_S1_TRIGGERING"]["DEFAULT_DIST_S1_WINDOW_DELTA_DAYS"],
            token=None,
            cmr=settings["DAAC_ENVIRONMENTS"][args.endpoint if "endpoint" in args else "OPS"]["BASE_URL"],
            settings=settings,
            bursts_to_products=bursts_to_products,
        )
        download_batch_id_to_k_granules = baseline_granule_retriever.retrieve_baseline_granules_for_affected_batches(batch_id_to_current_granules)
        self.download_batch_id_to_k_granules.update(download_batch_id_to_k_granules)

        # track whether baseline granules are present or not. batches with no baseline granules are ineligible for further evaluation
        download_batch_id_to_job_submittable = {}
        for download_batch_id, baseline_granules in download_batch_id_to_k_granules.items():
            if not len(baseline_granules):
                download_batch_id = fix_batch_id(download_batch_id)

                download_batch_id_split = download_batch_id.split("_")
                product_id = f'{download_batch_id_split[0].removeprefix("p")}_{download_batch_id_split[1]}'
                logger.info(f"No baseline granules found for {product_id=} {download_batch_id=}.")
                download_batch_id_to_job_submittable[download_batch_id] = False  # TODO chrisjrd: mark True / remove after new SAS delivery. as of 2026-02-05
            else:
                download_batch_id_to_job_submittable[download_batch_id] = True
        self.download_batch_id_to_job_submittable.update(download_batch_id_to_job_submittable)

        for download_batch_id, job_submittable in download_batch_id_to_job_submittable.items():
            dao.update_state_config_fields(
                fix_batch_id(download_batch_id),
                is_runnable=job_submittable,  # TODO chrisjrd: review
            )

        total_granules = [g for g in chain.from_iterable(batch_id_to_current_granules.values())]  # current granules only. no baseline granules.
        # TODO chrisjrd: only used to create download job params. check if actually used in download job
        query_timerange = None

        dist_dependency = DistDependency(logger, dist_products, bursts_to_products, product_to_bursts, settings)

        evaluator = RtcBatchEvaluator(
            logger=logger,
            download_batch_id_to_k_granules=download_batch_id_to_k_granules,
            batch_id_to_current_granules=batch_id_to_current_granules,
            download_batch_id_to_job_submittable=download_batch_id_to_job_submittable,
            dist_dependency=dist_dependency,
            es_conn=RTCForDistProductCatalog(logger),
            settings=settings,
        )
        evaluator.evaluate(total_granules, query_timerange)

        # save (evaluator result) URLs to state-config for workflow refactor-ability
        for batch_id in evaluator.batch_id_to_current_urls_map:
            dao.update_state_config_fields(
                fix_batch_id(batch_id),
                current_urls=evaluator.batch_id_to_current_urls_map[batch_id],
                baseline_urls=evaluator.batch_id_to_baseline_urls_map[batch_id]
            )

        for download_batch_id, _ in evaluator._unusable_batch_id_to_current_urls_map.items():
            dao.update_state_config_fields(
                fix_batch_id(download_batch_id),
                is_runnable=False,
                is_usable=False,
            )
        for download_batch_id, _ in evaluator.usable_batch_id_to_current_urls_map.items():
            dao.update_state_config_fields(
                fix_batch_id(download_batch_id),
                is_runnable=False,
                is_usable=True,
            )
        if not evaluator.usable_batch_id_to_current_urls_map:
            logger.info("No usable batch_ids found.")
            return

        for download_batch_id, _ in evaluator._unsubmittable_batch_id_to_current_urls_map.items():
            dao.update_state_config_fields(
                fix_batch_id(download_batch_id),
                is_runnable=False,
                is_submittable=False,
            )
        for download_batch_id, _ in evaluator.submittable_batch_id_to_current_urls_map.items():
            dao.update_state_config_fields(
                fix_batch_id(download_batch_id),
                is_runnable=True,
                is_submittable=True,
            )
        if not evaluator.submittable_batch_id_to_current_urls_map:
            logger.info("No submittable batch_ids found.")
            return


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
    parser.add_argument("--batch-id", type=str, default=None,
                        help="Specific batch_id to evaluate (event-triggered mode). "
                             "If omitted, runs in timer mode scanning all expired configs.")
    return parser


if __name__ == "__main__":
    main()