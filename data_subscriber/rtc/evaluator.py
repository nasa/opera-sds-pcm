import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from typing import Optional

import dateutil.parser
import pandas as pd
from more_itertools import first, flatten

from data_subscriber import es_conn_util
from data_subscriber.rtc import evaluator_core, rtc_catalog
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client
from data_subscriber.rtc.mgrs_bursts_collection_db_client import product_burst_id_to_mapping_burst_id, \
    burst_id_to_relative_orbit_numbers
from rtc_utils import rtc_granule_regex, rtc_relative_orbit_number_regex
from util.grq_client import get_body

logger = logging.getLogger(__name__)


def main(
        coverage_target: Optional[int] = None,
        required_min_age_minutes_for_partial_burstsets: int = 0,
        mgrs_set_id_acquisition_ts_cycle_indexes: Optional[set[str]] = None,
        min_num_bursts: Optional[int] = None,
        *args,
        **kwargs
):
    logger.info(f"{coverage_target=}, {min_num_bursts=}")
    if coverage_target is not None and min_num_bursts is not None:
        raise AssertionError("Both coverage_target and min_num_bursts was specified. Specify one or the other.")
    if coverage_target is None and min_num_bursts is None:
        raise AssertionError("Both coverage_target and min_num_bursts were not specified. Specify one or the other.")

    if coverage_target is None:
        coverage_target = 0
    if min_num_bursts is None:
        min_num_bursts = 0

    # query GRQ catalog
    grq_es = es_conn_util.get_es_connection(logger)

    if mgrs_set_id_acquisition_ts_cycle_indexes:
        logger.info(f"Supplied {mgrs_set_id_acquisition_ts_cycle_indexes=}. Adding criteria to query")
        es_docs = []
        for mgrs_set_id_acquisition_ts_cycle_idx in mgrs_set_id_acquisition_ts_cycle_indexes:
            body = get_body(match_all=False)
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_idx}})
            # this constraint seems redundant, but it results in more consistent results
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id": mgrs_set_id_acquisition_ts_cycle_idx.split("$")[0]}})
            tmp_es_docs = grq_es.query(body=body, index=rtc_catalog.ES_INDEX_PATTERNS)
            # filter out any redundant results
            tmp_es_docs = [doc for doc in tmp_es_docs if doc["_source"]["mgrs_set_id_acquisition_ts_cycle_index"] in mgrs_set_id_acquisition_ts_cycle_indexes]
            es_docs.extend(tmp_es_docs)
        # NOTE: skipping job-submission filters to allow reprocessing
    else:
        # query 1: query for unsubmitted docs
        body = get_body(match_all=False)
        body["query"]["bool"]["must_not"].append({"exists": {"field": "download_job_ids"}})
        unsubmitted_docs = grq_es.query(body=body, index=rtc_catalog.ES_INDEX_PATTERNS)
        logger.info(f"Found {len(unsubmitted_docs)=}")

        # query 2: query for submitted but not 100%
        body = get_body(match_all=False)
        body["query"]["bool"]["must"].append({"exists": {"field": "download_job_ids"}})
        body["query"]["bool"]["must"].append({"range": {"coverage": {"gte": 0, "lt": 100}}})
        submitted_but_incomplete_docs = grq_es.query(body=body, index=rtc_catalog.ES_INDEX_PATTERNS)
        logger.info(f"Found {len(submitted_but_incomplete_docs)=}")

        es_docs = unsubmitted_docs + submitted_but_incomplete_docs

    evaluator_results = {
        "coverage_target": coverage_target,
        "mgrs_sets": defaultdict(list)
    }

    if not es_docs:
        logger.warning("No pending RTC products found. No further evaluation.")
        return evaluator_results

    # extract product IDs, map to rows, later extract URLs
    product_id_to_product_files_map = defaultdict(list)
    for doc in es_docs:
        product_id_to_product_files_map[doc["_source"]["granule_id"]].append(doc["_source"])
    rtc_product_ids = product_id_to_product_files_map.keys()

    coverage_result_set_id_to_product_sets_map = evaluate_rtc_products(rtc_product_ids, coverage_target)
    coverage_results_short = {coverage_group: list(id_to_sets) for coverage_group, id_to_sets in coverage_result_set_id_to_product_sets_map.items()}
    logger.info(f"{coverage_results_short=}")

    logger.info("Converting coverage results to evaluator results")
    mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
    for coverage_group, id_to_sets in coverage_result_set_id_to_product_sets_map.items():
        if coverage_group == -1:
            logger.info(f"Skipping results that don't meet the target coverage ({coverage_target=}), {coverage_group=}: {list(id_to_sets)}")
            continue
        logger.info(f"Results that meet the target coverage ({coverage_target=}), {coverage_group=}: {list(id_to_sets)}")
        mgrs_set_id_to_product_sets_docs_map = join_product_file_docs(id_to_sets, product_id_to_product_files_map)
        for mgrs_set_id, product_sets_docs in mgrs_set_id_to_product_sets_docs_map.items():
            for product_set_docs in product_sets_docs:
                number_of_bursts_expected = mgrs[mgrs["mgrs_set_id"] == mgrs_set_id].iloc[0]["number_of_bursts"]
                number_of_bursts_actual = len(product_set_docs)
                coverage_actual = int(number_of_bursts_actual / number_of_bursts_expected * 100)
                evaluator_results["mgrs_sets"][mgrs_set_id].append({
                        "coverage_actual": coverage_actual,
                        "coverage_group": coverage_group,
                        "product_set": product_set_docs
                    })

    # not native-id flow, grace period does not apply
    #  if 100% coverage target set, grace period does not apply and sets have been handled already above
    if mgrs_set_id_acquisition_ts_cycle_indexes or coverage_target == 100:
        # native-id flow, grace period does not apply
        #  if 100% coverage target set, grace period does not apply and sets have been handled already above
        logger.info("ignoring grace period")
        pass
    elif coverage_target != 100:
        # skip recent target covered sets to wait for more data
        #  until H hours have passed since most recent retrieval
        for mgrs_set_id in list(evaluator_results["mgrs_sets"]):
            product_set_and_coverage_dicts = evaluator_results["mgrs_sets"][mgrs_set_id]
            product_burstset_index_to_skip_processing = set()
            for i, product_set_and_coverage_dict in enumerate(product_set_and_coverage_dicts):
                coverage_group = product_set_and_coverage_dict["coverage_group"]
                if coverage_target is not None and coverage_group != coverage_target:
                    continue
                product_burstset = product_set_and_coverage_dict["product_set"]
                if min_num_bursts is not None:
                    number_of_bursts = len(product_burstset)
                    logger.info(f"{number_of_bursts=}, {min_num_bursts=}")
                    if number_of_bursts < min_num_bursts:
                        product_burstset_index_to_skip_processing.add(i)
                        continue
                    continue
                retrieval_dts = {
                    dateutil.parser.parse(product_doc["creation_timestamp"])
                    for rtc_granule_id_to_product_docs_map in product_burstset
                    for product_doc in chain.from_iterable(rtc_granule_id_to_product_docs_map.values())
                }
                max_retrieval_dt = max(*retrieval_dts) if len(retrieval_dts) > 1 else first(retrieval_dts)
                grace_period_minutes_remaining = timedelta(minutes=required_min_age_minutes_for_partial_burstsets) - (datetime.now() - max_retrieval_dt)
                if datetime.now() - max_retrieval_dt < timedelta(minutes=required_min_age_minutes_for_partial_burstsets):
                    # burst set meets target, but not old enough. continue to ignore
                    logger.info(f"Target covered burst still within grace period ({grace_period_minutes_remaining=}). Will not process at this time. {mgrs_set_id=}, {i=}")
                    product_burstset_index_to_skip_processing.add(i)
                else:
                    # burst set meets target, and old enough. process
                    logger.info(f"Target covered burst set aged out of grace period ({grace_period_minutes_remaining=}). Will process at this time. {mgrs_set_id=}, {i=}")
                    pass

            for i in sorted(product_burstset_index_to_skip_processing, reverse=True):
                logger.info(f"Removing target covered burst still within grace period. {mgrs_set_id=}, {i=}")
                del evaluator_results["mgrs_sets"][mgrs_set_id][i]
                if not evaluator_results["mgrs_sets"][mgrs_set_id]:
                    del evaluator_results["mgrs_sets"][mgrs_set_id]

    evaluator_results["mgrs_sets"] = dict(evaluator_results["mgrs_sets"])

    return evaluator_results


def evaluate_rtc_products(rtc_product_ids, coverage_target, *args, **kwargs):
    # load MGRS tile collection DB
    mgrs_burst_collections_gdf = mbc_client.cached_load_mgrs_burst_db(filter_land=True)

    # transform product list to DataFrame for evaluation
    cmr_df = load_cmr_df(rtc_product_ids, mgrs_burst_collections_gdf)
    cmr_df = cmr_df.sort_values(by=["relative_orbit_number", "acquisition_dt", "burst_id_normalized", "product_id"])
    cmr_orbits = list(set(flatten(cmr_df["relative_orbit_numbers"].to_list())))
    # a_cmr_df = cmr_df[cmr_df["product_id"].apply(lambda x: x.endswith("S1A_30_v0.4"))]
    # b_cmr_df = cmr_df[cmr_df["product_id"].apply(lambda x: x.endswith("S1B_30_v0.4"))]

    mbc_filtered_gdf = mgrs_burst_collections_gdf[mgrs_burst_collections_gdf["relative_orbit_number"].isin(cmr_orbits)]
    logger.info(f"{len(mbc_filtered_gdf)=}")

    # group by orbit and acquisition time (and burst ID)
    orbit_to_products_map = defaultdict(
        partial(defaultdict, partial(defaultdict, list)))  # optimized data structure to avoid dataframe queries
    for record in cmr_df.to_dict('records'):
        for relative_orbit_number in record["relative_orbit_numbers"]:
            orbit_to_products_map[relative_orbit_number][record["acquisition_dt"]][
                record["burst_id_normalized"]].append(record)
    # TODO chrisjrd: group by time window to eliminate downstream for-loop

    # split into orbits frames
    orbit_to_mbc_orbit_dfs_map = {
        orbit: mbc_filtered_gdf[mbc_filtered_gdf["relative_orbit_number"] == orbit]
        for orbit in cmr_orbits
    }

    logger.info("grouping by sliding time windows")
    orbit_to_interval_to_products_map = evaluator_core.create_orbit_to_interval_to_products_map(orbit_to_products_map, cmr_orbits)
    coverage_result_set_id_to_product_sets_map = evaluator_core.process(orbit_to_interval_to_products_map, orbit_to_mbc_orbit_dfs_map, coverage_target)
    return coverage_result_set_id_to_product_sets_map


def load_cmr_df(rtc_product_ids, gdf):
    cmr_df_records = []
    for product_id in rtc_product_ids:
        match_product_id = re.match(rtc_granule_regex, product_id)
        acquisition_dts = match_product_id.group("acquisition_ts")
        burst_id = match_product_id.group("burst_id")

        burst_id_normalized = product_burst_id_to_mapping_burst_id(burst_id)
        match_burst_id = re.match(rtc_relative_orbit_number_regex, burst_id_normalized)
        relative_orbit_number = int(match_burst_id.group("relative_orbit_number"))
        relative_orbit_numbers = burst_id_to_relative_orbit_numbers(gdf, burst_id_normalized)

        cmr_df_record = {
            "product_id": product_id,
            "acquisition_dts": acquisition_dts,
            "acquisition_dt": dateutil.parser.parse(acquisition_dts),
            "burst_id": burst_id,
            "burst_id_normalized": burst_id_normalized,
            "relative_orbit_number": relative_orbit_number,
            "relative_orbit_numbers": relative_orbit_numbers,
            "product_id_short": (burst_id_normalized, acquisition_dts),
        }
        cmr_df_records.append(cmr_df_record)
    cmr_df = pd.DataFrame(cmr_df_records)
    return cmr_df


def join_product_file_docs(result_set_id_to_product_sets_map, product_id_to_product_files_map):
    set_to_product_file_docs_map = defaultdict(list)
    for mgrs_set_id, sets in result_set_id_to_product_sets_map.items():
        for set_ in sets:
            product_details = list()
            for product_id in set_:
                product_details.append({product_id: product_id_to_product_files_map[product_id]})
            set_to_product_file_docs_map[mgrs_set_id].append(product_details)
    return set_to_product_file_docs_map


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-target", type=int, default=100)
    parser.add_argument("--grace-period", type=int, default=0)
    parser.add_argument("--rtc-product-ids", nargs="*")
    parser.add_argument("--main", action="store_true", default=False)
    args = parser.parse_args(sys.argv[1:])
    if args.main:
        evaluator_results = main(
            coverage_target=args.coverage_target,
            required_min_age_minutes_for_partial_burstsets=args.grace_period
        )
        print(json.dumps(evaluator_results))
    else:
        evaluate_rtc_products(**vars(args))
