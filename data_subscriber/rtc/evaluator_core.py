import concurrent
import itertools
import logging
import math
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from functools import partial
from typing import Iterable

from geopandas import GeoDataFrame
from pandas import Series

from util.sds_itertools import windowed_by_predicate

logger = logging.getLogger(__name__)

Interval = namedtuple("Interval", ["start", "end"])


def create_orbit_to_interval_to_products_map(orbit_to_products_map, orbits: Iterable[int]):

    orbit_to_interval_to_products_map = defaultdict(partial(defaultdict, partial(defaultdict, set)))
    futures = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for orbit in orbits:
            future = executor.submit(_create_orbit_to_interval_to_products_map_helper, orbit_to_products_map, orbit)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            orbit_to_interval_to_products_map.update(future.result())

    return orbit_to_interval_to_products_map


def _create_orbit_to_interval_to_products_map_helper(orbit_to_products_map, orbit: int):
    orbit_to_interval_to_products_map = defaultdict(partial(defaultdict, partial(defaultdict, set)))

    acquisition_dts = sorted(orbit_to_products_map[orbit].keys())
    BURST_SET_MAX_DURATION_SECONDS = 62 * 2.7 + 1  # 62==max_sized burst set, 2.7 ~ time between bursts, 1 == safe margin
    BURST_SET_MAX_DURATION_MINUTES = math.ceil(BURST_SET_MAX_DURATION_SECONDS / 60)
    dt_windows = windowed_by_predicate(iterable=acquisition_dts, pred=lambda a, b: max(a, b) - min(a, b) <= timedelta(minutes=BURST_SET_MAX_DURATION_MINUTES), sorted_=True, set_=False)

    # remove redundant subsets
    dt_intervals = [Interval(w[0], w[-1]) for w in dt_windows]
    dt_intervals = [a for a in dt_intervals if not any(issubinterval(a, b) for b in dt_intervals)]

    for dt_interval in dt_intervals:
        acquisition_dts = orbit_to_products_map[orbit].keys()
        for acquisition_dt in acquisition_dts:
            if dt_interval.start <= acquisition_dt <= dt_interval.end:
                orbit_to_interval_to_products_map[orbit][dt_interval].update(orbit_to_products_map[orbit][acquisition_dt])

    return orbit_to_interval_to_products_map


def issubinterval(x: tuple[datetime], y: tuple[datetime], strict=True):
    if strict:  # half-open interval
        return (x[0] > y[0] and x[1] <= y[1]) or (x[0] >= y[0] and x[1] < y[1])
    else:  # closed interval
        return x[0] >= y[0] and x[1] <= y[1]


def process(orbit_to_interval_to_products_map: dict, orbit_to_mbc_orbit_dfs_map: dict, coverage_target: int = 99):
    """The main entry point into evaluator core"""
    logger.info("BEGIN")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_orbit, orbit_to_interval_to_products_map, orbit, mbc_orbit_df, coverage_target)
            for orbit, mbc_orbit_df in orbit_to_mbc_orbit_dfs_map.items()
        ]
        coverage_to_set_id_to_product_sets_maps = [future.result() for future in concurrent.futures.as_completed(futures)]
    logger.info("DONE")

    logger.info("Cleaning up the sets")

    logger.info("Collecting as set of sets")
    coverage_result_set_id_to_product_sets_map = {
        100: {},
        coverage_target: {},
        -1: {}
    }
    for coverage_to_set_id_to_product_sets_map in coverage_to_set_id_to_product_sets_maps:
        fully_covered_set_id_to_product_sets_map = coverage_to_set_id_to_product_sets_map[100]
        for set_id, product_sets in fully_covered_set_id_to_product_sets_map.items():
            s = product_sets
            r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
            coverage_result_set_id_to_product_sets_map[100][set_id] = r

        target_covered_set_id_to_product_sets_map = coverage_to_set_id_to_product_sets_map[coverage_target]
        for set_id, product_sets in target_covered_set_id_to_product_sets_map.items():
            s = product_sets
            r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
            coverage_result_set_id_to_product_sets_map[coverage_target][set_id] = r

        not_covered_set_id_to_product_sets_map = coverage_to_set_id_to_product_sets_map[-1]
        for set_id, product_sets in not_covered_set_id_to_product_sets_map.items():
            s = product_sets
            r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
            coverage_result_set_id_to_product_sets_map[-1][set_id] = r

    return coverage_result_set_id_to_product_sets_map


def _find_set_coverage_in_orbit(orbit_to_window_to_records_map: dict, orbit, mbc_orbit_df: GeoDataFrame, coverage_target: int):
    if not orbit_to_window_to_records_map:
        return {}
    if mbc_orbit_df is None or mbc_orbit_df.empty:
        return {}

    logger.info(f"Processing {orbit=}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_time_window, window, orbit_to_window_to_records_map, mbc_orbit_df, coverage_target)
            for window in orbit_to_window_to_records_map[orbit]
        ]
        set_id_to_product_sets_maps = [future.result() for future in concurrent.futures.as_completed(futures)]

    coverage_set_id_to_product_sets_map_final = {
        100: defaultdict(set),
        coverage_target: defaultdict(set),
        -1: defaultdict(set)
    }
    for coverage_set_id_to_product_sets_map in set_id_to_product_sets_maps:
        fully_covered_set_id_to_product_sets_map = coverage_set_id_to_product_sets_map[100]
        for set_id, product_set in fully_covered_set_id_to_product_sets_map.items():
            if product_set:
                coverage_set_id_to_product_sets_map_final[100][set_id].add(product_set)

        target_covered_set_id_to_product_sets_map = coverage_set_id_to_product_sets_map[coverage_target]
        for set_id, product_set in target_covered_set_id_to_product_sets_map.items():
            if product_set:
                coverage_set_id_to_product_sets_map_final[coverage_target][set_id].add(product_set)

        not_covered_set_id_to_product_sets_map = coverage_set_id_to_product_sets_map[-1]
        for set_id, product_set in not_covered_set_id_to_product_sets_map.items():
            if product_set:
                coverage_set_id_to_product_sets_map_final[-1][set_id].add(product_set)

    return dict(coverage_set_id_to_product_sets_map_final)


def _find_set_coverage_in_time_window(time_window, orbit_to_window_to_products_map: dict, mbc_orbit_df: GeoDataFrame, coverage_target: int):
    logger.debug(f"Processing {time_window=}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_burst, burst_set_row, orbit_to_window_to_products_map, time_window, coverage_target)
            for index, burst_set_row in mbc_orbit_df.iterrows()
        ]
        found_product_sets = [future.result() for future in concurrent.futures.as_completed(futures)]

    coverage_to_mgrs_set_id_to_product_sets_map = {
        100: defaultdict(set),
        coverage_target: defaultdict(set),
        -1: defaultdict(set)
    }
    for mgrs_set_id, coverage_results_map in found_product_sets:
        result_sets_fully_covered = coverage_results_map[100]
        if result_sets_fully_covered:  # filter out empty/partial results
            for mgrs_set_id, product_sets in result_sets_fully_covered.items():  # TODO chrisjrd: may be singleton dict
                if product_sets:
                    coverage_to_mgrs_set_id_to_product_sets_map[100][mgrs_set_id].add(product_sets)

        result_sets_target_covered = coverage_results_map[coverage_target]
        if result_sets_target_covered:  # filter out empty/partial results
            for mgrs_set_id, product_sets in result_sets_target_covered.items():  # TODO chrisjrd: may be singleton dict
                if product_sets:
                    coverage_to_mgrs_set_id_to_product_sets_map[coverage_target][mgrs_set_id].add(product_sets)

        result_sets_not_covered = coverage_results_map[-1]
        if result_sets_not_covered:  # filter out empty/partial results
            for mgrs_set_id, product_sets in result_sets_not_covered.items():  # TODO chrisjrd: may be singleton dict
                if product_sets:
                    coverage_to_mgrs_set_id_to_product_sets_map[-1][mgrs_set_id].add(product_sets)

    coverage_to_mgrs_set_id_to_product_sets_map[100] = remove_redundant_subsets(coverage_to_mgrs_set_id_to_product_sets_map[100])
    coverage_to_mgrs_set_id_to_product_sets_map[coverage_target] = remove_redundant_subsets(coverage_to_mgrs_set_id_to_product_sets_map[coverage_target])
    coverage_to_mgrs_set_id_to_product_sets_map[-1] = remove_redundant_subsets(coverage_to_mgrs_set_id_to_product_sets_map[-1])
    return coverage_to_mgrs_set_id_to_product_sets_map


def _find_set_coverage_in_burst(burst_set_row: Series, orbit_to_window_to_products_map: dict, time_window, coverage_target: int):
    orbits = burst_set_row["orbits"]
    cmr_bursts = set(itertools.chain.from_iterable(
        orbit_to_window_to_products_map[orbit][time_window].keys()
        for orbit in orbits
    ))
    found_bursts = set(burst_set_row["bursts_parsed"]).intersection(cmr_bursts)
    mgrs_set_id = burst_set_row["mgrs_set_id"]

    # if not found_bursts:
    #     return mgrs_set_id, {True: {}, False: {}}

    coverage = len(found_bursts) / burst_set_row["number_of_bursts"]

    logger.debug(f"{mgrs_set_id=}")
    product_set = {
        product["product_id"]  # TODO chrisjrd: consider returning full product. client code can filter for ID
        for burst in found_bursts
        for orbit in orbits
        # the defaultdict creates an empty set() upon lookup with []. workaround is using .get() to return a default empty list
        for product in orbit_to_window_to_products_map[orbit][time_window].get(burst, [])[:1]  # TODO chrisjrd: which one to add?
    }

    coverage_results_map = {
        100: {},
        coverage_target: {},
        -1: {}
    }

    if int(coverage * 100) >= 100:
        coverage_results_map[100] = {mgrs_set_id: frozenset(product_set)}
    elif int(coverage * 100) >= coverage_target:
        coverage_results_map[coverage_target] = {mgrs_set_id: frozenset(product_set)}
    else:
        coverage_results_map[-1] = {mgrs_set_id: frozenset(product_set)}
    return mgrs_set_id, coverage_results_map


def remove_redundant_subsets(mgrs_set_id_to_product_sets_map: dict):
    mgrs_set_id_to_product_sets_clean_map = {}
    for mgrs_set_id, sets in mgrs_set_id_to_product_sets_map.items():
        sets_fs = frozenset(filter(lambda it: it, sets))  # filter out empty results. dedupe
        s = sets_fs
        r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
        sets_fs = frozenset(r)
        if sets_fs:
            # keep the first longest burst subset, in case distinct sets are found (extremely unlikely)
            mgrs_set_id_to_product_sets_clean_map[mgrs_set_id] = max(sets_fs, key=len)

    return mgrs_set_id_to_product_sets_clean_map
