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


def process(orbit_to_interval_to_products_map: dict, orbit_to_mbc_orbit_dfs_map: dict):
    """The main entry point into evaluator core"""
    logger.info("BEGIN")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_orbit, orbit_to_interval_to_products_map, orbit, mbc_orbit_df)
            for orbit, mbc_orbit_df in orbit_to_mbc_orbit_dfs_map.items()
        ]
        set_id_to_product_sets_maps = [future.result() for future in concurrent.futures.as_completed(futures)]
    logger.info("DONE")

    logger.info("Cleaning up the sets")

    logger.info("Collecting as set of sets")
    result_set_id_to_product_sets_map = {}
    incomplete_result_set_id_to_product_sets_map = {}
    product_sets_final = set()
    incomplete_product_sets_final = set()
    for set_id_to_product_sets_map, incomplete_set_id_to_product_sets_map in set_id_to_product_sets_maps:
        for set_id, product_sets in set_id_to_product_sets_map.items():
            s = product_sets
            r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
            result_set_id_to_product_sets_map[set_id] = r

            for product_set in product_sets:
                product_sets_final.add(product_set)
        for set_id, product_sets in incomplete_set_id_to_product_sets_map.items():
            s = product_sets
            r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
            incomplete_result_set_id_to_product_sets_map[set_id] = r

            for product_set in product_sets:
                incomplete_product_sets_final.add(product_set)
    product_sets_final = frozenset(product_sets_final)
    incomplete_product_sets_final = frozenset(incomplete_product_sets_final)
    logger.info(f"{len(product_sets_final)=}")
    logger.info(f"{len(incomplete_product_sets_final)=}")

    logger.info("Removing redundant subsets")

    s = product_sets_final
    r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
    product_sets_final = r
    logger.info(f"{len(product_sets_final)=}")

    s = incomplete_product_sets_final
    r = {a for a in s if not any(a < b for b in s)}  # remove redundant subsets
    incomplete_product_sets_final = r
    logger.info(f"{len(incomplete_product_sets_final)=}")

    logger.info("Optionally converting to list of sets")
    product_sets_final = [list(fs) for fs in product_sets_final]  # convert to lists
    return result_set_id_to_product_sets_map, incomplete_result_set_id_to_product_sets_map


def _find_set_coverage_in_orbit(orbit_to_window_to_records_map: dict, orbit, mbc_orbit_df: GeoDataFrame):
    if not orbit_to_window_to_records_map:
        return {}
    if mbc_orbit_df is None or mbc_orbit_df.empty:
        return {}

    logger.info(f"Processing {orbit=}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_time_window, window, orbit_to_window_to_records_map, mbc_orbit_df)
            for window in orbit_to_window_to_records_map[orbit]
        ]
        set_id_to_product_sets_maps = [future.result() for future in concurrent.futures.as_completed(futures)]

    set_id_to_product_sets_map_final = defaultdict(set)
    incomplete_set_id_to_product_sets_map_final = defaultdict(set)
    for set_id_to_product_sets_map, incomplete_set_id_to_product_sets_map in set_id_to_product_sets_maps:
        for set_id, product_set in set_id_to_product_sets_map.items():
            if product_set:  # filter out empty results
                set_id_to_product_sets_map_final[set_id].add(product_set)
        for set_id, product_set in incomplete_set_id_to_product_sets_map.items():
            if product_set:  # filter out empty results
                incomplete_set_id_to_product_sets_map_final[set_id].add(product_set)

    return dict(set_id_to_product_sets_map_final), dict(incomplete_set_id_to_product_sets_map_final)


def _find_set_coverage_in_time_window(time_window, orbit_to_window_to_products_map: dict, mbc_orbit_df: GeoDataFrame):
    logger.debug(f"Processing {time_window=}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(_find_set_coverage_in_burst, burst_set_row, orbit_to_window_to_products_map, time_window)
            for index, burst_set_row in mbc_orbit_df.iterrows()
        ]
        found_product_sets = [future.result() for future in concurrent.futures.as_completed(futures)]

    mgrs_set_id_to_product_sets_map = defaultdict(set)  # list of product sets
    incomplete_mgrs_set_id_to_product_sets_map = defaultdict(set)  # list of product sets
    for mgrs_set_id, results_sets_not_covered, results_sets_covered in found_product_sets:
        if results_sets_covered:  # filter out empty/partial results
            for mgrs_set_id, product_sets in results_sets_covered.items():  # TODO chrisjrd: may be singleton dict
                if product_sets:
                    mgrs_set_id_to_product_sets_map[mgrs_set_id].add(product_sets)
        if results_sets_not_covered:  # filter out empty/partial results
            for mgrs_set_id, product_sets in results_sets_not_covered.items():  # TODO chrisjrd: may be singleton dict
                if product_sets:
                    incomplete_mgrs_set_id_to_product_sets_map[mgrs_set_id].add(product_sets)

    mgrs_set_id_to_product_sets_map = remove_redundant_subsets(mgrs_set_id_to_product_sets_map)
    incomplete_mgrs_set_id_to_product_sets_map = remove_redundant_subsets(incomplete_mgrs_set_id_to_product_sets_map)
    return mgrs_set_id_to_product_sets_map, incomplete_mgrs_set_id_to_product_sets_map


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


def _find_set_coverage_in_burst(burst_set_row: Series, orbit_to_window_to_products_map: dict, time_window):
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

    results_partitioned = {True: {}, False: {}}
    if coverage >= 0.99:  # TODO chrisjrd: finalize coverage value. store and read from config somehow
        results_partitioned[True] = {mgrs_set_id: frozenset(product_set)}

        return mgrs_set_id, results_partitioned[False], results_partitioned[True]
    else:
        results_partitioned[False] = {mgrs_set_id: frozenset(product_set)}

        return mgrs_set_id, results_partitioned[False], results_partitioned[True]
