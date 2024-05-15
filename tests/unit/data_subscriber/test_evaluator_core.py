import itertools

import more_itertools

from data_subscriber.rtc.evaluator_core import remove_subsets, reduce_to_largest_set, _find_set_coverage_in_burst


def test_find_set_coverage_in_burst__when_full_coverage():
    # ARRANGE
    mgrs_tile_collection_db_row = {
        "orbits": [1],
        "bursts_parsed": {"T1-1-IW1", "T1-1-IW2", "T1-1-IW3"},
        "mgrs_set_id": "test_mgrs_set_id",
        "number_of_bursts": 3
    }

    # ACT
    id, set_, coverage = _find_set_coverage_in_burst(
        burst_set_row=mgrs_tile_collection_db_row,
        orbit_to_window_to_products_map={
            1: {
                (0, 1): {
                    "T1-1-IW1": [{"product_id": "A"}],
                    "T1-1-IW2": [{"product_id": "B"}],
                    "T1-1-IW3": [{"product_id": "C"}]
                }
            }
        },
        time_window=(0, 1)
    )

    # ASSERT
    assert id == "test_mgrs_set_id"
    assert set_ == {"A", "B", "C"}
    assert coverage == 100


def test_find_set_coverage_in_burst__when_full_coverage__and_multi_revisions():
    # ARRANGE
    mgrs_tile_collection_db_row = {
        "orbits": [1],
        "bursts_parsed": {"T1-1-IW1", "T1-1-IW2", "T1-1-IW3"},
        "mgrs_set_id": "test_mgrs_set_id",
        "number_of_bursts": 3
    }

    # ACT
    id, set_, coverage = _find_set_coverage_in_burst(
        burst_set_row=mgrs_tile_collection_db_row,
        orbit_to_window_to_products_map={
            1: {
                (0, 1): {
                    "T1-1-IW1": [{"product_id": "A"}],
                    "T1-1-IW2": [{"product_id": "B"}],
                    "T1-1-IW3": [{"product_id": "C-r1"}, {"product_id": "C-r2"}]
                }
            }
        },
        time_window=(0, 1)
    )

    # ASSERT
    assert id == "test_mgrs_set_id"
    assert set_ == {"A", "B", "C-r2"}
    assert coverage == 100


def test_find_set_coverage_in_burst__when_partial_coverage():
    # ARRANGE
    mgrs_tile_collection_db_row = {
        "orbits": [1],
        "bursts_parsed": {"T1-1-IW1", "T1-1-IW2", "T1-1-IW3"},
        "mgrs_set_id": "test_mgrs_set_id",
        "number_of_bursts": 3
    }

    # ACT
    id, set_, coverage = _find_set_coverage_in_burst(
        burst_set_row=mgrs_tile_collection_db_row,
        orbit_to_window_to_products_map={
            1: {
                (0, 1): {
                    "T1-1-IW1": [{"product_id": "A"}],
                    "T1-1-IW2": [{"product_id": "B"}]
                }
            }
        },
        time_window=(0, 1)
    )

    # ASSERT
    assert id == "test_mgrs_set_id"
    assert set_ == {"A", "B"}
    assert coverage == 66


def test_find_set_coverage_in_burst__when_partial_coverage_2():
    # ARRANGE
    mgrs_tile_collection_db_row = {
        "orbits": [1],
        "bursts_parsed": {"T1-1-IW1", "T1-1-IW2", "T1-1-IW3"},
        "mgrs_set_id": "test_mgrs_set_id",
        "number_of_bursts": 3
    }

    # ACT
    id, set_, coverage = _find_set_coverage_in_burst(
        burst_set_row=mgrs_tile_collection_db_row,
        orbit_to_window_to_products_map={
            1: {
                (0, 1): {
                    "T1-1-IW1": [{"product_id": "A"}]
                }
            }
        },
        time_window=(0, 1)
    )

    # ASSERT
    assert id == "test_mgrs_set_id"
    assert set_ == {"A"}
    assert coverage == 33


def test_find_set_coverage_in_burst__when_no_coverage():
    # ARRANGE
    mgrs_tile_collection_db_row = {
        "orbits": [1],
        "bursts_parsed": {"T1-1-IW1", "T1-1-IW2", "T1-1-IW3"},
        "mgrs_set_id": "test_mgrs_set_id",
        "number_of_bursts": 3
    }

    # ACT
    id, set_, coverage = _find_set_coverage_in_burst(
        burst_set_row=mgrs_tile_collection_db_row,
        orbit_to_window_to_products_map={
            1: {
                (0, 1): {}
            }
        },
        time_window=(0, 1)
    )

    # ASSERT
    assert id == "test_mgrs_set_id"
    assert set_ == set()
    assert coverage == 0


def test_reduce_to_largest_set():
    # ARRANGE
    sets = [
        frozenset(set_)
        for set_ in itertools.chain(
            more_itertools.powerset({"A", "B", "C"}),
            more_itertools.powerset({"D", "E", "F"})
        )
    ]

    # ACT
    r = reduce_to_largest_set(sets)

    # ASSERT
    assert r in [frozenset({"A", "B", "C"}), frozenset({"D", "E", "F"})]


def test_remove_subsets():
    # ARRANGE
    sets = {frozenset(set_) for set_ in more_itertools.powerset({"A", "B", "C"})}

    # ACT
    r = remove_subsets(sets)

    # ASSERT
    assert r == frozenset({frozenset({"A", "B", "C"})})


def test_remove_subsets__when_empty_return_empty():
    # ARRANGE
    sets = set()

    # ACT
    r = remove_subsets(sets)

    # ASSERT
    assert r == set()


def test_remove_subsets__when_contains_empty_return_empty():
    # ARRANGE
    sets = {frozenset(), frozenset()}

    # ACT
    r = remove_subsets(sets)

    # ASSERT
    assert r == set()
