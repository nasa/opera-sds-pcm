import random
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber import query

_jpl = {"granule_id": "JPL", "bounding_box": [
        {"lon": -118.17243, "lat": 34.20025},
        {"lon": -118.17243, "lat": 34.19831},
        {"lon": -118.17558, "lat": 34.19831},
        {"lon": -118.17558, "lat": 34.20025},
        {"lon": -118.17243, "lat": 34.20025}
    ]}

_vegas = {"granule_id": "Vegas", "bounding_box": [
        {"lon": -115.06839, "lat": 36.28141},
        {"lon": -115.06839, "lat": 36.07957},
        {"lon": -115.28098, "lat": 36.07957},
        {"lon": -115.28098, "lat": 36.28141},
        {"lon": -115.06839, "lat": 36.28141}
    ]}

_paris = {"granule_id": "Paris", "bounding_box": [
        {"lon": 2.3522, "lat": 48.8566},
        {"lon": 2.3522, "lat": 48.8577},
        {"lon": 2.3533, "lat": 48.8577},
        {"lon": 2.3533, "lat": 48.8566},
        {"lon": 2.3522, "lat": 48.8566}
    ]}

def get_set(filtered_granules):
    result_set = set()
    for granule in filtered_granules:
        result_set.add(granule["granule_id"])

    return result_set

def test_all():

    include_regions = None
    exclude_regions = None

    granules = []
    granules.append(_jpl)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" in result_set

def test_north_america():

    include_regions = "north_america_opera"
    exclude_regions = None

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)
    granules.append(_paris)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" in result_set
    assert "Vegas" in result_set
    assert "Paris" not in result_set

def test_california():

    include_regions = "california_opera"
    exclude_regions = None

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" in result_set
    assert "Vegas" not in result_set

def test_north_america_except_california():

    include_regions = "north_america_opera"
    exclude_regions = "california_opera"

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" not in result_set
    assert "Vegas" in result_set

def test_all_except_nevada():

    include_regions = None
    exclude_regions = "nevada_opera"

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" in result_set
    assert "Vegas" not in result_set

def test_nevada():

    include_regions = "nevada_opera"
    exclude_regions = None

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)
    granules.append(_paris)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" not in result_set
    assert "Vegas" in result_set
    assert "Paris" not in result_set
