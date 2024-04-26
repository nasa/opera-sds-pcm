from data_subscriber import query
from data_subscriber.geojson_utils import localize_include_exclude

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

_S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC = {
    "granule_id": "S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC",
    "bounding_box": [{'lat': 46.255867, 'lon': -76.713303},
                     {'lat': 44.639988, 'lon': -76.258331},
                     {'lat': 45.03212, 'lon': -73.094223},
                     {'lat': 46.648479, 'lon': -73.453896},
                     {'lat': 46.255867, 'lon': -76.713303}]
}

def get_set(filtered_granules):
    result_set = set()
    for granule in filtered_granules:
        result_set.add(granule["granule_id"])

    return result_set

def localize_for_unittest(include_regions, exclude_regions):
    class Arg:
        def __init__(self):
            self.include_regions = include_regions
            self.exclude_regions = exclude_regions
    localize_include_exclude(Arg())

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
    localize_for_unittest(include_regions, exclude_regions)

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
    localize_for_unittest(include_regions, exclude_regions)

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
    localize_for_unittest(include_regions, exclude_regions)

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
    localize_for_unittest(include_regions, exclude_regions)

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
    localize_for_unittest(include_regions, exclude_regions)

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)
    granules.append(_paris)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" not in result_set
    assert "Vegas" in result_set
    assert "Paris" not in result_set

def test_10TFP():
    include_regions = "10TFP"
    exclude_regions = None
    localize_for_unittest(include_regions, exclude_regions)

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)
    granules.append(_paris)
    granules.append(_S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" not in result_set
    assert "Vegas" not in result_set
    assert "Paris" not in result_set
    assert "S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC" not in result_set

def test_cslc_s1_priority_framebased():
    include_regions = "dissolved_cslc-s1_priority_framebased"
    exclude_regions = None
    localize_for_unittest(include_regions, exclude_regions)

    granules = []
    granules.append(_jpl)
    granules.append(_vegas)
    granules.append(_paris)
    granules.append(_S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC)

    filtered_granules = query.filter_granules_by_regions(granules, include_regions, exclude_regions)
    result_set = get_set(filtered_granules)

    assert "JPL" not in result_set
    assert "Vegas" not in result_set
    assert "Paris" not in result_set
    assert "S1A_IW_SLC__1SDV_20160728T225204_20160728T225231_012355_0133F4_8423_SLC" not in result_set