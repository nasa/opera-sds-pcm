from geo.geo_util import does_bbox_intersect_north_america


def test_bbox_not_in_north_america():
    # random bbox from CMR
    # https://cmr.earthdata.nasa.gov/search/granules.umm_json?provider=ASF&temporal=2022-09-27T00:00:00Z%2C2022-09-28T01:00:00Z&ShortName=SENTINEL-1A_SLC
    bbox = [
        {"lon": -91.324852, "lat": 11.026079},
        {"lon": -90.954483, "lat": 9.222121},
        {"lon": -88.683533, "lat": 9.676103},
        {"lon": -89.040413, "lat": 11.475266},
        {"lon": -91.324852, "lat": 11.026079}
    ]
    assert not does_bbox_intersect_north_america(bbox)


def test_bbox_in_north_america():
    # Colorado bbox
    bbox = [
        {"lon": -109.060253, "lat": 36.992426},
        {"lon": -109.060253, "lat": 41.003444},
        {"lon": -102.041524, "lat": 41.003444},
        {"lon": -102.041524, "lat": 36.992426},
        {"lon": -109.060253, "lat": 36.992426}
    ]
    assert does_bbox_intersect_north_america(bbox)
