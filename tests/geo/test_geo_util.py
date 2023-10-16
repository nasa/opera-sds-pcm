from geo.geo_util import does_bbox_intersect_north_america, does_bbox_intersect_california, Coordinate
from util.geo_util import polygon_from_bounding_box, polygon_from_mgrs_tile

from shapely.geometry import Polygon

def test_bbox_not_in_north_america():
    # random bbox from CMR
    # https://cmr.earthdata.nasa.gov/search/granules.umm_json?provider=ASF&temporal=2022-09-27T00:00:00Z%2C2022-09-28T01:00:00Z&ShortName=SENTINEL-1A_SLC
    bbox: list[Coordinate] = [
        {"lon": -91.324852, "lat": 11.026079},
        {"lon": -90.954483, "lat": 9.222121},
        {"lon": -88.683533, "lat": 9.676103},
        {"lon": -89.040413, "lat": 11.475266},
        {"lon": -91.324852, "lat": 11.026079}
    ]
    assert not does_bbox_intersect_north_america(bbox)


def test_bbox_in_north_america():
    # Colorado bbox
    bbox: list[Coordinate] = [
        {"lon": -109.060253, "lat": 36.992426},
        {"lon": -109.060253, "lat": 41.003444},
        {"lon": -102.041524, "lat": 41.003444},
        {"lon": -102.041524, "lat": 36.992426},
        {"lon": -109.060253, "lat": 36.992426}
    ]
    assert does_bbox_intersect_north_america(bbox)

def test_bbox_not_in_california():
    # Las Vegas-ish bbox
    bbox: list[Coordinate] = [
        {"lon": -115.06839, "lat": 36.28141},
        {"lon": -115.06839, "lat": 36.07957},
        {"lon": -115.28098, "lat": 36.07957},
        {"lon": -115.28098, "lat": 36.28141}
    ]
    assert not does_bbox_intersect_california(bbox)

def test_bbox_in_california():
    # JPL-ish bbox
    bbox: list[Coordinate] = [
        {"lon": -118.17243, "lat": 34.20025},
        {"lon": -118.17243, "lat": 34.19831},
        {"lon": -118.17558, "lat": 34.19831},
        {"lon": -118.17558, "lat": 34.20025}
    ]
    assert does_bbox_intersect_california(bbox)

def test_polygon_from_bounding_box_nominal():
    """Test application of margin to bounding box which does not cross anti-meridian"""
    # bbox obtained from S1A_IW_SLC__1SDH_20230628T122459_20230628T122529_049186_05EA1E_AD77
    bounding_box = [-77.210869, 81.085464, -55.746243, 83.767433]
    poly = polygon_from_bounding_box(bounding_box, margin_in_km=100)

    expected_poly = Polygon.from_bounds(xmin=-47.47175197993814,
                                        ymin=80.1871487229285,
                                        xmax=-85.48536002006186,
                                        ymax=84.6657482770715)

    assert poly.bounds == expected_poly.bounds

def test_polygon_from_bounding_box_antimeridian():
    """Test application of margin to bounding box which crosses the anti-meridian"""
    # Raw bbox obtained from S1B_IW_SLC__1SDV_20210730T183014_20210730T183044_028027_0357ED_4B46
    bounding_box = [-176.387039, 67.454239, 178.635727, 69.701736]
    poly = polygon_from_bounding_box(bounding_box, margin_in_km=100)

    expected_poly = Polygon.from_bounds(xmin=-173.79754190105595,
                                        ymin=66.5559237229285,
                                        xmax=176.04622990105597,
                                        ymax=70.6000512770715)

    assert poly.bounds == expected_poly.bounds

    # "Unwrapped" bbox from S1B_IW_SLC__1SDV_20210730T183014_20210730T183044_028027_0357ED_4B46,
    # as would be returned from geo_util.bounding_box_from_slc_granule()
    bounding_box = [177.793381, 67.454239, 184.918289, 69.701736]
    poly = polygon_from_bounding_box(bounding_box, margin_in_km=100)

    expected_poly = Polygon.from_bounds(xmin=175.20388390105597,
                                        ymin=66.5559237229285,
                                        xmax=187.50778609894402,
                                        ymax=70.6000512770715)

    assert poly.bounds == expected_poly.bounds

def test_polygon_from_mgrs_tile_nominal():
    """Reproduce ADT results from values provided with code"""
    poly = polygon_from_mgrs_tile('15SXR', margin_in_km=0)

    expected_poly = Polygon.from_bounds(xmin=-90.81751155385777,
                                        ymin=31.572733739486036,
                                        xmax=-91.99766472766642,
                                        ymax=32.577473659397235)

    assert poly.bounds == expected_poly.bounds

def test_polygon_from_mgrs_tile_antimeridian():
    """Test MGRS tile code conversion with a tile that crosses the anti-meridian"""
    poly = polygon_from_mgrs_tile('T60VXQ', margin_in_km=0)

    expected_poly = Polygon.from_bounds(xmin=-178.93677941363356,
                                        ymin=62.13198085489144,
                                        xmax= 178.82637550795243,
                                        ymax=63.16076767648831)

    assert poly.bounds == expected_poly.bounds