"""The burst coverage audit must ask CMR for the shortest bounding box that covers its region.

The box is the only spatial filter CMR applies; the polygon filter runs afterwards on what came back.
A region with coordinates on both sides of the antimeridian used to get the plain min/max box, which
spans every longitude: the North America audit listed about ten SLCs for each one it kept.
"""
import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("USER", "pytest")  # cmr_client builds its Client-Id from it

from tools.ops.cmr_audit.cmr_audit_burst_coverage import geojson_to_bbox, shortest_lon_arc

REPO = Path(__file__).resolve().parents[5]


def _region(*polygons):
    """A FeatureCollection with one Polygon feature per list of (lon, lat) corners."""
    features = [{"type": "Feature", "properties": {},
                 "geometry": {"type": "Polygon", "coordinates": [[list(p) for p in corners] + [list(corners[0])]]}}
                for corners in polygons]
    return {"type": "FeatureCollection", "features": features}


CONUS = _region([(-125, 24), (-66, 24), (-66, 49), (-125, 49)])
ALEUTIANS_EAST_OF_180 = _region([(172, 51), (179.8, 51), (179.8, 53), (172, 53)])
ALASKA_WEST_OF_180 = _region([(-179.1, 51), (-130, 51), (-130, 71), (-179.1, 71)])


def _both_sides():
    return {"type": "FeatureCollection",
            "features": ALEUTIANS_EAST_OF_180["features"] + ALASKA_WEST_OF_180["features"] + CONUS["features"]}


def test_a_region_clear_of_the_antimeridian_gets_the_plain_box():
    assert geojson_to_bbox(CONUS) == (-125, 24, -66, 49)


def test_a_region_on_both_sides_of_the_antimeridian_gets_a_box_that_crosses_it():
    west, south, east, north = geojson_to_bbox(_both_sides())
    # 172°E eastward through 180° to 66°W: 122° of longitude, not the 359° of a min/max box
    assert (west, east) == (172, -66)
    assert west > east
    assert (south, north) == (24, 71)


def test_the_arc_starts_after_the_widest_gap_between_longitudes():
    assert shortest_lon_arc([-170, 170]) == (170, -170)        # 20° through 180°, not 340° through 0°
    assert shortest_lon_arc([-100, -90, 100]) == (100, -90)    # 170° through 180°, not 200° through 0°
    assert shortest_lon_arc([10, 20, 30]) == (10, 30)
    assert shortest_lon_arc([-179.9, 179.9]) == (179.9, -179.9)
    assert shortest_lon_arc([42.0]) == (42.0, 42.0)


def test_the_buffer_widens_every_side_and_wraps_longitude_at_180():
    west, south, east, north = geojson_to_bbox(ALEUTIANS_EAST_OF_180, buffer_deg=0.5)
    assert (west, south, north) == (171.5, 50.5, 53.5)
    assert east == pytest.approx(-179.7)   # 179.8 + 0.5 wrapped: the buffered box now crosses 180°
    assert west > east


def test_the_buffer_clamps_latitude_and_never_produces_a_minus_180_east_edge():
    polar = _region([(100, 85), (179.5, 85), (179.5, 89.8), (100, 89.8)])
    west, south, east, north = geojson_to_bbox(polar, buffer_deg=0.5)
    assert (west, south, east, north) == (99.5, 84.5, 180.0, 90.0)


def test_a_buffer_that_closes_the_circle_gives_the_global_box():
    # A corner every 1° round the globe: no gap is wider than the 0.5° buffer on each side, so the
    # buffered arc closes and the box is the whole world (as it is for a global land file)
    corners = [(lon + 0.5, 0) for lon in range(-180, 180)] + [(179.5, 10), (-179.5, 10)]
    assert geojson_to_bbox(_region(corners), buffer_deg=0.5) == (-180.0, -0.5, 180.0, 10.5)
    assert geojson_to_bbox(_region(corners)) == (-179.5, 0, 179.5, 10)


def test_two_corners_at_the_antimeridian_mean_a_sliver_across_it_not_the_whole_globe():
    # GeoJSON edges take the short way round, so a polygon from 179.8°E to 179.8°W is 0.4° wide
    sliver = _region([(-179.8, 0), (179.8, 0), (179.8, 10), (-179.8, 10)])
    assert geojson_to_bbox(sliver) == (179.8, 0, -179.8, 10)


def test_an_empty_geojson_is_refused():
    with pytest.raises(ValueError):
        geojson_to_bbox({"type": "FeatureCollection", "features": []})


def test_the_north_america_region_crosses_the_antimeridian_from_guam_to_the_atlantic():
    # The repo's region file: Guam/CNMI and the western Aleutians sit east of 180°. Before the fix the
    # box for it, with the audit's default 0.5° buffer, was -179.64,-15.03,180.0,71.91.
    geojson = json.loads((REPO / "geo" / "north_america_opera.geojson").read_text())
    west, south, east, north = geojson_to_bbox(geojson, buffer_deg=0.5)
    assert west == pytest.approx(144.12, abs=0.01)
    assert east == pytest.approx(-64.06, abs=0.01)
    assert west > east
    assert (south, north) == (pytest.approx(-15.03, abs=0.01), pytest.approx(71.91, abs=0.01))
