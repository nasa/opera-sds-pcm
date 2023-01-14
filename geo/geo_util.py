import json
import logging
from functools import cache
from pathlib import Path
from typing import TypedDict

from osgeo import ogr


class Coordinate(TypedDict):
    lat: float
    lon: float


def does_bbox_intersect_north_america(bbox: list[Coordinate]) -> bool:
    """
    Check whether a bbox intersects North America (OPERA).

    North America (OPERA) is defined as the land areas covering the southern Canadian border with the
    United States, The United States and its territories, Mexico, and Central America.

    :param bbox: a list of coordinate dicts. `bbox["lat"]` refers to the latitude component of the coordinate.
                 `bbox["lon"]` refers to the longitudinal component of the coordinate.
    :return: True if the given coordinates intersect with North America (OPERA). Otherwise False.
    """
    logging.info(f"{bbox=}")

    bbox_ring = ogr.Geometry(ogr.wkbLinearRing)
    for coordinate in bbox:
        bbox_ring.AddPoint(coordinate["lon"], coordinate["lat"])
    bbox_poly = ogr.Geometry(ogr.wkbPolygon)
    bbox_poly.AddGeometry(bbox_ring)

    na_geom = _load_north_america_opera_geometry_collection()

    is_bbox_in_north_america = na_geom.Intersects(bbox_poly)
    logging.info(f"{is_bbox_in_north_america=}")
    return is_bbox_in_north_america


@cache
def _load_north_america_opera_geometry_collection() -> ogr.Geometry:
    north_america_opera_geojson = _cached_load_north_america_opera_geojson()

    na_geoms = ogr.Geometry(ogr.wkbGeometryCollection)
    for feature in north_america_opera_geojson["features"]:
        na_geoms.AddGeometry(ogr.CreateGeometryFromJson(json.dumps(feature["geometry"])))

    logging.info("Loaded geojson as osgeo GeometryCollection")
    return na_geoms


@cache
def _cached_load_north_america_opera_geojson() -> dict:
    """Loads a RFC7946 GeoJSON file."""
    with Path(__file__).parent.joinpath('north_america_opera.geojson').open() as fp:
        geojson_obj: dict = json.load(fp)

    logging.info("Loaded geojson")
    return geojson_obj
