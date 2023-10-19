import json
import logging
from functools import cache
from pathlib import Path
from typing import TypedDict

from osgeo import ogr

logger = logging.getLogger(__name__)


_NORTH_AMERICA = "north_america_opera"
_CALIFORNIA = "california_opera"

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
    return does_bbox_intersect_region(bbox, _NORTH_AMERICA)

def does_bbox_intersect_california(bbox: list[Coordinate]) -> bool:
    """
    Check whether a bbox intersects California (OPERA).

    California (OPERA) is defined as the land areas covering the State of California in the United States.

    :param bbox: a list of coordinate dicts. `bbox["lat"]` refers to the latitude component of the coordinate.
                 `bbox["lon"]` refers to the longitudinal component of the coordinate.
    :return: True if the given coordinates intersect with North America (OPERA). Otherwise False.
    """
    return does_bbox_intersect_region(bbox, _CALIFORNIA)

def does_bbox_intersect_region(bbox: list[Coordinate], region) -> bool:
    """
    Check whether a bbox intersects a particular region defined by a geojson file.

    :param bbox: a list of coordinate dicts. `bbox["lat"]` refers to the latitude component of the coordinate.
                 `bbox["lon"]` refers to the longitudinal component of the coordinate.
           region: string name of the geojson file without the extension
    :return: True if the given coordinates intersect with North America (OPERA). Otherwise False.
    """
    logger.info(f"{bbox=}")

    bbox_ring = ogr.Geometry(ogr.wkbLinearRing)
    for coordinate in bbox:
        bbox_ring.AddPoint(coordinate["lon"], coordinate["lat"])
    bbox_poly = ogr.Geometry(ogr.wkbPolygon)
    bbox_poly.AddGeometry(bbox_ring)

    na_geom = _load_region_opera_geometry_collection(region)

    is_bbox_in_region = na_geom.Intersects(bbox_poly)
    logger.info(f"{is_bbox_in_region=}")
    return is_bbox_in_region


@cache
def _load_region_opera_geometry_collection(region) -> ogr.Geometry:
    region_opera_geojson = _cached_load_region_opera_geojson(region)

    na_geoms = ogr.Geometry(ogr.wkbGeometryCollection)
    for feature in region_opera_geojson["features"]:
        na_geoms.AddGeometry(ogr.CreateGeometryFromJson(json.dumps(feature["geometry"])))

    logger.info("Loaded geojson as osgeo GeometryCollection")
    return na_geoms


@cache
def _cached_load_region_opera_geojson(region) -> dict:
    """Loads a RFC7946 GeoJSON file."""
    geojson = region + '.geojson'
    fp = open(geojson)
    geojson_obj: dict = json.load(fp)

    logger.info("Loaded " + geojson)
    return geojson_obj
