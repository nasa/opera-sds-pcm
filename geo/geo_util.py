import json
import logging
from functools import cache
from pathlib import Path
from typing import Union

import matplotlib
import numpy as np


def is_in_north_america(lat: Union[float, tuple[float, float]], lon: float) -> bool:
    """
    Check whether a coordinate is in North America (OPERA).

    North America (OPERA) is defined as the land areas covering the southern Canadian border with the
    United States, The United States and its territories, Mexico, and Central America.

    :param lat: the latitude component of the coordinate. If a tuple is supplied, the tuples elements are considered
                the coordinate components, and param `lon` is ignored.
    :param lon: the longitudinal component of the coordinate.
    :return: True if the given coordinate lies within North America (OPERA). Otherwise False.
    """
    point = lat if type(lat) == tuple else (lat, lon)
    logging.info(f'{point=}')

    north_america_opera_geojson = cached_load_north_america_opera_geojson()

    countries_features = north_america_opera_geojson["features"]
    country_feature: dict
    for country_feature in countries_features:
        country_geometry = country_feature["geometry"]
        country_name = country_feature["properties"]["WB_NAME"]
        logging.info(f"Checking {country_name} for {point=}")

        logging.debug(f'{country_geometry["type"]}')
        if country_geometry["type"] == "Polygon":
            country_polygon = country_geometry["coordinates"]
            if is_point_in_north_america := is_within_polygon(point, country_polygon):
                return is_point_in_north_america
        elif country_geometry["type"] == "MultiPolygon":
            country_polygons = country_geometry["coordinates"]
            for i, country_polygon in enumerate(country_polygons, start=1):
                if is_point_in_north_america := is_within_polygon(point, country_polygon):
                    return is_point_in_north_america
        else:
            logging.warning(f'Unrecognized {country_geometry["type"]=}')
    return False


@cache
def cached_load_north_america_opera_geojson() -> dict:
    """Loads a RFC7946 GeoJSON file."""
    with Path(__file__).parent.joinpath('north_america_opera.geojson').open() as fp:
        geojson_obj: dict = json.load(fp)
    return geojson_obj


def is_within_polygon(point: tuple[float, float], polygon: matplotlib.path.Path) -> bool:
    for i, gon in enumerate(polygon, start=1):
        poly_path = matplotlib.path.Path(np.array([coordinate for coordinate in gon]))
        is_point_within = poly_path.contains_point(point, radius=0.000000000000011)  # radius expands polygon border for comparison. needed for edge cases
        if is_point_within:
            logging.debug(f"{point=} is in polygon {i}: {is_point_within=}")
            return is_point_within
