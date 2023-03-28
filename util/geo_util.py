#!/usr/bin/env python

import mgrs
import numpy as np
import shapely.ops
import shapely.wkt

from osgeo import osr
from shapely.geometry import box, LinearRing, Point, Polygon


EARTH_APPROX_CIRCUNFERENCE = 40075017.


def polygon_from_mgrs_tile(mgrs_tile_code, margin_in_km,
                           flag_use_m_to_deg_conversion_at_equator=True):
    """
    Create a polygon (EPSG:4326) from the lat/lon coordinates corresponding to
    a MGRS tile bounding box.

    Parameters
    -----------
    mgrs_tile_code : str
        MGRS tile code corresponding to the polygon to derive.
    margin_in_km : float
        Margin in kilometers to be added to MGRS bounding box
    flag_use_m_to_deg_conversion_at_equator : bool
        Flag to use the conversion from meters to lat/lon degrees at
        the Equator, rather than adding the margin to the MGRS tile
        grid in meters before conversion to geographic coordinates (lat/lon).
        This option is given because of the asymmetry in converting the
        margin in km to degrees near the poles. For example, a margin
        of 200km near the Equator is equivalent to 1.8 deg (latitude or
        longitude). At 82 degrees latitude, the same 200km is equivalent to
        12.9 degrees in longitude.

    Notes
    -----
    This function was adapted from the get_geographic_boundaries_from_mgrs_tile
    function developed by Gustavo Shiroma.
    See https://github.com/opera-adt/PROTEUS/blob/08fd57c64fec6f9d2e02da7e84aca86982f9bccd/src/proteus/core.py#L93

    In the case of antimeridian crossing, `lon_max - lon_min` will be greater
    than 180 deg, and the MGRS tile polygon will represent the complement
    (in longitude) of the actual tile polygon. This edge case will be detected
    and handled by the subsequent function  `check_dateline()`

    Returns
    -------
    poly: shapely.Geometry.Polygon
        Bounding polygon corresponding to the provided MGRS tile code.
    margin_in_km: float, optional
        Margin in kilometers to be added to MGRS bounding box

    """
    mgrs_obj = mgrs.MGRS()

    if mgrs_tile_code.startswith('T'):
        mgrs_tile_code = mgrs_tile_code[1:]

    lower_left_utm_coordinate = mgrs_obj.MGRSToUTM(mgrs_tile_code)
    utm_zone = lower_left_utm_coordinate[0]
    is_northern = lower_left_utm_coordinate[1] == 'N'
    x_min = lower_left_utm_coordinate[2]
    y_min = lower_left_utm_coordinate[3]

    # create UTM spatial reference
    utm_coordinate_system = osr.SpatialReference()
    utm_coordinate_system.SetWellKnownGeogCS("WGS84")
    utm_coordinate_system.SetUTM(utm_zone, is_northern)

    # create geographic (lat/lon) spatial reference
    wgs84_coordinate_system = osr.SpatialReference()
    wgs84_coordinate_system.SetWellKnownGeogCS("WGS84")

    # create transformation of coordinates from UTM to geographic (lat/lon)
    transformation = osr.CoordinateTransformation(
        utm_coordinate_system, wgs84_coordinate_system
    )

    # compute boundaries
    elevation = 0
    lat_min = None
    lat_max = None
    lon_min = None
    lon_max = None

    # Add margin to the bounding polygon
    if flag_use_m_to_deg_conversion_at_equator:
        km_to_deg_at_equator = 1000. / (EARTH_APPROX_CIRCUNFERENCE / 360.)
        margin_in_deg = margin_in_km * km_to_deg_at_equator
    else:
        margin_in_deg = 0

    for offset_x_multiplier in range(2):
        for offset_y_multiplier in range(2):

            # We are using MGRS 100km x 100km tiles
            # HLS tiles have 4.9 km of margin => width/length = 109.8 km
            x = x_min - 4.9 * 1000 + offset_x_multiplier * 109.8 * 1000
            y = y_min - 4.9 * 1000 + offset_y_multiplier * 109.8 * 1000

            if not flag_use_m_to_deg_conversion_at_equator:
                x += (2 * (float(offset_x_multiplier) - 0.5) *
                      margin_in_km * 1000)
                y += (2 * (float(offset_y_multiplier) - 0.5) *
                      margin_in_km * 1000)

            lat, lon, z = transformation.TransformPoint(x, y, elevation)

            if flag_use_m_to_deg_conversion_at_equator:
                lon += 2 * (float(offset_x_multiplier) - 0.5) * margin_in_deg
                lat += 2 * (float(offset_y_multiplier) - 0.5) * margin_in_deg

            if lat_min is None or lat_min > lat:
                lat_min = lat
            if lat_max is None or lat_max < lat:
                lat_max = lat
            if lon_min is None or lon_min > lon:
                lon_min = lon
            if lon_max is None or lon_max < lon:
                lon_max = lon

    # In the case of antimeridian crossing, `lon_max - lon_min` will be greater
    # than 180 deg, and the MGRS tile polygon will represent the complement
    # (in longitude) of the actual tile polygon. This edge case will be detected
    # and handled by the subsequent function `check_dateline()`
    coords = [lon_min, lat_min, lon_max, lat_max]

    poly = box(*coords)

    return poly


def check_dateline(poly):
    """
    Split `poly` if it crosses the dateline.

    Parameters
    ----------
    poly : shapely.geometry.Polygon
        Input polygon.

    Returns
    -------
    polys : list of shapely.geometry.Polygon
        A list containing: the input polygon if it didn't cross the dateline, or
        two polygons otherwise (one on either side of the dateline).

    """
    x_min, _, x_max, _ = poly.bounds

    # Check dateline crossing
    if ((x_max - x_min > 180.0) or (x_min <= 180.0 <= x_max)):
        dateline = shapely.wkt.loads('LINESTRING( 180.0 -90.0, 180.0 90.0)')

        # build new polygon with all longitudes between 0 and 360
        x, y = poly.exterior.coords.xy
        new_x = (k + (k <= 0.) * 360 for k in x)
        new_ring = LinearRing(zip(new_x, y))

        # Split input polygon
        # (https://gis.stackexchange.com/questions/232771/splitting-polygon-by-linestring-in-geodjango_)
        merged_lines = shapely.ops.linemerge([dateline, new_ring])
        border_lines = shapely.ops.unary_union(merged_lines)
        decomp = shapely.ops.polygonize(border_lines)

        polys = list(decomp)
        for polygon_count in range(2):
            x, y = polys[polygon_count].exterior.coords.xy
            # if there are no longitude values above 180, continue
            if not any([k > 180 for k in x]):
                continue

            # otherwise, wrap longitude values down by 360 degrees
            x_wrapped_minus_360 = np.asarray(x) - 360
            polys[polygon_count] = Polygon(zip(x_wrapped_minus_360, y))

        assert (len(polys) == 2)
    else:
        # If dateline is not crossed, treat input poly as list
        polys = [poly]

    return polys


def point2epsg(lon, lat):
    """
    Return an EPSG code based on the provided lat/lon point.

    Parameters
    ----------
    lat: float
        Latitude coordinate of the point
    lon: float
        Longitude coordinate of the point

    Returns
    -------
    EPSG code corresponding to the point lat/lon coordinates.

    Raises
    ------
    ValueError
        If the EPSG code cannot be determined from the provided lat/lon.

    """
    if lon >= 180.0:
        lon = lon - 360.0
    if lat >= 75.0:
        return 3413
    elif lat <= -75.0:
        return 3031
    elif lat > 0:
        return 32601 + int(np.round((lon + 177) / 6.0))
    elif lat < 0:
        return 32701 + int(np.round((lon + 177) / 6.0))
    else:
        raise ValueError(f'Could not determine projection for {lat},{lon}')


def epsg_from_polygon(polys):
    """
    Determine EPSG code for each polygon in polys.

    EPSG is computed for a regular list of points. EPSG is assigned based on
    majority criteria.

    Parameters
    -----------
    polys: list of shapely.Geometry.Polygon
        List of shapely Polygons

    Returns
    -------
    epsgs: list of int
        List of EPSG codes corresponding to elements in polys

    """
    epsgs = []

    # Make a regular grid based on polys min/max latitude longitude
    for p in polys:
        x_min, y_min, x_max, y_max = p.bounds
        xx, yy = np.meshgrid(np.linspace(x_min, x_max, 250),
                             np.linspace(y_min, y_max, 250))
        x = xx.flatten()
        y = yy.flatten()

        # Query to determine the zone
        zones = []
        for lx, ly in zip(x, y):
            # Create a point with grid coordinates
            pp = Point(lx, ly)

            # If Point is in polys, compute EPSG
            if pp.within(p):
                zones.append(point2epsg(lx, ly))

        # Count different EPSGs
        vals, counts = np.unique(zones, return_counts=True)

        # Get the EPSG for Polys
        epsgs.append(vals[np.argmax(counts)])

    return epsgs


def transform_polygon_coords_to_epsg(polys, epsgs):
    """
    Transform coordinates of polys (list of polygons) to target epsgs (list of
    EPSG codes).

    Parameters
    ----------
    polys: list of shapely.Geometry.Polygon
        List of shapely polygons
    epsgs: list of str
        List of EPSG codes corresponding to elements in polys

    Returns
    -------
    poly : list of shapely.Geometry.Polygon
         A list containing a single polygon which spans the extent of all
         transformed polygons.

    """
    # Assert validity of inputs
    assert(len(polys) == len(epsgs))

    # Transform each point of the perimeter in target EPSG coordinates
    llh = osr.SpatialReference()
    llh.ImportFromEPSG(4326)
    tgt = osr.SpatialReference()

    x_min, y_min, x_max, y_max = [], [], [], []
    tgt_x, tgt_y = [], []

    for poly, epsg in zip(polys, epsgs):
        x, y = poly.exterior.coords.xy
        tgt.ImportFromEPSG(int(epsg))
        trans = osr.CoordinateTransformation(llh, tgt)

        for lx, ly in zip(x, y):
            dummy_x, dummy_y, dummy_z = trans.TransformPoint(ly, lx, 0)
            tgt_x.append(dummy_x)
            tgt_y.append(dummy_y)

        x_min.append(min(tgt_x))
        y_min.append(min(tgt_y))
        x_max.append(max(tgt_x))
        y_max.append(max(tgt_y))

    # return a polygon
    poly = [Polygon([(min(x_min), min(y_min)), (min(x_min), max(y_max)),
                     (max(x_max), max(y_max)), (max(x_max), min(y_min))])]

    return poly
