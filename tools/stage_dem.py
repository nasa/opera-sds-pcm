#!/usr/bin/env python3

# DEM staging

import argparse
import os
import backoff

import boto3
import mgrs
import numpy as np
import pyproj
import shapely.ops
import shapely.wkt

from commons.logger import logger
from commons.logger import LogLevels

from osgeo import gdal, osr
from shapely.geometry import LinearRing, Point, Polygon, box

# Enable exceptions
gdal.UseExceptions()

S3_DEM_BUCKET = "opera-dem"
"""Name of the S3 bucket containing the full DEM's to crop from"""


def get_parser():
    """Returns the command line parser for stage_dem.py"""
    parser = argparse.ArgumentParser(
        description="Stage and verify DEM for processing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-o', '--output', type=str, action='store',
                        default='dem.vrt', dest='outfile',
                        help='Output DEM filepath (VRT format).')
    parser.add_argument('-f', '--filepath', type=str, action='store',
                        help='Filepath to user DEM. If provided, will be used '
                             'to determine overlap between provided DEM, and '
                             'DEM to be downloaded based on the MGRS tile code '
                             'or bounding box.')
    parser.add_argument('-m', '--margin', type=int, action='store',
                        default=5, help='Margin for DEM bounding box in km.')
    parser.add_argument('-b', '--bbox', type=float, action='store',
                        dest='bbox', default=None, nargs='+',
                        help='Spatial bounding box of the DEM region in '
                             'latitude/longitude (WSEN, decimal degrees)')
    parser.add_argument('-t', '--tile-code', type=str, default=None,
                        help='MGRS tile code identifier for the DEM region')
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")

    return parser


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
    if (x_max - x_min) > 180.0:
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
        assert (len(polys) == 2)
    else:
        # If dateline is not crossed, treat input poly as list
        polys = [poly]

    return polys


def determine_polygon(tile_code, bbox=None):
    """
    Determine bounding polygon using MGRS tile code or user-defined bounding box.

    Parameters
    ----------
    tile_code: str
        MGRS tile code corresponding to the polygon to derive.
    bbox: list, optional
        Bounding box with lat/lon coordinates (decimal degrees) in the form of
        [West, South, East, North]. If provided, takes precedence over the tile
        code.

    Returns
    -------
    poly: shapely.Geometry.Polygon
        Bounding polygon corresponding to the MGRS tile code or bbox shape on
        the ground.

    """
    if bbox is not None:
        logger.info('Determining polygon from bounding box')
        poly = box(bbox[0], bbox[1], bbox[2], bbox[3])
    else:
        logger.info(f'Determining polygon from MGRS tile code {tile_code}')
        poly = get_polygon_from_mgrs(tile_code)

    logger.debug(f'Derived polygon {str(poly)}')

    return poly


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


def get_polygon_from_mgrs(tile_code):
    """
    Create a polygon (EPSG:4326) from the lat/lon coordinates corresponding to
    a MGRS tile bounding box.

    Parameters
    -----------
    tile_code : str
        MGRS tile code corresponding to the polygon to derive.

    Returns
    -------
    poly: shapely.Geometry.Polygon
        Bounding polygon corresponding to the provided MGRS tile code.

    """
    mgrs_obj = mgrs.MGRS()

    geod = pyproj.Geod(ellps='WGS84')

    if tile_code.startswith('T'):
        tile_code = tile_code[1:]

    lat_min, lon_min = mgrs_obj.toLatLon(tile_code, inDegrees=True)
    x_var = geod.line_length([lon_min, lon_min], [lat_min, lat_min + 1])
    y_var = geod.line_length([lon_min, lon_min + 1], [lat_min, lat_min])

    mgrs_tile_edge_size = 109.8 * 1000

    lat_max = lat_min + (mgrs_tile_edge_size / x_var)
    lon_max = lon_min + (mgrs_tile_edge_size / y_var)

    coords = list(map(round, [lon_min, lat_min, lon_max, lat_max]))

    poly = box(*coords)

    return poly


def determine_projection(polys):
    """
    Determine EPSG code for each polygon in polys.

    EPSG is computed for a regular list of points. EPSG is assigned based on a
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
    logger.info("Determining EPSG code(s) for region polygon(s)")

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

    logger.debug(f'Derived the following EPSG codes: {epsgs}')
    return epsgs


@backoff.on_exception(backoff.expo, Exception, max_tries=8, max_value=32)
def translate_dem(vrt_filename, output_path, x_min, x_max, y_min, y_max):
    """
    Translate a DEM from the opera-dem bucket.

    Notes
    -----
    This function is decorated to perform retries using exponential backoff to
    make the remote call resilient to transient issues stemming from network
    access, authorization and AWS throttling (see "Query throttling" section at
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html).

    Parameters
    ----------
    vrt_filename: str
        Path to the input VRT file
    output_path: str
        Path to the translated output GTiff file
    x_min: float
        Minimum longitude bound of the sub-window
    x_max: float
        Maximum longitude bound of the sub-window
    y_min: float
        Minimum latitude bound of the sub-window
    y_max: float
        Maximum latitude bound of the sub-window

    """
    logger.info(f"Translating DEM for projection window {str([x_min, y_max, x_max, y_min])} "
                f"to {output_path}")
    ds = gdal.Open(vrt_filename, gdal.GA_ReadOnly)
    gdal.Translate(
        output_path, ds, format='GTiff', projWin=[x_min, y_max, x_max, y_min]
    )


def download_dem(polys, epsgs, margin, outfile):
    """
    Download a DEM from the opera-dem bucket.

    Parameters:
    ----------
    polys: list of shapely.geometry.Polygon
        List of shapely polygons.
    epsgs: list of str
        List of EPSG codes corresponding to polys.
    margin: float
        Buffer margin (in km) applied for DEM download.
    outfile:
        Path to the where the output DEM file is to be staged.

    """
    if 3031 in epsgs:
        epsgs = [3031] * len(epsgs)
        polys = transform_polygon_coords(polys, epsgs)

        # Need one EPSG as in polar stereo we have one big polygon
        epsgs = [3031]
        margin = margin * 1000
    elif 3413 in epsgs:
        epsgs = [3413] * len(epsgs)
        polys = transform_polygon_coords(polys, epsgs)

        # Need one EPSG as in polar stereo we have one big polygon
        epsgs = [3413]
        margin = margin * 1000
    else:
        # set epsg to 4326 for each element in the list
        epsgs = [4326] * len(epsgs)

        # convert margin to degree (approx formula)
        margin = margin / 40000 * 360

    # Download DEM for each polygon/epsg
    file_prefix = os.path.splitext(outfile)[0]
    dem_list = []

    for idx, (epsg, poly) in enumerate(zip(epsgs, polys)):
        vrt_filename = f'/vsis3/{S3_DEM_BUCKET}/EPSG{epsg}/EPSG{epsg}.vrt'
        poly = poly.buffer(margin)
        output_path = f'{file_prefix}_{idx}.tif'
        dem_list.append(output_path)
        x_min, y_min, x_max, y_max = poly.bounds
        translate_dem(vrt_filename, output_path, x_min, x_max, y_min, y_max)

    # Build vrt with downloaded DEMs
    gdal.BuildVRT(outfile, dem_list)


def transform_polygon_coords(polys, epsgs):
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


def check_dem_overlap(dem_filepath, polys):
    """
    Evaluate overlap between a user-provided DEM and DEM that stage_dem.py would
    download based on MGRS tile code or bbox provided information.

    Parameters
    ----------
    dem_filepath: str
        Filepath to the user-provided DEM.
    polys: list of shapely.geometry.Polygon
        List of polygons computed from MGRS code or bbox.

    Returns
    -------
    perc_area: float
        Area (in percentage) covered by the intersection between the
        user-provided DEM and the DEM downloadable by stage_dem.py

    """
    from isce3.io import Raster  # pylint: disable=import-error

    # Get local DEM edge coordinates
    DEM = Raster(dem_filepath)
    ulx, xres, xskew, uly, yskew, yres = DEM.get_geotransform()
    lrx = ulx + (DEM.width * xres)
    lry = uly + (DEM.length * yres)
    poly_dem = Polygon([(ulx, uly), (ulx, lry), (lrx, lry), (lrx, uly)])

    # Initialize epsg
    epsg = [DEM.get_epsg()] * len(polys)

    if DEM.get_epsg() != 4326:
        polys = transform_polygon_coords(polys, epsg)

    perc_area = 0
    for poly in polys:
        perc_area += (poly.intersection(poly_dem).area / poly.area) * 100

    return perc_area


def check_aws_connection():
    """
    Check connection to the AWS s3://opera-dem bucket.

    Raises
    ------
    RuntimeError
       If no connection can be established.

    """
    logger.info(f'Checking connection to AWS S3 {S3_DEM_BUCKET} bucket.')
    s3 = boto3.resource('s3')
    obj = s3.Object(S3_DEM_BUCKET, 'EPSG4326/EPSG4326.vrt')

    try:
        obj.get()['Body'].read()
        logger.info('Connection test successful.')
    except Exception:
        errmsg = (f'No access to the {S3_DEM_BUCKET} s3 bucket. '
                  f'Check your AWS credentials and re-run the code.')
        raise RuntimeError(errmsg)


def main(opts):
    """
    Main script to execute DEM staging.

    Parameters:
    ----------
    opts : argparse.Namespace
        Arguments parsed from the command-line.

    """
    # Set the logging level
    if opts.log_level:
        LogLevels.set_level(opts.log_level)

    # Check if MGRS tile code or bbox are provided
    if opts.tile_code is None and opts.bbox is None:
        errmsg = ("Need to provide reference MGRS tile code or bounding box. "
                  "Cannot download DEM.")
        raise ValueError(errmsg)

    # Make sure that output file has VRT extension
    if not opts.outfile.lower().endswith('.vrt'):
        err_msg = "DEM output filename extension is not .vrt"
        raise ValueError(err_msg)

    # Determine polygon based on MGRS info or bbox
    poly = determine_polygon(opts.tile_code, opts.bbox)

    # Check dateline crossing. Returns list of polygons
    polys = check_dateline(poly)

    if opts.filepath and os.path.isfile(opts.filepath):
        logger.info('Checking overlap with user-provided DEM')

        try:
            overlap = check_dem_overlap(opts.filepath, polys)

            logger.info(f'DEM coverage is {overlap} %')

            if overlap < 75.:
                logger.warning('WARNING: Insufficient DEM coverage (< 75%). Errors might occur')
        except ImportError:
            logger.warning('Unable to import from isce3 package, cannot determine '
                           'DEM overlap.')

    # Check connection to AWS s3 opera-dem bucket
    check_aws_connection()

    # Determine EPSG code
    epsgs = determine_projection(polys)

    # Download DEM
    download_dem(polys, epsgs, opts.margin, opts.outfile)

    logger.info(f'Done, DEM stored locally to {opts.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
