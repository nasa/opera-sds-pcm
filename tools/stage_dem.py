#!/usr/bin/env python3

# DEM staging

import argparse
import os

import backoff
import numpy as np
from osgeo import gdal, osr

from opera_commons.logger import LogLevels
from opera_commons.logger import logger
from util.geo_util import (check_dateline,
                           epsg_from_polygon,
                           polygon_from_bounding_box,
                           polygon_from_mgrs_tile)
from util.pge_util import check_aws_connection

# Enable exceptions
gdal.UseExceptions()

S3_DEM_BUCKET = "opera-dem"
"""Name of the default S3 bucket containing the global DEM to crop from"""


def get_parser():
    """Returns the command line parser for stage_dem.py"""
    parser = argparse.ArgumentParser(
        description="Stage and verify DEM for processing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-o', '--output', type=str, action='store',
                        default='dem.vrt', dest='outfile',
                        help='Output DEM filepath (VRT format).')
    parser.add_argument('-s', '--s3-bucket', type=str, action='store',
                        default=S3_DEM_BUCKET, dest='s3_bucket',
                        help='Name of the S3 bucket containing the global DEM '
                             'to extract from.')
    parser.add_argument('-k', '--s3-key', type=str, action='store',
                        default="", dest="s3_key",
                        help='S3 key path utilized with the bucket name to derive '
                             'the location of the DEM to extract from. If the '
                             'desired DEM is at the top-level of the provided '
                             'bucket, this argument is not needed.')
    parser.add_argument('-t', '--tile-code', type=str, default=None,
                        help='MGRS tile code identifier for the DEM region')
    parser.add_argument('-b', '--bbox', type=float, action='store',
                        dest='bbox', default=None, nargs='+',
                        help='Spatial bounding box of the DEM region in '
                             'latitude/longitude (WSEN, decimal degrees)')
    parser.add_argument('-m', '--margin', type=int, action='store',
                        default=5, help='Margin for DEM bounding box in km.')
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")

    return parser


def determine_polygon(tile_code, bbox=None, margin_in_km=50):
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
    margin_in_km: float, optional
        Margin in kilometers to be added to MGRS bounding box obtained from the
        MGRS `tile_code`. This margin is not added to the bounding box
        defined from the input parameter `bbox`.

    Returns
    -------
    poly: shapely.Geometry.Polygon
        Bounding polygon corresponding to the MGRS tile code or bbox shape on
        the ground.

    """
    if bbox:
        logger.info('Determining polygon from bounding box')
        poly = polygon_from_bounding_box(bbox, margin_in_km)
    else:
        logger.info(f'Determining polygon from MGRS tile code {tile_code}')
        poly = polygon_from_mgrs_tile(tile_code, margin_in_km)

    logger.debug(f'Derived polygon {str(poly)}')

    return poly


@backoff.on_exception(backoff.expo, Exception, max_time=600, max_value=32)
def translate_dem(vrt_filename, output_path, x_min, x_max, y_min, y_max):
    """
    Translate a DEM from S3 to a region matching the provided boundaries.

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

    # update cropping coordinates to not exceed the input DEM bounding box
    input_x_min, xres, _, input_y_max, _, yres = ds.GetGeoTransform()
    length = ds.GetRasterBand(1).YSize
    width = ds.GetRasterBand(1).XSize

    # declare lambda function to snap min/max X and Y coordinates over the
    # DEM grid
    snap_coord = \
        lambda val, snap, offset, round_func: round_func(
            float(val - offset) / snap) * snap + offset

    # Snap edge coordinates using the DEM pixel spacing
    # (xres and yres) and starting coordinates (input_x_min and
    # input_x_max). Maximum values are rounded using np.ceil
    # and minimum values are rounded using np.floor
    snapped_x_min = snap_coord(x_min, xres, input_x_min, np.floor)
    snapped_x_max = snap_coord(x_max, xres, input_x_min, np.ceil)
    snapped_y_min = snap_coord(y_min, yres, input_y_max, np.floor)
    snapped_y_max = snap_coord(y_max, yres, input_y_max, np.ceil)

    input_y_min = input_y_max + length * yres
    input_x_max = input_x_min + width * xres

    adjusted_x_min = max(snapped_x_min, input_x_min)
    adjusted_x_max = min(snapped_x_max, input_x_max)
    adjusted_y_min = max(snapped_y_min, input_y_min)
    adjusted_y_max = min(snapped_y_max, input_y_max)

    logger.info(f"Adjusted projection window {str([adjusted_x_min, adjusted_y_max, adjusted_x_max, adjusted_y_min])}")

    try:
        gdal.Translate(
            output_path, ds, format='GTiff',
            projWin=[adjusted_x_min, adjusted_y_max, adjusted_x_max, adjusted_y_min]
        )
    except RuntimeError as err:
        if "negative width and/or height" in str(err):
            logger.warning(
                f"Adjusted window translation failed due to negative width and/or "
                f"height, defaulting to original projection window"
            )
            gdal.Translate(
                output_path, ds, format='GTiff', projWin=[x_min, y_max, x_max, y_min]
            )
        else:
            raise

    # stage_dem.py takes a bbox as an input. The longitude coordinates
    # of this bbox are unwrapped i.e., range in [0, 360] deg. If the
    # bbox crosses the anti-meridian, the script divides it in two
    # bboxes neighboring the anti-meridian. Here, x_min and x_max
    # represent the min and max longitude coordinates of one of these
    # bboxes. We Add 360 deg if the min longitude of the downloaded DEM
    # tile is < 180 deg i.e., there is a dateline crossing.
    # This ensures that the mosaicked DEM VRT will span a min
    # range of longitudes rather than the full [-180, 180] deg
    sr = osr.SpatialReference(ds.GetProjection())
    epsg_str = sr.GetAttrValue("AUTHORITY", 1)

    if x_min <= -180.0 and epsg_str == '4326':
        ds = gdal.Open(output_path, gdal.GA_Update)
        geotransform = list(ds.GetGeoTransform())
        geotransform[0] += 360.0
        ds.SetGeoTransform(tuple(geotransform))


def download_dem(polys, epsgs, dem_location, outfile):
    """
    Download a DEM from the specified S3 bucket.

    Parameters:
    ----------
    polys: list of shapely.geometry.Polygon
        List of shapely polygons.
    epsgs: list of str
        List of EPSG codes corresponding to polys.
    dem_location : str
       S3 bucket and key containing the global DEM to download from.
    outfile:
        Path to the where the output DEM file is to be staged.

    """
    # set epsg to 4326 for each element in the list
    epsgs = [4326] * len(epsgs)

    # Download DEM for each polygon/epsg
    file_prefix = os.path.splitext(outfile)[0]
    dem_list = []

    for idx, (epsg, poly) in enumerate(zip(epsgs, polys)):
        vrt_filename = f'/vsis3/{dem_location}/EPSG{epsg}/EPSG{epsg}.vrt'
        output_path = f'{file_prefix}_{idx}.tif'
        dem_list.append(output_path)
        x_min, y_min, x_max, y_max = poly.bounds
        translate_dem(vrt_filename, output_path, x_min, x_max, y_min, y_max)

    # Build vrt with downloaded DEMs
    gdal.BuildVRT(outfile, dem_list)


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

    # Check if we were provided an explicit "None" for the s3_bucket,
    # which can occur when arguments are set up by a chimera precondition function
    if not opts.s3_bucket:
        opts.s3_bucket = S3_DEM_BUCKET

    # Determine polygon based on MGRS grid reference with a margin, or bbox
    poly = determine_polygon(opts.tile_code, opts.bbox, opts.margin)

    # Check dateline crossing. Returns list of polygons
    polys = check_dateline(poly)

    # Check connection to the S3 bucket
    logger.info(f'Checking connection to AWS S3 {opts.s3_bucket} bucket.')

    # Determine where to look for the sample vrt file to read as a connection test
    if opts.s3_key:
        test_key = '/'.join([opts.s3_key, 'EPSG4326/EPSG4326.vrt'])
    else:
        test_key = 'EPSG4326/EPSG4326.vrt'

    check_aws_connection(bucket=opts.s3_bucket, key=test_key)

    # Determine EPSG code
    logger.info("Determining EPSG code(s) for region polygon(s)")

    epsgs = epsg_from_polygon(polys)

    logger.debug(f'Derived the following EPSG codes: {epsgs}')

    # Download DEM
    dem_location = '/'.join([opts.s3_bucket, opts.s3_key]) if opts.s3_key else opts.s3_bucket
    download_dem(polys, epsgs, dem_location, opts.outfile)

    logger.info(f'Done, DEM stored locally to {opts.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
