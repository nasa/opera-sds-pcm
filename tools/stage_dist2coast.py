#!/usr/bin/env python3

# dist2coast mask staging

import argparse
import os
import backoff

import boto3

from osgeo import gdal
from shapely.geometry import box

from commons.logger import logger
from commons.logger import LogLevels
from util.geo_util import (check_dateline,
                           polygon_from_mgrs_tile)

# Enable exceptions
gdal.UseExceptions()

S3_DIST2COAST_BUCKET = "opera-dist2coast"
"""Name of the default S3 bucket containing the global coastline mask to crop from"""

DEFAULT_DIST2COAST_FILE = "dist_to_GSHHG_v2.3.7_1m_int16.tif"
"""Name of the default dist2coast mask to use"""


def get_parser():
    """Returns the command line parser for stage_dist2coast.py"""
    parser = argparse.ArgumentParser(
        description="Stage and verify coastline mask for processing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-o', '--output', type=str, action='store',
                        default='dist2coast.vrt', dest='outfile',
                        help='Output filepath (VRT format).')
    parser.add_argument('-s', '--s3-bucket', type=str, action='store',
                        default=S3_DIST2COAST_BUCKET, dest='s3_bucket',
                        help='Name of the S3 bucket containing the global '
                             'coastline map to extract from.')
    parser.add_argument('-f', '--file', type=str, action='store',
                        default=DEFAULT_DIST2COAST_FILE, dest='dist2coast_file',
                        help='Name of the dist2coast map file to look up in within '
                             'the designated S3 bucket.')
    parser.add_argument('-t', '--tile-code', type=str, default=None,
                        help='MGRS tile code identifier for the coastline region')
    parser.add_argument('-b', '--bbox', type=float, action='store',
                        dest='bbox', default=None, nargs='+',
                        help='Spatial bounding box of the coastline region in '
                             'latitude/longitude (WSEN, decimal degrees)')
    parser.add_argument('-m', '--margin', type=int, action='store',
                        default=5, help='Margin for bounding box in km.')
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")

    return parser


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
    if bbox:
        logger.info('Determining polygon from bounding box')
        poly = box(bbox[0], bbox[1], bbox[2], bbox[3])
    else:
        logger.info(f'Determining polygon from MGRS tile code {tile_code}')
        poly = polygon_from_mgrs_tile(tile_code)

    logger.debug(f'Derived polygon {str(poly)}')

    return poly


@backoff.on_exception(backoff.expo, Exception, max_tries=8, max_value=32)
def translate_mask(tif_filename, output_path, x_min, x_max, y_min, y_max):
    """
    Translate a coastline mask from S3 to a region matching the provided boundaries.

    Notes
    -----
    This function is decorated to perform retries using exponential backoff to
    make the remote call resilient to transient issues stemming from network
    access, authorization and AWS throttling (see "Query throttling" section at
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html).

    Parameters
    ----------
    tif_filename: str
        Path to the input tif file
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
    logger.info(f"Translating dist2coast for projection window {str([x_min, y_max, x_max, y_min])} "
                f"to {output_path}")
    ds = gdal.Open(tif_filename, gdal.GA_ReadOnly)
    gdal.Translate(
        output_path, ds, format='GTiff', projWin=[x_min, y_max, x_max, y_min]
    )


def download_dist2coast(polys, dist2coast_bucket, dist2coast_file, margin, outfile):
    """
    Download a coastline mask from the specified S3 bucket.

    Parameters:
    ----------
    polys: list of shapely.geometry.Polygon
        List of shapely polygons.
    dist2coast_bucket : str
        Name of the S3 bucket containing the global coastline map to download from.
    dist2coast_file : str
        Name of the global dist2coast mask within the S3 bucket.
    margin: float
        Buffer margin (in km) applied for download.
    outfile:
        Path to the where the output file is to be staged.

    """
    # convert margin to degree (approx formula)
    margin = margin / 40000 * 360

    # Download coastline mask for each polygon
    file_prefix = os.path.splitext(outfile)[0]
    dist2coast_list = []

    for idx, poly in enumerate(polys):
        dist2coast_path = f'/vsis3/{dist2coast_bucket}/{dist2coast_file}'
        poly = poly.buffer(margin)
        output_path = f'{file_prefix}_{idx}.tif'
        dist2coast_list.append(output_path)
        x_min, y_min, x_max, y_max = poly.bounds
        translate_mask(dist2coast_path, output_path, x_min, x_max, y_min, y_max)

    # Build vrt with downloaded masks
    gdal.BuildVRT(outfile, dist2coast_list)


def check_aws_connection(dist2coast_bucket, dist2coast_file):
    """
    Check connection to the provided S3 bucket.

    Parameters
    ----------
    dist2coast_bucket : str
        Name of the bucket to use with the connection test.
    dist2coast_file : str
        Name of the global dist2coast mask within the S3 bucket.

    Raises
    ------
    RuntimeError
       If no connection can be established.

    """
    s3 = boto3.resource('s3')
    obj = s3.Object(dist2coast_bucket, dist2coast_file)

    try:
        logger.info(f'Attempting test read of s3://{obj.bucket_name}/{obj.key}')
        obj.get()['Body'].read()
        logger.info('Connection test successful.')
    except Exception:
        errmsg = (f'No access to the {dist2coast_bucket} s3 bucket. '
                  f'Check your AWS credentials and re-run the code.')
        raise RuntimeError(errmsg)


def main(opts):
    """
    Main script to execute coastline mask staging.

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
                  "Cannot download coastline mask.")
        raise ValueError(errmsg)

    # Check if we were provided an explicit "None" for the s3_bucket,
    # which can occur when arguments are set up by a chimera precondition function
    if not opts.s3_bucket:
        opts.s3_bucket = S3_DIST2COAST_BUCKET

    # Determine polygon based on MGRS info or bbox
    poly = determine_polygon(opts.tile_code, opts.bbox)

    # Check dateline crossing. Returns list of polygons
    polys = check_dateline(poly)

    # Check connection to the S3 bucket
    logger.info(f'Checking connection to AWS S3 {opts.s3_bucket} bucket.')

    check_aws_connection(opts.s3_bucket, opts.dist2coast_file)

    # Download coastline mask
    download_dist2coast(polys, opts.s3_bucket, opts.dist2coast_file, opts.margin, opts.outfile)

    logger.info(f'Done, coastline mask stored locally to {opts.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
