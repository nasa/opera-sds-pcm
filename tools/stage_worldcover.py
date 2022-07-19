#!/usr/bin/env python3

# ESA Worldcover Map staging

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

S3_WORLDCOVER_BUCKET = "opera-world-cover"
"""Name of the default S3 bucket containing the full Worldcover map to crop from"""

WORLDCOVER_VERSION = "v100"
"""Version of the Worldcover map to obtain"""

WORLDCOVER_YEAR = "2020"
"""Year of the Worldcover map to obtain"""


def get_parser():
    """Returns the command line parser for stage_worldcover.py"""
    parser = argparse.ArgumentParser(
        description="Stage and verify Worldcover map for processing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-o', '--output', type=str, action='store',
                        default='worldcover.vrt', dest='outfile',
                        help='Output Worldcover filepath (VRT format).')
    parser.add_argument('-s', '--s3-bucket', type=str, action='store',
                        default=S3_WORLDCOVER_BUCKET, dest='s3_bucket',
                        help='Name of the S3 bucket containing the full Worldcover '
                             'map to extract from.')
    parser.add_argument('-v', '--worldcover-version', type=str, action='store',
                        default=WORLDCOVER_VERSION, dest='worldcover_ver',
                        help='Version number used to identify the specific Worldcover '
                             'map to extract from.')
    parser.add_argument('-y', '--worldcover-year', type=str, action='store',
                        default=WORLDCOVER_YEAR, dest='worldcover_year',
                        help='Year used to identify the specific Worldcover map '
                             'to extract from.')
    parser.add_argument('-t', '--tile-code', type=str, default=None,
                        help='MGRS tile code identifier for the Worldcover region')
    parser.add_argument('-b', '--bbox', type=float, action='store',
                        dest='bbox', default=None, nargs='+',
                        help='Spatial bounding box of the Worldcover region in '
                             'latitude/longitude (WSEN, decimal degrees)')
    parser.add_argument('-m', '--margin', type=int, action='store',
                        default=5, help='Margin for Worldcover bounding box in km.')
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
def translate_worldcover(vrt_filename, output_path, x_min, x_max, y_min, y_max):
    """
    Translate a Worldcover map from the esa-worldcover bucket.

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
    logger.info(f"Translating Worldcover for projection window {str([x_min, y_max, x_max, y_min])} "
                f"to {output_path}")
    ds = gdal.Open(vrt_filename, gdal.GA_ReadOnly)
    gdal.Translate(
        output_path, ds, format='GTiff', projWin=[x_min, y_max, x_max, y_min]
    )


def download_worldcover(polys, worldcover_bucket, worldcover_ver,
                        worldcover_year, margin, outfile):
    """
    Download a Worldcover map from the esa-worldcover bucket.

    Parameters:
    ----------
    polys: list of shapely.geometry.Polygon
        List of shapely polygons.
    worldcover_bucket : str
        Name of the S3 bucket containing the full Worldcover map to download from.
    worldcover_ver : str
        Version of the full Worldcover map to download from. Becomes part of the
        S3 key used to download.
    worldcover_year : str
        Year of the full Worldcover map to download from. Becomes part of the
        S3 key used to download.
    margin: float
        Buffer margin (in km) applied for Worldcover download.
    outfile:
        Path to the where the output Worldcover file is to be staged.

    """
    # convert margin to degree (approx formula)
    margin = margin / 40000 * 360

    # Download Worldcover map for each polygon/epsg
    file_prefix = os.path.splitext(outfile)[0]
    wc_list = []

    for idx, poly in enumerate(polys):
        vrt_filename = (
            f'/vsis3/{worldcover_bucket}/{worldcover_ver}/{worldcover_year}/'
            f'ESA_WorldCover_10m_{worldcover_year}_{worldcover_ver}_Map_AWS.vrt'
        )

        poly = poly.buffer(margin)
        output_path = f'{file_prefix}_{idx}.tif'
        wc_list.append(output_path)
        x_min, y_min, x_max, y_max = poly.bounds
        translate_worldcover(vrt_filename, output_path, x_min, x_max, y_min, y_max)

    # Build vrt with downloaded maps
    gdal.BuildVRT(outfile, wc_list)


def check_aws_connection(worldcover_bucket):
    """
    Check connection to the provided S3 bucket.

    Parameters
    ----------
    worldcover_bucket : str
        Name of the bucket to use with the connection test.

    Raises
    ------
    RuntimeError
       If no connection can be established.

    """
    s3 = boto3.resource('s3')
    obj = s3.Object(worldcover_bucket, 'readme.html')

    try:
        logger.info(f'Attempting test read of s3://{obj.bucket_name}/{obj.key}')
        obj.get()['Body'].read()
        logger.info('Connection test successful.')
    except Exception:
        errmsg = (f'No access to the {worldcover_bucket} s3 bucket. '
                  f'Check your AWS credentials and re-run the code.')
        raise RuntimeError(errmsg)


def main(opts):
    """
    Main script to execute Worldcover map staging.

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
                  "Cannot download Worldcover map.")
        raise ValueError(errmsg)

    # Make sure that output file has VRT extension
    if not opts.outfile.lower().endswith('.vrt'):
        err_msg = "Worldcover output filename extension is not .vrt"
        raise ValueError(err_msg)

    # Check if we were provided an explicit "None" for the bucket parameters,
    # which can occur when arguments are set up by a chimera precondition function
    if not opts.s3_bucket:
        opts.s3_bucket = S3_WORLDCOVER_BUCKET

    if not opts.worldcover_ver:
        opts.worldcover_ver = WORLDCOVER_VERSION

    if not opts.worldcover_year:
        opts.worldcover_year = WORLDCOVER_YEAR

    # Determine polygon based on MGRS info or bbox
    poly = determine_polygon(opts.tile_code, opts.bbox)

    # Check dateline crossing. Returns list of polygons
    polys = check_dateline(poly)

    # Check connection to the S3 bucket
    logger.info(f'Checking connection to AWS S3 {opts.s3_bucket} bucket.')

    check_aws_connection(opts.s3_bucket)

    # Download Worldcover map(s)
    download_worldcover(polys, opts.s3_bucket, opts.worldcover_ver,
                        opts.worldcover_year, opts.margin, opts.outfile)

    logger.info(f'Done, Worldcover map stored locally to {opts.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
