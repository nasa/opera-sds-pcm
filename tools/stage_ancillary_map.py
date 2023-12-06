#!/usr/bin/env python3

# Staging script for ancillary map inputs, such as HAND, Worldcover, etc...

import argparse
import os

import backoff

from osgeo import gdal

from commons.logger import logger
from commons.logger import LogLevels
from util.geo_util import (check_dateline,
                           polygon_from_bounding_box)
from util.pge_util import check_aws_connection

# Enable exceptions
gdal.UseExceptions()

def get_parser():
    """Returns the command line parser for stage_ancillary_map.py"""
    parser = argparse.ArgumentParser(
        description="Stage a sub-region of a global map, stored in S3, based on "
                    "a provided bounding box region. This script leverages the "
                    "VRT virtual table feature of GDAL to sub-select a region of a"
                    "global map from an arbitrary projection window (bbox).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-o', '--output-file', type=str, action='store',
                        required=True, dest='outfile',
                        help='Path to the output VRT file which composes '
                             'each of the staged map sub-regions. Must be a file'
                             'ending with the .vrt extension.')
    parser.add_argument('-s', '--s3-bucket', type=str, action='store',
                        required=True, dest='s3_bucket',
                        help='Name of the S3 bucket containing the global '
                             'map to extract from.')
    parser.add_argument('-k', '--s3-key', type=str, action='store',
                        required=True, dest="s3_key",
                        help='S3 key path to the GDAL VRT file for the global map.')
    parser.add_argument('-b', '--bbox', type=float, action='store',
                        required=True, dest='bbox', nargs=4, metavar='VAL',
                        help='Spatial bounding box of the map sub-region to stage '
                             'in latitude/longitude (WSEN, decimal degrees)')
    parser.add_argument('-m', '--margin', type=int, action='store',
                        default=5, help='Margin, in km, to apply to the sub-region '
                                        'denoted by the provided bounding box.')
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")

    return parser


@backoff.on_exception(backoff.expo, Exception, max_time=600, max_value=32)
def download_map(polys, map_bucket, map_vrt_key, outfile):
    """
    Download a map subregion corresponding to the provided polygon(s)
    from the designated S3 location.

    Parameters
    ----------
    polys : list of shapely.geometry.Polygon
        List of polygons comprising the sub-regions of the global map to download.
    map_bucket : str
        Name of the S3 bucket containing the global map.
    map_vrt_key : str
        S3 key path to the location of the global map VRT within the
        bucket.
    outfile : str
        Path to where the output map VRT (and corresponding tifs) will be staged.

    """
    # Download the map for each provided Polygon
    file_prefix = os.path.splitext(outfile)[0]
    region_list = []

    for idx, poly in enumerate(polys):
        vrt_filename = f'/vsis3/{map_bucket}/{map_vrt_key}'
        output_path = f'{file_prefix}_{idx}.tif'
        region_list.append(output_path)

        x_min, y_min, x_max, y_max = poly.bounds

        logger.info(
            f"Translating map for projection window "
            f"{str([x_min, y_max, x_max, y_min])} to {output_path}"
        )

        ds = gdal.Open(vrt_filename, gdal.GA_ReadOnly)

        gdal.Translate(
            output_path, ds, format='GTiff', projWin=[x_min, y_max, x_max, y_min]
        )

    # Build VRT with downloaded sub-regions
    gdal.BuildVRT(outfile, region_list)


def main(args):
    """
    Main script to execute ancillary map staging.

    Parameters
    ----------
    args : argparse.Namespace
        Arguments parsed from the command-line.

    """
    # Set the logging level
    if args.log_level:
        LogLevels.set_level(args.log_level)

    # Make sure that output file has VRT extension
    if not args.outfile.lower().endswith('.vrt'):
        err_msg = "Output filename extension is not .vrt"
        raise ValueError(err_msg)

    # Derive the region polygon from the provided bounding box
    logger.info('Determining polygon from bounding box')
    poly = polygon_from_bounding_box(args.bbox, args.margin)

    # Check dateline crossing
    polys = check_dateline(poly)

    # Check connection to the S3 bucket
    logger.info(f'Checking connection to AWS S3 {args.s3_bucket} bucket.')
    check_aws_connection(bucket=args.s3_bucket, key=args.s3_key)

    # Download the map for each polygon region and assemble them into a
    # single output VRT file
    download_map(polys, args.s3_bucket, args.s3_key, args.outfile)

    logger.info(f'Done, ancillary map stored locally to {args.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
