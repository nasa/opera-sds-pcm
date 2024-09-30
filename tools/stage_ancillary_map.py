#!/usr/bin/env python3

# Staging script for ancillary map inputs, such as HAND, Worldcover, etc...

import argparse
import os

import backoff
import boto3
import numpy as np
import shapely.ops
import shapely.wkt
from osgeo import gdal
from shapely.geometry import LinearRing, Polygon, box

EARTH_APPROX_CIRCUMFERENCE = 40075017.
EARTH_RADIUS = EARTH_APPROX_CIRCUMFERENCE / (2 * np.pi)

# Enable exceptions
gdal.UseExceptions()


def check_aws_connection(bucket, key):
    """
    Check connection to the provided S3 bucket by performing a test read
    on the provided bucket/key location.

    Parameters
    ----------
    bucket : str
        Name of the S3 bucket to use with the connection test.
    key : str, optional
        S3 key path to append to the bucket name.

    Raises
    ------
    RuntimeError
        If not connection can be established.

    """
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)

    try:
        print(f'Attempting test read of s3://{obj.bucket_name}/{obj.key}')
        obj.get()['Body'].read()
        print('Connection test successful.')
    except Exception:
        errmsg = (f'No access to the {bucket} S3 bucket. '
                  f'Check your AWS credentials and re-run the code.')
        raise RuntimeError(errmsg)


def margin_km_to_deg(margin_in_km):
    """Converts a margin value from kilometers to degrees"""
    km_to_deg_at_equator = 1000. / (EARTH_APPROX_CIRCUMFERENCE / 360.)
    margin_in_deg = margin_in_km * km_to_deg_at_equator

    return margin_in_deg


def margin_km_to_longitude_deg(margin_in_km, lat=0):
    """Converts a margin value from kilometers to degrees as a function of latitude"""
    delta_lon = (180 * 1000 * margin_in_km /
                 (np.pi * EARTH_RADIUS * np.cos(np.pi * lat / 180)))

    return delta_lon


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

        for polygon_count in range(len(polys)):
            x, y = polys[polygon_count].exterior.coords.xy
            # if there are no longitude values above 180, continue
            if not any([k > 180 for k in x]):
                continue

            # otherwise, wrap longitude values down by 360 degrees
            x_wrapped_minus_360 = np.asarray(x) - 360
            polys[polygon_count] = Polygon(zip(x_wrapped_minus_360, y))

    else:
        # If dateline is not crossed, treat input poly as list
        polys = [poly]

    return polys


def polygon_from_bounding_box(bounding_box, margin_in_km):
    """
    Create a polygon (EPSG:4326) from the lat/lon coordinates corresponding to
    a provided bounding box.

    Parameters
    -----------
    bounding_box : list
        Bounding box with lat/lon coordinates (decimal degrees) in the form of
        [West, South, East, North].
    margin_in_km : float
        Margin in kilometers to be added to the resultant polygon.

    Returns
    -------
    poly: shapely.Geometry.Polygon
        Bounding polygon corresponding to the provided bounding box with
        margin applied.

    """
    lon_min = bounding_box[0]
    lat_min = bounding_box[1]
    lon_max = bounding_box[2]
    lat_max = bounding_box[3]

    # note we can also use the center lat here
    lat_worst_case = max([lat_min, lat_max])

    # convert margin to degree
    lat_margin = margin_km_to_deg(margin_in_km)
    lon_margin = margin_km_to_longitude_deg(margin_in_km, lat=lat_worst_case)

    # Check if the bbox crosses the antimeridian and apply the margin accordingly
    # so that any resultant DEM is split properly by check_dateline
    if lon_max - lon_min > 180:
        lon_min, lon_max = lon_max, lon_min

    poly = box(lon_min - lon_margin, max([lat_min - lat_margin, -90]),
               lon_max + lon_margin, min([lat_max + lat_margin, 90]))

    return poly


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

        print(
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
    # Make sure that output file has VRT extension
    if not args.outfile.lower().endswith('.vrt'):
        err_msg = "Output filename extension is not .vrt"
        raise ValueError(err_msg)

    # Derive the region polygon from the provided bounding box
    print('Determining polygon from bounding box')
    poly = polygon_from_bounding_box(args.bbox, args.margin)

    # Check dateline crossing
    polys = check_dateline(poly)

    # Check connection to the S3 bucket
    print(f'Checking connection to AWS S3 {args.s3_bucket} bucket.')
    check_aws_connection(bucket=args.s3_bucket, key=args.s3_key)

    # Download the map for each polygon region and assemble them into a
    # single output VRT file
    download_map(polys, args.s3_bucket, args.s3_key, args.outfile)

    print(f'Done, ancillary map stored locally to {args.outfile}')


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
