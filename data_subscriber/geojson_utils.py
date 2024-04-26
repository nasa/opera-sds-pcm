import boto3
import logging
import os

from geo.geo_util import does_bbox_intersect_region
from util.conf_util import SettingsConf

def localize_include_exclude(args):

    geojsons = []
    if args.include_regions is not None:
        geojsons.extend(args.include_regions.split(","))

    if args.exclude_regions is not None:
        geojsons.extend(args.exclude_regions.split(","))

    localize_geojsons(geojsons)

def localize_geojsons(geojsons):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]

    # First try to get the geojsons from the official S3 location. If not found, try to use one in the current working directory
    for geojson in geojsons:
        key = geojson.strip() + ".geojson"

        try:
            download_from_s3(bucket, key, key)
        except Exception as e:
            logging.error("Exception while fetching geojson files: %s from S3 bucket %s. \
Will try to fetch the same file from current working directory - this should only be done in testing scenarios. " % (key, bucket) + str(e))

        # See if the file exists in the current working directory
        if not os.path.exists(key):
            raise Exception("Geojson file %s not found in S3 bucket %s or the current working directory" % (key, bucket))

def does_granule_intersect_regions(granule, intersect_regions):
    regions = intersect_regions.split(',')
    for region in regions:
        region = region.strip()
        if does_bbox_intersect_region(granule["bounding_box"], region):
            return True, region

    return False, None

def filter_granules_by_regions(granules, include_regions, exclude_regions):
    '''Filters granules based on include and exclude regions lists'''
    filtered = []

    for granule in granules:

        # Skip this granule if it's not in the include list
        if include_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, include_regions)
            if result is False:
                logging.info(
                    f"The following granule does not intersect with any include regions. Skipping processing %s"
                    % granule.get("granule_id"))
                continue

        # Skip this granule if it's in the exclude list
        if exclude_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, exclude_regions)
            if result is True:
                logging.info(f"The following granule intersects with the exclude region %s. Skipping processing %s"
                             % (region, granule.get("granule_id")))
                continue

        # If both filters don't apply, add this granule to the list
        filtered.append(granule)

    return filtered

def download_from_s3(bucket, file, path):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, file).download_file(path)
    except Exception as e:
        raise Exception("Exception while fetching file: %s from S3 bucket %s. " % (file, bucket) + str(e))
