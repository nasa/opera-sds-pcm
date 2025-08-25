
import ast
import os
import re
from collections import defaultdict
from functools import cache
from pathlib import Path

import boto3
import geopandas as gpd
from geopandas import GeoDataFrame
from mypy_boto3_s3 import S3Client
from pyproj import Transformer

from opera_commons.logger import get_logger
from util.conf_util import SettingsConf


def tree():
    """
    Simple implementation of a tree data structure in python. Essentially a defaultdict of default dicts.

    Usage: foo = tree() ; foo["a"]["b"]["c"]... = bar
    """
    return defaultdict(tree)


def dicts(t):
    """Utility function for casting a tree to a complex dict"""
    return {k: dicts(t[k]) for k in t}


@cache
def cached_load_mgrs_burst_db(filter_land=True):
    """see :func:`~data_subscriber.rtc.mgrs_bursts_collection_db_client.load_mgrs_burst_db`"""
    logger = get_logger()
    logger.info(f"Cache loading MGRS burst database.")
    logger.debug(f"{filter_land=}")
    return load_mgrs_burst_db(filter_land)


def load_mgrs_burst_db(filter_land=True):
    """see :func:`~data_subscriber.rtc.mgrs_bursts_collection_db_client.load_mgrs_burst_db_raw`"""
    logger = get_logger()
    logger.info(f"Initial load of MGRS burst database from disk.")

    vector_gdf = load_mgrs_burst_db_raw(filter_land)

    # parse collection columns encoded as string to collections
    vector_gdf["bursts_parsed"] = vector_gdf["bursts"].apply(lambda it: set(ast.literal_eval(it))).values  # downcast to safeguard against index order issues
    vector_gdf["mgrs_tiles_parsed"] = vector_gdf["mgrs_tiles"].apply(lambda it: set(ast.literal_eval(it))).values  # downcast to safeguard against index order issues
    # some burst sets are composed of bursts from different orbits
    vector_gdf["orbits"] = vector_gdf["bursts_parsed"].apply(lambda bursts: {int(b[1:4]) for b in bursts}).values

    return vector_gdf


def load_mgrs_burst_db_raw(filter_land=True) -> GeoDataFrame:
    """Loads the MGRS Tile Collection Database. On AWS environments, this will localize from a known S3 location."""
    logger = get_logger()
    mtc_local_filepath = Path(os.environ.get("MGRS_TILE_COLLECTION_DB_FILEPATH", "~/Downloads/MGRS_tile_collection_v0.3.sqlite")).expanduser()

    if mtc_local_filepath.exists():
        vector_gdf = gpd.read_file(mtc_local_filepath, crs="EPSG:4326")  # , bbox=(-230, 0, -10, 90))  # bbox=(-180, -90, 180, 90)  # global
    else:
        settings = SettingsConf().cfg
        mgrs_tile_collection_db_s3path = settings["MGRS_TILE_COLLECTION_DB_S3PATH"]
        match_s3path = re.match("s3://(?P<bucket_name>[^/]+)/(?P<object_key>.+)", mgrs_tile_collection_db_s3path)

        s3_client: S3Client = boto3.session.Session().client("s3")
        mtc_download_filepath = Path(Path(mgrs_tile_collection_db_s3path).name)
        s3_client.download_file(Bucket=match_s3path.group("bucket_name"), Key=match_s3path.group("object_key"), Filename=str(mtc_download_filepath))
        vector_gdf = gpd.read_file(mtc_download_filepath, crs="EPSG:4326")  # , bbox=(-230, 0, -10, 90))  # bbox=(-180, -90, 180, 90)  # global

    # na_gdf = gpd.read_file(Path("geo/north_america_opera.geojson"), crs="EPSG:4326")
    # vector_gdf = vector_gdf.overlay(na_gdf, how="intersection")
    logger.debug(f"pre water/land filter: {len(vector_gdf)=}")

    if filter_land:
        vector_gdf = vector_gdf[vector_gdf["land_ocean_flag"].isin(["water/land", "land"])]  # filter out water (water == no relevant data)
        logger.debug(f"post water/land filter: {len(vector_gdf)=}")

    return vector_gdf


def get_bounding_box_for_mgrs_set_id(mgrs_burst_collections_gdf: GeoDataFrame, mgrs_set_id):
    """
    Extracts the bounding box for the provided MGRS tile set ID from within the
    MGRS burst collection database.

    Parameters
    ----------
    mgrs_burst_collections_gdf : GeoDataFrame
        The MGRS burst collection database as read into memory.
    mgrs_set_id : str
        Identifier of the MGRS tile set to extract the bounding box for.

    returns
    -------
    bounding_box : list
        List containing the bounding box in West South East North (WSEN) order.

    """
    gdf = mgrs_burst_collections_gdf
    if not len(gdf[gdf["mgrs_set_id"] == mgrs_set_id]):
        raise Exception(f"No MGRS burst database entry for {mgrs_set_id}")

    proj_32645 = 'EPSG:{}'.format(gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0].EPSG)  # int(32645)
    proj_4326 = gdf.crs  # "EPSG:4326"
    transformer = Transformer.from_crs(proj_32645, proj_4326)

    # Extract the bounding box coordinates in EPSG4326.
    # Note that the coordinates seem to be stored in reverse lat/lon order, with
    # xmin/max corresponding to longitude and ymin/max corresponding to latitude
    ymin, xmin = transformer.transform(
        xx=gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0].xmin,
        yy=gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0].ymin
    )
    ymax, xmax = transformer.transform(
        xx=gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0].xmax,
        yy=gdf[gdf["mgrs_set_id"] == mgrs_set_id].iloc[0].ymax
    )

    # Return the bounding box in the expected WSEN order
    return [xmin, ymin, xmax, ymax]


def get_reduced_rtc_native_id_patterns(mgrs_burst_collections_gdf: GeoDataFrame):
    """Extracts all unique CMR native-id patterns that cover all the MGRS sets in the given GeoDataFrame"""
    rtc_native_id_patterns_burst_sets = get_rtc_native_id_patterns_burst_sets(mgrs_burst_collections_gdf)
    rtc_native_id_patterns = reduce_bursts_to_cmr_patterns(rtc_native_id_patterns_burst_sets)
    return rtc_native_id_patterns


def get_rtc_native_id_patterns_burst_sets(mgrs_burst_collections_gdf: GeoDataFrame):
    rtc_native_id_patterns_burst_sets = {
        "OPERA_L2_RTC-S1_{burst_id}".format(burst_id=mapping_burst_id_to_product_burst_id(burst_id))
        for _, row in mgrs_burst_collections_gdf.iterrows()
        for burst_id in row["bursts_parsed"]
    }
    return rtc_native_id_patterns_burst_sets


def reduce_bursts_to_cmr_patterns(rtc_native_id_patterns_burst_sets):
    native_id_pattern_tree = tree()
    for pattern in rtc_native_id_patterns_burst_sets:
        native_id_pattern_tree[pattern[:-7]][pattern[:-6]][pattern[:-5]][pattern[:-4]][pattern]
    native_id_pattern_tree = dicts(native_id_pattern_tree)
    rtc_native_id_patterns = set()
    for k1, v1 in native_id_pattern_tree.items():
        if len(v1.keys()) == 10:
            rtc_native_id_patterns.add(k1)
        else:
            for k2, v2 in v1.items():
                if len(v2.keys()) == 10:
                    rtc_native_id_patterns.add(k2)
                else:
                    for k3, v3 in v2.items():
                        if len(v3.keys()) == 10:
                            rtc_native_id_patterns.add(k3)
                        else:
                            for k4, v4 in v3.items():
                                if len(v4.keys()) == 3:  # got to the list of full native-ids
                                    rtc_native_id_patterns.add(k4)
                                else:
                                    rtc_native_id_patterns.update(set(v4.keys()))  # all the individual beams (1/3 or 2/3)
    rtc_native_id_patterns = {p + "*" for p in rtc_native_id_patterns}

    return rtc_native_id_patterns


def burst_id_to_mgrs_set_ids(gdf: GeoDataFrame, burst_id):
    mgrs_set_ids = gdf[{burst_id} < gdf["bursts_parsed"]]["mgrs_set_id"].unique().tolist()
    mgrs_set_ids.sort(key=natural_keys)
    return mgrs_set_ids


def burst_id_to_relative_orbit_numbers(gdf: GeoDataFrame, burst_id):
    relative_orbit_numbers = gdf[{burst_id} < gdf["bursts_parsed"]]["relative_orbit_number"].unique().tolist()
    relative_orbit_numbers.sort()
    return relative_orbit_numbers


def product_burst_id_to_mapping_burst_id(product_burst_id):
    return product_burst_id.lower().replace("-", "_")


def mapping_burst_id_to_product_burst_id(mapping_burst_id):
    return mapping_burst_id.upper().replace("_", "-")

# solution for natural sorting taken from here:
#  https://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside


def natural_keys(text):
    return [tryfloat(c) for c in re.split(r'[+-]?(\d+(?:[.]\d*)?|[.]\d+)', text)]


def tryfloat(text):
    try:
        retval = float(text)
    except ValueError:
        retval = text
    return retval
