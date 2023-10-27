import ast
import logging
from functools import cache
from pathlib import Path

import geopandas as gpd
from geopandas import GeoDataFrame

logger = logging.getLogger(__name__)


@cache
def cached_load_mgrs_burst_db(filter_land=True):
    return load_mgrs_burst_db(filter_land)


def load_mgrs_burst_db(filter_land=True):
    vector_gdf = load_mgrs_burst_db_raw(filter_land)

    # parse collection columns encoded as string to collections
    vector_gdf["bursts_parsed"] = vector_gdf["bursts"].apply(lambda it: set(ast.literal_eval(it))).values  # downcast to safeguard against index order issues
    vector_gdf["mgrs_tiles_parsed"] = vector_gdf["mgrs_tiles"].apply(lambda it: set(ast.literal_eval(it))).values  # downcast to safeguard against index order issues
    # some burst sets are composed of bursts from different orbits
    vector_gdf["orbits"] = vector_gdf["bursts_parsed"].apply(lambda bursts: {int(b[1:4]) for b in bursts}).values

    return vector_gdf


def load_mgrs_burst_db_raw(filter_land=True):
    # TODO chrisjrd: finalize location before final commit (docker image? s3?)
    vector_gdf = gpd.read_file(Path("~/Downloads/MGRS_tile_collection_v0.2.sqlite").expanduser(), crs="EPSG:4326")  # , bbox=(-230, 0, -10, 90))  # bbox=(-180, -90, 180, 90)  # global
    # na_gdf = gpd.read_file(Path("geo/north_america_opera.geojson"), crs="EPSG:4326")
    # vector_gdf = vector_gdf.overlay(na_gdf, how="intersection")
    logger.info(f"{len(vector_gdf)=}")
    if filter_land:
        vector_gdf = vector_gdf[vector_gdf["land_ocean_flag"] == "water/land"]  # filter out water (water == no relevant data)
        logger.info(f"{len(vector_gdf)=}")

    return vector_gdf


def burst_id_to_mgrs_set_ids(gdf: GeoDataFrame, burst_id):
    return gdf[{burst_id} < gdf["bursts_parsed"]]["mgrs_set_id"].unique().tolist()


def product_burst_id_to_mapping_burst_id(product_burst_id):
    return product_burst_id.lower().replace("-", "_")
