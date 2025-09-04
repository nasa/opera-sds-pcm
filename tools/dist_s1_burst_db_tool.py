#!/usr/bin/env python3
from collections import defaultdict
import os
import logging
import pickle
import argparse
import csv
from tqdm import tqdm
import geopandas as gpd
import requests
from datetime import datetime, timezone, timedelta
from data_subscriber.url import determine_acquisition_cycle
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import parse_local_burst_db_pickle, localize_dist_burst_db, trigger_from_cmr_survey_csv

burst_geometry_file_url = "https://github.com/opera-adt/burst_db/releases/download/v0.9.0/burst-id-geometries-simple-0.9.0.geojson.zip"
burst_geometry_file = "burst-id-geometries-simple-0.9.0.geojson.zip"

''' Tool to query the DIST S1 burst database 
    The burst database file must be in the same directory as this script'''

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", dest="verbose", help="If true, print out verbose information.", required=False, default=False)
parser.add_argument("--db-file", dest="db_file", help="Specify the DIST-S1 burst database parquet file \
on the local file system instead of using the standard one in S3 ancillary", required=False)
parser.add_argument("--no-geometry", dest="no_geometry", action="store_true",
                    help="Do not print burst geometry information. This speeds up this tool significantly.", required=False, default=False)
subparsers = parser.add_subparsers(dest="subparser_name", required=True)

server_parser = subparsers.add_parser("list", help="List all tile numbers")

server_parser = subparsers.add_parser("summary", help="List all tile numbers, number of products and their bursts")

server_parser = subparsers.add_parser("native_id", help="Print information based on native_id")
server_parser.add_argument("native_id", help="The RTC native id from CMR")

server_parser = subparsers.add_parser("tile_id", help="Print information based on tile")
server_parser.add_argument("tile_id", help="The tile ID")
server_parser.add_argument("--first-product-datetime", help="Use the first product datetime to generate datetime for rest of the products in this tile", required=False, default=None)

server_parser = subparsers.add_parser("burst_id", help="Print information based on burst id.")
server_parser.add_argument("burst_id", help="Burst id looks like T175-374393-IW1.")

server_parser = subparsers.add_parser("trigger_granules", help="Run the list of granules through the triggering logic. Listed by increasing latest acquisition time.")
server_parser.add_argument("cmr_survey_csv", help="The cmr survey csv file")
server_parser.add_argument("--complete-tiles-only", help="Only trigger complete tiles", required=False, default=False)
server_parser.add_argument("--tile-to-trigger", help="Only trigger a specific tile. This will print out all the RTC granules used in triggering.", required=False, default=None)

args = parser.parse_args()

if args.db_file:
    # First see if a pickle file exists
    pickle_file_name = args.db_file + ".pickle"
    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = parse_local_burst_db_pickle(args.db_file, pickle_file_name)
else:
    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()

if args.no_geometry is False:
    #Check to see if burst_geometry_file exists on the local filesystem
    if not os.path.exists(burst_geometry_file):
        print(f"Downloading burst geometry file from {burst_geometry_file_url}")
        response = requests.get(burst_geometry_file_url)
        response.raise_for_status()
        with open(burst_geometry_file, 'wb') as f:
            f.write(response.content)
    else:
        print(f"Using existing burst geometry file: {burst_geometry_file}")
    print(f"Reading burst geometry file: {burst_geometry_file}")
    burst_grid = gpd.read_file(burst_geometry_file)

def get_burst_geometry(burst_id):
    """Get the geometry of a burst given its ID."""
    burst_id_converted = burst_id.lower().replace('-', '_')
    burst_geom = burst_grid[burst_grid['burst_id_jpl'] == burst_id_converted]
    if burst_geom.empty:
        print(f"No geometry found for {burst_id}")
        return None
    return burst_geom.geometry.iloc[0].bounds

if args.subparser_name == "list":
    l = list(all_tile_ids)
    print("Tile IDs (%d): \n" % len(l), l)

elif args.subparser_name == "summary":

    # Print out the number of tiles, products, and unique bursts
    print("Number of tiles: ", len(all_tile_ids))
    print("Number of products: ", len(product_to_bursts.keys()))
    print("Number of unique bursts: ", len(bursts_to_products.keys()))

    # Find the tile with the most products and then print out all the products and their bursts
    tile_with_most_products = max(dist_products.items(), key=lambda x: len(x[1]))
    print("Tile with most products: ", tile_with_most_products[0], "with", len(tile_with_most_products[1]), "products")
    print("Tile ID, Number of Products, Product IDs, Bursts")
    tile_id = tile_with_most_products[0]
    for product_id in sorted(list(dist_products[tile_id])):
        burst_ids = sorted(list(product_to_bursts[product_id]))
        print(f"{product_id} ({len(burst_ids)} bursts): {burst_ids}")

elif args.subparser_name == "native_id":
    burst_id, acquisition_dts = parse_r2_product_file_name(args.native_id, "L2_RTC_S1")
    acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, args.native_id)
    products = bursts_to_products[burst_id]

    if len(products) == 0:
        print("No DIST-S1 products are associated with burst id: ", burst_id)
        exit(-1)

    print("Burst id: ", burst_id)
    print("Acquisition datetime: ", acquisition_dts)
    print("Acquisition index: ", acquisition_index)
    print("Product IDs: ", products)
    for product in products:
        print("--product-id-time: ", f"{product},{acquisition_dts}")
    if args.no_geometry is False:
        print("Burst geometry minx, miny, maxx, maxy: ", get_burst_geometry(burst_id))

elif args.subparser_name == "tile_id":
    tile_id = args.tile_id
    if tile_id not in dist_products.keys():
        print("Tile ID: ", tile_id, "does not exist")
        exit(-1)

    # datetime looks like this: 20250614T015042Z
    if args.first_product_datetime:
        first_product_datetime = datetime.strptime(args.first_product_datetime, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    else:
        first_product_datetime = None

    print("Tile ID: ", tile_id)
    print("Product IDs and burst ids: ")
    product_ids = sorted(list(dist_products[tile_id]))
    first_product_first_burst_id = sorted(list(product_to_bursts[product_ids[0]]))[0]
    first_burst_identification_number = int(first_product_first_burst_id.split("-")[1])
    for product_id in product_ids:
        burst_ids = sorted(list(product_to_bursts[product_id]))
        if first_product_datetime:
            current_burst_identification_number = int(burst_ids[0].split("-")[1])
            delta_seconds = 12 * 24 * 60 * 60 * (current_burst_identification_number - first_burst_identification_number) / 375887
            product_datetime = first_product_datetime + timedelta(seconds=delta_seconds)
            product_datetime_str = product_datetime.strftime("%Y%m%dT%H%M%SZ")
            print(f"{product_id} ({product_datetime_str}) ({len(burst_ids)} bursts): {burst_ids}")
        else:
            print(f"{product_id} ({len(burst_ids)} bursts): {burst_ids}")

elif args.subparser_name == "burst_id":
    burst_id = args.burst_id
    if burst_id not in bursts_to_products.keys():
        print("Burst id: ", burst_id, "is not associated with any products")
        exit(-1)

    print("Burst id: ", burst_id)
    product_ids = bursts_to_products[burst_id]
    print("Product IDs: ({len(product_ids))", product_ids)
    if args.no_geometry is False:
        print("Burst geometry minx, miny, maxx, maxy: ", get_burst_geometry(burst_id))

elif args.subparser_name == "trigger_granules":
    print("Triggering granules")

    products_triggered, granules_triggered, tiles_untriggered, unused_rtc_granule_count = \
        trigger_from_cmr_survey_csv(args.cmr_survey_csv, args.complete_tiles_only, 0, datetime.now(timezone.utc), product_to_bursts, bursts_to_products)
    
    if args.tile_to_trigger:
        products_triggered = {k: v for k, v in products_triggered.items() if k.startswith(args.tile_to_trigger)}

    # Sort products_triggered by their latest acquisition time
    products_triggered_sorted = sorted(products_triggered.items(), key=lambda x: x[1].latest_acquisition)
    for product_id, product in products_triggered_sorted:
        print(f"{product_id=} {product.latest_acquisition.strftime('%Y-%m-%d %H:%M:%S')} {product.used_bursts=} {product.possible_bursts=}")
        if args.tile_to_trigger:
            print(f"RTC granules: {product.rtc_granules}\n")
    
