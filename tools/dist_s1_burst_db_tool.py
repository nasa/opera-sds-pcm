#!/usr/bin/env python3
from collections import defaultdict
import os
import logging
import pickle
from data_subscriber.dist_s1_utils import process_dist_burst_db, localize_dist_burst_db
import argparse
import csv
from tqdm import tqdm
from data_subscriber.url import determine_acquisition_cycle
from data_subscriber.cslc_utils import parse_r2_product_file_name

''' Tool to query the DIST S1 burst database 
    The burst database file must be in the same directory as this script'''

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", dest="verbose", help="If true, print out verbose information.", required=False, default=False)
parser.add_argument("--db-file", dest="db_file", help="Specify the DIST-S1 database json file \
on the local file system instead of using the standard one in S3 ancillary", required=False)
subparsers = parser.add_subparsers(dest="subparser_name", required=True)

server_parser = subparsers.add_parser("list", help="List all tile numbers")

server_parser = subparsers.add_parser("summary", help="List all tile numbers, number of products and their bursts")

server_parser = subparsers.add_parser("native_id", help="Print information based on native_id")
server_parser.add_argument("native_id", help="The RTC native id from CMR")

server_parser = subparsers.add_parser("tile_id", help="Print information based on tile")
server_parser.add_argument("tile_id", help="The tile ID")

server_parser = subparsers.add_parser("burst_id", help="Print information based on burst id.")
server_parser.add_argument("burst_id", help="Burst id looks like T175-374393-IW1.")

args = parser.parse_args()

if args.db_file:
    # First see if a pickle file exists
    pickle_file_name = args.db_file + ".pickle"
    try:
        with open(pickle_file_name, "rb") as f:
            dist_products, bursts_to_products, product_to_bursts, all_tile_ids = pickle.load(f)
            logger.info("Loaded DIST-S1 burst database from pickle file.")
    except FileNotFoundError:
        logger.info(f"Could not find {pickle_file_name}. Processing DIST-S1 burst database file.")
        logger.info(f"Using local DIST-S1 database parquet file: {args.db_file}")
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = process_dist_burst_db(args.db_file)
        # Check to see if the DIST_BURST_DB_PICKLE_NAME file exists and create it if it doesn't
        if not os.path.isfile(pickle_file_name):
            with open(pickle_file_name, "wb") as f:
                pickle.dump((dist_products, bursts_to_products, product_to_bursts, all_tile_ids), f)
                logger.info(f"Saved DIST-S1 burst database to {pickle_file_name}.")
    disp_burst_map_file = args.db_file
else:
    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()
    disp_burst_map_file = None

if args.subparser_name == "list":
    l = list(all_tile_ids)
    print("Tile IDs (%d): \n" % len(l), l)

elif args.subparser_name == "summary":
    print("Not yet implemented")
    exit(0)
    l = list(disp_burst_map.keys())
    print([(f, len(disp_burst_map[f].burst_ids), len(disp_burst_map[f].sensing_datetimes))  for f in l])

    print("Frame numbers: %d" % len(l))

    # Add up all the sensing times and print it out
    total_sensing_times = 0
    for f in l:
        total_sensing_times += len(disp_burst_map[f].sensing_datetimes)
    print("Total sensing times: ", total_sensing_times)

    # Add up and print out the total number of granules.
    total_granules = 0
    for f in l:
        total_granules += len(disp_burst_map[f].burst_ids) * len(disp_burst_map[f].sensing_datetimes)
    print("Total granules: ", total_granules)

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

elif args.subparser_name == "tile_id":
    tile_id = args.tile_id
    if tile_id not in dist_products.keys():
        print("Tile ID: ", tile_id, "does not exist")
        exit(-1)

    print("Tile ID: ", tile_id)
    print("Product IDs and burst ids: ")
    for product_id in sorted(list(dist_products[tile_id])):
        burst_ids = sorted(list(product_to_bursts[product_id]))
        print(f"{product_id} ({len(burst_ids)} bursts): {burst_ids}")

elif args.subparser_name == "burst_id":
    burst_id = args.burst_id
    if burst_id not in bursts_to_products.keys():
        print("Burst id: ", burst_id, "is not associated with any products")
        exit(-1)

    print("Burst id: ", burst_id)
    product_ids = bursts_to_products[burst_id]
    print("Product IDs: ({len(product_ids))", product_ids)
