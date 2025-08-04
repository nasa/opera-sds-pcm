import sys
import os
from functools import cache
import pickle
from copy import deepcopy

import pandas as pd
from collections import defaultdict
import dateutil.parser
from datetime import date, datetime

from opera_commons.logger import get_logger
from rtc_utils import determine_acquisition_cycle, _EPOCH_S1A, _EPOCH_S1C, _EPOCH_S1D
from data_subscriber.cslc_utils import parse_r2_product_file_name, localize_anc_json
from data_subscriber.url import rtc_for_dist_unique_id
from data_subscriber.cslc_utils import PENDING_JOBS_ES_INDEX_NAME

DEFAULT_DIST_BURST_DB_NAME = "mgrs_burst_lookup_table.parquet"
DIST_BURST_DB_PICKLE_NAME = "mgrs_burst_lookup_table.pickle"
K_OFFSETS_AND_COUNTS = "[(365, 3), (730, 3), (1095, 3)]"
PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD = "rtc_for_download"

logger = get_logger()

def parse_local_burst_db_pickle(db_file_name, pickle_file_name):
    """Parse the local DIST-S1 burst database pickle file or process the parquet file if the pickle file does not exist."""
    try:
        with open(pickle_file_name, "rb") as f:
            dist_products, bursts_to_products, product_to_bursts, all_tile_ids = pickle.load(f)
            logger.info("Loaded DIST-S1 burst database from pickle file.")
    except FileNotFoundError:
        logger.info(f"Could not find {pickle_file_name}. Processing DIST-S1 burst database file.")
        logger.info(f"Using local DIST-S1 database parquet file: {db_file_name}")
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = process_dist_burst_db(db_file_name)
        # Check to see if the DIST_BURST_DB_PICKLE_NAME file exists and create it if it doesn't
        if not os.path.isfile(pickle_file_name):
            with open(pickle_file_name, "wb") as f:
                pickle.dump((dist_products, bursts_to_products, product_to_bursts, all_tile_ids), f)
                logger.info(f"Saved DIST-S1 burst database to {pickle_file_name}.")

    return dist_products, bursts_to_products, product_to_bursts, all_tile_ids
@cache
def localize_dist_burst_db():

    # First see if a pickle file exists
    try:
        with open(DIST_BURST_DB_PICKLE_NAME, "rb") as f:
            dist_products, bursts_to_products, product_to_bursts, all_tile_ids = pickle.load(f)
            logger.info("Loaded DIST-S1 burst database from pickle file.")
            return dist_products, bursts_to_products, product_to_bursts, all_tile_ids
    except FileNotFoundError:
        logger.info(f"Could not find {DIST_BURST_DB_PICKLE_NAME}. Processing DIST-S1 burst database file.")

    try:
        file = localize_anc_json("DIST_S1_BURST_DB_S3PATH")
    except:
        logger.warning(f"Could not download DISD-S1 burst database json from settings.yaml field DIST_S1_BURST_DB_S3PATH from S3. "
                       f"Attempting to use local copy named {DEFAULT_DIST_BURST_DB_NAME}.")
        file = DEFAULT_DIST_BURST_DB_NAME

    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = process_dist_burst_db(file)

    # Check to see if the DIST_BURST_DB_PICKLE_NAME file exists and create it if it doesn't
    if not os.path.isfile(DIST_BURST_DB_PICKLE_NAME):
        with open(DIST_BURST_DB_PICKLE_NAME, "wb") as f:
            pickle.dump((dist_products, bursts_to_products, product_to_bursts, all_tile_ids), f)
            logger.info(f"Saved DIST-S1 burst database to {DIST_BURST_DB_PICKLE_NAME}.")

    return dist_products, bursts_to_products, product_to_bursts, all_tile_ids

@cache
def process_dist_burst_db(file):
    dist_products = defaultdict(set)
    bursts_to_products = defaultdict(set)
    product_to_bursts = defaultdict(set)

    df = pd.read_parquet(file)
    all_tile_ids = df['mgrs_tile_id'].unique()
    all_burst_ids = set()

    rtc_bursts_reused = 0

    logger.info(f"Processing {df.shape[0]} rows in the DIST-S1 burst database file...")

    # Create a dictionary of tile ids and the products that are associated with them
    for index, row in df.iterrows():
        #print(row['mgrs_tile_id'], row['acq_group_id_within_mgrs_tile'])
        tile_id = row['mgrs_tile_id']
        unique_acquisition = row['acq_group_id_within_mgrs_tile']
        product_id = tile_id + "_" + str(unique_acquisition)
        if product_id not in dist_products[tile_id]:
            dist_products[tile_id].add(product_id)

        jpl_burst_id = row['jpl_burst_id']
        bursts_to_products[jpl_burst_id].add(product_id)
        product_to_bursts[product_id].add(jpl_burst_id)

        if jpl_burst_id in all_burst_ids:
            rtc_bursts_reused += 1
        all_burst_ids.add(row['jpl_burst_id'])

    print(f"Total of {len(all_burst_ids)} unique RTC bursts in this database file.")
    print(f"RTC Bursts were reused {rtc_bursts_reused} times in this database file.")

    return dist_products, bursts_to_products, product_to_bursts, all_tile_ids

class DIST_S1_Product(object):
    def __init__(self):
        self.possible_bursts = 0
        self.used_bursts = 0
        self.rtc_granules = []
        self.acquisition_index = None
        self.earliest_acquisition = None
        self.latest_acquisition = None
        self.earliest_creation = None

def dist_s1_download_batch_id(granule):
    """Fro DIST-S1 download_batch_id is a function of the granule's frame_id and acquisition_cycle"""

    download_batch_id = "p"+str(granule["product_id"]) + "_a" + str(granule["acquisition_cycle"])

    return download_batch_id

def build_rtc_native_ids(product_id: int, product_to_bursts):
    """Builds the native_id string for a given DIST-S1 product. The native_id string is used in the CMR query."""

    native_ids = list(product_to_bursts[product_id])
    native_ids = sorted(native_ids) # Sort to just enforce consistency
    return len(native_ids), "OPERA_L2_RTC-S1_" + "*&native-id[]=OPERA_L2_RTC-S1_".join(native_ids) + "*"

def dist_s1_split_download_batch_id(download_batch_id):
    """Split the download_batch_id into product_id and acquisition_cycle by utilizing split by _
    example: p33UVB_4_a302 -> 33UVB_4, 302"""

    product_id = download_batch_id.split("_")[0][1:]
    acquisition_cycle = download_batch_id.split("_")[2][1:]

    return product_id, acquisition_cycle

def rtc_granules_by_acq_index(granules):
    '''Returns a dict where the key is the acq index and the value is a list of granules'''
    granules_by_acq_index = defaultdict(list)
    for granule in granules:
        burst_id, acquisition_dts = parse_r2_product_file_name(granule["granule_id"], "L2_RTC_S1")
        acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, granule["granule_id"])
        granules_by_acq_index[acquisition_index].append(granule)

    return granules_by_acq_index

def basic_decorate_granule(granule):
    '''Decorate the granule with the burst_id, frame_id, and acquisition_cycle in place'''

    burst_id, acquisition_dts = parse_r2_product_file_name(granule["granule_id"], "L2_RTC_S1")
    granule["burst_id"] = burst_id
    granule["acquisition_ts"] = dateutil.parser.isoparse(acquisition_dts[:-1])  # convert to datetime object
    granule["acquisition_cycle"] = determine_acquisition_cycle(granule["burst_id"], acquisition_dts, granule["granule_id"])
    granule["mission"] = granule["granule_id"].split("_")[6] # S1A, S1B, S1C, S1D

def decorate_granule(granule):
    granule["tile_id"], granule["acquisition_group"] = granule["product_id"].split("_")
    granule["batch_id"] = granule["product_id"] + "_" + str(granule["acquisition_cycle"])
    granule["download_batch_id"] = dist_s1_download_batch_id(granule)
    granule["unique_id"] = rtc_for_dist_unique_id(granule["download_batch_id"], granule["burst_id"])

def extend_rtc_for_dist_records(bursts_to_products, granules, no_duplicate=False, force_product_id=None):
    '''Extend the granules list with duplicated granules with different product_ids.'''

    extended_granules = []

    for granule in granules:
        rtc_granule_id = granule["granule_id"]
        product_ids = list(bursts_to_products[granule["burst_id"]])

        if len(product_ids) == 0:
            logger.warning(f"Skipping {rtc_granule_id} as it does not belong to any DIST-S1 product.")
            continue

        granule["product_id"] = force_product_id if force_product_id else product_ids[0]
        decorate_granule(granule)

        if len(product_ids) > 1 and no_duplicate == False:
            for product_id in product_ids[1:]:
                new_granule = deepcopy(granule)
                new_granule["product_id"] = force_product_id if force_product_id else product_id
                decorate_granule(new_granule)
                extended_granules.append(new_granule)

    granules.extend(extended_granules)

def get_unique_rtc_id_for_dist(granule_id):
    '''Get the unique id for a DIST-S1 triggering. The unique id is the granule_id up to the acquisition time.
    example: "OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0" -> "OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z"
    '''
    return "_".join(granule_id.split("_")[0:5])

def add_unique_rtc_granules(granules_dict: dict, granules: list) -> None:
    '''Add unique granules to the granules_dict. The key is a tuple of granule_id up to the acquisition time and the batch_id.
    example: ("OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z", "p31RGQ_3_a302") and the value is the granule itself.
    If there's more than one granule for the same key, use the one with the latest production date.
    The dictionary is updated in place.
    '''

    for granule in granules:
        unique_granule = get_unique_rtc_id_for_dist(granule["granule_id"])
        key = (unique_granule, granule["batch_id"])
        if key in granules_dict:
            granules_dict[key] = granule if granule["granule_id"] > granules_dict[key]["granule_id"] else granules_dict[key]
        else:
            granules_dict[key] = granule
def compute_dist_s1_triggering(product_to_bursts, denorm_granules_dict, complete_bursts_only, grace_mins, now, all_tile_ids = None):
    '''Given a list of tuples that represent denormalized granules, compute the triggering of DIST-S1 products
    Denormalized means is that the RTC granules already went through extension and therefore potential duplication based on producd_id
    and therefore do not need to be duplicated again.

    One tuple looks like this: ('OPERA_L2_RTC-S1_T168-359432-IW2_20231217T052423Z', '33VVE_4_302')
    '''

    unused_rtc_granule_count = 0
    products_triggered = defaultdict(DIST_S1_Product)
    granules_triggered = defaultdict(bool)
    if all_tile_ids:
        tiles_untriggered = set(all_tile_ids)
        all_tiles_set = set(all_tile_ids)
    else:
        tiles_untriggered = None

    for d_g, granule in denorm_granules_dict.items():

        # If this granule was from the unsubmitted list, we will use the creation_timestamp from ES to evaludate against the grace period
        # creation_timestamp looks like this: 2025-04-17T00:19:08.283857
        creation_timestamp = dateutil.parser.isoparse(granule["creation_timestamp"]) if "creation_timestamp" in granule else now

        partial_granule_id = d_g[0]
        acq_datetime = dateutil.parser.isoparse(d_g[0].split("_")[-1])
        acquisition_index = int(d_g[1].split("_")[-1])
        # print(burst_id, acq_datetime, acquisition_index)

        batch_id = d_g[1]
        product_id = "_".join(batch_id.split("_")[0:2])
        triggered_product = products_triggered[batch_id]
        triggered_product.rtc_granules.append(partial_granule_id)
        triggered_product.possible_bursts = len(product_to_bursts[product_id])
        triggered_product.used_bursts += 1

        # earliest_creation is used for grace period evaluation
        if triggered_product.earliest_creation is None or creation_timestamp < triggered_product.earliest_creation:
            triggered_product.earliest_creation = creation_timestamp

        if triggered_product.earliest_acquisition is None or acq_datetime < triggered_product.earliest_acquisition:
            triggered_product.earliest_acquisition = acq_datetime
        if triggered_product.latest_acquisition is None or acq_datetime > triggered_product.latest_acquisition:
            triggered_product.latest_acquisition = acq_datetime
        if triggered_product.acquisition_index is None:
            triggered_product.acquisition_index = acquisition_index

        if all_tile_ids:
            tile_id = product_id.split("_")[0]
            if tile_id in tiles_untriggered:
                tiles_untriggered.remove(tile_id)
            else:
                if tile_id not in all_tiles_set:
                    print(f"Tile ID {tile_id}: {partial_granule_id} does not belong to any DIST-S1 product.")
                    unused_rtc_granule_count += 1

    # If complete_bursts_only is True, remove all products_triggered where used_bursts != possible_bursts
    # Also update granules_triggered which is a map from granule id to boolean where True means the granule was used
    if complete_bursts_only:
        for product_id, product in list(products_triggered.items()):
            if product.possible_bursts != product.used_bursts:
                mins_delta = (now - product.earliest_creation).total_seconds() / 60
                if mins_delta < grace_mins:
                    del products_triggered[product_id]
                    for granule_id in product.rtc_granules:
                        granules_triggered[granule_id] = False
                else:
                    logger.info(f"Product {product_id} was triggered with {product.used_bursts} out of {product.possible_bursts} bursts. "
                                f"Earliest creation time is {product.earliest_creation} and current time is {now}, a delta of {mins_delta} minutes. This is outside of the grace period {grace_mins} minutes.")
            else:
                for granule_id in product.rtc_granules:
                    granules_triggered[granule_id] = True
                logger.info(f"Product {product_id} was triggered with {product.used_bursts} out of {product.possible_bursts} bursts. ")

    return products_triggered, granules_triggered, tiles_untriggered, unused_rtc_granule_count


def parse_k_parameter(k_offsets_and_counts):
    '''Parse the k parameter from the command line. The k parameter is a list of tuples where each tuple is a list of offsets and counts.
    example: "[(0, 1), (2, 3), (4, 5)]" -> [(0, 1), (2, 3), (4, 5)]
    '''
    k_offsets_and_counts = k_offsets_and_counts.strip("[]").split("),")
    k_offsets_and_counts = [k.strip(" ()") for k in k_offsets_and_counts]
    k_offsets_and_counts = [tuple(map(int, k.split(","))) for k in k_offsets_and_counts]
    return k_offsets_and_counts

def previous_product_download_batch_id(dist_products, download_batch_id):
    """Determine the previous product download batch id for a given batch id.
    """
    tile_id, acquisition_group, acquisition_cycle = download_batch_id.split("_")
    tile_id = tile_id[1:] # Remove the "p" from the tile_id
    acquisition_group = int(acquisition_group)
    acquisition_cycle = int(acquisition_cycle[1:]) # Remove the "a" from the acquisition cycle

    while True:
        if acquisition_group > 0:
            acquisition_group -= 1
            prev_product = tile_id + "_" + str(acquisition_group)
            if prev_product in dist_products[tile_id]:
                break
        else: # If the acquisition group is 0, we need to decrement the acquisition cycle and set the acquisition group to max for that tile
            acquisition_cycle -= 1
            prev_product = max(dist_products[tile_id], key=lambda x: int(x.split("_")[-1]))
            acquisition_group = int(prev_product.split("_")[-1])
            break
    prev_product_download_batch_id = "p" + tile_id + "_" + str(acquisition_group) + "_a" + str(acquisition_cycle)

    return prev_product_download_batch_id

def previous_product_download_batch_id_from_rtc(dist_products, bursts_to_products, download_batch_id, granule_ids):
    '''Determine the previous product download batch id for a given batch id among list of RTC granules.'''

    # TODO: As the first pass we're going to inefficient brute force search. But it may actually being practical ...
    # because we'll do like 100 dict look ups at most and so that's less than 10 milliseconds. We should still optimize this.

    granules_dict, rtc_granules = granule_list_to_trigger_data_structure(granule_ids, bursts_to_products)
    min_acquisition_cycle = min(g["acquisition_cycle"] for g in rtc_granules)
    download_batch_id_dict = {}
    for g in rtc_granules:
        download_batch_id_dict[g["download_batch_id"]] = g

    while True:
        try_previous_id = previous_product_download_batch_id(dist_products, download_batch_id)
        if try_previous_id in download_batch_id_dict:
            return try_previous_id
        else:
            try_acquisition_cycle = int(try_previous_id.split("_")[-1][1:])
            if try_acquisition_cycle < min_acquisition_cycle:
                return None
            else:
                download_batch_id = try_previous_id

    return None # Should never get here

def granule_list_to_trigger_data_structure(granule_ids, bursts_to_products):
    '''Convert a list of rtc granule ids to a trigger data structure.'''
    granules_dict = {}
    granules = []
    for g in granule_ids:
        burst_id, acquisition_dts = parse_r2_product_file_name(g, "L2_RTC_S1")
        acquisition_cycle = determine_acquisition_cycle(burst_id, acquisition_dts, g)
        # Only add the granule if it belongs to a DIST-S1 product
        if burst_id in bursts_to_products:
            granules.append({"granule_id": g, "burst_id": burst_id, "acquisition_cycle": acquisition_cycle})
    for g in granules:
        basic_decorate_granule(g)
        #granules_dict[g["granule_id"]] = g
    extend_rtc_for_dist_records(bursts_to_products, granules)
    add_unique_rtc_granules(granules_dict, granules)

    return granules_dict, granules

def trigger_from_cmr_survey_csv(cmr_survey_csv, complete_bursts_only, grace_mins, now, product_to_bursts, bursts_to_products):

    min_acq_datetime = None
    max_acq_datetime = None

    # Open up RTC CMR survey CSV file and parse the native IDs and then start computing triggering logic
    rtc_survey = pd.read_csv(cmr_survey_csv)
    granule_ids = []
    for index, row in rtc_survey.iterrows():
        rtc_granule_id = row['# Granule ID']
        granule_ids.append(rtc_granule_id)
        burst_id, acquisition_dts = parse_r2_product_file_name(rtc_granule_id, "L2_RTC_S1")
        acq_datetime = dateutil.parser.isoparse(acquisition_dts)
        #acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, rtc_granule_id)
        # print(burst_id, acq_datetime, acquisition_index)
        if min_acq_datetime is None or acq_datetime < min_acq_datetime:
            min_acq_datetime = acq_datetime
        if max_acq_datetime is None or acq_datetime > max_acq_datetime:
            max_acq_datetime = acq_datetime
    rtc_granule_count = len(granule_ids)

    granules_dict, granules = granule_list_to_trigger_data_structure(granule_ids, bursts_to_products)

    logger.info("\nComputing for triggered DIST-S1 products...")
    products_triggered, granules_triggered, tiles_untriggered, unused_rtc_granule_count = \
        compute_dist_s1_triggering(product_to_bursts, granules_dict, complete_bursts_only, grace_mins, now)
    
    logger.info(f"RTC granule count: {rtc_granule_count}")
    logger.info(f"Total of {len(products_triggered)} products were triggered by RTC data between {min_acq_datetime} and {max_acq_datetime}")
    time_delta = max_acq_datetime - min_acq_datetime
    total_days = time_delta.total_seconds() / 86400
    logger.info(f"Total of {total_days} days between the earliest and latest acquisition time.")
    logger.info(f"Which yields an average of {len(products_triggered) / total_days} products per day.")

    return products_triggered, granules_triggered, tiles_untriggered, unused_rtc_granule_count

if __name__ == "__main__":

    db_file = sys.argv[1]
    cmr_survey_file = sys.argv[2]

    unique_bursts = set()
    df = pd.read_parquet(db_file)

    #for index, row in df.iterrows():
    #    unique_bursts.add(row['jpl_burst_id'])

    #print(dist_products)

    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = parse_local_burst_db_pickle(db_file, db_file+".pickle")
    print(f"There are {all_tile_ids.size} unique tiles.")

    row_count = df.shape[0]

    '''print(f"{row_count=}")
    another_row_count = 0
    for product, bursts in product_to_bursts.items():
        another_row_count += len(bursts)
    print(f"{another_row_count=}")'''

    all_product_count = 0
    for tile_id, products in dist_products.items():
        all_product_count += len(products)

    print(f"There are {all_product_count} unique products, {all_product_count/12} per day because this total is for 12 days")
    print(f"On average there are {all_product_count / all_tile_ids.size} products per tile.")
    print(f"On average there are {row_count / all_product_count} rows per product which means that number of RTC bursts were used in one product generation.")
    print("Example tile and products:")
    print(dist_products['01FBE'])
    #print(f"Total rows is {row_count} and there are {len(unique_bursts)} unique bursts. Therefore each burst is used on average {row_count/len(unique_bursts)} times.")

    logger.info("\nReading RTC CMR survey CSV file...")
    products_triggered, granules_triggered, tiles_untriggered, unused_rtc_granule_count = \
        trigger_from_cmr_survey_csv(cmr_survey_file, True, 200, datetime.now(), product_to_bursts, bursts_to_products)

    # Compute average burst usage percentage
    total_bursts = 0
    total_used_bursts = 0
    for product_id, product in products_triggered.items():
        total_bursts += product.possible_bursts
        total_used_bursts += product.used_bursts
    print(f"Average burst usage is {total_used_bursts / total_bursts * 100}%")
    #print(f"Total of {len(tiles_untriggered)} tiles were not triggered by RTC data. This is {len(tiles_untriggered) / all_tile_ids.size * 100}% of all tiles.")
    print(f"Total of {unused_rtc_granule_count} RTC granules were not used in any product generation.")

    print("Example product and RTC granule IDs:")

    # Write out all products triggered into a json file
    logger.info("\nWriting out products_triggered.json...")
    import json
    def json_default(value):
        if isinstance(value, date):
            return dict(datetime=value.strftime('%Y-%m-%d %H:%M:%SZ'))
        else:
            return value.__dict__

    with open('products_triggered.json', 'w') as f:
        json.dump(products_triggered, f, default=json_default, indent=4)

    product_name = list(products_triggered.keys())[0]
    print(product_name)
    print(f"Earliest acquisition time for {product_name} is {products_triggered[product_name].earliest_acquisition} and latest is {products_triggered[product_name].latest_acquisition}")
    print(f"Product {product_name} has {products_triggered[product_name].used_bursts} out of {products_triggered[product_name].possible_bursts} bursts used.")
    print(f"Product {product_name} has acquisition index {products_triggered[product_name].acquisition_index}")
    print(products_triggered[product_name].rtc_granules)



