import sys
from functools import cache

import pandas as pd
from collections import defaultdict
import dateutil.parser
from datetime import date

from commons.logger import get_logger
from cslc_utils import parse_r2_product_file_name, localize_anc_json
from url import determine_acquisition_cycle

DEFAULT_DIST_BURST_DB_NAME= "mgrs_burst_lookup_table.parquet"

logger = get_logger()

@cache
def localize_dist_burst_db():

    try:
        file = localize_anc_json("DIST_S1_BURST_DB_S3PATH")
    except:
        logger.warning(f"Could not download DISD-S1 burst database json from settings.yaml field DIST_S1_BURST_DB_S3PATH from S3. "
                       f"Attempting to use local copy named {DEFAULT_DIST_BURST_DB_NAME}.")
        file = DEFAULT_DIST_BURST_DB_NAME

    return process_dist_burst_db(file)

def process_dist_burst_db(file):
    dist_products = defaultdict(set)
    bursts_to_products = defaultdict(set)
    product_to_bursts = defaultdict(set)

    df = pd.read_parquet(file)
    all_tile_ids = df['mgrs_tile_id'].unique()

    # Create a dictionary of tile ids and the products that are associated with them
    for index, row in df.iterrows():
        #print(row['mgrs_tile_id'], row['acq_group_id_within_mgrs_tile'])
        tile_id = row['mgrs_tile_id']
        unique_acquisition = row['acq_group_id_within_mgrs_tile']
        product_id = tile_id + "_" + str(unique_acquisition)
        if product_id not in dist_products[tile_id]:
            dist_products[tile_id].add(product_id)
        bursts_to_products[row['jpl_burst_id']].add(product_id)
        product_to_bursts[product_id].add(row['jpl_burst_id'])

    return dist_products, bursts_to_products, product_to_bursts, all_tile_ids

class DIST_S1_Product(object):
    def __init__(self):
        self.possible_bursts = 0
        self.used_bursts = 0
        self.rtc_granules = []
        self.acquisition_index = None
        self.earliest_acquisition = None
        self.latest_acquisition = None

def compute_dist_s1_triggering(bursts_to_products, granule_ids, all_tile_ids):

    unused_rtc_granule_count = 0
    products_triggered = defaultdict(DIST_S1_Product)
    tiles_untriggered = set(all_tile_ids)
    all_tiles_set = set(all_tile_ids)

    for rtc_granule_id in granule_ids:
        burst_id, acquisition_dts = parse_r2_product_file_name(rtc_granule_id, "L2_RTC_S1")
        acq_datetime = dateutil.parser.isoparse(acquisition_dts)
        acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, rtc_granule_id)
        # print(burst_id, acq_datetime, acquisition_index)

        product_ids = bursts_to_products[burst_id]
        for product_id in product_ids:
            triggered_product = products_triggered[product_id + "_" + str(acquisition_index)]
            triggered_product.rtc_granules.append(rtc_granule_id)
            triggered_product.possible_bursts = len(product_to_bursts[product_id])
            triggered_product.used_bursts += 1
            if triggered_product.earliest_acquisition is None or acq_datetime < triggered_product.earliest_acquisition:
                triggered_product.earliest_acquisition = acq_datetime
            if triggered_product.latest_acquisition is None or acq_datetime > triggered_product.latest_acquisition:
                triggered_product.latest_acquisition = acq_datetime
            if triggered_product.acquisition_index is None:
                triggered_product.acquisition_index = acquisition_index

            tile_id = product_id.split("_")[0]
            if tile_id in tiles_untriggered:
                tiles_untriggered.remove(tile_id)
            else:
                if tile_id not in all_tiles_set:
                    print(f"Tile ID {tile_id}: {rtc_granule_id} does not belong to any DIST-S1 product.")
                    unused_rtc_granule_count += 1

    return products_triggered, tiles_untriggered, unused_rtc_granule_count

if __name__ == "__main__":

    db_file = sys.argv[1]
    cmr_survey_file = sys.argv[2]

    unique_bursts = set()
    df = pd.read_parquet(db_file)

    #for index, row in df.iterrows():
    #    unique_bursts.add(row['jpl_burst_id'])

    #print(dist_products)

    logger.info("\nProcessing DIST-S1 burst database...")

    dist_products, bursts_to_products, product_to_bursts, all_tile_ids = process_dist_burst_db(db_file)
    print(f"There are {all_tile_ids.size} unique tiles.")

    row_count = df.shape[0]
    all_product_count = 0
    for tile_id, products in dist_products.items():
        all_product_count += len(products)

    print(f"There are {all_product_count} unique products, {all_product_count/12} per day because this total is for 12 days")
    print(f"On average there are {all_product_count / all_tile_ids.size} products per tile.")
    print(f"On average there are {row_count / all_product_count} rows per product which means that number of RTC bursts were used in one product generation.")
    print("Example tile and products:")
    print(dist_products['01FBE'])
    #print(f"Total rows is {row_count} and there are {len(unique_bursts)} unique bursts. Therefore each burst is used on average {row_count/len(unique_bursts)} times.")

    min_acq_datetime = None
    max_acq_datetime = None

    logger.info("\nReading RTC CMR survey CSV file...")

    # Open up RTC CMR survey CSV file and parse the native IDs and then start computing triggering logic
    rtc_survey = pd.read_csv(cmr_survey_file)
    granule_ids = []
    for index, row in rtc_survey.iterrows():
        rtc_granule_id = row['# Granule ID']
        granule_ids.append(rtc_granule_id)
        burst_id, acquisition_dts = parse_r2_product_file_name(rtc_granule_id, "L2_RTC_S1")
        acq_datetime = dateutil.parser.isoparse(acquisition_dts)
        acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, rtc_granule_id)
        # print(burst_id, acq_datetime, acquisition_index)
        if min_acq_datetime is None or acq_datetime < min_acq_datetime:
            min_acq_datetime = acq_datetime
        if max_acq_datetime is None or acq_datetime > max_acq_datetime:
            max_acq_datetime = acq_datetime
    rtc_granule_count = len(granule_ids)

    logger.info("\nComputing for triggered DIST-S1 products...")
    products_triggered, tiles_untriggered, unused_rtc_granule_count = compute_dist_s1_triggering(bursts_to_products, granule_ids, all_tile_ids)

    # Compute average burst usage percentage
    total_bursts = 0
    total_used_bursts = 0
    for product_id, product in products_triggered.items():
        total_bursts += product.possible_bursts
        total_used_bursts += product.used_bursts
    print(f"Average burst usage is {total_used_bursts / total_bursts * 100}%")
    print(f"Total of {len(tiles_untriggered)} tiles were not triggered by RTC data. This is {len(tiles_untriggered) / all_tile_ids.size * 100}% of all tiles.")
    print(f"Total of {unused_rtc_granule_count} RTC granules were not used in any product generation.")

    print("RTC granule count:", rtc_granule_count)
    print(f"Total of {len(products_triggered)} products were triggered by RTC data between {min_acq_datetime} and {max_acq_datetime}")
    time_delta = max_acq_datetime - min_acq_datetime
    total_days = time_delta.total_seconds() / 86400
    print(f"Total of {total_days} days between the earliest and latest acquisition time.")
    print(f"Which yields an average of {len(products_triggered) / total_days} products per day.")
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



