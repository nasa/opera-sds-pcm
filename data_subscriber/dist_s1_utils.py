import sys

import pandas as pd
import re
from collections import defaultdict
import dateutil.parser
from datetime import date

from cslc_utils import parse_r2_product_file_name
from url import determine_acquisition_cycle

unique_bursts = set()
all_product_count = 0
row_count = 0

df = pd.read_parquet('mgrs_burst_lookup_table.parquet')

all_tile_ids = df['mgrs_tile_id'].unique()
print(f"There are {all_tile_ids.size} unique tiles.")

dist_products = defaultdict(set)
bursts_to_products = defaultdict(set)
product_to_bursts = defaultdict(set)

# Create a dictionary of tile ids and the products that are associated with them
for index, row in df.iterrows():
    #print(row['mgrs_tile_id'], row['acq_group_id_within_mgrs_tile'])
    tile_id = row['mgrs_tile_id']
    unique_acquisition = row['acq_group_id_within_mgrs_tile']
    product_id = tile_id + "_" + str(unique_acquisition)
    row_count += 1
    unique_bursts.add(row['jpl_burst_id'])
    if product_id not in dist_products[tile_id]:
        all_product_count += 1
        dist_products[tile_id].add(product_id)
    bursts_to_products[row['jpl_burst_id']].add(product_id)
    product_to_bursts[product_id].add(row['jpl_burst_id'])

#print(dist_products)

print(f"There are {all_product_count} unique products, {all_product_count/12} per day because this total is for 12 days")
print(f"On average there are {all_product_count / all_tile_ids.size} products per tile.")
print(f"On average there are {row_count / all_product_count} rows per product which means that number of RTC bursts were used in one product generation.")
print("Example tile and products:")
print(dist_products['01FBE'])
print(f"Total rows is {row_count} and there are {len(unique_bursts)} unique bursts. Therefore each burst is used on average {row_count/len(unique_bursts)} times.")

print("\nComputing for triggered DIST-S1 products...")

min_acq_datetime = None
max_acq_datetime = None
rtc_granule_count = 0

class DIST_S1_Product(object):
    def __init__(self):
        self.possible_bursts = 0
        self.used_bursts = 0
        self.rtc_granules = []
        self.acquisition_index = None
        self.earliest_acquisition = None
        self.latest_acquisition = None

products_triggered = defaultdict(DIST_S1_Product)

# Open up RTC CMR survey CSV file and parse the native IDs and then start computing triggering logic
rtc_survey = pd.read_csv(sys.argv[1])
for index, row in rtc_survey.iterrows():
    rtc_granule_id = row['# Granule ID']
    burst_id, acquisition_dts = parse_r2_product_file_name(rtc_granule_id, "L2_RTC_S1")
    acq_datetime = dateutil.parser.isoparse(acquisition_dts)
    acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, rtc_granule_id)
    #print(burst_id, acq_datetime, acquisition_index)
    if min_acq_datetime is None or acq_datetime < min_acq_datetime:
        min_acq_datetime = acq_datetime
    if max_acq_datetime is None or acq_datetime > max_acq_datetime:
        max_acq_datetime = acq_datetime
    rtc_granule_count += 1

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

# Compute average burst usage percentage
total_bursts = 0
total_used_bursts = 0
for product_id, product in products_triggered.items():
    total_bursts += product.possible_bursts
    total_used_bursts += product.used_bursts
print(f"Average burst usage is {total_used_bursts / total_bursts * 100}%")

print("RTC granule count:", rtc_granule_count)
print(f"Total of {len(products_triggered)} products were triggered by RTC data between {min_acq_datetime} and {max_acq_datetime}")
time_delta = max_acq_datetime - min_acq_datetime
total_days = time_delta.total_seconds() / 86400
print(f"Total of {total_days} days between the earliest and latest acquisition time.")
print(f"Which yields an average of {len(products_triggered) / total_days} products per day.")
print("Example product and RTC granule IDs:")

# Write out all products triggered into a json file
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



