import argparse
import concurrent.futures
import multiprocessing
import random
import re
import sqlite3
import time
import sys
import requests

import pandas as pd
from tabulate import tabulate
import tqdm
import logging

from opv_util import (generate_url_params, parallel_fetch, retrieve_r3_products, get_total_granules, get_burst_id,
                      get_burst_sensing_datetime, get_burst_ids_from_file)
from opv_disp_s1 import validate_disp_s1, map_cslc_bursts_to_frames

from data_subscriber.cslc_utils import parse_cslc_file_name, localize_disp_frame_burst_hist

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_granules_from_query(start, end, timestamp, endpoint, provider = 'ASF', shortname = 'OPERA_L2_RTC-S1_V1'):
    """
    Fetches granule metadata from the CMR API within a specified temporal range using parallel requests.

    :start: Start time in ISO 8601 format.
    :end: End time in ISO 8601 format.
    :timestamp: Type of timestamp to filter granules (e.g., 'TEMPORAL', 'PRODUCTION').
    :endpoint: CMR API endpoint ('OPS' or 'UAT').
    :provider: Data provider ID (default 'ASF').
    :shortname: Short name of the product (default 'OPERA_L2_RTC-S1_V1').
    :return: List of granule metadata.
    """

    granules = []

    base_url, params = generate_url_params(start=start, end=end, timestamp_type=timestamp, endpoint=endpoint, provider=provider, short_name=shortname)

    # Construct the URL for the total granules query
    total_granules = get_total_granules(base_url, params)
    print(f"Total granules: {total_granules}")
    print(f"Querying CMR for time range {start} to {end}.")

    # Exit with error code if no granules to process
    if (total_granules == 0):
        print(f"Error: no granules to process.")
        sys.exit(1)

    # Optimize page_size and number of workers based on total_granules
    page_size = min(1000, total_granules)

    # Initialize progress bar
    tqdm.tqdm._instances.clear()  # Clear any existing tqdm instances
    print()

    # Main loop to fetch granules, update progress bar, and extract burst_ids
    with tqdm.tqdm(total=total_granules, desc="Fetching granules", position=0) as pbar_global:
        downloaded_batches = multiprocessing.Value('i', 0)  # For counting downloaded batches
        total_batches = (total_granules + page_size - 1) // page_size

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # NOTE: parallelized workers beyond 5 not working well, but here's some code to make it work in the future
            # max_workers = min(5, (total_granules + page_size - 1) // page_size)
            # futures = [executor.submit(parallel_fetch, base_url, params, page_num, page_size, downloaded_batches, total_batches) for page_num in range(1, total_batches + 1)]
            futures = []
            for page_num in range(1, total_batches + 1):
                future = executor.submit(parallel_fetch, base_url, params, page_num, page_size, downloaded_batches)
                futures.append(future)
                random_delay = random.uniform(0, 0.1)
                time.sleep(random_delay) # Stagger the submission of function calls for CMR optimization
                logging.debug(f"Scheduled granule fetch for batch {page_num}")

            for future in concurrent.futures.as_completed(futures):
                granules_result = future.result()
                pbar_global.update(len(granules_result))

                granules.extend(granules_result)

    print("\nGranule fetching complete.")

    # Integrity check for total granules
    total_downloaded = sum(len(future.result()) for future in futures)
    if total_downloaded != total_granules:
        print(f"\nError: Expected {total_granules} granules, but downloaded {total_downloaded}. Try running again after some delay.")
        sys.exit(1)
    
    return granules

def get_granule_ids_from_granules(granules):
    """
    Extracts granule IDs from a list of granule metadata.

    :granules: List of granule metadata dictionaries.
    :return: List of granule IDs.
    """

    granule_ids = []

    for granule in granules:
        granule_id = granule.get("umm").get("GranuleUR")
        granule_ids.append(granule_id)

    return granule_ids

def get_burst_ids_and_sensing_times_from_query(start, end, timestamp, endpoint, provider = 'ASF', shortname = 'OPERA_L2_RTC-S1_V1'):
    """
    Fetches burst IDs and their sensing times from the CMR API within a specified temporal range.

    :start: Start time in ISO 8601 format.
    :end: End time in ISO 8601 format.
    :timestamp: Type of timestamp for filtering (e.g., 'TEMPORAL', 'PRODUCTION').
    :endpoint: CMR API endpoint ('OPS' or 'UAT').
    :provider: Data provider ID (default 'ASF').
    :shortname: Product short name (default 'OPERA_L2_RTC-S1_V1').
    :return: Two dictionaries - one mapping burst IDs to granule IDs, and another mapping burst IDs to sensing times.
    """

    granules = get_granules_from_query(start=start, end=end, timestamp=timestamp, endpoint=endpoint, provider=provider, shortname=shortname)
    if (granules):
        granule_ids = get_granule_ids_from_granules(granules)
    else:
        logging.error("Problem querying for granules. Unable to proceed.")
        sys.exit(1)

    burst_ids = {}
    burst_dates = {}

    # Extract burst IDs, dates from granule IDs
    for granule_id in granule_ids:
        if shortname == 'OPERA_L2_RTC-S1_V1':
            burst_id = get_burst_id(granule_id)
            burst_date = get_burst_sensing_datetime(granule_id)
        elif shortname == 'OPERA_L2_CSLC-S1_V1':
            burst_id, burst_date = parse_cslc_file_name(granule_id)
        if (burst_id and burst_date):
            burst_ids[burst_id] = granule_id
            burst_dates[burst_id] = burst_date
        else:
            print(f"\nWarning: Could not extract burst ID from malformed granule ID {granule_id}.")
    
    return burst_ids, burst_dates



def validate_dswx_s1(smallest_date, greatest_date, endpoint, df):
    """
    Validates that the granules from the CMR query are accurately reflected in the DataFrame provided.
    It extracts granule information based on the input dates and checks which granules are missing from the DataFrame.
    The function then updates the DataFrame to include a count of unprocessed bursts based on the missing granules. 
    The logic can be summarized as:
    1. Gather list of expected RTC granule IDs (provided dataframe)
    2. Query CMR for list of actual RTC granule IDs used for DSWx-S1 production, aggregate these into a list
    3. Compare list (1) with list (2) and return a new dataframe containing a column 'Unprocessed RTC Native IDs' with the
       discrepancies

    :param smallest_date: datetime.datetime
        The earliest date in the range (ISO 8601 format).
    :param greatest_date: datetime.datetime
        The latest date in the range (ISO 8601 format).
    :param endpoint: str
        CMR environment ('UAT' or 'OPS') to specify the operational setting for the data query.
    :param df: pandas.DataFrame
        A DataFrame containing columns with granule identifiers which will be checked against the CMR query results.
        
    :return: pandas.DataFrame or bool
        A modified DataFrame with additional columns 'Unprocessed RTC Native IDs' and 'Unprocessed RTC Native IDs Count' showing
        granules not found in the CMR results and their count respectively. Returns False if the CMR query fails.
    
    Raises:
        requests.exceptions.RequestException if the CMR query fails, which is logged as an error.
    """

    dswx_s1_mgrs_tiles_to_rtc_bursts = {}

    all_granules = retrieve_r3_products(smallest_date, greatest_date, endpoint, 'OPERA_L3_DSWX-S1_V1')

    try:

        # Extract MGRS tiles and create the mapping to InputGranules
        available_rtc_bursts = []
        pattern = r"(OPERA_L2_RTC-S1_[\w-]+_\d+T\d+Z_\d+T\d+Z_S1[AB]_30_v\d+\.\d+)"
        for item in all_granules:
            input_granules = item['umm']['InputGranules']
            # native_id = item['meta']['native-id']
            mgrs_tile_id = None

            # Extract the MGRS Tile ID
            for attr in item['umm']['AdditionalAttributes']:
                if attr['Name'] == 'MGRS_TILE_ID':
                    mgrs_tile_id = attr['Values'][0]
                    break

            # Extract the granule burst ID from the full path
            for path in input_granules:
                match = re.search(pattern, path)
                if match:
                    if mgrs_tile_id:
                        # Add the MGRS Tile ID and associated InputGranules to the dictionary
                        if mgrs_tile_id in dswx_s1_mgrs_tiles_to_rtc_bursts:
                            dswx_s1_mgrs_tiles_to_rtc_bursts[mgrs_tile_id].append(match.group(1))
                        else:
                            dswx_s1_mgrs_tiles_to_rtc_bursts[mgrs_tile_id] = [match.group(1)]
                    available_rtc_bursts.append(match.group(1))

        #unique_available_rtc_bursts = set(available_rtc_bursts)

        # Function to identify missing bursts
        def filter_and_find_missing(row):
            rtc_bursts_in_df_row = set(row['Covered RTC Native IDs'].split(', '))
            mgrs_tiles_in_df_row = set(row['MGRS Tiles'].split(', '))

            unique_available_rtc_bursts = {
                item
                for key in mgrs_tiles_in_df_row
                if f"T{key}" in dswx_s1_mgrs_tiles_to_rtc_bursts
                for item in dswx_s1_mgrs_tiles_to_rtc_bursts[f"T{key}"]
            }

            unprocessed_rtc_bursts = rtc_bursts_in_df_row - unique_available_rtc_bursts
            if unprocessed_rtc_bursts:
                return ', '.join(unprocessed_rtc_bursts)
            return None  # or pd.NA 

        # Function to count missing bursts
        def count_missing(row):
            count = len(row['Unprocessed RTC Native IDs'].split(', '))
            return count

        # Apply the function and create a new column 'Unprocessed RTC Native IDs'
        df['Unprocessed RTC Native IDs'] = df.apply(filter_and_find_missing, axis=1)
        df = df.dropna(subset=['Unprocessed RTC Native IDs'])

        # Using loc to safely modify the DataFrame without triggering SettingWithCopyWarning
        df.loc[:, 'Unprocessed RTC Native IDs Count'] = df.apply(count_missing, axis=1)

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from CMR: {e}")
        
    return False

if __name__ == '__main__':
    # Create an argument parser
    parser = argparse.ArgumentParser(description="CMR Query with Temporal Range and SQLite DB Access")
    parser.add_argument("--timestamp", required=False, default='TEMPORAL', metavar="TEMPORAL|REVISION|PRODUCTION|CREATED",  help="Use temporal, revision, or production time in start / end time granule query to CMR. Ex. --timestamp revision")
    parser.add_argument("--start", required=False, help="Temporal start time (ISO 8601 format)")
    parser.add_argument("--end", required=False, help="Temporal end time (ISO 8601 format)")
    parser.add_argument("--dswx_s1_mgrs_db", required=False, help="Path to the MGRS Tile Set SQLite database file")
    parser.add_argument("--file", required=False, help="Optional file path containing granule IDs")
    parser.add_argument("--threshold", required=False, help="Completion threshold minimum to filter results by (percentage format - leave out the % sign)")
    parser.add_argument("--matching_burst_count", required=False, help="Matching burst count to filter results by. Typically four or more is advised. Using this with the --threshold flag makes this flag inactive (only one of '--threshold' or '--matching_burst_count' may be used)")
    parser.add_argument("--verbose", action='store_true', help="Verbose and detailed output")
    parser.add_argument("--endpoint_daac_input", required=False, choices=['UAT', 'OPS'], default='OPS', help='CMR endpoint venue for RTC granules')
    parser.add_argument("--endpoint_daac_output", required=False, choices=['UAT', 'OPS'], default='OPS', help='CMR endpoint venue for DSWx-S1 granules')
    parser.add_argument("--validate", action='store_true', help="Validate if DSWx-S1 products have been delivered for given time range (use --timestamp TEMPORAL mode only)")
    parser.add_argument("--product", required=True, choices=['DSWx-S1', 'DISP-S1'], default='DSWx-S1', help="The product to validate")
    # Parse the command-line arguments
    args = parser.parse_args()

    if (args.product == "DSWx-S1"):

        burst_ids = {}
        burst_dates = {}

        # Check if file input is provided, otherwise use CMR API to get burst IDs
        if args.file:
            burst_ids, burst_dates = get_burst_ids_from_file(filename=args.file)
        else:
            burst_ids, burst_dates = get_burst_ids_and_sensing_times_from_query(args.start, args.end, args.timestamp, args.endpoint_daac_input)

        # Connect to the MGRS Tile Set SQLITE database
        conn = sqlite3.connect(args.dswx_s1_mgrs_db)
        cursor = conn.cursor()

        # Query to retrieve all mgrs_set_id and their bursts
        query = "SELECT mgrs_set_id, bursts, mgrs_tiles FROM mgrs_burst_db WHERE land_ocean_flag <> 'water'"
        cursor.execute(query)
        mgrs_data = cursor.fetchall()

        # Initialize DataFrame to store results
        df = pd.DataFrame(columns=['MGRS Set ID', 'Coverage Percentage', 'Covered RTC Native IDs', 'Covered RTC Burst IDs', 'Total RTC Burst IDs', 'Covered RTC Burst ID Count', 'Total RTC Burst IDs Count', 'MGRS Tiles', 'MGRS Tiles Count', 'RTC Burst ID Dates'])

        # Initialize a list to store data for DataFrame
        data_for_df = []

        # Iterate through each mgrs_set_id and calculate coverage, also update a progress bar
        print()
        for mgrs_set_id, bursts_string, mgrs_tiles_string in tqdm.tqdm(mgrs_data, desc="Calculating coverage"):

            # Main logic for coverage calculation:
            # 1. Identify RTC burst IDs we want to check (i.e. bursts_list)
            # 2. For each MGRS Set ID (i.e. mgrs_set_id), find the matching intersection (i.e. match_count) of RTC burst IDs (i.e. bursts_list) that map to the tile's burst IDs (i.e. burst_ids)
            # 3. Return the percentage of matches compared to the total number of bursts associated with the MGRS Tile Set ID (i.e. mgrs_set_id)
            bursts_list = bursts_string.strip("[]").replace("'", "").replace(" ", "").split(',')
            mgrs_tiles_list = mgrs_tiles_string.strip("[]").replace("'", "").replace(" ", "").split(',')
            matching_burst_ids = {}
            matching_burst_dates = {} 

            for burst in burst_ids:
                if burst in bursts_list:
                    matching_burst_ids[burst] = burst_ids[burst]
                    matching_burst_dates[burst] = burst_dates[burst]

            match_burst_count = len(matching_burst_ids)
            coverage_percentage = round((match_burst_count / len(bursts_list)) * 100, 2) if bursts_list else 0.0

            # Collect the db data we will need later
            data_for_df.append({
                'MGRS Set ID': mgrs_set_id,
                'Coverage Percentage': coverage_percentage,
                'Covered RTC Native IDs': ', '.join(list(matching_burst_ids.values())),
                'Covered RTC Burst IDs': ', '.join(list(matching_burst_ids.keys())),
                'Total RTC Burst IDs': ', '.join(bursts_list),
                'Covered RTC Burst ID Count': len(matching_burst_ids),
                'Total RTC Burst IDs Count': len(bursts_list),
                'MGRS Tiles': ', '.join(mgrs_tiles_list),
                'MGRS Tiles Count': len(mgrs_tiles_list),
                'RTC Burst ID Dates': [pd.to_datetime(date, format='%Y%m%dT%H%M%SZ') for date in matching_burst_dates.values()],
                'Unprocessed RTC Native IDs': '',
                'Unprocessed RTC Native IDs Count': 0
            })

        # Close the database connection safely
        conn.close()

        # Create DataFrame from the collected data to use for fancy stuff
        df = pd.DataFrame(data_for_df)

        # Apply threshold filtering if provided or use a minimum burst count match for filtering if provided. This is the place for more fancy logic if needed.
        if args.threshold:
            threshold = float(args.threshold)
            df = df[df['Coverage Percentage'] >= threshold]
        elif args.matching_burst_count:
            matching_burst_count = int(args.matching_burst_count)
            df = df[df['Covered RTC Burst ID Count'] >= matching_burst_count]

        # Pretty print results - adjust tablefmt accordingly (https://github.com/astanin/python-tabulate#table-format)
        print()

        if args.validate and len(df) > 0:
            burst_dates_series = df['RTC Burst ID Dates'].explode()
            smallest_date = burst_dates_series.min()
            greatest_date = burst_dates_series.max()

            print()
            print(f"Expected DSWx-S1 product sensing time range: {smallest_date} to {greatest_date}")

            validated_df = validate_dswx_s1(smallest_date, greatest_date, args.endpoint_daac_output, df)

            print()
            if len(validated_df) == 0:
                if (args.verbose):
                    print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count', 'Unprocessed RTC Native IDs Count', 'Covered RTC Native IDs', 'Unprocessed RTC Native IDs', 'MGRS Tiles']], headers='keys', tablefmt='plain', showindex=False))
                else:
                    print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count', 'Unprocessed RTC Native IDs Count']], headers='keys', tablefmt='plain', showindex=False))
                print()
                print(f"✅ Validation successful: All DSWx-S1 products ({df['MGRS Tiles Count'].sum()}) available at CMR for corresponding matched input RTC bursts within sensing time range.")

            else:
                print(f"Incomplete MGRS Set IDs ({len(validated_df)}) out of total MGRS Set IDs expected ({len(df)}) and expected DSWx-S1 products ({df['MGRS Tiles Count'].sum()})")
                if (args.verbose):
                    print(tabulate(validated_df[['MGRS Set ID','Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count', 'Unprocessed RTC Native IDs Count', 'Covered RTC Native IDs', 'Unprocessed RTC Native IDs', 'MGRS Tiles']], headers='keys', tablefmt='plain', showindex=False))
                else:
                    print(tabulate(validated_df[['MGRS Set ID','Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count', 'Unprocessed RTC Native IDs Count']], headers='keys', tablefmt='plain', showindex=False))
                print()
                print(f"❌ Validation failed: Mismatch in DSWx-S1 products available at CMR for corresponding matched input RTC bursts within sensing time range.")

        else:
            if (args.verbose):
                print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count', 'Covered RTC Native IDs', 'MGRS Tiles']], headers='keys', tablefmt='plain', showindex=False))
            else:
                print(tabulate(df[['MGRS Set ID', 'Coverage Percentage', 'Total RTC Burst IDs Count', 'Covered RTC Burst ID Count']], headers='keys', tablefmt='plain', showindex=False))
            print(f"Expected DSWx-S1 products: {df['MGRS Tiles Count'].sum()}, MGRS Set IDs covered: {len(df)}")
    elif (args.product == 'DISP-S1'):
        # Gather list of bursts and dates for CSLC sening time range
        burst_ids, burst_dates  = get_burst_ids_and_sensing_times_from_query(start=args.start, end=args.end, endpoint='OPS',  timestamp=args.timestamp, shortname='OPERA_L2_CSLC-S1_V1')

        # Process the disp s1 consistent database file
        frames_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()

        # Generate a table that has frames, all bursts, and matching bursts listed
        df = map_cslc_bursts_to_frames(burst_ids=burst_ids.keys(), bursts_to_frames = burst_to_frames, frames_to_bursts=frames_to_bursts)

        print(df)

        print(burst_dates)
        smallest_date = None
        greatest_date = None

        validate_disp_s1(smallest_date, greatest_date, args.endpoint_daac_output, df)

        # Filter to only those frames that have full coverage (i.e. all bursts == matching)
        df = df[df['All Possible Bursts Count'] == df['Matching Bursts Count']]

        if (args.verbose):
            print(tabulate(df[['Frame ID','All Possible Bursts', 'Matching Bursts']], headers='keys', tablefmt='plain', showindex=False))
        else:
            print(tabulate(df[['Frame ID','All Possible Bursts Count', 'Matching Bursts Count']], headers='keys', tablefmt='plain', showindex=False))

    else:
        logging.error(f"Arguments for for --product '{args.product}' missing or not invalid.")