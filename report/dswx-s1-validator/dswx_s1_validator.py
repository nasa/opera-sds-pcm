import argparse
import concurrent.futures
import multiprocessing
import random
import re
import sqlite3
import time
import sys
import requests

from cmr import GranuleQuery
from requests import get, exceptions
import pandas as pd
from tabulate import tabulate
import tqdm
from urllib.parse import urlparse, parse_qs, urlencode
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# NOTE: This should be contributed to https://github.com/nasa/python_cmr to be included as part of the library
def get_custom(url, params):
    """
    Get results for a specific page with a defined page size.

    :url: Base url to query cmr
    :params: The parameter arguments for the given url
    :returns: Query results as a dict (json) object
    """

    if ('page_num' not in params):
        params['page_num'] = 1
    if ('page_size' not in params):
        params['page_size'] = 2000

    try:
        response = get(url, params=params)
        logging.debug(response.url)        

        response.raise_for_status()
    except Exception as e:
        print(f"Error detected: {e}")
        raise RuntimeError(e)

    # Extract results based on JSON format
    return response.json()

def fetch_with_backoff(url, params):
    """
    Fetch a batch of granules with exponential backoff and jitter.

    :url: Base url to query cmr
    :params: The parameter arguments for the given url
    :returns: Batch of granules (json/dict objects)
    """
    base_delay = 1  # seconds
    max_delay = 60  # seconds
    attempts = 0

    while True:
        try:
            # Attempt to fetch granules
            response = get_custom(url, params)
            batch_granules = response['items']
            return batch_granules
        except Exception as e:
            # Exponential backoff with jitter
            attempts += 1
            delay = min(max_delay, base_delay * 2 ** attempts)
            jitter = random.uniform(0, delay / 2)
            time.sleep(delay + jitter)
            print(f"Retrying page {params.get('page_num')} after delay of {delay + jitter} seconds due to error: {e}")

def parallel_fetch(url, params, page_num, page_size, downloaded_batches, total_batches):
    """
    Fetches granules in parallel using the provided API.

    :url: Base url to query cmr
    :params: The parameter arguments for the given url
    :page_num (int): The page number of the granule query.
    :page_size (int): The number of granules to fetch per page.
    :downloaded_batches (multiprocessing.Value): A shared integer value representing
        the number of batches that have been successfully downloaded.
    :total_batches (int): The total number of batches to be downloaded.

    :returns (list): A list of batch granules fetched from the API.
    """

    params['page_num'] = page_num
    params['page_size'] = page_size

    try:
        logging.debug(f"Fetching {url} with {params}")
        batch_granules = fetch_with_backoff(url, params)
        logging.debug(f"Fetch success: {len(batch_granules)} batch granules downloaded.")
    except Exception as e:
        logging.error(f"Failed to fetch granules for page {page_num}: {e}")
        batch_granules = []
    finally:
        with downloaded_batches.get_lock():  # Safely increment the count
            downloaded_batches.value += 1
        return batch_granules

def get_burst_id(granule_id):
    """
    Extracts the burst ID from a given granule ID string.

    :granule_id (str): The granule ID from which to extract the burst ID.
    :returns (str): The extracted burst ID, or an empty string if not found.
    """
    burst_id = ''
    if granule_id:
      match = re.search(r'_T(\d+)-(\d+)-([A-Z]+\d+)_\d+T\d+Z_\d+T\d+Z_S1[AB]_\d+_v\d+\.\d+', granule_id)
      if match:
          t_number = match.group(1)
          orbit_number = match.group(2)
          iw_number = match.group(3).lower()
          burst_id = f't{t_number}_{orbit_number}_{iw_number}'

    return burst_id

def get_burst_sensing_datetime(granule_id):
    """
    Extracts the burst sensing date-time from a given granule ID string.

    :granule_id (str): The granule ID from which to extract the burst ID.
    :returns (str): The extracted burst sensing date-time, or an empty string if not found.
    """
    burst_date = ''
    if granule_id:
      match = re.search(r'_T\d+-\d+-[A-Z]+\d+_(\d+T\d+Z)_\d+T\d+Z_S1[AB]_\d+_v\d+\.\d+', granule_id)
      if match:
          burst_date = match.group(1)

    return burst_date

def get_total_granules(url, params, retries=5, backoff_factor=1):
    """
    Attempts to get the total number of granules with retry and exponential backoff.

    :url: Base url to query cmr
    :params: The parameter arguments for the given url
    :retries: Number of retry attempts.
    :backoff_factor: Factor to determine the next sleep time.
    :return: Total number of granules.
    """
    params['page_size'] = 0

    # url = construct_query_url(url, params)
    for attempt in range(retries):
        try:
            response = get_custom(url, params)
            return response['hits']
        except RuntimeError as e:
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)
            else:
                raise RuntimeError("Failed to get total granules after several attempts.")



def get_burst_ids_from_file(filename):

    burst_ids = {}
    burst_dates = {}
    with open(filename, 'r') as file:
        granule_ids = [line.strip() for line in file.readlines()]
        for granule_id in granule_ids:
            burst_id = get_burst_id(granule_id)
            burst_date = get_burst_sensing_datetime(granule_id)
            if (burst_id and burst_date):
                burst_ids[burst_id] = granule_id
                burst_dates[burst_id] = burst_date
            else:
                print(f"\nWarning: Could not extract burst information from malformed granule ID {granule_id}.")

    return burst_ids, burst_dates

def generate_url_params(start, end, endpoint = 'OPS', provider = 'ASF', short_name = 'OPERA_L2_RTC-S1_V1', window_length_days = 30, timestamp_type = 'temporal'):
    # Ensure start and end times are provided
    if not start or not end:
        raise ValueError("Start and end times are required if no file input is provided.")

    # Base URL for granule searches
    base_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    if endpoint == 'UAT':
        base_url = "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"
    params = {
        'provider': provider,
        'ShortName[]': short_name
    }

    # Set CMR param to ignore granule searches prior to a certain date
    start_datetime = datetime.fromisoformat(start)
    temporal_start_datetime = start_datetime - timedelta(days=window_length_days) # 30 days by default design - check with PCM team
    params['temporal'] = f"{temporal_start_datetime.isoformat()}"

    # Set time query type for CMR
    if timestamp_type.lower() == "production":
        params['production_date'] = f"{start},{end}"
    elif timestamp_type.lower() == "revision":
        params['revision_date'] = f"{start},{end}"
    elif timestamp_type.lower() == "created": 
        params['created_at'] = f"{start},{end}"
    else: # default time query type if not provided or set to temporal
        params['temporal'] = f"{start},{end}"

    return base_url, params

def get_burst_ids_from_query(start, end, timestamp, endpoint):

    burst_ids = {}
    burst_dates = {}

    base_url, params = generate_url_params(start=start, end=end, timestamp_type=timestamp, endpoint=endpoint)

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
    num_workers = min(5, (total_granules + page_size - 1) // page_size)

    # Initialize progress bar
    tqdm.tqdm._instances.clear()  # Clear any existing tqdm instances
    print()

    # Main loop to fetch granules, update progress bar, and extract burst_ids
    with tqdm.tqdm(total=total_granules, desc="Fetching granules", position=0) as pbar_global:
        downloaded_batches = multiprocessing.Value('i', 0)  # For counting downloaded batches
        total_batches = (total_granules + page_size - 1) // page_size

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # futures = [executor.submit(parallel_fetch, base_url, params, page_num, page_size, downloaded_batches, total_batches) for page_num in range(1, total_batches + 1)]
            futures = []
            for page_num in range(1, total_batches + 1):
                future = executor.submit(parallel_fetch, base_url, params, page_num, page_size, downloaded_batches, total_batches)
                futures.append(future)
                random_delay = random.uniform(0, 0.1)
                time.sleep(random_delay) # Stagger the submission of function calls for CMR optimization
                logging.debug(f"Scheduled granule fetch for batch {page_num}")

            for future in concurrent.futures.as_completed(futures):
                granules = future.result()
                pbar_global.update(len(granules))

                # RegEx for extracting burst IDs from granule IDs
                pattern = r'_T(\d+)-(\d+)-([A-Z]+\d+)_\d+T\d+Z_\d+T\d+Z_S1A_\d+_v\d+\.\d+'
                for granule in granules:
                    granule_id = granule.get("umm").get("GranuleUR")
                    burst_id = get_burst_id(granule_id)
                    burst_date = get_burst_sensing_datetime(granule_id)
                    if (burst_id and burst_date):
                        burst_ids[burst_id] = granule_id
                        burst_dates[burst_id] = burst_date
                    else:
                        print(f"\nWarning: Could not extract burst ID from malformed granule ID {granule_id}.")
    print("\nGranule fetching complete.")

    # Integrity check for total granules
    total_downloaded = sum(len(future.result()) for future in futures)
    if total_downloaded != total_granules:
        print(f"\nError: Expected {total_granules} granules, but downloaded {total_downloaded}. Try running again after some delay.")
        sys.exit(1)
    
    return burst_ids, burst_dates

def validate_mgrs_tiles(smallest_date, greatest_date, unique_mgrs_tiles, endpoint='UAT'):
    """
    Validates that the MGRS tiles from the CMR query match the provided unique MGRS tiles list.

    :param smallest_date: The earliest date in the range (ISO 8601 format).
    :param greatest_date: The latest date in the range (ISO 8601 format).
    :param unique_mgrs_tiles: List of unique MGRS tiles to validate against.
    :param endpoint: CMR environment ('UAT' or 'OPS').
    :return: Boolean indicating if the validation was successful.
    """

    # Convert Timestamps to strings in ISO 8601 format
    smallest_date_iso = smallest_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    greatest_date_iso = greatest_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    # Add 'T' prefix to MGRS tiles if not already present
    formatted_mgrs_tiles = [f"T{tile}" if not tile.startswith('T') else tile for tile in unique_mgrs_tiles]

    # Generate the base URL and parameters for the CMR query
    base_url, params = generate_url_params(
        start=smallest_date_iso,
        end=greatest_date_iso,
        endpoint=endpoint,
        provider='',  # or the appropriate provider
        short_name='OPERA_L3_DSWX-S1_PROVISIONAL_V0',  # Use the specific product short name
        timestamp_type='temporal'  # Ensure this matches the query requirements
    )

    # Update the params dictionary directly to include any specific parameters needed
    params['page_size'] = 1000  # Ensuring to fetch enough data in one go

    # Construct the full URL for the request
    full_url = f"{base_url}?{urlencode(params)}"

    # Make the HTTP request
    try:
        response = requests.get(full_url)
        response.raise_for_status()  # Raises a HTTPError for bad responses
        granules = response.json()

        # Extract MGRS tiles from the response
        retrieved_tiles = []
        for item in granules['items']:
            for attribute in item['umm']['AdditionalAttributes']:
                if 'AdditionalAttributes' in item['umm'] and attribute['Name'] == 'MGRS_TILE_ID':
                    retrieved_tiles.append(attribute['Values'][0])

        # Validate against provided unique MGRS tiles
        retrieved_tiles_set = set(retrieved_tiles)
        unique_mgrs_tiles_set = set(formatted_mgrs_tiles)

        if retrieved_tiles_set == unique_mgrs_tiles_set:
            print(f"Validation successful: All DSWx-S1 tiles available at CMR for input RTC sensing time range: {smallest_date_iso} to {greatest_date_iso}.")
            return True
        else:
            print(f"Validation failed: Mismatch in DSWx-S1 tiles available at CMR for input RTC sensing time range: {smallest_date_iso} to {greatest_date_iso}.")
            print(f"Expected({len(unique_mgrs_tiles_set)}): {unique_mgrs_tiles_set}")
            print(f"Received({len(retrieved_tiles_set)}): {retrieved_tiles_set}")
            return False

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from CMR: {e}")
        return False

if __name__ == '__main__':
    # Create an argument parser
    parser = argparse.ArgumentParser(description="CMR Query with Temporal Range and SQLite DB Access")
    parser.add_argument("--timestamp", required=False, default='TEMPORAL', metavar="TEMPORAL|REVISION|PRODUCTION|CREATED",  help="Use temporal, revision, or production time in start / end time granule query to CMR. Ex. --timestamp revision")
    parser.add_argument("--start", required=False, help="Temporal start time (ISO 8601 format)")
    parser.add_argument("--end", required=False, help="Temporal end time (ISO 8601 format)")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file")
    parser.add_argument("--file", required=False, help="Optional file path containing granule IDs")
    parser.add_argument("--threshold", required=False, help="Completion threshold minimum to filter results by (percentage format - leave out the % sign)")
    parser.add_argument("--matching_burst_count", required=False, help="Matching burst count to filter results by. Typically four or more is advised. Using this with the --threshold flag makes this flag inactive (only one of '--threshold' or '--matching_burst_count' may be used)")
    parser.add_argument("--verbose", action='store_true', help="Verbose and detailed output")
    parser.add_argument("--endpoint", required=False, choices=['UAT', 'OPS'], default='OPS', help='CMR endpoint venue')

    # Parse the command-line arguments
    args = parser.parse_args()

    burst_ids = {}
    burst_dates = {}

    # Check if file input is provided, otherwise use CMR API to get burst IDs
    if args.file:
        burst_ids, burst_dates = get_burst_ids_from_file(filename=args.file)
    else:
        burst_ids, burst_dates = get_burst_ids_from_query(args.start, args.end, args.timestamp, args.endpoint)

    # Connect to the MGRS Tile Set SQLITE database
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    # Query to retrieve all mgrs_set_id and their bursts
    query = "SELECT mgrs_set_id, bursts, mgrs_tiles FROM mgrs_burst_db WHERE land_ocean_flag <> 'water'"
    cursor.execute(query)
    mgrs_data = cursor.fetchall()

    # Initialize DataFrame to store results
    df = pd.DataFrame(columns=['MGRS Set ID', 'Coverage Percentage', 'Matching Granules', 'Matching Bursts', 'Total Bursts', 'Matching Burst Count', 'Total Burst Count', 'MGRS Tiles', 'MGRS Tiles Count', 'Burst Dates'])

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

        # Logic to set the DSWx-S1 sensor datetime: pick the middle sorted value from the list of input RTC burst sensor date times
        # burst_sorted_dates = sorted(matching_burst_dates.values())
        # if len(burst_sorted_dates) > 0:
        #     #burst_date_middle_value = len(burst_sorted_dates) // 2
        #     #dswx_s1_sensor_datetime = burst_sorted_dates[burst_date_middle_value] 

        #     dswx_s1_sensor_startdate = burst_sorted_dates[0]
        #     dswx_s1_sensor_enddate = burst_sorted_dates[-1]
        # else:
        #     print(f"\nError: Expected RTC bursts to be greater than zero.")
        #     sys.exit(1)

        # Collect the db data we will need later
        data_for_df.append({
            'MGRS Set ID': mgrs_set_id,
            'Coverage Percentage': coverage_percentage,
            'Matching Granules': ', '.join(list(matching_burst_ids.values())),
            'Matching Bursts': ', '.join(list(matching_burst_ids.keys())),
            'Total Bursts': ', '.join(bursts_list),
            'Matching Burst Count': len(matching_burst_ids),
            'Total Burst Count': len(bursts_list),
            'MGRS Tiles': ', '.join(mgrs_tiles_list),
            'MGRS Tiles Count': len(mgrs_tiles_list),
            #'DSWx-S1 Granule ID Signature': [f"OPERA_L3_DSWx-S1_T{mgrs_tile}_{dswx_s1_sensor_datetime}_*_S1A_30_v0.4" for mgrs_tile in mgrs_tiles_list],
            'Burst Dates': [pd.to_datetime(date, format='%Y%m%dT%H%M%SZ') for date in matching_burst_dates.values()]
        })

        logging.debug(f"len(matching_burst_dates) = {matching_burst_dates}")

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
        df = df[df['Matching Burst Count'] >= matching_burst_count]

    # Pretty print results - adjust tablefmt accordingly (https://github.com/astanin/python-tabulate#table-format)
    print()
    print('MGRS Set IDs covered:', len(df))
    if (args.verbose):
        print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Matching Granules', 'Matching Bursts', 'Matching Burst Count', 'Total Burst Count', 'MGRS Tiles', 'MGRS Tiles Count', 'Burst Dates']], headers='keys', tablefmt='plain', showindex=False))
    else:
        print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Matching Burst Count', 'Total Burst Count', 'MGRS Tiles']], headers='keys', tablefmt='plain', showindex=False))

    # Assuming 'df' is your DataFrame already containing datetime objects in 'Burst Dates'
    # First, you might need to explode the 'Burst Dates' column if it contains lists of dates
    burst_dates_series = df['Burst Dates'].explode()

    # Now find the smallest and greatest date
    smallest_date = burst_dates_series.min()
    greatest_date = burst_dates_series.max()

    # Output the results
    print()

    mgrs_tiles_series = df['MGRS Tiles'].str.split(', ').explode()
    unique_mgrs_tiles = mgrs_tiles_series.unique()

    # Example usage:
    #smallest_date = '2024-05-12T09:32:35.000'
    #greatest_date = '2024-05-12T09:33:18.000'
    #unique_mgrs_tiles = ["T20HNF", "T20HMF", "T20HLF", "T20HKF", "T19HGA", "T20HME", "T20HNE", "T20HLE", "T19HGV", "T20HKE", "T20HMD", "T20HLD", "T20HKD", "T19HGU"]
    validate_mgrs_tiles(smallest_date, greatest_date, unique_mgrs_tiles)