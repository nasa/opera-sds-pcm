import argparse
import concurrent.futures
import multiprocessing
import random
import re
import sqlite3
import time
import sys

from cmr import GranuleQuery
from requests import get, exceptions
import pandas as pd
from tabulate import tabulate
import tqdm

# NOTE: This should be contributed to https://github.com/nasa/python_cmr to be included as part of the library
def get_custom(self, page_num=1, page_size=2000):
    """
    Get results for a specific page with a defined page size.

    :page_num: The page number to retrieve.
    :page_size: The number of results per page.
    :returns: query results as a list
    """

    url = self._build_url()
    if self.headers is None:
        self.headers = {}

    params = {
        'page_size': page_size,
        'page_num': page_num
    }
    if 'cmr-search-after' in self.headers:
        params['cmr-search-after'] = self.headers['cmr-search-after']

    # Make an HTTP request for the specified page
    response = get(url, headers=self.headers, params=params)

    try:
        response.raise_for_status()
    except exceptions.HTTPError as ex:
        raise RuntimeError(ex.response.text)

    # Extract results based on JSON format
    if self._format == "json":
        results = response.json()['feed']['entry']
    else:
        results = [response.text]

    # Reset the 'cmr-search-after' header
    if 'cmr-search-after' in self.headers:
        del self.headers['cmr-search-after']

    return results

def fetch_with_backoff(api, page_num, page_size):
    """
    Fetch a batch of granules with exponential backoff and jitter.

    :api: CMR API object
    :page_num: Page number to fetch
    :page_size: Size of each page
    :returns: Batch of granules
    """
    base_delay = 1  # seconds
    max_delay = 60  # seconds
    attempts = 0

    while True:
        try:
            # Attempt to fetch granules
            batch_granules = get_custom(api, page_num, page_size)
            return batch_granules
        except Exception as e:
            # Exponential backoff with jitter
            attempts += 1
            delay = min(max_delay, base_delay * 2 ** attempts)
            jitter = random.uniform(0, delay / 2)
            time.sleep(delay + jitter)
            print(f"Retrying page {page_num} after delay of {delay + jitter} seconds due to error: {e}")

def parallel_fetch(api, page_num, page_size, downloaded_batches, total_batches):
    """
    Fetches granules in parallel using the provided API.

    Args:
        api (GranuleQuery): An instance of GranuleQuery used to fetch granules.
        page_num (int): The page number of the granule query.
        page_size (int): The number of granules to fetch per page.
        downloaded_batches (multiprocessing.Value): A shared integer value representing
            the number of batches that have been successfully downloaded.
        total_batches (int): The total number of batches to be downloaded.

    Returns:
        list: A list of batch granules fetched from the API.
    """
    try:
        batch_granules = fetch_with_backoff(api, page_num, page_size)
        return batch_granules
    finally:
        with downloaded_batches.get_lock():  # Safely increment the count
            downloaded_batches.value += 1

def get_burst_id(granule_id):
    """
    Extracts the burst ID from a given granule ID string.

    Args:
        granule_id (str): The granule ID from which to extract the burst ID.

    Returns:
        str: The extracted burst ID, or an empty string if not found.
    """
    burst_id = ''
    if granule_id:
      match = re.search(r'_T(\d+)-(\d+)-([A-Z]+\d+)_\d+T\d+Z_\d+T\d+Z_S1A_\d+_v\d+\.\d+', granule_id)
      if match:
          t_number = match.group(1)
          orbit_number = match.group(2)
          iw_number = match.group(3).lower()
          burst_id = f't{t_number}_{orbit_number}_{iw_number}'

    return burst_id

def get_total_granules(api, retries=5, backoff_factor=1):
    """
    Attempts to get the total number of granules with retry and exponential backoff.

    :param api: CMR API object
    :param retries: Number of retry attempts
    :param backoff_factor: Factor to determine the next sleep time
    :return: Total number of granules
    """
    for attempt in range(retries):
        try:
            return api.short_name("OPERA_L2_RTC-S1_V1").hits()
        except exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            time.sleep(backoff_factor * (2 ** attempt))
    raise RuntimeError("Failed to get total granules after several attempts.")



if __name__ == '__main__':
    # Create an argument parser
    parser = argparse.ArgumentParser(description="CMR Query with Temporal Range and SQLite DB Access")
    parser.add_argument("--start", required=False, help="Temporal start time (ISO 8601 format)")
    parser.add_argument("--end", required=False, help="Temporal end time (ISO 8601 format)")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file")
    parser.add_argument("--file", required=False, help="Optional file path containing granule IDs")
    parser.add_argument("--threshold", required=False, help="Completion threshold minimum to filter results by (percentage format - leave out the %)")
    parser.add_argument("--verbose", action='store_true', help="Verbose and detailed output")

    # Parse the command-line arguments
    args = parser.parse_args()

    burst_ids = {}

    # Check if file input is provided, otherwise use CMR API to get burst IDs
    if args.file:
        with open(args.file, 'r') as file:
            granule_ids = [line.strip() for line in file.readlines()]
            for granule_id in granule_ids:
              burst_id = get_burst_id(granule_id)
              if (burst_id):
                  burst_ids[burst_id] = granule_id
              else:
                  print(f"\nWarning: Could not extract burst ID from malformed granule ID {granule_id}.")
    else:
        # Ensure start and end times are provided
        if not args.start or not args.end:
            raise ValueError("Start and end times are required if no file input is provided.")

        # Initialize the CMR API
        api = GranuleQuery()

        # Query for granules with the specified temporal range and collection short name
        api.temporal(args.start, args.end)
        print(f"Querying CMR for time range {args.start} to {args.end}.")
        total_granules = get_total_granules(api)
        print(f"Querying CMR for {total_granules} granules.")

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
                futures = [executor.submit(parallel_fetch, api, page_num, page_size, downloaded_batches, total_batches) for page_num in range(1, total_batches + 1)]

                for future in concurrent.futures.as_completed(futures):
                    granules = future.result()
                    pbar_global.update(len(granules))

                    # RegEx for extracting burst IDs from granule IDs
                    pattern = r'_T(\d+)-(\d+)-([A-Z]+\d+)_\d+T\d+Z_\d+T\d+Z_S1A_\d+_v\d+\.\d+'
                    for granule in granules:
                        granule_id = granule.get("producer_granule_id")
                        burst_id = get_burst_id(granule_id)
                        if (burst_id):
                            burst_ids[burst_id] = granule_id
                        else:
                            print(f"\nWarning: Could not extract burst ID from malformed granule ID {granule_id}.")
        print("\nGranule fetching complete.")

        # Integrity check for total granules
        total_downloaded = sum(len(future.result()) for future in futures)
        if total_downloaded != total_granules:
            print(f"\nWarning: Expected {total_granules} granules, but downloaded {total_downloaded}.")

    # Connect to the MGRS Tile Set SQLITE database
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    # Query to retrieve all mgrs_set_id and their bursts
    query = "SELECT mgrs_set_id, bursts FROM mgrs_burst_db WHERE land_ocean_flag <> 'water'"
    cursor.execute(query)
    mgrs_data = cursor.fetchall()

    # Initialize DataFrame to store results
    df = pd.DataFrame(columns=['MGRS Set ID', 'Coverage Percentage', 'Matching Granules', 'Matching Bursts', 'Total Bursts', 'Matching Burst Count', 'Total Burst Count'])

    # Initialize a list to store data for DataFrame
    data_for_df = []

    # Iterate through each mgrs_set_id and calculate coverage, also update a progress bar
    print()
    for mgrs_set_id, bursts_string in tqdm.tqdm(mgrs_data, desc="Calculating coverage"):

        # Main logic for coverage calculation:
        # 1. Identify RTC burst IDs we want to check (i.e. bursts_list)
        # 2. For each MGRS Set ID (i.e. mgrs_set_id), find the matching intersection (i.e. match_count) of RTC burst IDs (i.e. bursts_list) that map to the tile's burst IDs (i.e. burst_ids)
        # 3. Return the percentage of matches compared to the total number of bursts associated with the MGRS Tile Set ID (i.e. mgrs_set_id)
        bursts_list = bursts_string.strip("[]").replace("'", "").replace(" ", "").split(',')
        # matching_bursts = [burst for burst in bursts_list if burst in burst_ids.keys]
        matching_ids = {} 
        for burst in burst_ids:
            if burst in bursts_list:
                matching_ids[burst] = burst_ids[burst]

        match_count = len(matching_ids)
        coverage_percentage = round((match_count / len(bursts_list)) * 100, 2) if bursts_list else 0.0

        # Collect the db data we will need later
        data_for_df.append({
            'MGRS Set ID': mgrs_set_id,
            'Coverage Percentage': coverage_percentage,
            'Matching Granules': ', '.join(list(matching_ids.values())),
            'Matching Bursts': ', '.join(list(matching_ids.keys())),
            'Total Bursts': ', '.join(bursts_list),
            'Matching Burst Count': len(matching_ids),
            'Total Burst Count': len(bursts_list)
        })

    # Close the database connection safely
    conn.close()

    # Create DataFrame from the collected data to use for fancy stuff
    df = pd.DataFrame(data_for_df)

    # Apply threshold filtering if provided. This is the place for more fancy logic if needed.
    if args.threshold:
        threshold = float(args.threshold)
        df = df[df['Coverage Percentage'] >= threshold]

    # Pretty print results - adjust tablefmt accordingly (https://github.com/astanin/python-tabulate#table-format)
    print()
    print('MGRS Set IDs covered:', len(df))
    if (args.verbose):
        print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Matching Granules', 'Matching Bursts', 'Matching Burst Count', 'Total Burst Count']], headers='keys', tablefmt='plain', showindex=False))
    else:
        print(tabulate(df[['MGRS Set ID','Coverage Percentage', 'Matching Burst Count', 'Total Burst Count']], headers='keys', tablefmt='plain', showindex=False))
