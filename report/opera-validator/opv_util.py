import time
import random
import sys
import re
import concurrent.futures
import multiprocessing
import tqdm
import requests
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode
from requests import get

# Constants
BURST_AND_DATE_GRANULE_PATTERN = r'_T(\d+)-(\d+)-([A-Z]+\d+)_(\d+T\d+Z)_(\d+T\d+Z)'
CMR_GRANULES_API_ENDPOINT="https://cmr.earthdata.nasa.gov/search/granules.umm_json"
CMR_UAT_GRANULES_API_ENDPOINT="https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json"

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

def parallel_fetch(url, params, page_num, page_size, downloaded_batches):
    """
    Fetches granules in parallel using the provided API.

    :url: Base url to query cmr
    :params: The parameter arguments for the given url
    :page_num (int): The page number of the granule query.
    :page_size (int): The number of granules to fetch per page.
    :downloaded_batches (multiprocessing.Value): A shared integer value representing
        the number of batches that have been successfully downloaded.

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
def generate_url_params(start, end, endpoint = 'OPS', provider = 'ASF', short_name = 'OPERA_L2_RTC-S1_V1', window_length_days = 30, timestamp_type = 'temporal'):
    """
    Generates URL parameters for querying granules from CMR (Common Metadata Repository) based on provided criteria.

    This function constructs the base URL and parameters necessary for making granule search requests to the CMR API.
    It configures search parameters including the provider, the product short name, and a temporal filter that limits
    searches to a specific time range around the provided start and end dates. The temporal filter can be adjusted based
    on production, revision, or creation dates, or a default window surrounding the specified dates.

    :param start: The starting date-time for the temporal range (ISO 8601 format).
    :param end: The ending date-time for the temporal range (ISO 8601 format).
    :param endpoint: Optional; specifies the API endpoint ('OPS' for operational, 'UAT' for user acceptance testing). Defaults to 'OPS'.
    :param provider: Optional; specifies the data provider's ID. Defaults to 'ASF'.
    :param short_name: Optional; specifies the short name of the data product. Defaults to 'OPERA_L2_RTC-S1_V1'.
    :param window_length_days: Optional; sets the number of days before the start date to also include in searches. Defaults to 30 days.
    :param timestamp_type: Optional; determines the type of timestamp to use for filtering ('temporal', 'production', 'revision', 'created'). Defaults to 'temporal'.

    :return: A tuple containing the base URL and a dictionary of parameters for the granule search.
    """

    # Ensure start and end times are provided
    if not start or not end:
        raise ValueError("Start and end times are required if no file input is provided.")

    # Base URL for granule searches
    base_url = CMR_GRANULES_API_ENDPOINT
    if endpoint == 'UAT':
        base_url = CMR_UAT_GRANULES_API_ENDPOINT
    params = {
        'provider': provider,
        'ShortName[]': short_name
    }

    # Set CMR param to ignore granule searches prior to a certain date
    start_datetime = datetime.fromisoformat(start.replace("Z", "+00:00"))
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

def retrieve_r3_products(smallest_date, greatest_date, endpoint, shortname):

    # Convert timestamps to strings in ISO 8601 format
    smallest_date_iso = smallest_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    greatest_date_iso = greatest_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

    # Generate the base URL and parameters for the CMR query
    base_url, params = generate_url_params(
        start=smallest_date_iso,
        end=greatest_date_iso,
        endpoint=endpoint,
        provider='',  # leave blank
        short_name=shortname,  # Use the specific product short name
        timestamp_type='temporal'  # Ensure this matches the query requirements
    )

    # Update the params dictionary directly to include any specific parameters needed
    params['page_size'] = 1000  # Set the page size to 1000
    params['page_num'] = 1  # Start with the first page

    all_granules = []

    while True:
        # Construct the full URL for the request
        full_url = f"{base_url}?{urlencode(params)}"

        # Make the HTTP request
        response = requests.get(full_url)
        response.raise_for_status()  # Raises a HTTPError for bad responses
        granules = response.json()

        # Append the current page's granules to the all_granules list
        all_granules.extend(granules['items'])

        # Check if we've retrieved all pages
        if len(all_granules) >= granules['hits']:
            break

        # Increment the page number for the next iteration
        params['page_num'] += 1

    return all_granules

def get_burst_id(granule_id):
    """
    Extracts the burst ID from a given granule ID string.

    :granule_id (str): The granule ID from which to extract the burst ID.
    :returns (str): The extracted burst ID, or an empty string if not found.
    """
    burst_id = ''
    if granule_id:
      match = re.search(BURST_AND_DATE_GRANULE_PATTERN, granule_id)
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
      match = re.search(BURST_AND_DATE_GRANULE_PATTERN, granule_id)
      if match:
          burst_date = match.group(4)

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
    """
    Reads a file containing granule IDs and extracts both burst IDs and sensing date-times from them.

    This function opens a specified file and reads through each line, treating each line as a granule ID.
    For each granule ID, it attempts to extract a burst ID and the corresponding burst sensing datetime.
    If successful, these are stored in dictionaries mapping burst IDs to granule IDs and burst dates respectively.
    If the extraction fails (indicating malformed data), a warning is printed.

    :param filename: The path to the file containing the granule IDs.
    :return: A tuple of two dictionaries:
             1. burst_ids: Mapping of burst IDs to granule IDs.
             2. burst_dates: Mapping of burst IDs to their sensing date-times.
    """

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


def get_granules_from_query(start, end, timestamp, endpoint, provider='ASF', shortname='OPERA_L2_RTC-S1_V1'):
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

    base_url, params = generate_url_params(start=start, end=end, timestamp_type=timestamp, endpoint=endpoint,
                                           provider=provider, short_name=shortname)

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
                time.sleep(random_delay)  # Stagger the submission of function calls for CMR optimization
                logging.debug(f"Scheduled granule fetch for batch {page_num}")

            for future in concurrent.futures.as_completed(futures):
                granules_result = future.result()
                pbar_global.update(len(granules_result))

                granules.extend(granules_result)

    print("\nGranule fetching complete.")

    # Integrity check for total granules
    total_downloaded = sum(len(future.result()) for future in futures)
    if total_downloaded != total_granules:
        print(
            f"\nError: Expected {total_granules} granules, but downloaded {total_downloaded}. Try running again after some delay.")
        sys.exit(1)

    return granules