#!/usr/bin/env python3

"""
===================
stage_orbit_file.py
===================

Script to query and download the appropriate Orbit Ephemeris file for the time
range covered by an input SLC SAFE archive.

"""

import argparse
import os
import re
import requests

from datetime import datetime, timedelta
from os.path import abspath

import backoff

from commons.logger import logger
from commons.logger import LogLevels

DEFAULT_QUERY_ENDPOINT = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for the Copernicus Data Space Ecosystem (CDSE) query REST service"""

DEFAULT_AUTH_ENDPOINT = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
"""Default URL endpoint for performing user authentication with CDSE"""

DEFAULT_DOWNLOAD_ENDPOINT = 'https://zipper.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for CDSE download REST service"""

ORBIT_TYPE_POE = 'POEORB'
"""Orbit type identifier for Precise Orbit Ephemeris"""

ORBIT_TYPE_RES = 'RESORB'
"""Orbit type identifier for Restituted Orbit Ephemeris"""

VALID_ORBIT_TYPES = (ORBIT_TYPE_POE, ORBIT_TYPE_RES)
"""List of the valid orbit types that this script supports querying for"""

T_ORBIT = (12 * 86400.0) / 175.0
"""
Orbital period of Sentinel-1 in seconds: 12 days * 86400.0 seconds/day, 
divided into 175 orbits
"""

ORBIT_PAD = 60
"""Time padding to be applied to temporal queries"""

DEFAULT_SENSING_START_MARGIN = timedelta(seconds=T_ORBIT + ORBIT_PAD)
"""
Temporal margin to apply to the start time of a frame to make sure that the 
ascending node crossing is included when choosing the orbit file
"""

DEFAULT_SENSING_STOP_MARGIN = timedelta(seconds=ORBIT_PAD)
"""
Temporal margin to apply to the stop time of a frame to make sure that the 
ascending node crossing is included when choosing the orbit file
"""

class NoQueryResultsException(Exception):
    """Custom exception to identify empty results from a query"""
    pass


class NoSuitableOrbitFileException(Exception):
    """Custom exception to identify no orbit files meeting overlap criteria"""


def to_datetime(value):
    """Helper function to covert command-line arg to datetime object"""
    return datetime.strptime(value, "%Y%m%dT%H%M%S")


def get_parser():
    """Returns the command line parser for stage_orbit_file.py"""
    parser = argparse.ArgumentParser(
        description="Query and stage an Orbit Ephemeris file for use with an "
                    "SLC-based processing job. The appropriate Orbit file is "
                    "queried for based on the time range covered by an input SLC "
                    "swath. The swath time range is determined from the file name "
                    "of the desired SLC SAFE archive file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-o", "--output-directory", type=str, action='store',
                        default=abspath(os.curdir),
                        help="Specify the directory to store the output Orbit file. "
                             "Has no effect if --url-only is specified.")
    parser.add_argument("-O", "--orbit-type", type=str, action='store',
                        choices=VALID_ORBIT_TYPES, default=ORBIT_TYPE_RES,
                        help="Specify the type of orbit file to query for, either "
                             "Precise (POEORB) or Restituted (RESORB)."),
    parser.add_argument("-u", "--username", type=str, action='store', required=True,
                        help="Specify a user name to use with the query/download "
                             "requests. Must be provided for all requests.")
    parser.add_argument("-p", "--password", type=str, action='store', required=True,
                        help="Specify a password to use with the query/download "
                             "requests. Must be provided for all requests.")
    parser.add_argument("--url-only", action="store_true",
                        help="Only output the URL from where the resulting Orbit "
                             "file may be downloaded from.")
    parser.add_argument("--query-endpoint", type=str, action='store',
                        default=DEFAULT_QUERY_ENDPOINT, metavar='URL',
                        help="Specify the query service URL endpoint to which the "
                             "query itself will be appended.")
    parser.add_argument("--auth-endpoint", type=str, action='store',
                        default=DEFAULT_AUTH_ENDPOINT, metavar='URL',
                        help="Specify the authentication service URL endpoint "
                             "which will be used to obtain an authentication token "
                             "using the provided username and password.")
    parser.add_argument("--download-endpoint", type=str, action='store',
                        default=DEFAULT_DOWNLOAD_ENDPOINT, metavar='URL',
                        help="Specify the download service URL endpoint from which "
                             "the Orbit file will be obtained from. Has no effect when "
                             "--url-only is provided.")
    parser.add_argument("--sensing-start-range", type=str, action='store',
                        default=None, metavar='YYYYmmddTHHMMSS',
                        help="Datetime of the sensing range start time used to select "
                             "an overlapping orbit file. If not provided, the "
                             "sensing start time of the input SAFE file is used.")
    parser.add_argument("--sensing-stop-range", type=str, action='store',
                        default=None, metavar='YYYYmmddTHHMMSS',
                        help="Datetime of the sensing range stop time used to select "
                             "an overlapping orbit file. If not provided, the "
                             "sensing stop time of the input SAFE file is used.")
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")
    parser.add_argument("input_safe_file", type=str, action='store',
                        help="Name of the input SLC SAFE archive to obtain the "
                             "corresponding Orbit file for. This may be the file "
                             "name only, or a full/relative path to the file.")

    return parser


def parse_orbit_time_range_from_safe(input_safe_file):
    """
    Parses the time range covered by the input SLC SAFE file, so it can be used
    with the query for a corresponding Orbit file. The mission ID (S1A or S1B)
    is also parsed, since this also becomes part of the query.

    Parameters
    ----------
    input_safe_file : str
        Name of the SAFE file to parse. May be just the file name or a path to
        the file.

    Returns
    -------
    mission_id : str
        The mission ID parsed from the SAFE file name, should always be one
        of S1A or S1B.
    safe_start_time : str
        The start time parsed from the SAFE file name in YYYYmmddTHHMMSS format.
    safe_stop_time : str
        The stop time parsed from the SAFE file name in YYYYmmddTHHMMSS format.

    Raises
    ------
    RuntimeError
        If the provided SAFE file name cannot be parsed according to the expected
        file name conventions.

    """
    # Remove any path and extension info from the provided file name
    safe_filename = os.path.splitext(os.path.basename(input_safe_file))[0]

    logger.debug(f'input_safe_file: {input_safe_file}')
    logger.debug(f'safe_filename: {safe_filename}')

    # Parse the SAFE file name with the following regex, derived from the
    # official naming conventions, which can be referenced here:
    # https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar/naming-conventions
    safe_regex_pattern = (
        r"(?P<mission_id>S1A|S1B)_(?P<beam_mode>IW)_(?P<product_type>SLC)(?P<resolution>_)_"
        r"(?P<level>1)(?P<class>S)(?P<pol>SH|SV|DH|DV)_(?P<start_ts>\d{8}T\d{6})_"
        r"(?P<stop_ts>\d{8}T\d{6})_(?P<orbit_num>\d{6})_(?P<data_take_id>[0-9A-F]{6})_"
        r"(?P<product_id>[0-9A-F]{4})"
    )
    safe_regex = re.compile(safe_regex_pattern)
    match = safe_regex.match(safe_filename)

    if not match:
        raise RuntimeError(
            f'SAFE file name {safe_filename} does not conform to expected format'
        )

    # Extract the file name portions we care about
    mission_id = match.groupdict()['mission_id']
    safe_start_time = match.groupdict()['start_ts']
    safe_stop_time = match.groupdict()['stop_ts']

    logger.debug(f'mission_id: {mission_id}')
    logger.debug(f'safe_start_time: {safe_start_time}')
    logger.debug(f'safe_stop_time: {safe_stop_time}')

    return mission_id, safe_start_time, safe_stop_time


def construct_orbit_file_query(mission_id, orbit_type, search_start_time, search_stop_time):
    """
    Constructs the query used with the query endpoint URL to determine the
    available Orbit files for the given time range.

    Parameters
    ----------
    mission_id : str
        The mission ID parsed from the SAFE file name, should always be one
        of S1A or S1B.
    orbit_type : str
        String identifying the type of orbit file to query for. Should be either
        POEORB for Precise Orbit files, or RESORB for Restituted.
    search_start_time : str
        The start time to use with the query in YYYYmmddTHHMMSS format.
        Any resulting orbit files will have a starting time before this value.
    search_stop_time : str
        The stop time to use with the query in YYYYmmddTHHMMSS format.
        Any resulting orbit files will have an ending time after this value.

    Returns
    -------
    query : str
        The Orbit file query string formatted as the query service expects.

    """
    # Convert the start/stop time strings to datetime objects
    search_start_datetime = datetime.strptime(search_start_time, "%Y%m%dT%H%M%S")
    search_stop_datetime = datetime.strptime(search_stop_time, "%Y%m%dT%H%M%S")

    logger.debug(f'search_start_datetime: {search_start_datetime}')
    logger.debug(f'search_stop_datetime: {search_stop_datetime}')

    # Set up templates that use the OData domain specific syntax expected by the
    # query service
    query_template = (
        "startswith(Name,'{mission_id}') and contains(Name,'AUX_{orbit_type}') "
        "and ContentDate/Start lt '{start_time}' and ContentDate/End gt '{stop_time}'"
    )

    # Format the query template using the values we were provided
    query_start_date_str = search_start_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    query_stop_date_str = search_stop_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    query = query_template.format(start_time=query_start_date_str,
                                  stop_time=query_stop_date_str,
                                  mission_id=mission_id,
                                  orbit_type=orbit_type)

    logger.debug(f'query: {query}')

    return query

def fatal_code(err_code):
    """Only retry for common transient errors"""
    return err_code not in [429, 500, 503, 504]

def backoff_logger(details):
    """Log details about the current backoff/retry"""
    logger.warning(
        f"Backing off {details['target']} function for {details['wait']:0.1f} "
        f"seconds after {details['tries']} tries."
    )
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")

@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=fatal_code,
                      on_backoff=backoff_logger,
                      interval=15)
def query_orbit_file_service(endpoint_url, query):
    """
    Submits a request to the Orbit file query REST service, and returns the
    JSON-formatted response.

    Parameters
    ----------
    endpoint_url : str
        The URL for the query endpoint, to which the query is appended to as
        the payload.
    query : str
        The query for the Orbit files to find, filtered by a time range and mission
        ID corresponding to the provided SAFE SLC archive file.

    Returns
    -------
    query_results : list of dict
        The list of results from a successful query. Each result should
        be a Python dictionary containing the details of the orbit file which
        matched the query.

    Raises
    ------
    RuntimeError
        If the request fails for any reason (HTTP return code other than 200).
    NoQueryResultsException
        If the query request is successful, but returns no results for the provided
        parameters.

    """
    # Set up parameters to be included with query request
    query_params = {'$filter': query, '$orderby': 'ContentDate/Start asc', '$top': 1}

    # Make the HTTP GET request on the endpoint URL, no credentials are required
    response = requests.get(endpoint_url, params=query_params)

    logger.debug(f'response.url: {response.url}')
    logger.debug(f'response.status_code: {response.status_code}')

    response.raise_for_status()

    # Response should be within the text body as JSON
    json_response = response.json()

    logger.debug(f'json_response: {json_response}')

    query_results = json_response['value']

    # Check for empty list of results
    if not query_results:
        raise NoQueryResultsException('No results returned from parsed query results')

    return query_results


def select_orbit_file(query_results, safe_start_time, safe_stop_time):
    """
    Iterates over the results of an orbit file query, searching for the first
    valid orbit file to download. A valid orbit file is one whose validity
    time range fully envelops the validity time range of the corresponding
    SLC SAFE archive after the start/stop margins are applied to the SAFE
    start/stop times.

    Parameters
    ----------
    query_results : list of dict
        The list of results from a successful query. Each result should
        be a Python dictionary containing the details of the orbit file which
        matched the query.
    safe_start_time : str
        The start time parsed from the SAFE file name in YYYYmmddTHHMMSS format.
    safe_stop_time : str
        The stop time parsed from the SAFE file name in YYYYmmddTHHMMSS format.

    Raises
    ------
    NoSuitableOrbitFileException
        If no suitable orbit file can be found within the provided list of entries.

    Returns
    -------
    orbit_file_name : str
        Name of the selected orbit file.
    orbit_file_request_id : str
        Request ID used to perform the actual download request of the selected
        orbit file.

    """
    orbit_regex_pattern = (
        r'(?P<mission_id>S1A|S1B)_(?P<file_class>OPER)_(?P<category>AUX)_'
        r'(?P<semantic_desc>POEORB|RESORB)_(?P<site>OPOD)_'
        r'(?P<creation_ts>\d{8}T\d{6})_V(?P<valid_start_ts>\d{8}T\d{6})_'
        r'(?P<valid_stop_ts>\d{8}T\d{6})[.](?P<format>EOF)$'
    )
    orbit_regex = re.compile(orbit_regex_pattern)

    # Parse each result from the query, and look for a suitable orbit file
    # candidate among the results
    for query_result in query_results:
        try:
            # Get the request ID from the entry element, this is the primary
            # piece of info needed by the download service to acquire the Orbit file
            orbit_file_request_id = query_result['Id']

            # Get the name of the candidate orbit file, so we can check its time
            # range against the input SLC's
            orbit_file_name = query_result['Name']
        except KeyError as err:
            logger.warning(
                f'Could not parse expected filed from query result. Missing key: {str(err)}'
            )
            continue

        # Parse the validity time range from the orbit file name
        match = orbit_regex.match(orbit_file_name)

        if not match:
            logger.warning(
                f'Orbit file name {orbit_file_name} does not conform to expected format'
            )
            continue

        orbit_start_time = match.groupdict()['valid_start_ts']
        orbit_stop_time = match.groupdict()['valid_stop_ts']

        # Check that the validity time range of the orbit file fully envelops
        # the SAFE validity time range parsed earlier
        safe_start_datetime = datetime.strptime(safe_start_time, "%Y%m%dT%H%M%S")
        safe_stop_datetime = datetime.strptime(safe_stop_time, "%Y%m%dT%H%M%S")
        orbit_start_datetime = datetime.strptime(orbit_start_time, "%Y%m%dT%H%M%S")
        orbit_stop_datetime = datetime.strptime(orbit_stop_time, "%Y%m%dT%H%M%S")

        logger.info(f'Evaluating orbit file {orbit_file_name}')
        logger.debug(f'{safe_start_time=}')
        logger.debug(f'{safe_stop_time=}')
        logger.debug(f'{orbit_start_time=}')
        logger.debug(f'{orbit_stop_time=}')

        if orbit_start_datetime < safe_start_datetime and orbit_stop_datetime > safe_stop_datetime:
            logger.info(f'Orbit file is suitable for use')

            # Return the two pieces of info we need to download the file
            return orbit_file_name, orbit_file_request_id
        else:
            logger.info('Orbit file time range does not fully overlap sensing time range, skipping')
    # If here, there were no valid orbit file candidates returned from the query
    else:
        raise NoSuitableOrbitFileException(
            "No suitable orbit file could be found within the results of the query"
        )

@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=fatal_code,
                      on_backoff=backoff_logger,
                      interval=15)
def get_access_token(endpoint_url, username, password):
    """
    Acquires an access token from the CDSE authentication endpoint using the
    credentials provided by the user.

    Parameters
    ----------
    endpoint_url : str
        URL to the authentication endpoint to provide credentials to.
    username : str
        Username of the account to authenticate with.
    password : str
        Password of the account to authenticate with.

    Returns
    -------
    access_token : str
        The access token parsed from a successful authentication request.
        This token must be included with download requests for them to be valid.

    Raises
    ------
    RuntimeError
        If the authentication request fails, or an invalid response is returned
        from the service.

    """
    # Set up the payload to the authentication service
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }

    response = requests.post(endpoint_url, data=data,)
    response.raise_for_status()

    # Parse the access token from the response
    try:
        access_token = response.json()["access_token"]
    except KeyError:
        raise RuntimeError(
            'Failed to parsed expected field "access_token" from authentication response.'
        )

    return access_token


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=fatal_code,
                      on_backoff=backoff_logger,
                      interval=15)
def download_orbit_file(request_url, output_directory, orbit_file_name, access_token):
    """
    Downloads an Orbit file using the provided request URL, which should contain
    the product ID for the file to download, as obtained from a query result.

    The output file is named according to the orbit_file_name parameter, and
    should correspond to the file name parsed from the query result. The output
    file is written to the directory indicated by output_directory.

    Parameters
    ----------
    request_url : str
        The full request URL, which includes the download endpoint, as well as
        a payload that contains the product ID for the Orbit file to be downloaded.
    output_directory : str
        The directory to store the downloaded Orbit file to.
    orbit_file_name : str
        The file name to assign to the Orbit file once downloaded to disk. This
        should correspond to the file name parsed from a query result.
    access_token : str
        Access token returned from an authentication request with the provided
        username and password. Must be provided with all download requests for
        the download service to respond.

    Returns
    -------
    output_orbit_file_path : str
        The full path to where the resulting Orbit file was downloaded to.

    Raises
    ------
    RuntimeError
        If the request fails for any reason (HTTP return code other than 200).

    """
    # Make the HTTP GET request to obtain the Orbit file contents
    headers = {"Authorization": f"Bearer {access_token}"}
    session = requests.Session()
    session.headers.update(headers)
    response = session.get(request_url, headers=headers, stream=True)

    logger.debug(f'r.url: {response.url}')
    logger.debug(f'r.status_code: {response.status_code}')

    response.raise_for_status()

    # Write the contents to disk
    output_orbit_file_path = os.path.join(output_directory, orbit_file_name)

    with open(output_orbit_file_path, 'wb') as outfile:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                outfile.write(chunk)

    return output_orbit_file_path


def main(args):
    """
    Main script to execute Orbit file staging.

    Parameters
    ----------
    args: argparse.Namespace
        Arguments parsed from the command-line.

    """
    # Set the logging level
    if args.log_level:
        LogLevels.set_level(args.log_level)

    logger.info(f"Determining Orbit file for input SAFE file {args.input_safe_file}")

    # Parse the relevant info from the input SAFE filename
    (mission_id,
     safe_start_time,
     safe_stop_time) = parse_orbit_time_range_from_safe(args.input_safe_file)

    logger.info(f"Parsed time range {safe_start_time} - {safe_stop_time} from SAFE filename")

    # Determine the query time range
    search_start_time = args.sensing_start_range or safe_start_time
    search_stop_time = args.sensing_stop_range or safe_stop_time

    # Construct the query based on the time range parsed from the input file
    query = construct_orbit_file_query(
        mission_id, args.orbit_type, search_start_time, search_stop_time
    )

    # Make the query to determine what Orbit files are available for the time
    # range
    logger.info(f"Querying for Orbit file(s) from endpoint {args.query_endpoint}")

    query_results = query_orbit_file_service(args.query_endpoint, query)

    # Select an appropriate orbit file from the list returned from the query
    orbit_file_name, orbit_file_request_id = select_orbit_file(
        query_results, safe_start_time, safe_stop_time
    )

    # Obtain an access token for use with the download request from the provided
    # credentials
    access_token = get_access_token(args.auth_endpoint, args.username, args.password)

    # Construct the URL used to download the Orbit file
    download_url = f"{args.download_endpoint}({orbit_file_request_id})/$value"

    # If user request the URL only, print it to standard out and the log
    if args.url_only:
        logger.info('URL-only requested')
        logger.info(download_url)
        print(download_url)
    # Otherwise, download the Orbit file using the file name parsed from the
    # query result to the directory specified by the user
    else:
        logger.info(
            f"Downloading Orbit file {orbit_file_name} from service endpoint "
            f"{args.download_endpoint}"
        )
        output_orbit_file_path = download_orbit_file(
            download_url, args.output_directory, orbit_file_name, access_token
        )

        logger.info(f"Orbit file downloaded to {output_orbit_file_path}")


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
