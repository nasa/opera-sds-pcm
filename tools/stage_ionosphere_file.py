#!/usr/bin/env python3

"""
========================
stage_ionosphere_file.py
========================

Script to download the appropriate Ionosphere Correction file corresponding
to the start date of an input SLC SAFE archive.

"""

import argparse
import datetime
import netrc
import os
import re
import subprocess
import sys

from os.path import abspath, join

import requests

from commons.logger import logger
from commons.logger import LogLevels

DEFAULT_DOWNLOAD_ENDPOINT = "https://cddis.nasa.gov/archive/gnss/products/ionex"
"""Default URL endpoint for Ionosphere download requests"""

DEFAULT_EDL_ENDPOINT = "urs.earthdata.nasa.gov"
"""Default endpoint for authenticating with EarthData Login"""

IONOSPHERE_TYPE_JPLG = "jplg"
IONOSPHERE_TYPE_JPRG = "jprg"
VALID_IONOSPHERE_TYPES = [IONOSPHERE_TYPE_JPLG, IONOSPHERE_TYPE_JPRG]
"""The valid Ionosphere file types that this script can download"""

class IonosphereFileNotFoundException(Exception):
    """Exception to identify no result found (404) for a requested Ionosphere archive"""
    pass

def get_parser():
    """Returns the command line parser for stage_ionosphere_file.py"""
    parser = argparse.ArgumentParser(
        description="Downloads and stages an Ionosphere Correction file for use "
                    "with an SLC-based processing job. The appropriate Ionosphere "
                    "file is obtained based on the start date of the input SLC "
                    "archive. The start date is determined from the file name "
                    "of the desired SLC SAFE archive file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-o", "--output-directory", type=str, action='store',
                        default=abspath(os.curdir),
                        help="Specify the directory to store the downloaded file. "
                             "Has no effect if --url-only is specified.")
    parser.add_argument("-u", "--username", type=str, action='store',
                        default=None,
                        help="Specify the EarthData Login user name to use with "
                             "the download request. If a username is not provided, "
                             "it is obtained from the local .netrc file.")
    parser.add_argument("-p", "--password", type=str, action='store',
                        default=None,
                        help="Specify the EarthData Login password to use with "
                             "the download request. If a password is not provided, "
                             "it is obtained from the local .netrc file.")
    parser.add_argument("-t", "--type", type=str.lower, choices=VALID_IONOSPHERE_TYPES,
                        default=IONOSPHERE_TYPE_JPLG,
                        help=f"Specify the type of Ionosphere file to download. "
                             f"Must be one of {VALID_IONOSPHERE_TYPES}")
    parser.add_argument("--url-only", action="store_true",
                        help="Only output the URL from where the resulting Ionosphere "
                             "file may be downloaded from.")
    parser.add_argument("--download-endpoint", type=str, action='store',
                        default=DEFAULT_DOWNLOAD_ENDPOINT, metavar='URL',
                        help="Specify the download service URL endpoint from which "
                             "the Ionosphere file will be obtained from. Has no "
                             "effect when --url-only is provided.")
    parser.add_argument("--log-level",
                        type=lambda log_level: LogLevels[log_level].value,
                        choices=LogLevels.list(),
                        default=LogLevels.INFO.value,
                        help="Specify a logging verbosity level.")
    parser.add_argument("input_safe_file", type=str, action='store',
                        help="Name of the input SLC SAFE archive to obtain the "
                             "corresponding Ionosphere Correction file for. "
                             "This may be the file name only, or a full/relative "
                             "path to the file.")

    return parser

# TODO: this should land in a "login" utility module at some point
class SessionWithHeaderRedirection(requests.Session):
    """
    Class with an override of the requests.Session.rebuild_auth to maintain
    headers when redirected by EarthData Login.

    This code was adapted from the examples available here:
    https://urs.earthdata.nasa.gov/documentation/for_users/data_access/python

    """
    AUTH_HOST = DEFAULT_EDL_ENDPOINT

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)

            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
                del headers['Authorization']

        return


def parse_start_date_from_safe(input_safe_file):
    """
    Parses the start date from the name of an input SLC archive.

    Parameters
    ----------
    input_safe_file : str
        Path or name of an SLC archive to parse the start date from.

    Returns
    -------
    safe_start_date : str
        The start date parsed from the SLC filename in YYYYMMDD format.

    Raises
    ------
    RuntimeError
        If the provided SLC name does not conform to the expected format.

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

    safe_start_time = match.groupdict()['start_ts']

    logger.debug(f'safe_start_time: {safe_start_time}')

    safe_start_date = safe_start_time.split('T')[0]

    logger.debug(f'safe_start_date: {safe_start_date}')

    return safe_start_date


def safe_start_date_to_julian_day(safe_start_date):
    """
    Converts a start date parsed from an SLC file name to the corresponding
    year and day of year (aka Julian day).

    Parameters
    ----------
    safe_start_date : str
        Start date parsed from an SLC filename in YYYYMMDD format.

    Returns
    -------
    year : str
        The year parsed from the start date as a string.
    doy : str
        The Julian day of year parsed from the start date as a string.

    """
    date_format = "%Y%m%d"
    dt = datetime.datetime.strptime(safe_start_date, date_format)
    time_tuple = dt.timetuple()
    year = time_tuple.tm_year
    doy = time_tuple.tm_yday

    # Make sure the DOY is zero-padded
    doy = f"{int(doy):03d}"

    logger.debug(f'year: {year} doy: {doy}')

    return str(year), str(doy)


def download_ionosphere_archive(request_url, username, password, output_directory):
    """
    Downloads an Ionosphere Correction archive using the provided credentials
    for EarthData Login.

    Parameters
    ----------
    request_url : str
        The URL to the Ionosphere file to download.
    username : str
        The EDL username to authenticate the request.
    password : str
        The EDL password to authenticate the request.
    output_directory : str
        Path to the location to download the archive to.

    Returns
    -------
    output_ionosphere_archive_path : str
        The path to where the Ionosphere archive was downloaded to.

    Raises
    ------
    IonosphereFileNotFoundException
        If the requested Ionosphere file could not be found.
    RuntimeError
        If the download fails for any other reason.

    """
    # Create a session with the user credentials that are used to authenticate
    # access to EarthData Login
    session = SessionWithHeaderRedirection(username, password)

    # Make the HTTP GET request to obtain the Ionosphere archive
    response = session.get(request_url, stream=True)

    logger.debug(f'response.url: {response.url}')
    logger.debug(f'response.status_code: {response.status_code}')

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        status_code = err.response.status_code

        if status_code == 404:
            raise IonosphereFileNotFoundException(
                f'Could not find an Ionosphere file at {request_url}'
            )

        raise RuntimeError(
            f'Failed to download Ionosphere file from {response.url}, reason: {str(err)}'
        )

    # Write the contents to disk
    archive_name = request_url[request_url.rfind('/') + 1:]
    output_ionosphere_archive_path = os.path.join(output_directory, archive_name)

    with open(output_ionosphere_archive_path, 'wb') as outfile:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            outfile.write(chunk)

    return output_ionosphere_archive_path


def uncompress_ionosphere_archive(output_ionosphere_archive_path):
    """
    Uncompresses a downloaded Ionosphere archive file to obtain the actual
    Ionosphere Correction file.

    Parameters
    ----------
    output_ionosphere_archive_path : str
        Path to the compress Ionosphere archive that was downloaded to local
        disk.

    Returns
    -------
    extraction_path : str
        Path to the uncompressed Ionosphere TEC file on local disk.

    """
    archive_name = os.path.basename(output_ionosphere_archive_path)
    unzipped_archive_name = os.path.splitext(archive_name)[0]
    extraction_dir = os.path.dirname(output_ionosphere_archive_path)
    extraction_path = join(extraction_dir, unzipped_archive_name)

    logger.info(f'Unzipping archive {archive_name} to {extraction_dir}...')

    result = subprocess.run(
        ['gunzip', '-c', output_ionosphere_archive_path],
        check=True, capture_output=True
    )

    logger.info(f'Writing uncompressed Ionosphere data to {extraction_path}...')

    with open(extraction_path, 'wb') as outfile:
        outfile.write(result.stdout)

    return extraction_path


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

    # Make sure we got both a username and password if either were provided via
    # command-line
    if bool(args.username) ^ bool(args.password):
        raise ValueError(
            'Both a username and password must be supplied when provided via command-line'
        )

    # Get username/password from netrc file if neither were provided via command-line
    if args.username is None and args.password is None and not args.url_only:
        args.username, _, args.password = netrc.netrc().authenticators(DEFAULT_EDL_ENDPOINT)

    logger.info(f"Determining Ionosphere file for input SAFE file {args.input_safe_file}")

    # Parse the relevant info from the input SAFE filename
    safe_start_date = parse_start_date_from_safe(args.input_safe_file)

    logger.info(f"Parsed start date {safe_start_date} from SAFE filename")

    # Convert start date to Year and Day of Year (Julian date)
    year, doy = safe_start_date_to_julian_day(safe_start_date)

    # Formulate the archive name and URL location based on the file type and
    # the Julian date of the SLC archive
    archive_name = f"{args.type}{doy}0.{year[2:]}i.Z"
    request_url = join(args.download_endpoint, year, doy, archive_name)

    # If user request the URL only, print it to standard out and the log
    if args.url_only:
        logger.info('URL-only requested')
        logger.info(request_url)
        print(request_url)
        return 0

    # Download the compressed archive file from the endpoint
    logger.info(f"Downloading for Ionosphere Correction archive file(s) from "
                f"endpoint {args.download_endpoint}")

    output_ionosphere_archive_path = download_ionosphere_archive(
        request_url, args.username, args.password, args.output_directory
    )

    logger.info(f'Ionosphere archive downloaded to {output_ionosphere_archive_path}')

    # Uncompress the file
    output_ionosphere_file_path = uncompress_ionosphere_archive(output_ionosphere_archive_path)

    # Remove the compressed version
    os.unlink(output_ionosphere_archive_path)

    logger.info('Ionosphere Correction file staging complete')

    return output_ionosphere_file_path


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args)
    sys.exit(0)
