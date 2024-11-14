#!/usr/bin/env python3

"""
========================
dataspace_s1_download.py
========================

Script to query and download the Sentinel-1 files from ESA's Dataspace system.

"""

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from getpass import getuser

import backoff
import requests

from commons.logger import logger, LogLevels

DEFAULT_QUERY_ENDPOINT = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for the Copernicus Data Space Ecosystem (CDSE) query REST service"""

DEFAULT_AUTH_ENDPOINT = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'
"""Default URL endpoint for performing user authentication with CDSE"""

DEFAULT_DOWNLOAD_ENDPOINT = 'https://download.dataspace.copernicus.eu/odata/v1/Products'
"""Default URL endpoint for CDSE download REST service"""

DEFAULT_SESSION_ENDPOINT = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/account/sessions'
"""Default URL endpoint to manage authentication sessions with CDSE"""

ISO_TIME = '%Y-%m-%dT%H:%M:%SZ'
"""Temporal format required by ODATA API: yyyy-mm-ddTHH:MM:SSZ"""

QUERY_PAGE_SIZE = 1000
"""Max number of files returned per query"""


def fatal_code(err: requests.exceptions.RequestException) -> bool:
    """Only retry for common transient errors"""
    return err.response.status_code not in [418, 429, 500, 502, 503, 504]


def to_datetime(value):
    """Helper function to covert command-line arg to datetime object"""
    return datetime.strptime(value, ISO_TIME)


class NoQueryResultsException(Exception):
    """Custom exception to identify empty results from a query"""
    pass


class DataspaceSession:
    """
    Context manager class to wrap credentialed operations in.
    Creates a session and gets its token on entry, and deletes the session on exit.
    """

    def __init__(self, username, password):
        self.__token, self.__session = self._get_token(username, password)

    @backoff.on_exception(backoff.constant, requests.exceptions.RequestException, max_time=300, giveup=fatal_code,
                          interval=15)
    def _get_token(self, username, password):
        data = {
            'client_id': 'cdse-public',
            'username': username,
            'password': password,
            'grant_type': 'password'
        }

        response = requests.post(DEFAULT_AUTH_ENDPOINT, data=data)

        logger.info(f'Get Dataspace token for {username}: {response.status_code}')

        response.raise_for_status()

        try:
            access_token = response.json()['access_token']
            session_id = response.json()['session_state']
        except KeyError as e:
            raise RuntimeError(
                f'Failed to parse expected field "{str(e)}" from authentication response.'
            )

        logger.info(f'Created Dataspace session {session_id}')

        return access_token, session_id

    @backoff.on_exception(backoff.constant, requests.exceptions.RequestException, max_time=300, giveup=fatal_code,
                          interval=15)
    def _delete_token(self):
        url = f'{DEFAULT_SESSION_ENDPOINT}/{self.__session}'
        headers = {'Authorization': f'Bearer {self.__token}', 'Content-Type': 'application/json'}

        response = requests.delete(url=url, headers=headers)

        logger.info(f'Delete request {response.url}: {response.status_code}')
        response.raise_for_status()

    def __enter__(self):
        return self.__session, self.__token

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._delete_token()


def get_parser():
    """Returns the command line parser for dataspace_s1_download.py"""

    parser = argparse.ArgumentParser(
        description="Query and stage Sentinel-1 SAFE files from S1-A/B/C based on temporal and filename restrictions.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    general = parser.add_argument_group('General Options')

    general.add_argument(
        '-o', '--output-directory',
        default=os.path.abspath(os.curdir),
        help='Directory into which to write downloaded files. Has no effect if --url-only is set.'
    )

    general.add_argument(
        '--url-only',
        action='store_true',
        help='Only output download URLs for matching file(s).'
    )

    auth = parser.add_argument_group('Authentication Options')

    auth.add_argument(
        '--username',
        default=getuser(),
        help='Username of Dataspace account.'
    )

    # Note: Not setting required here since we shouldn't need to authenticate if --url-only is set
    auth.add_argument(
        '--password',
        help='Password of Dataspace account. Required unless --url-only is set.'
    )

    filtering = parser.add_argument_group('Filtering Options')

    filtering.add_argument(
        '-p', '--platform',
        required=True,
        choices=['S1A', 'S1B', 'S1C', 'S1D'],
        help='Sentinel-1 platform to query'
    )

    filtering.add_argument(
        '-s', '--substring', '--substrings',
        nargs='+',
        dest='substring',
        help='Substring to search for in filenames'
    )

    filtering.add_argument(
        '-b', '--start-date',
        type=to_datetime,
        help='Start date for temporal search in YYYY-MM-DDTHH:MM:SSZ format'
    )

    filtering.add_argument(
        '-e', '--end-date',
        type=to_datetime,
        help='End date for temporal search in YYYY-MM-DDTHH:MM:SSZ format'
    )

    return parser


def build_query_filter(platform, *args):
    filter_string = f"Collection/Name eq 'SENTINEL-1' and startswith(Name,'{platform}') and endswith(Name,'SAFE')"

    filter_string = ' and '.join([filter_string] + list(args))
    return {'$filter': filter_string, '$orderby': 'ContentDate/Start asc', '$top': str(QUERY_PAGE_SIZE)}


@backoff.on_exception(backoff.constant, requests.exceptions.RequestException, max_time=300, giveup=fatal_code, interval=15)
def _do_query(url, **kwargs):
    response = requests.get(url, **kwargs)

    logger.debug(f'GET {response.url}: {response.status_code}')
    response.raise_for_status()
    return response.json()


def query(params):
    results = []

    response = _do_query(DEFAULT_QUERY_ENDPOINT, params=params)

    results.extend(response['value'])

    # while len(results) < MAX_RESULT_SIZE and '@odata.nextLink' in response:
    while '@odata.nextLink' in response:
        response = _do_query(response['@odata.nextLink'])
        results.extend(response['value'])

    return results


def main():
    parser = get_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    query_filters = []

    if args.substring is not None:
        query_filters.extend([f"contains(Name,'{substring}')" for substring in args.substring])

    if args.start_date is not None:
        query_filters.append(f'ContentDate/Start gt {datetime.strftime(args.start_date, ISO_TIME)}')

    if args.end_date is not None:
        query_filters.append(f'ContentDate/Start lt {datetime.strftime(args.end_date, ISO_TIME)}')

    query_results = query(build_query_filter(args.platform, *query_filters))

    if len(query_results) == 0:
        raise NoQueryResultsException()

    if args.url_only:
        urls = {r['Name']: f'{DEFAULT_DOWNLOAD_ENDPOINT}({r["Id"]})/$value' for r in query_results}
        print(json.dumps(urls, indent=4))
    else:
        with DataspaceSession(args.username, args.password) as (session, token):
            @backoff.on_exception(backoff.constant, requests.exceptions.RequestException, max_time=300,
                                  giveup=fatal_code, interval=15)
            def do_download(gid, filename):
                start_t = datetime.now()

                url = f'{DEFAULT_DOWNLOAD_ENDPOINT}({gid})/$value'
                headers = {"Authorization": f"Bearer {token}"}
                session = requests.Session()
                session.headers.update(headers)

                response = session.get(url, headers=headers, stream=True)
                logger.info(f'Download request {response.url}: {response.status_code}')

                response.raise_for_status()

                out_path = os.path.join(args.output_directory, filename)
                size = 0

                with open(out_path, 'wb') as fp:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            size += len(chunk)
                            fp.write(chunk)

                logger.info(f'Completed download for {gid} to {out_path} ({size:,} bytes in {datetime.now() - start_t})')

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []

                for result in query_results:
                    futures.append(executor.submit(do_download, result['Id'], result['Name']))

                for f in as_completed(futures):
                    f.result()


def validate_args(parser, args):
    if args.password is None and not args.url_only:
        print('Either password must be provided or --url-only flag must be set')
        parser.print_help()
        exit(1)

    if args.start_date is not None and args.end_date is not None:
        if args.start_date > args.end_date:
            raise ValueError('--start-date must be before --end-date')

    if not args.url_only and all([a is None for a in [args.start_date, args.end_date, args.substring]]):
        raise ValueError('At least one filtering option must be set if --url-only is unset')


if __name__ == '__main__':
    main()
