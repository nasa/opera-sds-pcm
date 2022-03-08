#!/usr/bin/env python3

# Forked from github.com:podaac/data-subscriber.git


import argparse
import json
import logging
import netrc
import os
import shutil
import sys
from datetime import datetime, timedelta
from http.cookiejar import CookieJar
from urllib import request
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import boto3
import requests

from data_subscriber.es_connection import get_data_subscriber_connection


class SessionWithHeaderRedirection(requests.Session):
    """
    Borrowed from https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    def __init__(self, username, password, auth_host):
        super().__init__()
        self.auth = (username, password)
        self.auth_host = auth_host

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers['Authorization']
        return


def run():
    parser = create_parser()
    args = parser.parse_args()

    try:
        validate(args)
    except ValueError as v:
        logging.error(v)
        exit()

    IP_ADDR = "127.0.0.1"
    EDL = "urs.earthdata.nasa.gov"
    CMR = "cmr.earthdata.nasa.gov"
    TOKEN_URL = f"https://{CMR}/legacy-services/rest/tokens"
    NETLOC = urlparse("https://urs.earthdata.nasa.gov").netloc
    ES_CONN = get_data_subscriber_connection(logging.getLogger(__name__))

    LOGLEVEL = 'DEBUG' if args.verbose else 'INFO'
    logging.basicConfig(level=LOGLEVEL)
    logging.info("Log level set to " + LOGLEVEL)
    logging.debug(f"sys.argv = {sys.argv}")

    username, password = setup_earthdata_login_auth(EDL)
    token = get_token(TOKEN_URL, 'daac-subscriber', IP_ADDR, EDL)

    downloads = get_all_undownloaded(ES_CONN) if args.index_mode.lower() == "download" else query_cmr(args, token, CMR)

    if downloads != []:
        if args.index_mode.lower() == "query":
            update_es_index(ES_CONN, downloads)
        else:
            upload_url_list(username, password, NETLOC, ES_CONN, downloads, args)

    delete_token(TOKEN_URL, token)
    logging.info("END")


def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--collection-shortname", dest="collection", required=True,
                        help="The collection shortname for which you want to retrieve data.")
    parser.add_argument("-s", "--s3bucket", dest="s3_bucket", required=True,
                        help="The s3 bucket where data products will be downloaded.")
    parser.add_argument("-sd", "--start-date", dest="startDate", default=False,
                        help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z")
    parser.add_argument("-ed", "--end-date", dest="endDate", default=False,
                        help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z")
    parser.add_argument("-b", "--bounds", dest="bbox", default="-180,-90,180,90",
                        help="The bounding rectangle to filter result in. Format is W Longitude,S Latitude,E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from the command line. Default: \"-180,-90,180,90\".")
    parser.add_argument("-m", "--minutes", dest="minutes", type=int, default=60,
                        help="How far back in time, in minutes, should the script look for data. If running this script as a cron, this value should be equal to or greater than how often your cron runs (default: 60 minutes).")
    parser.add_argument("-e", "--extension_list", dest="extension_list", default="TIF",
                        help="The file extension mapping of products to download (band/mask). Defaults to all .tif files.")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Verbose mode.")
    parser.add_argument("-p", "--provider", dest="provider", default='LPCLOUD',
                        help="Specify a provider for collection search. Default is LPCLOUD.")
    parser.add_argument("-i", "--index-mode", dest="index_mode", default="Disabled",
                        help="-i \"query\" will execute the query and update the ES index without downloading files. -i \"download\" will download all files from the ES index marked as not yet downloaded.")

    return parser


def validate(args):
    validate_bounds(args.bbox)

    if args.startDate:
        validate_date(args.startDate, "start")

    if args.endDate:
        validate_date(args.endDate, "end")

    if args.minutes:
        validate_minutes(args.minutes)


def validate_bounds(bbox):
    bounds = bbox.split(',')
    value_error = ValueError(
        f"Error parsing bounds: {bbox}. Format is <W Longitude>,<S Latitude>,<E Longitude>,<N Latitude> without spaces ")

    if len(bounds) != 4:
        raise value_error

    for b in bounds:
        try:
            float(b)
        except ValueError:
            raise value_error


def validate_date(date, type='start'):
    try:
        datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ValueError(
            f"Error parsing {type} date: {date}. Format must be like 2021-01-14T00:00:00Z")


def validate_minutes(minutes):
    try:
        int(minutes)
    except ValueError:
        raise ValueError(f"Error parsing minutes: {minutes}. Number must be an integer.")


def setup_earthdata_login_auth(endpoint):
    # ## Authentication setup
    #
    # This function will allow Python scripts to log into any Earthdata Login
    # application programmatically.  To avoid being prompted for
    # credentials every time you run and also allow clients such as curl to log in,
    # you can add the following to a `.netrc` (`_netrc` on Windows) file in
    # your home directory:
    #
    # ```
    # machine urs.earthdata.nasa.gov
    #     login <your username>
    #     password <your password>
    # ```
    #
    # Make sure that this file is only readable by the current user
    # or you will receive an error stating
    # "netrc access too permissive."
    #
    # `$ chmod 0600 ~/.netrc`
    #
    # You'll need to authenticate using the netrc method when running from
    # command line with [`papermill`](https://papermill.readthedocs.io/en/latest/).
    # You can log in manually by executing the cell below when running in the
    # notebook client in your browser.*

    """
    Set up the request library so that it authenticates against the given
    Earthdata Login endpoint and is able to track cookies between requests.
    This looks in the .netrc file first and if no credentials are found,
    it prompts for them.

    Valid endpoints include:
        urs.earthdata.nasa.gov - Earthdata Login production
    """
    username = password = ""
    try:
        username, _, password = netrc.netrc().authenticators(endpoint)
    except (FileNotFoundError):
        logging.error("There's no .netrc file")
    except (TypeError):
        logging.error("The endpoint isn't in the netrc file")

    manager = request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, endpoint, username, password)
    auth = request.HTTPBasicAuthHandler(manager)

    jar = CookieJar()
    processor = request.HTTPCookieProcessor(jar)
    opener = request.build_opener(auth, processor)
    opener.addheaders = [('User-agent', 'daac-subscriber')]
    request.install_opener(opener)

    return username, password


def get_token(url: str, client_id: str, user_ip: str, endpoint: str) -> str:
    username, _, password = netrc.netrc().authenticators(endpoint)
    xml = f"<?xml version='1.0' encoding='utf-8'?><token><username>{username}</username><password>{password}</password><client_id>{client_id}</client_id><user_ip_address>{user_ip}</user_ip_address></token>"
    headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
    resp = requests.post(url, headers=headers, data=xml)
    response_content = json.loads(resp.content)
    token = response_content['token']['id']

    return token


def get_all_undownloaded(ES_CONN):
    undownloaded = ES_CONN.query_undownloaded()
    return [result['_source']['url'] for result in undownloaded]


def query_cmr(args, token, CMR):
    PAGE_SIZE = 2000
    EXTENSION_LIST_MAP = {"L30": ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"],
                          "S30": ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"],
                          "TIF": ["tif"]}
    time_range_is_defined = args.startDate or args.endDate

    data_within_last_timestamp = args.startDate if time_range_is_defined else (
            datetime.utcnow() - timedelta(minutes=args.minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        'scroll': "true",
        'page_size': PAGE_SIZE,
        'sort_key': "-start_date",
        'provider': args.provider,
        'ShortName': args.collection,
        'updated_since': data_within_last_timestamp,
        'token': token,
        'bounding_box': args.bbox,
    }

    if time_range_is_defined:
        temporal_range = get_temporal_range(args.startDate, args.endDate,
                                            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
        params['temporal'] = temporal_range
        logging.debug("Temporal Range: " + temporal_range)

    logging.debug("Provider: " + args.provider)
    logging.debug("Updated Since: " + data_within_last_timestamp)

    # Get the query parameters as a string and then the complete search url:
    url = f"https://{CMR}/search/granules.umm_json?{urlencode(params)}"

    logging.debug(url)

    with urlopen(url) as url:
        results = json.loads(url.read().decode())

    logging.debug(
        f"{str(results['hits'])} new granules found for {args.collection} since {data_within_last_timestamp}")

    downloads = [u['URL'] for item in results['items'] for u in item['umm']['RelatedUrls']]

    if len(downloads) >= PAGE_SIZE:
        logging.info(
            f"Warning: only the most recent {str(PAGE_SIZE)} granules will be downloaded; Try adjusting your search criteria.")

    # filter list based on extension
    filtered_downloads = [f
                          for f in downloads
                          for extension in EXTENSION_LIST_MAP.get(args.extension_list.upper())
                          if extension in f]

    logging.debug(f"Found {str(len(filtered_downloads))} total files")

    return filtered_downloads


def get_temporal_range(start, end, now):
    start = start if start is not False else None
    end = end if end is not False else None

    if start is not None and end is not None:
        return "{},{}".format(start, end)
    if start is not None and end is None:
        return "{},{}".format(start, now)
    if start is None and end is not None:
        return "1900-01-01T00:00:00Z,{}".format(end)

    raise ValueError("One of start-date or end-date must be specified.")


def product_is_duplicate(es_conn, filename, url):
    result = product_exists_in_es(es_conn, filename)

    if not result:
        create_product_in_es(es_conn, filename, url)
        return False

    return product_is_downloaded(result)


def product_exists_in_es(es_conn, filename):
    return es_conn.query_existence(filename)


def create_product_in_es(es_conn, filename, url):
    es_conn.post(id=filename, url=url)


def product_is_downloaded(result):
    return result["_source"]["downloaded"]


def convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)

def update_es_index(ES_CONN, downloads):
    filtered_downloads = [f for f in downloads if "s3://" in f]

    for url in filtered_downloads:
        filename = url.split('/')[-1]
        product_is_duplicate(ES_CONN, filename, url) #Implicitly adds new product to index

def upload_url_list(username, password, NETLOC, ES_CONN, downloads, args):
    session = SessionWithHeaderRedirection(username, password, NETLOC)
    aws_creds = get_aws_creds(session)
    s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                       aws_secret_access_key=aws_creds['secretAccessKey'],
                       aws_session_token=aws_creds['sessionToken'],
                       region_name='us-west-2').client("s3")

    tmp_dir = "/tmp/data_subscriber"
    os.makedirs(tmp_dir, exist_ok=True)

    num_successes = num_failures = num_skipped = 0
    filtered_downloads = [f for f in downloads if "s3://" in f]

    for url in filtered_downloads:
        try:
            filename = url.split('/')[-1]
            if product_is_duplicate(ES_CONN, filename, url):
                logging.info(f"SKIPPING: {url}")
                num_skipped = num_skipped + 1
            else:
                result = s3_transfer(url, args.s3_bucket, s3, tmp_dir)
                if "failed_download" in result:
                    raise Exception(result["failed_download"])
                else:
                    logging.debug(str(result))

                mark_product_as_downloaded(ES_CONN, filename)
                logging.info(f"{str(datetime.now())} SUCCESS: {url}")
                num_successes = num_successes + 1
        except Exception as e:
            logging.error(f"{str(datetime.now())} FAILURE: {url}")
            num_failures = num_failures + 1
            logging.error(e)

    shutil.rmtree(tmp_dir)
    logging.info(f"Files downloaded: {str(num_successes)}")
    logging.info(f"Duplicate files skipped: {str(num_skipped)}")
    logging.info(f"Files failed to download: {str(num_failures)}")


def get_aws_creds(session):
    with session.get("https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials") as r:
        if r.status_code != 200:
            r.raise_for_status()

        return r.json()


def s3_transfer(url, bucket_name, s3, tmp_dir, staging_area=""):
    file_name = os.path.basename(url)

    source = url[len("s3://"):].partition('/')
    source_bucket = source[0]
    source_key = source[2]

    target_bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name
    target_key = os.path.join(staging_area, file_name)

    try:
        s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

        target_s3 = boto3.resource("s3")
        target_s3.Bucket(target_bucket).upload_file(f"{tmp_dir}/{target_key}", target_key)

        return {"successful_download": target_key}
    except Exception as e:
        return {"failed_download": e}


def mark_product_as_downloaded(es_conn, filename):
    es_conn.mark_downloaded(filename)


def delete_token(url: str, token: str) -> None:
    try:
        headers = {'Content-Type': 'application/xml', 'Accept': 'application/json'}
        url = '{}/{}'.format(url, token)
        resp = requests.request('DELETE', url, headers=headers)
        if resp.status_code == 204:
            logging.info("CMR token successfully deleted")
        else:
            logging.error("CMR token deleting failed.")
    except:
        logging.error("Error deleting the token")
    exit(0)


if __name__ == '__main__':
    run()
