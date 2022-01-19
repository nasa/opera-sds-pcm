#!/usr/bin/env python3

# Forked from github.com:podaac/data-subscriber.git


import argparse
import json
import logging
import netrc
import os
from datetime import datetime, timedelta
from http.cookiejar import CookieJar
from urllib import request
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import boto3
import requests
from botocore.exceptions import ClientError
from smart_open import open


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
    IPAddr = "127.0.0.1"
    edl = "urs.earthdata.nasa.gov"
    cmr = "cmr.earthdata.nasa.gov"
    token_url = "https://" + cmr + "/legacy-services/rest/tokens"
    parsed_url = urlparse("https://urs.earthdata.nasa.gov")
    page_size = 2000
    defined_time_range = False

    parser = create_parser()
    args = parser.parse_args()

    LOGLEVEL = 'DEBUG' if args.verbose else os.environ.get('SUBSCRIBER_LOGLEVEL', 'INFO').upper()
    logging.basicConfig(level=LOGLEVEL)
    logging.debug("Log level set to " + LOGLEVEL)

    try:
        validate(args)
    except ValueError as v:
        logging.error(v)
        exit()

    username, password = setup_earthdata_login_auth(edl)
    token = get_token(token_url, 'podaac-subscriber', IPAddr, edl)
    mins = args.minutes  # In this case download files ingested in the last 60 minutes -- change this to whatever setting is needed

    provider = args.provider

    start_date_time = args.startDate
    end_date_time = args.endDate

    if start_date_time or end_date_time:
        defined_time_range = True

    short_name = args.collection
    extensions = args.extensions

    s3_bucket = args.s3_bucket

    # **The search retrieves granules ingested during the last `n` minutes.
    # ** A file in your local data dir  file that tracks updates to your data directory,
    # if one file exists.
    #

    # This is the default way of finding data if no other
    if defined_time_range:
        data_within_last_timestamp = start_date_time
    else:
        data_within_last_timestamp = (datetime.utcnow() - timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Change this to whatever extent you need. Format is W Longitude,S Latitude,E Longitude,N Latitude
    bounding_extent = args.bbox

    # There are several ways to query for CMR updates that occured during a given timeframe. Read on in the CMR Search documentation:
    # * https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-with-new-granules (Collections)
    # * https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-with-revised-granules (Collections)
    # * https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#g-production-date (Granules)
    # * https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#g-created-at (Granules)
    # The `created_at` parameter works for our purposes. It's a granule search parameter that returns the records ingested since the input timestamp.

    params = {
        'scroll': "true",
        'page_size': page_size,
        'sort_key': "-start_date",
        'provider': provider,
        'ShortName': short_name,
        'updated_since': data_within_last_timestamp,
        'token': token,
        'bounding_box': bounding_extent,
    }

    if defined_time_range:
        temporal_range = get_temporal_range(start_date_time, end_date_time,
                                            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))  # noqa E501
        params['temporal'] = temporal_range
        logging.debug("Temporal Range: " + temporal_range)

    logging.debug("Provider: " + provider)
    logging.debug("Updated Since: " + data_within_last_timestamp)

    # Get the query parameters as a string and then the complete search url:
    query = urlencode(params)
    url = "https://" + cmr + "/search/granules.umm_json?" + query

    logging.debug(url)

    # Get a new timestamp that represents the UTC time of the search.
    # Then download the records in `umm_json` format for granules
    # that match our search parameters:
    with urlopen(url) as f:
        results = json.loads(f.read().decode())

    logging.debug(str(results[
                          'hits']) + " new granules found for " + short_name + " since " + data_within_last_timestamp)  # noqa E501

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Neatly print the first granule record (if one was returned):
    # if len(results['items'])>0:
    #    print(dumps(results['items'][0], indent=2))

    # The link for http access can be retrieved from each granule
    # record's `RelatedUrls` field.
    # The download link is identified by `"Type": "GET DATA"` but there are
    # other data files in EXTENDED METADATA" field.
    # Select the download URL for each of the granule records:

    downloads_all = []

    downloads_data = [[u['URL'] for u in r['umm']['RelatedUrls'] if
                       u['Type'] == "GET DATA" and ('Subtype' not in u or u['Subtype'] != "OPENDAP DATA")] for r in
                      results['items']]
    downloads_metadata = [[u['URL'] for u in r['umm']['RelatedUrls'] if u['Type'] == "EXTENDED METADATA"] for r in
                          results['items']]

    for f in downloads_data:
        downloads_all.append(f)
    for f in downloads_metadata:
        downloads_all.append(f)

    downloads = [item for sublist in downloads_all for item in sublist]

    if len(downloads) >= page_size:
        logging.info("Warning: only the most recent " + str(
            page_size) + " granules will be downloaded; try adjusting your search criteria (suggestion: reduce time period or spatial region of search) to ensure you retrieve all granules.")

    # filter list based on extension
    if not extensions:
        extensions = [".nc", ".h5", ".zip"]
    filtered_downloads = []
    for f in downloads:
        for extension in extensions:
            if f.lower().endswith(extension):
                filtered_downloads.append(f)

    downloads = filtered_downloads

    logging.debug("Found " + str(len(downloads)) + " total files to download")
    logging.debug("Downloading files with extensions: " + str(extensions))

    # Finish by downloading the files to the data directory in a loop.
    # Overwrite `.update` with a new timestamp on success.
    success_cnt = failure_cnt = 0

    if args.s3_bucket:
        for f in downloads:
            try:
                for extension in extensions:
                    if f.lower().endswith(extension):
                        upload_return = upload(f, SessionWithHeaderRedirection(username, password, parsed_url.netloc),
                                               token, s3_bucket)
                        if "failed_download" in upload_return:
                            raise Exception(upload_return["failed_download"])
                        logging.info(str(datetime.now()) + " SUCCESS: " + f)
                        success_cnt = success_cnt + 1
            except Exception as e:
                logging.error(str(datetime.now()) + " FAILURE: " + f)
                failure_cnt = failure_cnt + 1
                logging.error(e)

    # If there were updates to the local time series during this run and no
    # exceptions were raised during the download loop, then overwrite the
    #  timestamp file that tracks updates to the data folder
    #   (`resources/nrt/.update`):
    if len(results['items']) > 0:
        if not failure_cnt > 0:
            with open(s3_bucket + "/.update", "w") as f:
                f.write(timestamp)

    logging.info("Downloaded: " + str(success_cnt) + " files\n")
    logging.info("Files Failed to download:" + str(failure_cnt) + "\n")
    delete_token(token_url, token)
    logging.info("END \n\n")


def convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--collection-shortname", dest="collection", required=True,
                        help="The collection shortname for which you want to retrieve data.")  # noqa E501
    parser.add_argument("-s", "--s3bucket", dest="s3_bucket", required=True,
                        help="The s3 bucket where data products will be downloaded.")

    parser.add_argument("-sd", "--start-date", dest="startDate",
                        help="The ISO date time before which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z",
                        default=False)  # noqa E501
    parser.add_argument("-ed", "--end-date", dest="endDate",
                        help="The ISO date time after which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z",
                        default=False)  # noqa E501
    parser.add_argument("-b", "--bounds", dest="bbox",
                        help="The bounding rectangle to filter result in. Format is W Longitude,S Latitude,E Longitude,N Latitude without spaces. Due to an issue with parsing arguments, to use this command, please use the -b=\"-180,-90,180,90\" syntax when calling from the command line. Default: \"-180,-90,180,90\".",
                        default="-180,-90,180,90")  # noqa E501
    parser.add_argument("-m", "--minutes", dest="minutes",
                        help="How far back in time, in minutes, should the script look for data. If running this script as a cron, this value should be equal to or greater than how often your cron runs (default: 60 minutes).",
                        type=int, default=60)  # noqa E501
    parser.add_argument("-e", "--extensions", dest="extensions",
                        help="The extensions of products to download. Default is [.nc, .h5, .zip]",
                        default=[".nc", ".h5", ".zip"], action='append')  # noqa E501
    parser.add_argument("--version", dest="version", action="store_true",
                        help="Display script version information and exit.")  # noqa E501
    parser.add_argument("--verbose", dest="verbose", action="store_true", help="Verbose mode.")  # noqa E501
    parser.add_argument("-p", "--provider", dest="provider", default='LPCLOUD',
                        help="Specify a provider for collection search. Default is LPCLOUD.")  # noqa E501
    return parser


def delete_token(url: str, token: str) -> None:
    try:
        headers: Dict = {'Content-Type': 'application/xml', 'Accept': 'application/json'}  # noqa E501
        url = '{}/{}'.format(url, token)
        resp = requests.request('DELETE', url, headers=headers)
        if resp.status_code == 204:
            logging.info("CMR token successfully deleted")
        else:
            logging.error("CMR token deleting failed.")
    except:  # noqa E722
        logging.error("Error deleting the token")
    exit(0)


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


def get_token(url: str, client_id: str, user_ip: str, endpoint: str) -> str:
    try:
        token: str = ''
        username, _, password = netrc.netrc().authenticators(endpoint)
        xml: str = """<?xml version='1.0' encoding='utf-8'?>
        <token><username>{}</username><password>{}</password><client_id>{}</client_id>
        <user_ip_address>{}</user_ip_address></token>""".format(username, password, client_id, user_ip)  # noqa E501
        headers: Dict = {'Content-Type': 'application/xml', 'Accept': 'application/json'}  # noqa E501
        resp = requests.post(url, headers=headers, data=xml)
        response_content: Dict = json.loads(resp.content)
        token = response_content['token']['id']

    # What error is thrown here? Value Error? Request Errors?
    except:  # noqa E722
        logging.error("Error getting the token - check user name and password")
    return token


def product_exists(bucket, key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


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
    try:
        username, _, password = netrc.netrc().authenticators(endpoint)
    except (FileNotFoundError, TypeError):
        # FileNotFound = There's no .netrc file
        # TypeError = The endpoint isn't in the netrc file,
        #  causing the above to try unpacking None
        logging.error("There's no .netrc file or the The endpoint isn't in the netrc file")  # noqa E501

    manager = request.HTTPPasswordMgrWithDefaultRealm()
    manager.add_password(None, endpoint, username, password)
    auth = request.HTTPBasicAuthHandler(manager)

    jar = CookieJar()
    processor = request.HTTPCookieProcessor(jar)
    opener = request.build_opener(auth, processor)
    opener.addheaders = [('User-agent', 'daac-subscriber')]
    request.install_opener(opener)

    return username, password


def upload(url, session, token, bucket_name, staging_area="", chunk_size=25600):
    """
    This will basically transfer the file contents of the given url to an S3 bucket

    :param url: url to the file.
    :param session: SessionWithHeaderRedirection object.
    :param token: token.
    :param bucket_name: The S3 bucket name to transfer the file url contents to.
    :param staging_area: A staging area where the file url contents will go to. If none, contents will be found
     in the top level of the bucket.
    :param chunk_size: the number of bytes to stream at a time.

    :return:
    """
    file_name = os.path.basename(url)
    bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name

    key = os.path.join(staging_area, file_name)
    upload_start_time = datetime.utcnow()
    headers = {"Echo-Token": token}
    try:
        try:
            with session.get(url, headers=headers, stream=True) as r:
                if r.status_code != 200:
                    r.raise_for_status()
                logging.info("Uploading {} to Bucket={}, Key={}".format(file_name, bucket_name, key))
                total_bytes = 0
                with open("s3://{}/{}".format(bucket, key), "wb") as out:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        logging.debug("Uploading {} byte(s)".format(len(chunk)))
                        out.write(chunk)
                        total_bytes += len(chunk)
            upload_end_time = datetime.utcnow()
            upload_duration = upload_end_time - upload_start_time
            upload_stats = {
                "file_name": file_name,
                "file_size (in bytes)": total_bytes,
                "upload_duration (in seconds)": upload_duration.total_seconds(),
                "upload_start_time": convert_datetime(upload_start_time),
                "upload_end_time": convert_datetime(upload_end_time)
            }
            return upload_stats
        except ConnectionResetError as ce:
            raise Exception(str(ce))
        except requests.exceptions.HTTPError as he:
            raise Exception(str(he))
    except Exception as e:
        return {"failed_download": e}


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
        f"Error parsing bounds: {bbox}. Format is <W Longitude>,<S Latitude>,<E Longitude>,<N Latitude> without spaces ")  # noqa E501

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
            f"Error parsing {type} date: {date}. Format must be like 2021-01-14T00:00:00Z")  # noqa E501


def validate_minutes(minutes):
    try:
        int(minutes)
    except ValueError:
        raise ValueError(f"Error parsing minutes: {minutes}. Number must be an integer.")  # noqa E501


if __name__ == '__main__':
    run()
