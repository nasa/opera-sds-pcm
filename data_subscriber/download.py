import itertools
import json
import logging
import shutil
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import PurePath, Path
from typing import Any, Iterable

import boto3
import dateutil.parser
import requests
import requests.utils
import validators
from cachetools.func import ttl_cache
from smart_open import open

import extractor.extract
from data_subscriber import ionosphere_download
from data_subscriber.url import _to_batch_id, _to_orbit_number, _has_url, _to_url, _to_https_url
from data_subscriber.cmr import PRODUCT_PROVIDER_MAP
from data_subscriber.query import DateTimeRange

from util.conf_util import SettingsConf

logger = logging.getLogger(__name__)

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

        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers["Authorization"]

def run_download(args, token, es_conn, netloc, username, password, job_id):
    download = DaacDownload.get_download_object(args)
    download.run_download(args, token, es_conn, netloc, username, password, job_id)

class DaacDownload:

    def __init__(self, provider):
        self.provider = provider
        self.cfg = SettingsConf().cfg  # has metadata extractor config

        self.downloads_dir = None

    @staticmethod
    def get_download_object(args):
        provider = PRODUCT_PROVIDER_MAP[args.collection] if hasattr(args, "collection") else args.provider
        if provider == "LPCLOUD":
            from data_subscriber.lpdaac_download import DaacDownloadLpdaac
            return DaacDownloadLpdaac(provider)
        elif provider == "ASF":
            from data_subscriber.asf_download import DaacDownloadAsf
            return DaacDownloadAsf(provider)

        raise Exception("Unknown product provider: " + provider)

    def run_download(self, args, token, es_conn, netloc, username, password, job_id):
        download_timerange = self.get_download_timerange(args)
        all_pending_downloads: Iterable[dict] = es_conn.get_all_between(
            dateutil.parser.isoparse(download_timerange.start_date),
            dateutil.parser.isoparse(download_timerange.end_date),
            args.use_temporal
        )
        logger.info(f"{len(list(all_pending_downloads))=}")

        downloads = all_pending_downloads
        if args.batch_ids:
            logger.info(f"Filtering pending downloads by {args.batch_ids=}")
            id_func = _to_batch_id if self.provider == "LPCLOUD" else _to_orbit_number
            downloads = list(filter(lambda d: id_func(d) in args.batch_ids, all_pending_downloads))
            logger.info(f"{len(downloads)=}")
            logger.debug(f"{downloads=}")

        if not downloads:
            logger.info(f"No undownloaded files found in index.")
            return

        if args.smoke_run:
            logger.info(f"{args.smoke_run=}. Restricting to 1 tile(s).")
            args.batch_ids = args.batch_ids[:1]

        session = SessionWithHeaderRedirection(username, password, netloc)

        logger.info("Creating directories to process products")

        # house all file downloads
        self.downloads_dir = Path("downloads")
        self.downloads_dir.mkdir(exist_ok=True)

        if args.dry_run:
            logger.info(f"{args.dry_run=}. Skipping downloads.")

        self.perform_download(session, es_conn, downloads, args, token, job_id)

        logger.info(f"Removing directory tree. {self.downloads_dir}")
        shutil.rmtree(self.downloads_dir)

    def perform_download(self, session, es_conn, downloads, args, token, job_id):
        pass

    def get_download_timerange(self, args):
        start_date = args.start_date if args.start_date else "1900-01-01T00:00:00Z"
        end_date = args.end_date if args.end_date else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        download_timerange = DateTimeRange(start_date, end_date)
        logger.info(f"{download_timerange=}")
        return download_timerange


    def extract_one_to_one(self, product: Path, settings_cfg: dict, working_dir: Path, extra_metadata=None, name_postscript='') -> PurePath:
        """Creates a dataset for the given product.

        :param product: the product to create datasets for.
        :param settings_cfg: the settings.yaml config as a dict.
        :param working_dir: the working directory for the extract process. Serves as the output directory for the extraction.
        :param extra_metadata: extra metadata to add to the dataset.
        """
        # create dataset dir for product
        # (this also extracts the metadata to *.met.json file)
        logger.info("Creating dataset directory")
        dataset_dir = extractor.extract.extract(
            product=str(product),
            product_types=settings_cfg["PRODUCT_TYPES"],
            workspace=str(working_dir.resolve()),
            extra_met=extra_metadata,
            name_postscript=name_postscript
        )
        logger.info(f"{dataset_dir=}")
        return PurePath(dataset_dir)


    def download_product_using_s3(self, url, token, target_dirpath: Path, args) -> Path:

        aws_creds = self.get_aws_creds(token)
        logger.debug(f"{self.get_aws_creds.cache_info()=}")

        s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                           aws_secret_access_key=aws_creds['secretAccessKey'],
                           aws_session_token=aws_creds['sessionToken'],
                           region_name='us-west-2').client("s3")
        product_download_path = self._s3_download(url, s3, str(target_dirpath))
        return product_download_path.resolve()

    def _handle_url_redirect(self, url, token):
        if not validators.url(url):
            raise Exception(f"Malformed URL: {url}")

        r = requests.get(url, allow_redirects=False)

        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        return requests.get(r.headers["Location"], headers=headers, allow_redirects=True)


    @ttl_cache(ttl=3300)  # 3300s == 55m. Refresh credentials before expiry. Note: validity period is 60 minutes
    def get_aws_creds(self, token):
        return self._get_aws_creds(token)


    def _s3_download(self, url, s3, tmp_dir, staging_area=""):
        file_name = PurePath(url).name
        target_key = str(Path(staging_area, file_name))

        source = url[len("s3://"):].partition("/")
        source_bucket = source[0]
        source_key = source[2]

        s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

        return Path(f"{tmp_dir}/{target_key}")

    '''
    TODO: Remove these functions?
    Doesn't look like they are being used.
    def _https_transfer(url, bucket_name, token, staging_area=""):
        file_name = PurePath(url).name
        bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name
        key = Path(staging_area, file_name).name

        upload_start_time = datetime.utcnow()

        try:
            logger.info(f"Requesting from {url}")
            r = _handle_url_redirect(url, token)
            if r.status_code != 200:
                r.raise_for_status()

            with open("https.tmp", "wb") as file:
                file.write(r.content)

            logger.info(f"Uploading {file_name} to {bucket=}, {key=}")
            with open("https.tmp", "rb") as file:
                s3 = boto3.client("s3")
                s3.upload_fileobj(file, bucket, key)

            upload_end_time = datetime.utcnow()
            upload_duration = upload_end_time - upload_start_time
            upload_stats = {"file_name": file_name,
                            "file_size (in bytes)": r.headers.get("Content-Length"),
                            "upload_duration (in seconds)": upload_duration.total_seconds(),
                            "upload_start_time": _convert_datetime(upload_start_time),
                            "upload_end_time": _convert_datetime(upload_end_time)}
            logger.debug(f"{upload_stats=}")

            return upload_stats
        except (Exception, ConnectionResetError, requests.exceptions.HTTPError) as e:
            logger.error(e)
            return {"failed_download": e}

    def _convert_datetime(self, datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
        if isinstance(datetime_obj, datetime):
            return datetime_obj.strftime(strformat)
        return datetime.strptime(str(datetime_obj), strformat)


    def _to_s3_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


    def group_download_urls_by_granule_id(download_urls):
    granule_id_to_download_urls_map = defaultdict(list)
    for download_url in download_urls:
        # remove both suffixes to get granule ID (e.g. removes .Fmask.tif)
        granule_id = PurePath(download_url).with_suffix("").with_suffix("").name
        granule_id_to_download_urls_map[granule_id].append(download_url)
    return granule_id_to_download_urls_map

    def _s3_transfer(url, bucket_name, s3, tmp_dir, staging_area=""):
    try:
        _s3_download(url, s3, tmp_dir, staging_area)
        target_key = _s3_upload(url, bucket_name, tmp_dir, staging_area)

        return {"successful_download": target_key}
    except Exception as e:
        return {"failed_download": e}

    def _s3_upload(url, bucket_name, tmp_dir, staging_area=""):
    file_name = PurePath(url).name
    target_key = str(Path(staging_area, file_name))
    target_bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name

    target_s3 = boto3.resource("s3")
    target_s3.Bucket(target_bucket).upload_file(f"{tmp_dir}/{target_key}", target_key)

    return target_key
            '''