import logging
import shutil
from datetime import datetime
from pathlib import PurePath, Path
from typing import Iterable

import backoff
import boto3
import dateutil.parser
import requests
import requests.utils
import validators
from cachetools.func import ttl_cache

import extractor.extract
from data_subscriber.cmr import Provider, CMR_TIME_FORMAT
from data_subscriber.query import DateTimeRange
from data_subscriber.url import _to_batch_id, _to_orbit_number
from util.conf_util import SettingsConf
from tools.stage_orbit_file import fatal_code

logger = logging.getLogger(__name__)

AWS_REGION = "us-west-2"

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


class DaacDownload:

    def __init__(self, provider):
        self.provider = provider
        self.daac_s3_cred_settings_key = None
        self.cfg = SettingsConf().cfg  # has metadata extractor config

        logger.info("Creating directories to process products")

        # house all file downloads
        self.downloads_dir = Path("downloads")
        self.downloads_dir.mkdir(exist_ok=True)

    def run_download(self, args, token, es_conn, netloc, username, password, cmr,
                           job_id, rm_downloads_dir=True):
        product_to_product_filepaths_map = {}
        downloads = self.get_downloads(args, es_conn)

        if not downloads:
            logger.info(f"No undownloaded files found in index.")
            return product_to_product_filepaths_map

        if args.dry_run:
            logger.info(f"{args.dry_run=}. Skipping downloads.")
            return product_to_product_filepaths_map

        session = SessionWithHeaderRedirection(username, password, netloc)

        product_to_product_filepaths_map = self.perform_download(
            session, es_conn, downloads, args, token, job_id
        )

        if rm_downloads_dir:
            logger.info(f"Removing directory tree. {self.downloads_dir}")
            shutil.rmtree(self.downloads_dir)

        return product_to_product_filepaths_map

    def get_downloads(self, args, es_conn):
        # This is a special case where we are being asked to download exactly one granule
        # identified its unique id. In such case we shouldn't gather all pending downloads at all;
        # simply find entries for that one granule
        if args.batch_ids and len(args.batch_ids) == 1:
            one_granule = args.batch_ids[0]
            logger.info(f"Downloading files for the granule {one_granule}")
            downloads = es_conn.get_download_granule_revision(one_granule)
        else:
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
                id_func = (_to_batch_id
                           if self.provider in (Provider.LPCLOUD, Provider.ASF_RTC, Provider.ASF_CSLC)
                           else _to_orbit_number)

                downloads = list(filter(lambda d: id_func(d) in args.batch_ids, all_pending_downloads))
                logger.info(f"{len(downloads)=}")
                logger.debug(f"{downloads=}")

        return downloads

    def perform_download(self, session, es_conn, downloads, args, token, job_id):
        pass

    def get_download_timerange(self, args):
        start_date = args.start_date if args.start_date else "1900-01-01T00:00:00Z"
        end_date = args.end_date if args.end_date else datetime.utcnow().strftime(CMR_TIME_FORMAT)
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

        if self.cfg["USE_DAAC_S3_CREDENTIALS"] is True:
            aws_creds = self.get_aws_creds(token)
            logger.debug(f"{self.get_aws_creds.cache_info()=}")
            s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                               aws_secret_access_key=aws_creds['secretAccessKey'],
                               aws_session_token=aws_creds['sessionToken'],
                               region_name=AWS_REGION).client("s3")
        else:
            s3 = boto3.Session(region_name=AWS_REGION).client("s3")

        product_download_path = self._s3_download(url, s3, str(target_dirpath))
        return product_download_path.resolve()

    @backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
    def _handle_url_redirect(self, url, token):
        if not validators.url(url):
            raise Exception(f"Malformed URL: {url}")

        r = requests.get(url, allow_redirects=False)

        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        return requests.get(r.headers["Location"], headers=headers, allow_redirects=True)

    @ttl_cache(ttl=3300)  # 3300s == 55m. Refresh credentials before expiry. Note: validity period is 60 minutes
    def get_aws_creds(self, token):
        return self._get_aws_creds(token)

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=3,
                          jitter=None,
                          giveup=fatal_code)
    def _get_aws_creds(self, token):
        with requests.get(self.cfg["DAAC_S3_CRED_URLS"][self.daac_s3_cred_settings_key],
                          headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()

            return r.json()

    @backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
    def _s3_download(self, url, s3, tmp_dir, staging_area=""):
        file_name = PurePath(url).name
        target_key = str(Path(staging_area, file_name))

        source = url[len("s3://"):].partition("/")
        source_bucket = source[0]
        source_key = source[2]

        s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

        return Path(f"{tmp_dir}/{target_key}")
