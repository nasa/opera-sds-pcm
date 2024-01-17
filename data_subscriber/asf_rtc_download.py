import concurrent.futures
import logging
import os
from collections import defaultdict
from pathlib import PurePath, Path

import backoff
import requests
import requests.utils
from requests.exceptions import HTTPError

from data_subscriber.download import DaacDownload
from data_subscriber.url import _to_urls, _to_https_urls, _rtc_url_to_chunk_id

logger = logging.getLogger(__name__)


def giveup_asf_daac_credentials_requests(e):
    """giveup function for use with @backoff decorator when issuing DAAC requests using blocking `requests` functions."""
    if isinstance(e, HTTPError):
        if e.response.status_code == 502:  # Bad Gateway. transient error when getting s3credentials
            return False
    return False


class AsfDaacRtcDownload(DaacDownload):
    def perform_download(
        self,
        session: requests.Session,
        es_conn,
        downloads: list[dict],
        args,
        token,
        job_id
    ):
        logger.info(f"downloading {len(downloads)} documents")

        if args.dry_run:
            logger.info(f"{args.dry_run=}. Skipping download.")
            downloads = []

        product_to_product_filepaths_map = defaultdict(set)
        if args.smoke_run:
            logger.info(f"{args.smoke_run=}. Capping downloads.")
            downloads = downloads[:1]
        num_downloads = len(downloads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, os.cpu_count() + 4)) as executor:
            futures = [
                executor.submit(self.perform_download_single, download, token, args, download_counter, num_downloads)
                for download_counter, download in enumerate(downloads, start=1)
            ]
            list_product_id_product_filepath = [future.result() for future in concurrent.futures.as_completed(futures)]
            for product_id_product_filepath in list_product_id_product_filepath:
                for product_id, product_filepath in product_id_product_filepath:
                    product_to_product_filepaths_map[product_id].add(product_filepath)

        for download in downloads:
            logger.info(f"Marking as downloaded. {download['id']=}")
            es_conn.mark_product_as_downloaded(download['id'], job_id)

        logger.info(f"downloaded {len(product_to_product_filepaths_map)} products")
        return product_to_product_filepaths_map

    def perform_download_single(self, download, token, args, download_counter, num_downloads):
        logger.info(f"Downloading {download_counter} of {num_downloads} downloads")

        if args.transfer_protocol == "https":
            product_urls = _to_https_urls(download)
        else:
            product_urls = _to_urls(download)

        list_product_id_product_filepath = []

        # Small hack: if product_urls is not a list, make it a list. This is used for CSLC downloads.
        # TODO: Change this more upstream so that this hack is not needed
        if not isinstance(product_urls, list):
            product_urls = [product_urls]

        for product_url in product_urls:
            logger.info(f"Processing {product_url=}")
            product_id = _rtc_url_to_chunk_id(product_url, str(download['revision_id']))
            product_download_dir = self.downloads_dir / product_id
            product_download_dir.mkdir(exist_ok=True)
            if product_url.startswith("s3"):
                product_filepath = self.download_product_using_s3(
                    product_url,
                    token,
                    target_dirpath=product_download_dir.resolve(),
                    args=args
                )
            else:
                product_filepath = self.download_asf_product(
                    product_url, token, product_download_dir
                )
            logger.info(f"{product_filepath=}")

            list_product_id_product_filepath.append((product_id, product_filepath))
        return list_product_id_product_filepath

    def download_asf_product(self, product_url, token: str, target_dirpath: Path):
        logger.info(f"Requesting from {product_url}")

        asf_response = self._handle_url_redirect(product_url, token)
        asf_response.raise_for_status()

        product_filename = PurePath(product_url).name
        product_download_path = target_dirpath / product_filename
        with open(product_download_path, "wb") as file:
            file.write(asf_response.content)
        return product_download_path.resolve()

    @backoff.on_exception(
        backoff.expo,
        exception=Exception,
        max_tries=3,
        jitter=None,
        giveup=giveup_asf_daac_credentials_requests
    )
    def _get_aws_creds(self, token):
        logger.info("entry")
        with requests.get("https://cumulus.asf.alaska.edu/s3credentials", headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()
            return r.json()
