import concurrent.futures
import logging
import os
from collections import defaultdict
from pathlib import PurePath, Path

import requests
import requests.utils
from more_itertools import partition

from data_subscriber.download import DaacDownload
from data_subscriber.url import _has_url, _to_url, _to_https_url, _rtc_url_to_chunk_id

logger = logging.getLogger(__name__)


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
            logger.debug(f"{args.dry_run=}. Skipping download.")
            downloads = []

        downloads[:], downloads_without_urls = partition(lambda it: not _has_url(it), downloads)

        if list(downloads_without_urls):
            logger.error(f"Some documents do not have a download URL")

        product_to_product_filepaths_map = defaultdict(set)
        downloads = downloads[:1]  # TODO chrisjrd: useful for testing locally. remove before final commit
        num_downloads = len(downloads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, os.cpu_count() + 4)) as executor:
            futures = [
                executor.submit(self.perform_download_single, download, token, args, download_counter, num_downloads)
                for download_counter, download in enumerate(downloads, start=1)
            ]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
            for result in results:
                product_id, product_filepath = result
                product_to_product_filepaths_map[product_id].add(product_filepath)

        for download in downloads:
            logger.info(f"Marking as downloaded. {download['id']=}")
            es_conn.mark_product_as_downloaded(download['id'], job_id)

        logger.info(f"downloaded {len(product_to_product_filepaths_map)} products")
        return product_to_product_filepaths_map

    def perform_download_single(self, download, token, args, download_counter, num_downloads):
        logger.info(f"Downloading {download_counter} of {num_downloads} downloads")

        if args.transfer_protocol == "https":
            product_url = _to_https_url(download)
        else:
            product_url = _to_url(download)
        logger.info(f"Processing {product_url=}")
        product_id = _rtc_url_to_chunk_id(product_url, str(download['revision_id']))
        product_download_dir = self.downloads_dir / product_id
        product_download_dir.mkdir(exist_ok=True)
        if product_url.startswith("s3"):
            product = product_filepath = self.download_product_using_s3(
                product_url,
                token,
                target_dirpath=product_download_dir.resolve(),
                args=args
            )
        else:
            product = product_filepath = self.download_asf_product(
                product_url, token, product_download_dir
            )
        logger.info(f"{product_filepath=}")
        return product_id, product_filepath

    def download_asf_product(self, product_url, token: str, target_dirpath: Path):
        logger.info(f"Requesting from {product_url}")

        asf_response = self._handle_url_redirect(product_url, token)
        asf_response.raise_for_status()

        product_filename = PurePath(product_url).name
        product_download_path = target_dirpath / product_filename
        with open(product_download_path, "wb") as file:
            file.write(asf_response.content)
        return product_download_path.resolve()

    def _get_aws_creds(self, token):
        logger.info("entry")
        with requests.get("https://cumulus.asf.alaska.edu/s3credentials", headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()
            return r.json()
