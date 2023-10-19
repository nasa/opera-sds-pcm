import logging
from pathlib import PurePath, Path

import requests
import requests.utils

from data_subscriber.download import DaacDownload
from data_subscriber.url import _has_url, _to_url, _to_https_url, _rtc_url_to_chunk_id

logger = logging.getLogger(__name__)


class AsfDaacRtcDownload(DaacDownload):
    def perform_download(self,
            session: requests.Session,
            es_conn,
            downloads: list[dict],
            args,
            token,
            job_id
    ):
        for download in downloads:
            if not _has_url(download):
                continue

            if args.transfer_protocol == "https":
                product_url = _to_https_url(download)
            else:
                product_url = _to_url(download)

            logger.info(f"Processing {product_url=}")
            product_id = _rtc_url_to_chunk_id(product_url, str(download['revision_id']))

            product_download_dir = self.downloads_dir / product_id
            product_download_dir.mkdir(exist_ok=True)

            # download product
            if args.dry_run:
                logger.debug(f"{args.dry_run=}. Skipping download.")
                continue

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

        with requests.get("https://sentinel1.asf.alaska.edu/s3credentials",
                          headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()

            return r.json()
