import json
import logging
import os
from datetime import datetime
from pathlib import PurePath, Path

import backoff
import requests
import requests.utils

from data_subscriber.download import DaacDownload
from data_subscriber.url import _has_url, _to_urls, _to_https_urls, _slc_url_to_chunk_id, form_batch_id

logger = logging.getLogger(__name__)

_S3_CREDS_SENTINEL_URL = "https://sentinel1.asf.alaska.edu/s3credentials"

class DaacDownloadAsf(DaacDownload):

    def __init__(self, provider):
        super().__init__(provider)
        self.daac_s3_cred_settings_key = "SLC_DOWNLOAD"

    """This is practically an abstract class. You should never instantiate this."""
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
                product_url = _to_https_urls(download)
            else:
                product_url = _to_urls(download)

            logger.info(f"Processing {product_url=}")
            product_id = _slc_url_to_chunk_id(product_url, str(download['revision_id']))

            product_download_dir = self.downloads_dir / product_id
            product_download_dir.mkdir(exist_ok=True)

            # download product
            if args.dry_run:
                logger.info(f"{args.dry_run=}. Skipping download.")
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

            logger.info(f"{product_filepath=}")

            logger.info(f"Marking as downloaded. {download['id']=}")
            es_conn.mark_product_as_downloaded(download['id'], job_id)

            logger.info(f"product_url_downloaded={product_url}")

            additional_metadata = {}
            try:
                additional_metadata['processing_mode'] = download['processing_mode']
            except:
                logger.warning("processing_mode not found in the slc_catalog ES index")

            if download.get("intersects_north_america"):
                logger.info("adding additional dataset metadata (intersects_north_america)")
                additional_metadata["intersects_north_america"] = True

            dataset_dir = self.extract_one_to_one(product, self.cfg, working_dir=Path.cwd(),
                                                  extra_metadata=additional_metadata,
                                                  name_postscript='-r'+str(download['revision_id']))

            self.update_pending_dataset_with_index_name(dataset_dir, '-r'+str(download['revision_id']))

            # Rename the dataset_dir to match the pattern w revision_id
            new_dataset_dir = dataset_dir.parent / form_batch_id(dataset_dir.name, str(download['revision_id']))
            logger.info(f"{new_dataset_dir}")
            os.rename(str(dataset_dir), str(new_dataset_dir))

            self.download_orbit_file(new_dataset_dir, product_filepath, additional_metadata)

            if (additional_metadata['processing_mode'] in ("historical", "reprocessing")):
                logger.info(
                    f"Processing mode is {additional_metadata['processing_mode']}. "
                    f"Attempting to download ionosphere correction file."
                )
                self.download_ionosphere_file(new_dataset_dir, product_filepath)

            logger.info(f"Removing {product_filepath}")
            product_filepath.unlink(missing_ok=True)

    def download_orbit_file(self, dataset_dir, product_filepath, additional_metadata):
        pass

    def download_ionosphere_file(self, dataset_dir, product_filepath):
        pass

    def download_asf_product(self, product_url, token: str, target_dirpath: Path):
        logger.info(f"Requesting from {product_url}")

        asf_response = self._handle_url_redirect(product_url, token)
        asf_response.raise_for_status()

        product_filename = PurePath(product_url).name
        product_download_path = target_dirpath / product_filename
        with open(product_download_path, "wb") as file:
            file.write(asf_response.content)
        return product_download_path.resolve()

    def update_pending_dataset_metadata_with_ionosphere_metadata(self, dataset_dir: PurePath, ionosphere_metadata: dict):
        pass

    def update_pending_dataset_with_index_name(self, dataset_dir: PurePath, postscript):
        logger.info("Updating dataset's dataset.json with index name")

        with Path(dataset_dir / f"{dataset_dir.name}{postscript}.dataset.json").open("r") as fp:
            dataset_json: dict = json.load(fp)

        with Path(dataset_dir / f"{dataset_dir.name}{postscript}.met.json").open("r") as fp:
            met_dict: dict = json.load(fp)

        dataset_json.update({
            "index": {
                "suffix": ("{version}_{dataset}-{date}".format(
                    version=dataset_json["version"],
                    dataset=met_dict["ProductType"],
                    date=datetime.utcnow().strftime("%Y.%m")
                )).lower()  # suffix index name with `-YYYY.MM
            }
        })

        with Path(dataset_dir / f"{dataset_dir.name}.dataset.json").open("w") as fp:
            json.dump(dataset_json, fp)