import logging
import os
from collections import defaultdict
from pathlib import PurePath, Path

import requests
import requests.utils
from more_itertools import partition

import extractor.extract
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.url import _has_url, _to_url, _to_https_url, _rtc_url_to_chunk_id
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf

logger = logging.getLogger(__name__)


class AsfDaacCslcDownload(AsfDaacRtcDownload):
    def run_download(self, args, token, es_conn, netloc, username, password, job_id, rm_downloads_dir=True):

        # TODO: this is a hack to get the batch_id. It should be passed in as an argument or dynamically generated
        batch_id = "abc"
        settings = SettingsConf().cfg

        # First, download the files from ASF
        product_to_product_filepaths_map: dict[str, set[Path]] = super().run_download(args, token, es_conn, netloc, username, password, job_id, rm_downloads_dir=False)

        # TODO: This code is copied from data_subscriber/query.py. It should be refactored into a common function
        logger.info("Extracting metadata from CSLC products")
        product_to_products_metadata_map = defaultdict(list[dict])
        for product, filepaths in product_to_product_filepaths_map.items():
            for filepath in filepaths:
                dataset_id, product_met, dataset_met = extractor.extract.extract_in_mem(
                    product_filepath=filepath,
                    product_types=settings["PRODUCT_TYPES"],
                    workspace_dirpath=Path.cwd()
                )
                product_to_products_metadata_map[product].append(product_met)

        logger.info(f"Uploading CSLC input files to S3")
        files_to_upload = [fp for fp_set in product_to_product_filepaths_map.values() for fp in fp_set]
        s3paths: list[str] = concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                  key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                  files=files_to_upload)

