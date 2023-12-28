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
from util.job_submitter import try_submit_mozart_job

logger = logging.getLogger(__name__)


class AsfDaacCslcDownload(AsfDaacRtcDownload):
    async def run_download(self, args, token, es_conn, netloc, username, password, job_id, rm_downloads_dir=True):

        # There should always be only one batch_id
        batch_id = args.batch_ids[0]
        settings = SettingsConf().cfg

        # First, download the files from ASF
        product_to_product_filepaths_map: dict[str, set[Path]] = await super().run_download(args, token, es_conn, netloc, username, password, job_id, rm_downloads_dir=False)

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


        # TODO: This code differs from data_subscriber/rtc/rtc_job_submitter.py. Ideally both should be refactored into a common function
        # Now submit DISP-S1 SCIFLO job
        logger.info(f"Submitting DISP-S1 SCIFLO job")

        product = {
            "_id": batch_id,
            "_source": {
                "dataset": "dummy_dataset",
                "metadata": {
                    "batch_id": batch_id,
                    "product_paths": {"L2_DISP_S1": s3paths},
                    "FileName": batch_id,
                    "id": batch_id,
                    "bounding_box": None, #TODO: Do we need this?
                    "Files": [
                        {
                            "FileName": PurePath(s3path).name,
                            "FileSize": 1,
                            "FileLocation": os.path.dirname(s3path),
                            "id": PurePath(s3path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3path in s3paths
                    ]
                }
            }
        }

        return try_submit_mozart_job(
            product=product,
            job_queue=f'opera-job_worker-{"sciflo-l3_disp_s1"}',
            rule_name=f'trigger-{"SCIFLO_L3_DISP_S1"}',
            params=self.create_job_params(params),
            job_spec=f'job-{"SCIFLO_L3_DISP_S1"}:{args.release_version}',
            job_type=f'hysds-io-{"SCIFLO_L3_DISP_S1"}:{args.release_version}',
            job_name=f'job-WF-{"SCIFLO_L3_DISP_S1"}'
        )

    def create_job_params(self, product):
        return [
            {
                "name": "dataset_type",
                "from": "value",
                "type": "text",
                "value": "L2_CSLC_S1"
            },
            {
                "name": "input_dataset_id",
                "type": "text",
                "from": "value",
                "value": product["_source"]["metadata"]["batch_id"]
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": product["_source"]
            }
        ]



