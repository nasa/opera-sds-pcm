
import copy
import logging
import os

from os.path import basename
from pathlib import PurePath, Path

from data_subscriber import ionosphere_download
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job

from data_subscriber.cslc_utils import (localize_disp_frame_burst_json,
                                        split_download_batch_id,
                                        get_bounding_box_for_frame)

logger = logging.getLogger(__name__)


class AsfDaacCslcDownload(AsfDaacRtcDownload):

    def __init__(self, provider):
        super().__init__(provider)
        self.disp_burst_map, self.burst_to_frame, metadata, version = localize_disp_frame_burst_json()

    async def run_download(self, args, token, es_conn, netloc, username, password, job_id, rm_downloads_dir=True):

        settings = SettingsConf().cfg
        product_id = "_".join([batch_id for batch_id in args.batch_ids])
        logger.info(f"{product_id=}")
        cslc_s3paths = []
        ionosphere_s3paths = []

        new_args = copy.deepcopy(args)
        for batch_id in args.batch_ids:
            logger.info(f"Downloading CSLC files for batch {batch_id}")
            new_args.batch_ids = [batch_id]

            # First, download the files from ASF
            product_to_filepaths: dict[str, set[Path]] = await super().run_download(new_args, token, es_conn,
                                                                                    netloc, username, password,
                                                                                    job_id, rm_downloads_dir=False)

            logger.info(f"Uploading CSLC input files to S3")
            cslc_files_to_upload = [fp for fp_set in product_to_filepaths.values() for fp in fp_set]
            cslc_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                     key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                     files=cslc_files_to_upload))

            # Download all Ionosphere files corresponding to the dates covered by the
            # input CSLC set
            logger.info(f"Downloading Ionosphere files for {batch_id}")
            ionosphere_paths = self.download_ionosphere_files_for_cslc_batch(cslc_files_to_upload,
                                                                             self.downloads_dir)

            logger.info(f"Uploading Ionosphere files to S3")
            ionosphere_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                           key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                           files=ionosphere_paths))

            # Delete the files from the file system after uploading to S3
            if rm_downloads_dir:
                logger.info("Removing downloaded files from local filesystem")
                for fp_set in product_to_filepaths.values():
                    for fp in fp_set:
                        os.remove(fp)

                for iono_file in ionosphere_paths:
                    os.remove(iono_file)

        # Compute bounding box for frame. All batches should have the same frame_id so we pick the first one
        frame_id, _ = split_download_batch_id(args.batch_ids[0])
        frame = self.disp_burst_map[int(frame_id)]
        bounding_box = get_bounding_box_for_frame(frame)
        print(f'{bounding_box=}')

        # TODO: This code differs from data_subscriber/rtc/rtc_job_submitter.py. Ideally both should be refactored into a common function
        # Now submit DISP-S1 SCIFLO job
        logger.info(f"Submitting DISP-S1 SCIFLO job")

        product = {
            "_id": product_id,
            "_source": {
                "dataset": f"L3_DISP_S1-{product_id}",
                "metadata": {
                    "batch_id": product_id,
                    "frame_id": frame_id, # frame_id should be same for all download batches
                    "product_paths": {
                        "L2_CSLC_S1": cslc_s3paths,
                        "IONOSPHERE_TEC": ionosphere_s3paths
                    },
                    "FileName": product_id,
                    "id": product_id,
                    "bounding_box": bounding_box,
                    "Files": [
                        {
                            "FileName": PurePath(s3path).name,
                            "FileSize": 1,
                            "FileLocation": os.path.dirname(s3path),
                            "id": PurePath(s3path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3path in cslc_s3paths + ionosphere_s3paths
                    ]
                }
            }
        }

        proc_mode_suffix = ""
        if "proc_mode" in args and args.proc_mode == "historical":
            proc_mode_suffix = "_hist"

        return try_submit_mozart_job(
            product=product,
            job_queue=f'opera-job_worker-sciflo-l3_disp_s1{proc_mode_suffix}',
            rule_name=f'trigger-SCIFLO_L3_DISP_S1{proc_mode_suffix}',
            params=self.create_job_params(product),
            job_spec=f'job-SCIFLO_L3_DISP_S1{proc_mode_suffix}:{settings["RELEASE_VERSION"]}',
            job_type=f'hysds-io-SCIFLO_L3_DISP_S1{proc_mode_suffix}:{settings["RELEASE_VERSION"]}',
            job_name=f'job-WF-SCIFLO_L3_DISP_S1{proc_mode_suffix}'
        )

    def get_downloads(self, args, es_conn):
        # For CSLC download, the batch_ids are globally unique so there's no need to query for dates
        all_downloads = []
        for batch_id in args.batch_ids:
            downloads = es_conn.get_download_granule_revision(batch_id)
            logger.info(f"Got {len(downloads)=} downloads for {batch_id=}")
            all_downloads.extend(downloads)

        return all_downloads

    def download_ionosphere_files_for_cslc_batch(self, cslc_files, download_dir):
        # Reduce the provided CSLC paths to just the filenames
        cslc_files = list(map(lambda path: basename(path), cslc_files))

        downloaded_ionosphere_dates = set()
        downloaded_ionosphere_files = set()

        for cslc_file in cslc_files:
            logger.info(f'Downloading Ionosphere file for CSLC granule {cslc_file}')

            cslc_file_tokens = cslc_file.split('_')
            acq_datetime = cslc_file_tokens[4]
            acq_date = acq_datetime.split('T')[0]

            logger.debug(f'{acq_date=}')

            if acq_date not in downloaded_ionosphere_dates:
                ionosphere_filepath = ionosphere_download.download_ionosphere_correction_file(
                    dataset_dir=download_dir, product_filepath=cslc_file
                )

                downloaded_ionosphere_dates.add(acq_date)
                downloaded_ionosphere_files.add(ionosphere_filepath)
            else:
                logger.info(f'Already downloaded Ionosphere file for date {acq_date}, skipping...')

        return downloaded_ionosphere_files

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
