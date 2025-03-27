from pathlib import PurePath, Path
import os
import urllib
import boto3
from datetime import datetime, timezone
import hashlib
import logging

from util.ctx_util import JobContext
from util.aws_util import concurrent_s3_client_try_upload_file
from util.job_util import is_running_outside_verdi_worker_context
from util.job_submitter import try_submit_mozart_job

from data_subscriber.url import rtc_for_dist_unique_id
from data_subscriber.dist_s1_utils import dist_s1_split_download_batch_id, localize_dist_burst_db
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.asf_cslc_download import AsfDaacCslcDownload
from util.conf_util import SettingsConf

class AsfDaacRtcForDistDownload(AsfDaacCslcDownload):

    def __init__(self, provider):
        super().__init__(provider)

        #self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = localize_dist_burst_db()

    def run_download(self, args, token, es_conn, netloc, username, password, cmr,
                     job_id, rm_downloads_dir=True):

        settings = SettingsConf().cfg

        if not is_running_outside_verdi_worker_context():
            job_context = JobContext("_context.json").ctx
            product_metadata = job_context["product_metadata"]
            self.logger.info(f"{product_metadata=}")
        else:
            # Get context by: 1) looking up the job_id in the GRQ ES index, 2) using that to get the context from Mozart ES

            product_metadata = None

        rtc_s3paths = []
        to_mark_downloaded = []

        # Sort the batch_ids by acq_cycle_index
        batch_ids = sorted(args.batch_ids, key=lambda batch_id: dist_s1_split_download_batch_id(batch_id)[1], reverse=True)
        latest_acq_cycle_index = dist_s1_split_download_batch_id(batch_ids[0])[1]
        tile_id = (dist_s1_split_download_batch_id(batch_ids[0])[0]).split("_")[0]

        for index, batch_id in enumerate(batch_ids):
            self.logger.info(f"Downloading RTC files for batch {batch_id}")

            granule_sizes = []

            if args.transfer_protocol == "https":
                # Need to skip over AsfDaacRtcDownload.run_download() and invoke base DaacDownload.run_download()
                rtc_products_to_filepaths: dict[str, set[Path]] = super(AsfDaacRtcDownload, self).run_download(
                    args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=False
                )
                self.logger.info(f"Uploading RTC input files to S3")
                rtc_files_to_upload = [fp for fp_set in rtc_products_to_filepaths.values() for fp in fp_set]
                rtc_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                         key_prefix=f"tmp/dist_s1/{batch_id}",
                                                                         files=rtc_files_to_upload))

                for granule_id, fp_set in rtc_products_to_filepaths.items():
                    filepath = list(fp_set)[0]
                    file_size = os.path.getsize(filepath)
                    granule_sizes.append((granule_id, file_size))

            # For s3 we can use the files directly so simply copy over the paths
            else:  # s3 or auto
                self.logger.info(
                    "Skipping download RTC bursts and instead using ASF S3 paths for direct SCIFLO PGE ingestion")

                '''For RTC download, the batch_ids are globally unique so there's no need to query for dates
                Granules are stored in either rtc_for_dist_catalog or baseline_rtc_catalog index. 
                We assume that the latest batch_id (defined as the one with the greatest acq_cycle_index) is stored in rtc_for_dist_catalog. 
                The rest are stored in baseline_rtc_catalog'''
                downloads = self.get_downloads(batch_id, es_conn)

                batch_rtc_s3paths = [download["s3_url"] for download in downloads]
                rtc_s3paths.extend(batch_rtc_s3paths)
                if len(batch_rtc_s3paths) == 0:
                    raise Exception(
                        f"No s3_path found for {batch_id}. You probably should specify https transfer protocol.")

                for p in batch_rtc_s3paths:
                    # Split the following into bucket name and key
                    # 's3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1/OPERA_L2_CSLC-S1_T122-260026-IW3_20231214T011435Z_20231215T075814Z_S1A_VV_v1.0/OPERA_L2_CSLC-S1_T122-260026-IW3_20231214T011435Z_20231215T075814Z_S1A_VV_v1.0.h5'
                    parsed_url = urllib.parse.urlparse(p)
                    bucket = parsed_url.netloc
                    key = parsed_url.path[1:]
                    granule_id = p.split("/")[-1]

                    try:
                        head_object = boto3.client("s3").head_object(Bucket=bucket, Key=key)
                        self.logger.info(f"Adding RTC file: {p}")
                    except Exception as e:
                        self.logger.error("Failed when accessing the S3 object:" + p)
                        raise e
                    file_size = int(head_object["ContentLength"])

                    granule_sizes.append((granule_id, file_size))

                rtc_files_to_upload = [Path(p) for p in batch_rtc_s3paths]  # Need this for querying static CSLCs

                rtc_products_to_filepaths = {}  # Dummy when trying to delete files later in this function

            # Create list of RTC files marked as downloaded, this will be used as the very last step in this function
            for granule_id, file_size in granule_sizes:
                native_id = granule_id.split(".h5")[0]  # remove file extension and revision id
                burst_id, _ = parse_r2_product_file_name(native_id, "L2_RTC_S1")
                unique_id = rtc_for_dist_unique_id(batch_id, burst_id)
                to_mark_downloaded.append((unique_id, file_size))

        # TODO: Look up bounding box for frame?

        # Now submit DISP-S1 SCIFLO job
        self.logger.info(f"Submitting DIST-S1 SCIFLO job")

        product = {
            "_id": batch_id,
            "_source": {
                "dataset": f"L3_DIST_S1-{batch_id}",
                "metadata": {
                    "batch_id": batch_id,
                    "mgrs_tile_id": tile_id,  # frame_id should be same for all download batches
                    "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "product_paths": {
                        "L2_RTC_S1": rtc_s3paths,
                    },
                    "FileName": batch_id,
                    "id": batch_id,
                    "bounding_box": None, #TOD: Fill this in
                    "acquisition_cycle": latest_acq_cycle_index,
                    "Files": [
                        {
                            "FileName": PurePath(s3path).name,
                            "FileSize": 1,  # TODO? Get file size? It's also 1 in rtc_job_submitter.py
                            "FileLocation": os.path.dirname(s3path),
                            "id": PurePath(s3path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3path in rtc_s3paths
                    ]
                }
            }
        }

        # Compute payload hash by first sorting cslc_s3paths, create a string out of it, and then computing md5 hash
        payload_hash = hashlib.md5("".join(sorted(rtc_s3paths)).encode()).hexdigest()
        logging.info(f"Computed payload hash for SCIFLO job submission: {payload_hash}")

        submitted = try_submit_mozart_job(
            product=product,
            job_queue=f'opera-job_worker-sciflo-l3_dist_s1',
            rule_name=f'trigger-SCIFLO_L3_DIST_S1',
            params=self.create_job_params(product),
            job_spec=f'job-SCIFLO_L3_DIST_S1:{settings["RELEASE_VERSION"]}',
            job_type=f'hysds-io-SCIFLO_L3_DIST_S1:{settings["RELEASE_VERSION"]}',
            job_name=f'job-WF-SCIFLO_L3_DIST_S1-batch-{batch_id}',
            payload_hash=payload_hash
        )

        # Mark the RTC files as downloaded in the RTC ES with the file size only after SCIFLO job has been submitted
        for unique_id, file_size in to_mark_downloaded:
            es_conn.mark_product_as_downloaded(unique_id, job_id, filesize=file_size)

        return submitted
