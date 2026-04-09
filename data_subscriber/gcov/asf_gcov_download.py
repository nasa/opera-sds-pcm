import os
from pathlib import PurePath
from datetime import datetime, timezone
from os.path import basename, splitext

from concurrent.futures import ThreadPoolExecutor, as_completed

from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from opera_commons.logger import get_logger
from data_subscriber.gcov_utils import load_mgrs_track_frame_db, submit_dswx_ni_job, get_gcov_products_to_process, split_mgrs_set_id_and_cycle_number
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.job_util import is_running_outside_verdi_worker_context

class AsfDaacGcovDownload(AsfDaacRtcDownload):
    def __init__(self, provider, mgrs_track_frame_db_file=None):
        super().__init__(provider)
        self.logger = get_logger()

        # source track frame db from ancillary bucket or loads local copy
        self.mgrs_track_frame_db = load_mgrs_track_frame_db(mgrs_track_frame_db_file=mgrs_track_frame_db_file)    
    
    def run_download(self, args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=True):
        provider = args.provider  # "ASF-GCOV" / "ASF-NISAR-GCOV"
        settings = SettingsConf().cfg

        if not is_running_outside_verdi_worker_context():
            job_context = JobContext("_context.json").ctx
            product_metadata = job_context["product_metadata"]
            self.logger.info(f"{product_metadata=}")
        
        mgrs_set_ids_and_cycle_numbers_to_process = [
            split_mgrs_set_id_and_cycle_number(mgrs_set_id_and_cycle_number)
            for mgrs_set_id_and_cycle_number in args.batch_ids
        ]
        sets_to_process = get_gcov_products_to_process(mgrs_set_ids_and_cycle_numbers_to_process, es_conn)

        use_https = args.transfer_protocol == 'https'

        if use_https:
            self.logger.info('Downloading L2 NISAR GCOV products over HTTPS')

            product_urls = set()

            for set_to_download in sets_to_process:
                for url in set_to_download.gcov_input_product_https_urls:
                    product_urls.add(url)

            product_urls = list(product_urls)
            localized_url_map = {}

            with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() + 4)) as executor:
                futures = [
                    executor.submit(self._localize_url_single, url, token, i, len(product_urls))
                    for i, url in enumerate(product_urls, start=1)
                ]

                for future in as_completed(futures):
                    url, loclized_path = future.result()
                    localized_url_map[url] = loclized_path

            self.logger.info('Pushing downloaded L2 NISAR GCOV products to OPERA S3')

            for set_to_download in sets_to_process:
                batch_id = f'{set_to_download.mgrs_set_id}${set_to_download.cycle_number}'
                product_paths = [localized_url_map[url] for url in set_to_download.gcov_input_product_https_urls]

                set_to_download.gcov_input_product_urls = concurrent_s3_client_try_upload_file(
                    bucket=settings['DATASET_BUCKET'],
                    key_prefix=f'tmp/dswx_ni/{batch_id}',
                    files=product_paths
                )
        else:
            self.logger.info('Bypassing downloads in favor of direct S3 localization from the DAAC')

        for set_to_download in sets_to_process:
            doc_ids = []

            for url in set_to_download.gcov_input_product_urls:
                filename = basename(url)
                doc_ids.append(f'{splitext(filename)[0]}${set_to_download.mgrs_set_id}')

            for doc_id in set(doc_ids):
                self.logger.info(f'Marking doc {doc_id} as downloaded')
                es_conn.mark_product_as_downloaded(doc_id, job_id)

        return self.submit_dswx_ni_job_submission_handler(sets_to_process, settings)

    def _localize_url_single(self, url, token, counter, num):
        self.logger.info(f'Downloading {url} {counter}/{num}')

        product_filepath = self.download_asf_product(url, token, self.downloads_dir)
        self.logger.info(f'Downloaded {url} -> {product_filepath}')

        return url, product_filepath

    def submit_dswx_ni_job_submission_handler(self, sets_to_process, settings):
        self.logger.info(f"Triggering DSWx-NI jobs for {len(sets_to_process)} unique MGRS sets and cycle numbers to process")
        jobs = self.trigger_dswx_ni_jobs(sets_to_process, settings)
        return jobs

    def create_dswx_ni_job_params(self, set_to_process):
        metadata = {
            "dataset": f"L3_DSWx_NI-{set_to_process.mgrs_set_id}-{set_to_process.cycle_number}",
            "metadata": {
                "mgrs_set_id": set_to_process.mgrs_set_id,
                "cycle_number": set_to_process.cycle_number,
                "product_paths": {"L2_NISAR_GCOV": set_to_process.gcov_input_product_urls},  # The S3 paths to localize
                "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "FileName": set_to_process.mgrs_set_id,
                "id": set_to_process.mgrs_set_id,
                "bounding_box": self.mgrs_track_frame_db.get_bounding_box_for_mgrs_set_id(set_to_process.mgrs_set_id),
                "Files": [
                    {
                        "FileName": PurePath(s3_path).name,
                        "FileSize": 1, 
                        "FileLocation": s3_path,
                        "id": PurePath(s3_path).name,
                        "product_paths": "$.product_paths"
                    } for s3_path in set_to_process.gcov_input_product_urls
                ]
            }
        }
        return [{
            "name": "mgrs_set_id",
            "from": "value",
            "type": "text",
            "value": set_to_process.mgrs_set_id
        }, {
            "name": "cycle_number",
            "from": "value",
            "type": "text",
            "value": set_to_process.cycle_number
        }, {
            "name": "gcov_input_product_urls",
            "from": "value",
            "type": "object",
            "value": set_to_process.gcov_input_product_urls
        },
        {
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": metadata
        }]

    def trigger_dswx_ni_jobs(self, sets_to_process, settings):
        return [
            submit_dswx_ni_job(
                params=self.create_dswx_ni_job_params(set_to_process),
                job_queue=f'opera-job_worker-{"sciflo-l3_dswx_ni"}',
                job_name=f"job-WF-SCIFLO_L3_DSWx_NI-{set_to_process.mgrs_set_id}-{set_to_process.cycle_number}",
                release_version=settings["RELEASE_VERSION"]
            )
            for set_to_process in sets_to_process
        ]