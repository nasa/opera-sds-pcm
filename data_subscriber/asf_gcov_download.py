from pathlib import PurePath
from datetime import datetime, timezone

from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from opera_commons.logger import get_logger
from data_subscriber.gcov_utils import load_mgrs_track_frame_db, submit_dswx_ni_job
from util.job_util import is_running_outside_verdi_worker_context

class AsfDaacGcovDownload(AsfDaacRtcDownload):
    def __init__(self, provider, mgrs_track_frame_db_file=None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.logger = get_logger()

        # source track frame db from ancillary bucket or loads local copy
        self.mgrs_track_frame_db = load_mgrs_track_frame_db(mgrs_track_frame_db_file=mgrs_track_frame_db_file)    
    
    def run_download(self, args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=True):
        provider = args.provider  # "ASF-GCOV"
        settings = SettingsConf().cfg

        if not is_running_outside_verdi_worker_context():
            job_context = JobContext("_context.json").ctx
            product_metadata = job_context["product_metadata"]
            self.logger.info(f"{product_metadata=}")
        
        mgrs_set_ids_and_cycle_numbers_to_process = args.batch_ids
        sets_to_process = get_gcov_products_to_process(mgrs_set_ids_and_cycle_numbers_to_process, es_conn)

    def submit_dswx_ni_job_submission_handler(self, sets_to_process, query_timerange):
        self.logger.info(f"Triggering DSWx-NI jobs for {len(sets_to_process)} unique MGRS sets and cycle numbers to process")
        jobs = self.trigger_dswx_ni_jobs(sets_to_process)
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
                "FileLocation": set_to_process.gcov_input_product_urls, 
                "id": set_to_process.mgrs_set_id,
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

    def trigger_dswx_ni_jobs(self, sets_to_process):
        return [submit_dswx_ni_job(
            params=self.create_dswx_ni_job_params(set_to_process),
            job_queue=self.args.job_queue,
            job_name=f"job-WF-SCIFLO_L3_DSWx_NI-{set_to_process.mgrs_set_id}-{set_to_process.cycle_number}",
            release_version=self.settings["RELEASE_VERSION"]
        ) for set_to_process in sets_to_process]