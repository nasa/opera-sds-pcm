import concurrent.futures
import os
import re
import uuid
from collections import defaultdict, namedtuple
from itertools import chain
from pathlib import PurePath, Path

import requests.utils
from more_itertools import first

from data_subscriber.catalog import ProductCatalog
from data_subscriber.download import DaacDownload
from data_subscriber.rtc.rtc_job_submitter import submit_dswx_s1_job_submissions_tasks
from data_subscriber.url import _to_urls, _to_https_urls, _rtc_url_to_chunk_id
from rtc_utils import rtc_product_file_revision_regex
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.job_util import is_running_outside_verdi_worker_context


class AsfDaacRtcDownload(DaacDownload):

    def __init__(self, provider):
        super().__init__(provider)
        self.daac_s3_cred_settings_key = "RTC_DOWNLOAD"

    def run_download(self, args, token, es_conn, netloc, username, password, cmr,
                     job_id, rm_downloads_dir=True):
        provider = args.provider  # "ASF-RTC"
        settings = SettingsConf().cfg

        if not is_running_outside_verdi_worker_context():
            job_context = JobContext("_context.json").ctx
            product_metadata = job_context["product_metadata"]
            self.logger.info(f"{product_metadata=}")

        affected_mgrs_set_id_acquisition_ts_cycle_indexes = args.batch_ids
        self.logger.info(f"{affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")

        # convert to "batch_id" mapping
        batch_id_to_products_map = {}

        for affected_mgrs_set_id_acquisition_ts_cycle_index in affected_mgrs_set_id_acquisition_ts_cycle_indexes:
            es_docs = es_conn.filter_catalog_by_sets([affected_mgrs_set_id_acquisition_ts_cycle_index])
            batch_id_to_products_map[affected_mgrs_set_id_acquisition_ts_cycle_index] = es_docs

        succeeded = []
        failed = []

        # create args for downloading products
        Namespace = namedtuple(
            "Namespace",
            ["provider", "transfer_protocol", "batch_ids", "dry_run", "smoke_run"],
            defaults=[provider, args.transfer_protocol, None, args.dry_run, args.smoke_run]
        )

        uploaded_batch_id_to_products_map = {}
        uploaded_batch_id_to_s3paths_map = {}

        for batch_id, product_burstset in batch_id_to_products_map.items():
            args_for_downloader = Namespace(provider=provider, batch_ids=[batch_id])

            run_download_kwargs = {
                "token": token,
                "es_conn": es_conn,
                "netloc": netloc,
                "username": username,
                "password": password,
                "cmr": cmr,
                "job_id": job_id
            }

            product_to_product_filepaths_map: dict[str, set[Path]] = super().run_download(
                args=args_for_downloader, **run_download_kwargs, rm_downloads_dir=False
            )

            self.logger.info("Uploading MGRS burst set files to S3")
            burst_id_to_files_to_upload = defaultdict(set)

            for product_id, fp_set in product_to_product_filepaths_map.items():
                for fp in fp_set:
                    match_product_id = re.match(rtc_product_file_revision_regex, product_id)
                    burst_id = match_product_id.group("burst_id")
                    burst_id_to_files_to_upload[burst_id].add(fp)

            s3paths: list[str] = []

            for burst_id, filepaths in burst_id_to_files_to_upload.items():
                s3paths.extend(
                    concurrent_s3_client_try_upload_file(
                        bucket=settings["DATASET_BUCKET"],
                        key_prefix=f"tmp/dswx_s1/{batch_id}/{burst_id}",
                        files=filepaths
                    )
                )

            uploaded_batch_id_to_products_map[batch_id] = product_burstset
            uploaded_batch_id_to_s3paths_map[batch_id] = s3paths

            self.logger.info(f"Submitting MGRS burst set download job {batch_id=}, num_bursts={len(product_burstset)}")

            # create args for job-submissions
            args_for_job_submitter = namedtuple(
                "Namespace",
                ["chunk_size", "release_version"],
                defaults=[1, args.release_version]
            )()

            if args.dry_run:
                self.logger.info(f"{args.dry_run=}. Skipping job submission. Producing mock job ID")
                results = [uuid.uuid4()]
            else:
                self.logger.info(f"Submitting batches for DSWx-S1 job: {list(uploaded_batch_id_to_s3paths_map)}")
                job_submission_tasks = submit_dswx_s1_job_submissions_tasks(uploaded_batch_id_to_s3paths_map,
                                                                            args_for_job_submitter, settings)
                results = multithread_gather(job_submission_tasks)

            suceeded_batch = [job_id for job_id in results if isinstance(job_id, str)]
            failed_batch = [e for e in results if isinstance(e, Exception)]

            if suceeded_batch:
                for product in uploaded_batch_id_to_products_map[batch_id]:
                    if not product.get("dswx_s1_jobs_ids"):
                        product["dswx_s1_jobs_ids"] = []

                    product["dswx_s1_jobs_ids"].append(first(suceeded_batch))

                if args.dry_run:
                    self.logger.info(f"{args.dry_run=}. Skipping marking jobs as downloaded. Producing mock job ID")
                else:
                    es_conn.mark_products_as_job_submitted({batch_id: uploaded_batch_id_to_products_map[batch_id]})

                succeeded.extend(suceeded_batch)
                failed.extend(failed_batch)

                # manual cleanup since we needed to preserve downloads for manual s3 uploads
                if rm_downloads_dir:
                    for fp in chain.from_iterable(burst_id_to_files_to_upload.values()):
                        fp.unlink(missing_ok=True)

                    self.logger.info("Removed downloads from disk")

        return {
            "success": succeeded,
            "fail": failed
        }

    def perform_download(self, session: requests.Session, es_conn: ProductCatalog,
                         downloads: list[dict], args, token, job_id):
        self.logger.info(f"downloading {len(downloads)} documents")

        if args.dry_run:
            self.logger.info(f"{args.dry_run=}. Skipping download.")
            downloads = []

        product_to_product_filepaths_map = defaultdict(set)
        num_downloads = len(downloads)
        download_id_to_downloads_map = {download["id"]: download for download in downloads}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, os.cpu_count() + 4)) as executor:
            futures = [
                executor.submit(self.perform_download_single, download, token, args, download_counter, num_downloads)
                for download_counter, download in enumerate(downloads, start=1)
            ]
            list_product_id_product_filepath = [future.result() for future in concurrent.futures.as_completed(futures)]
            for product_id_product_filepath in list_product_id_product_filepath:
                for product_id, product_filepath, download_id, filesize in product_id_product_filepath:
                    product_to_product_filepaths_map[product_id].add(product_filepath)
                    if not download_id_to_downloads_map[download_id].get("filesize"):
                        download_id_to_downloads_map[download_id]["filesize"] = 0
                    download_id_to_downloads_map[download_id]["filesize"] += filesize

        for download in downloads:
            self.logger.info(f"Marking as downloaded. {download['id']=}")
            es_conn.mark_product_as_downloaded(download['id'], job_id, download_id_to_downloads_map[download["id"]]["filesize"])

        self.logger.info(f"downloaded {len(product_to_product_filepaths_map)} products")
        return product_to_product_filepaths_map

    def perform_download_single(self, download, token, args, download_counter, num_downloads):
        self.logger.info(f"Downloading {download_counter} of {num_downloads} downloads")

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
            self.logger.info(f"Processing {product_url=}")
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
            self.logger.info(f"{product_filepath=}")

            list_product_id_product_filepath.append((product_id, product_filepath, download["id"], os.path.getsize(product_filepath)))
        return list_product_id_product_filepath

    def download_asf_product(self, product_url, token: str, target_dirpath: Path):
        self.logger.info(f"Requesting from {product_url}")

        asf_response = self._handle_url_redirect(product_url, token)
        asf_response.raise_for_status()

        product_filename = PurePath(product_url).name
        product_download_path = target_dirpath / product_filename
        with open(product_download_path, "wb") as file:
            file.write(asf_response.content)
        return product_download_path.resolve()


def multithread_gather(job_submission_tasks):
    """
    Given a list of tasks, executes them concurrently and gathers the results.
    Exceptions are returned as results rather than re-raised.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, os.cpu_count() + 4)) as executor:
        futures = [executor.submit(job_submission_task) for job_submission_task in job_submission_tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                result = exc
            results.append(result)
    return results
