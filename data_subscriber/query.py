
import argparse
import asyncio
import hashlib
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import dateutil.parser
from more_itertools import chunked

from commons.logger import get_logger
from data_subscriber.cmr import (async_query_cmr,
                                 ProductType, DateTimeRange, PGEProduct,
                                 COLLECTION_TO_PRODUCT_TYPE_MAP,
                                 COLLECTION_TO_PROVIDER_TYPE_MAP)
from data_subscriber.cslc.cslc_dependency import CSLCDependency
from data_subscriber.cslc_utils import split_download_batch_id, save_blocked_download_job
from data_subscriber.geojson_utils import (localize_include_exclude,
                                           filter_granules_by_regions)
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from data_subscriber.url import form_batch_id, _slc_url_to_chunk_id
from hysds_commons.job_utils import submit_mozart_job


class CmrQuery:
    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        self.logger = get_logger()
        self.args = args
        self.token = token
        self.es_conn = es_conn
        self.cmr = cmr
        self.job_id = job_id
        self.settings = settings
        self.proc_mode = args.proc_mode

        self.validate_args()

    def validate_args(self):
        pass

    def run_query(self):
        query_dt = datetime.now()
        now = datetime.utcnow()
        query_timerange: DateTimeRange = get_query_timerange(self.args, now)

        self.logger.info("CMR Query STARTED")

        granules = self.query_cmr(query_timerange, now)

        self.logger.info("CMR Query FINISHED")

        # Get rid of duplicate granules. This happens often for CSLC and TODO: probably RTC
        granules = self.eliminate_duplicate_granules(granules)

        if self.args.smoke_run:
            self.logger.info(f"{self.args.smoke_run=}. Restricting to 1 granule(s).")
            granules = granules[:1]

        # If processing mode is historical, apply the include/exclude-region filtering
        if self.proc_mode == "historical":
            self.logger.info(f"Processing mode is historical so applying include and exclude regions...")

            # Fetch all necessary geojson files from S3
            localize_include_exclude(self.args)
            granules[:] = filter_granules_by_regions(granules, self.args.include_regions, self.args.exclude_regions)

        # TODO: This function only applies to CSLC, merge w RTC at some point
        # Generally this function returns the same granules as input but for CSLC (and RTC if also refactored),
        # triggering logic is applied to granules to determine which ones need to be downloaded
        download_granules = self.determine_download_granules(granules)

        '''TODO: Optional. For CSLC query jobs, make sure that we got all the bursts here according to database json.
        Otherwise, fail this job'''

        self.logger.info("Granule Cataloguing STARTED")

        self.catalog_granules(granules, query_dt)

        self.logger.info("Granule Cataloguing FINISHED")

        # TODO: This function only applies to RTC, merge w CSLC at some point
        batch_id_to_products_map = self.refresh_index()

        if self.args.subparser_name == "full":
            self.logger.info("Skipping download job submission. Download will be performed directly.")

            if COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.RTC:
                self.args.provider = COLLECTION_TO_PROVIDER_TYPE_MAP[self.args.collection]
                self.args.batch_ids = self.affected_mgrs_set_id_acquisition_ts_cycle_indexes
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.CSLC:
                self.args.provider = COLLECTION_TO_PROVIDER_TYPE_MAP[self.args.collection]
                self.args.chunk_size = self.args.k
                self.args.batch_ids = list(set(granule["download_batch_id"] for granule in download_granules))

            return {"download_granules": download_granules}

        if self.args.no_schedule_download:
            self.logger.info("Forcefully skipping download job submission.")
            return {"download_granules": download_granules}

        if not self.args.chunk_size:
            self.logger.info("Insufficient chunk size (%s). Skipping download job submission.", str(self.args.chunk_size))
            return {"download_granules": download_granules}

        if COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.RTC:
            job_submission_tasks = submit_rtc_download_job_submissions_tasks(batch_id_to_products_map.keys(), self.args, self.settings)
            results = asyncio.gather(*job_submission_tasks, return_exceptions=True)
        else:
            job_submission_tasks = self.download_job_submission_handler(download_granules, query_timerange)
            results = job_submission_tasks

        succeeded = [job_id for job_id in results if isinstance(job_id, str)]
        failed = [e for e in results if isinstance(e, Exception)]

        self.logger.debug(f"{results=}")
        self.logger.info(f"{succeeded=}")
        self.logger.info(f"{failed=}")
        self.logger.debug(f"{download_granules=}")

        return {
            "success": succeeded,
            "fail": failed,
            "download_granules": download_granules
        }

    def query_cmr(self, timerange, now: datetime):
        granules = asyncio.run(async_query_cmr(self.args, self.token, self.cmr, self.settings, timerange, now))
        return granules

    def eliminate_duplicate_granules(self, granules):
        """
        If we have two granules with the same granule_id, we only keep the one
        with the latest revision_id. This should be very rare.
        """
        granule_dict = {}

        for granule in granules:
            granule_id = granule.get("granule_id")

            if granule_id in granule_dict:
                if granule.get("revision_id") > granule_dict[granule_id].get("revision_id"):
                    granule_dict[granule_id] = granule
            else:
                granule_dict[granule_id] = granule

        granules = list(granule_dict.values())

        return granules

    def prepare_additional_fields(self, granule, args, granule_id):
        additional_fields = {
            "revision_id": granule.get("revision_id"),
            "processing_mode": args.proc_mode
        }

        return additional_fields

    def extend_additional_records(self, granules):
        pass

    def determine_download_granules(self, granules):
        return granules

    def catalog_granules(self, granules, query_dt, force_es_conn = None):

        es_conn = force_es_conn if force_es_conn else self.es_conn

        for granule in granules:
            granule_id = granule.get("granule_id")

            additional_fields = self.prepare_additional_fields(granule, self.args, granule_id)

            update_url_index(
                es_conn,
                granule.get("filtered_urls"),
                granule,
                self.job_id,
                query_dt,
                temporal_extent_beginning_dt=dateutil.parser.isoparse(granule["temporal_extent_beginning_datetime"]),
                revision_date_dt=dateutil.parser.isoparse(granule["revision_date"]),
                **additional_fields
            )

            self.update_granule_index(granule)

    def update_granule_index(self, granule):
        pass

    def refresh_index(self):
        pass

    def download_job_submission_handler(self, granules, query_timerange):
        batch_id_to_urls_map = defaultdict(set)

        # DIST-S1 products are generated from RTC input files. RTC input files are also used by DSWx-S1 products.
        # COLLECTION_TO_PRODUCT_TYPE_MAP does not allow for one collection to be used by multiple products so we'll deal piece-wise for now.
        # TODO: Refactor in the future.
        if self.args.product and self.args.product == PGEProduct.DIST_1:
            product_type = PGEProduct.DIST_1
        else:
            product_type = COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection]

        for granule in granules:
            granule_id = granule.get("granule_id")
            revision_id = granule.get("revision_id")

            if granule.get("filtered_urls"):
                # group URLs by this mapping func. E.g. group URLs by granule_id
                if product_type == ProductType.HLS:
                    url_grouping_func = form_batch_id
                elif product_type == ProductType.SLC:
                    url_grouping_func = _slc_url_to_chunk_id
                elif product_type == ProductType.CSLC:
                    # For CSLC force chunk_size to be the same as k in args
                    if self.args.k:
                        self.args.chunk_size = self.args.k
                elif product_type == PGEProduct.DIST_1:
                    url_grouping_func = None
                elif product_type in (ProductType.RTC, ProductType.CSLC_STATIC):
                    raise NotImplementedError(
                        f"Download job submission is not supported for product type {product_type}"
                    )
                else:
                    raise ValueError(f"Can't use {self.args.collection=} to select grouping function.")

                for filter_url in granule.get("filtered_urls"):
                    if product_type == ProductType.CSLC:
                        batch_id_to_urls_map[granule["download_batch_id"]].add(filter_url)
                    else:
                        batch_id_to_urls_map[url_grouping_func(granule_id, revision_id)].add(filter_url)

        self.logger.debug(f"{batch_id_to_urls_map=}")

        job_submission_tasks = self.submit_download_job_submissions_tasks(batch_id_to_urls_map, query_timerange)

        return job_submission_tasks

    def get_download_chunks(self, batch_id_to_urls_map):
        return chunked(batch_id_to_urls_map.items(), n=self.args.chunk_size)

    def submit_download_job_submissions_tasks(self, batch_id_to_urls_map, query_timerange):
        job_submission_tasks = []

        if COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.CSLC:
            # Note that self.disp_burst_map_hist and self.blackout_dates_obj are created in the child class
            cslc_dependency = CSLCDependency(
                self.args.k, self.args.m, self.disp_burst_map_hist, self.args,
                self.token, self.cmr, self.settings, self.blackout_dates_obj
            )

        for batch_chunk in self.get_download_chunks(batch_id_to_urls_map):
            chunk_batch_ids = []
            chunk_urls = []
            for batch_id, urls in batch_chunk:
                chunk_batch_ids.append(batch_id)
                chunk_urls.extend(urls)

            # If we are downlaoding SLC input data, we will compute payload hash using the granule_id without the revision_id
            # NOTE: This will only work properly if the chunk size is 1 which should always be the case for SLC downloads
            payload_hash = None
            if COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.SLC:
                granule_to_hash = ''
                for batch_id in chunk_batch_ids:
                    granule_id, revision_id = self.es_conn.granule_and_revision(batch_id)
                    granule_to_hash += granule_id

                payload_hash = hashlib.md5(granule_to_hash.encode()).hexdigest()

            self.logger.debug(f"{chunk_batch_ids=}")
            self.logger.debug(f"{payload_hash=}")
            self.logger.debug(f"{chunk_urls=}")

            params = self.create_download_job_params(query_timerange, chunk_batch_ids)

            product_type = COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection].lower()
            if COLLECTION_TO_PRODUCT_TYPE_MAP[self.args.collection] == ProductType.CSLC:
                frame_id = split_download_batch_id(chunk_batch_ids[0])[0]
                acq_indices = [split_download_batch_id(chunk_batch_id)[1] for chunk_batch_id in chunk_batch_ids]
                job_name = f"job-WF-{product_type}_download-frame-{frame_id}-acq_indices-{min(acq_indices)}-to-{max(acq_indices)}"

                # See if all the compressed cslcs are satisfied. If not, do not submit the job. Instead, save all the job info in ES
                # and wait for the next query to come in. Any acquisition index will work because all batches
                # require the same compressed cslcs
                if not cslc_dependency.compressed_cslc_satisfied(frame_id, acq_indices[0], self.es_conn.es_util):
                    self.logger.info(f"Not all compressed CSLCs are satisfied so this download job is blocked until they are satisfied.")
                    save_blocked_download_job(self.es_conn.es_util, self.settings["RELEASE_VERSION"],
                                              product_type, params, self.args.job_queue, job_name,
                                              frame_id, acq_indices[0], self.args.k, self.args.m, chunk_batch_ids)

                    # While we technically do not have a download job here, we mark it as so in ES.
                    # That's because this flag is used to determine if the granule has been triggered or not
                    for batch_id, urls in batch_chunk:
                        self.es_conn.mark_download_job_id(batch_id, "PENDING")

                    continue # don't actually submit download job

            else:
                job_name = f"job-WF-{product_type}_download-{chunk_batch_ids[0]}"

            download_job_id = submit_download_job(release_version=self.settings["RELEASE_VERSION"],
                    product_type=product_type,
                    params=params,
                    job_queue=self.args.job_queue,
                    job_name = job_name,
                    payload_hash = payload_hash
                )

            # Record download job id in ES
            for batch_id, urls in batch_chunk:
                self.es_conn.mark_download_job_id(batch_id, download_job_id)

            job_submission_tasks.append(download_job_id)

        return job_submission_tasks

    def create_download_job_params(self, query_timerange, chunk_batch_ids):
        args = self.args
        download_job_params = [
            {
                "name": "batch_ids",
                "value": "--batch-ids " + " ".join(chunk_batch_ids) if chunk_batch_ids else "",
                "from": "value"
            },
            {
                "name": "smoke_run",
                "value": "--smoke-run" if args.smoke_run else "",
                "from": "value"
            },
            {
                "name": "dry_run",
                "value": "--dry-run" if args.dry_run else "",
                "from": "value"
            },
            {
                "name": "endpoint",
                "value": f"--endpoint={args.endpoint}",
                "from": "value"
            },
            {
                "name": "start_datetime",
                "value": f"--start-date={query_timerange.start_date}",
                "from": "value"
            },
            {
                "name": "end_datetime",
                "value": f"--end-date={query_timerange.end_date}",
                "from": "value"
            },
            {
                "name": "use_temporal",
                "value": "--use-temporal" if args.use_temporal else "",
                "from": "value"
            },
            {
                "name": "chunk_size",
                "value": f"--chunk-size={args.chunk_size}" if args.chunk_size else "",
                "from": "value"
            },
            {
                "name": "transfer_protocol",
                "value": f"--transfer-protocol={args.transfer_protocol}",
                "from": "value"
            },
            {
                "name": "proc_mode",
                "value": f"--processing-mode={args.proc_mode}",
                "from": "value"
            }
        ]
        self.logger.debug(f"{download_job_params=}")
        return download_job_params


def submit_download_job(*, release_version=None, product_type: str, params: list[dict[str, str]],
                        job_queue: str, job_name = None, payload_hash = None) -> str:
    job_spec_str = f"job-{product_type}_download:{release_version}"

    return _submit_mozart_job_minimal(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec_str
        },
        job_queue=job_queue,
        provider_str=product_type,
        job_name=job_name,
        payload_hash = payload_hash
    )


def _submit_mozart_job_minimal(*, hysdsio: dict, job_queue: str, provider_str: str, job_name = None, payload_hash = None) -> str:
    if not job_name:
        job_name = f"job-WF-{provider_str}_download"

    return submit_mozart_job(
        hysdsio=hysdsio,
        product={},
        rule={
            "rule_name": f"trigger-{provider_str}_download",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name=job_name,
        payload_hash=payload_hash,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component=None
    )


def update_url_index(
        es_conn,
        urls: list[str],
        granule: dict,
        job_id: str,
        query_dt: datetime,
        temporal_extent_beginning_dt: datetime,
        revision_date_dt: datetime,
        *args,
        **kwargs
):
    # group pairs of URLs (http and s3) by filename
    filename_to_urls_map = defaultdict(list)
    for url in urls:
        filename = Path(url).name
        filename_to_urls_map[filename].append(url)

    for filename, filename_urls in filename_to_urls_map.items():
        es_conn.process_url(filename_urls, granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args, **kwargs)

def get_query_timerange(args: argparse.Namespace, now: datetime):
    logger = get_logger()

    now_minus_minutes_dt = (
        now - timedelta(minutes=args.minutes)
        if not args.native_id
        else dateutil.parser.isoparse("1900-01-01T00:00:00Z")
    )

    start_date = args.start_date if args.start_date else now_minus_minutes_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = args.end_date if args.end_date else now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_timerange = DateTimeRange(start_date, end_date)

    logger.debug(f"{query_timerange=}")

    return query_timerange
