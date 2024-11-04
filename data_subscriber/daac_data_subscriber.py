#!/usr/bin/env python3

import argparse
import asyncio
import concurrent.futures
import os
import re
import sys
import uuid
from collections import defaultdict, namedtuple
from itertools import chain
from pathlib import Path
from urllib.parse import urlparse

from more_itertools import first
from smart_open import open

from commons.logger import configure_library_loggers, get_logger
from data_subscriber.asf_cslc_download import AsfDaacCslcDownload
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.asf_slc_download import AsfDaacSlcDownload
from data_subscriber.catalog import ProductCatalog
from data_subscriber.cmr import (ProductType,
                                 Provider, get_cmr_token,
                                 COLLECTION_TO_PROVIDER_TYPE_MAP,
                                 COLLECTION_TO_PRODUCT_TYPE_MAP)
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog, CSLCStaticProductCatalog
from data_subscriber.cslc.cslc_query import CslcCmrQuery
from data_subscriber.cslc.cslc_static_query import CslcStaticCmrQuery
from data_subscriber.gcov.gcov_catalog import NisarGcovProductCatalog
from data_subscriber.gcov.gcov_query import NisarGcovCmrQuery
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.hls.hls_query import HlsCmrQuery
from data_subscriber.lpdaac_download import DaacDownloadLpdaac
from data_subscriber.parser import create_parser, validate_args
from data_subscriber.query import update_url_index
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.rtc.rtc_job_submitter import submit_dswx_s1_job_submissions_tasks
from data_subscriber.rtc.rtc_query import RtcCmrQuery
from data_subscriber.slc.slc_catalog import SLCProductCatalog
from data_subscriber.slc.slc_query import SlcCmrQuery
from data_subscriber.survey import run_survey
from rtc_utils import rtc_product_file_revision_regex
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id, is_running_outside_verdi_worker_context


@exec_wrapper
def main():
    run(sys.argv)


def run(argv: list[str]):
    parser = create_parser()
    args = parser.parse_args(argv[1:])

    validate_args(args)

    logger = get_logger(args.verbose)
    configure_library_loggers()

    es_conn = supply_es_conn(args)

    if args.file:
        with open(args.file, "r") as f:
            update_url_index(es_conn, f.readlines(), None, None, None)
        exit(0)

    logger.debug(f"daac_data_subscriber.py invoked with {args=}")

    job_id = supply_job_id()
    logger.debug(f"Using {job_id=}")

    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(args.endpoint, settings)

    results = {}

    if args.subparser_name == "survey":
        run_survey(args, token, cmr, settings)

    if args.subparser_name == "query" or args.subparser_name == "full":
        results["query"] = run_query(args, token, es_conn, cmr, job_id, settings)

    if args.subparser_name == "download" or args.subparser_name == "full":
        netloc = urlparse(f"https://{edl}").netloc

        if args.provider == Provider.ASF_RTC:
            results["download"] = run_rtc_download(args, token, es_conn, netloc, username, password, cmr, job_id)
        else:
            results["download"] = run_download(args, token, es_conn, netloc, username, password, cmr, job_id)

    logger.debug(f"{len(results)=}")
    logger.debug(f"{results=}")
    logger.info("END")

    return results


def run_query(args: argparse.Namespace, token: str, es_conn: ProductCatalog, cmr, job_id, settings):
    product_type = COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection]

    if product_type == ProductType.HLS:
        cmr_query = HlsCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.SLC:
        cmr_query = SlcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.CSLC:
        cmr_query = CslcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.CSLC_STATIC:
        cmr_query = CslcStaticCmrQuery(args, token, es_conn, cmr, job_id, settings)
    # RTC is a special case in that it needs to run asynchronously
    elif product_type == ProductType.RTC:
        cmr_query = RtcCmrQuery(args, token, es_conn, cmr, job_id, settings)
        result = asyncio.run(cmr_query.run_query())
        return result
    elif product_type == ProductType.NISAR_GCOV:
        cmr_query = NisarGcovCmrQuery(args, token, es_conn, cmr, job_id, settings)
    else:
        raise ValueError(f'Unknown collection type "{args.collection}" provided')

    return cmr_query.run_query()

def run_download(args, token, es_conn, netloc, username, password, cmr, job_id):
    provider = (COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                if hasattr(args, "collection") else args.provider)

    if provider == Provider.LPCLOUD:
        downloader = DaacDownloadLpdaac(provider)
    elif provider in (Provider.ASF, Provider.ASF_SLC):
        downloader = AsfDaacSlcDownload(provider)
    elif provider == Provider.ASF_RTC:
        downloader = AsfDaacRtcDownload(provider)
    elif provider == Provider.ASF_CSLC:
        downloader = AsfDaacCslcDownload(provider)
    elif provider == Provider.ASF_CSLC_STATIC:
        raise NotImplementedError("Direct download of CSLC-STATIC products is not supported")
    else:
        raise ValueError(f'Unknown product provider "{provider}"')

    downloader.run_download(args, token, es_conn, netloc, username, password, cmr, job_id)


def run_rtc_download(args, token, es_conn, netloc, username, password, cmr, job_id):
    logger = get_logger()
    provider = args.provider  # "ASF-RTC"
    settings = SettingsConf().cfg

    if not is_running_outside_verdi_worker_context():
        job_context = JobContext("_context.json").ctx
        product_metadata = job_context["product_metadata"]
        logger.info(f"{product_metadata=}")

    affected_mgrs_set_id_acquisition_ts_cycle_indexes = args.batch_ids
    logger.info(f"{affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")

    es_conn: RTCProductCatalog

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
        downloader = AsfDaacRtcDownload(provider)

        run_download_kwargs = {
            "token": token,
            "es_conn": es_conn,
            "netloc": netloc,
            "username": username,
            "password": password,
            "cmr": cmr,
            "job_id": job_id
        }

        product_to_product_filepaths_map: dict[str, set[Path]] = downloader.run_download(
            args=args_for_downloader, **run_download_kwargs, rm_downloads_dir=False)

        logger.info(f"Uploading MGRS burst set files to S3")
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

        logger.info(f"Submitting MGRS burst set download job {batch_id=}, num_bursts={len(product_burstset)}")

        # create args for job-submissions
        args_for_job_submitter = namedtuple(
            "Namespace",
            ["chunk_size", "release_version"],
            defaults=[1, args.release_version]
        )()

        if args.dry_run:
            logger.info(f"{args.dry_run=}. Skipping job submission. Producing mock job ID")
            results = [uuid.uuid4()]
        else:
            logger.info(f"Submitting batches for DSWx-S1 job: {list(uploaded_batch_id_to_s3paths_map)}")
            job_submission_tasks = submit_dswx_s1_job_submissions_tasks(uploaded_batch_id_to_s3paths_map, args_for_job_submitter, settings)
            results = multithread_gather(job_submission_tasks)

        suceeded_batch = [job_id for job_id in results if isinstance(job_id, str)]
        failed_batch = [e for e in results if isinstance(e, Exception)]

        if suceeded_batch:
            for product in uploaded_batch_id_to_products_map[batch_id]:
                if not product.get("dswx_s1_jobs_ids"):
                    product["dswx_s1_jobs_ids"] = []

                product["dswx_s1_jobs_ids"].append(first(suceeded_batch))

            if args.dry_run:
                logger.info(f"{args.dry_run=}. Skipping marking jobs as downloaded. Producing mock job ID")
            else:
                es_conn.mark_products_as_job_submitted({batch_id: uploaded_batch_id_to_products_map[batch_id]})

            succeeded.extend(suceeded_batch)
            failed.extend(failed_batch)

            # manual cleanup since we needed to preserve downloads for manual s3 uploads
            for fp in chain.from_iterable(burst_id_to_files_to_upload.values()):
                fp.unlink(missing_ok=True)

            logger.info("Removed downloads from disk")

    return {
        "success": succeeded,
        "fail": failed
    }


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


def supply_es_conn(args):
    logger = get_logger()
    provider = (COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                if hasattr(args, "collection")
                else args.provider)

    if provider == Provider.LPCLOUD:
        es_conn = HLSProductCatalog(logger)
    elif provider in (Provider.ASF, Provider.ASF_SLC):
        es_conn = SLCProductCatalog(logger)
    elif provider == Provider.ASF_RTC:
        es_conn = RTCProductCatalog(logger)
    elif provider == Provider.ASF_CSLC:
        es_conn = CSLCProductCatalog(logger)
    elif provider == Provider.ASF_CSLC_STATIC:
        es_conn = CSLCStaticProductCatalog(logger)
    elif provider == Provider.ASF_NISAR_GCOV:
        es_conn = NisarGcovProductCatalog(logger)
    else:
        raise ValueError(f'Unsupported provider "{provider}"')

    return es_conn


if __name__ == "__main__":
    main()
