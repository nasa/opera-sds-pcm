import argparse
import asyncio
import logging
import shutil
import sys
from collections import namedtuple, defaultdict
from functools import partial
from pathlib import Path, PurePath

import backoff
import boto3
import dateutil.parser
import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, partition
from mypy_boto3_s3 import S3Client

from commons.logger import NoJobUtilsFilter, NoBaseFilter
from tools import stage_ionosphere_file
from tools.stage_ionosphere_file import IonosphereFileNotFoundException
from util import grq_client as grq_client, job_util
from util.exec_util import exec_wrapper
from util.grq_client import try_update_slc_dataset_with_ionosphere_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@exec_wrapper
def main():
    logger_hysds_commons = logging.getLogger("hysds_commons")
    logger_hysds_commons.addFilter(NoJobUtilsFilter())

    logger_elasticsearch = logging.getLogger("elasticsearch")
    logger_elasticsearch.addFilter(NoBaseFilter())

    asyncio.run(run(sys.argv))


async def run(argv: list[str]):
    logger.info(f"{argv=}")
    parser = create_parser()
    args = parser.parse_args(argv[1:])

    results = defaultdict(list)
    exceptions = []

    slc_datasets = get_pending_slc_datasets(args)
    logger.info(f"{len(slc_datasets)=}")

    logger.info("Creating directories to process products")
    downloads_dir = Path("downloads")  # house all file downloads
    downloads_dir.mkdir(exist_ok=True)

    for i, slc_dataset in enumerate(slc_datasets, start=1):
        db_id = slc_dataset["_id"]
        product_id = slc_dataset["_source"]["metadata"]["id"]
        logger.info(f"Processing {product_id=}. {i} of {len(slc_datasets)} products")
        dataset_dir = downloads_dir / product_id

        try:
            dataset_dir.mkdir(exist_ok=True)

            if not slc_dataset["_source"]["metadata"].get("intersects_north_america"):
                logging.info("dataset doesn't cover North America. Skipping.")
                continue

            if not slc_dataset["_source"]["metadata"].get("processing_mode") == "forward":
                logging.info("dataset not captured in forward processing mode. Skipping.")
                continue

            if slc_dataset["_source"]["metadata"].get("intersects_north_america") \
                    and slc_dataset["_source"]["metadata"].get("processing_mode") == "forward":

                logger.info("Downloading ionosphere correction file")
                try:
                    output_ionosphere_filepath = download_ionosphere_correction_file(dataset_dir, product_id)
                except IonosphereFileNotFoundException:
                    logger.info("Couldn't find an ionosphere correction file. Skipping to next SLC dataset.")
                    continue
                ionosphere_url = get_ionosphere_correction_file_url(dataset_dir, product_id)
                logger.info(f"{output_ionosphere_filepath=}")
                logger.info(f"{ionosphere_url=}")

                slc_dataset_s3_url: str = next(iter(filter(lambda url: url.startswith("s3"), slc_dataset["_source"]["browse_urls"])))
                logger.info(f"{slc_dataset_s3_url=}")

                s3_bucket, s3_key = try_s3_upload_file(slc_dataset_s3_url, output_ionosphere_filepath)

                job_submission_results = await submit_cslc_jobs([slc_dataset], args)

                if job_submission_results["success"]:
                    results["success"].extend(job_submission_results["success"])
                elif job_submission_results["fail"]:
                    results["fail"].extend(job_submission_results["fail"])
                else:
                    pass

                if job_submission_results["fail"]:
                    logging.info(f"Job submission failure result for {product_id=}. Collecting exception result. Skipping to next SLC dataset.")
                    exceptions.extend(job_submission_results["fail"])
                    continue

                ionosphere_metadata = generate_ionosphere_metadata(output_ionosphere_filepath, ionosphere_url, s3_bucket, s3_key)
                try_update_slc_dataset_with_ionosphere_metadata(index=slc_dataset["_index"], product_id=db_id, ionosphere_metadata=ionosphere_metadata)
        except Exception as e:
            logging.info(f"An exception occurred while processing {product_id=}. Collecting exception. Skipping to next SLC dataset.")
            exceptions.append(e)
            continue
        finally:
            logging.info(f"Removing {dataset_dir=}")
            shutil.rmtree(dataset_dir)

    if exceptions:
        logging.error(f"During job execution, {len(exceptions)} exceptions occurred. Wrapping and raising.")
        raise Exception(exceptions)

    logger.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)

    results = dict(results)  # convert to regular dict
    logger.info(f"{results=}")
    logger.info("END")

    return results


def generate_ionosphere_metadata(output_ionosphere_filepath, ionosphere_url, s3_bucket, s3_key):
    ionosphere_metadata = {
        "ionosphere": {
            "job_id": job_util.supply_job_id(),
            "s3_url": f"s3://{s3_bucket}/{s3_key}/{output_ionosphere_filepath.name}",
            "source_url": ionosphere_url
        }
    }
    # DEV: compare to CoreMetExtractor.py
    ionosphere_metadata["ionosphere"]["FileLocation"] = str(output_ionosphere_filepath.parent)
    ionosphere_metadata["ionosphere"]["FileSize"] = Path(output_ionosphere_filepath).stat().st_size
    ionosphere_metadata["ionosphere"]["FileName"] = output_ionosphere_filepath.name

    return ionosphere_metadata


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def try_s3_upload_file(slc_dataset_s3_url, output_ionosphere_filepath):
    s3_uri_tokens = slc_dataset_s3_url.split('/')
    s3_bucket = s3_uri_tokens[3]
    s3_key = '/'.join(s3_uri_tokens[5:])  # skip redundant `/browse/` fragment at index 4
    s3_client: S3Client = boto3.client("s3")
    s3_client.upload_file(Filename=str(output_ionosphere_filepath), Bucket=s3_bucket, Key=f"{s3_key}/{output_ionosphere_filepath.name}")
    return s3_bucket, s3_key


def get_pending_slc_datasets(args):
    slc_dataset_timerange = get_arg_timerange(args)
    slc_datasets = grq_client.get_slc_datasets_without_ionosphere_data(
        dateutil.parser.isoparse(slc_dataset_timerange.start_date),
        dateutil.parser.isoparse(slc_dataset_timerange.end_date)
    )
    return slc_datasets


async def submit_cslc_jobs(products, args):
    job_submission_tasks = _create_job_submission_tasks(args, products)
    job_submission_task_results = await _execute_job_submission_tasks(job_submission_tasks)

    logging.info(f"{len(job_submission_task_results)=}")
    logging.debug(f"{job_submission_task_results=}")

    task_successes, task_failures = partition(lambda it: isinstance(it, Exception), job_submission_task_results)
    cslc_job_ids = list(task_successes)
    task_exceptions = list(task_failures)

    results = {
        "success": cslc_job_ids,
        "fail": task_exceptions
    }
    return results


async def _execute_job_submission_tasks(job_submission_tasks):
    job_submission_task_results = []
    task_chunks = list(chunked(job_submission_tasks, 1))
    for i, task_chunk in enumerate(task_chunks, start=1):  # CMR recommends 2-5 threads.
        logger.info(f"Processing batch {i} of {len(task_chunks)}")
        task_results = await asyncio.gather(*task_chunk, return_exceptions=True)
        job_submission_task_results.extend(task_results)

    return job_submission_task_results


def _create_job_submission_tasks(args, products):
    job_submission_tasks = []
    for product in products:
        job_submission_tasks.append(_create_job_submission_task(args, product))

    return job_submission_tasks


def _create_job_submission_task(args, product):
    logger.info(f'Creating CSLC job submission task for {product["_id"]=}')
    loop = asyncio.get_event_loop()
    job_submission_task = loop.run_in_executor(
        executor=None,
        func=partial(submit_cslc_job_helper, release_version=args.release_version, product=product))
    return job_submission_task


def submit_cslc_job_helper(*, release_version=None, product: dict) -> str:
    return _try_submit_mozart_job_minimal(release_version=release_version, product=product)


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def _try_submit_mozart_job_minimal(*, release_version: str, product: dict) -> str:
    return _submit_mozart_job_minimal(release_version=release_version, product=product)


def _submit_mozart_job_minimal(*, release_version: str, product: dict) -> str:
    return submit_mozart_job(
        hysdsio=None,  # setting None forces lookup based on rule and component.
        product=product,
        rule={
            "job_type": f"hysds-io-SCIFLO_L2_CSLC_S1:{release_version}",
            "rule_name": f"trigger-SCIFLO_L2_CSLC_S1",
            "queue": "opera-job_worker-sciflo-l2_cslc_s1",
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name=f"job-WF-SCIFLO_L2_CSLC_S1",
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component="grq"  # hysds-io information is in the hysds_ios-grq index rather thann hysds_ios-mozart
    )


def create_parser():
    parser = argparse.ArgumentParser()

    start_date = {"positionals": ["--start-date"],
                  "kwargs": {"dest": "start_date",
                             "default": None,
                             "help": "The ISO date time after which data should be retrieved. For Example, "
                                     "--start-date 2021-01-14T00:00:00Z"}}

    end_date = {"positionals": ["--end-date"],
                "kwargs": {"dest": "end_date",
                           "default": None,
                           "help": "The ISO date time before which data should be retrieved. For Example, --end-date "
                                   "2021-01-14T00:00:00Z"}}

    release_version = {"positionals": ["--release-version"],
                       "kwargs": {"dest": "release_version",
                                  "help": "The release version of the CSLC job-spec."}}

    parser_arg_list = [start_date, end_date, release_version]
    _add_arguments(parser, parser_arg_list)

    return parser


def _add_arguments(parser, arg_list):
    for argument in arg_list:
        parser.add_argument(*argument["positionals"], **argument["kwargs"])


DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


def get_arg_timerange(args):
    start_date = args.start_date
    end_date = args.end_date

    download_timerange = DateTimeRange(start_date, end_date)
    logger.info(f"{download_timerange=}")
    return download_timerange


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None, giveup=lambda e: isinstance(e, IonosphereFileNotFoundException))
def download_ionosphere_correction_file(dataset_dir, product_filepath):
    logger.info("Downloading associated Ionosphere Correction file")
    try:
        stage_ionosphere_file_args = stage_ionosphere_file.get_parser().parse_args(
            [
                f"--type={stage_ionosphere_file.IONOSPHERE_TYPE_JPLG}",
                f"--output-directory={str(dataset_dir)}",
                str(product_filepath)
            ]
        )
        output_ionosphere_file_path = stage_ionosphere_file.main(stage_ionosphere_file_args)
        logger.info("Added JPLG Ionosphere correction file to dataset")
    except IonosphereFileNotFoundException:
        logger.warning("JPLG file type could not be found, querying for JPRG file type")
        try:
            stage_ionosphere_file_args = stage_ionosphere_file.get_parser().parse_args(
                [
                    f"--type={stage_ionosphere_file.IONOSPHERE_TYPE_JPRG}",
                    f"--output-directory={str(dataset_dir)}",
                    str(product_filepath)
                ]
            )
            output_ionosphere_file_path = stage_ionosphere_file.main(stage_ionosphere_file_args)
            logger.info("Added JPRG Ionosphere correction file to dataset")
        except IonosphereFileNotFoundException:
            logger.warning(f"Could not find any Ionosphere Correction file for product {product_filepath}")
            raise

    return PurePath(output_ionosphere_file_path)


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None, giveup=lambda e: isinstance(e, IonosphereFileNotFoundException))
def get_ionosphere_correction_file_url(dataset_dir, product_filepath):
    logger.info("Downloading associated Ionosphere Correction file")
    try:
        stage_ionosphere_file_args = stage_ionosphere_file.get_parser().parse_args(
            [
                f"--type={stage_ionosphere_file.IONOSPHERE_TYPE_JPLG}",
                f"--output-directory={str(dataset_dir)}",
                f"--url-only",
                str(product_filepath)
            ]
        )
        ionosphere_url = stage_ionosphere_file.main(stage_ionosphere_file_args)
        logger.info("Added JPLG Ionosphere correction file to dataset")
    except IonosphereFileNotFoundException:
        logger.warning("JPLG file type could not be found, querying for JPRG file type")
        try:
            stage_ionosphere_file_args = stage_ionosphere_file.get_parser().parse_args(
                [
                    f"--type={stage_ionosphere_file.IONOSPHERE_TYPE_JPRG}",
                    f"--output-directory={str(dataset_dir)}",
                    f"--url-only",
                    str(product_filepath)
                ]
            )
            ionosphere_url = stage_ionosphere_file.main(stage_ionosphere_file_args)
            logger.info("Added JPRG Ionosphere correction file to dataset")
        except IonosphereFileNotFoundException:
            logger.warning(f"Could not find any Ionosphere Correction file for product {product_filepath}")
            raise

    return ionosphere_url


if __name__ == "__main__":
    main()
