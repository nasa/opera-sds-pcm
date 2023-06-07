import argparse
import asyncio
import logging
import shutil
import sys
from collections import namedtuple
from pathlib import Path, PurePath

import boto3
import dateutil.parser
from mypy_boto3_s3 import S3Client

from tools import stage_ionosphere_file
from tools import stage_orbit_file
from tools.stage_ionosphere_file import IonosphereFileNotFoundException
from tools.stage_orbit_file import NoQueryResultsException
from util import grq_client as grq_client, job_util
from util.conf_util import SettingsConf
from util.exec_util import exec_wrapper
from util.grq_client import update_slc_dataset_with_ionosphere_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@exec_wrapper
async def run(argv: list[str]):
    parser = create_parser()
    args = parser.parse_args(argv[1:])
    logger.info(f"{argv=}")

    results = {}
    slc_dataset_timerange = get_arg_timerange(args)
    slc_datasets = grq_client.get_slc_datasets_without_ionosphere_data(
        dateutil.parser.isoparse(slc_dataset_timerange.start_date),
        dateutil.parser.isoparse(slc_dataset_timerange.end_date)
    )
    settings_cfg = SettingsConf().cfg  # has metadata extractor config
    logger.info("Creating directories to process products")
    # house all file downloads
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)
    for slc_dataset in slc_datasets:
        product_id = slc_dataset["_id"]
        logger.info(f"Processing {product_id=}")

        additional_metadata = {}
        dataset_dir = downloads_dir / product_id
        dataset_dir.mkdir(exist_ok=True)

        logger.info("Downloading orbit file")
        download_orbit_file(dataset_dir, product_id, settings_cfg)

        if additional_metadata.get("intersects_north_america", False):
            logger.info("Downloading ionosphere correction file")
            output_ionosphere_filepath = download_ionosphere_correction_file(dataset_dir, product_id)
            output_ionosphere_filepath = PurePath(output_ionosphere_filepath)
            ionosphere_url = get_ionosphere_correction_file_url(dataset_dir, product_id)
            logger.info(f"{output_ionosphere_filepath=}")
            logger.info(f"{ionosphere_url=}")

            slc_dataset_s3_url = next(iter(filter(lambda url: url.startswith("s3"), slc_dataset["_source"]["browse_urls"])))
            logger.info(f"{slc_dataset_s3_url=}")

            s3_uri_tokens = slc_dataset_s3_url.split('/')
            s3_bucket = s3_uri_tokens[3]
            s3_key = '/'.join(s3_uri_tokens[5:])  # skip redundant `/browse/` fragment at index 4

            s3_client: S3Client = boto3.client("s3")
            s3_client.upload_file(Filename=str(output_ionosphere_filepath), Bucket=s3_bucket, Key=f"{s3_key}/{output_ionosphere_filepath.name}")

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

            update_slc_dataset_with_ionosphere_metadata(index=slc_dataset["_index"], product_id=product_id, ionosphere_metadata=ionosphere_metadata)

            logger.info(f"Removing {output_ionosphere_filepath}")
            Path(output_ionosphere_filepath).unlink()

            # TODO chrisjrd: submit CSLC job
    logger.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)
    results["download"] = None

    logger.info(f"{results=}")
    logger.info("END")

    return results


def create_parser():
    parser = argparse.ArgumentParser()

    start_date = {"positionals": ["-s", "--start-date"],
                  "kwargs": {"dest": "start_date",
                             "default": None,
                             "help": "The ISO date time after which data should be retrieved. For Example, "
                                     "--start-date 2021-01-14T00:00:00Z"}}

    end_date = {"positionals": ["-e", "--end-date"],
                "kwargs": {"dest": "end_date",
                           "default": None,
                           "help": "The ISO date time before which data should be retrieved. For Example, --end-date "
                                   "2021-01-14T00:00:00Z"}}

    parser_arg_list = [start_date, end_date]
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


def download_orbit_file(dataset_dir, product_filepath, settings_cfg):
    logger.info("Downloading associated orbit file")
    try:
        logger.info(f"Querying for Precise Ephemeris Orbit (POEORB) file")
        stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
            [
                f"--output-directory={str(dataset_dir)}",
                "--orbit-type=POEORB",
                f"--query-time-range={settings_cfg.get('POE_ORBIT_TIME_RANGE', stage_orbit_file.DEFAULT_POE_TIME_RANGE)}",
                str(product_filepath)
            ]
        )
        stage_orbit_file.main(stage_orbit_file_args)
    except NoQueryResultsException:
        logger.warning("POEORB file could not be found, querying for Restituted Orbit (ROEORB) file")
        stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
            [
                f"--output-directory={str(dataset_dir)}",
                "--orbit-type=RESORB",
                f"--query-time-range={settings_cfg.get('RES_ORBIT_TIME_RANGE', stage_orbit_file.DEFAULT_RES_TIME_RANGE)}",
                str(product_filepath)
            ]
        )
        stage_orbit_file.main(stage_orbit_file_args)


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
            logger.warning(
                f"Could not find any Ionosphere Correction file for product {product_filepath}"
            )

    return output_ionosphere_file_path


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
            logger.warning(
                f"Could not find any Ionosphere Correction file for product {product_filepath}"
            )

    return ionosphere_url


if __name__ == "__main__":
    asyncio.run(run(sys.argv))
