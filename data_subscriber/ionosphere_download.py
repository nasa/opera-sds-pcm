import argparse
import asyncio
import logging
import shutil
import sys
from collections import namedtuple
from functools import partial
from pathlib import Path, PurePath

import boto3
import dateutil.parser
import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, always_iterable
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

    slc_datasets = get_pending_slc_datasets(args)

    settings_cfg = SettingsConf().cfg  # has metadata extractor config
    logger.info("Creating directories to process products")
    downloads_dir = Path("downloads")  # house all file downloads
    downloads_dir.mkdir(exist_ok=True)
    for slc_dataset in slc_datasets:
        product_id = slc_dataset["_id"]
        logger.info(f"Processing {product_id=}")

        dataset_dir = downloads_dir / product_id
        dataset_dir.mkdir(exist_ok=True)

        download_orbit_file(dataset_dir, product_id, settings_cfg)

        if True:  # TODO chrisjrd: remove after testing
        # if slc_dataset["_source"]["metadata"].get("intersects_north_america"):
            logger.info("Downloading ionosphere correction file")
            output_ionosphere_filepath = download_ionosphere_correction_file(dataset_dir, product_id)
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

            job_submission_result = await submit_cslc_job(slc_dataset)
            if job_submission_result["fail"]:
                raise next(iter(job_submission_result["fail"]))
    logger.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)
    results["download"] = None

    logger.info(f"{results=}")
    logger.info("END")

    return results


def get_pending_slc_datasets(args):
    slc_dataset_timerange = get_arg_timerange(args)
    slc_datasets = grq_client.get_slc_datasets_without_ionosphere_data(
        dateutil.parser.isoparse(slc_dataset_timerange.start_date),
        dateutil.parser.isoparse(slc_dataset_timerange.end_date)
    )
    return slc_datasets


async def submit_cslc_job(products):
    # logger.info(f"Submitting CSLC job for {product_id=}")
    products = always_iterable(products) if not isinstance(products, dict) else [products]
    MyNamedTuple = namedtuple("MyNamedTuple",
                              ["chunk_size", "release_version",
                               "dataset_type", "input_dataset_id", "product_metadata",
                               "job_queue"])
    args = MyNamedTuple(chunk_size=1, release_version="issue_478",
                        dataset_type="dataset_jpath:_source.dataset", input_dataset_id="dataset_jpath:_id", product_metadata="lambda ds: { 'metadata': ds['metadata'] }",
                        job_queue="TBD")

    results = []
    job_submission_tasks = []
    loop = asyncio.get_event_loop()
    logging.info(f"{args.chunk_size=}")
    for chunk in chunked(products, n=args.chunk_size):
        for product in chunk:
            job_submission_tasks.append(
                loop.run_in_executor(
                    executor=None,
                    func=partial(
                        submit_cslc_job_helper,
                        release_version=args.release_version,
                        params=[
                            # TODO chrisjrd: see if literal values are accepted, otherwise copy from hysds-io.
                            {
                                "name": "product_path",
                                "lambda": "lambda ds: list(filter(lambda x: x.startswith('s3://'), ds['urls']))[0]",
                                "from": "value"
                            },
                            {
                                "name": "dataset_type",
                                "value": args.dataset_type,
                                "from": "value"
                            },
                            {
                                "name": "input_dataset_id",
                                "value": args.input_dataset_id,
                                "from": "value"
                            },
                            {
                                "name": "product_metadata",
                                "value": args.product_metadata,
                                "from": "value"
                            },
                            # TODO chrisjrd: find a way to avoid this duplication of hysds-io. May not be avoidable.
                            {
                              "name": "module_path",
                              "from": "value",
                              "type": "text",
                              "value": "/home/ops/verdi/ops/opera-pcm"
                            },
                            {
                              "name": "wf_dir",
                              "from": "value",
                              "type": "text",
                              "value": "/home/ops/verdi/ops/opera-pcm/opera_chimera/wf_xml"
                            },
                            {
                              "name": "wf_name",
                              "from": "value",
                              "type": "text",
                              "value": "L2_CSLC_S1"
                            },
                            {
                              "name": "accountability_module_path",
                              "from": "value",
                              "type": "text",
                              "value": "opera_chimera.accountability"
                            },
                            {
                              "name": "accountability_class",
                              "from": "value",
                              "type": "text",
                              "value": "OperaAccountability"
                            },
                            {
                              "name": "pge_runconfig_dir",
                              "from": "value",
                              "type": "text",
                              "value": "pge_runconfig_dir"
                            },
                            {
                              "name": "pge_input_dir",
                              "from": "value",
                              "type": "text",
                              "value": "pge_input_dir"
                            },
                            {
                              "name": "pge_output_dir",
                              "from": "value",
                              "type": "text",
                              "value": "pge_output_dir"
                            },
                            {
                              "name": "container_home",
                              "from": "value",
                              "type": "text",
                              "value": "/home/compass_user"
                            },
                            {
                              "name": "container_working_dir",
                              "from": "value",
                              "type": "text",
                              "value": "/home/compass_user/scratch"
                            }
                        ],
                        product=product
                    )
                )
            )
        results.extend(await asyncio.gather(*job_submission_tasks, return_exceptions=True))
    logging.info(f"{len(results)=}")
    logging.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logging.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logging.info(f"{failed=}")

    return {
        "success": succeeded,
        "fail": failed
    }


def submit_cslc_job_helper(*, release_version=None, product: dict) -> str:
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

    return PurePath(output_ionosphere_file_path)


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
