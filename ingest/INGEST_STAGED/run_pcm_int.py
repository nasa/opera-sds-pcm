#!/usr/bin/env python
"""
Submits ingest jobs.
"""

import os
import logging
import argparse
import subprocess
import traceback
import json

from purge_ISL import purge_isl
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.checksum_util import create_dataset_checksums, get_file_checksum
from extractor import extract
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)


log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

BASE_PATH = os.path.dirname(__file__)


@exec_wrapper
def main(id, src_data):
    """Main."""
    cfg = SettingsConf().cfg
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.info("job_context: {}".format(json.dumps(job_context, indent=2)))
    data_name = None

    # set force publish (disable no-clobber)
    if cfg.get(oc_const.FORCE_INGEST, {}).get("INGEST_STAGED", False):
        jc.set("_force_ingest", True)
        jc.save()

    # crack open signal file
    if job_context.get("checksum") is True:
        # open up the signal file
        urls = job_context.get("data_url")
        for url in urls:
            if not url.endswith(id):
                checksum_file = url[url.rfind("/") + 1 :]
        checksum_value = open(checksum_file, "r").read()

        checksum_type = job_context.get("checksum_type")
        file_content = open(id, "rb").read()
        file_checksum = get_file_checksum(file_content, checksum_type)

        if checksum_value != file_checksum:
            error = "Checksums don't match. \nChecksum in signal file: {}. \n File checksum: {}".format(
                checksum_value, file_checksum
            )
            logger.error(error)
            raise Exception(error)
    elif isinstance(job_context.get("checksum"), str):
        cs = job_context.get("checksum")
        logger.info("Calculate checksum for {}".format(id))
        s = (
            subprocess.check_output(
                "openssl md5 -binary " + id + " | base64", shell=True
            )
            .rstrip()
            .decode("utf-8")
        )
        if cs != s:
            error = "Checksums don't match. \nChecksum in job_context: {}. \n Local checksum: {}".format(
                cs, s
            )
            logger.error(error)
            raise Exception(error)

    logger.info("Passed checksum check. Proceeding with ingesting staged file.")
    for type, type_cfg in list(cfg["PRODUCT_TYPES"].items()):
        matched = type_cfg["Pattern"].match(id)
        if matched:
            data_name = type
            break
    if not data_name:
        error = "{} does not match any staged data patterns".format(id)
        logger.error(error)
        raise Exception(error)
    try:
        output_dir = os.path.abspath("output")
        dataset_dir = extract.extract(
            src_data, cfg["PRODUCT_TYPES"], output_dir, job_context["prod_met"]
        )
        create_dataset_checksums(dataset_dir, "md5")
        logger.info("Created dataset: {}".format(dataset_dir))

        purge_isl.purge_isl_urls(job_context["prod_met"]["ISL_urls"])
    except subprocess.CalledProcessError as cpe:
        logger.error(
            "Error while trying to extract metadata from {}:\n{}".format(id, cpe.output)
        )
        raise Exception(
            "Error while trying to extract metadata from {}:\n{}".format(id, cpe.output)
        )
    except Exception:
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("id", help="INGEST L0A Generic")
    parser.add_argument("src_data", help="source data file")
    args = parser.parse_args()
    main(args.id, args.src_data)
