#!/usr/bin/env python
"""
L3 DSWx state config upserter job
"""
import json
import sys

from commons.es_connection import get_grq_es
from commons.logger import logger
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

ancillary_es = get_grq_es(logger)  # getting GRQ's Elasticsearch connection

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


@exec_wrapper
def evaluate():
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.debug(f"job_context: {json.dumps(job_context, indent=2)}")

    from pathlib import Path
    logger.info(Path(".").resolve())
    for child in Path(".").resolve().iterdir():
        logger.info(child)


if __name__ == "__main__":
    logger.info(sys.argv)
    evaluate()
