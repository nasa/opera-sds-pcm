#!/usr/bin/env python
"""
L3 DSWx state config upserter job
"""
import json
from functools import partial
from typing import Dict

from commons.es_connection import get_grq_es
from commons.logger import logger
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

ancillary_es = get_grq_es(logger)  # getting GRQ's Elasticsearch connection

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"

to_json = partial(json.dumps, indent=2)


@exec_wrapper
def evaluate():
    jc = JobContext("_context.json")
    job_context: Dict = jc.ctx
    logger.debug(f"job_context={to_json(job_context)}")

    metadata: Dict = job_context["product_metadata"]["metadata"]

    state_config = {metadata['band_or_qa']: True}  # e.g. { "fmask": 1 }
    state_config_doc_update_result: Dict = ancillary_es.update_document(
        id=generate_doc_id(metadata),
        index=f"grq_1_opera_state_config",
        body=to_update_doc(state_config)
    )
    logger.info(f"{to_json(state_config_doc_update_result)}")


def generate_doc_id(metadata) -> str:
    """Generate a unique but deterministic document ID"""
    metadata_id: str = metadata['id']  # e.g. :HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask"
    suffix = f".{metadata['band_or_qa']}"  # e.g. ".fmask"
    return remove_suffix(metadata_id, suffix)


def remove_suffix(input_string, suffix) -> str:
    """Polyfill for str.removesuffix function introduced in Python 3.9"""
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


def to_update_doc(input_dict: Dict):
    return {
        "doc_as_upsert": True,
        "doc": input_dict
    }


if __name__ == "__main__":
    evaluate()
