#!/usr/bin/env python
"""
L3 DSWx state config upserter job
"""
import json
import time
from functools import partial
from typing import Dict, List

from commons.es_connection import get_grq_es
from commons.logger import logger
from util import common_util
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

grq_es = get_grq_es(logger)  # getting GRQ's Elasticsearch connection

to_json = partial(json.dumps, indent=2)


@exec_wrapper
def evaluate():
    jc = JobContext("_context.json")
    job_context: Dict = jc.ctx
    logger.debug(f"job_context={to_json(job_context)}")

    metadata: Dict = job_context["product_metadata"]["metadata"]

    state_config_doc_id = generate_doc_id(metadata)
    state_config = {
        metadata["band_or_qa"]: f"{job_context['product_paths']}/{metadata['FileName']}"
    }  # e.g. { "Fmask": "s3://...Fmask.tif" }

    upsert_state_config(state_config, state_config_doc_id)

    time.sleep(5)  # allow some time for the upserted record to be query-able

    merged_state_config = get_updated_state_config(state_config_doc_id)

    # NOTE republishing a dataset will clobber the old document
    create_state_config_dataset(merged_state_config, state_config_doc_id)


def generate_doc_id(metadata: Dict) -> str:
    """Generate a unique but deterministic document ID."""
    metadata_id: str = metadata["id"]  # e.g. :HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask"
    suffix = f".{metadata['band_or_qa']}"  # e.g. ".Fmask"
    return remove_suffix(metadata_id, suffix)


def upsert_state_config(state_config: Dict, state_config_doc_id: str):
    state_config_doc_update_result: Dict = grq_es.update_document(
        index="grq_1_opera_state_config",
        id=state_config_doc_id,
        body=to_update_doc(state_config),
        # TODO chrisjrd: handle race condition appropriately.
        #  setting to an arbitrary number greater than the number of expected input files, even if 1 retry would suffice
        retry_on_conflict=20
    )

    logger.info(f"state_config_doc_update_result={to_json(state_config_doc_update_result)}")


def to_update_doc(input_dict: Dict) -> Dict:
    """Convert a document into the upsert format expected by Elasticsearch."""
    return {
        "doc_as_upsert": True,
        "doc": input_dict
    }


def get_updated_state_config(state_config_doc_id: str) -> Dict:
    updated_state_config_doc = get_updated_state_config_doc(state_config_doc_id)
    return updated_state_config_doc["_source"]


def get_updated_state_config_doc(state_config_doc_id: str) -> Dict:
    state_config_docs_query_results: List[Dict] = grq_es.query(
        index="grq_1_opera_state_config",
        q=f"_id:{state_config_doc_id}"
    )
    logger.info(f"{state_config_docs_query_results=}")

    updated_state_config_doc: Dict = state_config_docs_query_results[0]  # first row of results
    return updated_state_config_doc


def create_state_config_dataset(merged_state_config: Dict, state_config_doc_id: str):
    common_util.create_state_config_dataset(
        dataset_name=f"{state_config_doc_id}_state_config",
        metadata=merged_state_config,
        start_time=None
    )


def remove_suffix(input_string: str, suffix: str) -> str:
    """Polyfill for str.removesuffix function introduced in Python 3.9."""
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


if __name__ == "__main__":
    evaluate()
