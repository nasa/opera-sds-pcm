#!/usr/bin/env python
"""
Run an on-demand PGE/Sciflo job
"""

import argparse
import os
import sys

from chimera.run_sciflo import main as run_sciflo
from commons.es_connection import get_grq_es
from commons.logger import logger
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from util.ctx_util import JobContext

ancillary_es = get_grq_es(logger)

BASE_PATH = os.path.dirname(__file__)


def main(context_file):
    context_file = os.path.abspath(context_file)

    logger.info(f'Running on-demand job with context file {context_file}')

    if not os.path.exists(context_file):
        raise RuntimeError(f'Context file {context_file} cannot be found')

    # Read the provided context file
    jc = JobContext(context_file)
    job_context = jc.ctx

    logger.info(f"job_context: {job_context}")

    if oc_const.INPUT_DATASET_ID not in job_context:
        raise RuntimeError(f'No {oc_const.INPUT_DATASET_ID} key set in provided context')

    # Extract the dataset ID for the elasticsearch lookup
    input_dataset_id = job_context[oc_const.INPUT_DATASET_ID]

    logger.info(f"Querying metadata for input dataset ID {input_dataset_id}")

    indexes = ["grq_*_l2_hls_l30", "grq_*_l2_hls_s30"]
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"_id": input_dataset_id}
                    }
                ]
            }
        }
    }

    try:
        # Query elasticsearch for the provided dataset ID
        result = ancillary_es.search(body=query, index=indexes)

        hits = result.get("hits", {}).get("hits", [])
        logger.info("query hit count: {}".format(len(hits)))

        if len(hits) > 0:
            # Update _context.json with fields needed by sciflo
            metadata = hits[0]["_source"]["metadata"]
            product_metadata = {'metadata': metadata}

            logger.info(f"Setting product_metadata in context: {product_metadata}")
            jc.set('product_metadata', product_metadata)

            dataset_type = hits[0]["_source"][oc_const.DATASET_TYPE]
            logger.info(f"Setting dataset_type in context: {dataset_type}")
            jc.set(oc_const.DATASET_TYPE, dataset_type)

            logger.info(f"Updating input_dataset_id to {input_dataset_id}")
            jc.set(oc_const.INPUT_DATASET_ID, input_dataset_id)

            jc.save()
        else:
            raise RuntimeError(
                f"No hits returned querying for dataset ID {input_dataset_id}.\n"
                f"Please ensure the dataset has been ingested before reattempting "
                f"the on-demand job."
            )
    except Exception as err:
        raise RuntimeError(
            f"Exception caught while querying for dataset ID {input_dataset_id}, "
            f"reason: {str(err)}"
        )

    if not all([key in job_context for key in ['wf_dir', 'wf_name']]):
        raise RuntimeError(
            'Workflow data missing from provided context.\n'
            'Please ensure the "wf_dir" and "wf_name" keys are set properly.'
        )

    # Hand off to sciflo to execute the on-demand PGE job
    sfl_file = "/".join([job_context['wf_dir'], job_context['wf_name'] + '.sf.xml'])
    output_folder = "output"

    return run_sciflo(sfl_file, context_file, output_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("context_file", help="HySDS context file")
    args = parser.parse_args()
    sys.exit(main(args.context_file))
