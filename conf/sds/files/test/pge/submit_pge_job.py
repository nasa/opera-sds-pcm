#!/usr/bin/env python

"""
Python script to submit a PGE job
"""

import argparse
import json
import backoff

from hysds_commons.job_utils import submit_mozart_job
from hysds.es_util import get_grq_es
from commons.logger import logger

BACKOFF_CONF = {}  # back-off configuration

grq_es = get_grq_es()  # get connection to GRQ's Elasticsearch


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


def submit_to_mozart(job_name, queue_name, params, release_version):
    rule = {
        "rule_name": "manual_{}".format(job_name),
        "queue": queue_name,
        "priority": "5",
        "kwargs": "{}",
        "enable_dedup": False,
    }

    mozart_job_id = submit_mozart_job(
        {},
        rule,
        hysdsio={
            "id": "internal-temporary-wiring",
            "params": params,
            "job-specification": "{}:{}".format(job_name, release_version),
        },
        job_name="%s-%s" % (job_name, release_version),
    )

    logger.info("Job ID: {}".format(mozart_job_id))
    print("Job ID: {}".format(mozart_job_id))
    return mozart_job_id


@backoff.on_exception(
    backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time
)
def perform_query(query, index):
    results = grq_es.search(index=index, body=query)

    count = results["hits"]["total"]["value"]
    if count != 0:
        return results
    raise RuntimeError(
        "ERROR: No datasets found with following query: {}.\n".format(
            json.dumps(query, indent=2)
        )
    )


def get_input(input_name, input_dataset_type):
    query = {"query": {"bool": {}}}

    condition = []

    match = {"match": {"_id": input_name}}
    condition.append(match)

    query["query"]["bool"]["must"] = condition
    if input_dataset_type == "state-config":
        index = "grq_*_ldf-state-config"
    else:
        index = "grq_*_{}".format(input_dataset_type.lower())

    logger.info("index: {}".format(index))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    results = perform_query(query, index)
    if results["hits"]["total"]["value"] == 0:
        raise RuntimeError("Could not find {} in ElasticSearch".format(input_name))

    urls = results["hits"]["hits"][0]["_source"]["urls"]
    metadata = results["hits"]["hits"][0]["_source"]["metadata"]
    dataset_id = results["hits"]["hits"][0]["_id"]
    s3_urls = []
    if input_dataset_type == "state-config":
        for url in metadata["rrst_product_paths"]:
            s3_urls.append(url)
        product_metadata = {"metadata": metadata}
    else:
        for url in urls:
            if url.startswith("s3://"):
                s3_urls.append(url)
                break
        if len(s3_urls) == 0:
            raise RuntimeError(
                "Could not find S3 url of {} in the returned results.".format(
                    input_name
                )
            )
        product_metadata = [{"metadata": metadata}]
    return s3_urls, product_metadata, dataset_id


def submit_job(
    input_name,
    input_dataset_type,
    job_type,
    release_version,
    file_size_limit="700M",
    accountability_module=None,
    accountability_class=None,
    es_index="pass",
    not_sciflo=False,
):
    job_name = "job-SCIFLO_{}".format(job_type)
    queue_name = "opera-job_worker-sciflo-{}".format(job_type.lower())
    if not_sciflo:
        job_name = "job-{}".format(job_type)
        queue_name = "opera-job_worker-{}".format(job_type.lower())

    product_paths, product_metadata, dataset_id = get_input(
        input_name, input_dataset_type
    )
    params = [
        {"name": "product_paths", "from": "value", "value": product_paths},
        {"name": "product_metadata", "from": "value", "value": product_metadata},
        {"name": "dataset_type", "from": "value", "value": input_dataset_type},
        {"name": "input_dataset_id", "from": "value", "value": dataset_id},
        {
            "name": "module_path",
            "from": "value",
            "type": "text",
            "value": "/home/ops/verdi/ops/opera-pcm",
        },
        {
            "name": "accountability_module_path",
            "from": "value",
            "type": "text",
            "value": accountability_module,
        },
        {
            "name": "accountability_class",
            "from": "value",
            "type": "text",
            "value": accountability_class,
        },
        {"name": "es_index", "from": "value", "type": "text", "value": es_index},
        {
            "name": "wf_dir",
            "from": "value",
            "type": "text",
            "value": "/home/ops/verdi/ops/opera-pcm/opera_chimera/wf_xml",
        },
        {"name": "purpose", "from": "value", "type": "text", "value": job_type},
    ]

    # add job-specific parameter defaults
    if job_type == "L0A":
        params.append(
            {
                "name": "file_size_limit",
                "from": "value",
                "type": "text",
                "value": file_size_limit,
            }
        )
    elif job_type == "GCOV":
        params.extend(
            [
                {
                    "name": "fullcovariance",
                    "from": "value",
                    "type": "text",
                    "value": "False",
                },
                {
                    "name": "output_type",
                    "from": "value",
                    "type": "text",
                    "value": "gamma0",
                },
                {
                    "name": "algorithm_type",
                    "from": "value",
                    "type": "text",
                    "value": "area_projection",
                },
                {
                    "name": "output_posting",
                    "from": "value",
                    "type": "text",
                    "value": "[20, 100]",
                },
            ]
        )

    # submit job
    submit_to_mozart(job_name, queue_name, params, release_version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("input", help="input file")
    parser.add_argument("--input_dataset_type", help="The input dataset type.")
    parser.add_argument(
        "--accountability_module",
        help="The accountability module for the chimera implementation. Optional",
    )
    parser.add_argument(
        "--accountability_class",
        help="The class within the passeed accountability module. Optional",
    )
    parser.add_argument(
        "--es_index",
        help="The accountability index the product will be stored in. Optional",
    )
    parser.add_argument("--job_type", help="The job type.")
    parser.add_argument("--release_version", help="The job release version.")
    parser.add_argument(
        "--file_size_limit", default="700M", help="FileSizeLimit value for L0A"
    )
    parser.add_argument(
        "--max_value", type=int, default=64, help="maximum backoff time"
    )
    parser.add_argument("--max_time", type=int, default=900, help="maximum total time")
    parser.add_argument(
        "--not_sciflo",
        default=False,
        action="store_true",
        help="set if not a sciflo job",
    )

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    if args.accountability_module:
        submit_job(
            args.input,
            args.input_dataset_type,
            args.job_type,
            args.release_version,
            args.file_size_limit,
        )
    else:
        submit_job(
            args.input,
            args.input_dataset_type,
            args.job_type,
            args.release_version,
            args.file_size_limit,
            accountability_module=args.accountability_module,
            accountability_class=args.accountability_class,
            es_index=args.es_index,
            not_sciflo=args.not_sciflo,
        )
