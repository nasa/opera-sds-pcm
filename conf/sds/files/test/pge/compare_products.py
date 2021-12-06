#!/usr/bin/env python

"""
Tool that does comparisons of PCM generated products to a set of given expected products
in a batch mode way.
"""

import argparse
import json
import os.path

import backoff

from hysds.utils import download_file

from commons.logger import logger
from commons.constants import product_metadata as pm
from commons.es_connection import get_grq_es

from util.conf_util import YamlConf

from pcm_commons.tools.sdsdiff.compare_hdf import compare_hdf, DEFAULT_ABSOLUTE_TOLERANCE, DEFAULT_RELATIVE_TOLERANCE
from pcm_commons.tools.sdsdiff.compare_bin import compare_bin

BACKOFF_CONF = {}  # back-off configuration

ancillary_es = get_grq_es(logger)  # get connection to GRQ's Elasticsearch


def lookup_max_value():
    """Runtime configuration of backoff max_value."""
    return BACKOFF_CONF["max_value"]


def lookup_max_time():
    """Runtime configuration of backoff max_time."""
    return BACKOFF_CONF["max_time"]


@backoff.on_exception(
    backoff.expo, Exception, max_value=lookup_max_value, max_time=lookup_max_time
)
def perform_query(query, index):
    results = ancillary_es.search(index=index, body=query, size=1)

    count = results["hits"]["total"]["value"]
    if count != 0:
        return results
    raise RuntimeError(
        "ERROR: No datasets found with following query: {}.\n".format(
            json.dumps(query, indent=2)
        )
    )


def get_s3_url(urls):
    for url in urls:
        if url.startswith("s3://"):
            return url
    raise RuntimeError("Could not find an S3 url: {}".format(urls))


def get_product(dataset, crid, target_dir, dataset_id=None):
    query = {"query": {"bool": {}}}
    if dataset_id:
        conditions = {
            "_id": dataset_id
        }
    else:
        conditions = {
            "system_version": crid,
            "metadata.tags.keyword": "PGE"
        }
    query["query"]["bool"]["must"] = ancillary_es.construct_bool_query(conditions)
    index = "grq_*_{}".format(dataset.lower())

    logger.info("index: {}".format(index))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    results = perform_query(query, index)
    if results["hits"]["total"]["value"] == 0:
        raise RuntimeError("Could not find product using the following query: query={}, index={}".format(
            json.dumps(query), index))

    source = results.get("hits", {}).get("hits", [])[0]["_source"]

    urls = source.get("urls", {})
    file_name = source.get("metadata", {}).get(pm.FILE_NAME, None)
    if file_name is None:
        raise RuntimeError("Missing {} from the product metadata".format(source.get("metadata", {})))

    # This gets the S3 url to the dataset
    s3_url = get_s3_url(urls)
    # This gets the S3 url to the actual file needed
    s3_product_url = os.path.join(s3_url, source.get("metadata", {}).get(pm.FILE_NAME))

    target_file = os.path.join(target_dir, file_name)
    logger.info("Copying {} to {}".format(s3_product_url, target_file))
    download_file(s3_product_url, target_file)

    return target_file


def compare_products(dataset, pcm_product, expected_product, rtol, atol, compare_overrides, verbose_flag, file_handler):
    logger.info("Comparing {} to {}".format(pcm_product, expected_product))
    if dataset.upper().startswith("L0A"):
        file_compare_flag = compare_bin(pcm_product, expected_product,
                                        verbose_flag=verbose_flag)
    else:
        file_compare_flag = compare_hdf(pcm_product, expected_product,
                                        rtol=rtol, atol=atol,
                                        override_file=compare_overrides,
                                        verbose_flag=verbose_flag)
    if file_compare_flag:
        msg = "SUCCESS: Successfully compared {} to {}.\n".format(product_file, expected_product)
        logger.info(msg)
    else:
        msg = "ERROR: Did not successfully compare {} to {}.\n".format(product_file, expected_product)
        logger.error(msg)

    file_handler.write(msg)
    return file_compare_flag


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("config", help="Configuration file in YAML format.")
    parser.add_argument("results_file", help="result file")
    parser.add_argument("--crid", help="The CRID associated with the PCM generated products.", required=True)
    parser.add_argument("--rtol", type=float,
                        help="Relative tolerance for floating point comparisons of variable data.",
                        default=DEFAULT_RELATIVE_TOLERANCE)
    parser.add_argument("--atol", type=float,
                        help='Absolute tolerance for floating point comparisons of variable data.',
                        default=DEFAULT_ABSOLUTE_TOLERANCE)
    parser.add_argument('--verbosity',
                        help='Turn on the verbose reporting for each group.',
                        action="store_true")
    parser.add_argument("--workspace",
                        help="Optionally define a workspace area to download the PCM generated products. "
                             "Default is /current_dir/pcm-products")
    parser.add_argument("--overrides",
                        help="Optionally define an overrides json file to ignore certain attributes or define "
                             "new tolerances.")
    parser.add_argument(
        "--max_value", type=int, default=64, help="maximum backoff time"
    )
    parser.add_argument("--max_time", type=int, default=300, help="maximum total time")

    args = parser.parse_args()

    BACKOFF_CONF["max_value"] = args.max_value
    BACKOFF_CONF["max_time"] = args.max_time

    config = YamlConf(args.config).cfg

    workspace = config.get("workspace", None)
    if workspace is None:
        workspace = os.getcwd()
    else:
        workspace = os.path.abspath(workspace)

    if args.verbosity:
        verbose_flag = True
    else:
        verbose_flag = False

    compare_overrides = None
    if args.overrides:
        compare_overrides = args.overrides
    workspace = os.path.join(os.getcwd(), "pcm-products")
    if args.workspace:
        workspace = os.path.abspath(args.workspace)

    products = config.get("products", {})
    logger.info("products : {}".format(products))
    with open(args.results_file, "w") as f:
        for dataset in products.keys():
            logger.info("dataset : {}".format(dataset))
            dataset_info = products.get(dataset)
            if isinstance(dataset_info, list):
                # Loop through the given set of products to compare
                for dict_item in dataset_info:
                    product_file = get_product(dataset,
                                               args.crid,
                                               workspace,
                                               dataset_id=dict_item.get("dataset_id"))
                    expected_product = dict_item.get("expected_product", None)
                    if expected_product is None:
                        raise RuntimeError("Missing required expected_product for {}".format(dataset))
                    compare_flag = compare_products(dataset,
                                                    product_file,
                                                    expected_product,
                                                    rtol=args.rtol,
                                                    atol=args.atol,
                                                    compare_overrides=compare_overrides,
                                                    verbose_flag=verbose_flag,
                                                    file_handler=f)
            else:
                # For each dataset, download the generated product first
                product_file = get_product(dataset,
                                           args.crid,
                                           workspace)
                expected_product = products.get(dataset, {}).get("expected_product", None)
                if expected_product is None:
                    raise RuntimeError("Missing required expected_product for {}".format(dataset))

                compare_flag = compare_products(dataset,
                                                product_file,
                                                expected_product,
                                                rtol=args.rtol,
                                                atol=args.atol,
                                                compare_overrides=compare_overrides,
                                                verbose_flag=verbose_flag,
                                                file_handler=f)
