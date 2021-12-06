"""
Code to convert PGE outputs to HySDS-style datasets.

@author: mcayanan
"""
from __future__ import print_function

import os
import sys
import shutil

import traceback
import subprocess

from commons.logger import logger

from extractor import extract
from util.conf_util import SettingsConf, PGEOutputsConf
from util.checksum_util import create_dataset_checksums

PRIMARY_KEY = "Primary"
SECONDARY_KEY = "Secondary"
OPTIONAL_KEY = "Optional"
DEFAULT_HASH_ALGO = "sha256"

DATASETS_DIR_NAME = "datasets"


def convert(
    product_dir,
    pge_name,
    rc_file=None,
    pge_output_conf_file=None,
    settings_conf_file=None,
    extra_met=None
):
    created_datasets = []

    pge_outputs_cfg = PGEOutputsConf(pge_output_conf_file).cfg
    pge_config = pge_outputs_cfg[pge_name]

    product_dir = os.path.abspath(product_dir)

    # Check to see if all expected outputs were generated
    products = process_outputs(product_dir, pge_config["Outputs"])

    if extra_met:
        extra_met.update({"tags": ["PGE"]})
    else:
        extra_met = {"tags": ["PGE"]}

    settings = SettingsConf(settings_conf_file).cfg

    """ Create the datasets """
    output_types = [PRIMARY_KEY, OPTIONAL_KEY]
    for output_type in output_types:
        for product in products[output_type].keys():
            logger.info("Converting {} to a dataset".format(product))
            dataset_dir = extract.extract(
                os.path.join(product_dir, product),
                settings[extract.PRODUCT_TYPES_KEY],
                os.path.join(product_dir, DATASETS_DIR_NAME),
                extra_met=extra_met,
            )
            hashcheck = False
            if "hashcheck" in products[output_type][product]:
                hashcheck = products[output_type][product]["hashcheck"]

            if hashcheck:
                hash_algo = DEFAULT_HASH_ALGO
                if "hash_algo" in products[output_type][product]:
                    hash_algo = products[output_type][product]["hash_algo"]
                print("hash_algo : {}".format(hash_algo))

                create_dataset_checksums(os.path.join(dataset_dir, product), hash_algo)
            created_datasets.append(dataset_dir)

    # Rename RunConfig to its dataset
    if rc_file:
        renamed_rc_file = os.path.join(product_dir, "{}.rc.yaml".format(os.path.basename(created_datasets[0])))
        logger.info("Copying RunConfig file to {}".format(renamed_rc_file))
        shutil.copyfile(rc_file, renamed_rc_file)
        products[SECONDARY_KEY][os.path.basename(renamed_rc_file)] = {"hashcheck": False}

    for dataset in created_datasets:
        #  TODO: Do we append all Secondary Products to each dataset???

        for secondary_product in products[SECONDARY_KEY].keys():
            if secondary_product.endswith("met"):
                logger.info(
                    "No need to copy met file, {}, into dataset, {}, as the extractor already did that for us".format(
                        secondary_product, dataset
                    )
                )
            else:
                source = os.path.join(product_dir, secondary_product)
                target = os.path.join(dataset, secondary_product)
                logger.info("Copying {} to {}".format(source, target))
                shutil.copy(source, target)

                hashcheck = False
                if "hashcheck" in products[SECONDARY_KEY][secondary_product]:
                    hashcheck = products[SECONDARY_KEY][secondary_product]["hashcheck"]
                    print("hashcheck : {}".format(hashcheck))
                if hashcheck:
                    hash_algo = DEFAULT_HASH_ALGO
                    if "hash_algo" in products[SECONDARY_KEY][secondary_product]:
                        hash_algo = products[SECONDARY_KEY][secondary_product][
                            "hash_algo"
                        ]
                    print("hash_algo : {}".format(hash_algo))
                    create_dataset_checksums(target, hash_algo)
    return created_datasets


def get_patterns(pattern_obj_array):
    patterns = {}
    for pattern_obj in pattern_obj_array:
        hashcheck = False
        if "regex" in pattern_obj:
            if "verify" in pattern_obj:
                hashcheck = pattern_obj["verify"]
            if "hash" in pattern_obj:
                hash_algo = pattern_obj["hash"]
            else:
                hash_algo = DEFAULT_HASH_ALGO
            patterns[pattern_obj["regex"]] = {
                "hashcheck": hashcheck,
                "hash_algo": hash_algo,
            }
    return patterns


def process_outputs(product_dir, expected_outputs):
    output_files = os.listdir(product_dir)
    products = {PRIMARY_KEY: {}, SECONDARY_KEY: {}, OPTIONAL_KEY: {}}

    primary_patterns = get_patterns(expected_outputs[PRIMARY_KEY])
    secondary_patterns = get_patterns(expected_outputs[SECONDARY_KEY])
    optional_patterns = get_patterns(expected_outputs[OPTIONAL_KEY])

    for pattern in list(primary_patterns.keys()) + list(secondary_patterns.keys()):
        found_it = False

        for output_file in output_files:
            match = pattern.search(output_file)
            if match:
                found_it = True
                logger.info(
                    "Found file {} with regex pattern {}".format(
                        output_file, pattern.pattern
                    )
                )
                if pattern in primary_patterns.keys():
                    products[PRIMARY_KEY][output_file] = primary_patterns[pattern]
                else:
                    products[SECONDARY_KEY][output_file] = secondary_patterns[pattern]

        if found_it is False:
            raise IOError(
                "Could not find expected output product "
                "with the pattern '{}'".format(pattern.pattern)
            )

    for pattern in optional_patterns.keys():
        for output_file in output_files:
            match = pattern.search(output_file)
            if match:
                products[OPTIONAL_KEY][output_file] = optional_patterns[pattern]

    return products


def main():
    """
    Main entry point
    """
    product_dir = sys.argv[1]
    product_type = sys.argv[2]
    rc_file = None
    if len(sys.argv) == 4:
        rc_file = sys.argv[3]

    convert(product_dir, product_type, rc_file)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError:
        sys.exit(1)
    except Exception:
        logger.error(traceback.format_exc())
        sys.exit(1)
