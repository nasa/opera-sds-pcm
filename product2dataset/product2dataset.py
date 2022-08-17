"""
Code to convert PGE outputs to HySDS-style datasets.

@author: mcayanan
Adapted for OPERA PCM by Scott Collins

"""
from __future__ import print_function

import glob
import json
import os
import shutil
import subprocess
import sys
import traceback
from pathlib import Path, PurePath
from typing import Dict, List

from commons.logger import logger
from extractor import extract
from util import datasets_json_util, job_json_util
from util.checksum_util import create_dataset_checksums
from util.conf_util import SettingsConf, PGEOutputsConf

PRIMARY_KEY = "Primary"
SECONDARY_KEY = "Secondary"
OPTIONAL_KEY = "Optional"
DEFAULT_HASH_ALGO = "sha256"
DATASETS_DIR_NAME = "datasets"


def convert(
        product_dir: str,
        pge_name: str,
        rc_file: str = None,
        pge_output_conf_file:str = None,
        settings_conf_file: str = None,
        extra_met: Dict = None,
        **kwargs
) -> List:
    """Convert a product (directory of files) into a list of datasets.

    :param product_dir: Local filepath to the product.
    :param pge_name: PGE outputs config entry key. See `PGEOutputsConf`.
    :param rc_file: Local filepath to the RunConfig file.
    :param pge_output_conf_file: Local filepath to the `pge_output.yaml` file.
    :param settings_conf_file: Local filepath to the `settings.yaml` file.
    :param extra_met: Extra metadata to include in *each* created dataset.
    """
    extra_met = extra_met if extra_met else {}

    # Check to see if all expected outputs were generated
    product_dir = os.path.abspath(product_dir)
    pge_outputs_cfg = PGEOutputsConf(pge_output_conf_file).cfg
    pge_config = pge_outputs_cfg[pge_name]
    products = process_outputs(product_dir, pge_config["Outputs"])

    extra_met.update({"tags": ["PGE"]})

    settings = SettingsConf(settings_conf_file).cfg

    # Create the datasets
    created_datasets = set()
    output_types = [PRIMARY_KEY, OPTIONAL_KEY]
    for output_type in output_types:
        for product in products[output_type].keys():
            logger.info(f"Converting {product} to a dataset")

            dataset_dir = extract.extract(
                os.path.join(product_dir, product),
                settings[extract.PRODUCT_TYPES_KEY],
                os.path.join(product_dir, DATASETS_DIR_NAME),
                extra_met=extra_met,
            )

            hashcheck = products[output_type][product].get("hashcheck", False)

            if hashcheck:
                hash_algo = products[output_type][product].get("hash_algo", DEFAULT_HASH_ALGO)
                create_dataset_checksums(os.path.join(dataset_dir, product), hash_algo)

            created_datasets.add(dataset_dir)

    for dataset in created_datasets:
        dataset_id = PurePath(dataset).name

        # Merge all created .met.json files into a single one for use with accountability reporting
        dataset_met_json = {"Files": []}
        combined_file_size = 0
        for met_json_file in glob.iglob(os.path.join(dataset, '*.met.json')):
            with open(met_json_file, 'r') as infile:
                met_json = json.load(infile)
                combined_file_size += int(met_json["FileSize"])

                # Extract a copy of the "Product*" key/values to include at the top level
                # They should be the same values for each file in the dataset
                product_keys = list(filter(lambda key: key.startswith("Product"), met_json.keys()))

                for product_key in product_keys:
                    extra_met[product_key] = met_json[product_key]
                    met_json.pop(product_key)

                dataset_met_json["Files"].append(met_json)

            # Remove the individual .met.json files after they've been merged
            os.unlink(met_json_file)

        # Add fields to the top-level of the .met.json file
        dataset_met_json["FileSize"] = combined_file_size
        dataset_met_json["FileName"] = dataset_id
        dataset_met_json["id"] = dataset_id

        if pge_name == "L3_HLS":
            logger.info(f"Detected {pge_name} for publishing. Creating {pge_name} PGE-specific entries.")
            state_config_product_metadata: Dict = {key: value for key, value in kwargs["state_config_product_metadata"].items() if key != "@timestamp"}

            first_product_info_key: str = list(state_config_product_metadata.keys())[0]  # typically a band name or QA mask like "B01" or "Fmask"
            first_product_info: Dict = state_config_product_metadata[first_product_info_key]

            logger.info(list(Path(".").iterdir()))
            with open(PurePath("./_job.json")) as fp:
                job_json_dict = json.load(fp)

            with open(PurePath("./datasets.json")) as fp:
                datasets_json_dict = json.load(fp)

            dataset_type = job_json_util.find_param_value(job_json_dict, "dataset_type")
            dataset_type = dataset_type.split("-")[0]  # extract from dataset type like "L2_HLS_S30-state-config"

            l2_hls_publish_s3_bucket = datasets_json_util.find_s3_bucket(datasets_json_dict, dataset_type)

            l2_hls_publish_s3_url = datasets_json_util.find_s3_url(datasets_json_dict, dataset_type)
            l2_hls_publish_s3_url_parts = PurePath(l2_hls_publish_s3_url).parts

            dataset_met_json["input_granule_id"] = PurePath(first_product_info["id"]).stem  # strip band from ID to get granule ID
            dataset_met_json["product_urls"] = [
                f'{l2_hls_publish_s3_url_parts[0]}'  # http:
                f'//{l2_hls_publish_s3_url_parts[1]}'  # <bucket>.s3.<region>.amazonaws.com/<key>
                f'/products/{file["id"]}/{file["FileName"]}'
                for file in dataset_met_json["Files"]]
            dataset_met_json["product_s3_paths"] = [
                f'products/{file["id"]}/{file["FileName"]}'
                for file in dataset_met_json["Files"]]

        if "dswx_hls" in dataset_id.lower():
            collection_name: str = settings.get("DSWX_COLLECTION_NAME")
            dataset_met_json["CollectionName"] = collection_name
            logger.info(f"Setting CollectionName {collection_name} for DAAC delivery.")

        dataset_met_json.update(extra_met)
        dataset_met_json_path = os.path.join(dataset, f"{dataset_id}.met.json")

        logger.info(f"Creating combined dataset metadata file {dataset_met_json_path}")
        with open(dataset_met_json_path, 'w') as outfile:
            json.dump(dataset_met_json, outfile, indent=2)

        # Rename RunConfig to its dataset
        if rc_file:
            renamed_rc_file = os.path.join(product_dir, f"{os.path.basename(dataset)}.rc.yaml")
            logger.info(f"Copying RunConfig file to {renamed_rc_file}")
            shutil.copyfile(rc_file, renamed_rc_file)
            products[SECONDARY_KEY][os.path.basename(renamed_rc_file)] = {"hashcheck": False}

        for secondary_product in products[SECONDARY_KEY].keys():
            source = os.path.join(product_dir, secondary_product)
            target = os.path.join(dataset, secondary_product)
            logger.info(f"Copying {source} to {target}")
            shutil.copy(source, target)

            hashcheck = products[SECONDARY_KEY][secondary_product].get("hashcheck", False)

            if hashcheck:
                hash_algo = products[SECONDARY_KEY][secondary_product].get("hash_algo", DEFAULT_HASH_ALGO)
                create_dataset_checksums(target, hash_algo)

    return list(created_datasets)


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
                    f"Found file {output_file} with regex pattern {pattern.pattern}"
                )
                if pattern in primary_patterns.keys():
                    products[PRIMARY_KEY][output_file] = primary_patterns[pattern]
                else:
                    products[SECONDARY_KEY][output_file] = secondary_patterns[pattern]

        if found_it is False:
            raise IOError(
                f"Could not find expected output product with the pattern '{pattern.pattern}'"
            )

    for pattern in optional_patterns.keys():
        for output_file in output_files:
            match = pattern.search(output_file)
            if match:
                logger.info(
                    f"Found optional file {output_file} with regex pattern {pattern.pattern}"
                )
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
