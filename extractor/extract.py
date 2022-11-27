"""
Extractor Tool to be used to convert products into HySDS-style datasets.

Original Author: mcayanan
Adapted for OPERA PCM by Scott Collins

"""
from __future__ import print_function

import sys
import os
import shutil
import json
import argparse
import subprocess
import traceback
from typing import Dict, Optional

from util.conf_util import SettingsConf
from datetime import datetime

from commons.logger import logger
from commons.constants import product_metadata as pm

from shapely.ops import transform
from shapely.geometry import shape, mapping

from importlib import import_module

REGEX_ID_KEY = "id"
EXTRACTOR_KEY = "Extractor"
PRODUCT_TYPES_KEY = "PRODUCT_TYPES"
STRIP_FILE_EXTENSION_KEY = "Strip_File_Extension"
IS_COMPRESSED = "IsCompressed"

MULTI_OUTPUT_PRODUCT_TYPES = ['L3_DSWx_HLS']
"""
List of the product types (from settings.yaml) which produce multiple output files
which should all be bundled in the same dataset.
"""


def crawl(target_dir, product_types, workspace, extra_met=None):
    for root, subdirs, files in os.walk(target_dir):
        for f in files:
            full_file_path = os.path.join(root, f)

            try:
                extract(full_file_path, product_types, workspace, extra_met=extra_met)
            except Exception as e:
                logger.error(str(e))


def extract(
        product: str,
        product_types: Dict,
        workspace: str,
        extra_met: Optional[Dict] = None
):
    """Create a dataset (directory), with metadata extracted from the input product.

    :param product: product filepath
    :param product_types: Product config as defined in `settings.yaml`
    :param workspace: workspace directory. This directory will house created dataset directories.
    :param extra_met: extra metadata to include in the created dataset.
    """
    # Get the dataset id (product name)
    logger.debug(f"extract : product: {product}, product_types: {product_types}, "
                 f"workspace: {workspace}, extra_met: {extra_met}")

    dataset_id = create_dataset_id(product, product_types)

    logger.debug(f"extract : dataset_id: {dataset_id}")

    dataset_dir = os.path.join(workspace, dataset_id)

    if os.path.exists(dataset_dir):
        logger.warning(f"Dataset directory {dataset_dir} already exists")
    else:
        logger.info(f"Creating dataset directory {dataset_dir}")
        os.makedirs(dataset_dir)

    # Copy product to dataset directory
    logger.info(f"Moving {product} to dataset directory")
    shutil.copyfile(product, os.path.join(dataset_dir, os.path.basename(product)))

    try:
        found, product_met, ds_met, alt_ds_met = extract_metadata(
            os.path.join(dataset_dir, os.path.basename(product)),
            product_types,
            extra_met
        )

        # Write the metadata extracted from the product to the .met.json file
        if found:
            product_met.update({"id": dataset_id})

            if extra_met:
                product_met.update(extra_met)

            product_met_file = os.path.join(
                dataset_dir, f"{os.path.splitext(os.path.basename(product))[0]}.met.json"
            )

            with open(product_met_file, "w") as outfile:
                json.dump(product_met, outfile, indent=2)

            logger.info(f"Created the extracted metadata file: {product_met_file}")
        else:
            shutil.rmtree(dataset_dir)
            msg = (f"Product did not match any match pattern in the Settings.yaml: "
                   f"{os.path.basename(product)}")
            logger.error(msg)
            raise ValueError(msg)

        # Create the dataset.json file, if it hasn't been created already
        dataset_met_file = os.path.join(dataset_dir, dataset_id + ".dataset.json")

        if not os.path.exists(dataset_met_file):
            dataset_met = create_dataset_json(product_met, ds_met, alt_ds_met)

            with open(dataset_met_file, "w") as outfile:
                json.dump(dataset_met, outfile, indent=2)

            logger.info("Created the dataset.json file: {}".format(dataset_met_file))
        else:
            logger.info(f"dataset.json file already exists for {dataset_id}")

        logger.info("Successfully created/updated a dataset: {}".format(dataset_dir))
    except subprocess.CalledProcessError as se:
        logger.error("{}".format(se))
        shutil.rmtree(dataset_dir)
        raise
    except Exception as e:
        logger.error("{}".format(e))
        if os.path.exists(dataset_dir):
            shutil.rmtree(dataset_dir)
        raise

    return dataset_dir


def create_dataset_id(product, product_types):
    """
    Creates the dataset directory name for give product path.

    Parameters
    ----------
    product : str
        Path to the product to create the dataset ID for
    product_types : dict
        Maps product types to their extract configurations. Sourced from
        settings.yaml

    Returns
    -------
    dataset_id : str
        The dataset ID to use with the given product.

    """
    dataset_id = None

    logger.debug(f"extract.create_dataset_id product_types.keys: {product_types.keys()}")
    logger.info(f"Product is {product}")

    for product_type in list(product_types.keys()):
        match = product_types[product_type]["Pattern"].search(os.path.basename(product))

        if match:
            # Check if the regex matched one of multiple output products which
            # should be bundled with the same dataset ID, and if so use the "id"
            # match group value
            if product_type in MULTI_OUTPUT_PRODUCT_TYPES and REGEX_ID_KEY in match.groupdict():
                dataset_id = match.groupdict()[REGEX_ID_KEY]
            # Otherwise, default to using the product's filename to derive the dataset ID
            else:
                if product_types[product_type][STRIP_FILE_EXTENSION_KEY]:
                    dataset_id = os.path.splitext(os.path.basename(product))[0]
                else:
                    dataset_id = os.path.basename(product)

            if "Suffix" in product_types[product_type]:
                suffix = product_types[product_type]["Suffix"].strip()
                dataset_id = "{}{}".format(dataset_id, suffix)

            break

    if dataset_id is None:
        msg = (
            f"Error while trying to create the dataset directory: "
            f"File does not match any pattern in the settings.yaml: "
            f"{os.path.basename(product)}"
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Derived dataset ID is {dataset_id}")

    return dataset_id


def extract_metadata(product, product_types, catalog_met=None):
    metadata = {}
    found = False
    ds_met = {}
    alt_ds_met = {}

    for product_type in list(product_types.keys()):
        match = product_types[product_type]["Pattern"].search(os.path.basename(product))

        if match:
            logger.info(f"Found match pattern with type {product_type}")
            extractor = product_types[product_type][EXTRACTOR_KEY]
            pattern = product_types[product_type]["Pattern"].pattern
            ds_met = product_types[product_type]["Dataset_Keys"]
            ds_met.update({"type": product_type})

            if "Alt_Dataset_Keys" in product_types[product_type]:
                alt_ds_met = product_types[product_type]["Alt_Dataset_Keys"]
                alt_ds_met.update({"type": product_type})

            if extractor is not None:
                config = product_types[product_type].get('Configuration', {})

                if catalog_met is not None:
                    config["catalog_metadata"] = catalog_met

                extractor_tokens = extractor.rsplit(".", 1)  # e.g. "extractor.FilenameRegexMetExtractor"
                module = import_module(extractor)
                cls = getattr(module, extractor_tokens[1])  # e.g. "FilenameRegexMetExtractor"
                cls_object = cls()

                try:
                    metadata = cls_object.extract(product, pattern, config)
                    metadata[pm.PRODUCT_TYPE] = product_type
                except Exception as err:
                    logger.error(
                        f"Error while extracting metadata for {os.path.basename(product)}: {str(err)}"
                    )
                    raise

            found = True
            break

    return found, metadata, ds_met, alt_ds_met


def create_dataset_json(product_metadata, ds_met, alt_ds_met):
    """
    Creates the dataset.json file needed by HySDS
    """
    dataset_info = {}

    if "dataset_version" in product_metadata:
        version = str(product_metadata["dataset_version"])
    elif "VersionID" in product_metadata:
        version = str(product_metadata["VersionID"])
    else:
        logger.info(
            "Nor dataset_version nor CompositeReleaseID nor VersionID could not be found in "
            "the product metadata. Setting version to 1 in .dataset.json."
        )
        version = "1"

    logger.info(f"Setting version field in .dataset.json to {version}")
    dataset_info.update({"version": version})
    dataset_info.update({"creation_timestamp": datetime.utcnow().isoformat("T")[:-3]})

    if "Bounding_Polygon" in product_metadata:
        logger.info(f"Bounding_Polygon is {product_metadata['Bounding_Polygon']}")
        m = shape(product_metadata["Bounding_Polygon"])
        pz = transform(lambda *x: x[:2], m)
        logger.info("Transformed Bounding_Polygon: {}".format(pz))
        dataset_info.update({"location": mapping(pz)})

    for key in ["starttime", "endtime"]:
        if ds_met and key in ds_met and ds_met[key] in product_metadata:
            logger.info(f"Setting {ds_met[key]} field as the {key} in the datasets json")
            dataset_info.update({key: product_metadata[ds_met[key]]})
        elif alt_ds_met and key in alt_ds_met and alt_ds_met[key] in product_metadata:
            logger.info(f"Setting {alt_ds_met[key]} field as the {key} in the datasets json")
            dataset_info.update({key: product_metadata[alt_ds_met[key]]})

    logger.info("dataset_info is {}".format(dataset_info))

    return dataset_info


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(
        description="Converts given product to a HySDS-style dataset and "
                    "extracts necessary metadata."
    )
    parser.add_argument(
        "--workspace",
        nargs=1,
        required=False,
        help="Optionally define a workspace area to perform the extraction.",
    )
    parser.add_argument(
        "--settings",
        nargs=1,
        required=False,
        help="Optionally specify the path to the settings.yaml file. "
             "By default, it will use the one under verdi/ops/opera-pcm/conf",
    )
    parser.add_argument(
        "--extra-met",
        nargs=1,
        required=False,
        help="Optionally specify a json file containing extra metadata to add to "
             "the resulting dataset."
    )
    parser.add_argument(
        "target",
        help="A product file to convert and extract or a directory to crawl and extract."
    )

    return parser


def main():
    """
    Main entry point
    """
    args = get_parser().parse_args()
    target = os.path.abspath(args.target)
    workspace = os.getcwd()

    if args.settings:
        settings = SettingsConf(args.settings[0]).cfg
    else:
        settings = SettingsConf().cfg

    if args.workspace:
        workspace = os.path.abspath(args.workspace[0])

    extra_met = None

    if args.extra_met:
        extra_met = json.loads(args.extra_met[0])

    if os.path.isdir(target):
        crawl(target, settings[PRODUCT_TYPES_KEY], workspace, extra_met=extra_met)
    else:
        try:
            extract(target, settings[PRODUCT_TYPES_KEY], workspace, extra_met=extra_met)
        except subprocess.CalledProcessError:
            sys.exit(1)
        except Exception:
            logger.error(traceback.format_exc())
            sys.exit(1)


if __name__ == "__main__":
    main()
