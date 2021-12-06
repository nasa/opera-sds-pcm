"""
Extractor Tool to be used to convert products into HySDS-style datasets.

@author: mcayanan
"""
from __future__ import print_function

import sys
import os
import shutil
import json
import argparse
import subprocess
import traceback
import glob
import gzip

from util.conf_util import SettingsConf
from util.common_util import fix_timestamp
from util.checksum_util import create_dataset_checksums
from datetime import datetime

from copy import deepcopy

from commons.logger import logger
from commons.constants import product_metadata as pm

from shapely.ops import transform
from shapely.geometry import shape, mapping

from importlib import import_module

EXTRACTOR_KEY = "Extractor"
PRODUCT_TYPES_KEY = "PRODUCT_TYPES"
SIGNAL_FILE_EXTENSION = {"tlm": "qac"}
STRIP_FILE_EXTENSION_KEY = "Strip_File_Extension"
IS_COMPRESSED = "IsCompressed"


def crawl(target_dir, product_types, workspace, extra_met=None):
    for root, subdirs, files in os.walk(target_dir):
        for f in files:
            full_file_path = os.path.join(root, f)
            """ Skip .met or signal """
            if full_file_path.endswith(".met") or full_file_path.endswith(
                SIGNAL_FILE_EXTENSION
            ):
                continue
            try:
                extract(full_file_path, product_types, workspace, extra_met=extra_met)
            except Exception as e:
                logger.error(str(e))


def extract(product, product_types, workspace, extra_met=None,
            create_hash=False, hash_type="md5", context_file=None):
    """ Get the dataset id (product name) """
    dataset_id, product, is_compressed, orig_unzip_prod_name = create_dataset_id(
        product, product_types
    )
    logger.info("extract : {} : {}: {}: {}".format(product, product_types, workspace, extra_met))
    logger.info("extract : dataset_id, product, is_compressed, orig_unzip_prod_name {} : {} : {} : {}".format(dataset_id, product, is_compressed, orig_unzip_prod_name))
    dataset_dir = os.path.join(workspace, dataset_id)
    if os.path.exists(dataset_dir):
        raise OSError("Dataset directory already exists: {}".format(dataset_dir))

    temp_dir = os.path.join(workspace, dataset_id + ".temp")

    if not os.path.exists(workspace):
        os.makedirs(workspace)

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)

    """ Copy product to dataset directory """
    logger.info("Move {} to  dataset directory ".format(product))
    shutil.copyfile(product, os.path.join(temp_dir, os.path.basename(product)))
    if is_compressed:
        shutil.copyfile(
            orig_unzip_prod_name,
            os.path.join(temp_dir, os.path.basename(orig_unzip_prod_name)),
        )

    """ Move associated signal file if it exists """
    prod_type = product.split(".")[1]
    signal_ext = SIGNAL_FILE_EXTENSION.get(prod_type)
    signal_file = "{}.{}".format(product, signal_ext)

    if os.path.exists(signal_file):
        shutil.copyfile(
            signal_file, os.path.join(temp_dir, os.path.basename(signal_file))
        )

    """ Move associated met file if it exists """
    for met_ext in [".met", ".met.json"]:
        met_file = product + met_ext
        if os.path.exists(met_file):
            shutil.copyfile(
                met_file,
                os.path.join(temp_dir, "{}.orig".format(os.path.basename(met_file))),
            )

    """ Move temp directory to actual dataset directory """
    os.rename(temp_dir, dataset_dir)

    try:
        """ If the original met json file exists, load it in."""
        orig_met = {}
        orig_json_met_file = glob.glob(os.path.join(dataset_dir, "*.orig"))
        if len(orig_json_met_file) != 0:
            if os.path.exists(orig_json_met_file[0]):
                with open(orig_json_met_file[0]) as json_file:
                    orig_met = json.load(json_file)

        """ fix ISO8601 timestamps from PGE/SAS that are incompatible with ES"""
        for k in orig_met:
            if isinstance(orig_met[k], str):
                orig_met[k] = fix_timestamp(orig_met[k])
        """
        Temporarily merge the orig_met with the extra_met in case we need to use it when
        extracting metadata
        """
        catalog_met = deepcopy(orig_met)
        if extra_met:
            catalog_met = merge_met(catalog_met, extra_met)

        """ Call metadata extractor to create the .met.json """
        """Need to add .gz for compressed file"""
        if is_compressed:
            found, product_met, ds_met, alt_ds_met = extract_metadata(
                os.path.join(dataset_dir, os.path.basename(orig_unzip_prod_name)),
                product_types,
                catalog_met,
            )
            """Remove zip file """
            os.remove(os.path.join(dataset_dir, os.path.basename(orig_unzip_prod_name)))
        else:
            found, product_met, ds_met, alt_ds_met = extract_metadata(
                os.path.join(dataset_dir, os.path.basename(product)),
                product_types,
                catalog_met,
            )

        if found:
            """Metadata specified in the .met takes precedence over the ones
            set by the extractor.
            """
            product_met.update({"id": dataset_id})
            if extra_met:
                product_met.update(extra_met)

            product_met = merge_met(orig_met, product_met)
            # For R2 L0B, the Bounding_Polygon is a string in the metadata:
            #   "Bounding_Polygon": "{ 'coordinates': [ [ [ -54.5779364809, 3.17501288828, 661.000000002 ],
            #     [ -54.5768895296, 3.17523014835, 661.000000001],
            #     [ -54.5758427039, 3.17544738120, 660.999999999],
            #     -54.5779364809, 3.17501288828, 661.000000002] ] ],
            #     'type': 'Polygon' }",
            # Therefore, this kludge
            # will essentially convert it back to a dictionary so that the shape function
            # won't complain
            polygon = product_met.get("Bounding_Polygon", None)
            if polygon and isinstance(polygon, str):
                poly_str = polygon.replace("'", "\"")
                product_met["Bounding_Polygon"] = json.loads(poly_str)
            product_met_file = os.path.join(
                dataset_dir, "{}.met.json".format(dataset_id)
            )
            with open(product_met_file, "w") as f:
                json.dump(product_met, f, indent=2)
                f.close()
            logger.info("Created the met.json file: {}".format(product_met_file))
        else:
            shutil.rmtree(dataset_dir)
            raise ValueError(
                "Product did not match any match pattern in the "
                "Settings.yaml: {}".format(dataset_dir)
            )

        """ Create the dataset.json """
        dataset_met = create_datasetjson(product_met, ds_met, alt_ds_met)
        dataset_met_file = os.path.join(dataset_dir, dataset_id + ".dataset.json")
        with open(dataset_met_file, "w") as f:
            json.dump(dataset_met, f, indent=2)
            f.close()
        logger.info("Created the dataset.json file: {}".format(dataset_met_file))

        if create_hash is True:
            f = os.path.join(dataset_dir, os.path.basename(product))
            logger.info("Creating checksum file for {}".format(f))
            create_dataset_checksums(f, hash_type)

        logger.info("Successfully created a dataset: {}".format(dataset_dir))
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
    This creates the dataset directory name.

    :param product:
    :param product_types:
    :return:
    """
    dataset_id = None
    is_compressed = None
    orig_unzip_prod_name = None
    logger.info("extract.create_dataset_id product_types.keys:")
    logger.info("Product is {}".format(product))
    for type in list(product_types.keys()):
        logger.info(type)
        match = product_types[type]["Pattern"].search(os.path.basename(product))
        if match:
            if product_types[type][STRIP_FILE_EXTENSION_KEY]:
                if IS_COMPRESSED in product_types[type]:
                    is_compressed = True
                    orig_unzip_prod_name = product
                    dataset_id = os.path.splitext(os.path.basename(product))[0].split(
                        "."
                    )[0]
                    """uncompress"""
                    unzip_prod_name = product[:-3]
                    logger.info("unzip_prod_name is {}".format(unzip_prod_name))
                    with gzip.open(product, "rb") as f_in:
                        with open(unzip_prod_name, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    product = unzip_prod_name
                    logger.info("After unzipping, product is {}".format(product))
                else:
                    dataset_id = os.path.splitext(os.path.basename(product))[0]
            else:
                dataset_id = os.path.basename(product)

            if "Suffix" in product_types[type]:
                suffix = product_types[type]["Suffix"].strip()
                dataset_id = "{}{}".format(dataset_id, suffix)

    if dataset_id is None:
        msg = (
            "Error while trying to create the dataset directory: File does "
            "not match any pattern in the settings.yaml: {}".format(
                os.path.basename(product)
            )
        )
        logger.error(msg)
        raise ValueError(msg)
    logger.info("Dataset ID will be {}".format(dataset_id))
    return dataset_id, product, is_compressed, orig_unzip_prod_name


def merge_met(source, target):
    """
    If metadata key exists in the source, then that takes precedence over
    what is specified in the target.
    """
    for key in list(target.keys()):
        logger.info("key : {}".format(key))
        if key not in source:
            source[key] = target[key]
        elif key == pm.PRODUCT_COUNTER:
            source[key] = target[key]
        elif key == "restaged":
            logger.info("source[key] : {}".format(source[key]))
            logger.info("target[key] : {}".format(target[key]))
            if source[key] != target[key]:
                source[key] = True
        else:
            logger.warning("{} already exists in the original .met.".format(key))
    return source


def extract_metadata(product, product_types, catalog_met=None):
    metadata = {}
    found = False
    ds_met = {}

    for type in list(product_types.keys()):
        pattern = None
        match = product_types[type]["Pattern"].search(os.path.basename(product))
        if match:
            logger.info("Found match pattern with type {}".format(type))
            extractor = product_types[type][EXTRACTOR_KEY]
            pattern = product_types[type]["Pattern"].pattern
            ds_met = product_types[type]["Dataset_Keys"]
            ds_met.update({"type": type})
            alt_ds_met = None
            if "Alt_Dataset_Keys" in product_types[type]:
                alt_ds_met = product_types[type]["Alt_Dataset_Keys"]
                alt_ds_met.update({"type": type})
            if extractor is not None:
                config = product_types[type].get('Configuration', {})
                if catalog_met is not None:
                    config["catalog_metadata"] = catalog_met
                extractor_tokens = extractor.rsplit(".", 1)
                module = import_module(extractor)
                cls = getattr(module, extractor_tokens[1])
                cls_object = cls()
                try:
                    metadata = cls_object.extract(product, pattern, config)
                    metadata[pm.PRODUCT_TYPE] = type
                except subprocess.CalledProcessError as e:
                    logger.error(
                        "Error while extracting metadata for {}\n{}".format(
                            os.path.basename(product), e.output
                        )
                    )
                    raise
            found = True
            break

    return found, metadata, ds_met, alt_ds_met


def create_datasetjson(metjson, ds_met, alt_ds_met):
    """
    Creates the dataset.json file needed by HySDS
    """
    dataset_info = {}
    if "CompositeReleaseID" in metjson:
        version = metjson["CompositeReleaseID"].lower()
    elif "VersionID" in metjson:
        version = str(metjson["VersionID"])
    else:
        logger.info(
            "CompositeReleaseID or VersionID could not be found in "
            "the .met.json. Setting version to 1 in .dataset.json"
        )
        version = "1"
    logger.info("Setting version field in .dataset.json to {}".format(version))
    dataset_info.update({"version": version})
    dataset_info.update({"creation_timestamp": datetime.utcnow().isoformat("T")[:-3]})
    if "Bounding_Polygon" in metjson:
        logger.info("Bounding_Polygon is {}".format(metjson["Bounding_Polygon"]))
        m = shape(metjson["Bounding_Polygon"])
        pz = transform(lambda *x: x[:2], m)
        logger.info("Transformed Bounding_Polygon: {}".format(pz))
        dataset_info.update({"location": mapping(pz)})
    for key in ["starttime", "endtime"]:
        if ds_met and key in ds_met and ds_met[key] in metjson:
            logger.info(
                "Setting {} field as the {} in the datasets "
                "json".format(ds_met[key], key)
            )
            dataset_info.update({key: metjson[ds_met[key]]})
        elif alt_ds_met and key in alt_ds_met and alt_ds_met[key] in metjson:
            logger.info(
                "Setting {} field as the {} in the datasets "
                "json".format(alt_ds_met[key], key)
            )
            dataset_info.update({key: metjson[alt_ds_met[key]]})
    logger.info("dataset_info is {}".format(dataset_info))
    return dataset_info


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(
        description="Converts given product to a HySDS-style dataset "
        "and extracts necessary metadata."
    )
    parser.add_argument(
        "--workspace",
        nargs=1,
        required=False,
        help="Optionally define a workspace area to " "perform the extraction.",
    )
    parser.add_argument(
        "--settings",
        nargs=1,
        required=False,
        help="Optionally specify the path to the "
        "settings.yaml file. By default, it will use the "
        "one under verdi/ops/opera-pcm/conf",
    )
    parser.add_argument(
        "target",
        help="A product file to convert and extract "
        "or a directory to crawl and extract.",
    )
    parser.add_argument(
        "--extra_met",
        nargs=1,
        required=False,
        help="Optionally specify a json that contains extra "
             "metadata to add to the resulting dataset."
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
