import json
import os
import re
from datetime import datetime
from typing import Dict

import backoff

import job_accountability.catalog
from chimera.commons.accountability import Accountability
from chimera.logger import logger
from data_subscriber.es_conn_util import get_es_connection
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from util.conf_util import SettingsConf

grq_es = get_es_connection(logger)


def get_dataset(key, datasets_cfg):
    dataset = None
    for ds in datasets_cfg["datasets"]:
        dataset_regex = re.compile(ds["match_pattern"])
        file_name = os.path.basename(key)
        match = dataset_regex.search("/{}".format(file_name))
        if match:
            group_dict = match.groupdict()
            ipath = ds["ipath"].format(**group_dict)
            dataset = ipath.split("hysds::data/", 1)[1]
            break

    return dataset


def get_dataset_type(file):
    cfg = SettingsConf().cfg
    data_name = None
    for type, type_cfg in list(cfg["PRODUCT_TYPES"].items()):
        matched = type_cfg["Pattern"].match(file)
        if matched:
            data_name = type
            break

    return data_name


@backoff.on_exception(backoff.expo, Exception, max_value=13, max_time=34)
def search_es(query, idx):
    results = grq_es.search(body=query, index=idx)
    if len(results["hits"]["hits"]) > 0:
        return results
    raise RuntimeError


def remove_suffix(input_string: str, suffix: str) -> str:
    """Polyfill for str.removesuffix function introduced in Python 3.9."""
    if suffix and input_string.endswith(suffix):
        return input_string[:-len(suffix)]
    return input_string


class OperaAccountability(Accountability):
    def __init__(self, context: Dict, work_dir: str):
        Accountability.__init__(self, context, work_dir)

        self.trigger_dataset_type = self.context[oc_const.DATASET_TYPE]
        self.trigger_dataset_id = self.context[oc_const.INPUT_DATASET_ID]
        self.input_files_type = self.trigger_dataset_type

        product_metadata = self.context["product_metadata"]

        # TODO: kludge to support providing dummy metadata as a hardcoded string,
        #       either containing the json formatted metadata itself, or an S3 URI
        #       to a json file containing the product metadata, within the
        #       hysds-io.json file for DSWx-S1 and DISP-S1, remove when appropriate
        try:
            metadata = product_metadata["metadata"]
        except Exception as err:
            if isinstance(product_metadata, str):
                if product_metadata.startswith("s3://"):
                    import boto3
                    bucket, key = product_metadata.split('/', 2)[-1].split('/', 1)
                    s3 = boto3.resource('s3')
                    obj = s3.Object(bucket, key)
                    product_metadata = obj.get()['Body'].read()

                metadata = json.loads(product_metadata)["metadata"]
            else:
                raise err

        input_metadata = {}
        if self.input_files_type in ('L2_HLS_L30', 'L2_HLS_S30'):
            self.product_paths = [os.path.join(metadata['FileName'])]
            input_metadata["ids"] = [nested_product["id"] for nested_product in metadata["Files"]]
            input_metadata["filenames"] = [nested_product["FileName"] for nested_product in metadata["Files"]]
        elif self.input_files_type in ('L1_S1_SLC',):
            self.product_paths = [os.path.join(metadata['FileLocation'], metadata['FileName'])]
        elif self.input_files_type in ('L2_GSLC_NI',):
            self.product_paths = []  # TODO: Populate this when given proper metadata
        elif self.input_files_type in ('L2_RTC_S1', 'L2_CSLC_S1',):
            self.product_paths = [os.path.join(file_metadata['FileLocation'], file_metadata['FileName'])
                                  for file_metadata in metadata.get('Files', {})]
        elif self.input_files_type in ('L2_CSLC_S1_STATIC',):
            self.product_paths = metadata["product_paths"]["L2_CSLC_S1_STATIC"]
        elif self.input_files_type in ('L2_NISAR_GCOV',):
            self.product_paths = metadata["product_paths"]["L2_NISAR_GCOV"]
        elif self.input_files_type in ('L4_TROPO',):
            self.product_paths = [os.path.join(metadata['FileLocation'], metadata['FileName'])]
        else:
            raise RuntimeError(f'Unknown input file type "{self.input_files_type}"')

        self.output_type = self.context["wf_name"]
        self.inputs = [os.path.basename(product_path) for product_path in self.product_paths]
        self.input_metadata = input_metadata

    def create_job_entry(self):
        if self.job_id is not None:
            payload = {
                "output_data_type": self.output_type,
                "inputs": self.inputs,
                "input_data_type": self.input_files_type,
                "trigger_dataset_type": self.trigger_dataset_type,
                "trigger_dataset_id": self.trigger_dataset_id,
                "created_at": datetime.now().isoformat()
            }
            if self.input_metadata:
                payload["metadata"] = self.input_metadata
            grq_es.index_document(index=job_accountability.catalog.generate_es_index_name(), id=self.job_id, body=payload)
        else:
            raise Exception("Unable to create job_accountability_catalog entry: {}".format(self.product_paths))

    def get_entries(self):
        entries = []
        for input_path in self.product_paths:
            input = os.path.basename(input_path)
            results = grq_es.query(body={
                            "query": {"bool": {"must": [{"term": {"_id": input}}]}}
                        }, index="grq")
            entries.extend(results)
        return entries

    def flatten_and_merge_accountability(self):
        entries = self.get_entries()
        acc = {}
        for entry in entries:
            if "accountability" not in entry["_source"]["metadata"]:
                acc_obj = {}
            else:
                acc_obj = entry["_source"]["metadata"]["accountability"]
            logger.debug("entry accountability object: {}".format(acc_obj))
            for pge in acc_obj:
                if pge in acc:
                    if "id" in acc_obj[pge]:
                        acc[pge]["outputs"].append(acc_obj[pge]["id"])
                    else:
                        acc[pge]["outputs"].extend(acc_obj[pge]["outputs"])

                    if "trigger_dataset_id" in acc_obj[pge]:
                        acc[pge]["trigger_dataset_ids"].append(acc_obj[pge]["trigger_dataset_id"])
                    else:
                        acc[pge]["trigger_dataset_ids"].extend(acc_obj[pge]["trigger_dataset_ids"])

                    if "job_id" in acc_obj[pge]:
                        acc[pge]["job_ids"].append(acc_obj[pge]["job_id"])
                    else:
                        acc[pge]["job_ids"].extend(acc_obj[pge]["job_ids"])

                    acc[pge]["inputs"].extend(acc_obj[pge]["inputs"])

                    acc[pge] = {
                        "outputs": list(set(acc[pge]["outputs"])),
                        "inputs": list(set(acc[pge]["inputs"])),
                        "input_data_type": acc_obj[pge]["input_data_type"],
                        "job_ids": list(set(acc[pge]["job_ids"])),
                        "trigger_dataset_type": acc_obj[pge]["trigger_dataset_type"],
                        "trigger_dataset_ids": list(set(acc[pge]["trigger_dataset_ids"]))
                    }

                    if "metadata" in acc_obj[pge]:
                        acc[pge]["metadata"] = acc_obj[pge]["metadata"]
                else:
                    acc[pge] = {
                        "inputs": acc_obj[pge]["inputs"],
                        "input_data_type": acc_obj[pge]["input_data_type"],
                        "trigger_dataset_type": acc_obj[pge]["trigger_dataset_type"],
                    }

                    if "id" in acc_obj[pge]:
                        acc[pge]["outputs"] = [acc_obj[pge]["id"]]
                    else:
                        acc[pge]["outputs"] = acc_obj[pge]["outputs"]

                    if "job_id" in acc_obj[pge]:
                        acc[pge]["job_ids"] = [acc_obj[pge]["job_id"]]
                    else:
                        acc[pge]["job_ids"] = acc_obj[pge]["job_ids"]

                    if "trigger_dataset_id" in acc_obj[pge]:
                        acc[pge]["trigger_dataset_ids"] = [acc_obj[pge]["trigger_dataset_id"]]
                    else:
                        acc[pge]["trigger_dataset_ids"] = acc_obj[pge]["trigger_dataset_ids"]

                    if acc_obj[pge].get("metadata"):
                        acc[pge]["metadata"] = acc_obj[pge]["metadata"]

        logger.debug("accountability obj: {}".format(acc))
        return acc

    def update_product_met_json(self, job_result):
        """Creates a .met.json with updated accountability metadata."""
        work_dir = job_result.get("work_dir")
        datasets_path = f"{work_dir}/{self.context['pge_output_dir']}/datasets/"
        datasets = os.listdir(datasets_path)
        old_accountability = self.flatten_and_merge_accountability()

        for dataset in datasets:
            new_accountability = old_accountability.copy()
            new_accountability[self.output_type] = {
                "id": dataset,
                "job_id": self.job_id,
                "inputs": self.inputs,
                "input_data_type": self.input_files_type,
                "trigger_dataset_type": self.trigger_dataset_type,
                "trigger_dataset_id": self.trigger_dataset_id
            }

            if self.input_metadata:
                new_accountability[self.output_type]["metadata"] = self.input_metadata

            output_met_json_filepath = f"{datasets_path}/{dataset}/{dataset}.met.json"
            with open(output_met_json_filepath, "r") as f:
                met_json = json.load(f)
                met_json["accountability"] = new_accountability

            with open(output_met_json_filepath, "w") as f:
                logger.debug(f"{met_json=}")
                json.dump(met_json, f)

    def set_products(self, job_results):
        self.update_product_met_json(job_result=job_results)
        return {}
