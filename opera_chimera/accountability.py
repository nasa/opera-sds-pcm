from typing import Dict, List

import backoff
import re
import json
import os
from datetime import datetime

from chimera.commons.accountability import Accountability

from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

from util.conf_util import SettingsConf
from chimera.logger import logger

from job_accountability.es_connection import get_job_accountability_connection

grq_es = get_job_accountability_connection(logger)


PGE_STEP_DICT = {
    # "L0A": "L0A_L_RRST_PP"
}


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


class OperaAccountability(Accountability):
    def __init__(self, context, work_dir):
        Accountability.__init__(self, context, work_dir)

        self.trigger_dataset_type = context.get(oc_const.DATASET_TYPE)
        self.trigger_dataset_id = context.get(oc_const.INPUT_DATASET_ID)
        # self.step = context.get(oc_const.STEP)  # TODO chrisjrd: resolve
        # self.product_paths = context.get(oc_const.PRODUCT_PATHS)

        metadata: Dict[str, str] = context["product_metadata"]["metadata"]
        self.product_paths: List[str] = [product_path for band_or_qa, product_path in metadata.items() if band_or_qa != '@timestamp']

        self.inputs = []
        # TODO chrisjrd: resolve getting dataset type associated with the input files
        # if os.path.exists("{}/datasets.json".format(work_dir)):
        #     with open("{}/datasets.json".format(work_dir), "r") as reader:
        #         datasets_cfg = json.load(reader)
        #
        #     if isinstance(self.product_paths, list):
        #         self.input_files_type = get_dataset(self.product_paths[0], datasets_cfg)
        #     else:
        #         self.input_files_type = get_dataset(self.product_paths, datasets_cfg)
        # else:
        #     self.input_files_type = context.get(oc_const.DATASET_TYPE)
        self.input_files_type = "TBD"  # TODO chrisjrd: resolve
        if isinstance(self.product_paths, list):
            self.inputs = list(map(lambda x: os.path.basename(x), self.product_paths))
        else:
            self.inputs = [os.path.basename(self.product_paths)]
        # TODO chrisjrd: resolve
        #  Use:
        #  * wf_name/purpose job param from job-spec
        #  * PGE output type from PGE config YAML
        #  * .sf.xml name (workflow name / wf_name?)
        # self.output_type = PGE_STEP_DICT[self.step]
        self.output_type = "L3_DSWx"  # got this from PGE config YAML

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
            grq_es.index_document(index=oc_const.JOB_ACCOUNTABILITY_INDEX, id=self.job_id, body=payload)
        else:
            raise Exception("Unable to create job_accountability_catalog entry: {}".format(self.product_paths))

    def get_entries(self):
        entries = []
        if isinstance(self.product_paths, list):
            for input_path in self.product_paths:
                input = os.path.basename(input_path)
                results = grq_es.query(body={
                                "query": {"bool": {"must": [{"term": {"_id": input}}]}}
                            }, index="grq")
                entries.extend(results)
        else:
            input = os.path.basename(self.product_paths)
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
            logger.info("entry accountability object: {}".format(acc_obj))
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

        logger.info("accountability obj: {}".format(acc))
        return acc

    def update_product_met_json(self, job_result):
        work_dir = job_result.get("work_dir")
        datasets_path = "{}/output/datasets/".format(work_dir)
        datasets = os.listdir(datasets_path)
        accountability_obj = self.flatten_and_merge_accountability()

        for dataset in datasets:
            output_met_json = "{}/{}/{}.met.json".format(datasets_path, dataset, dataset)
            met_json = None
            with open(output_met_json, "r") as f:
                met_json = json.load(f)
                accountability_obj_copy = accountability_obj.copy()
                accountability_obj_copy[self.output_type] = {
                    "id": dataset,
                    "job_id": self.job_id,
                    "inputs": self.inputs,
                    "input_data_type": self.input_files_type,
                    "trigger_dataset_type": self.trigger_dataset_type,
                    "trigger_dataset_id": self.trigger_dataset_id
                }
                met_json["accountability"] = accountability_obj_copy
            with open(output_met_json, "w") as f:
                logger.info("to write: {}".format(met_json))
                if met_json is not None:
                    json.dump(met_json, f)

    def set_products(self, job_results):
        self.update_product_met_json(job_result=job_results)
        return {}
