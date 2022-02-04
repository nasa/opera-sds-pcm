import os
import copy
import hashlib
import json
import traceback
import re

from datetime import datetime

from opera_chimera.accountability import OperaAccountability

from commons.logger import logger
from chimera.pge_job_submitter import PgeJobSubmitter
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

from pass_accountability.es_connection import get_pass_accountability_connection

from wrapper.opera_pge_wrapper import run_pipeline

from hysds.utils import download_file, publish_datasets, get_disk_usage, makedirs

pass_es = get_pass_accountability_connection(logger)

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


class OperaPgeJobSubmitter(PgeJobSubmitter):
    def __init__(
        self,
        context,
        run_config,
        pge_config_file,
        settings_file,
        wuid=None,
        job_num=None,
    ):
        PgeJobSubmitter.__init__(
            self,
            context,
            run_config,
            pge_config_file,
            settings_file,
            wuid=wuid,
            job_num=job_num,
        )
        self.accountability = OperaAccountability(self._context)

    def get_payload_hash(self, job_type):
        """
        Function to implement a custom payload creation for the purpose of dedup,
        if you don't want the default HySDS
        payload calculation
        :param job_type:
        :return:
        """
        logger.info("Calling OPERA's payload hash calculation.")
        clean_payload = copy.deepcopy(self._run_config)

        # delete the keys from clean_payload that change on every run even though
        # runconfig is technically the same
        if "product_metadata" in clean_payload:
            del clean_payload["product_metadata"]

        clean_payload["job_type"] = job_type
        logger.info(f"Clean Payload: {json.dumps(clean_payload, indent=2)}")
        return hashlib.md5(
            json.dumps(clean_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def perform_adaptation_tasks(self, job_json):
        logger.info(f"_wuid: {self._wuid}")
        logger.info(f"_job_num: {self._job_num}")
        # encapsulate in a try catch so that the error and traceback is propagated
        error = ""
        trace = ""
        task_id = ""
        job_id = ""
        output_datasets = []
        logger.info(f"job_json: {json.dumps(job_json, indent=2)}")
        if self._wuid is None and self._job_num is None:
            # download urls
            pge_metrics = {"download": [], "upload": []}
            print(f'job_json["localize_urls"] : {job_json["localize_urls"]}')

            for localize_url in job_json["localize_urls"]:
                url = localize_url["url"]
                path = localize_url.get("local_path", None)
                if url.startswith("/"):
                    if os.path.isfile(url):
                        logger.info(f"{url} already exists, not localizing")
                        continue
                if path is None:
                    path = "%s/" % self._base_work_dir
                else:
                    if path.startswith("/"):
                        pass
                    else:
                        path = os.path.join(self._base_work_dir, path)
                if os.path.isdir(path) or path.endswith("/"):
                    path = os.path.join(path, os.path.basename(url))
                dir_path = os.path.dirname(path)
                makedirs(dir_path)
                logger.info(f"Localizing {url}")
                loc_t1 = datetime.utcnow()
                try:
                    download_file(url, path)
                except Exception as e:
                    trace = traceback.format_exc()
                    error = str(e)
                    raise RuntimeError(f"Failed to download {url}: {error}\n{trace}")
                loc_t2 = datetime.utcnow()
                loc_dur = (loc_t2 - loc_t1).total_seconds()
                path_disk_usage = get_disk_usage(path)
                pge_metrics["download"].append(
                    {
                        "url": url,
                        "path": path,
                        "disk_usage": path_disk_usage,
                        "time_start": loc_t1.isoformat() + "Z",
                        "time_end": loc_t2.isoformat() + "Z",
                        "duration": loc_dur,
                        "transfer_rate": path_disk_usage / loc_dur,
                    }
                )

            # pge_metrics.json is already created in get_dems() in precondition_function.py
            with open(os.path.join(self._base_work_dir, "pge_metrics.json"), "a") as f:
                json.dump(pge_metrics, f, indent=2)

            # set additional files to triage
            self._context["_triage_additional_globs"] = ["output", "RunConfig.yaml"]

            # set force publish (disable no-clobber)
            if not (
                match := re.search(
                    r"^job-(.+):.+$", self._context["job_specification"]["id"]
                )
            ):
                raise RuntimeError(
                    "Failed to extract job type from job specification ID: "
                    f"{self._context['job_specification']['id']}")
            job_type = match.group(1)
            force_publish = self._settings.get(oc_const.FORCE_INGEST, {}).get(
                job_type, False
            )
            if force_publish:
                self._context["_force_ingest"] = True

            # sync updates to context JSON file
            with open(os.path.join(self._base_work_dir, "_context.json"), "w") as f:
                json.dump(self._context, f, indent=2)

            local_job_json = None

            # Write out the context going to the PGE step in case we need to debug further
            with open(os.path.join(os.getcwd(), "context_pge_step.json"), "w") as f:
                json.dump(job_json, f, sort_keys=True, indent=2)

            with open(self._base_work_dir + "/_job.json", "r+") as job:
                logger.info(f"job_path: {job}")
                local_job_json = json.load(job)

            job_id = local_job_json["job_info"]["job_payload"]["payload_task_id"]
            output_datasets = []
            try:
                output_datasets = run_pipeline(job_json, self._base_work_dir)
            except Exception as e:
                trace = traceback.format_exc()
                error = str(e)
                raise RuntimeError(f"Failed to run pipeline {job_json}: {error}\n{trace}")

            logger.info(f"prod_dir: {self._base_work_dir}")
            logger.info(f"dataset_files: {output_datasets}")

            pid = 0
            pge_info = {}
            with open(self._base_work_dir + "/_pid", "r+") as pid:
                pid = int(pid.read())
            if self._settings.get(oc_const.PGE_SIM_MODE, True):
                pge_info = {
                    "time_start": datetime.utcnow().strftime(ISO_DATETIME_PATTERN)
                    + "Z",
                    "status": 0,
                    "stdout": "",
                    "stderr": "",
                }
            else:
                with open(self._base_work_dir + "/_pge_info.json", "r+") as pge_info:
                    pge_info = json.load(pge_info)
            if local_job_json is not None and pid != 0:
                # grab task_id from _job.json
                task_id = local_job_json["task_id"]
                logger.info(f"job_info: {local_job_json['job_info']}")
                # get disk usage
                disk_usage = get_disk_usage(self._base_work_dir)
                local_job_json["job_info"]["metrics"]["job_dir_size"] = disk_usage
                local_job_json["job_info"]["pid"] = pid
                local_job_json["job_info"].update(pge_info)
                local_job_json["job_info"]["datasets_cfg_file"] = (
                    self._base_work_dir + "/datasets.json"
                )
                result = publish_datasets(local_job_json, self._context)
                pge_metrics["upload"].extend(
                    local_job_json.get("job_info", {})
                    .get("metrics", {})
                    .get("products_staged", [])
                )
                with open(
                    os.path.join(self._base_work_dir, "pge_metrics.json"), "w"
                ) as f:
                    json.dump(pge_metrics, f, indent=2)
                if not result:
                    raise Exception(f"Failed to publish datasets: {local_job_json}")
            return {
                "output_datasets": output_datasets,
                "error": error,
                "traceback": trace,
                "task_id": task_id,
                "job_id": job_id,
            }
        else:
            return job_json
