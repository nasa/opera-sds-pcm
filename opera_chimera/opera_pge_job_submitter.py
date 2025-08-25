import copy
import hashlib
import json
import os
import traceback
from datetime import datetime, timezone

from chimera.pge_job_submitter import PgeJobSubmitter
from opera_commons.logger import logger
from hysds.utils import get_disk_usage, makedirs
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from util.pge_util import download_file_with_hysds, write_pge_metrics
from wrapper.opera_pge_wrapper import run_pipeline

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
        logger.info("Clean Payload: {}".format(json.dumps(clean_payload, indent=2)))
        return hashlib.md5(
            json.dumps(clean_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def perform_adaptation_tasks(self, job_json):
        logger.debug("_wuid: {}".format(self._wuid))
        logger.debug("_job_num: {}".format(self._job_num))

        error = ""
        trace = ""
        task_id = ""

        logger.debug("job_json: {}".format(json.dumps(job_json, indent=2)))

        if self._wuid is None and self._job_num is None:
            # download urls
            pge_metrics = {"download": [], "upload": []}

            for localize_url in job_json["localize_urls"]:
                url = localize_url["url"]
                path = localize_url.get("local_path", None)

                if url.startswith("/"):
                    if os.path.isfile(url):
                        logger.info("{} already exists, not localizing".format(url))
                        continue

                if path is None:
                    path = "%s/" % self._base_work_dir
                else:
                    if not path.startswith("/"):
                        path = os.path.join(self._base_work_dir, path)

                if os.path.isdir(path) or path.endswith("/"):
                    path = os.path.join(path, os.path.basename(url))

                dir_path = os.path.dirname(path)
                makedirs(dir_path)

                logger.info("Localizing {}".format(url))

                # Download the current file and update current PGE download metrics
                # with results of transfer
                new_pge_metrics = download_file_with_hysds(url, path)
                pge_metrics["download"].extend(new_pge_metrics["download"])

            # Commit metrics for all downloaded files back to disk
            write_pge_metrics(os.path.join(self._base_work_dir, "pge_metrics.json"), pge_metrics)

            # set additional files to triage
            self._context["_triage_additional_globs"] = [
                "output", "RunConfig.yaml", "pge_input_dir", "pge_runconfig_dir",
                "pge_output_dir", "pge_scratch_dir"
            ]

            # set force publish (disable no-clobber)
            force_publish = self._settings.get(oc_const.FORCE_INGEST, {}).get(
                oc_const.INGEST_STAGED, False
            )

            if force_publish:
                logger.info("Disabling no-clobber errors")
                self._context["_force_ingest"] = True

            # sync updates to context JSON file
            with open(os.path.join(self._base_work_dir, "_context.json"), "w") as f:
                json.dump(self._context, f, indent=2)

            # Write out the context going to the PGE step in case we need to debug further
            with open(os.path.join(os.getcwd(), "context_pge_step.json"), "w") as f:
                json.dump(job_json, f, sort_keys=True, indent=2)

            with open(self._base_work_dir + "/_job.json", "r+") as job:
                logger.info("job_path: {}".format(job))
                local_job_json = json.load(job)

            job_id = local_job_json["job_info"]["job_payload"]["payload_task_id"]

            try:
                output_datasets = run_pipeline(job_json, self._base_work_dir)
            except Exception as e:
                trace = traceback.format_exc()
                error = str(e)
                raise RuntimeError(
                    "Failed to run pipeline {}: {}\n{}".format(job_json, error, trace)
                )

            logger.debug("dataset_files: {}".format(output_datasets))

            with open(self._base_work_dir + "/_pid", "r+") as pid:
                pid = int(pid.read())

            if self._settings.get(oc_const.PGE_SIM_MODE, True):
                pge_info = {
                    "time_start": datetime.now(timezone.utc).strftime(ISO_DATETIME_PATTERN)
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

                # get disk usage
                disk_usage = get_disk_usage(self._base_work_dir)
                local_job_json["job_info"]["metrics"]["job_dir_size"] = disk_usage
                local_job_json["job_info"]["pid"] = pid
                local_job_json["job_info"].update(pge_info)
                local_job_json["job_info"]["datasets_cfg_file"] = (
                    self._base_work_dir + "/datasets.json"
                )

                logger.debug("job_info: {}".format(local_job_json["job_info"]))

            return {
                "output_datasets": output_datasets,
                "error": error,
                "traceback": trace,
                "task_id": task_id,
                "job_id": job_id,
            }
        else:
            return job_json
