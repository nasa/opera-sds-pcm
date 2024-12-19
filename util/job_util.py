import json
import logging
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def is_running_outside_verdi_worker_context():
    return not Path("_job.json").exists()


def supply_job_id():
    if is_running_outside_verdi_worker_context():
        logger.info("Running outside of job context. Generating random job ID")
        job_id = str(uuid.uuid4())
    else:
        with open("_job.json", "r+") as job:
            logger.debug("job_path: %s", str(job))
            local_job_json = json.load(job)
            logger.debug(f"{local_job_json=!s}")
        job_id = local_job_json["job_info"]["job_payload"]["payload_task_id"]

    return job_id
