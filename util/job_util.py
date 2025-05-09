import concurrent.futures
import json
import logging
import os
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


def multithread_gather(job_submission_tasks, max_workers=min(32, (os.cpu_count() or 1) + 4)):
    """
    Given a list of tasks, executes them concurrently and gathers the results.
    Exceptions are returned as results rather than re-raised.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(job_submission_task) for job_submission_task in job_submission_tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                result = exc
            results.append(result)
    return results
