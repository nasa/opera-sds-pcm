import asyncio
import os
from datetime import datetime, timezone
from functools import partial
from pathlib import PurePath
from typing import Dict, List, Optional, Any

from commons.logger import get_logger
from util.job_submitter import try_submit_mozart_job


async def example(mgrs_set_to_s3paths_map, args, settings=None):
    """
    Sample usage function for submitting DSWx-NI jobs.
    
    Args:
        mgrs_set_to_s3paths_map: Dictionary mapping MGRS set IDs to lists of S3 paths
        args: Command-line arguments
        settings: Settings dictionary
    """
    logger = get_logger()
    job_submission_tasks = submit_dswx_ni_job_submissions_tasks(mgrs_set_to_s3paths_map, args, settings)
    results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
    logger.info(f"{len(results)=}")
    logger.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logger.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logger.info(f"{failed=}")


def submit_dswx_ni_job_submissions_tasks(mgrs_set_to_s3paths_map, args, settings=None):
    """
    Create job submission tasks for DSWx-NI processing.
    
    Args:
        mgrs_set_to_s3paths_map: Dictionary mapping MGRS set IDs to lists of S3 paths
        args: Command-line arguments
        settings: Settings dictionary
        
    Returns:
        List of job submission tasks
    """
    logger = get_logger()
    job_submission_tasks = []
    
    for mgrs_set_id, s3paths in mgrs_set_to_s3paths_map.items():
        if not s3paths:
            logger.warning(f"No S3 paths for MGRS set {mgrs_set_id}. Skipping job submission.")
            continue
            
        # Generate a unique batch ID for this job
        batch_id = f"DSWx-NI-{mgrs_set_id}-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
        
        # Create the product metadata
        product = {
            "_id": batch_id,
            "_source": {
                "dataset": f"L3_DSWx_NI-{batch_id}",
                "metadata": {
                    "batch_id": batch_id,
                    "product_paths": {"NISAR_L2_GCOV": s3paths},
                    "mgrs_set_id": mgrs_set_id,
                    "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "FileName": batch_id,
                    "id": batch_id,
                    "Files": [
                        {
                            "FileName": PurePath(s3path).name,
                            "FileSize": 1,
                            "FileLocation": os.path.dirname(s3path),
                            "id": PurePath(s3path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3path in s3paths
                    ]
                }
            }
        }
        
        # Get the appropriate release version
        release_version = args.release_version if hasattr(args, "release_version") and args.release_version else settings.get("RELEASE_VERSION", "main")

        # Create and append the job submission task
        job_submission_tasks.append(
            partial(
                submit_dswx_ni_job,
                product=product,
                job_queue=f'opera-job_worker-{"sciflo-l3_dswx_ni"}',
                rule_name=f'trigger-{"SCIFLO_L3_DSWx_NI"}',
                params=create_job_params(product),
                job_spec=f'job-{"SCIFLO_L3_DSWx_NI"}:{release_version}',
                job_name=f'job-WF-{"SCIFLO_L3_DSWx_NI"}'
            )
        )
        
        logger.info(f"Created job submission task for MGRS set {mgrs_set_id} with {len(s3paths)} input files")
        
    return job_submission_tasks


def create_job_params(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Create job parameters for a DSWx-NI job.
    
    Args:
        product: Product dictionary with metadata
        
    Returns:
        List of job parameters
    """
    return [
        {
            "name": "dataset_type",
            "from": "value",
            "type": "text",
            "value": "NISAR_L2_GCOV"
        },
        {
            "name": "input_dataset_id",
            "type": "text",
            "from": "value",
            "value": product["_source"]["metadata"]["mgrs_set_id"]
        },
        {
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": product["_source"]
        }
    ]


def submit_dswx_ni_job(*, product: Dict[str, Any], job_queue: str, rule_name: str, 
                       params: List[Dict[str, Any]], job_spec: str, 
                       job_type: Optional[str] = None, job_name: str) -> str:
    """
    Submit a DSWx-NI job to Mozart.
    
    Args:
        product: Product dictionary with metadata
        job_queue: Job queue name
        rule_name: Rule name for the job
        params: Job parameters
        job_spec: Job specification
        job_type: Optional job type
        job_name: Job name
        
    Returns:
        Job ID
    """
    return try_submit_mozart_job(
        product=product,
        job_queue=job_queue,
        rule_name=rule_name,
        params=params,
        job_spec=job_spec,
        job_type=job_type,
        job_name=job_name
    )
