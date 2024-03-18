import asyncio
import logging
import os
from functools import partial
from pathlib import PurePath
from typing import Optional

from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client
from util.job_submitter import try_submit_mozart_job

logger = logging.getLogger(__name__)


async def example(batch_id_to_urls_map, args):
    """Sample usage function."""
    job_submission_tasks = submit_dswx_s1_job_submissions_tasks(batch_id_to_urls_map, args)
    results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
    logger.info(f"{len(results)=}")
    logger.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logger.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logger.info(f"{failed=}")


def submit_dswx_s1_job_submissions_tasks(uploaded_batch_id_to_s3paths_map, args, settings=None):
    job_submission_tasks = []
    mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
    for batch_id, s3paths in uploaded_batch_id_to_s3paths_map.items():
        mgrs_set_id = batch_id.split("$")[0]
        bounding_box = mbc_client.get_bounding_box_for_mgrs_set_id(mgrs, mgrs_set_id)

        product = {
            "_id": batch_id,
            "_source": {
                "dataset": f"L3_DSWx_S1-{batch_id}",
                "metadata": {
                    "batch_id": batch_id,
                    "product_paths": {"L2_RTC_S1": s3paths},
                    "mgrs_set_id": mgrs_set_id,
                    "FileName": batch_id,
                    "id": batch_id,
                    "bounding_box": bounding_box,
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

        job_submission_tasks.append(
            asyncio.get_event_loop().run_in_executor(
                executor=None,
                func=partial(
                    submit_dswx_s1_job,
                    product=product,
                    # job_queue=f'opera-job_worker-{"sciflo-l3_dswx_s1"}',
                    job_queue=None,  # TODO chrisjrd: testing if this uses queue name from rule.
                    rule_name=f'trigger-{"SCIFLO_L3_DSWx_S1"}',
                    params=create_job_params(product),
                    job_spec=f'job-{"SCIFLO_L3_DSWx_S1"}:{args.release_version or settings["RELEASE_VERSION"]}',
                    job_name=f'job-WF-{"SCIFLO_L3_DSWx_S1"}'
                )
            )
        )
    return job_submission_tasks


def create_job_params(product):
    return [
        {
          "name":"dataset_type",
          "from": "value",
          "type":"text",
          "value":"L2_RTC_S1"
        },
        {
          "name":"input_dataset_id",
          "type":"text",
          "from":"value",
          "value": product["_source"]["metadata"]["mgrs_set_id"]
        },
        {
           "name": "product_metadata",
           "from": "value",
           "type": "object",
           "value": product["_source"]
        }
    ]


def submit_dswx_s1_job(*, product: dict, job_queue: str, rule_name, params: list[dict[str, str]], job_spec: str, job_type: Optional[str] = None, job_name) -> str:
    return try_submit_mozart_job(
        product=product,
        job_queue=job_queue,
        rule_name=rule_name,
        params=params,
        job_spec=job_spec,
        job_type=job_type,
        job_name=job_name
    )
