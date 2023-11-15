import asyncio
import json
import logging
import uuid
from functools import partial
from typing import Optional

from util.job_submitter import try_submit_mozart_job

logger = logging.getLogger(__name__)


async def example(batch_id_to_urls_map, args):
    """Sample usage function."""
    job_submission_tasks = submit_job_submissions_tasks(batch_id_to_urls_map, args)
    results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
    logger.info(f"{len(results)=}")
    logger.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logger.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logger.info(f"{failed=}")


def submit_job_submissions_tasks(batch_id_to_urls_map, args):
    job_submission_tasks = []
    for batch_id, urls in batch_id_to_urls_map.items():
        chunk_id = str(uuid.uuid4())
        logger.info(f"{chunk_id=}")
        # TODO chrisjrd: implement realistic product
        product = {
            "_id": "dummy_id",
            "_source": {
                "dataset": "",
                "metadata": {
                    "batch_id": batch_id,
                    "urls": urls,
                    "FileName": "OPERA_L2_RTC-S1_T047-100908-IW3_20200702T231843Z_20230305T140222Z_S1B_30_v0.1_VV.tif",
                    "id": "OPERA_L2_RTC-S1_T047-100908-IW3_20200702T231843Z_20230305T140222Z_S1B_30_v0.1",
                    "$comment": "$._source.metadata.Files[] is currently a placeholder",  # TODO chrisjrd: remove placeholder and comment
                    "Files": [
                        {
                            "FileName": "OPERA_L2_RTC-S1_T047-100908-IW3_20200702T231843Z_20230305T140222Z_S1B_30_v0.1_VV.tif",
                            "FileSize": 1,
                            "FileLocation": "/DNM",
                            "id": "OPERA_L2_RTC-S1_T047-100908-IW3_20200702T231843Z_20230305T140222Z_S1B_30_v0.1",
                        }
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
                    job_queue=args.job_queue or f'opera-job_worker-{"sciflo-l3_dswx_s1"}',
                    rule_name=f'trigger-{"SCIFLO_L3_DSWx_S1"}',  # TODO chrisjrd: use actual value
                    # job_type=f'hysds-io-{"SCIFLO_L3_DSWx_S1"}:{args.release_version}',
                    params=create_job_params(product),
                    job_spec=f'job-{"SCIFLO_L3_DSWx_S1"}:{args.release_version}',  # TODO chrisjrd: use actual values
                    job_name=f'job-WF-{"SCIFLO_L3_DSWx_S1"}',  # TODO chrisjrd: use actual value
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
          "value": "OPERA_L2_RTC-S1_T047-100908-IW3_20200702T231843Z_20230305T140222Z_S1B_30_v0.1"
        },
        {
           "name": "product_metadata",
           "from": "value",
           "type": "object",
           "value": json.dumps(product["_source"])
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
