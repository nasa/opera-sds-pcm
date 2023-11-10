import asyncio
import json
import logging
import uuid
from functools import partial
from typing import Optional

import backoff
from hysds_commons.job_utils import submit_mozart_job

logger = logging.getLogger(__name__)


async def example(batch_id_to_urls_map, args):
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
    return _try_submit_mozart_job_minimal(
        product=product or {},
        job_queue=job_queue,
        rule_name=rule_name,
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec
        },
        # job_type=job_type,
        job_name=job_name
    )


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def _try_submit_mozart_job_minimal(*, product: Optional[dict], job_queue: str, rule_name, hysdsio: dict, job_name) -> str:
    return _submit_mozart_job_minimal(
        product=product,
        job_queue=job_queue,
        rule_name=rule_name,
        hysdsio=hysdsio,
        job_name=job_name
    )


def _submit_mozart_job_minimal(*, product: Optional[dict], job_queue: str, rule_name, hysdsio: dict, job_name) -> str:
    return submit_mozart_job(
        product=product or {},
        rule={
            "rule_name": rule_name,
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        hysdsio=hysdsio,
        queue=None,
        job_name=job_name,
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None
    )


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def _try_submit_mozart_job_by_rule_minimal(*, product: Optional[dict], rule_name, job_type: str, job_queue: str, job_name) -> str:
    return _submit_mozart_job_by_rule_minimal(
        product=product,
        rule_name=rule_name,
        job_type=job_type,
        job_queue=job_queue,
        job_name=job_name
    )


def _submit_mozart_job_by_rule_minimal(*, product: Optional[dict], rule_name, job_type: str, job_queue: str, job_name) -> str:
    return submit_mozart_job(
        product=product or {},
        rule={
            "rule_name": rule_name,
            "job_type": job_type,
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        hysdsio=None,  # setting None forces lookup based on rule and component.
        queue=None,
        job_name=job_name,
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component="grq"  # hysds-io information is in the hysds_ios-grq index rather thann hysds_ios-mozart
    )