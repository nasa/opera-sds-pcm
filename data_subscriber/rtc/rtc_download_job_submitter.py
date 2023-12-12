import asyncio
import json
import logging
from functools import partial
from typing import Optional

from util.job_submitter import try_submit_mozart_job

logger = logging.getLogger(__name__)


async def example(batch_id_to_urls_map, args):
    """Sample usage function."""
    job_submission_tasks = submit_rtc_download_job_submissions_tasks(batch_id_to_urls_map, args)
    results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
    logger.info(f"{len(results)=}")
    logger.info(f"{results=}")

    succeeded = [job_id for job_id in results if isinstance(job_id, str)]
    logger.info(f"{succeeded=}")
    failed = [e for e in results if isinstance(e, Exception)]
    logger.info(f"{failed=}")


def submit_rtc_download_job_submissions_tasks(batch_ids, args):
    job_submission_tasks = []
    for batch_id in batch_ids:
        mgrs_set_id_acquisition_ts_cycle_index = batch_id
        mgrs_set_id = mgrs_set_id_acquisition_ts_cycle_index.split("$")[0]

        product = {
            "_source": {
                "metadata": {
                    "batch_id": mgrs_set_id_acquisition_ts_cycle_index,
                    "mgrs_set_id": mgrs_set_id,
                }
            }
        }

        job_submission_tasks.append(
            asyncio.get_event_loop().run_in_executor(
                executor=None,
                func=partial(
                    submit_dswx_s1_job,
                    product=product,
                    job_queue=args.job_queue or f'opera-job_worker-{"rtc_data_download"}',
                    rule_name=f"trigger-rtc_download",
                    params=create_rtc_download_job_params(args, product=product, batch_ids=[batch_id]),
                    job_spec=f'job-{"rtc_download"}:{args.release_version}',
                    job_name=f"job-WF-rtc_download"
                )
            )
        )
    return job_submission_tasks


def create_rtc_download_job_params(args=None, product=None, batch_ids=None):
    return [
        {
            "name": "batch_ids",
            "value": "--batch-ids " + " ".join(batch_ids) if batch_ids else "",
            "from": "value"
        },
        {
            "name": "smoke_run",
            "value": "--smoke-run" if args.smoke_run else "",
            "from": "value"
        },
        {
            "name": "dry_run",
            "value": "--dry-run" if args.dry_run else "",
            "from": "value"
        },
        {
            "name": "endpoint",
            "value": f"--endpoint={args.endpoint}",
            "from": "value"
        },
        {
            "name": "transfer_protocol",
            "value": f"--transfer-protocol={args.transfer_protocol}",
            "from": "value"
        },
        {
            "name": "proc_mode",
            "value": f"--processing-mode={args.proc_mode}",
            "from": "value"
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
