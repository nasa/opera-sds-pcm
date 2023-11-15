"""Functions for submitting HySDS jobs"""
import uuid
from typing import Optional

import backoff
from hysds_commons.job_utils import submit_mozart_job as submit_job


def try_submit_mozart_job(*, product: dict, job_queue: str, rule_name, params: list[dict[str, str]], job_spec: str, job_type: Optional[str] = None, job_name) -> str:
    """
    Submits a HySDS job. Must be executed within a HySDS cluster.

    Returns a unique job ID if successful, else raises an Exception
    """
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
    return submit_job(
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
    return submit_job(
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
