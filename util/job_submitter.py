"""Functions for submitting HySDS jobs"""
import uuid
from typing import Optional

import backoff
from hysds_commons.job_utils import submit_mozart_job as submit_job


def try_submit_mozart_job(*, product: dict, job_queue: str, rule_name, params: list[dict[str, str]], job_spec: str, job_type: Optional[str] = None, job_name, payload_hash=None) -> str:
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
        job_name=job_name,
        payload_hash=payload_hash
    )


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def _try_submit_mozart_job_minimal(*, product: Optional[dict], job_queue: str, rule_name, hysdsio: dict, job_name, payload_hash=None) -> str:
    """
    Submits a mozart job with the minimal number of required parameters.
    Clients should note that jobs are submitted with enable_dedup=true.
    """
    return _submit_mozart_job_minimal(
        product=product,
        job_queue=job_queue,
        rule_name=rule_name,
        hysdsio=hysdsio,
        job_name=job_name,
        payload_hash=payload_hash
    )


def _submit_mozart_job_minimal(*, product: Optional[dict], job_queue: str, rule_name, hysdsio: dict, job_name, payload_hash=None) -> str:
    """Do not call directly. See wrapper with @backoff decorator for usage"""
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
        payload_hash=payload_hash,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None
    )


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, jitter=None)
def _try_submit_mozart_job_by_rule_minimal(*, product: Optional[dict], rule_name, job_type: str, job_queue: str, job_name, payload_hash=None) -> str:
    """Submits a mozart job with hysdsio specified by an existing rule, rather than a given hysdsio specification."""
    return _submit_mozart_job_by_rule_minimal(
        product=product,
        rule_name=rule_name,
        job_type=job_type,
        job_queue=job_queue,
        job_name=job_name,
        payload_hash=payload_hash
    )


def _submit_mozart_job_by_rule_minimal(*, product: Optional[dict], rule_name, job_type: str, job_queue: str, job_name, payload_hash=None) -> str:
    """Do not call directly. See wrapper with @backoff decorator for usage"""
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
        payload_hash=payload_hash,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component="grq"  # hysds-io information is in the hysds_ios-grq index rather thann hysds_ios-mozart
    )
