import logging
import warnings
from functools import partial
from typing import Callable

import backoff
import requests
from requests import Response
from requests.auth import HTTPBasicAuth

from benchmark import conftest
from benchmark.benchmark_test_util import raise_

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s")

config = conftest.config

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

request_job_statuses: Callable[..., Response] = partial(
    requests.post,
    url=f'https://{config["ES_HOST"]}/mozart_es/job_status/_msearch?',
    verify=False,
    auth=HTTPBasicAuth(config["ES_USER"], config["ES_PASSWORD"]),
    headers={"Content-Type": "application/x-ndjson"}
)


# COPIED FROM BENCHMARK
def success_handler(details):
    if details["tries"] > 1:
        logging.info(
            f'Successfully called {details["target"].__name__}(...) after {details["tries"]} tries and {details["elapsed"]:f} seconds')


def jobs_finished(job_statuses):
    return job_statuses != set() and job_statuses.issubset({"job-completed", "job-deduped", "job-failed", "job-revoked"})


# BASED ON BENCHMARK
@backoff.on_predicate(
    backoff.constant,
    lambda job_statuses: not jobs_finished(job_statuses),
    max_time=60 * 10,
    interval=60,
    jitter=None,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
)
def wait_for_query_jobs_to_finish(job_type: str):
    return wait_for_job_to_finish_helper(job_type)


@backoff.on_predicate(
    backoff.constant,
    lambda job_statuses: not jobs_finished(job_statuses),
    max_time=60 * 40,
    interval=60,
    jitter=None,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
)
def wait_for_download_jobs_to_finish(job_type: str):
    return wait_for_job_to_finish_helper(job_type)


@backoff.on_predicate(
    backoff.constant,
    lambda job_statuses: not jobs_finished(job_statuses),
    max_time=60 * 60 * 5,  # TODO chrisjrd: 138 granules in 31 minutes
    interval=60 * 5,
    jitter=None,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
)
def wait_for_slc_pge_jobs_to_finish(job_type: str):
    return wait_for_job_to_finish_helper(job_type)


@backoff.on_predicate(
    backoff.constant,
    lambda job_statuses: not jobs_finished(job_statuses),
    max_time=60 * 30,  # TODO chrisjrd: 138 granules in 31 minutes
    interval=60,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
)
def wait_for_pge_jobs_to_finish(job_type: str):
    return wait_for_job_to_finish_helper(job_type)


@backoff.on_predicate(
    backoff.constant,
    lambda job_statuses: not jobs_finished(job_statuses),
    max_time=60 * 30,
    interval=60,
    jitter=None,
    on_success=success_handler,
    on_giveup=lambda _: raise_(Exception()),
)
def wait_for_jobs_to_finish(job_type: str):
    return wait_for_job_to_finish_helper(job_type)


def wait_for_job_to_finish_helper(job_type: str):
    logging.debug(f"{job_type=}")
    tosca_response_body_dict = search_jobs(job_type=job_type)
    job_statuses = [
        hit["_source"]["status"]
        for response in tosca_response_body_dict["responses"]
        for hit in response["hits"]["hits"]
    ]
    logging.debug(f"num_jobs={len(job_statuses)}")
    job_statuses = {job_status for job_status in job_statuses}
    logging.debug(f"{job_statuses=}")
    return job_statuses


def search_jobs(job_type: str):
    payload = '''\
{"preference":"figaro-results"}
{"query":{\
"bool":{\
"must":[\
{"bool":{\
"must":[\
{"term":{"resource":"job"}},\
{"term":{"job.type":"%s"}}]}}]}},\
"size":10000,\
"aggs":{\
"job.type":{\
"terms":{\
"field":"job.type",\
"size":1000,\
"order":{"_count":"desc"}}}},\
"_source":{\
"includes":["_index","_id","status","resource","payload_id","@timestamp","short_error","error","traceback","msg_details","tags","job.name","job.priority","job.retry_count","job.type","job.job_info.execute_node","job.job_info.facts.ec2_instance_type","job.job_info.job_queue","job.job_info.duration","job.job_info.job_url","job.job_info.time_queued","job.job_info.time_start","job.job_info.time_end","job.job_info.metrics.products_staged.id","job.delivery_info.redelivered","event.traceback","user_tags","dedup_job","endpoint_id"],\
"excludes":[]},\
"from":0,\
"sort":[{"@timestamp":{"order":"desc","missing":"_last","unmapped_type":"long"}}]}
''' % job_type
    tosca_response: Response = request_job_statuses(data=payload)
    tosca_response_body_dict: dict = tosca_response.json()

    num_search_results = len([
        hit["_source"]
        for response in tosca_response_body_dict["responses"]
        for hit in response["hits"]["hits"]
    ])
    logging.info(f'{job_type=}, {num_search_results=}')

    logging.debug(f'{job_type=}, {tosca_response_body_dict=}')
    return tosca_response_body_dict
