
import asyncio
import contextlib
import itertools
import math
import os
from math import ceil
from typing import Optional, Iterable

import aiohttp
import backoff
import requests
from requests.exceptions import HTTPError

from commons.logger import get_logger


async def async_cmr_posts(url, request_bodies: list):
    """Given a list of request bodies, performs CMR queries asynchronously, returning the response JSONs."""
    async with aiohttp.ClientSession() as session:
        tasks = []
        sem = asyncio.Semaphore(1)

        for request_body in request_bodies:
            tasks.append(async_cmr_post(url, request_body, session, sem))
        responses = await asyncio.gather(*tasks)

    return list(itertools.chain.from_iterable(responses))


async def async_cmr_post(url, data: str, session: aiohttp.ClientSession, sem: Optional[asyncio.Semaphore]):
    """Issues a request asynchronously. If a semaphore is provided, it will use it as a context manager."""
    logger = get_logger()

    sem = sem if sem is not None else contextlib.nullcontext()

    async with sem:
        page_size = 2000  # default is 10, max is 2000
        data += f"&page_size={page_size}"

        logger.debug(f"async_cmr_post({url=}..., {len(data)=:,}, {data[-250:]=}")
        max_pages = 1  # cap the number of pages (requests) to scroll through results.
        # after first response, update with the smallest of the forced max and the number of hits

        current_page = 1
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Client-Id': f'nasa.jpl.opera.sds.pcm.data_subscriber.{os.environ["USER"]}'
        }

        logger.info("Issuing request. This may take a while depending on search page size and number of pages/results.")

        response_jsons = []
        while current_page <= max_pages:
            async with await fetch_post_url(session, url, data, headers) as response:
                response_json = await response.json()
                response_jsons.append(response_json)

            if current_page == 1:
                logger.debug(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
                max_pages = math.ceil(response_json["hits"]/page_size)
                logger.debug("Updating max pages to %d", max_pages)

            logger.debug(f'CMR query (cmr-query-page {current_page} of {ceil(response_json["hits"]/page_size)}): '
                         f'{len(response_json["items"])=:,}')

            cmr_search_after = response.headers.get("CMR-Search-After")
            logger.debug(f"{cmr_search_after=}")

            if cmr_search_after:
                headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

            if len(response_json["items"]) < page_size:
                logger.info("Reached end of CMR search results. Ending query.")
                break

            current_page += 1
            if not current_page <= max_pages:
                logger.warning(
                    "Reached max pages limit (%d). Not all search results exhausted. "
                    "Adjust limit or time ranges to process all hits, then re-run this script.",
                    max_pages
                )

        return response_jsons


def giveup_cmr_requests(e):
    """giveup function for use with @backoff decorator when issuing CMR queries to retry on intermittent 504 errors."""
    if isinstance(e, aiohttp.ClientResponseError):
        if e.status == 413 and e.message == "Payload Too Large":  # give up. Fix bug
            return True
        if e.status == 400:  # Bad Request. give up. Fix bug
            return True
        if e.status == 504 and e.message == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    return False


@backoff.on_exception(
    backoff.expo,
    exception=(aiohttp.ClientResponseError, aiohttp.ClientOSError),  # ClientOSError happens when connection is closed by peer
    max_tries=7,  # NOTE: increased number of attempts because of random API unreliability and slowness
    jitter=None,
    giveup=giveup_cmr_requests
)
async def fetch_post_url(session: aiohttp.ClientSession, url, data: str, headers):
    return await session.post(url, data=data, headers=headers, raise_for_status=True)


def cmr_requests_get(args, request_url, params):
    """
    DEPRECATED. Issues a CMR request using GET.
    Newer code should use cmr_client.async_cmr_post.
    """
    page_size = 2000  # default is 10, max is 2000
    params["page_size"] = page_size

    logger.debug(f"_request_search_concurrent({request_url=}, {params=}")

    response_jsons = []
    max_pages = 1  # cap the number of pages (requests) to scroll through results.
    # update after first response

    current_page = 1
    headers = {
        'Client-Id': f'nasa.jpl.opera.sds.pcm.data_subscriber.{os.environ["USER"]}'
    }
    while current_page <= max_pages:
        response = try_request_get(request_url, params, headers, raise_for_status=True)
        response_json = response.json()
        response_jsons.append(response_json)

        if current_page == 1:
            logger.info(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
            max_pages = math.ceil(response_json["hits"] / page_size)
            logger.info(f"Updating max pages to {max_pages=}")
        logger.info(f'CMR number of granules (cmr-query-page {current_page} of {max_pages}): {len(response_json["items"])=:,}')

        cmr_search_after = response.headers.get("CMR-Search-After")
        logger.debug(f"{cmr_search_after=}")
        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

        if len(response_json["items"]) < page_size:
            logger.info("Reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if not current_page <= max_pages:
            logger.warning(
                f"Reached max pages limit. {max_pages=}"
                "Not all search results exhausted. "
                "Adjust limit or time ranges to process all hits, then re-run this script."
            )

    return response_jsons


def giveup_cmr_requests(e):
    """giveup function for use with @backoff decorator when issuing CMR requests using blocking `requests` functions."""
    if isinstance(e, HTTPError):
        if e.response.status_code == 413 and e.response.reason == "Payload Too Large":  # give up. Fix bug
            return True
        if e.response.status_code == 400:  # Bad Request. give up. Fix bug
            return True
        if e.response.status_code == 504 and e.response.reason == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    return False


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError,),
    max_tries=7,  # NOTE: increased number of attempts because of random API unreliability and slowness
    jitter=None,
    giveup=giveup_cmr_requests
)
def try_request_get(request_url, params, headers=None, raise_for_status=True):
    response = requests.get(request_url, params=params, headers=headers)
    if raise_for_status:
        response.raise_for_status()
    return response


def paramss_to_request_body(paramss: Iterable[dict]):
    """See params_to_request_body"""
    return [params_to_request_body(params) for params in paramss]


def params_to_request_body(params: dict):
    """
    Utility function for converting a dict of request params (i.e. GET query params) into a form encoded request body
    (POST form params) acceptable by CMR.

    Iterables will have their param names suffixed with `[]` if needed, like "native-id[]" or "ShortName[]"
    """
    s = ""
    for k, v in params.items():
        if isinstance(v, Iterable) and not isinstance(v, str):
            tmp = ""
            for it in v:
                if k.endswith("[]"):
                    tmp += "&{}={}".format(k, it)
                else:
                    tmp += "&{}[]={}".format(k, it)
            s += tmp
        else:
            s += "&{}={}".format(k, v)
    return s
