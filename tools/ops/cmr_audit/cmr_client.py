
import asyncio
import contextlib
import itertools
import json
import math
import os
from typing import Optional, Iterable

import aiohttp
import backoff
import requests
from requests.exceptions import HTTPError

from opera_commons.logger import get_logger


async def async_cmr_posts(url, request_bodies: list, sem: Optional[asyncio.Semaphore] = None,
                           output_dir: Optional[str] = None, verify_complete: bool = False):
    """Given a list of request bodies, performs CMR queries asynchronously, returning the response JSONs.

    When output_dir is set, streams results to JSONL files on disk and returns list of file paths instead.
    verify_complete is passed through to async_cmr_post.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []

        concurrency = 1 if len(request_bodies) == 1 else min(len(request_bodies), 15)
        sem = asyncio.Semaphore(concurrency) if not sem else sem

        if output_dir:
            paths = []
            for i, request_body in enumerate(request_bodies):
                path = os.path.join(output_dir, f"cmr_batch_{i}.jsonl")
                paths.append(path)
                tasks.append(async_cmr_post(url, request_body, session, sem, output_path=path,
                                            verify_complete=verify_complete))
            await asyncio.gather(*tasks)
            return paths
        else:
            for request_body in request_bodies:
                tasks.append(async_cmr_post(url, request_body, session, sem, verify_complete=verify_complete))
            responses = await asyncio.gather(*tasks)
            return list(itertools.chain.from_iterable(responses))


class IncompleteCmrResultsError(Exception):
    """CMR counted more granules than it returned, or answered with CMR-Timed-Out.

    Only raised when ``async_cmr_post`` is asked to ``verify_complete``. Carries the counts so the
    caller can log what CMR claimed versus what it delivered.
    """

    def __init__(self, hits, items: int, timed_out: bool, pages: int, request: str, reason: Optional[str] = None):
        self.hits = hits
        self.items = items
        self.timed_out = timed_out
        self.pages = pages
        self.request = request
        self.reason = reason or ("CMR-Timed-Out" if timed_out else shortfall_reason(hits, items))
        super().__init__(f"incomplete CMR results ({self.reason}, {pages} page(s)) for {request}")


def page_incomplete_reason(response_json: dict, headers) -> Optional[str]:
    """Why a single CMR page cannot be trusted, or None.

    One policy for every CMR consumer here: a page CMR marked CMR-Timed-Out is a subset of the
    answer, whatever its item count.
    """
    if str(headers.get("CMR-Timed-Out", "")).lower() == "true":
        return "CMR-Timed-Out"
    return None


def shortfall_reason(hits, items_seen: int) -> Optional[str]:
    """Why an answer is short of CMR's own count, or None (complete, or no count to check against)."""
    if hits is not None and items_seen < hits:
        return f"{items_seen:,} of {hits:,} items returned"
    return None


async def async_cmr_post(url, data: str, session: aiohttp.ClientSession, sem: Optional[asyncio.Semaphore] = None,
                         output_path: Optional[str] = None, verify_complete: bool = False, max_attempts: int = 3):
    """Issues a request asynchronously. If a semaphore is provided, it will use it as a context manager.

    When output_path is set, streams items to a JSONL file on disk instead of accumulating in memory.
    Returns empty list when output_path is set (items are on disk), otherwise returns list of response JSONs.

    With verify_complete, the result is checked against CMR's own count: no page may carry a CMR-Timed-Out
    header and the items returned across all pages must reach ``hits`` (CMR's count is read from the first
    page and can drift upward while scrolling, so more items than counted is fine; fewer is not). An
    incomplete result is re-queried from the first page (max_attempts in total) and then raised as
    IncompleteCmrResultsError, so a caller never mistakes a partial listing for the whole one. Without it
    (the default) a shortfall is only logged.
    """
    logger = get_logger()

    sem = sem if sem is not None else contextlib.nullcontext()

    async with sem:
        page_size = 2000  # default is 10, max is 2000
        data += f"&page_size={page_size}"

        logger.debug(f"async_cmr_post({url=}..., {len(data)=:,}, {data[-250:]=}")

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Client-Id': f'nasa.jpl.opera.sds.pcm.data_subscriber.{os.environ["USER"]}'
        }

        # A retry rewinds the output file to where this call started writing.
        output_offset = os.path.getsize(output_path) if output_path and os.path.exists(output_path) else 0

        for attempt in range(1, max_attempts + 1):
            if attempt > 1 and output_path:
                with open(output_path, 'r+b') as f:
                    f.truncate(output_offset)

            response_jsons, hits, items_seen, problem, pages = await _fetch_all_pages(
                session, url, data, dict(headers), page_size, output_path, logger)

            why = problem or shortfall_reason(hits, items_seen)
            if why is None:
                return response_jsons

            if not verify_complete:
                logger.warning("CMR results may be incomplete (%s) for %s", why, data[-250:])
                return response_jsons

            logger.warning("Incomplete CMR results (%s) on attempt %d of %d for %s", why, attempt, max_attempts,
                           data[-250:])
            if attempt < max_attempts:
                await asyncio.sleep(2 ** attempt)  # give CMR a moment rather than re-issuing into the same state

        raise IncompleteCmrResultsError(hits, items_seen, why == "CMR-Timed-Out", pages, data[-250:], reason=why)


async def _fetch_all_pages(session, url, data, headers, page_size, output_path, logger):
    """Pages through one CMR query. Returns (response_jsons, hits, items_seen, problem, pages).

    ``problem`` is the first page-level reason the answer cannot be trusted (see page_incomplete_reason),
    or None; a shortfall against ``hits`` is the caller's call via shortfall_reason.
    """
    max_pages = 1  # cap the number of pages (requests) to scroll through results.
    # after first response, update with the smallest of the forced max and the number of hits

    current_page = 1
    hits = None
    items_seen = 0
    problem = None
    pages_fetched = 0

    logger.info("Issuing request. This may take a while depending on search page size and number of pages/results.")

    response_jsons = []
    while current_page <= max_pages:
        async with await fetch_post_url(session, url, data, headers) as response:
            response_json = await response.json()
        pages_fetched += 1

        if output_path:
            with open(output_path, 'a') as f:
                for item in response_json["items"]:
                    f.write(json.dumps(item) + '\n')
        else:
            response_jsons.append(response_json)

        items_seen += len(response_json["items"])
        problem = problem or page_incomplete_reason(response_json, response.headers)

        if current_page == 1:
            hits = response_json.get("hits")
            logger.debug(f'CMR number of granules (cmr-query): {hits=}')
            max_pages = math.ceil((hits or 0) / page_size)
            logger.debug("Updating max pages to %d", max_pages)

        logger.debug(f'CMR query (cmr-query-page {current_page} of {max_pages}): '
                     f'{len(response_json["items"])=:,}')

        cmr_search_after = response.headers.get("CMR-Search-After")
        logger.debug(f"{cmr_search_after=}")

        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})
        else:
            logger.info("Reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if not current_page <= max_pages:
            logger.warning(
                "Reached max pages limit (%d). Not all search results exhausted. "
                "Adjust limit or time ranges to process all hits, then re-run this script.",
                max_pages
            )

    return response_jsons, hits, items_seen, problem, pages_fetched


def is_transient_cmr_error(exc: BaseException) -> bool:
    """A request failure worth retrying or skipping over: CMR/network trouble, not a bad request.

    The same split giveup_cmr_requests makes for the backoff decorator: 4xx (bad bbox, payload too
    large) is permanent and must surface; 429/5xx, dropped connections and timeouts are transient.
    """
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 429 or exc.status >= 500
    return isinstance(exc, (aiohttp.ClientOSError, aiohttp.ServerTimeoutError, asyncio.TimeoutError))


def giveup_cmr_requests(e):
    """giveup function for use with @backoff decorator when issuing CMR queries using blocking `requests` functions to retry on intermittent 504 errors."""
    if isinstance(e, aiohttp.ClientResponseError):
        if e.status == 413 and e.message == "Payload Too Large":  # give up. Fix bug
            return True
        if e.status == 400:  # Bad Request. give up. Fix bug
            return True
        if e.status == 504 and e.message == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    if isinstance(e, HTTPError):
        if e.response.status_code == 413 and e.response.reason == "Payload Too Large":  # give up. Fix bug
            return True
        if e.response.status_code == 400:  # Bad Request. give up. Fix bug
            return True
        if e.response.status_code == 504 and e.response.reason == "Gateway Time-out":  # CMR sometimes returns this. Don't give up hope
            return False
    return False  # True to give up. False to keep trying.


@backoff.on_exception(
    backoff.expo,
    exception=(aiohttp.ClientResponseError, aiohttp.ClientOSError),  # ClientOSError happens when connection is closed by peer
    max_tries=7,  # NOTE: increased number of attempts because of random API unreliability and slowness
    jitter=None,
    giveup=giveup_cmr_requests
)
@backoff.on_exception(backoff.expo, aiohttp.ServerTimeoutError, max_tries=2)
async def fetch_post_url(session: aiohttp.ClientSession, url, data: str, headers):
    return await session.post(url, data=data, headers=headers, raise_for_status=True)


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError,),
    max_tries=7,  # NOTE: increased number of attempts because of random API unreliability and slowness
    jitter=None,
    giveup=giveup_cmr_requests
)
@backoff.on_exception(backoff.expo, requests.exceptions.Timeout, max_tries=2)
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
        if k == 'token' and v is None:
            continue

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
