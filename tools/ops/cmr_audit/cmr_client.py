
import asyncio
import contextlib
import itertools
import json
import logging
import math
import os
from typing import Optional, Iterable

import aiohttp
import backoff
import requests
from requests.exceptions import HTTPError

# This module's own logger: it propagates to whatever the calling program configured. (The shared
# opera_commons get_logger() reconfigures the root logger with force=True on first use, which closes the
# file handlers the audits attach, so it must not be called from here.)
logger = logging.getLogger(__name__)


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
    answer, whatever its item count; and a page with no hit count to check against while CMR says
    there is more (CMR-Search-After) cannot be verified either.
    """
    if str(headers.get("CMR-Timed-Out", "")).lower() == "true":
        return "CMR-Timed-Out"
    if hit_count(response_json, headers) is None and headers.get("CMR-Search-After"):
        return "no hit count but more pages available"
    return None


def hit_count(response_json: dict, headers) -> Optional[int]:
    """CMR's count of the whole answer: ``hits`` in a umm_json page, the CMR-Hits header for the legacy json feed."""
    hits = response_json.get("hits")
    if hits is None and headers.get("CMR-Hits") is not None:
        try:
            hits = int(headers["CMR-Hits"])
        except (TypeError, ValueError):
            hits = None
    return hits


def page_items(response_json: dict) -> list:
    """The granules of one page: umm_json ``items``, or ``feed.entry`` of the legacy json rendering."""
    if "items" in response_json:
        return response_json["items"]
    return response_json.get("feed", {}).get("entry", [])


def concept_id(item: dict) -> Optional[str]:
    """A granule's concept id from either rendering (umm_json ``meta.concept-id``, legacy json ``id``)."""
    return item.get("meta", {}).get("concept-id") if "meta" in item else item.get("id")


def native_id(item: dict) -> Optional[str]:
    """A granule's native id from either rendering (umm_json ``meta.native-id``, legacy json ``title``)."""
    return item.get("meta", {}).get("native-id") if "meta" in item else item.get("title")


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
    incomplete result is re-queried from the first page (max_attempts in total). When the last attempt is
    still short of ``hits`` although every page came back clean, the granules CMR left out of the umm_json
    rendering are recovered one by one (see _recover_dropped_granules); only when that fails too is
    IncompleteCmrResultsError raised, so a caller never mistakes a partial listing for the whole one.
    Without verify_complete (the default) a shortfall is only logged.
    """
    sem = sem if sem is not None else contextlib.nullcontext()

    page_size = 2000  # default is 10, max is 2000
    data += f"&page_size={page_size}"

    logger.debug(f"async_cmr_post({url=}..., {len(data)=:,}, {data[-250:]=}")

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Client-Id': f'nasa.jpl.opera.sds.pcm.data_subscriber.{os.environ["USER"]}'
    }

    # output_path only ever keeps the rows of a call that succeeded: every other exit (retry, final
    # incomplete, exception while paging) rewinds to where this call started writing.
    output_offset = os.path.getsize(output_path) if output_path and os.path.exists(output_path) else 0

    def rewind():
        if output_path:
            with open(output_path, 'r+b') as f:
                f.truncate(output_offset)

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            rewind()

        # The semaphore slot is held only while talking to CMR, not during the pause between attempts.
        async with sem:
            try:
                response_jsons, hits, items_seen, problem, pages, concept_ids = await _fetch_all_pages(
                    session, url, data, dict(headers), page_size, output_path)
            except BaseException:
                rewind()
                raise

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

    # The last attempt is still short. When every page came back clean and only the count disagrees,
    # CMR has left records out of the umm_json rendering; those can be fetched one by one.
    recovered = None
    if problem is None and url.endswith(".umm_json"):
        async with sem:
            try:
                recovered = await _recover_dropped_granules(session, url, data, dict(headers), page_size,
                                                            set(concept_ids))
            except BaseException:
                rewind()
                raise
    if recovered is not None and hits is not None and items_seen + len(recovered) >= hits:
        if output_path:
            with open(output_path, 'a') as f:
                for item in recovered:
                    f.write(json.dumps(item) + '\n')
        else:
            response_jsons.append({"hits": hits, "items": recovered})
        logger.warning("Recovered %d granule(s) CMR left out of its umm_json listing (%s) for %s: %s",
                       len(recovered), why, data[-250:], ", ".join(str(native_id(item)) for item in recovered))
        return response_jsons

    rewind()
    raise IncompleteCmrResultsError(hits, items_seen, why == "CMR-Timed-Out", pages, data[-250:], reason=why)


async def _fetch_all_pages(session, url, data, headers, page_size, output_path):
    """Pages through one CMR query. Returns (response_jsons, hits, items_seen, problem, pages, concept_ids).

    Reads both renderings: umm_json pages (``hits`` and ``items``) and the legacy json feed (``feed.entry``,
    the count only in the CMR-Hits header). ``concept_ids`` are the concept ids of every item, in order.
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
    concept_ids = []

    logger.info("Issuing request. This may take a while depending on search page size and number of pages/results.")

    response_jsons = []
    while current_page <= max_pages:
        async with await fetch_post_url(session, url, data, headers) as response:
            response_json = await response.json()
        pages_fetched += 1
        items = page_items(response_json)

        if output_path:
            with open(output_path, 'a') as f:
                for item in items:
                    f.write(json.dumps(item) + '\n')
        else:
            response_jsons.append(response_json)

        items_seen += len(items)
        concept_ids.extend(concept_id(item) for item in items)
        problem = problem or page_incomplete_reason(response_json, response.headers)

        if current_page == 1:
            hits = hit_count(response_json, response.headers)
            logger.debug(f'CMR number of granules (cmr-query): {hits=}')
            max_pages = math.ceil((hits or 0) / page_size)
            logger.debug("Updating max pages to %d", max_pages)

        logger.debug(f'CMR query (cmr-query-page {current_page} of {max_pages}): '
                     f'{len(items)=:,}')

        cmr_search_after = response.headers.get("CMR-Search-After")
        logger.debug(f"{cmr_search_after=}")

        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})
        else:
            logger.info("Reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if current_page > max_pages and (hits is None or items_seen < hits):
            # CMR sends CMR-Search-After on the last page as well, so running out of pages only
            # matters when the items collected so far fall short of the count.
            logger.warning(
                "Reached max pages limit (%d) with %s of %s items. Not all search results exhausted. "
                "Adjust limit or time ranges to process all hits, then re-run this script.",
                max_pages, f"{items_seen:,}", hits
            )

    return response_jsons, hits, items_seen, problem, pages_fetched, concept_ids


async def _recover_dropped_granules(session, url, data, headers, page_size, seen_concept_ids):
    """The granules a short umm_json listing left out, or None when they cannot be recovered.

    CMR's umm_json rendering of some ASF collections leaves records out of a page while counting them
    in CMR-Hits (seen on the Sentinel-1 burst collection and, for 2026-08-10, on SENTINEL-1C_SLC: 164
    counted, 156 rendered, the same on every attempt); the same records render fine when asked for by
    id. The legacy json rendering of the same query still lists every record, so it supplies the
    concept ids the umm_json pages lack, and those are fetched from umm_json by concept id, 100 per
    request. None when the legacy listing is short of the count too or a fetch by id comes back short;
    the caller then raises as it would without this step.
    """
    legacy_url = url[:-len("umm_json")] + "json"
    _, hits, entries_seen, problem, _, legacy_ids = await _fetch_all_pages(
        session, legacy_url, data, dict(headers), page_size, None)
    why = problem or shortfall_reason(hits, entries_seen)
    if why is not None:
        logger.warning("The legacy json rendering is incomplete too (%s); cannot recover the listing for %s",
                       why, data[-250:])
        return None

    missing = [cid for cid in legacy_ids if cid not in seen_concept_ids]
    if not missing:
        return None

    recovered = []
    for start in range(0, len(missing), 100):
        batch = missing[start:start + 100]
        body = "".join(f"&concept_id[]={cid}" for cid in batch) + f"&page_size={len(batch)}"
        async with await fetch_post_url(session, url, body, dict(headers)) as response:
            response_json = await response.json()
        items = page_items(response_json)
        if len(items) < len(batch):
            logger.warning("CMR returned %d of %d granules asked for by concept id; cannot recover the listing for %s",
                           len(items), len(batch), data[-250:])
            return None
        recovered.extend(items)
    return recovered


def is_transient_cmr_error(exc: BaseException) -> bool:
    """A request failure worth retrying or skipping over: CMR/network trouble, not a bad request.

    The same split giveup_cmr_requests makes for the backoff decorator: 4xx (bad bbox, payload too
    large) is permanent and must surface; 429/5xx, dropped connections and timeouts are transient.
    """
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 429 or exc.status >= 500
    # ClientConnectionError covers ClientOSError, ServerDisconnectedError and ServerTimeoutError;
    # ClientPayloadError is a response cut off mid-body.
    return isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError))


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
