import asyncio
import contextlib
import logging
import math
import os
from math import ceil
from typing import Optional

import aiohttp
import backoff

logger = logging.getLogger(__name__)


async def async_cmr_post(url, data: str, session: aiohttp.ClientSession, sem: Optional[asyncio.Semaphore]):
    sem = sem if sem is not None else contextlib.nullcontext()
    async with sem:
        page_size = 2000  # default is 10, max is 2000
        data += f"&page_size={page_size}"

        logger.debug(f"async_post_cmr({url=}..., {len(data)=:,}, {data[-250:]=}")
        max_pages = 1  # cap the number of pages (requests) to scroll through results.
        # after first response, update with the smaller of the forced max and the number of hits

        current_page = 1
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Client-Id': f'nasa.jpl.opera.sds.pcm.cmr_audit.{os.environ["USER"]}'
        }
        response_jsons = []
        while current_page <= max_pages:
            async with await fetch_post_url(session, url, data, headers) as response:
                response_json = await response.json()
                response_jsons.append(response_json)

            if current_page == 1:
                logger.info(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
                max_pages = math.ceil(response_json["hits"]/page_size)
                logger.info(f"Updating max pages to {max_pages=}")
            logger.debug(f'CMR number of granules (cmr-query-page {current_page} of {ceil(response_json["hits"]/page_size)}): {len(response_json["items"])=:,}')

            cmr_search_after = response.headers.get("CMR-Search-After")
            logger.debug(f"{cmr_search_after=}")
            if cmr_search_after:
                headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

            if len(response_json["items"]) < page_size:
                logger.debug("Reached end of CMR search results. Ending query.")
                break

            current_page += 1
            if not current_page <= max_pages:
                logger.warning(
                    "Reached max pages limit. "
                    "Not all search results exhausted. "
                    "Adjust limit or time ranges to process all hits, then re-run this script."
                )

        return response_jsons


def giveup_cmr_requests(e):
    if isinstance(e, aiohttp.ClientResponseError):
        if e.status == 413 and e.message == "Payload Too Large":  # give up. Fix bug
            return True
        if e.status == 400:  # Bad Requesst. give up. Fix bug
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
