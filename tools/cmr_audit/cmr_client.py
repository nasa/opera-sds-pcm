import logging
import os
from math import ceil

import aiohttp
import backoff

logger = logging.getLogger(__name__)


async def async_cmr_post(url, data: str, session: aiohttp.ClientSession):
    page_size = 2000  # default is 10, max is 2000
    data += f"&page_size={page_size}"

    logger.debug(f"async_post_cmr({url=}..., {len(data)=:,}, {data[-250:]=}")

    cmr_granules = set()
    cmr_granules_detailed = {}
    # page_num, offset (0-based), page_size, sort_key
    # You can not page past the 1 millionth item.
    # Additionally granule queries which do not target a set of collections are limited to paging up to the 10000th item.
    max_pages = int(100_000/2000)  # cap the number of pages (requests) to scroll through results. CMR's hard limit is page_size * max_pages <= 100,000

    current_page = 1
    cmr_search_after = ""
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Client-Id': f'nasa.jpl.opera.sds.pcm.cmr_audit.{os.environ["USER"]}'
    }
    while current_page <= max_pages:
        async with await fetch_post_url(session, url, data, headers) as response:
            response_json = await response.json()

        if current_page == 1:
            logger.info(f'CMR number of granules (cmr-query): {response_json["hits"]=:,}')
        logger.debug(f'CMR number of granules (cmr-query-page {current_page} of {ceil(response_json["hits"]/page_size)}): {len(response_json["items"])=:,}')
        cmr_granules.update({item["meta"]["native-id"] for item in response_json["items"]})
        cmr_granules_detailed.update({item["meta"]["native-id"]: item for item in response_json["items"]})  # DEV: uncomment as needed

        cmr_search_after = response.headers.get("CMR-Search-After")
        logger.debug(f"{cmr_search_after=}")
        if cmr_search_after:
            headers.update({"CMR-Search-After": response.headers["CMR-Search-After"]})

        if len(response_json["items"]) < page_size:
            logger.debug("Reached end of CMR search results. Ending query.")
            break

        current_page += 1
        if not current_page <= max_pages:
            logger.warning("Reached max pages limit. Not all search results exhausted. Adjust limit or time ranges to process all hits, then re-run this script.")

    return cmr_granules, cmr_granules_detailed


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
    max_tries=3,
    jitter=None,
    giveup=giveup_cmr_requests
)
async def fetch_post_url(session: aiohttp.ClientSession, url, data: str, headers):
    return await session.post(url, data=data, headers=headers, raise_for_status=True)
