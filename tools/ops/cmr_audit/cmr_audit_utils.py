import asyncio
import datetime
import logging
import urllib
from io import StringIO
from pprint import pprint
from typing import Union, Iterable, Optional

import aiohttp
import dateutil.parser
import more_itertools
from dateutil.rrule import rrule, HOURLY, DAILY
from more_itertools import always_iterable

from tools.ops.cmr_audit.cmr_client import async_cmr_post

logger = logging.getLogger(__name__)


async def async_get_cmr_granules(collection_short_name, temporal_date_start: str, temporal_date_end: str,
                                 platform_short_name: Union[str, Iterable[str]], concurrency=None):
    logger.debug(f"entry({collection_short_name=}, {temporal_date_start=}, {temporal_date_end=}, {platform_short_name})")

    sem = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

        # cycle through subset timeranges to split up requests
        post_cmr_tasks = []

        temporal_start_dt = dateutil.parser.isoparse(temporal_date_start)
        temporal_end_dt = dateutil.parser.isoparse(temporal_date_end)
        range_days = rrule(freq=DAILY, dtstart=temporal_start_dt, interval=1, until=temporal_end_dt)
        for day in range_days:
            logger.debug(f"{day=!s}")
            if day >= temporal_end_dt:
                logger.debug(f"Current day is beyond the global end datetime. Skipping")
                break

            # NOTE: keep freq+interval and duration in sync
            range_hours = rrule(freq=HOURLY, dtstart=day, interval=12, until=day + datetime.timedelta(days=1))
            duration = datetime.timedelta(hours=12)
            for hour in range_hours:
                logger.debug(f"{hour=!s}")

                if hour == temporal_end_dt:  # reached global end datetime
                    logger.debug("EDGECASE: hour matches global end datetime. Skipping")
                    break
                if hour >= day + datetime.timedelta(days=1):  # current hour goes into the next day. skip and let next day iteration handle.
                    logger.debug("EDGECASE: hour goes into next day (i.e. next outer loop iteration). Skipping")
                    break

                local_start_dt_str = hour.isoformat(timespec="milliseconds")
                if temporal_end_dt < hour + duration:  # if on last partial hour, use global end datetime
                    logger.debug("Clamping local end datetime for trailing partial duration")
                    local_end_dt = temporal_end_dt
                else:
                    local_end_dt = hour + duration
                local_end_dt_str = local_end_dt.isoformat(timespec="milliseconds")

                request_body = request_body_supplier(collection_short_name, temporal_date_start=local_start_dt_str, temporal_date_end=local_end_dt_str, platform_short_name=platform_short_name)
                logger.debug(f"Creating request task for {local_start_dt_str=}, {local_end_dt_str=}")
                post_cmr_tasks.append(get_cmr_audit_granules(request_url, request_body, session, sem))

                if local_end_dt == temporal_end_dt:  # processed last partial hour. prevent further iterations.
                    logger.debug("EDGECASE: processed last partial hour. Preempting")
                    break

        logger.debug(f"Number of query requests to make: {len(post_cmr_tasks)=}")

        logger.debug("Batching tasks")
        cmr_granules = set()
        cmr_granules_details = {}
        task_chunks = list(more_itertools.chunked(post_cmr_tasks, concurrency or len(post_cmr_tasks)))  # CMR recommends 2-5 threads.
        for i, task_chunk in enumerate(task_chunks, start=1):
            logger.debug(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = more_itertools.partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False))
            for post_cmr_tasks_result in post_cmr_tasks_results:
                cmr_granules.update(post_cmr_tasks_result[0])
            # DEV: uncomment as needed
                cmr_granules_details.update(post_cmr_tasks_result[1])

        logger.info(f"{collection_short_name} {len(cmr_granules)=:,}")
        return cmr_granules, cmr_granules_details


def request_body_supplier(collection_short_name, temporal_date_start: str, temporal_date_end: str, platform_short_name: Union[str, Iterable[str]]):
    if collection_short_name == "HLSL30" or collection_short_name == "HLSS30":
        return (
            "provider=LPCLOUD"
            f"&short_name[]={collection_short_name}"
            "&bounding_box=-180,-60,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            "&options[platform][exclude_collection]=true"
            f'{"&platform[]=" + "&platform[]=".join(always_iterable(platform_short_name))}'
            ""
        )
    if collection_short_name == "SENTINEL-1A_SLC" or collection_short_name == "SENTINEL-1B_SLC":
        return (
            "provider=ASF"
            f"&short_name[]={collection_short_name}"
            "&bounding_box=-180,-60,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            f'{"&platform[]=" + "&platform[]=".join(always_iterable(platform_short_name))}'
            "&attribute[]=string,BEAM_MODE,IW"
        )
    if collection_short_name == "OPERA_L2_RTC-S1_V1":
        return (
            "provider=ASF"
            f"&short_name[]={collection_short_name}"
            "&bounding_box=-180,-90,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
        )
    if collection_short_name == "OPERA_L3_DSWX-S1_V1":
        return (
            "provider=POCLOUD"
            f"&short_name[]={collection_short_name}"
            "&bounding_box=-180,-90,180,90"
            "&sort_key=-start_date"
            # f"&revision_date[]={revision_date_start},{revision_date_end}"  # DEV: left for documentation purposes
            f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
        )
    raise Exception(f"Unsupported collection short name. {collection_short_name=}")


async def get_cmr_audit_granules(url, data: str, session: aiohttp.ClientSession, sem: Optional[asyncio.Semaphore]):
    response_jsons = await async_cmr_post(url, data, session, sem)
    cmr_granules, cmr_granules_detailed = to_cmr_audit_granules(response_jsons)
    return cmr_granules, cmr_granules_detailed


def to_cmr_audit_granules(cmr_response_jsons):
    cmr_granules = set()
    cmr_granules_detailed = {}
    for response_json in cmr_response_jsons:
        cmr_granules.update({item["meta"]["native-id"] for item in response_json["items"]})
        cmr_granules_detailed.update({item["meta"]["native-id"]: item for item in response_json["items"]})  # DEV: uncomment as needed
    return cmr_granules, cmr_granules_detailed


def pstr(o):
    sio = StringIO()
    pprint(o, stream=sio)
    sio.seek(0)
    return sio.read()

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise Exception('Boolean value expected.')
