import re
from pathlib import Path
from typing import Any
from deprecated import deprecated

import rtc_utils
from commons.logger import get_logger

_EPOCH_S1A = rtc_utils._EPOCH_S1A

def form_batch_id(granule_id, revision_id):
    return granule_id+'-r'+str(revision_id)

def form_batch_id_cslc(granule_id, revision_id):
    return granule_id+'.h5-r'+str(revision_id)

def cslc_unique_id(download_batch_id, burst_id):
    return download_batch_id+'_'+burst_id

def rtc_for_dist_unique_id(download_batch_id, burst_id):
    return download_batch_id+'_'+burst_id

def _to_batch_id(dl_doc: dict[str, Any]):
    return form_batch_id(dl_doc['granule_id'], dl_doc['revision_id'])

def _to_orbit_number(dl_doc: dict[str, Any]):
    url = _to_urls(dl_doc)
    return _slc_url_to_chunk_id(url, dl_doc['revision_id'])


def _slc_url_to_chunk_id(url, revision_id):
    input_filename = Path(url).name
    input_filename = input_filename[:-4]+'.zip'
    return form_batch_id(input_filename, revision_id)


def _rtc_url_to_chunk_id(url, revision_id):
    input_filename = Path(url).name
    return form_batch_id(input_filename, revision_id)


def _to_urls(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    elif dl_dict.get("s3_urls"):
        return dl_dict["s3_urls"]
    else:
        raise Exception(f"Couldn't find any S3 paths in {dl_dict=}")

def _url_to_tile_id(url: str):
    tile_re = r"T\w{5}"

    input_filename = Path(url).name
    tile_id: str = re.findall(tile_re, input_filename)[0]
    return tile_id


def _to_tile_id(dl_doc: dict[str, Any]):
    return _url_to_tile_id(_to_urls(dl_doc))


def _has_url(dl_dict: dict[str, Any]):
    logger = get_logger()
    result = _has_s3_url(dl_dict) or _has_https_url(dl_dict)

    if not result:
        logger.error(f"Couldn't find any URL in {dl_dict=}")

    return result


def _has_https_url(dl_dict: dict[str, Any]):
    logger = get_logger()
    result = dl_dict.get("https_url")

    if not result:
        logger.warning(f"Couldn't find any HTTPS URL in {dl_dict=}")

    return result


def _has_s3_url(dl_dict: dict[str, Any]):
    logger = get_logger()
    result = dl_dict.get("s3_url")

    if not result:
        logger.warning(f"Couldn't find any S3 URL in {dl_dict=}")

    return result


def _to_https_urls(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    elif dl_dict.get("https_urls"):
        return dl_dict["https_urls"]
    else:
        raise Exception(f"Couldn't find any URLs in {dl_dict=}")

@deprecated("This function has been deprecated. Use rtc_utils.determine_acquisition_cycle instead.")
def determine_acquisition_cycle(burst_id, acquisition_dts, granule_id):
    return rtc_utils.determine_acquisition_cycle(burst_id, acquisition_dts, granule_id)
