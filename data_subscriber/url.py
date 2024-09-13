import logging
import re
from pathlib import Path
from typing import Any
import dateutil
from datetime import datetime, timedelta

_EPOCH_S1A = "20140101T000000Z"

def form_batch_id(granule_id, revision_id):
    return granule_id+'-r'+str(revision_id)

def form_batch_id_cslc(granule_id, revision_id):
    return granule_id+'.h5-r'+str(revision_id)

def cslc_unique_id(download_batch_id, burst_id):
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
    result = _has_s3_url(dl_dict) or _has_https_url(dl_dict)

    if not result:
        logging.error(f"Couldn't find any URL in {dl_dict=}")

    return result


def _has_https_url(dl_dict: dict[str, Any]):
    result = dl_dict.get("https_url")

    if not result:
        logging.warning(f"Couldn't find any HTTPS URL in {dl_dict=}")

    return result


def _has_s3_url(dl_dict: dict[str, Any]):
    result = dl_dict.get("s3_url")

    if not result:
        logging.warning(f"Couldn't find any S3 URL in {dl_dict=}")

    return result


def _to_https_urls(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("https_url"):
        return dl_dict["https_url"]
    elif dl_dict.get("https_urls"):
        return dl_dict["https_urls"]
    else:
        raise Exception(f"Couldn't find any URLs in {dl_dict=}")

def determine_acquisition_cycle(burst_id, acquisition_dts, granule_id, epoch = None, cycle_days = 12):
    """RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
    The cycle restarts periodically with some miniscule drift over time and the life of the mission."""
    # RTC: Calculating the Collection Cycle Index (Part 1):
    #  required constants

    if epoch is not None:
        instrument_epoch = dateutil.parser.isoparse(epoch)  # We use whatever was passed in
    else:
        MISSION_EPOCH_S1A = dateutil.parser.isoparse(_EPOCH_S1A)  # set approximate mission start date
        MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
        instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B

    MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
    ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=cycle_days).total_seconds()

    # RTC: Calculating the Collection Cycle Index (Part 2):
    #  RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
    #  The cycle restarts periodically with some miniscule drift over time and the life of the mission.
    burst_identification_number = int(burst_id.split(sep="-")[1])
    seconds_after_mission_epoch = (dateutil.parser.isoparse(acquisition_dts) - instrument_epoch).total_seconds()
    acquisition_index = (
                                seconds_after_mission_epoch - (ACQUISITION_CYCLE_DURATION_SECS * (
                                    burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
                        ) / ACQUISITION_CYCLE_DURATION_SECS

    #print(acquisition_index)

    acquisition_cycle = round(acquisition_index)
    assert acquisition_cycle >= 0, f"Acquisition cycle is negative: {acquisition_cycle=}"
    return acquisition_cycle
