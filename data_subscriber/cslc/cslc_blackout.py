import json
from copy import deepcopy
from collections import defaultdict
import dateutil
import logging
from functools import cache
import asyncio

from data_subscriber.url import cslc_unique_id
from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT, DateTimeRange
from data_subscriber.cslc_utils import localize_anc_json, sensing_time_day_index, parse_cslc_native_id, parse_cslc_file_name, download_batch_id_forward_reproc

DEFAULT_DISP_BLACKOUT_DATE_NAME = 'opera-disp-s1-blackout-dates.json'

logger = logging.getLogger(__name__)

@cache
def localize_disp_blackout_dates():
    try:
        file = localize_anc_json("DISP_S1_BLACKOUT_DATES_S3PATH")
    except:
        logger.warning(f"Could not download DISP-S1 blackout dates file from settings.yaml field DISP_S1_BLACKOUT_DATES_S3PATH from S3. Attempting to use local copy named {DEFAULT_DISP_BLACKOUT_DATE_NAME}.")
        file = DEFAULT_DISP_BLACKOUT_DATE_NAME

    return process_disp_blackout_dates(file)

@cache
def process_disp_blackout_dates(file):
    '''Process the disp blackout dates json file and return a dictionary'''

    j = json.load(open(file))

    '''Parse json file that looks like this
    "blackout_dates": {
       "831": []
        "832":  [ ["2024-12-30T23:05:24", "2025-03-15T23:05:24"], ...],
        ...
        "46543": [ ["2024-11-15T23:05:24", "2025-04-30T23:05:24"], ...]
        }
    }'''

    frame_blackout_dates = defaultdict(list)
    for frame in j["blackout_dates"]:
        for dates in j["blackout_dates"][frame]:
            frame_blackout_dates[int(frame)].append((dateutil.parser.isoparse(dates[0]), dateutil.parser.isoparse(dates[1])))

    return frame_blackout_dates

class DispS1BlackoutDates:

    def __init__(self, frame_blackout_dates, frame_to_burst, burst_to_frames):
        self.frame_to_burst = frame_to_burst
        self.burst_to_frames = burst_to_frames
        self.frame_blackout_acq_indices = defaultdict(list)

        # Populate for the beginning and end of the time range
        for frame_id, blackout_dates in frame_blackout_dates.items():
            for start_date, end_date in blackout_dates:
                acq_index_start = sensing_time_day_index(start_date, frame_id, self.frame_to_burst)
                acq_index_end = sensing_time_day_index(end_date, frame_id, self.frame_to_burst)
                self.frame_blackout_acq_indices[frame_id].append((acq_index_start, acq_index_end, start_date, end_date))

        # Now take out any black out dates from frame_to_burst data structures


    def is_in_blackout(self, frame_id, sensing_time):
        '''The sensing time of the frame is in blackout if any of its upto 27 bursts are in the blackout date range'''

        if frame_id not in self.frame_blackout_acq_indices:
            return False, None

        # If the sensing_time is within the blackout date acquisition date index range, it's blacked out
        acq_index = sensing_time_day_index(sensing_time, frame_id, self.frame_to_burst)
        for acq_index_start, acq_index_end, start_date, end_date in self.frame_blackout_acq_indices[frame_id]:
            if acq_index_start <= acq_index <= acq_index_end:
                return True, (start_date, end_date)

        return False, None

    def extend_additional_records(self, granules, proc_mode, no_duplicate=False, force_frame_id = None):
        """Add frame_id, burst_id, and acquisition_cycle to all granules.
        In forward  and re-processing modes, extend the granules with potentially additional records
        if a burst belongs to two frames."""

        extended_granules = []
        for granule in granules:
            granule_id = granule["granule_id"]

            burst_id, acquisition_dts, acquisition_cycles, frame_ids = (
                parse_cslc_native_id(granule_id, self.burst_to_frames, self.frame_to_burst))

            granule["acquisition_ts"] = acquisition_dts

            granule["burst_id"] = burst_id
            granule["frame_id"] = frame_ids[0] if force_frame_id is None else force_frame_id
            granule["acquisition_cycle"] = acquisition_cycles[granule["frame_id"]]
            granule["download_batch_id"] = download_batch_id_forward_reproc(granule)
            granule["unique_id"] = cslc_unique_id(granule["download_batch_id"], granule["burst_id"])

            if proc_mode not in ["forward"] or no_duplicate:
                continue

            # If this burst belongs to two frames, make a deep copy of the granule and append to the list
            if len(frame_ids) == 2:
                new_granule = deepcopy(granule)
                new_granule["frame_id"] = self.burst_to_frames[burst_id][1]
                granule["acquisition_cycle"] = acquisition_cycles[granule["frame_id"]]
                new_granule["download_batch_id"] = download_batch_id_forward_reproc(new_granule)
                new_granule["unique_id"] = cslc_unique_id(new_granule["download_batch_id"], new_granule["burst_id"])
                extended_granules.append(new_granule)

        granules.extend(extended_granules)

def _filter_cslc_blackout_polarization(granules, proc_mode, blackout_dates_obj, no_duplicate, force_frame_id, vv_only = True):
    '''Filter for CSLC granules and filter for blackout dates and polarization'''

    filtered_granules = []

    # Get rid of any bursts that aren't in the disp-s1 consistent database. Need to do this before the extending records
    relevant_granules = []
    for granule in granules:
        burst_id, acquisition_dts = parse_cslc_file_name(granule['granule_id'])
        if burst_id not in blackout_dates_obj.burst_to_frames.keys() or len(blackout_dates_obj.burst_to_frames[burst_id]) == 0:
            logger.info(f"Skipping granule {granule['granule_id']} because {burst_id=} not in the historical database")
        else:
            relevant_granules.append(granule)

    blackout_dates_obj.extend_additional_records(relevant_granules, proc_mode, no_duplicate, force_frame_id)

    for granule in relevant_granules:

        if vv_only and "_VV_" not in granule["granule_id"]:
            logger.info(f"Skipping granule {granule['granule_id']} because it doesn't have VV polarization")
            continue

        frame_id = granule["frame_id"]

        is_black_out, dates = blackout_dates_obj.is_in_blackout(frame_id, granule["acquisition_ts"])
        if is_black_out:
            blackout_start = dates[0].strftime(CMR_TIME_FORMAT)
            blackout_end = dates[1].strftime(CMR_TIME_FORMAT)
            logger.info(f"Skipping granule {granule['granule_id']} because {frame_id=} falls on a blackout date {blackout_start=} {blackout_end=}")
            continue

        filtered_granules.append(granule)

    return filtered_granules

def query_cmr_cslc_blackout_polarization(args, token, cmr, settings, query_timerange, now, silent, blackout_dates_obj,
                                         no_duplicate, force_frame_id, vv_only = True):
    '''Query CMR for CSLC granules and filter for blackout dates and polarization'''

    granules = asyncio.run(async_query_cmr(args, token, cmr, settings, query_timerange, now, silent))
    return _filter_cslc_blackout_polarization(granules, args.proc_mode, blackout_dates_obj, no_duplicate, force_frame_id, vv_only)