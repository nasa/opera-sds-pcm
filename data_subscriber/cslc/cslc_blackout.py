
import asyncio
import json
from collections import defaultdict
from copy import deepcopy
from functools import cache

import dateutil

from opera_commons.logger import get_logger
from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT
from data_subscriber.cslc_utils import (localize_anc_json,
                                        sensing_time_day_index,
                                        parse_cslc_native_id,
                                        parse_cslc_file_name,
                                        download_batch_id_forward_reproc)
from data_subscriber.url import cslc_unique_id

DEFAULT_DISP_BLACKOUT_DATE_NAME = 'opera-disp-s1-blackout-dates.json'


@cache
def localize_disp_blackout_dates():
    logger = get_logger()

    try:
        file = localize_anc_json("DISP_S1_BLACKOUT_DATES_S3PATH")
    except:
        logger.warning("Could not download DISP-S1 blackout dates file from settings.yaml "
                       "field DISP_S1_BLACKOUT_DATES_S3PATH from S3. "
                       "Attempting to use local copy named %s.", DEFAULT_DISP_BLACKOUT_DATE_NAME)
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

    def __init__(self, frame_blackout_dates, frame_to_burst, burst_to_frames, log_warnings=False):
        self.frame_to_burst = frame_to_burst
        self.burst_to_frames = burst_to_frames
        self.frame_blackout_acq_indices = defaultdict(list)
        
        logger = get_logger()

        # Populate for the beginning and end of the time range
        for frame_id, blackout_dates in frame_blackout_dates.items():
            # Validate frame exists in burst database
            if frame_id not in frame_to_burst:
                logger.error(
                    f"\n{'='*80}\n"
                    f"DATA CONSISTENCY ERROR: Frame {frame_id}\n"
                    f"{'='*80}\n"
                    f"Frame {frame_id} has blackout dates defined but does NOT exist in the burst database.\n"
                    f"\n"
                    f"Blackout dates defined: {len(blackout_dates)} period(s)\n"
                    f"Sensing times in burst DB: N/A (frame not found)\n"
                    f"\n"
                    f"POSSIBLE CAUSES:\n"
                    f"  1. Frame ID typo in blackout dates JSON\n"
                    f"  2. Frame removed from burst database but not from blackout dates\n"
                    f"  3. Burst database is incomplete\n"
                    f"\n"
                    f"RECOMMENDED ACTIONS:\n"
                    f"  1. Verify frame {frame_id} is a valid frame ID\n"
                    f"  2. Check if frame should be in burst database\n"
                    f"  3. Remove from blackout dates JSON if frame is invalid\n"
                    f"  4. Add to burst database if frame is valid but missing\n"
                    f"\n"
                    f"{'='*80}"
                )
                logger.error(f"Frame {frame_id} has blackout dates but is not in burst database. Skipping.")
                continue
            
            frame = frame_to_burst[frame_id]
            
            # Validate frame has sensing times
            if len(frame.sensing_datetimes) == 0:
                logger.error(
                    f"\n{'='*80}\n"
                    f"DATA CONSISTENCY ERROR: Frame {frame_id}\n"
                    f"{'='*80}\n"
                    f"Frame {frame_id} has blackout dates but NO sensing times in the burst database.\n"
                    f"\n"
                    f"Blackout dates defined: {len(blackout_dates)} period(s)\n"
                    f"Sensing times in burst DB: 0\n"
                    f"Bursts defined: {len(frame.burst_ids)}\n"
                    f"\n"
                    f"POSSIBLE CAUSES:\n"
                    f"  1. Frame configuration is incomplete\n"
                    f"  2. Sensing times not populated in burst database\n"
                    f"  3. Frame is a placeholder/test frame\n"
                    f"\n"
                    f"RECOMMENDED ACTIONS:\n"
                    f"  1. Check if frame {frame_id} should have sensing time data\n"
                    f"  2. Populate sensing_time_list in burst database if missing\n"
                    f"  3. Remove from blackout dates if frame is inactive\n"
                    f"\n"
                    f"{'='*80}"
                )
                logger.error(f"Frame {frame_id} has blackout dates but no sensing times. Skipping.")
                continue
            
            # Validate sufficient sensing times for blackout date range
            first_sensing = frame.sensing_datetimes[0]
            last_sensing = frame.sensing_datetimes[-1]
            sensing_span_days = (last_sensing - first_sensing).days if len(frame.sensing_datetimes) > 1 else 0
            
            # Calculate blackout date range
            all_starts = [start for start, end in blackout_dates]
            all_ends = [end for start, end in blackout_dates]
            earliest_blackout = min(all_starts)
            latest_blackout = max(all_ends)
            blackout_span_days = (latest_blackout - earliest_blackout).days
            
            # Check if frame has suspiciously few sensing times for its blackout range
            if len(frame.sensing_datetimes) < 10 and blackout_span_days > 365:
                if log_warnings:
                    logger.warning(
                        f"\n{'='*80}\n"
                        f"DATA QUALITY WARNING: Frame {frame_id}\n"
                        f"{'='*80}\n"
                        f"Frame {frame_id} has a large blackout date range but very few sensing times.\n"
                        f"This may indicate incomplete data.\n"
                        f"\n"
                        f"Frame Statistics:\n"
                        f"  - Number of sensing times: {len(frame.sensing_datetimes)}\n"
                        f"  - First sensing time: {first_sensing.isoformat()}\n"
                        f"  - Last sensing time: {last_sensing.isoformat()}\n"
                        f"  - Sensing time span: {sensing_span_days} days\n"
                        f"  - Number of bursts: {len(frame.burst_ids)}\n"
                        f"\n"
                        f"Blackout Date Range:\n"
                        f"  - Number of blackout periods: {len(blackout_dates)}\n"
                        f"  - Earliest blackout: {earliest_blackout.isoformat()}\n"
                        f"  - Latest blackout: {latest_blackout.isoformat()}\n"
                        f"  - Blackout span: {blackout_span_days} days ({blackout_span_days/365:.1f} years)\n"
                        f"\n"
                        f"EXPECTED BEHAVIOR:\n"
                        f"  For a {blackout_span_days} day span with Sentinel-1's 6-day repeat cycle,\n"
                        f"  we'd expect approximately {blackout_span_days // 6} sensing times.\n"
                        f"  Found: {len(frame.sensing_datetimes)} (only {len(frame.sensing_datetimes) / (blackout_span_days // 6 + 1) * 100:.1f}% of expected)\n"
                        f"\n"
                        f"POSSIBLE CAUSES:\n"
                        f"  1. Burst database is incomplete for this frame\n"
                        f"  2. Frame has limited historical data\n"
                        f"  3. Blackout dates cover too wide a range\n"
                        f"\n"
                        f"RECOMMENDED ACTIONS:\n"
                        f"  1. Verify burst database has complete sensing_time_list for frame {frame_id}\n"
                        f"  2. If frame has limited data, reduce blackout date range accordingly\n"
                        f"  3. If frame is test/placeholder, remove from blackout dates\n"
                        f"\n"
                        f"Processing will continue, but blackout filtering may not work as expected.\n"
                        f"{'='*80}"
                    )
                continue
            
            for start_date, end_date in blackout_dates:
                acq_index_start = sensing_time_day_index(start_date, frame_id, self.frame_to_burst)
                acq_index_end = sensing_time_day_index(end_date, frame_id, self.frame_to_burst)
                self.frame_blackout_acq_indices[frame_id].append((acq_index_start, acq_index_end, start_date, end_date))

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
    logger = get_logger()
    filtered_granules = []

    # Get rid of any bursts that aren't in the disp-s1 consistent database. Need to do this before the extending records
    relevant_granules = []
    for granule in granules:
        burst_id, acquisition_dts = parse_cslc_file_name(granule['granule_id'])
        if burst_id not in blackout_dates_obj.burst_to_frames.keys() or len(blackout_dates_obj.burst_to_frames[burst_id]) == 0:
            logger.info("Skipping granule %s because burst_id=%s not in the historical database",
                        granule['granule_id'], burst_id)
        else:
            relevant_granules.append(granule)

    blackout_dates_obj.extend_additional_records(relevant_granules, proc_mode, no_duplicate, force_frame_id)

    for granule in relevant_granules:

        if vv_only and "_VV_" not in granule["granule_id"]:
            logger.info(f"Skipping granule %s because it doesn't have VV polarization", granule['granule_id'])
            continue

        frame_id = granule["frame_id"]

        is_black_out, dates = blackout_dates_obj.is_in_blackout(frame_id, granule["acquisition_ts"])
        if is_black_out:
            blackout_start = dates[0].strftime(CMR_TIME_FORMAT)
            blackout_end = dates[1].strftime(CMR_TIME_FORMAT)
            logger.info(f"Skipping granule %s because frame_id=%s falls on a blackout date blackout_start=%s blackout_end=%s",
                        granule['granule_id'], frame_id, blackout_start, blackout_end)
            continue

        filtered_granules.append(granule)

    return filtered_granules

def query_cmr_cslc_blackout_polarization(args, token, cmr, settings, query_timerange, now, verbose, blackout_dates_obj,
                                         no_duplicate, force_frame_id, vv_only = True):
    '''Query CMR for CSLC granules and filter for blackout dates and polarization'''

    granules = asyncio.run(async_query_cmr(args, token, cmr, settings, query_timerange, now, verbose))
    return _filter_cslc_blackout_polarization(granules, args.proc_mode, blackout_dates_obj, no_duplicate, force_frame_id, vv_only)
