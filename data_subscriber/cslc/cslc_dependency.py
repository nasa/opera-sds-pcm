from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timedelta
import dateutil
import logging

from data_subscriber.cmr import CMR_TIME_FORMAT, DateTimeRange
from data_subscriber.cslc_utils import parse_cslc_file_name, determine_acquisition_cycle_cslc, build_cslc_native_ids, build_ccslc_m_index, _C_CSLC_ES_INDEX_PATTERNS
from data_subscriber.cslc.cslc_blackout import query_cmr_cslc_blackout_polarization

logger = logging.getLogger(__name__)

class CSLCDependency:
    def __init__(self, k: int, m: int, frame_to_bursts, args, token, cmr, settings, blackout_dates_obj, VV_only = True):
        self.k = k
        self.m = m
        self.frame_to_bursts = frame_to_bursts
        self.args = args
        self.token = token
        self.cmr = cmr
        self.settings = settings
        self.blackout_dates_obj = blackout_dates_obj
        self.VV_only = VV_only

    def get_prev_day_indices(self, day_index: int, frame_number: int):
        '''Return the day indices of the previous acquisitions for the frame_number given the current day index'''

        if frame_number not in self.frame_to_bursts:
            raise Exception(f"Frame number {frame_number} not found in the historical database. \
    OPERA does not process this frame for DISP-S1.")

        frame = self.frame_to_bursts[frame_number]

        if day_index <= frame.sensing_datetime_days_index[-1]:
            # If the day index is within the historical database, simply return from the database
            # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
            list_index = frame.sensing_datetime_days_index.index(day_index)
            return frame.sensing_datetime_days_index[:list_index]
        else:
            # If not, we must query CMR and then append that to the database values
            start_date = frame.sensing_datetimes[-1] + timedelta(minutes=30)
            days_delta = day_index - frame.sensing_datetime_days_index[-1]
            end_date = start_date + timedelta(days=days_delta - 1) # We don't want the current day index in this
            query_timerange = DateTimeRange(start_date.strftime(CMR_TIME_FORMAT), end_date.strftime(CMR_TIME_FORMAT))
            acq_index_to_bursts, _ = self.get_k_granules_from_cmr(query_timerange, frame_number, silent = True)
            all_prev_indices = frame.sensing_datetime_days_index + sorted(list(acq_index_to_bursts.keys()))
            logger.debug(f"All previous day indices: {all_prev_indices}")
            return all_prev_indices
    def get_k_granules_from_cmr(self, query_timerange, frame_number: int, silent = False):
        '''Return two dictionaries that satisfy the burst pattern for the frame_number within the time range:
        1. acq_index_to_bursts: day index to set of burst ids
        2. acq_index_to_granules: day index to list of granules that match the burst
        '''

        # Add native-id condition in args. This query is always by temporal time.
        l, native_id = build_cslc_native_ids(frame_number, self.frame_to_bursts)
        args = deepcopy(self.args)
        args.native_id = native_id
        args.use_temporal = True

        granules = query_cmr_cslc_blackout_polarization(
            args, self.token, self.cmr, self.settings, query_timerange, datetime.utcnow(), silent, self.blackout_dates_obj, True, frame_number, self.VV_only)

        return self.k_granules_grouping(frame_number, granules)

    def k_granules_grouping(self, frame_number, granules: list):

        acq_index_to_bursts = defaultdict(set)
        acq_index_to_granules = defaultdict(list)
        frame = self.frame_to_bursts[frame_number]

        # Often we get duplicate CSLC granules which have the same burst id and acquisition date. In such case, use the latest production one
        latest_burstid_acqdate = {}
        for granule in granules:
            burstid_acqdate = granule["granule_id"].split("Z")[0]
            if burstid_acqdate in latest_burstid_acqdate:
                if granule["granule_id"] > latest_burstid_acqdate[burstid_acqdate]["granule_id"]:
                    latest_burstid_acqdate[burstid_acqdate] = granule
            else:
                latest_burstid_acqdate[burstid_acqdate] = granule

        unique_granules = latest_burstid_acqdate.values()

        for granule in unique_granules:
            burst_id, acq_dts = parse_cslc_file_name(granule["granule_id"])
            acq_time = dateutil.parser.isoparse(acq_dts[:-1])  # convert to datetime object
            g_day_index = determine_acquisition_cycle_cslc(acq_time, frame_number, self.frame_to_bursts)
            acq_index_to_bursts[g_day_index].add(burst_id)
            acq_index_to_granules[g_day_index].append(granule)

        # Get rid of the day indices that don't match the burst pattern
        for g_day_index in list(acq_index_to_bursts.keys()):
            if not acq_index_to_bursts[g_day_index].issuperset(frame.burst_ids):
                logger.info(
                    f"Removing day index {g_day_index} from k-cycle determination because it doesn't suffice the burst pattern")
                logger.info(f"{acq_index_to_bursts[g_day_index]}")
                del acq_index_to_bursts[g_day_index]
                del acq_index_to_granules[g_day_index]

        return acq_index_to_bursts, acq_index_to_granules

    def determine_k_cycle(self, acquisition_dts: datetime, day_index: int, frame_number: int, silent = False):
        '''Return where in the k-cycle this acquisition falls for the frame_number
        Must specify either acquisition_dts or day_index.
        Returns integer between 0 and k-1 where 0 means that it's at the start of the cycle

        Assumption: This current frame satisfies the burst pattern already; we don't need to check for that here'''

        if day_index is None:
            day_index = determine_acquisition_cycle_cslc(acquisition_dts, frame_number, self.frame_to_bursts)

        # If the day index is within the historical database it's much simpler
        # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
        # Clearly if we somehow end up with like 1000
        try:
            # array.index returns 0-based index so add 1
            frame = self.frame_to_bursts[frame_number]
            index_number = frame.sensing_datetime_days_index.index(day_index) + 1 # note "index" is overloaded term here
            return index_number % self.k
        except ValueError:
            # If not, we have to query CMR for all records after the historical database, filter out ones that don't match the burst pattern,
            # and then determine the k-cycle index
            start_date = frame.sensing_datetimes[-1] + timedelta(minutes=30) # Make sure we are not counting this last sensing time cycle

            if acquisition_dts is None:
                days_delta = day_index - frame.sensing_datetime_days_index[-1]
                end_date = start_date + timedelta(days=days_delta)
            else:
                end_date = acquisition_dts

            query_timerange = DateTimeRange(start_date.strftime(CMR_TIME_FORMAT), end_date.strftime(CMR_TIME_FORMAT))
            acq_index_to_bursts, _ = self.get_k_granules_from_cmr(query_timerange, frame_number, silent)

            # The k-index is then the complete index number (historical + post historical) mod k
            logger.info(f"{len(acq_index_to_bursts.keys())} day indices since historical that match the burst pattern: {acq_index_to_bursts.keys()}")
            logger.info(f"{len(frame.sensing_datetime_days_index)} day indices already in historical database.")
            index_number = len(frame.sensing_datetime_days_index) + len(acq_index_to_bursts.keys()) + 1
            return index_number % self.k

    def compressed_cslc_satisfied(self, frame_id, day_index, eu):

        if self.get_dependent_compressed_cslcs(frame_id, day_index, eu) == False:
            return False
        return True

    def get_dependent_compressed_cslcs(self, frame_id, day_index, eu):
        ''' Search for all previous M compressed CSLCs
            prev_day_indices: The acquisition cycle indices of all collects that show up in disp_burst_map previous of
                                the latest acq cycle index
        '''

        prev_day_indices = self.get_prev_day_indices(day_index, frame_id)

        ccslcs = []

        #special case for early sensing time series
        m = self.m
        if len(prev_day_indices) < self.k * (self.m-1):
            m = (len(prev_day_indices) // self.k ) + 1

        # Uses ccslc_m_index field which looks like T100-213459-IW3_417 (burst_id_acquisition-cycle-index)
        for mm in range(0, m - 1):  # m parameter is inclusive of the current frame at hand
            for burst_id in self.frame_to_bursts[frame_id].burst_ids:
                ccslc_m_index = get_dependent_ccslc_index(prev_day_indices, mm, self.k, burst_id)
                ccslc = eu.query(
                    index=_C_CSLC_ES_INDEX_PATTERNS,
                    body={"query": {"bool": {"must": [
                        {"term": {"metadata.ccslc_m_index.keyword": ccslc_m_index}},
                        {"term": {"metadata.frame_id": frame_id}}
                    ]}}})

                if len(ccslc) == 0:
                    logger.info("Compressed CSLCs for ccslc_m_index: %s was not found in GRQ ES", ccslc_m_index)
                    return False

                ccslcs.append(ccslc[0]) # There should only be one

        logger.info("All Compresseed CSLSs for frame %s at day index %s found in GRQ ES", frame_id, day_index)
        logger.info(ccslcs)
        return ccslcs
def get_dependent_ccslc_index(prev_day_indices, mm, k, burst_id):
    '''last_m_index: The index of the last M compressed CSLC, index into prev_day_indices
       acq_cycle_index: The index of the acq cycle, index into disp_burst_map'''
    num_prev_indices = len(prev_day_indices)
    last_m_index = num_prev_indices // k
    last_m_index *= k

    acq_cycle_index = prev_day_indices[last_m_index - 1 - (mm * k)]  # jump by k
    ccslc_m_index = build_ccslc_m_index(burst_id, acq_cycle_index)  # looks like t034_071112_iw3_461

    return ccslc_m_index