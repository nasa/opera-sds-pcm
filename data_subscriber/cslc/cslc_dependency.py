from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import dateutil

from opera_commons.logger import get_logger
from data_subscriber.cmr import CMR_TIME_FORMAT, DateTimeRange
from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.cslc_blackout import query_cmr_cslc_blackout_polarization
from data_subscriber.cslc.disp_s1_phases import lineage_start_pos
from data_subscriber.cslc_utils import parse_cslc_file_name, determine_acquisition_cycle_cslc, build_cslc_native_ids, \
    build_ccslc_m_index, _C_CSLC_ES_INDEX_PATTERNS
from util.common_util import backoff_wrapper

_CSC_ES_INDEX_PATTERNS = f"grq_*_{c.CSLC_S1_CYCLE_STATE_CONFIG}*"
# Ceiling on cycle state configs read back for one frame. The whole DISP-S1 campaign is a few
# hundred acquisitions per frame, so this is generous; it exists to bound the response.
_MAX_CYCLE_STATE_CONFIGS = 1000


class CSLCDependency:
    def __init__(self, k: int, m: int, frame_to_bursts, args, token, cmr, settings, blackout_dates_obj, VV_only = True,
                 es_util = None):
        self.logger = get_logger()
        self.k = k
        self.m = m
        self.frame_to_bursts = frame_to_bursts
        self.args = args
        self.token = token
        self.cmr = cmr
        self.settings = settings
        self.blackout_dates_obj = blackout_dates_obj
        self.VV_only = VV_only
        # Only consulted for frames the burst database lists without sensing datetimes, whose
        # position in their own series can only be counted from what has already been observed.
        self.es_util = es_util
        self._known_cycles_cache = {}

    def _known_complete_cycles(self, frame_number: int, es_util = None):
        '''Acquisition cycles of this frame that the system has seen a full burst pattern for.

        For a frame with no sensing datetimes in the burst database there is no recorded series
        to count position in, so the series is what has been observed: the cycle state configs
        the cycle evaluator publishes, which exist exactly when a cycle's bursts are all present.
        The k-cycle evaluator decides window position and compressed-CSLC boundaries from the
        same records, so counting them here keeps the download side and the evaluator in
        agreement by construction rather than by coincidence.

        Blacked-out acquisitions never reach the catalog and their state configs are marked, so
        they are excluded here as they are everywhere else.

        Returns day indices ascending.'''

        if frame_number in self._known_cycles_cache:
            return self._known_cycles_cache[frame_number]

        eu = es_util or self.es_util
        if eu is None:
            raise RuntimeError(
                f"Frame {frame_number} has no sensing datetimes in the burst database, so its "
                f"k-cycle position must be counted from published cycle state configs, but no "
                f"OpenSearch connection was supplied to CSLCDependency")

        result = backoff_wrapper(
            eu.query,
            index=_CSC_ES_INDEX_PATTERNS,
            body={"query": {"bool": {
                "must": [
                    {"term": {"dataset_type.keyword": c.CSLC_S1_CYCLE_STATE_CONFIG}},
                    {"term": {f"metadata.{c.FRAME_ID}": frame_number}},
                    {"term": {f"metadata.{c.IS_COMPLETE}": True}}
                ],
                "must_not": [
                    {"term": {f"metadata.{c.BLACKOUT}": True}},
                    {"term": {f"metadata.{c.DB_EXCLUDED}": True}},
                ]
            }},
                # A frame acquires a few hundred times over the campaign; the default page of
                # ten hits would silently undercount and move every position after the tenth.
                "size": _MAX_CYCLE_STATE_CONFIGS,
                "_source": [f"metadata.{c.ACQUISITION_CYCLE}"]})

        if result is not None and len(result) >= _MAX_CYCLE_STATE_CONFIGS:
            self.logger.warning(
                "Frame %s returned %d cycle state configs, the maximum this query asks for. The "
                "count may be short and the k-cycle position with it.", frame_number, len(result))

        cycles = set()
        for doc in result or []:
            cycle = (doc.get("_source", {}).get("metadata") or {}).get(c.ACQUISITION_CYCLE)
            if cycle is not None:
                cycles.add(int(cycle))

        cycles = sorted(cycles)
        self._known_cycles_cache[frame_number] = cycles
        self.logger.info("Frame %s has %d complete acquisition cycle(s) on record: %s",
                         frame_number, len(cycles), cycles)
        return cycles

    def lineage_start_list_index(self, frame_number: int, day_index: int):
        '''Return the position in the frame's sensing time list where the compressed CSLC lineage
        containing day_index begins.

        A frame processed from a burst database that carries processing-mode annotations restarts its
        lineage at every new historical phase, so all dependency math has to count from there rather
        than from the beginning of the series. Frames without annotations return 0, which is the whole
        series and therefore exactly the un-phased behavior.'''

        frame = self.frame_to_bursts[frame_number]
        phases = getattr(frame, "phases", None)
        if not phases:
            return 0

        try:
            list_index = frame.sensing_datetime_days_index.index(day_index)
        except ValueError:
            # Beyond the end of the database; such dates belong to the last chunk
            list_index = len(frame.sensing_datetime_days_index)

        return lineage_start_pos(phases, list_index)

    def get_prev_day_indices(self, day_index: int, frame_number: int, eu = None):
        '''Return the day indices of the previous acquisitions for the frame_number given the current day index'''

        if frame_number not in self.frame_to_bursts:
            raise Exception(f"Frame number {frame_number} not found in the historical database. \
    OPERA does not process this frame for DISP-S1.")

        frame = self.frame_to_bursts[frame_number]

        # A frame the burst database lists without sensing datetimes has no recorded series to
        # index into, so its previous acquisitions are the ones already observed.
        if not frame.sensing_datetime_days_index:
            return [cycle for cycle in self._known_complete_cycles(frame_number, eu) if cycle < day_index]

        lineage_start = self.lineage_start_list_index(frame_number, day_index)

        if day_index <= frame.sensing_datetime_days_index[-1]:
            # If the day index is within the historical database, simply return from the database
            # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
            list_index = frame.sensing_datetime_days_index.index(day_index)
            return frame.sensing_datetime_days_index[lineage_start:list_index]
        else:
            # If not, we must query CMR and then append that to the database values
            start_date = frame.sensing_datetimes[-1] + timedelta(minutes=30)
            days_delta = day_index - frame.sensing_datetime_days_index[-1]
            end_date = start_date + timedelta(days=days_delta - 1) # We don't want the current day index in this
            query_timerange = DateTimeRange(start_date.strftime(CMR_TIME_FORMAT), end_date.strftime(CMR_TIME_FORMAT))
            acq_index_to_bursts, _ = self.get_k_granules_from_cmr(query_timerange, frame_number, verbose = False)
            all_prev_indices = (frame.sensing_datetime_days_index[lineage_start:]
                                + sorted(list(acq_index_to_bursts.keys())))
            self.logger.debug(f"All previous day indices: {all_prev_indices}")
            return all_prev_indices

    def get_k_granules_from_cmr(self, query_timerange, frame_number: int, verbose = True):
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
            args, self.token, self.cmr, self.settings, query_timerange, datetime.now(timezone.utc).replace(tzinfo=None), verbose, self.blackout_dates_obj, True, frame_number, self.VV_only)

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
                self.logger.info(
                    f"Removing day index {g_day_index} from k-cycle determination because it doesn't suffice the burst pattern")
                self.logger.info(f"{acq_index_to_bursts[g_day_index]}")
                del acq_index_to_bursts[g_day_index]
                del acq_index_to_granules[g_day_index]

        return acq_index_to_bursts, acq_index_to_granules

    def determine_k_cycle(self, acquisition_dts: datetime, day_index: int, frame_number: int, verbose = True, eu = None):
        '''Return where in the k-cycle this acquisition falls for the frame_number
        Must specify either acquisition_dts or day_index.
        Returns integer between 0 and k-1 where 0 means that it's at the start of the cycle

        Assumption: This current frame satisfies the burst pattern already; we don't need to check for that here'''

        if day_index is None:
            day_index = determine_acquisition_cycle_cslc(acquisition_dts, frame_number, self.frame_to_bursts)

        frame = self.frame_to_bursts[frame_number]

        # A frame the burst database lists without sensing datetimes has no recorded series to
        # take a position in, so its position is how many complete acquisitions have been
        # observed up to and including this one.
        if not frame.sensing_datetime_days_index:
            known = self._known_complete_cycles(frame_number, eu)
            index_number = len([cycle for cycle in known if cycle < day_index]) + 1
            return index_number % self.k

        # If the day index is within the historical database it's much simpler
        # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
        # Clearly if we somehow end up with like 1000
        try:
            # array.index returns 0-based index so add 1. The count starts at the current lineage, which is
            # the beginning of the series unless the burst database splits the frame into phases.
            list_index = frame.sensing_datetime_days_index.index(day_index) # note "index" is overloaded term here
            index_number = list_index - self.lineage_start_list_index(frame_number, day_index) + 1
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
            acq_index_to_bursts, _ = self.get_k_granules_from_cmr(query_timerange, frame_number, verbose)

            # The k-index is then the complete index number (historical + post historical) mod k
            self.logger.info(f"{len(acq_index_to_bursts.keys())} day indices since historical that match the burst pattern: {acq_index_to_bursts.keys()}")
            self.logger.info(f"{len(frame.sensing_datetime_days_index)} day indices already in historical database.")
            index_number = (len(frame.sensing_datetime_days_index)
                            - self.lineage_start_list_index(frame_number, day_index)
                            + len(acq_index_to_bursts.keys()) + 1)
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

        prev_day_indices = self.get_prev_day_indices(day_index, frame_id, eu)

        ccslcs = []

        # special case for early sensing time series; prev_day_indices only spans the current lineage,
        # so a new historical phase ramps m back up from 1 exactly like the start of the series does
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
                    self.logger.info("Compressed CSLCs for ccslc_m_index: %s was not found in GRQ ES", ccslc_m_index)
                    return False

                ccslcs.append(ccslc[0]) # There should only be one

        self.logger.info("All Compresseed CSLSs for frame %s at day index %s found in GRQ ES", frame_id, day_index)
        self.logger.debug(ccslcs)

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