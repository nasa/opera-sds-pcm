import json
import re
from collections import defaultdict
from datetime import datetime
from functools import cache
from urllib.parse import urlparse
import backoff

import boto3
import dateutil
import elasticsearch
import opensearchpy

from opera_commons.logger import get_logger
from data_subscriber.cslc.disp_s1_phases import (PhaseKind, PhaseValidationError, parse_sensing_time_list,
                                                 phase_for_position, segment_phases)
from util import datasets_json_util
from util.conf_util import SettingsConf

DEFAULT_DISP_FRAME_BURST_DB_NAME = 'opera-disp-s1-consistent-burst-ids-with-datetimes.json'
DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME = 'frame-geometries-simple.geojson'
PENDING_JOBS_ES_INDEX_NAME = "grq_pending_jobs"
PENDING_TYPE_CSLC_DOWNLOAD = "cslc_download"
_C_CSLC_ES_INDEX_PATTERNS = "grq_1_l2_cslc_s1_compressed*"
PROCESSING_MODE_SETTINGS_FIELD = "DISP_S1_PROCESSING_MODE_ENABLED"
BURST_DB_EXCLUSION_SETTINGS_FIELD = "DISP_S1_BURST_DB_EXCLUSION_ENABLED"
# metadata.input_cmr_csv names the survey the database was built from, e.g.
# "cmr_survey_2016-07-01_to_2026-06-23.csv". It is the only carrier of the assessed
# range that survives a rename, which both deployed databases have had.
_CMR_SURVEY_RANGE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})")
# OPERA_L2_CSLC-S1_<burst>_<sensing>T..Z_<processing>T..Z_<sat>_<pol>_v<ver>
_CSLC_GRANULE_RE = re.compile(
    r"OPERA_L2_CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)")

settings = SettingsConf().cfg

class _HistBursts(object):
    def __init__(self):
        self.frame_number = None
        self.burst_ids = set()                 # Burst ids as strings in a set
        self.sensing_datetimes = []            # Sensing datetimes as datetime object, sorted
        self.sensing_seconds_since_first = []  # Sensing time in seconds since the first sensing time
        self.sensing_datetime_days_index = []  # Sensing time in days since the first sensing time, rounded to the nearest day
        self.processing_modes = None           # Processing-mode label per sensing datetime, or None when unannotated
        self.phases = None                     # Contiguous ProcessingPhase list derived from the labels, or None
        self.phase_error = None                # Why the labels were rejected, when they were
        self.processing_mode_batch_size = None # The k the labels were generated for

def get_s3_resource_from_settings(settings_field, settings_yaml_path=None):

    settings = SettingsConf(settings_yaml_path).cfg
    burst_file_url = urlparse(settings[settings_field])
    s3 = boto3.resource('s3')
    path = burst_file_url.path.lstrip("/")
    file = path.split("/")[-1]

    return s3, path, file, burst_file_url

logger = get_logger()

@backoff.on_exception(backoff.expo, Exception, max_time=30)
def localize_anc_json(settings_field, settings_yaml_path=None):
    '''Copy down a file from S3 whose path is defined in settings.yaml by settings_field'''

    s3, path, file, burst_file_url = get_s3_resource_from_settings(settings_field, settings_yaml_path)
    s3.Object(burst_file_url.netloc, path).download_file(file)

    return file

def processing_mode_enabled(settings_yaml_path=None):
    '''Return the master switch that governs whether processing-mode annotations in the burst database are used.

    When this is off, an annotated database is parsed exactly like an un-annotated one: the mode labels are
    dropped and every phase-aware code path falls back to the un-phased behavior.'''

    try:
        cfg = SettingsConf(settings_yaml_path).cfg if settings_yaml_path else settings
        return bool(cfg.get(PROCESSING_MODE_SETTINGS_FIELD, False))
    except Exception as e:
        logger.warning(f"Could not read {PROCESSING_MODE_SETTINGS_FIELD} from settings: {e}. Defaulting to disabled.")
        return False

def latest_cslc_per_burst(product_paths):
    """Keep one CSLC per burst and sensing time: the most recently processed.

    ASF republishes a burst under a new processing date when it reprocesses, and both
    granules stay in the catalog. Handing the SAS both is not merely wasteful -- it
    refuses the stack outright ("Duplicate dates passed for <burst>"), so a single
    reprocessing event silently blocks every affected sensing date. The historical path
    never sees this because cslc_catalog is keyed frame/cycle/burst and a reprocessed
    granule overwrites in place; this makes the forward path agree with it.

    The newest processing date wins, which is the same last-write-wins rule the catalog
    key applies. Anything whose filename does not parse is passed through untouched
    rather than dropped -- never silently discard an input we cannot reason about.
    """
    best = {}
    passthrough = []
    for path in product_paths or []:
        match = _CSLC_GRANULE_RE.search(str(path))
        if not match:
            passthrough.append(path)
            continue
        key = (match.group(1), match.group(2))       # burst id, sensing time
        processed = match.group(3)
        current = best.get(key)
        if current is None or processed > current[0]:
            best[key] = (processed, path)
    return sorted([v[1] for v in best.values()] + passthrough)


def burst_db_exclusion_enabled(settings_yaml_path=None):
    '''Return whether the burst database's exclusions are honored in forward processing.

    An acquisition the database does not list, on a date inside the range its CMR survey
    assessed, was left out deliberately -- so it must not occupy a k-slot or count as an
    unresolved gap. This is a correctness repair rather than an opt-in feature, so it is
    ON unless a venue explicitly turns it off: an absent key reads as enabled, and only
    a literal false in settings restores the previous behavior.'''

    try:
        cfg = SettingsConf(settings_yaml_path).cfg if settings_yaml_path else settings
        return bool(cfg.get(BURST_DB_EXCLUSION_SETTINGS_FIELD, True))
    except Exception as e:
        logger.warning(f"Could not read {BURST_DB_EXCLUSION_SETTINGS_FIELD} from settings: {e}. "
                       f"Defaulting to disabled.")
        return False

@cache
def disp_burst_db_assessed_end(file):
    '''Return the last date the burst database's CMR survey assessed, as YYYYMMDD, or None.

    Absence from sensing_time_list only means "deliberately excluded" inside this range.
    Past it the survey never looked, so absence carries no information and the date is
    ordinary forward work. Every failure path returns None, which disables the exclusion
    entirely -- never guess a range.'''

    try:
        j = json.load(open(file))
    except Exception as e:
        logger.warning(f"Could not read the burst database at {file} for its assessed range: {e}. "
                       f"Burst-database exclusions are disabled.")
        return None

    if not isinstance(j, dict):
        return None
    survey = ((j.get("metadata") or {}).get("input_cmr_csv") or "")
    match = _CMR_SURVEY_RANGE_RE.search(survey)
    if not match:
        logger.warning(f"Burst database {file} carries no parseable CMR survey range "
                       f"(metadata.input_cmr_csv={survey!r}). Burst-database exclusions are disabled.")
        return None
    return match.group(2).replace("-", "")

@cache
def localize_disp_burst_db_assessed_end(settings_yaml_path=None):
    '''The assessed end date of the deployed burst database, or None when unavailable.'''

    try:
        file = localize_anc_json("DISP_S1_BURST_DB_S3PATH", settings_yaml_path)
    except Exception:
        logger.warning(f"Could not download the DISP-S1 burst database for its assessed range. "
                       f"Attempting to use local copy named {DEFAULT_DISP_FRAME_BURST_DB_NAME}.")
        file = DEFAULT_DISP_FRAME_BURST_DB_NAME

    return disp_burst_db_assessed_end(file)

@cache
def localize_disp_frame_burst_hist(settings_yaml_path=None):

    try:
        file = localize_anc_json("DISP_S1_BURST_DB_S3PATH", settings_yaml_path)
    except:
        logger.warning(f"Could not download DISP-S1 burst database json from settings.yaml field DISP_S1_BURST_DB_S3PATH from S3. "
                       f"Attempting to use local copy named {DEFAULT_DISP_FRAME_BURST_DB_NAME}.")
        file = DEFAULT_DISP_FRAME_BURST_DB_NAME

    return process_disp_frame_burst_hist(file, processing_mode_enabled(settings_yaml_path))

@cache
def localize_frame_geo_json(settings_yaml_path=None):

    try:
        file = localize_anc_json("DISP_S1_FRAME_GEO_SIMPLE", settings_yaml_path=None)
    except:
        logger.warning(f"Could not download DISP-S1 frame geo simple json {DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME} from S3. "
                       f"Attempting to use local copy named {DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME}.")
        file = DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME

    return process_frame_geo_json(file)

def _calculate_sensing_time_day_index(sensing_time: datetime, first_frame_time: datetime):
    ''' Return the day index of the sensing time relative to the first sensing time of the frame'''

    delta = sensing_time - first_frame_time
    seconds = int(delta.total_seconds())
    day_index_high_precision = seconds / (24 * 3600)

    # Sanity check of the day index, 10 minute tolerance 10 / 24 / 60 = 0.0069444444 ~= 0.007
    remainder = day_index_high_precision - int(day_index_high_precision)
    
    # Smart rounding: Sentinel-1 has a 6-day repeat cycle, so day indices should be multiples of 6
    # When ambiguous (within ±10 minutes of boundary), round to the nearest multiple of 6
    if remainder > 0.493 and remainder < 0.507:
        day_index_low = int(day_index_high_precision)
        day_index_high = day_index_low + 1
        
        # Check which one is closer to a multiple of 6
        mod_low = day_index_low % 6
        mod_high = day_index_high % 6
        
        # Prefer the one that is a multiple of 6, or closer to one
        if mod_low == 0:
            # Low is exactly a multiple of 6
            day_index = day_index_low
        elif mod_high == 0:
            # High is exactly a multiple of 6
            day_index = day_index_high
        else:
            # Neither is a multiple of 6, use normal rounding
            # This shouldn't normally happen for Sentinel-1 data
            day_index = int(round(day_index_high_precision))
            logger = get_logger()
            logger.warning(f"Ambiguous day index {day_index_high_precision:.10f} where neither "
                         f"{day_index_low} (mod 6 = {mod_low}) nor {day_index_high} (mod 6 = {mod_high}) "
                         f"is a multiple of 6. Using normal rounding to {day_index}. "
                         f"This may indicate issues with the sensing time list in the consistent database "
                         f"and/or the blackout date ranges for the frame.")
    else:
        # Normal case: not ambiguous, use standard rounding
        day_index = int(round(day_index_high_precision))

    return day_index, seconds

def sensing_time_day_index(sensing_time: datetime, frame_number: int, frame_to_bursts):
    ''' Return the day index of the sensing time relative to the first sensing time of the frame AND
    seconds since the first sensing time of the frame'''

    frame = frame_to_bursts[frame_number]
    return (_calculate_sensing_time_day_index(sensing_time, frame.sensing_datetimes[0]))

def get_nearest_sensing_datetime(frame_sensing_datetimes, sensing_time):
    '''Return the nearest sensing datetime in the frame sensing datetime list that is not greater than the sensing time and
    the number of sensing datetimes until that datetime.
    It's a linear search in a sorted list but no big deal because there will only ever be a few hundred elements'''

    if len(frame_sensing_datetimes) == 0:
        return 0, None

    for i, dt in enumerate(frame_sensing_datetimes):
        if dt > sensing_time:
            return i, frame_sensing_datetimes[i-1]

    return len(frame_sensing_datetimes), frame_sensing_datetimes[-1]

def expand_batch_proc_frames(frames):
    '''Expand a batch proc frames list into frame numbers.

    Entries are either a frame number or an inclusive [start, end] range.'''

    expanded = []

    for frame in frames:
        if isinstance(frame, list):
            if len(frame) != 2:
                raise ValueError("Frame range must have two elements")
            if frame[0] > frame[1]:
                raise ValueError("Frame range must be in ascending order")
            expanded.extend(range(frame[0], frame[1] + 1))
        else:
            expanded.append(frame)

    return expanded

def validate_phased_batch_proc(proc, frame_to_bursts):
    '''Check a phased batch proc against the deployed burst database.

    Returns True when it can run, or a message explaining why it cannot: the phase walk needs
    every listed frame annotated by the same k the annotations were generated for.'''

    k = proc.get("k")

    for frame_id in expand_batch_proc_frames(proc.get("frames", [])):
        frame = frame_to_bursts.get(frame_id)
        if frame is None:
            return f"Frame {frame_id} not found in DISP-S1 Burst ID Database JSON"

        if frame.phases is None:
            reason = frame.phase_error or (
                f"the deployed burst database has no processing-mode annotations for it, or "
                f"{PROCESSING_MODE_SETTINGS_FIELD} is off")
            return f"Frame {frame_id} cannot be processed by a phased batch proc: {reason}"

        if frame.processing_mode_batch_size != k:
            return (f"Frame {frame_id} annotations were generated for batch size "
                    f"{frame.processing_mode_batch_size} but the batch proc uses k={k}")

    return True

def validate_unphased_batch_proc(proc, frame_to_bursts):
    '''Check that an un-phased batch proc's k-sets stay inside one gap-separated chunk each.

    Stepping k dates at a time from the start of the series ignores the phase labels, so a k-set
    can span a chunk boundary -- a stack whose dates straddle a multi-year acquisition gap. The
    SAS rejects that outright, so the whole k-set is wasted compute and the failure surfaces only
    after query, download and SCIFLO have all run.

    The test is the chunk ordinal, not the label. A historical_NN -> forward_NN transition is a
    label change within one chunk, with no gap between the two dates either side of it, and the
    SAS processes such a stack happily; only a change of ordinal marks a real gap. no_run carries
    no ordinal, so a k-set running into or out of one is rejected as well.

    Only the k-sets the batch proc would actually submit are checked, so an un-phased batch proc
    whose date range stops short of a chunk boundary is fine. Returns True or a message.

    One case this cannot catch: consecutive unusable chunks are merged into a single no_run run,
    so a k-set falling entirely inside one may still straddle a gap.'''

    if proc.get("phased", False) is True:
        return True

    k = proc.get("k")
    try:
        data_start = datetime.strptime(proc["data_start_date"], "%Y-%m-%dT%H:%M:%S")
        data_end = datetime.strptime(proc["data_end_date"], "%Y-%m-%dT%H:%M:%S")
    except (KeyError, ValueError):
        return True

    for frame_id in expand_batch_proc_frames(proc.get("frames", [])):
        frame = frame_to_bursts.get(frame_id)
        phases = getattr(frame, "phases", None) if frame is not None else None
        if not phases or not k:
            continue

        for position in range(0, len(frame.sensing_datetimes) - k + 1, k):
            window = frame.sensing_datetimes[position:position + k]
            if window[-1] > data_end:
                break                      # the walk reports itself finished here
            if window[0] < data_start:
                continue                   # the walk skips this k-set and steps on

            first = phase_for_position(phases, position)
            last = phase_for_position(phases, position + k - 1)
            if first.ordinal != last.ordinal:
                return (f"Frame {frame_id} k-set {window[0]:%Y-%m-%d}..{window[-1]:%Y-%m-%d} spans "
                        f"{first.label} and {last.label}, which sit in different gap-separated "
                        f"chunks. Stepping k dates at a time from the start of the series puts a "
                        f"multi-year gap inside one stack, which the SAS rejects. Set "
                        f'"phased": true to walk this frame phase by phase.')

    return True

def _phased_progress_counts(phases, num_sensing_times, state, k):
    '''Return (processable, processed) sensing date counts for a phase-annotated frame.

    Historical phases count in whole k-sets, which is the unit the historical tool submits;
    forward phases count per date; no_run phases are never processed and are out of scope.'''

    processable = 0
    processed = 0

    for phase in phases:
        if phase.kind is PhaseKind.NO_RUN:
            continue

        available = max(0, min(phase.end_pos, num_sensing_times) - phase.start_pos)
        done = max(0, min(phase.end_pos, state) - phase.start_pos)

        if phase.kind is PhaseKind.HISTORICAL:
            available -= available % k
            done -= done % k

        processable += available
        processed += min(done, available)

    return processable, processed

def calculate_historical_progress(frame_states: dict, end_date, frame_to_bursts, k=15, phased=False):
    '''Assumes start date of historical processing as the earlest date possible which is really the only
    way it should be run

    A phased batch proc accounts for progress per phase: no_run dates are excluded from the work
    to be done, so a frame with nothing processable reads as complete rather than as 0%.'''

    total_possible_sensingdates = 0
    total_processed_sensingdates = 0
    frame_completion = {}
    last_processed_datetimes = {}

    for frame, state in frame_states.items():
        logger.debug(f"Calculating percentage progress for {frame=}")
        frame = int(frame)
        num_sensing_times, _ = get_nearest_sensing_datetime(frame_to_bursts[frame].sensing_datetimes, end_date)

        phases = frame_to_bursts[frame].phases if phased else None
        if phases is not None:
            num_sensing_times, processed_sensingdates = _phased_progress_counts(phases, num_sensing_times, state, k)
        else:
            # Round down to the nearest k
            num_sensing_times = num_sensing_times - (num_sensing_times % k)
            processed_sensingdates = state

        total_possible_sensingdates += num_sensing_times
        total_processed_sensingdates += processed_sensingdates
        if num_sensing_times > 0:
            frame_completion[str(frame)] = round(processed_sensingdates / num_sensing_times * 100)
        else:
            # A phased frame with nothing processable (every phase no_run) is trivially complete
            frame_completion[str(frame)] = 100 if phases is not None else 0
        last_processed_datetimes[str(frame)] = frame_to_bursts[frame].sensing_datetimes[state-1] if state > 0 else None

    progress_percentage = round(total_processed_sensingdates / total_possible_sensingdates * 100) \
        if total_possible_sensingdates > 0 else 100
    return progress_percentage, frame_completion, last_processed_datetimes

def _attach_processing_phases(frame, labels, batch_size):
    '''Attach the processing-mode labels and the phases derived from them to one frame.

    A frame whose labels violate the labeler's contract keeps phases as None and records why in
    phase_error, so a caller can quarantine that one frame instead of failing the whole database.'''

    frame.processing_modes = labels
    frame.processing_mode_batch_size = batch_size

    if not batch_size:
        frame.phase_error = "burst database has no metadata.processing_mode_params.batch_size to segment phases with"
        logger.warning("Frame %s: %s", frame.frame_number, frame.phase_error)
        return

    try:
        frame.phases = segment_phases(labels, batch_size)
    except Exception as e:
        # The database is external input: no shape of it may take down a load that every DISP-S1
        # component depends on. PhaseValidationError is the contract violation; anything else
        # (a label that is not even a string, say) is quarantined the same way.
        frame.phase_error = str(e) if isinstance(e, PhaseValidationError) else f"{type(e).__name__}: {e}"
        logger.warning("Frame %s processing-mode labels rejected: %s", frame.frame_number, frame.phase_error)

@cache
def process_disp_frame_burst_hist(file, use_processing_modes=None):
    '''Process the disp frame burst map json file intended and return 3 dictionaries

    use_processing_modes governs whether mode annotations, if the file carries any, are retained;
    it defaults to the setting read by processing_mode_enabled().'''

    if use_processing_modes is None:
        use_processing_modes = processing_mode_enabled()

    j = json.load(open(file))
    if "data" in j:
        db_metadata = j.get("metadata", {})
        j = j["data"]
    else:
        logger.warning("No 'data' key found in the json file. Attempting to load the json file as an older format.")
        db_metadata = {}

    batch_size = (db_metadata.get("processing_mode_params") or {}).get("batch_size")

    frame_to_bursts = defaultdict(_HistBursts)
    burst_to_frames = defaultdict(list)         # List of frame numbers
    datetime_to_frames = defaultdict(list)      # List of frame numbers

    for frame in j:

        frame_to_bursts[int(frame)].frame_number = int(frame)

        b = frame_to_bursts[int(frame)].burst_ids
        for burst in j[frame]["burst_id_list"]:
            burst = burst.upper().replace("_", "-")
            b.add(burst)

            # Map from burst id to the frames
            burst_to_frames[burst].append(int(frame))
            assert len(burst_to_frames[burst]) <= 2  # A burst can belong to at most two frames

        sensing_datetimes, processing_modes = parse_sensing_time_list(j[frame]["sensing_time_list"])
        frame_to_bursts[int(frame)].sensing_datetimes = sensing_datetimes

        if use_processing_modes and processing_modes is not None:
            _attach_processing_phases(frame_to_bursts[int(frame)], processing_modes, batch_size)

        for sensing_time in frame_to_bursts[int(frame)].sensing_datetimes:
            day_index, seconds = sensing_time_day_index(sensing_time, int(frame), frame_to_bursts)
            frame_to_bursts[int(frame)].sensing_seconds_since_first.append(seconds)
            frame_to_bursts[int(frame)].sensing_datetime_days_index.append(day_index)

            # Build up dict of day_index to the frame object
            datetime_to_frames[sensing_time].append(int(frame))

    return frame_to_bursts, burst_to_frames, datetime_to_frames

@cache
def process_frame_geo_json(file):
    '''Process the frame-geometries-simple.geojson file as dictionary used for determining frame bounding box'''

    frame_geo_map = {}
    j = json.load(open(file))
    for feature in j["features"]:
        frame_id = feature["id"]
        geom = feature["geometry"]
        if geom["type"] == "Polygon":
            xmin = min([x for x, y in geom["coordinates"][0]])
            ymin = min([y for x, y in geom["coordinates"][0]])
            xmax = max([x for x, y in geom["coordinates"][0]])
            ymax = max([y for x, y in geom["coordinates"][0]])

        elif geom["type"] == "MultiPolygon":
            all_coords = []
            for coords in geom["coordinates"]:
                all_coords.extend(coords[0])

            ymin = min([y for x, y in all_coords])
            ymax = max([y for x, y in all_coords])

            # MultiPolygon is only used for frames that cross the meridian line.
            # Math looks funny but in the end we want the most-West x as min and most-East x as max
            xmin = -180
            xmax = 180
            for x,y in all_coords:
                if x < 0 and x > xmin:
                    xmin = x
                if x > 0 and x < xmax:
                    xmax = x

        frame_geo_map[frame_id] = [xmin, ymin, xmax, ymax]

    return frame_geo_map


def get_geojson_for_frame(frame_id, frame_geojson_map):
    """Returns the GeoJSON geometry for a frame, suitable for the 'location'
    field in HySDS datasets (visible on Tosca)."""
    return frame_geojson_map.get(frame_id)


def localize_frame_geojson_map(settings_yaml_path=None):
    """Load the frame geometries GeoJSON and return a dict mapping
    frame_id -> GeoJSON geometry (Polygon/MultiPolygon)."""
    try:
        file = localize_anc_json("DISP_S1_FRAME_GEO_SIMPLE", settings_yaml_path=None)
    except Exception:
        logger.warning(f"Could not download DISP-S1 frame geo simple json. "
                       f"Attempting to use local copy named {DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME}.")
        file = DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME

    frame_geojson_map = {}
    j = json.load(open(file))
    for feature in j["features"]:
        frame_geojson_map[feature["id"]] = feature["geometry"]
    return frame_geojson_map

def parse_r2_product_file_name(native_id, product_type):
    match_product_id = _datasets_json_match(product_type, native_id)
    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3 (for RTC and CSLC)
    acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z
    return burst_id, acquisition_dts

# TODO chrisjrd: move to dataset_util.py or similar
def _datasets_json_match(product_type, native_id):
    dataset_json = datasets_json_util.DatasetsJson()
    cslc_granule_regex = dataset_json.get(product_type)["match_pattern"]
    match_product_id = re.match(cslc_granule_regex, native_id)

    if not match_product_id:
        raise ValueError(f"{product_type} native ID {native_id} could not be parsed with regex from datasets.json")
    return match_product_id


def parse_cslc_file_name(native_id):
    return parse_r2_product_file_name(native_id, "L2_CSLC_S1")

def parse_ccslc_file_name(native_id):
    """Parse a Compressed CSLC filename and return (burst_id, last_date_time).

    last_date_time is the YYYYMMDD string of the last sensing date covered by the CCSLC.
    """
    dataset_json = datasets_json_util.DatasetsJson()
    regex = dataset_json.get("L2_CSLC_S1_COMPRESSED")["match_pattern"]
    match = re.match(regex, native_id)
    if not match:
        raise ValueError(f"CCSLC native ID {native_id} could not be parsed")
    return match.group("burst_id"), match.group("last_date_time")


# Shared CCSLC doc-ID date regex used by the catalog-ingest bootstrap pre-flight
# checks and the k-cycle evaluator's lineage bound lookup.
# Format: ...<ref>T<...>Z_<first_sec>T<...>Z_<last_sec>T<...>Z_<creation>T<...>Z_...
# Groups: (ref_date, first_secondary, last_secondary, creation_date) — YYYYMMDD.
# Self-contained (no datasets_json dependency) so it works in any execution
# context that processes CCSLC ES hits.
CCSLC_DOC_ID_DATE_RE = re.compile(
    r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_"
)


def parse_ccslc_doc_id_dates(doc_id):
    """Extract (ref, first_secondary, last_secondary, creation) dates from a
    CCSLC doc ID.

    Returns the 4-tuple of YYYYMMDD strings or None if the ID does not match
    the expected pattern. ``last_secondary`` is the k-boundary date the CCSLC
    sits on.
    """
    m = CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None

def generate_arbitrary_cslc_native_id(disp_burst_map_hist, frame_id, burst_number, acquisition_datetime: datetime,
                                      production_datetime: datetime, polarization):
    '''Generate a CSLC native id for testing purposes. THIS IS NOT a real CSLC ID, that exists in the real world!
    Burst number is integer between 0 and 26 which designates the burst number in the frame. In cases of frames not having all 27 bursts,
    it will simply wrap over'''

    frame = disp_burst_map_hist[frame_id]
    burst_number = burst_number % len(frame.burst_ids)
    burst_id = sorted(list(frame.burst_ids))[burst_number] # Sort for the order to be deterministic

    # Convert datetime objects into strings
    acquisition_datetime = acquisition_datetime.strftime("%Y%m%dT%H%M%SZ")
    production_datetime = production_datetime.strftime("%Y%m%dT%H%M%SZ")

    return f"OPERA_L2_CSLC-S1_{burst_id}_{acquisition_datetime}_{production_datetime}_S1A_{polarization}_v1.1"

def determine_acquisition_cycle_cslc(acquisition_dts: datetime, frame_number: int, frame_to_bursts):
    # TODO: We need to handle the case where the consistent burst db does not have any sensing datetimes for the frame

    day_index, seconds = sensing_time_day_index(acquisition_dts, frame_number, frame_to_bursts)
    return day_index

def parse_cslc_native_id(native_id, burst_to_frames, frame_to_bursts):

    burst_id, acquisition_dts = parse_cslc_file_name(native_id)
    acquisition_dts = dateutil.parser.isoparse(acquisition_dts[:-1])  # convert to datetime object

    frame_ids = burst_to_frames[burst_id]

    # Acquisition cycle is frame-dependent and one CSLC burst can belong to at most two frames
    acquisition_cycles = {}
    for frame_id in frame_ids:
        acquisition_cycles[frame_id] = determine_acquisition_cycle_cslc(acquisition_dts, frame_id, frame_to_bursts)

    assert len(acquisition_cycles) <= 2  # A burst can belong to at most two frames. If it doesn't, we have a problem.

    return burst_id, acquisition_dts, acquisition_cycles, frame_ids

def save_blocked_download_job(eu, job_type, release_version, product_type, params, job_queue, job_name, add_attributes):
    """Save the blocked download job in the ES index"""

    # It looks like we could use params to get similar information as from add_attributes but it's not easy to query ES for that.
    eu.index_document(
        index=PENDING_JOBS_ES_INDEX_NAME,
        id = job_name,
        body = {
                "job_type": job_type,
                "release_version": release_version,
                "job_name": job_name,
                "job_queue": job_queue,
                "job_params": params,
                "job_ts": datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z"),
                "product_type": product_type,
                **add_attributes,
                "submitted": False,
                "submitted_job_id": None
        }
    )

def get_pending_download_jobs(es):
    '''Retrieve all pending cslc download jobs from the ES index'''

    try:
        result =  es.query(
            index=PENDING_JOBS_ES_INDEX_NAME,
            body={"query": {
                    "bool": {
                        "must": [
                            {"term": {"submitted": False}}
                        ]
                    }
                }
            }
        )
    except (elasticsearch.exceptions.NotFoundError, opensearchpy.exceptions.NotFoundError) as e:
        return []

    return result

def mark_pending_download_job_submitted(es, doc_id, download_job_id):
    doc = {"submitted": True, "submitted_job_id": download_job_id}
    body = {
        "doc_as_upsert": True,
        "doc": doc
    }

    return es.update_document(
        index=PENDING_JOBS_ES_INDEX_NAME,
        id = doc_id,
        body=body
    )

def parse_cslc_burst_id(native_id):

    burst_id, _ = parse_cslc_file_name(native_id)
    return burst_id

def build_cslc_native_ids(frame: int, disp_burst_map):
    """Builds the native_id string for a given frame. The native_id string is used in the CMR query."""

    native_ids = list(disp_burst_map[frame].burst_ids)
    native_ids = sorted(native_ids) # Sort to just enforce consistency
    return len(native_ids), "OPERA_L2_CSLC-S1_" + "*&native-id[]=OPERA_L2_CSLC-S1_".join(native_ids) + "*"

def build_cslc_static_native_ids(burst_ids):
    """
    Builds the native_id string used with a CMR query for CSLC-S1 Static Layer
    products based on the provided list of burst IDs.
    """
    return "OPERA_L2_CSLC-S1-STATIC_" + "*&native-id[]=OPERA_L2_CSLC-S1-STATIC_".join(burst_ids) + "*"

def build_ccslc_m_index(burst_id, acquisition_cycle):
    return (burst_id + "_" + str(acquisition_cycle)).replace("-", "_").lower()
def download_batch_id_forward_reproc(granule):
    """For forward and re-processing modes, download_batch_id is a function of the granule's frame_id and acquisition_cycle"""

    download_batch_id = "f"+str(granule["frame_id"]) + "_a" + str(granule["acquisition_cycle"])

    return download_batch_id

def split_download_batch_id(download_batch_id):
    """Split the download_batch_id into frame_id and acquisition_cycle
    example: forward/reproc f7098_a145 -> 7098, 145"""
    frame_id, acquisition_cycle = download_batch_id.split("_")
    return int(frame_id[1:]), int(acquisition_cycle[1:])  # Remove the leading "f" and "a"

def get_bounding_box_for_frame(frame_id: int, frame_geo_map):
    """Returns a bounding box for a given frame in the format of [xmin, ymin, xmax, ymax] in EPSG4326 coordinate system"""

    return frame_geo_map[frame_id]

