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

from commons.logger import get_logger
from util import datasets_json_util
from util.conf_util import SettingsConf

DEFAULT_DISP_FRAME_BURST_DB_NAME = 'opera-disp-s1-consistent-burst-ids-with-datetimes.json'
DEFAULT_FRAME_GEO_SIMPLE_JSON_NAME = 'frame-geometries-simple.geojson'
PENDING_CSLC_DOWNLOADS_ES_INDEX_NAME = "grq_1_l2_cslc_s1_pending_downloads"
PENDING_TYPE_CSLC_DOWNLOAD = "cslc_download"
_C_CSLC_ES_INDEX_PATTERNS = "grq_1_l2_cslc_s1_compressed*"

class _HistBursts(object):
    def __init__(self):
        self.frame_number = None
        self.burst_ids = set()                 # Burst ids as strings in a set
        self.sensing_datetimes = []            # Sensing datetimes as datetime object, sorted
        self.sensing_seconds_since_first = []  # Sensing time in seconds since the first sensing time
        self.sensing_datetime_days_index = []  # Sensing time in days since the first sensing time, rounded to the nearest day

def get_s3_resource_from_settings(settings_field):
    settings = SettingsConf().cfg
    burst_file_url = urlparse(settings[settings_field])
    s3 = boto3.resource('s3')
    path = burst_file_url.path.lstrip("/")
    file = path.split("/")[-1]

    return s3, path, file, burst_file_url

@backoff.on_exception(backoff.expo, Exception, max_time=30)
def localize_anc_json(settings_field):
    '''Copy down a file from S3 whose path is defined in settings.yaml by settings_field'''

    s3, path, file, burst_file_url = get_s3_resource_from_settings(settings_field)
    s3.Object(burst_file_url.netloc, path).download_file(file)

    return file

@cache
def localize_disp_frame_burst_hist():
    logger = get_logger()

    try:
        file = localize_anc_json("DISP_S1_BURST_DB_S3PATH")
    except:
        logger.warning(f"Could not download DISP-S1 burst database json from settings.yaml field DISP_S1_BURST_DB_S3PATH from S3. "
                       f"Attempting to use local copy named {DEFAULT_DISP_FRAME_BURST_DB_NAME}.")
        file = DEFAULT_DISP_FRAME_BURST_DB_NAME

    return process_disp_frame_burst_hist(file)

@cache
def localize_frame_geo_json():
    logger = get_logger()

    try:
        file = localize_anc_json("DISP_S1_FRAME_GEO_SIMPLE")
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
    assert not (remainder > 0.493 and remainder < 0.507), \
        f"Potential ambiguous day index grouping: {day_index_high_precision=}"

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

    for i, dt in enumerate(frame_sensing_datetimes):
        if dt > sensing_time:
            return i, frame_sensing_datetimes[i-1]

    return len(frame_sensing_datetimes), frame_sensing_datetimes[-1]

def calculate_historical_progress(frame_states: dict, end_date, frame_to_bursts, k=15):
    '''Assumes start date of historical processing as the earlest date possible which is really the only way it should be run'''

    total_possible_sensingdates = 0
    total_processed_sensingdates = 0
    frame_completion = {}
    last_processed_datetimes = {}

    for frame, state in frame_states.items():
        frame = int(frame)
        num_sensing_times, _ = get_nearest_sensing_datetime(frame_to_bursts[frame].sensing_datetimes, end_date)

        # Round down to the nearest k
        num_sensing_times = num_sensing_times - (num_sensing_times % k)

        total_possible_sensingdates += num_sensing_times
        total_processed_sensingdates += state
        frame_completion[str(frame)] = round(state / num_sensing_times * 100) if num_sensing_times > 0 else 0
        last_processed_datetimes[str(frame)] = frame_to_bursts[frame].sensing_datetimes[state-1] if state > 0 else None

    progress_percentage = round(total_processed_sensingdates / total_possible_sensingdates * 100)
    return progress_percentage, frame_completion, last_processed_datetimes

@cache
def process_disp_frame_burst_hist(file):
    '''Process the disp frame burst map json file intended and return 3 dictionaries'''
    logger = get_logger()

    try:
        j = json.load(open(file))["data"]
    except:
        logger.warning("No 'data' key found in the json file. Attempting to load the json file as an older format.")
        j = json.load(open(file))

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

        frame_to_bursts[int(frame)].sensing_datetimes =\
            sorted([dateutil.parser.isoparse(t) for t in j[frame]["sensing_time_list"]])

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

def parse_cslc_file_name(native_id):
    dataset_json = datasets_json_util.DatasetsJson()
    cslc_granule_regex = dataset_json.get("L2_CSLC_S1")["match_pattern"]
    match_product_id = re.match(cslc_granule_regex, native_id)

    if not match_product_id:
        raise ValueError(f"CSLC native ID {native_id} could not be parsed with regex from datasets.json")

    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3
    acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z
    return burst_id, acquisition_dts

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

def save_blocked_download_job(eu, release_version, product_type, params, job_queue, job_name,
                              frame_id, acq_index, k, m, batch_ids):
    """Save the blocked download job in the ES index"""

    eu.index_document(
        index=PENDING_CSLC_DOWNLOADS_ES_INDEX_NAME,
        id = job_name,
        body = {
                "job_type": PENDING_TYPE_CSLC_DOWNLOAD,
                "release_version": release_version,
                "job_name": job_name,
                "job_queue": job_queue,
                "job_params": params,
                "job_ts": datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z"),
                "product_type": product_type,
                "frame_id": frame_id,
                "acq_index": acq_index,
                "k": k,
                "m": m,
                "batch_ids": batch_ids,
                "submitted": False,
                "submitted_job_id": None
        }
    )

def get_pending_download_jobs(es):
    '''Retrieve all pending cslc download jobs from the ES index'''

    try:
        result =  es.query(
            index=PENDING_CSLC_DOWNLOADS_ES_INDEX_NAME,
            body={"query": {
                    "bool": {
                        "must": [
                            {"term": {"submitted": False}},
                            {"match": {"job_type": PENDING_TYPE_CSLC_DOWNLOAD}}
                        ]
                    }
                }
            }
        )
    except elasticsearch.exceptions.NotFoundError as e:
        return []

    return result

def mark_pending_download_job_submitted(es, doc_id, download_job_id):
    return es.update_document(
        index=PENDING_CSLC_DOWNLOADS_ES_INDEX_NAME,
        id = doc_id,
        body={ "doc_as_upsert": True,
                "doc": {"submitted": True, "submitted_job_id": download_job_id}
        }
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

