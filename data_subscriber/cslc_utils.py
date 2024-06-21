import json
import re
from collections import defaultdict
import asyncio
from datetime import datetime, timedelta
import dateutil
import boto3
import logging

from data_subscriber.cmr import async_query_cmr, CMR_TIME_FORMAT, DateTimeRange
from util import datasets_json_util
from util.conf_util import SettingsConf

DISP_FRAME_BURST_MAP_HIST = 'opera-disp-s1-consistent-burst-ids-with-datetimes.json'
FRAME_GEO_SIMPLE_JSON = 'frame-geometries-simple.geojson'

class _HistBursts(object):
    def __init__(self):
        self.frame_number = None
        self.burst_ids = set()                  # Burst ids as strings in a set
        self.sensing_datetimes = []           # Sensing datetimes as datetime object, sorted
        self.sensing_seconds_since_first = [] # Sensing time in seconds since the first sensing time
        self.sensing_datetime_days_index = [] # Sensing time in days since the first sensing time, rounded to the nearest day

def localize_anc_json(file):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]
    try:
        s3 = boto3.resource('s3')
        s3.Object(bucket, file).download_file(file)
    except Exception as e:
        raise Exception("Exception while fetching CSLC ancillary file: %s. " % file + str(e))

    return file

def localize_disp_frame_burst_hist(file = DISP_FRAME_BURST_MAP_HIST):
    try:
        localize_anc_json(file)
    except:
        logging.warning(f"Could not download {file} from S3. Attempting to use local copy.")

    return process_disp_frame_burst_hist(file)

def localize_frame_geo_json(file = FRAME_GEO_SIMPLE_JSON):
    try:
        localize_anc_json(file)
    except:
        logging.warning(f"Could not download {file} from S3. Attempting to use local copy.")

    return process_frame_geo_json(file)

def _calculate_sensing_time_day_index(sensing_time, first_frame_time):
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

def sensing_time_day_index(sensing_time, frame_number, frame_to_bursts):
    ''' Return the day index of the sensing time relative to the first sensing time of the frame AND
    seconds since the first sensing time of the frame'''

    frame = frame_to_bursts[frame_number]
    return (_calculate_sensing_time_day_index(sensing_time, frame.sensing_datetimes[0]))

def process_disp_frame_burst_hist(file = DISP_FRAME_BURST_MAP_HIST):
    '''Process the disp frame burst map json file intended and return 3 dictionaries'''

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

def process_frame_geo_json(file = FRAME_GEO_SIMPLE_JSON):
    '''Process the frame-geometries-simple.geojson file as dictionary used for determining frame bounding box'''

    frame_geo_map = {}
    j = json.load(open(file))
    for feature in j["features"]:
        frame_geo_map[feature["id"]] = feature["geometry"]["coordinates"][0]

    return frame_geo_map

def _parse_cslc_file_name(native_id):
    dataset_json = datasets_json_util.DatasetsJson()
    cslc_granule_regex = dataset_json.get("L2_CSLC_S1")["match_pattern"]
    match_product_id = re.match(cslc_granule_regex, native_id)

    if not match_product_id:
        raise ValueError(f"CSLC native ID {native_id} could not be parsed with regex from datasets.json")

    return match_product_id

def determine_acquisition_cycle_cslc(acquisition_dts, frame_number, frame_to_bursts):
    sensing_time = dateutil.parser.isoparse(acquisition_dts[:-1]) #Take the timezone off because that doesn't exist in the database
    day_index, seconds = sensing_time_day_index(sensing_time, frame_number, frame_to_bursts)

    return day_index

def get_prev_day_indices(day_index, frame_number, frame_to_bursts, args, token, cmr, settings):
    '''Return the day indices of the previous acquisitions for the frame_number given the current day index'''

    frame = frame_to_bursts[frame_number]

    # If the day index is within the historical database it's much simpler
    # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
    try:
        # array.index returns 0-based index so add 1
        current_index = frame.sensing_datetime_days_index.index(day_index)
        return frame.sensing_datetime_days_index[:current_index]
    except ValueError:
        raise Exception("Currently non-historical processing mode is not supported for retrieving previous day indices.")

def determine_k_cycle(acquisition_dts, day_index, frame_number, frame_to_bursts, k, args, token, cmr, settings):
    '''Return where in the k-cycle this acquisition falls for the frame_number
    Must specify either acquisition_dts or day_index.
    Returns integer between 0 and k-1 where 0 means that it's at the start of the cycle'''

    if day_index is None:
        day_index = determine_acquisition_cycle_cslc(acquisition_dts, frame_number, frame_to_bursts)

    frame = frame_to_bursts[frame_number]

    # If the day index is within the historical database it's much simpler
    # ASSUMPTION: This is slow linear search but there will never be more than a couple hundred entries here so doesn't matter.
    # Clearly if we somehow end up with like 1000
    try:
        # array.index returns 0-based index so add 1
        index_number = frame.sensing_datetime_days_index.index(day_index) + 1 # note "index" is overloaded term here
        return index_number % k
    except ValueError:
        #TODO:
        # If not, we have to query CMR for all records for this frame, filter out ones that don't match the burst pattern,
        # and then determine the k-cycle index
        start_date = frame.sensing_datetimes[-1] + timedelta(minutes=30) # Make sure we are not counting this last sensing time cycle
        end_date = dateutil.parser.isoparse(acquisition_dts[:-1])

        # Add native-id condition in args
        l, native_id = build_cslc_native_ids(frame_number, frame_to_bursts)
        args.native_id = native_id

        query_timerange = DateTimeRange(start_date, end_date)
        granules = asyncio.run(async_query_cmr(args, token, cmr, settings, query_timerange, datetime.utcnow(), silent=True))
        print(granules)

        #raise Exception("Currently non-historical processing mode is not supported for determining k-cycle.")

    return -1

def parse_cslc_native_id(native_id, burst_to_frames, frame_to_bursts):
    match_product_id = _parse_cslc_file_name(native_id)

    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3
    frame_ids = burst_to_frames[burst_id]
    acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z

    # Acquisition cycle is frame-dependent and one CSLC burst can belong to at most two frames
    acquisition_cycles = {}
    for frame_id in frame_ids:
        acquisition_cycles[frame_id] = determine_acquisition_cycle_cslc(acquisition_dts, frame_id, frame_to_bursts)

    assert len(acquisition_cycles) <= 2  # A burst can belong to at most two frames. If it doesn't, we have a problem.

    return burst_id, acquisition_dts, acquisition_cycles, frame_ids

def parse_cslc_burst_id(native_id):
    match_product_id = _parse_cslc_file_name(native_id)

    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3

    return burst_id

def build_cslc_native_ids(frame, disp_burst_map):
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

def download_batch_id_hist(args, granule):
    """For historical processing mode, download_batch_id is a function of start_date, end_date, and frame_range
    Use underscore instead of other special characters and lower case so that it can be used in ES TERM search"""

    download_batch_id = args.start_date + "_" + args.end_date
    download_batch_id = download_batch_id + "_" + str(granule["frame_id"])
    download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

    return download_batch_id

def build_ccslc_m_index(burst_id, acquisition_cycle):
    return (burst_id + "_" + str(acquisition_cycle)).replace("-", "_").lower()
def download_batch_id_forward_reproc(granule):
    """For forward and re-processing modes, download_batch_id is a function of the granule's frame_id and acquisition_cycle"""

    download_batch_id = "f"+str(granule["frame_id"]) + "_a" + str(granule["acquisition_cycle"])
    download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

    return download_batch_id

def split_download_batch_id(download_batch_id):
    """Split the download_batch_id into frame_id and acquisition_cycle
    example: forward/reproc f7098_a145 -> 7098, 145
             historical     2023_10_01t00_00_00z_2023_10_25t00_00_00z_3601 -> 3601, None"""
    if download_batch_id.startswith("f"):
        frame_id, acquisition_cycle = download_batch_id.split("_")
        return int(frame_id[1:]), int(acquisition_cycle[1:])  # Remove the leading "f" and "a"
    else:
        frame_id = download_batch_id.split("_")[-1]
        return int(frame_id), None

def get_bounding_box_for_frame(frame_id, frame_geo_map):
    """Returns a bounding box for a given frame in the format of [xmin, ymin, xmax, ymax] in EPSG4326 coordinate system"""

    coords = frame_geo_map[frame_id]
    xmin = min([x for x, y in coords])
    ymin = min([y for x, y in coords])
    xmax = max([x for x, y in coords])
    ymax = max([y for x, y in coords])

    return [xmin, ymin, xmax, ymax]

