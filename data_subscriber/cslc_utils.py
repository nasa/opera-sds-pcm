import json
import re
from collections import defaultdict
from types import SimpleNamespace

import boto3
from pyproj import Transformer

from data_subscriber.url import determine_acquisition_cycle
from util import datasets_json_util
from util.conf_util import SettingsConf

DISP_FRAME_BURST_MAP_JSON = 'opera-s1-disp-frame-to-burst.json'
DISP_FRAME_BURST_MAP_HIST = 'opera-disp-s1-constent-burst-ids.json'

_CSLC_EPOCH_DATE = "20090222T000000Z"

# Seems a bit silly but need this class to match the interface with non-hist version
class _HistBursts(object):
    def __init__(self):
        self.burst_ids = []

def localize_anc_json(file):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]
    try:
        s3 = boto3.resource('s3')
        s3.Object(bucket, file).download_file(file)
    except Exception as e:
        raise Exception("Exception while fetching CSLC ancillary file: %s. " % file + str(e))

    return file

def localize_disp_frame_burst_json(file = DISP_FRAME_BURST_MAP_JSON):
    localize_anc_json(file)
    return process_disp_frame_burst_json(file)

def localize_disp_frame_burst_hist(file = DISP_FRAME_BURST_MAP_HIST):
    localize_anc_json(file)
    return process_disp_frame_burst_hist(file)

def process_disp_frame_burst_hist(file = DISP_FRAME_BURST_MAP_HIST):
    '''Process the disp frame burst map json file intended for historical processing only and return the data as a dictionary'''

    j = json.load(open(file))
    frame_to_bursts = defaultdict(_HistBursts)

    for frame in j:
        b = frame_to_bursts[int(frame)].burst_ids
        for burst in j[frame]:
            burst = burst.upper().replace("_", "-")
            b.append(burst)

    return frame_to_bursts

def process_disp_frame_burst_json(file = DISP_FRAME_BURST_MAP_JSON):

    j = json.load(open(file))

    metadata = j["metadata"]
    version = metadata["version"]
    data = j["data"]
    frame_data = {}

    frame_ids = []
    for f in data:
        frame_ids.append(f)

    # Note that we are using integer as the dict key instead of the original string so that it can be sorted
    # more predictably
    for frame_id in frame_ids:
        items = SimpleNamespace(**(data[frame_id]))

        # Convert burst ids to upper case and replace "_" with "-" because this is how it shows up in granule id
        for i in range(len(items.burst_ids)):
            items.burst_ids[i] = items.burst_ids[i].upper().replace("_", "-")

        frame_data[int(frame_id)] = items

    sorted_frame_data = dict(sorted(frame_data.items()))

    # Now create a map that maps burst id to frame id
    burst_to_frame = defaultdict(list)
    for frame_id in sorted_frame_data:
        for burst_id in sorted_frame_data[frame_id].burst_ids:
            burst_to_frame[burst_id].append(frame_id)
            assert len(burst_to_frame[burst_id]) <= 2  # A burst can belong to at most two frames

    return sorted_frame_data, burst_to_frame, metadata, version

def _parse_cslc_file_name(native_id):
    dataset_json = datasets_json_util.DatasetsJson()
    cslc_granule_regex = dataset_json.get("L2_CSLC_S1")["match_pattern"]
    match_product_id = re.match(cslc_granule_regex, native_id)

    if not match_product_id:
        raise ValueError(f"CSLC native ID {native_id} could not be parsed with regex from datasets.json")

    return match_product_id

def parse_cslc_native_id(native_id, burst_to_frame):
    match_product_id = _parse_cslc_file_name(native_id)

    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3
    acquisition_dts = match_product_id.group("acquisition_ts")  # e.g. 20210705T183117Z

    # Determine acquisition cycle, we use an older date for epoch because we process historical data for CSLC/DISP-S1
    acquisition_cycle = determine_acquisition_cycle(burst_id, acquisition_dts, native_id, _CSLC_EPOCH_DATE)

    frame_ids = burst_to_frame[burst_id]

    return burst_id, acquisition_dts, acquisition_cycle, frame_ids

def parse_cslc_burst_id(native_id):
    match_product_id = _parse_cslc_file_name(native_id)

    burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3

    return burst_id

def build_cslc_native_ids(frame, disp_burst_map):
    """Builds the native_id string for a given frame. The native_id string is used in the CMR query."""

    native_ids = disp_burst_map[frame].burst_ids
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

def get_bounding_box_for_frame(frame):
    """Returns a bounding box for a given frame in the format of [xmin, ymin, xmax, ymax]"""

    proj_from = f'EPSG:{frame.epsg}'
    transformer = Transformer.from_crs(proj_from, "EPSG:4326")

    xmin, ymin = transformer.transform(xx=frame.xmin, yy=frame.ymin)
    xmax, ymax = transformer.transform(xx=frame.xmax, yy=frame.ymax)

    return [xmin, ymin, xmax, ymax]

