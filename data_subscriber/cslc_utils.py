from collections import defaultdict
from types import SimpleNamespace
import json
import boto3
from pyproj import Transformer
from util.conf_util import SettingsConf
import re



DISP_FRAME_BURST_MAP_JSON = 'opera-s1-disp-frame-to-burst.json'

def localize_disp_frame_burst_json(file = DISP_FRAME_BURST_MAP_JSON):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]
    try:
        s3 = boto3.resource('s3')
        s3.Object(bucket, file).download_file(file)
    except Exception as e:
        raise Exception("Exception while fetching CSLC burst map file: %s. " % file + str(e))

    return process_disp_frame_burst_json(file)

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

def parse_cslc_native_id(native_id, disp_burst_map):
    pass

def build_cslc_native_ids(frame, disp_burst_map):
    """Builds the native_id string for a given frame. The native_id string is used in the CMR query."""

    native_ids = disp_burst_map[frame].burst_ids
    return "OPERA_L2_CSLC-S1_" + "*&native-id[]=OPERA_L2_CSLC-S1_".join(native_ids) + "*"

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

