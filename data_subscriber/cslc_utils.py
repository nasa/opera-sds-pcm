import json
from types import SimpleNamespace
import boto3
from util.conf_util import SettingsConf
from collections import defaultdict

DISP_FRAME_BURST_MAP_JSON = 'opera-s1-disp-frame-to-burst.json'

def localize_disp_frame_burst_json(file = DISP_FRAME_BURST_MAP_JSON):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]
    try:
        s3 = boto3.resource('s3')
        s3.Object(bucket, file).download_file(file)
    except Exception as e:
        raise Exception("Exception while fetching geojson file: %s. " % file + str(e))

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
        frame_data[int(frame_id)] = SimpleNamespace(**(data[frame_id]))

    sorted_frame_data = dict(sorted(frame_data.items()))

    # Now create a map that maps burst id to frame id
    burst_to_frame = defaultdict(list)
    for frame_id in sorted_frame_data:
        for burst_id in sorted_frame_data[frame_id].burst_ids:
            burst_to_frame[burst_id].append(frame_id)

    return sorted_frame_data, burst_to_frame, metadata, version

def build_cslc_native_ids(frame, disp_burst_map):

    native_ids = []
    for id in disp_burst_map[frame].burst_ids:
        native_ids.append(id.upper().replace("_", "-"))

    return "OPERA_L2_CSLC-S1_" + "*&native-id[]=OPERA_L2_CSLC-S1_".join(native_ids) + "*"

