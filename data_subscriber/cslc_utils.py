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

def build_cslc_native_ids(frame, disp_burst_map):
    """Builds the native_id string for a given frame. The native_id string is used in the CMR query."""

    native_ids = disp_burst_map[frame].burst_ids
    return "OPERA_L2_CSLC-S1_" + "*&native-id[]=OPERA_L2_CSLC-S1_".join(native_ids) + "*"

def download_batch_id_hist(args):
    """For historical processing mode, download_batch_id is a function of start_date, end_date, and frame_range
    Use underscore instead of other special characters and lower case so that it can be used in ES TERM search"""

    download_batch_id = args.start_date + "_" + args.end_date
    download_batch_id = download_batch_id + "_" + args.frame_range.split(",")[0]
    download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

    return download_batch_id

def download_batch_id_forward_reproc(granule):
    """For forward and re-processing modes, download_batch_id is a function of the granule's frame_id and acquisition_cycle"""

    download_batch_id = str(granule["frame_id"]) + "_" + str(granule["acquisition_cycle"])
    download_batch_id = download_batch_id.replace("-", "_").replace(":", "_").lower()

    return download_batch_id

