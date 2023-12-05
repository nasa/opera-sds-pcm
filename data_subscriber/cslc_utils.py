import json
from types import SimpleNamespace
import boto3
from util.conf_util import SettingsConf

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

    return sorted_frame_data, metadata, version

def build_cslc_native_ids(frame_start, frame_end, disp_burst_map):
    native_ids = []

    for f in range(frame_start, frame_end):
        frame = disp_burst_map[f]

        #TODO: Figure out if we want to perform geo filtering
        #TODO: CSLC should have only been created for North America so not sure if this filtering makes sense
        #if frame.is_north_america == True:
        for id in frame.burst_ids:
            native_ids.append(id.upper().replace("_", "-"))
        #else:
        #    logging.debug("Frame number %s is not within North America. Skipping." % f)

    return native_ids
def expand_clsc_frames(args, disp_burst_map):
    frame_start = int(args.frame_range.split(",")[0])
    frame_end = int(args.frame_range.split(",")[1])
    native_ids = build_cslc_native_ids(frame_start, frame_end, disp_burst_map)

    if len(native_ids) == 0:
        return False

    args.native_id = "OPERA_L2_CSLC-S1_"+"*&native-id[]=OPERA_L2_CSLC-S1_".join(native_ids) + "*"