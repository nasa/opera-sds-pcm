#!/usr/bin/env python3

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime

import backoff

from data_subscriber import cslc_utils
from data_subscriber.cmr import get_cmr_token
from data_subscriber.cslc.cslc_query import CslcCmrQuery
from data_subscriber.cslc.disp_s1_phases import (PhaseValidationError, parse_sensing_time_list,
                                                 segment_phases)
from data_subscriber.parser import create_parser
from data_subscriber.query import DateTimeRange
from util.conf_util import SettingsConf

''' Tool to update the DISP S1 burst database sensing_time_list with latest data from CMR.
    Writes out the new file with .mod added to the end of the file name

    Processing-mode-annotated databases store sensing_time_list as a mapping of sensing time ->
    mode label instead of a list. There is no way to label a sensing time that CMR reports but the
    labeler never saw, so this tool will only ever drop dates from an annotated frame; a frame that
    gains dates aborts the run. Use --drop-processing-modes to write an explicitly un-annotated
    database instead (the labeler then has to be re-run over it).'''

@backoff.on_exception(backoff.expo, Exception, max_tries=15)
def query_cmr_by_frame_and_dates_backoff(cslc_query, subs_args, token, cmr, settings, now, timerange, verbose=True):
    frame_id = int(subs_args.frame_id)
    return cslc_query.query_cmr_by_frame_and_dates(frame_id, subs_args, token, cmr, settings, now, timerange, verbose)

def phase_error(labels, batch_size):
    '''Return why this label sequence is invalid for batch_size, or None when it is valid'''

    try:
        segment_phases(labels, batch_size)
    except PhaseValidationError as e:
        return str(e)
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    return None

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--only-frames", help="Only update the sensing_time_list for these frames",
                    nargs="+", type=str)
parser.add_argument("--db-file", dest="db_file", help="Specify the DISP-S1 database json file \
on the local file system instead of using the standard one in S3 ancillary", required=False)
parser.add_argument("--drop-processing-modes", dest="drop_processing_modes", action="store_true",
                    help="Write the updated sensing_time_list as a plain list, discarding the processing-mode \
labels of an annotated database. The output is no longer an annotated database and the labeler must be re-run \
over it before it can be used for phased processing.")
prog_args = parser.parse_args()

if prog_args.db_file:
    logger.info(f"Using local DISP-S1 database json file: {prog_args.db_file}")
    disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.process_disp_frame_burst_hist(prog_args.db_file)
    db_file_name = prog_args.db_file
else:
    disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist()
    db_file_name=cslc_utils.DEFAULT_DISP_FRAME_BURST_DB_NAME

j = json.load(open(db_file_name))

# Published databases wrap the frames in {"metadata": ..., "data": ...}. Older ones are just the
# frames. Keep whichever shape came in so the metadata survives the round trip.
if "data" in j:
    db_metadata = j.get("metadata", {})
    frames = j["data"]
else:
    logger.warning("No 'data' key found in the json file. Treating it as an older format without metadata.")
    db_metadata = None
    frames = j

batch_size = ((db_metadata or {}).get("processing_mode_params") or {}).get("batch_size")

if any(isinstance(frames[frame]["sensing_time_list"], dict) for frame in frames):
    if prog_args.drop_processing_modes:
        logger.warning("This is a processing-mode-annotated database and --drop-processing-modes was given: the "
                       "output will be written WITHOUT mode labels. Re-run the labeler over it before using it "
                       "for phased processing.")
    else:
        logger.info("This is a processing-mode-annotated database. Mode labels will be preserved, and any frame "
                    "that CMR reports new sensing times for will abort the run because those times cannot be "
                    "labeled. Pass --drop-processing-modes to write an un-annotated database instead.")

if prog_args.only_frames:
    logger.info("Only updating sensing_time_list for the following frames:")
    logger.info(prog_args.only_frames)

# Query the CMR for the frame_id between the first and the last sensing datetime
subs_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--k=1", "--m=1", "--use-temporal", "--processing-mode=forward"])
settings = SettingsConf().cfg
cmr, token, username, password, edl = get_cmr_token(subs_args.endpoint, settings, get_token=False)
cslc_cmr_query = CslcCmrQuery(subs_args, token, None, cmr, None, settings)

now = datetime.now()
start_date = "2016-07-01T00:00:00Z" # This is the start of DISP-S1 processing time for the OPERA program
end_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
timerange = DateTimeRange(start_date, end_date)

refusals = []
dropped_modes = False

for frame in frames:
    if  prog_args.only_frames is not None and frame not in prog_args.only_frames:
        continue
    new_sensing_time_list = []
    logger.info(f"Updating {frame=}")
    subs_args.frame_id = frame
    all_granules = query_cmr_by_frame_and_dates_backoff(cslc_cmr_query, subs_args, token, cmr, settings, now, timerange, verbose=False)

    # Group them by acquisition cycle
    acq_cycles = defaultdict(set)
    acq_ts_map = defaultdict(list)
    for g in all_granules:
        if '_VV_' not in g["granule_id"]: # We only want to process VV polarization data
            continue
        acq_cycles[g["acquisition_cycle"]].add(g["burst_id"])
        acq_ts_map[g["acquisition_cycle"]].append(g["acquisition_ts"])

    bursts_we_want = disp_burst_map[int(frame)].burst_ids

    sorted_acq_cycles = sorted(acq_cycles.keys())
    for acq_cycle in sorted_acq_cycles:
        if acq_cycles[acq_cycle].issuperset(bursts_we_want):
            newtime = acq_ts_map[acq_cycle][0].strftime("%Y-%m-%dT%H:%M:%S")
            new_sensing_time_list.append(newtime) # we just need one representative datetime for each acq cycle

    old_time_list = frames[frame]["sensing_time_list"]
    print(f"{len(old_time_list)} in Old sensing_time_list for {frame=}")
    print(f"{list(old_time_list)}")
    print(f"{len(new_sensing_time_list)} in New sensing_time_list for {frame=}")
    print(f"{new_sensing_time_list}")

    # The annotated database maps sensing time -> mode label; the un-annotated one is a plain list.
    annotated = isinstance(old_time_list, dict)

    if not annotated or prog_args.drop_processing_modes:
        dropped_modes = dropped_modes or annotated
        frames[frame]["sensing_time_list"] = new_sensing_time_list
        continue

    new_sensing_times = set(new_sensing_time_list)
    added = [sensing_time for sensing_time in new_sensing_time_list if sensing_time not in old_time_list]
    if added:
        refusals.append(f"frame {frame}: CMR reports {len(added)} sensing time(s) that carry no processing-mode "
                        f"label (first {added[0]}, last {added[-1]}); an annotated database cannot be extended "
                        f"here")
        continue

    kept = [sensing_time for sensing_time in old_time_list if sensing_time in new_sensing_times]
    if len(kept) == len(old_time_list):
        continue  # nothing changed for this frame, leave the entry exactly as it came in

    if not batch_size:
        refusals.append(f"frame {frame}: sensing_time_list carries processing-mode labels but the database has no "
                        f"metadata.processing_mode_params.batch_size, so the updated labels cannot be validated")
        continue

    updated_time_list = {sensing_time: old_time_list[sensing_time] for sensing_time in kept}
    _, new_labels = parse_sensing_time_list(updated_time_list)
    new_error = phase_error(new_labels, batch_size)

    if new_error:
        _, old_labels = parse_sensing_time_list(old_time_list)
        old_error = phase_error(old_labels, batch_size)
        if old_error is None:
            refusals.append(f"frame {frame}: dropping the {len(old_time_list) - len(kept)} sensing time(s) CMR no "
                            f"longer reports would leave an invalid processing-mode layout: {new_error}")
            continue
        # The frame was already rejected by the phase model before we touched it, so this update is not
        # what broke it. Apply it anyway, but say so loudly.
        logger.warning(f"Frame {frame} processing-mode labels were already invalid before the update "
                       f"({old_error}); updating anyway, still invalid after: {new_error}")

    frames[frame]["sensing_time_list"] = updated_time_list

if refusals:
    logger.error("Refusing to write an updated database that would corrupt processing-mode labels:")
    for refusal in refusals:
        logger.error(f"  {refusal}")
    logger.error("Re-run the labeler over the updated timeline, or pass --drop-processing-modes to write an "
                 "explicitly un-annotated database.")
    sys.exit(1)

if dropped_modes and db_metadata is not None:
    # Do not leave labeling metadata on a file that no longer has labels
    db_metadata.pop("processing_mode_params", None)

new_file = db_file_name + ".mod"
with open(new_file, "w") as f:
    json.dump(j, f, indent=4)
    print(f"Updated file written to {new_file}")
