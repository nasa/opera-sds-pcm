#!/usr/bin/env python3

import logging
import json
import sys
from datetime import datetime
import argparse

from data_subscriber.cslc.disp_s1_phases import (PhaseValidationError, parse_sensing_time_list,
                                                 segment_phases)

''' Tool to truncate the DISP S1 burst database sensing_time_list to a specific datetime.
    Writes out the new file with .mod added to the end of the file name

    Processing-mode-annotated databases store sensing_time_list as a mapping of sensing time ->
    mode label instead of a list. Those labels are preserved: the surviving entries are written
    back as a mapping and the resulting label sequence is re-validated against the batch size the
    labels were generated for. A truncation that would leave a frame with an invalid phase layout
    (typically a historical run that is no longer a whole number of k-sized ministacks) aborts the
    run without writing anything, because a database that quarantines frames at load time is worse
    than a refusal.'''

TRUNCATION_DATETIME="2016-07-01T00:00:00"

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

def phase_error(labels, batch_size):
    '''Return why this label sequence is invalid for batch_size, or None when it is valid'''

    try:
        segment_phases(labels, batch_size)
    except PhaseValidationError as e:
        return str(e)
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    return None

parser = argparse.ArgumentParser()
parser.add_argument("file", help="The DISP S1 burst database file to truncate")

truncation_datetime = datetime.fromisoformat(TRUNCATION_DATETIME)

args = parser.parse_args()
j = json.load(open(args.file))

# Published databases wrap the frames in {"metadata": ..., "data": ...}. Older ones are just the
# frames. Keep whichever shape came in so the metadata survives the round trip.
if "data" in j:
    db_metadata = j.get("metadata", {})
    frames = j["data"]
else:
    logger.warning("No 'data' key found in the json file. Treating it as an older format without metadata.")
    db_metadata = {}
    frames = j

batch_size = (db_metadata.get("processing_mode_params") or {}).get("batch_size")

refusals = []

for frame in frames:
    sensing_time_list = frames[frame]["sensing_time_list"]

    # The annotated database maps sensing time -> mode label; the un-annotated one is a plain list.
    # Iterating yields the sensing times either way, but only the list may be mutated positionally.
    annotated = isinstance(sensing_time_list, dict)

    kept = []
    for sensing_time in sensing_time_list:
        if datetime.fromisoformat(sensing_time) < truncation_datetime:
            print(f"Truncating {frame} {sensing_time}")
        else:
            kept.append(sensing_time)

    if len(kept) == len(sensing_time_list):
        continue  # nothing to truncate for this frame, leave the entry exactly as it came in

    if not annotated:
        frames[frame]["sensing_time_list"] = kept
        continue

    if not batch_size:
        refusals.append(f"frame {frame}: sensing_time_list carries processing-mode labels but the database has no "
                        f"metadata.processing_mode_params.batch_size, so the truncated labels cannot be validated")
        continue

    new_sensing_time_list = {sensing_time: sensing_time_list[sensing_time] for sensing_time in kept}
    _, new_labels = parse_sensing_time_list(new_sensing_time_list)
    new_error = phase_error(new_labels, batch_size)

    if new_error:
        _, old_labels = parse_sensing_time_list(sensing_time_list)
        old_error = phase_error(old_labels, batch_size)
        if old_error is None:
            refusals.append(f"frame {frame}: truncation would leave an invalid processing-mode layout: {new_error}")
            continue
        # The frame was already rejected by the phase model before we touched it, so the truncation is
        # not what broke it. Truncate anyway, but say so loudly.
        logger.warning(f"Frame {frame} processing-mode labels were already invalid before truncation "
                       f"({old_error}); truncating anyway, still invalid after: {new_error}")

    frames[frame]["sensing_time_list"] = new_sensing_time_list

if refusals:
    logger.error("Refusing to write a truncated database that would corrupt processing-mode labels:")
    for refusal in refusals:
        logger.error(f"  {refusal}")
    logger.error("Re-generate the processing-mode labels for the truncated timeline instead of truncating an "
                 "annotated database.")
    sys.exit(1)

new_file = args.file + ".mod"
with open(new_file, "w") as f:
    json.dump(j, f, indent=4)
    print(f"Truncated file written to {new_file}")
