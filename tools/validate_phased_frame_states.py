#!/usr/bin/env python3
"""Validate a proposed frame_states map for a phased batch proc.

Run on Mozart from ~/mozart/ops/opera-pcm.  A cursor is an ABSOLUTE index into the
frame's sensing list, but inside a historical phase it must be PHASE-RELATIVE aligned:

    (cursor - phase.start_pos) % k == 0

A misaligned cursor does not fail cleanly -- the walk submits an offset k-set, runs past
the end of the phase, and quarantines the frame with a message that blames the phase
length instead of the cursor.

    python validate_frame_states.py batch_proc_region3a.json

Reads frame_states and k straight out of the batch proc file, so run it on the file you
are about to hand to pcm_batch.py create.
"""

import json
import sys

from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import phase_for_position
from data_subscriber.cslc_utils import expand_batch_proc_frames

K = 15


def main():
    proc = json.load(open(sys.argv[1]))
    k = int(proc.get("k", K))
    proposed = proc.get("frame_states")

    if proposed is None:
        frames = expand_batch_proc_frames(proc.get("frames", []))
        print("No frame_states in this batch proc -- every frame would start at 0.")
        print("Validating that default for %d frame(s).\n" % len(frames))
        proposed = {str(f): 0 for f in frames}

    if not proc.get("phased"):
        print('NOTE: "phased" is not true in this file, so the walk would use the legacy '
              'absolute grid and frame_states alignment below would not apply.\n')

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    ok = True

    for frame_str, cursor in sorted(proposed.items(), key=lambda x: int(x[0])):
        fid = int(frame_str)
        frame = frame_to_bursts.get(fid)

        if frame is None:
            print("FAIL  frame %-7s not in the burst database" % fid); ok = False; continue
        if getattr(frame, "phases", None) is None:
            print("FAIL  frame %-7s has no phases: %s"
                  % (fid, getattr(frame, "phase_error", "master switch off?"))); ok = False; continue

        n = len(frame.sensing_datetimes)
        if cursor == n:
            print("ok    frame %-7s cursor %-4d = end of frame (already complete)" % (fid, cursor))
            continue
        if not (0 <= cursor < n):
            print("FAIL  frame %-7s cursor %d outside [0, %d)" % (fid, cursor, n)); ok = False; continue

        try:
            ph = phase_for_position(frame.phases, cursor)
        except Exception as e:
            print("FAIL  frame %-7s cursor %d: %s" % (fid, cursor, e)); ok = False; continue

        if ph.label.startswith("historical_"):
            off = (cursor - ph.start_pos) % k
            if off:
                aligned = [p for p in range(ph.start_pos, ph.end_pos, k)]
                print("FAIL  frame %-7s cursor %-4d is inside %s [%d,%d) but is NOT phase-aligned "
                      "(offset %d). Use one of %s"
                      % (fid, cursor, ph.label, ph.start_pos, ph.end_pos, off, aligned))
                ok = False
            else:
                kset = (cursor - ph.start_pos) // k + 1
                total = ph.length // k
                print("ok    frame %-7s cursor %-4d -> %s, k-set %d of %d"
                      % (fid, cursor, ph.label, kset, total))
        else:
            print("ok    frame %-7s cursor %-4d -> %s (any position is valid here)"
                  % (fid, cursor, ph.label))

    print()
    print("RESULT:", "all cursors valid" if ok else "INVALID -- fix the failures above before creating the batch proc")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
