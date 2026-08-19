#!/usr/bin/env python3
"""Derive a frame_states map for a new campaign from the existing batch_proc index.

For each requested frame, find the furthest cursor any previous batch proc reached and
use that; frames no previous proc covered start at 0. Cursors carried over from an
un-phased campaign are then checked for phase alignment, because a legacy cursor is a
multiple of k in ABSOLUTE terms and that only coincides with phase alignment when the
phase itself starts on a multiple of k.

    python derive_frame_states.py 25278,33065,24726 [k]

Prints a frame_states block ready to paste into the batch proc.
"""

import json
import subprocess
import sys

from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import phase_for_position

ES = "https://localhost:9200/batch_proc/_search?size=200"


def fetch_procs():
    out = subprocess.run(
        ["curl", "-s", "-k", "--netrc-file", "/export/home/hysdsops/.netrc-os", "-XPOST", ES,
         "-H", "Content-Type: application/json", "-d", '{"query":{"match_all":{}}}'],
        capture_output=True, text=True, timeout=120)
    return json.loads(out.stdout)


def main():
    frames = [int(f) for f in sys.argv[1].split(",")]
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 15

    docs = fetch_procs().get("hits", {}).get("hits", [])
    best, source = {}, {}
    for h in docs:
        s = h["_source"]
        if s.get("job_type") != "cslc_query_hist":
            continue
        for fid, cur in (s.get("frame_states") or {}).items():
            fid, cur = int(fid), int(cur)
            if fid in frames and cur > best.get(fid, -1):
                best[fid] = cur
                source[fid] = "%s (%s)" % (str(s.get("label"))[:40], h["_id"])

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()

    print("# derived from the batch_proc index")
    states, problems = {}, []
    for fid in frames:
        cur = best.get(fid, 0)
        note = source.get(fid, "no previous batch proc -- starting at 0")
        fr = frame_to_bursts.get(fid)
        phases = getattr(fr, "phases", None) if fr is not None else None

        if phases and cur < len(fr.sensing_datetimes):
            ph = phase_for_position(phases, cur)
            if ph.label.startswith("historical_") and (cur - ph.start_pos) % k:
                aligned = [p for p in range(ph.start_pos, ph.end_pos, k)]
                back = max(p for p in aligned if p <= cur) if any(p <= cur for p in aligned) else ph.start_pos
                problems.append("#   frame %-7s cursor %d is inside %s [%d,%d) but NOT phase-aligned; "
                                "rewound to %d" % (fid, cur, ph.label, ph.start_pos, ph.end_pos, back))
                cur = back
            note += "  -> %s" % ph.label
        states[str(fid)] = cur
        print("#   frame %-7s cursor %-5d %s" % (fid, cur, note))

    if problems:
        print("#")
        print("# ADJUSTED -- legacy cursors that were not phase-aligned:")
        for p in problems:
            print(p)
        print("#   Rewinding re-runs that k-set. That is intentional: a misaligned cursor")
        print("#   quarantines the frame with a message that blames the phase, not the cursor.")

    print()
    print(json.dumps({"frame_states": states}, indent=4))
    return 0


if __name__ == "__main__":
    sys.exit(main())
