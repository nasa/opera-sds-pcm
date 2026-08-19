#!/usr/bin/env python3
"""Reconstruct frame_states from the compressed CSLC catalog when batch_proc is gone.

Preferred source for a new cluster is the restored batch_proc index
(tools/derive_phased_frame_states.py). Use this only when that index was not
snapshotted or not restored.

A compressed CSLC is published at the LAST date of a completed historical k-set, so
the cursor is the position just past the newest boundary that frame has. That is a
CONSERVATIVE reconstruction: forward dates do not produce boundaries, so a frame that
had progressed into a forward block is rewound to the end of its last historical
k-set and those forward dates are re-submitted. It never skips work.

    python reconstruct_frame_states.py 25278,33065,24726 [k]
"""

import json
import re
import subprocess
import sys

from data_subscriber import cslc_utils

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200/grq/_search?size=2000"
# ..._<sensing>T..Z_<lineage_start>T..Z_<boundary>T..Z_<created>T..Z_
CCSLC_RE = re.compile(r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_")


def boundaries_for(frame_id):
    body = json.dumps({
        "query": {"bool": {"must": [
            {"term": {"dataset.keyword": "L2_CSLC_S1_COMPRESSED"}},
            {"term": {"metadata.frame_id": frame_id}}]}},
        "_source": ["id"]})
    out = subprocess.run(["curl", "-s", "-k", "--netrc-file", NETRC, "-XPOST", ES,
                          "-H", "Content-Type: application/json", "-d", body],
                         capture_output=True, text=True, timeout=180)
    hits = json.loads(out.stdout).get("hits", {}).get("hits", [])
    dates = set()
    for h in hits:
        m = CCSLC_RE.search(h["_source"]["id"])
        if m:
            dates.add(m.group(3))
    return sorted(dates)


def main():
    frames = [int(f) for f in sys.argv[1].split(",")]
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 15

    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    states = {}

    print("# reconstructed from the compressed CSLC catalog (conservative)")
    for fid in frames:
        fr = frame_to_bursts.get(fid)
        if fr is None:
            print("#   frame %-7s not in the burst database -- skipped" % fid)
            continue

        bounds = boundaries_for(fid)
        if not bounds:
            states[str(fid)] = 0
            print("#   frame %-7s cursor 0     no compressed CSLC -- never processed" % fid)
            continue

        by_date = {d.strftime("%Y%m%d"): i for i, d in enumerate(fr.sensing_datetimes)}
        idx = [by_date[b] for b in bounds if b in by_date]
        if not idx:
            states[str(fid)] = 0
            print("#   frame %-7s cursor 0     boundaries %s not in this database vintage"
                  % (fid, bounds))
            continue

        cursor = max(idx) + 1
        states[str(fid)] = cursor
        note = ""
        phases = getattr(fr, "phases", None)
        if phases:
            for p in phases:
                if p.start_pos <= cursor < p.end_pos:
                    note = " -> %s" % p.label
                    break
        print("#   frame %-7s cursor %-5d newest boundary %s (position %d)%s"
              % (fid, cursor, max(bounds, key=lambda b: by_date.get(b, -1)), max(idx), note))

    print("#")
    print("# Conservative: forward dates produce no boundary, so a frame that had moved into a")
    print("# forward block is rewound to the end of its last historical k-set. Those forward")
    print("# dates get re-submitted. Nothing is skipped.")
    print()
    print(json.dumps({"frame_states": states}, indent=4))
    return 0


if __name__ == "__main__":
    sys.exit(main())
