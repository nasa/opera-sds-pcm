#!/usr/bin/env python3
"""Purge CSLC catalog entries superseded by a reprocessed granule.

ASF republishes a burst under a new processing date when it reprocesses. The forward
path indexes one GRQ dataset per granule, so both versions sit in the catalog for the
same burst and sensing time.

Processing no longer depends on this being cleaned up -- the k-cycle evaluator selects
the newest granule per burst -- so this is HYGIENE, not a fix. It exists because the
superseded documents still inflate every count taken off the catalog: granule audits,
coverage reconciliation against CMR, and anything comparing catalog totals to DAAC
totals. Run it when those numbers need to be trustworthy.

Only the GRQ document is removed. The granule itself lives in the ASF DAAC bucket and
is not ours to delete; nothing is removed from S3.

    python tools/purge_superseded_cslc_granules.py                    # dry run, all frames
    python tools/purge_superseded_cslc_granules.py --frame-id 24726
    python tools/purge_superseded_cslc_granules.py --frame-id 24726 --apply
"""

import argparse
import collections
import json
import os
import re
import subprocess
import sys
import time

NETRC = "/export/home/hysdsops/.netrc-os"
ES = "https://localhost:9200"
# OPERA_L2_CSLC-S1_<burst>_<sensing>T..Z_<processing>T..Z_<sat>_<pol>_v<ver>
GRANULE_RE = re.compile(r"OPERA_L2_CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)")


def es(method, path, body=None):
    cmd = ["curl", "-s", "-k", "--netrc-file", NETRC, "-X" + method, ES + path]
    if body is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(body)]
    out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, timeout=300).stdout
    try:
        return json.loads(out)
    except ValueError:
        return {}


def bursts_for_frame(frame_id):
    from data_subscriber import cslc_utils
    f2b, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    frame = f2b.get(int(frame_id))
    if frame is None:
        return None
    return sorted(frame.burst_ids)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--frame-id", type=int, help="restrict to one frame's bursts")
    ap.add_argument("--apply", action="store_true", help="delete; otherwise dry run")
    ap.add_argument("--out", default=os.path.expanduser("~"),
                    help="where to write the rollback dump")
    args = ap.parse_args()

    must = [{"term": {"dataset.keyword": "L2_CSLC_S1"}}]
    if args.frame_id:
        bursts = bursts_for_frame(args.frame_id)
        if not bursts:
            print("frame %s is not in the deployed burst database" % args.frame_id)
            return 1
        must.append({"terms": {"metadata.burst_id.keyword": bursts}})
        print("frame %s: %d bursts" % (args.frame_id, len(bursts)))

    r = es("POST", "/grq/_search", {"query": {"bool": {"must": must}},
                                    "size": 10000, "_source": ["id"]})
    hits = r.get("hits", {}).get("hits", [])
    total = (r.get("hits", {}).get("total") or {}).get("value", len(hits))
    if total > len(hits):
        print("WARNING: %d of %d documents returned; re-run with a narrower --frame-id"
              % (len(hits), total))

    groups = collections.defaultdict(list)
    for h in hits:
        m = GRANULE_RE.search(h["_source"]["id"])
        if m:
            groups[(m.group(1), m.group(2))].append(
                (m.group(3), h["_id"], h["_index"], h["_source"]["id"]))

    superseded = []
    for key, entries in groups.items():
        if len(entries) < 2:
            continue
        entries.sort()                    # by processing date
        keep = entries[-1]
        for old in entries[:-1]:
            superseded.append({"burst": key[0], "sensing": key[1],
                               "purge_id": old[3], "purge_doc": old[1],
                               "index": old[2], "kept": keep[3]})

    print("documents scanned      : %d" % len(hits))
    print("burst/sensing groups   : %d" % len(groups))
    print("superseded to purge    : %d" % len(superseded))
    by_date = collections.Counter(s["sensing"][:8] for s in superseded)
    for date, n in sorted(by_date.items()):
        print("   %s  %d granule(s)" % (date, n))
    for s in superseded[:5]:
        print("\n   purge: %s" % s["purge_id"])
        print("   keep : %s" % s["kept"])
    if len(superseded) > 5:
        print("\n   ... and %d more" % (len(superseded) - 5))

    if not superseded:
        print("\nnothing to do.")
        return 0

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    dump = os.path.join(args.out, "superseded_cslc_%s.json" % stamp)
    with open(dump, "w") as f:
        json.dump(superseded, f, indent=2)
    print("\nrollback dump (ids only; granules remain in the DAAC): %s" % dump)

    if not args.apply:
        print("\nDRY RUN -- nothing deleted. Re-run with --apply.")
        return 0

    ok = 0
    for s in superseded:
        res = es("DELETE", "/%s/_doc/%s?refresh=true" % (s["index"], s["purge_doc"]))
        if res.get("result") == "deleted":
            ok += 1
        else:
            print("   FAILED %s -> %s" % (s["purge_id"], json.dumps(res)[:160]))
    print("\ndeleted %d/%d" % (ok, len(superseded)))
    return 0 if ok == len(superseded) else 1


if __name__ == "__main__":
    sys.exit(main())
