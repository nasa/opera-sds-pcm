#!/usr/bin/env python3
"""Promote burst metadata to the top level of CSLC datasets published without it.

CSLC-S1 PGE datasets published before product2dataset promoted it carry their burst
id, acquisition time, sensor and polarization only on each published file
(metadata.Files[*]), and have no dataset starttime. Catalog-ingest datasets and newer
PGE datasets carry them at the top level.

Nothing depends on this tool: the DISP-S1 cycle evaluator reads either shape. It makes
the older documents uniform with the rest, so that anything querying metadata.burst_id
or starttime -- purge_superseded_cslc_granules.py --frame-id, audits, ad hoc queries --
sees every CSLC.

Documents are rewritten in place with _update_by_query. Nothing is republished or
deleted, and a document that already has metadata.burst_id is not touched, so the tool
is safe to re-run. Values are copied from the first file entry; they come from the
product filename and are the same on every file of a dataset.

Run it on mozart from the PCM checkout:

    cd ~/mozart/ops/opera-pcm
    PYTHONPATH=$PWD python tools/backfill_cslc_dataset_metadata.py                  # dry run
    PYTHONPATH=$PWD python tools/backfill_cslc_dataset_metadata.py --frame-id 15422 # one frame
    PYTHONPATH=$PWD python tools/backfill_cslc_dataset_metadata.py --apply
    PYTHONPATH=$PWD python tools/backfill_cslc_dataset_metadata.py --dataset-type L2_CSLC_S1_STATIC --apply
"""

import argparse
import json
import os
import subprocess
import sys
import time

ES = "https://localhost:9200"
NETRC = os.path.expanduser("~/.netrc-os")

INDEX_PATTERNS = {
    "L2_CSLC_S1": "grq_*_l2_cslc_s1-*",
    "L2_CSLC_S1_STATIC": "grq_*_l2_cslc_s1_static*",
}

# The same keys product2dataset promotes for each product type.
PROMOTED_KEYS = {
    "L2_CSLC_S1": ["burst_id", "acquisition_ts", "sensor", "pol"],
    "L2_CSLC_S1_STATIC": ["burst_id", "validity_ts", "sensor"],
}

# CSLCs are selected by sensing date through the dataset starttime; static layers are not.
SETS_TIME_RANGE = {"L2_CSLC_S1": True, "L2_CSLC_S1_STATIC": False}

PAINLESS = """
def md = ctx._source.metadata;
if (md == null) { ctx.op = 'noop'; return; }
def files = md.Files;
if (files == null || !(files instanceof List) || files.isEmpty()) { ctx.op = 'noop'; return; }
def first = files.get(0);
boolean changed = false;
for (def key : params.keys) {
  if (md.get(key) == null && first.get(key) != null) { md.put(key, first.get(key)); changed = true; }
}
if (params.set_time_range && ctx._source.get('starttime') == null && first.get('acquisition_ts') != null) {
  ctx._source.put('starttime', first.get('acquisition_ts'));
  ctx._source.put('endtime', first.get('acquisition_ts'));
  changed = true;
}
if (!changed) { ctx.op = 'noop'; }
"""


def build_query(dataset_type, burst_ids=None):
    """Documents of dataset_type that lack a top-level burst id, optionally for some bursts."""
    must = [{"term": {"dataset_type.keyword": dataset_type}}]
    if burst_ids:
        must.append({"terms": {"metadata.Files.burst_id.keyword": sorted(burst_ids)}})
    return {"bool": {"must": must, "must_not": [{"exists": {"field": "metadata.burst_id"}}]}}


def build_script(dataset_type):
    return {
        "lang": "painless",
        "source": PAINLESS,
        "params": {
            "keys": PROMOTED_KEYS[dataset_type],
            "set_time_range": SETS_TIME_RANGE[dataset_type],
        },
    }


def es(method, path, body=None):
    cmd = ["curl", "-s", "-k", "--netrc-file", NETRC, "-X" + method, ES + path]
    if body is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(body)]
    out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, timeout=300).stdout
    try:
        response = json.loads(out)
    except ValueError:
        raise RuntimeError(f"{method} {path}: not a JSON response: {out[:300]}")
    if isinstance(response, dict) and response.get("error"):
        raise RuntimeError(f"{method} {path}: {json.dumps(response['error'])[:500]}")
    return response


def count_by_index(index, query):
    response = es("POST", f"/{index}/_search", {
        "size": 1, "track_total_hits": True, "query": query, "_source": ["id"],
        "aggs": {"by_index": {"terms": {"field": "_index", "size": 100}}},
    })
    total = response["hits"]["total"]["value"]
    buckets = {b["key"]: b["doc_count"] for b in response["aggregations"]["by_index"]["buckets"]}
    sample = response["hits"]["hits"][0]["_id"] if response["hits"]["hits"] else None
    return total, buckets, sample


def frame_burst_ids(frame_id):
    from data_subscriber import cslc_utils
    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
    frame = frame_to_bursts.get(int(frame_id))
    return sorted(frame.burst_ids) if frame is not None else None


def wait_for_task(task_id, poll_secs):
    while True:
        response = es("GET", f"/_tasks/{task_id}")
        if response.get("completed"):
            return response.get("response", {}), response.get("error")
        status = response.get("task", {}).get("status", {})
        print(f"  ... {status.get('updated', 0) + status.get('noops', 0)}"
              f"/{status.get('total', '?')} processed")
        time.sleep(poll_secs)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset-type", choices=sorted(INDEX_PATTERNS), default="L2_CSLC_S1")
    ap.add_argument("--frame-id", type=int, help="restrict to one DISP-S1 frame's bursts")
    ap.add_argument("--poll-secs", type=int, default=10)
    ap.add_argument("--apply", action="store_true", help="rewrite documents; otherwise dry run")
    args = ap.parse_args()

    burst_ids = None
    if args.frame_id is not None:
        burst_ids = frame_burst_ids(args.frame_id)
        if not burst_ids:
            print(f"frame {args.frame_id} is not in the deployed burst database")
            return 1
        print(f"frame {args.frame_id}: {len(burst_ids)} bursts")

    index = INDEX_PATTERNS[args.dataset_type]
    query = build_query(args.dataset_type, burst_ids)
    total, by_index, sample = count_by_index(index, query)
    print(f"{args.dataset_type} documents without metadata.burst_id: {total}")
    for name, count in sorted(by_index.items()):
        print(f"   {name}: {count}")
    if sample:
        print(f"   e.g. {sample}")

    if not total:
        print("nothing to do")
        return 0
    if not args.apply:
        print("\nDRY RUN -- nothing changed. Re-run with --apply.")
        return 0

    response = es("POST", f"/{index}/_update_by_query?conflicts=proceed&slices=auto"
                          f"&wait_for_completion=false&refresh=true",
                  {"query": query, "script": build_script(args.dataset_type)})
    task_id = response.get("task")
    if not task_id:
        print(f"update_by_query did not start a task: {response}")
        return 1
    print(f"update_by_query task {task_id}")

    result, error = wait_for_task(task_id, args.poll_secs)
    if error:
        print(f"task failed: {json.dumps(error)[:500]}")
        return 1
    print(f"total={result.get('total')} updated={result.get('updated')} "
          f"noops={result.get('noops')} version_conflicts={result.get('version_conflicts')} "
          f"failures={len(result.get('failures') or [])}")

    remaining, _, sample = count_by_index(index, query)
    print(f"remaining without metadata.burst_id: {remaining}" + (f" (e.g. {sample})" if sample else ""))
    return 0 if not result.get("failures") else 1


if __name__ == "__main__":
    sys.exit(main())
