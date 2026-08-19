#!/usr/bin/env python3
"""Migrate CSC and KSC indices to apply correct ES index templates.

The CSC and KSC index templates were not registered in cluster.py, so
sensing_date was mapped as text instead of date (yyyyMMdd format).
This script:
  1. Loads the index templates into OpenSearch
  2. Creates new indices with the correct mappings (from the templates)
  3. Reindexes documents from old to new
  4. Swaps: deletes old index, reindexes back to original name
"""

import json
import sys
import time
from pathlib import Path

import requests

ES_URL = "http://localhost:9209"

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / \
    "conf/sds/files/elasticsearch/grq_es_templates"

MIGRATIONS = [
    {
        "index": "grq_1_cslc_s1-cycle-state-config",
        "template_file": "es_template_cslc_s1_cycle_state_config.json",
        "template_name": "cslc_s1_cycle_state_config_template",
    },
    {
        "index": "grq_1_disp_s1-kcycle-state-config",
        "template_file": "es_template_disp_s1_kcycle_state_config.json",
        "template_name": "disp_s1_kcycle_state_config_template",
    },
]


def es_request(method, path, data=None, timeout=120):
    url = f"{ES_URL}/{path}"
    kwargs = {"timeout": timeout, "headers": {"Content-Type": "application/json"}}
    if data is not None:
        kwargs["data"] = json.dumps(data)
    resp = getattr(requests, method)(url, **kwargs)
    return resp


def log(msg):
    print(f"[migrate] {msg}")


def load_template(template_file, template_name):
    path = TEMPLATES_DIR / template_file
    if not path.exists():
        log(f"ERROR: Template file not found: {path}")
        return False
    with open(path) as f:
        body = json.load(f)
    # Strip Elasticsearch-only ILM settings (not supported on OpenSearch)
    settings = body.get("template", {}).get("settings", {})
    settings.pop("index.lifecycle.name", None)
    resp = es_request("put", f"_index_template/{template_name}", data=body)
    if resp.status_code not in (200, 201):
        log(f"ERROR loading template {template_name}: {resp.text}")
        return False
    log(f"Loaded template: {template_name}")
    return True


def get_doc_count(index):
    resp = es_request("get", f"{index}/_count")
    if resp.status_code == 200:
        return resp.json().get("count", 0)
    return 0


def get_index_settings(index):
    resp = es_request("get", f"{index}/_settings")
    if resp.status_code == 200:
        settings = resp.json().get(index, {}).get("settings", {}).get("index", {})
        return int(settings.get("number_of_shards", 8)), int(settings.get("number_of_replicas", 1))
    return 8, 1


def migrate_index(index, template_file, template_name):
    """Migrate an index to use the correct template mapping."""

    # Check if source index exists
    resp = es_request("get", index)
    if resp.status_code == 404:
        log(f"Index {index} does not exist. Skipping.")
        return True

    doc_count = get_doc_count(index)
    shards, replicas = get_index_settings(index)
    log(f"Migrating {index} ({doc_count} docs, {shards} shards)")

    # Step 1: Load the template
    if not load_template(template_file, template_name):
        return False

    # Step 2: Create temp index (template auto-applies via index_patterns)
    temp_index = f"{index}_migrate_temp"

    # Delete temp if leftover from prior run
    es_request("delete", temp_index)
    time.sleep(1)

    # Create with same shard settings; mappings come from template
    resp = es_request("put", temp_index, data={
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": replicas,
        }
    })
    if resp.status_code not in (200, 201):
        log(f"ERROR creating {temp_index}: {resp.text}")
        return False
    log(f"Created {temp_index}")

    # Verify the mapping is correct
    resp = es_request("get", f"{temp_index}/_mapping")
    mapping = resp.json().get(temp_index, {}).get("mappings", {})
    sd_mapping = (mapping.get("properties", {})
                  .get("metadata", {})
                  .get("properties", {})
                  .get("sensing_date", {}))
    if sd_mapping.get("type") != "date":
        log(f"WARNING: sensing_date mapped as '{sd_mapping.get('type')}' "
            f"in {temp_index}, expected 'date'. Template may not have matched.")

    # Step 3: Reindex old -> temp using native _reindex API
    log(f"Reindexing {index} -> {temp_index} ...")
    resp = es_request("post", "_reindex?wait_for_completion=true&refresh=true", data={
        "source": {"index": index},
        "dest": {"index": temp_index},
    })
    if resp.status_code != 200:
        log(f"ERROR reindexing to temp: {resp.text}")
        es_request("delete", temp_index)
        return False

    reindex_result = resp.json()
    created = reindex_result.get("created", 0)
    failures = reindex_result.get("failures", [])
    if failures:
        log(f"ERROR: {len(failures)} reindex failures:")
        for f in failures[:3]:
            log(f"  {f}")
        es_request("delete", temp_index)
        return False
    log(f"Reindexed {created} docs to {temp_index}")

    # Step 4: Delete the original index
    resp = es_request("delete", index)
    if resp.status_code != 200:
        log(f"ERROR deleting {index}: {resp.text}")
        return False
    log(f"Deleted {index}")
    time.sleep(1)

    # Step 5: Create new index with original name (template applies)
    resp = es_request("put", index, data={
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": replicas,
        }
    })
    if resp.status_code not in (200, 201):
        log(f"ERROR recreating {index}: {resp.text}")
        log(f"DATA IS SAFE IN {temp_index}! Manually reindex to recover.")
        return False
    log(f"Recreated {index} with template mapping")

    # Step 6: Reindex temp -> original
    log(f"Reindexing {temp_index} -> {index} ...")
    resp = es_request("post", "_reindex?wait_for_completion=true&refresh=true", data={
        "source": {"index": temp_index},
        "dest": {"index": index},
    })
    if resp.status_code != 200:
        log(f"ERROR reindexing back: {resp.text}")
        log(f"DATA IS SAFE IN {temp_index}! Manually reindex to recover.")
        return False

    reindex_result = resp.json()
    created = reindex_result.get("created", 0)
    failures = reindex_result.get("failures", [])
    if failures:
        log(f"ERROR: {len(failures)} reindex failures on restore:")
        for f in failures[:3]:
            log(f"  {f}")
        log(f"DATA IS SAFE IN {temp_index}!")
        return False
    log(f"Reindexed {created} docs back to {index}")

    # Step 7: Verify
    final_count = get_doc_count(index)
    if final_count != doc_count:
        log(f"WARNING: doc count mismatch! Original={doc_count}, Final={final_count}")
        log(f"Keeping {temp_index} for safety.")
        return False

    # Step 8: Cleanup temp
    es_request("delete", temp_index)
    log(f"Deleted {temp_index}")

    # Verify mapping
    resp = es_request("get", f"{index}/_mapping")
    mapping = resp.json().get(index, {}).get("mappings", {})
    sd_mapping = (mapping.get("properties", {})
                  .get("metadata", {})
                  .get("properties", {})
                  .get("sensing_date", {}))
    log(f"Final sensing_date mapping: {sd_mapping}")
    log(f"Migration of {index} complete! ({final_count} docs)")
    return True


def main():
    log("Starting state-config index migration")
    log(f"ES URL: {ES_URL}")
    log(f"Templates dir: {TEMPLATES_DIR}")

    all_ok = True
    for m in MIGRATIONS:
        log(f"\n{'='*60}")
        ok = migrate_index(m["index"], m["template_file"], m["template_name"])
        if not ok:
            log(f"FAILED: {m['index']}")
            all_ok = False
        log(f"{'='*60}")

    if all_ok:
        log("\nAll migrations complete!")
    else:
        log("\nSome migrations failed. Check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
