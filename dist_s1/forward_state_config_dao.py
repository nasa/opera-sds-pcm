"""CRUD operations for DIST-S1 forward-mode state-configs in OpenSearch."""

import logging
from datetime import datetime, timedelta, UTC

from opensearchpy import helpers

from dist_s1.dataset_util import create_ds_dataset_json, write_ds_dataset_json, write_ds_met_json, create_dataset
from opera_commons.es_connection import get_grq_es
from util.grq_client import get_body

logger = logging.getLogger(__name__)

FWD_STATE_CONFIG_INDEX = "grq_1.0_dist_s1-fwd-state-config"


def fix_batch_id(batch_id):
    batch_id_split = batch_id.split("_")
    batch_id = f'{batch_id_split[0].removeprefix("p")}_{batch_id_split[1]}_{batch_id_split[3].removeprefix("a")}'
    return batch_id


def upsert_state_config(
    batch_id: str,
    rtc_granule_ids: list[str],
    expected_burst_count: int,
    grace_period_minutes: int,
    recreate_dataset_dir_on_update=False,
    **fields
):
    """Create or update a forward-mode state-config document.

    If the document already exists, appends new granule IDs (deduplicated),
    recomputes completeness, and updates last_modified + grace_period_expiry.

    :param batch_id: The batch id of the forward-mode state-config. Like "36TYL_0_S1C_368"
    :param tile_id: MGRS tile ID associated with the batch. like 36TYL
    :param agn: acquisition group ID within MGRS tile. See DIST-S1 lookup DB for relevant column.
    :param aci: acquisition cycle index.
    :param rtc_granule_ids: (partial) list of RTC-Granule IDs associated with this new batch. IDs will be merged with any existing list.
    :param expected_burst_count: See DIST-S1 lookup DB.
    :param grace_period_minutes: grace period (in minutes) for this batch.
    :param recreate_dataset_dir_on_update: If True, recreate dataset directory on update. This will re-publish the dataset using HySDS, while preserving the metadata in the existing GRQ document.
    :param fields: Additional fields to add to the GRQ document. Ignored for update operations.
    """
    now = datetime.now(UTC).isoformat(timespec="seconds")

    tile_id, agn, satellite, aci = batch_id.split("_")
    agn = int(agn)
    aci = int(aci)

    doc = {
        "batch_id": batch_id,  # e.g. 36TYL_0_S1C_368
        "tile_id": tile_id,  # e.g. 36TYL
        "agn": agn,  # typically 0, 1, 2, or 3. See DIST-S1 lookup DB
        "satellite": satellite,  # e.g. S1C, S1D
        "aci": aci,  # index 12-day cycles from epoch. From earliest acquired RTC in a batch

        "status": "NULL",  # null-value. enables querying for NULL/new docs.
        "is_runnable": False,  # whether a batch is usable. I.e, can be used in a SCIFLO run.

        "rtc_granule_ids": rtc_granule_ids,  # RTC granules (IDs) associated with a batch.

        "actual_burst_count": len(rtc_granule_ids),
        "expected_burst_count": expected_burst_count,  # see DIST-S1 lookup DB

        "creation_timestamp": now,  # insert time
        "last_modified_timestamp": now,  # upsert time
        "grace_period_expiry": _compute_grace_expiry(now, grace_period_minutes),  # calculated expiration time (grace period)

        "proc_mode": "forward",  # record proc mode in case we merge with DIST-S1 historical mode state-config index later

        **fields,  # mainly to support miscellaneous fields like bbox, k_offsets_counts, etc.
    }

    existing_state_config = query_state_config(batch_id)
    if not existing_state_config:
        # write out required local files to filesystem (dataset directory) to exit the job and write out the state-config for initial document insert by HySDS.
        dataset_id = f"DIST_S1_fwd-state-config_{batch_id}"

        ds_met_json = doc

        ds_dataset_json = create_ds_dataset_json(version="1.0")
        ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
        ds_met_json_path = write_ds_met_json(ds_met_json, dataset_id)
        dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-FWD-STATE-CONFIG")
        return

    # Scripted upsert: if exists, merge rtc_granule_ids and update timestamps
    script = {
        "source": """
            def existing = ctx._source.metadata.rtc_granule_ids;
            for (id in params.new_ids) {
                if (!existing.contains(id)) {
                    existing.add(id);
                }
            }
            ctx._source.metadata.rtc_granule_ids = existing;
            ctx._source.metadata.last_modified_timestamp = params.now;
            ctx._source.metadata.grace_period_expiry = params.grace_expiry;
            ctx._source.metadata.actual_burst_count = existing.size();
        """,
        "lang": "painless",
        "params": {
            "new_ids": rtc_granule_ids,
            "now": now,
            "grace_expiry": _compute_grace_expiry(now, grace_period_minutes),
        }
    }

    grq_es = get_grq_es()
    grq_es.es.update(
        index=FWD_STATE_CONFIG_INDEX,
        id=f"DIST_S1_fwd-state-config_{batch_id}",
        body={
            "script": script,
            "upsert": {"metadata": doc},
        },
        retry_on_conflict=3,
        # wait_for_active_shards=True,
        refresh=True,
    )
    logger.info(f"Upserted forward state-config: {batch_id}")

    if not recreate_dataset_dir_on_update:
        return

    existing_state_config = query_state_config(batch_id)
    if existing_state_config:
        # recreate dataset directory for upserted forward state-config document
        dataset_id = str(existing_state_config["_id"])
        ds_met_json = doc

        ds_dataset_json = create_ds_dataset_json(version="1.0")
        ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
        ds_met_json_path = write_ds_met_json(ds_met_json, dataset_id)
        dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-FWD-STATE-CONFIG")
        return

    logger.info(f"Recreated dataset directory for upserted forward state-config document")


def query_state_config(batch_id: str) -> dict | None:
    """Fetch a state-config document by batch_id."""
    grq_es = get_grq_es()

    body = get_body(match_all=False)
    del body["sort"]  # default sort not applicable for these specialized docs
    body["query"]["bool"]["must"].append({"term": {"metadata.batch_id.keyword": batch_id}})
    body["size"] = 1

    result = grq_es.es.search(index=FWD_STATE_CONFIG_INDEX, body=body, allow_no_indices=True, ignore=[404])
    total_hits = result.get("hits", {}).get("total", 0)
    if isinstance(total_hits, dict):
        total_hits = total_hits.get("value", 0)
    if not total_hits:
        return None
    return result["hits"]["hits"][0]["_source"]


def query_submittable_null_state_configs(tile_id=None) -> list[dict]:
    """
    Query state-configs where is_submittable=True AND status='NULL'.

    :param tile_id: optional query filter on tile.
    """
    grq_es = get_grq_es()
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.is_submittable": True}},
                    {"term": {"metadata.status.keyword": "NULL"}},
                ]
            }
        }
    }

    if tile_id:
        body["query"]["bool"]["must"].append({"term": {"metadata.tile_id.keyword": tile_id}})

    results = list(helpers.scan(grq_es.es, index=FWD_STATE_CONFIG_INDEX, query=body, size=10000))
    return [hit["_source"] for hit in results]


def query_expired_state_configs(now_iso: str) -> list[dict]:
    """Query state-configs whose grace_period_expiry has passed and status is still NULL."""
    grq_es = get_grq_es()
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.status.keyword": "NULL"}},
                    {"term": {"metadata.is_runnable": False}},
                    {"range": {"metadata.grace_period_expiry": {"lte": now_iso}}},
                ]
            }
        },
    }
    results = list(helpers.scan(grq_es.es, index=FWD_STATE_CONFIG_INDEX, query=body, size=10000))
    return [hit["_source"] for hit in results]


def query_state_configs_by_tile(tile_id: str) -> list[dict]:
    """Query all state-configs for a tile_id, sorted by aci ascending."""
    grq_es = get_grq_es()
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.tile_id.keyword": tile_id}},
                ]
            }
        },
        "sort": [{"metadata.aci": {"order": "asc", "unmapped_type" : "integer"}}],  # TODO chrisjrd: remove after index template integration
    }
    results = list(helpers.scan(grq_es.es, index=FWD_STATE_CONFIG_INDEX, query=body, size=10000))
    return [hit["_source"] for hit in results]


def query_state_configs_by_tile_agn(tile_id: str, agn: int) -> list[dict]:
    """Query all state-configs for a (tile_id, agn) pair, sorted by aci ascending."""
    grq_es = get_grq_es()
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.tile_id.keyword": tile_id}},
                    {"term": {"metadata.agn": agn}},
                ]
            }
        },
        "sort": [{"metadata.aci": {"order": "asc", "unmapped_type" : "integer"}}],  # TODO chrisjrd: remove after index template integration
    }
    results = list(helpers.scan(grq_es.es, index=FWD_STATE_CONFIG_INDEX, query=body, size=10000))
    return [hit["_source"] for hit in results]


def update_state_config_fields(batch_id: str, **fields):
    """Update arbitrary fields on a state-config document."""
    batch_id = fix_batch_id(batch_id)
    grq_es = get_grq_es()
    fields["last_modified_timestamp"] = datetime.now(UTC).isoformat(timespec="seconds")

    grq_es.es.update(
        index=FWD_STATE_CONFIG_INDEX,
        id=f"DIST_S1_fwd-state-config_{batch_id}",
        body={"doc": {"metadata": fields}},
        retry_on_conflict=3,
    )
    logger.info(f"Updated forward state-config {batch_id}: {list(fields.keys())}")


def _compute_grace_expiry(now_iso: str, grace_period_minutes: int) -> str:
    now_dt = datetime.fromisoformat(now_iso)
    expiry = now_dt + timedelta(minutes=grace_period_minutes)
    return expiry.isoformat(timespec="seconds")
