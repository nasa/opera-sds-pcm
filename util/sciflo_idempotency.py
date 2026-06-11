"""SCIFLO-level idempotency check for OPERA PGE jobs.

OPERA product IDs embed a ``creation_timestamp`` suffix so the default
HySDS no-clobber publisher cannot deduplicate across runs -- every re-run
yields a distinct ``_id``. This module provides a PGE-agnostic helper
that queries GRQ for any existing product matching the natural key (every
ID field *except* ``creation_timestamp``) and, when enabled in
settings.yaml, exits the SCIFLO cleanly before any expensive work runs.

Duplicate sources this catches:

- Celery-redelivered SCIFLO tasks after the original published its
  outputs (cluster A in the 2026-05-20 Grace test: 14 single-dups
  spaced ~2 min apart, consistent with worker termination + redeliver).
- Multiple trigger-rule fires on the same upstream state-config when
  the payload differs across fires (cluster B: cascade-race re-fires
  carrying divergent ``compressed_cslc_ids`` snapshots that bypass
  HySDS ``payload_hash`` dedup).

Activation is per-PGE via the ``SCIFLO_IDEMPOTENCY_CHECK`` section of
settings.yaml::

    SCIFLO_IDEMPOTENCY_CHECK:
      L3_DISP_S1: true

PGEs *not* listed run with the check disabled (default). Add a new entry
only after wiring the matching precondition into that PGE's chimera
config -- the precondition is what knows how to build the natural-key
ID wildcard for that product type.
"""

import logging
import sys
from typing import Optional

from util.common_util import create_info_message_files

logger = logging.getLogger(__name__)

SCIFLO_IDEMPOTENCY_CHECK_KEY = "SCIFLO_IDEMPOTENCY_CHECK"


def is_idempotency_check_enabled(pge_type: str, settings: dict) -> bool:
    """Return True if ``SCIFLO_IDEMPOTENCY_CHECK[pge_type]`` is truthy.

    Defaults to False for any PGE not explicitly listed -- new PGEs must
    add themselves to the config *and* wire their precondition before
    they get any protection.
    """
    section = (settings or {}).get(SCIFLO_IDEMPOTENCY_CHECK_KEY, {}) or {}
    return bool(section.get(pge_type, False))


def find_existing_product(index_pattern: str, query: dict) -> Optional[str]:
    """Return the ``_id`` of any GRQ document matching ``query``, else None.

    ``index_pattern`` is a wildcard pattern (e.g. ``"grq_*_l3_disp_s1*"``).
    ``query`` is an Elasticsearch query-DSL dict -- typically a
    ``wildcard`` on ``id.keyword`` constructed from the PGE's natural-key
    fields so the ``creation_timestamp`` suffix is the only unmatched
    portion.
    """
    # Deferred import: data_subscriber.es_conn_util pulls in
    # hysds.celery.app at module-load, which only resolves on a cluster
    # worker. Importing inside the function keeps this module loadable
    # in plain dev shells (and in unit tests that patch the helper).
    from data_subscriber import es_conn_util

    es = es_conn_util.get_es_connection(logger).es
    body = {
        "query": query,
        "size": 1,
        "_source": ["id"],
        "track_total_hits": False,
    }
    result = es.search(
        index=index_pattern, body=body,
        ignore_unavailable=True, allow_no_indices=True, expand_wildcards="open",
    )
    hits = result.get("hits", {}).get("hits", []) or []
    if not hits:
        return None
    hit = hits[0]
    return hit.get("_id") or hit.get("_source", {}).get("id")


def exit_if_existing_product(pge_type: str, settings: dict,
                             index_pattern: str, query: dict) -> None:
    """If enabled for ``pge_type`` *and* GRQ already has a matching product,
    log + ``sys.exit(0)`` so the SCIFLO ends as ``job-completed`` without
    publishing duplicate output.

    The clean exit short-circuits dolphin and every subsequent
    precondition; HySDS sees a normal success and the upstream
    state-config doc isn't re-indexed, so the trigger rule won't re-fire.

    When the check is disabled (``pge_type`` absent or set to false) or no
    match is found, this returns and the precondition chain continues.
    """
    if not is_idempotency_check_enabled(pge_type, settings):
        logger.info(
            f"SCIFLO idempotency check disabled for {pge_type} "
            f"(SCIFLO_IDEMPOTENCY_CHECK.{pge_type} not set to true); "
            f"proceeding without dedup check."
        )
        return

    existing_id = find_existing_product(index_pattern, query)
    if existing_id is None:
        logger.info(
            f"SCIFLO idempotency check passed for {pge_type}: no "
            f"existing product matches this run's natural key. Proceeding."
        )
        return

    logger.warning(
        f"SCIFLO idempotency check tripped for {pge_type}: existing "
        f"product {existing_id} matches this run's natural key. "
        f"Exiting cleanly (sys.exit 0) to avoid duplicate output."
    )
    # Surface the bail to Figaro / Mozart UI via the standard
    # _alt_msg.txt + _alt_msg_details.txt files. Without this an operator
    # cannot tell a duplicate-bail apart from a real successful SCIFLO --
    # both show job-completed (exit 0) with no published datasets, which
    # could be mistaken for a silent failure.
    create_info_message_files(
        msg=f"dup skip: {pge_type}",
        msg_details=(
            f"SCIFLO idempotency bail for {pge_type}.\n"
            f"Existing GRQ product matches this run's natural key "
            f"(every product-ID field except creation_timestamp), so "
            f"running the PGE would emit a duplicate.\n"
            f"Existing product _id: {existing_id}\n"
            f"Toggle SCIFLO_IDEMPOTENCY_CHECK.{pge_type}=false in "
            f"settings.yaml to disable this check (e.g. for a deliberate "
            f"reprocessing campaign)."
        ),
    )
    sys.exit(0)
