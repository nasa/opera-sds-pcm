"""CSLC-S1 Catalog Ingest (Path B).

Queries CMR for existing CSLC-S1 granules and creates metadata-only L2_CSLC_S1
datasets with DAAC S3 URLs.  HySDS post-processing publishes these datasets,
which then trigger the per-cycle evaluator (Stage 1).

This allows bootstrapping the evaluator pipeline from CMR inventory without
needing to re-run the CSLC PGE.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cmr import Collection, get_cmr_token
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    build_cslc_native_ids,
    parse_cslc_file_name,
)
from tools.ops.cmr_audit.cmr_client import async_cmr_posts, paramss_to_request_body

logger = logging.getLogger(__name__)

# OPERA-2467: number of 6-day sensing cycles to seed before an imported CCSLC's
# last_date. The first new KSC after the CCSLC needs the 14 most-recent prior
# sensing dates within the CCSLC's range to fill its k=15 sliding window; we
# extend the catalog-ingest start_date back by 13*6=78 days to cover them.
# Without seeding, the first KSC sits at 1/15 and processing stalls for ~84
# days while the window naturally fills.
SEED_TRAILING_CYCLES = 13
SENSING_CADENCE_DAYS = 6
SEED_TRAILING_DAYS = SEED_TRAILING_CYCLES * SENSING_CADENCE_DAYS

# OPERA-2468: maximum allowed gap (days) between an imported CCSLC's last_date
# and the next available CSLC sensing date. Beyond this, forward bootstrap is
# refused — the frame requires a historical restart (per nasa/opera-sds#133
# Case 3, frames F24726 and F33065 are the canonical examples). Two years
# matches the operator threshold called out in OPERA-2457. Overridable per
# job via the gap_threshold_days context field.
DEFAULT_GAP_THRESHOLD_DAYS = 730


class CslcCatalogIngest:
    """Queries CMR and creates metadata-only L2_CSLC_S1 datasets."""

    def __init__(self, settings, es_conn=None):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.settings = settings
        self.es_conn = es_conn

    def ingest(self, frame_ids, start_date, end_date,
               gap_threshold_days=DEFAULT_GAP_THRESHOLD_DAYS):
        """Query CMR for CSLC-S1 granules and create L2_CSLC_S1 datasets.

        Args:
            frame_ids: List of frame IDs (ints or strings).
            start_date: Start date (YYYY-MM-DDTHH:MM:SSZ).
            end_date: End date (YYYY-MM-DDTHH:MM:SSZ).
            gap_threshold_days: OPERA-2468 — refuse forward bootstrap for
                frames whose gap between imported CCSLC last_date and next
                CSLC exceeds this. Default 2 years.
        """
        cmr_hostname, token, _, _, _ = get_cmr_token("OPS", self.settings)

        total_created = 0
        for frame_id in frame_ids:
            frame_id = int(frame_id)
            if frame_id not in self.frame_to_bursts:
                logger.warning(f"Frame {frame_id} not in constDB. Skipping.")
                continue

            # Single ES lookup per frame, reused by both pre-flight checks.
            ccslc_last_date = self._get_latest_ccslc_last_date(frame_id)

            # OPERA-2468: refuse bootstrap if the time-series should be
            # broken (gap to next CSLC exceeds threshold).
            allowed, reason = self._check_bootstrap_gap(
                frame_id, ccslc_last_date, gap_threshold_days,
                cmr_hostname, token,
            )
            if not allowed:
                logger.warning(f"Frame {frame_id}: {reason}. Skipping.")
                continue
            logger.info(f"Frame {frame_id}: gap-check {reason}")

            # OPERA-2467: if an imported CCSLC exists for this frame, extend
            # start_date back to seed the trailing 14 CSLCs from within its
            # date range. This is per-frame because each frame has its own
            # most-recent CCSLC.
            frame_start_date = self._compute_seeded_start_date(
                frame_id, start_date, ccslc_last_date=ccslc_last_date,
            )

            items = self._query_cmr_for_frame(
                frame_id, frame_start_date, end_date, cmr_hostname, token
            )
            # Sort by temporal start so datasets are published in
            # chronological order.  This ensures cycle evaluators create
            # CSCs in date order, which gives the k-cycle evaluator the
            # correct date sequence for computing CCSLC boundary positions.
            items.sort(key=lambda it: (
                it.get("umm", {})
                .get("TemporalExtent", {})
                .get("RangeDateTime", {})
                .get("BeginningDateTime", "")
            ))
            logger.info(f"Frame {frame_id}: found {len(items)} granules in CMR")

            created = self._create_datasets(items, self.es_conn)
            total_created += created
            logger.info(f"Frame {frame_id}: created {created} datasets")

        logger.info(f"Catalog ingest complete. Total datasets created: {total_created}")

    # CCSLC ID date pattern:
    # OPERA_L2_COMPRESSED-CSLC-S1_F<frame>_T<burst>_<ref>T<...>_<first>T<...>_<last>T<...>_<creation>T<...>_VV_v<version>
    _CCSLC_DATE_RE = re.compile(
        r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_"
    )

    # CSLC granule ID date pattern (sensing date is the first YYYYMMDDT...Z):
    # OPERA_L2_CSLC-S1_<burst>_<sensing>T<...>_<creation>T<...>_S1A_VV_v<version>
    _CSLC_SENSING_DATE_RE = re.compile(r"_(\d{8})T\d+Z_")

    def _get_latest_ccslc_last_date(self, frame_id):
        """Return the latest imported CCSLC last_date (YYYYMMDD) for the frame,
        or None if no CCSLC is present.

        Shared lookup used by both ``_compute_seeded_start_date`` (OPERA-2467)
        and ``_check_bootstrap_gap`` (OPERA-2468). ES failures fall through
        as None with a warning so the caller can decide what to do.
        """
        if self.es_conn is None:
            return None

        try:
            result = self.es_conn.es.search(
                index="grq_*_l2_cslc_s1_compressed*",
                body={
                    "query": {"bool": {"must": [
                        {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                        {"term": {"metadata.frame_id": frame_id}},
                    ]}},
                    "size": 100,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                    "_source": False,
                },
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error querying CCSLCs: {e}. "
                f"Treating frame as having no imported CCSLC."
            )
            return None

        last_dates = set()
        for hit in result.get("hits", {}).get("hits", []):
            m = self._CCSLC_DATE_RE.search(hit["_id"])
            if m:
                # Groups: (ref_date, first_secondary, last_secondary, creation_date).
                # The k-boundary the CCSLC sits on is encoded by last_secondary.
                last_dates.add(m.group(3))

        return max(last_dates) if last_dates else None

    def _compute_seeded_start_date(self, frame_id, requested_start_date,
                                   ccslc_last_date=None):
        """Extend start_date backward to seed the trailing 14 CSLCs from
        an imported CCSLC's date range (OPERA-2467).

        If an imported CCSLC exists for the frame and the operator-requested
        start_date sits after ``CCSLC.last_date - 78 days``, returns the
        extended start_date so the catalog ingest pulls the 14 trailing
        sensing dates. Otherwise returns ``requested_start_date`` unchanged.

        Safe for fresh frames with no imported CCSLC (no adjustment), and
        for historical reprocess runs where the operator's start_date is
        already earlier than the seed window (no adjustment).

        ``ccslc_last_date`` may be supplied by the caller to avoid a second
        ES query; if None, the method looks it up itself.
        """
        if ccslc_last_date is None:
            ccslc_last_date = self._get_latest_ccslc_last_date(frame_id)

        if not ccslc_last_date:
            logger.info(
                f"Frame {frame_id}: no imported CCSLC found; "
                f"using operator start_date {requested_start_date} unchanged"
            )
            return requested_start_date

        last_dt = datetime.strptime(ccslc_last_date, "%Y%m%d").replace(tzinfo=timezone.utc)
        seed_dt = last_dt - timedelta(days=SEED_TRAILING_DAYS)
        seed_iso = seed_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            req_dt = datetime.strptime(
                requested_start_date, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning(
                f"Frame {frame_id}: could not parse start_date "
                f"{requested_start_date!r} (expected YYYY-MM-DDTHH:MM:SSZ). "
                f"Using as-is."
            )
            return requested_start_date

        if req_dt > seed_dt:
            logger.info(
                f"Frame {frame_id}: extending start_date "
                f"{requested_start_date} -> {seed_iso} to seed 14 trailing "
                f"CSLCs from CCSLC last_date={ccslc_last_date} (OPERA-2467)"
            )
            return seed_iso

        logger.info(
            f"Frame {frame_id}: operator start_date {requested_start_date} "
            f"already covers seed range "
            f"(CCSLC last_date={ccslc_last_date}, seed cutoff={seed_iso}); "
            f"no adjustment"
        )
        return requested_start_date

    def _get_next_cslc_sensing_date(self, frame_id, after_date,
                                    cmr_hostname, token):
        """Query CMR for the first CSLC sensing date strictly after ``after_date``.

        Uses a single burst from the frame (all bursts in a frame share the
        same acquisition cadence). Returns YYYYMMDD or None if no granule
        is found.
        """
        burst_ids = sorted(self.frame_to_bursts[frame_id].burst_ids)
        if not burst_ids:
            return None

        burst_id = burst_ids[0]
        after_dt = datetime.strptime(after_date, "%Y%m%d").replace(tzinfo=timezone.utc)
        start_iso = (after_dt + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        request_url = f"https://{cmr_hostname}/search/granules.umm_json"
        params = {
            "sort_key": "start_date",
            "provider": "ASF",
            "ShortName[]": [Collection.CSLC_S1_V1],
            "token": token,
            "native-id[]": [f"OPERA_L2_CSLC-S1_{burst_id}*"],
            "options[native-id][pattern]": "true",
            "temporal": f"{start_iso},{end_iso}",
            "page_size": 1,
        }

        try:
            items = asyncio.run(self._async_query(request_url, params))
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: CMR gap-check query failed: {e}. "
                f"Treating gap as unknown (will fall through to refuse)."
            )
            return None

        if not items:
            return None

        granule_ur = items[0].get("umm", {}).get("GranuleUR", "")
        m = self._CSLC_SENSING_DATE_RE.search(granule_ur)
        return m.group(1) if m else None

    def _check_bootstrap_gap(self, frame_id, ccslc_last_date, threshold_days,
                             cmr_hostname, token):
        """Decide whether forward bootstrap is allowed for the frame (OPERA-2468).

        Returns ``(allowed, message)``. Frames with no imported CCSLC are
        always allowed (greenfield bootstrap or pure historical reprocess).
        Frames whose next available CSLC sensing date is more than
        ``threshold_days`` after ``ccslc_last_date`` are refused — the
        time-series should be broken and historical reprocessing scheduled,
        not forward-mode continuation across the gap (per nasa/opera-sds#133
        Case 3).
        """
        if not ccslc_last_date:
            return True, "no imported CCSLC; nothing to gap-check"

        next_date = self._get_next_cslc_sensing_date(
            frame_id, ccslc_last_date, cmr_hostname, token
        )
        if next_date is None:
            return False, (
                f"no CSLC found in CMR after CCSLC last_date={ccslc_last_date}; "
                f"cannot determine gap — refusing forward bootstrap"
            )

        ccslc_dt = datetime.strptime(ccslc_last_date, "%Y%m%d").replace(tzinfo=timezone.utc)
        next_dt = datetime.strptime(next_date, "%Y%m%d").replace(tzinfo=timezone.utc)
        gap_days = (next_dt - ccslc_dt).days

        if gap_days > threshold_days:
            return False, (
                f"gap from CCSLC last_date={ccslc_last_date} to next CSLC "
                f"{next_date} is {gap_days} days, exceeds threshold of "
                f"{threshold_days} days; refusing forward bootstrap — "
                f"frame requires historical reprocessing"
            )

        return True, (
            f"gap from CCSLC last_date={ccslc_last_date} to next CSLC "
            f"{next_date} is {gap_days} days (<= {threshold_days}); allowed"
        )

    # Maximum burst IDs per CMR query.  Frames with many bursts (e.g. 27)
    # produce native-id patterns that cause CMR 400 errors.  Chunking
    # keeps each query small enough to succeed.
    CMR_BURST_CHUNK_SIZE = 5

    def _query_cmr_for_frame(self, frame_id, start_date, end_date,
                             cmr_hostname, token):
        """Query CMR for CSLC-S1 granules matching a frame's burst IDs.

        Chunks the burst IDs to avoid CMR 400 errors on large frames.
        """
        burst_ids = sorted(self.frame_to_bursts[frame_id].burst_ids)
        if not burst_ids:
            return []

        request_url = f"https://{cmr_hostname}/search/granules.umm_json"
        all_items = []
        seen_ids = set()

        for i in range(0, len(burst_ids), self.CMR_BURST_CHUNK_SIZE):
            chunk = burst_ids[i:i + self.CMR_BURST_CHUNK_SIZE]
            native_id = (
                "OPERA_L2_CSLC-S1_"
                + "*&native-id[]=OPERA_L2_CSLC-S1_".join(chunk)
                + "*"
            )

            params = {
                "sort_key": "start_date",
                "provider": "ASF",
                "ShortName[]": [Collection.CSLC_S1_V1],
                "token": token,
                "native-id[]": [native_id],
                "options[native-id][pattern]": "true",
                "temporal": f"{start_date},{end_date}",
            }

            logger.info(
                f"Frame {frame_id}: querying CMR for bursts "
                f"{i+1}-{min(i+len(chunk), len(burst_ids))}/{len(burst_ids)}"
            )
            items = asyncio.run(self._async_query(request_url, params))

            for item in items:
                granule_ur = item.get("umm", {}).get("GranuleUR", "")
                if granule_ur not in seen_ids:
                    seen_ids.add(granule_ur)
                    all_items.append(item)

        return all_items

    @staticmethod
    async def _async_query(request_url, params):
        """Run the CMR query and return raw UMM JSON items."""
        response_jsons = await async_cmr_posts(
            request_url, paramss_to_request_body([params])
        )
        return [
            item
            for rj in response_jsons
            for item in rj.get("items", [])
        ]

    @staticmethod
    def _create_datasets(items, es_conn=None):
        """Create metadata-only L2_CSLC_S1 dataset dirs from CMR UMM items.

        Each dataset dir contains {id}/{id}.met.json and {id}/{id}.dataset.json.
        HySDS post-processing (publish_datasets_parallel) publishes them to ES,
        which then triggers the per-cycle evaluator via the trigger rule.

        Datasets that already exist in ES are skipped to avoid NoClobberException
        on S3 (handles retries and overlap with historical downloads).
        """
        created = 0
        skipped = 0
        for item in items:
            granule_ur = item["umm"]["GranuleUR"]

            # VV-only — skip VH granules
            if "_VV_" not in granule_ur:
                continue

            # Extract S3 URLs for .h5 files
            s3_urls = [
                url_entry["URL"]
                for url_entry in item["umm"].get("RelatedUrls", [])
                if url_entry.get("URL", "").startswith("s3://")
                and url_entry["URL"].endswith(".h5")
            ]
            if not s3_urls:
                logger.warning(f"No S3 .h5 URLs for {granule_ur}. Skipping.")
                continue

            # Skip if already created in this run
            if os.path.isdir(granule_ur):
                continue

            # Skip if already published in ES (handles retries and historical overlap)
            if es_conn is not None:
                try:
                    result = es_conn.es.search(
                        index="grq_*_l2_cslc_s1-*",
                        body={"query": {"term": {"_id": granule_ur}}, "size": 0},
                    )
                    if result["hits"]["total"]["value"] > 0:
                        skipped += 1
                        continue
                except Exception as e:
                    logger.warning(f"ES check failed for {granule_ur}: {e}. Proceeding with creation.")

            # Extract temporal info
            temporal = item["umm"].get("TemporalExtent", {})
            if temporal.get("RangeDateTime"):
                start_time = temporal["RangeDateTime"]["BeginningDateTime"]
            else:
                start_time = temporal.get("SingleDateTime", "")

            # Extract burst_id from GranuleUR — required by cycle evaluator ES query
            try:
                burst_id, _ = parse_cslc_file_name(granule_ur)
            except ValueError:
                logger.warning(f"Could not parse burst_id from {granule_ur}. Skipping.")
                continue

            os.makedirs(granule_ur)

            # .met.json — metadata that goes into _source.metadata in ES
            metadata = {
                "burst_id": burst_id,
                "product_s3_paths": s3_urls,
                "catalog_ingest": True,
            }
            met_path = os.path.join(granule_ur, f"{granule_ur}.met.json")
            with open(met_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # .dataset.json — HySDS dataset descriptor
            dataset_info = {
                "version": "1",
                "starttime": start_time,
                "index": {
                    "suffix": "1_l2_cslc_s1-{}".format(
                        datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y.%m")
                    )
                },
            }
            ds_path = os.path.join(granule_ur, f"{granule_ur}.dataset.json")
            with open(ds_path, "w") as f:
                json.dump(dataset_info, f, indent=2)

            created += 1

        if skipped:
            logger.info(f"Skipped {skipped} datasets already in ES")
        return created


@exec_wrapper
def ingest():
    """HySDS job entry point."""
    from util.conf_util import SettingsConf
    from data_subscriber import es_conn_util

    jc = JobContext("_context.json")
    job_context = jc.ctx

    # Disable no-clobber for catalog ingest. Overlapping bursts between
    # frames can cause the same L2_CSLC_S1 product to be published by
    # multiple catalog ingest jobs — this is expected and safe since
    # catalog ingest only writes metadata.
    jc.set('_force_ingest', True)
    jc.save()

    frame_ids_str = job_context.get("frame_ids", "")
    start_date = job_context.get("start_date")
    end_date = job_context.get("end_date")
    gap_threshold_days = int(
        job_context.get("gap_threshold_days", DEFAULT_GAP_THRESHOLD_DAYS)
    )

    # Parse frame_ids — comma-separated string or list
    if isinstance(frame_ids_str, str):
        frame_ids = [f.strip() for f in frame_ids_str.split(",") if f.strip()]
    else:
        frame_ids = frame_ids_str

    settings = SettingsConf().cfg
    es_conn = es_conn_util.get_es_connection(logger)
    ingester = CslcCatalogIngest(settings, es_conn=es_conn)
    ingester.ingest(frame_ids, start_date, end_date,
                    gap_threshold_days=gap_threshold_days)


if __name__ == "__main__":
    ingest()
