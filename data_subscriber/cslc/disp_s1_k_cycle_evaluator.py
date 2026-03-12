"""DISP-S1 K-Cycle Evaluator.

Triggered by CSC with is_complete=true (Rule 2) or by on-demand re-evaluation
from an existing KSC.  Uses nearest-neighbor sliding window of K CSCs (not
fixed K-groups) to determine job readiness.

Creates a K-cycle state-config (KSC) containing full copies of all k CSC bodies,
product paths, bounding box, and compressed CSLC info so the DISP-S1 job needs
only the KSC.

The KSC is ALWAYS published (even when incomplete) so progress is visible in ES.
When is_complete=true, the downstream SCIFLO_L3_DISP_S1 job triggers via Rule 3.
"""

import logging

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_csc_id,
    make_ksc_id,
    find_csc,
    find_ksc,
    create_ksc,
    query_cscs_for_frame,
    query_incomplete_kscs_with_sensing_date,
)
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    localize_frame_geo_json,
    get_bounding_box_for_frame,
    save_blocked_download_job,
)
from data_subscriber import es_conn_util
from util.common_util import backoff_wrapper

logger = logging.getLogger(__name__)


class DispS1KCycleEvaluator:
    """Evaluates K-cycle completeness for DISP-S1 processing."""

    def __init__(self, es_conn, k=15, m=6):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.frame_geo_map = localize_frame_geo_json()
        self.es_conn = es_conn
        self.k = k
        self.m = m
        self._catalog_cache = {}  # frame_id -> catalog_by_date

    def evaluate(self, input_dataset_id, metadata, dataset_type, force_publish=False):
        """Main entry point.  Handles dual triggers.

        Args:
            input_dataset_id: The _id of the triggering dataset.
            metadata: The metadata dict from the triggering dataset.
            dataset_type: The dataset_type of the triggering dataset.
            force_publish: If True, bypass skip logic for on-demand re-evaluation.
        """
        if dataset_type == c.DISP_S1_KCYCLE_STATE_CONFIG:
            # Input B: Re-evaluation from existing KSC
            frame_id = metadata.get(c.FRAME_ID)
            sensing_date = metadata.get(c.SENSING_DATE)
            logger.info(f"KSC re-evaluation triggered: frame={frame_id}, "
                        f"sensing_date={sensing_date}")
            self._evaluate_k_cycle(frame_id, sensing_date,
                                   force_publish=force_publish, cascade=False)
        else:
            # Input A: Triggered by CSC with is_complete=true
            frame_id = metadata.get(c.FRAME_ID)
            sensing_date = metadata.get(c.SENSING_DATE)
            logger.info(f"K-cycle evaluator triggered by CSC: frame={frame_id}, "
                        f"sensing_date={sensing_date}, k={self.k}, m={self.m}")
            self._evaluate_k_cycle(frame_id, sensing_date,
                                   force_publish=force_publish, cascade=True)

    def _evaluate_k_cycle(self, frame_id, sensing_date, force_publish=False,
                          cascade=True):
        """Evaluate K-cycle completeness for a frame at a given sensing_date.

        Always re-assesses from scratch using nearest-neighbor CSC queries.
        """
        ksc_id = make_ksc_id(frame_id, sensing_date, self.k, self.m)

        # Skip logic: if KSC already exists with is_complete=true, skip
        if not force_publish:
            existing_metadata, _ = find_ksc(self.es_conn, ksc_id)
            if existing_metadata.get(c.IS_COMPLETE, False):
                logger.info(f"KSC {ksc_id} already complete. Skipping.")
                return

        # Step 1: Get k-1 nearest older CSCs + the triggering CSC = k total
        window_cscs = self._get_window_cscs(frame_id, sensing_date)

        if not window_cscs:
            logger.warning(f"No CSCs found for frame={frame_id}. Cannot create KSC.")
            return

        # Step 2: Build window_entries list and collect product paths
        window_entries = []
        all_cslc_paths = []
        window_sensing_dates = []

        for csc in window_cscs:
            csc_meta = csc.get("metadata", csc)
            sd = csc_meta.get(c.SENSING_DATE)
            from_catalog = csc_meta.get("_from_catalog", False)
            if from_catalog:
                entry_id = csc_meta.get("_catalog_batch_id", "")
                source = "cslc_catalog"
            else:
                entry_id = make_csc_id(frame_id, sd)
                source = "csc"
            window_sensing_dates.append(sd)
            window_entries.append({
                "id": entry_id,
                c.SENSING_DATE: sd,
                c.ACQUISITION_CYCLE: csc_meta.get(c.ACQUISITION_CYCLE),
                c.IS_COMPLETE: csc_meta.get(c.IS_COMPLETE, False),
                c.EXPECTED_BURST_IDS: csc_meta.get(c.EXPECTED_BURST_IDS, []),
                c.FOUND_BURST_IDS: csc_meta.get(c.FOUND_BURST_IDS, []),
                c.CSLC_PRODUCT_PATHS: csc_meta.get(c.CSLC_PRODUCT_PATHS, []),
                "source": source,
            })
            all_cslc_paths.extend(csc_meta.get(c.CSLC_PRODUCT_PATHS, []))

        # Step 3: Query m-1 CCSLCs
        compressed_cslc_satisfied, compressed_cslc_ids, ccslc_paths = (
            self._get_compressed_cslcs(frame_id, sensing_date)
        )

        # Step 4: Overlap filtering — exclude CSLCs matching CCSLC last_date
        # (handled implicitly by the CCSLC query returning distinct products)

        # Step 5: Compute bounding_box
        bounding_box = self._compute_bounding_box(frame_id)

        # Step 6: Compute save_compressed_cslc
        save_compressed_cslc = self._determine_save_compressed(
            frame_id, sensing_date
        )

        # Step 7: Build product_paths
        product_paths = {
            "L2_CSLC_S1": sorted(set(all_cslc_paths)),
            "L2_CSLC_S1_COMPRESSED": sorted(set(ccslc_paths)),
        }

        # Compute start_time from sensing_date
        start_time = (
            f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}T00:00:00"
        )

        # Step 8: Create KSC
        _, ksc_metadata = create_ksc(
            frame_id=frame_id,
            sensing_date=sensing_date,
            k=self.k,
            m=self.m,
            window_sensing_dates=window_sensing_dates,
            window_entries=window_entries,
            product_paths=product_paths,
            compressed_cslc_satisfied=compressed_cslc_satisfied,
            compressed_cslc_ids=compressed_cslc_ids,
            bounding_box=bounding_box,
            save_compressed_cslc=save_compressed_cslc,
            start_time=start_time,
        )

        # If all cycles complete but CCSLCs not ready, save as blocked job
        if ksc_metadata.get(c.ALL_CYCLES_COMPLETE) and not compressed_cslc_satisfied:
            logger.info(f"All cycles complete but CCSLCs not satisfied for "
                        f"{ksc_id}. Saving blocked job.")
            self._save_blocked_job(frame_id, sensing_date, ksc_id)

        # Step 9: Cascade re-evaluation of affected incomplete KSCs
        if cascade:
            self._re_evaluate_affected_kscs(frame_id, sensing_date)

    def _get_window_cscs(self, frame_id, sensing_date):
        """Build the k-element window for a given frame and sensing_date.

        First queries CSCs (forward processing state-configs).  For any
        sensing dates in the window that lack a CSC, falls back to the
        cslc_catalog (populated by historical processing).

        Returns list of CSC-compatible metadata dicts sorted by
        sensing_date ascending.
        """
        # Step 1: Collect all known sensing dates from CSCs
        all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
        csc_by_date = {}
        for hit in (all_cscs or []):
            source = hit.get("_source", hit)
            meta = source.get("metadata", source)
            sd = meta.get(c.SENSING_DATE)
            if sd:
                csc_by_date[sd] = meta

        # Step 2: Collect all known sensing dates from cslc_catalog
        catalog_by_date = self._query_cslc_catalog(frame_id)

        # Step 3: Merge — CSC takes precedence over catalog
        merged = {}
        for sd, meta in catalog_by_date.items():
            if sd not in csc_by_date:
                merged[sd] = meta
        merged.update(csc_by_date)

        if not merged:
            return []

        # Step 4: Sort by sensing_date, find trigger, take k window
        sorted_dates = sorted(merged.keys())

        trigger_idx = None
        for i, sd in enumerate(sorted_dates):
            if sd == sensing_date:
                trigger_idx = i
                break

        if trigger_idx is None:
            logger.warning(f"Triggering sensing_date={sensing_date} not found "
                           f"in CSCs or catalog for frame={frame_id}.")
            older = [d for d in sorted_dates if d < sensing_date]
            window_dates = older[-(self.k - 1):]
            return [merged[d] for d in window_dates]

        start = max(0, trigger_idx - (self.k - 1))
        window_dates = sorted_dates[start:trigger_idx + 1]

        from_csc = sum(1 for d in window_dates if d in csc_by_date)
        from_catalog = sum(1 for d in window_dates if d not in csc_by_date)
        logger.info(f"K-window for frame={frame_id}, sensing_date={sensing_date}: "
                    f"{len(window_dates)} dates ({from_csc} from CSC, "
                    f"{from_catalog} from catalog)")

        return [merged[d] for d in window_dates]

    def _query_cslc_catalog(self, frame_id):
        """Query cslc_catalog for historical CSLC entries for a frame.

        Returns dict mapping sensing_date (YYYYMMDD) to CSC-compatible
        metadata dicts.  Catalog entries are treated as complete since
        they were already processed by the historical pipeline.
        """
        if frame_id in self._catalog_cache:
            return self._catalog_cache[frame_id]

        try:
            result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"term": {"frame_id": frame_id}},
                    "size": 10000,
                },
                index="cslc_catalog*",
            )
        except Exception as e:
            logger.warning(f"Error querying cslc_catalog for frame {frame_id}: {e}")
            return {}

        if not result:
            self._catalog_cache[frame_id] = {}
            return {}

        # Group catalog entries by sensing_date
        by_date = {}
        for hit in result:
            source = hit.get("_source", hit)
            acq_ts = source.get("acquisition_ts", "")
            # acquisition_ts is "2017-08-22T17:29:27" — derive YYYYMMDD
            if isinstance(acq_ts, str) and len(acq_ts) >= 10:
                sd = acq_ts[:10].replace("-", "")
            else:
                continue
            if sd not in by_date:
                by_date[sd] = {
                    "acquisition_cycle": source.get("acquisition_cycle"),
                    "download_batch_id": source.get("download_batch_id", ""),
                    "burst_ids": [],
                    "product_paths": [],
                }
            burst_id = source.get("burst_id")
            if burst_id:
                by_date[sd]["burst_ids"].append(burst_id)
            s3_url = source.get("s3_url", "")
            if s3_url:
                by_date[sd]["product_paths"].append(s3_url)

        # Convert to CSC-compatible metadata dicts
        catalog_cscs = {}
        for sd, data in by_date.items():
            burst_ids = sorted(set(data["burst_ids"]))
            catalog_cscs[sd] = {
                c.SENSING_DATE: sd,
                c.ACQUISITION_CYCLE: data["acquisition_cycle"],
                c.IS_COMPLETE: True,
                c.EXPECTED_BURST_IDS: burst_ids,
                c.FOUND_BURST_IDS: burst_ids,
                c.CSLC_PRODUCT_PATHS: sorted(set(data["product_paths"])),
                "_from_catalog": True,
                "_catalog_batch_id": data["download_batch_id"],
            }

        logger.info(f"cslc_catalog returned {len(catalog_cscs)} dates "
                    f"for frame={frame_id}")
        self._catalog_cache[frame_id] = catalog_cscs
        return catalog_cscs

    def _get_compressed_cslcs(self, frame_id, sensing_date):
        """Query ES for m-1 CCSLCs for this frame.

        Returns (satisfied, ccslc_ids, ccslc_paths).
        """
        try:
            # Determine position using all known dates (CSCs + catalog)
            trigger_pos = self._get_date_position(frame_id, sensing_date)

            if trigger_pos is not None and trigger_pos < self.k:
                logger.info(f"Early window (position={trigger_pos} < k={self.k}). "
                            f"No prior CCSLCs required.")
                return True, [], []

            # Query ES for compressed CSLCs
            needed = self.m - 1
            if needed <= 0:
                return True, [], []

            result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                                {"term": {"metadata.frame_id": frame_id}},
                            ]
                        }
                    },
                    "size": needed,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                },
                index="grq_*_l2_cslc_s1_compressed",
            )

            if result and len(result) >= needed:
                ccslc_ids = [r["_id"] for r in result[:needed]]
                ccslc_paths = []
                for r in result[:needed]:
                    source = r.get("_source", {})
                    urls = source.get("urls", [])
                    s3_url = next((u for u in urls if u.startswith("s3://")), "")
                    if s3_url:
                        ccslc_paths.append(s3_url)
                return True, ccslc_ids, ccslc_paths

            return False, [], []

        except Exception as e:
            logger.warning(f"Error checking compressed CSLCs: {e}")
            return False, [], []

    def _compute_bounding_box(self, frame_id):
        """Compute bounding box for the frame."""
        try:
            return get_bounding_box_for_frame(frame_id, self.frame_geo_map)
        except Exception as e:
            logger.warning(f"Could not compute bounding box for frame {frame_id}: {e}")
            return []

    def _get_date_position(self, frame_id, sensing_date):
        """Get the position of sensing_date in the full date sequence.

        Merges CSC dates with cslc_catalog dates to get the complete
        sequence, including historical dates that predate CSC creation.
        Returns the 0-based index, or None if not found.
        """
        all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
        csc_dates = set(
            hit.get("_source", hit).get("metadata", hit).get(c.SENSING_DATE, "")
            for hit in (all_cscs or [])
        )
        catalog_dates = set(self._query_cslc_catalog(frame_id).keys())
        all_dates = sorted(csc_dates | catalog_dates)

        if sensing_date in all_dates:
            return all_dates.index(sensing_date)
        return None

    def _determine_save_compressed(self, frame_id, sensing_date):
        """Determine whether this job should save compressed CSLCs.

        Uses position in the full date sequence (CSCs + catalog):
        save_compressed_cslc = (position + 1) % k == 0
        """
        try:
            position = self._get_date_position(frame_id, sensing_date)
            if position is not None:
                return (position + 1) % self.k == 0
            return False
        except Exception as e:
            logger.warning(f"Error determining save_compressed: {e}")
            return False

    def _re_evaluate_affected_kscs(self, frame_id, sensing_date):
        """Find incomplete KSCs containing this sensing_date and re-evaluate them.

        When a CSC becomes complete, other KSCs whose window includes this
        sensing_date may now also become complete.
        """
        try:
            affected = query_incomplete_kscs_with_sensing_date(
                self.es_conn, frame_id, self.k, self.m, sensing_date,
                exclude_reference_date=sensing_date,
            )

            if not affected:
                return

            logger.info(f"Re-evaluating {len(affected)} affected incomplete KSCs "
                        f"for frame={frame_id}, sensing_date={sensing_date}")

            for hit in affected:
                source = hit.get("_source", hit)
                meta = source.get("metadata", source)
                ksc_sensing_date = meta.get(c.SENSING_DATE)
                if ksc_sensing_date:
                    logger.info(f"Re-evaluating KSC for sensing_date={ksc_sensing_date}")
                    self._evaluate_k_cycle(
                        frame_id, ksc_sensing_date,
                        force_publish=True, cascade=False,
                    )

        except Exception as e:
            logger.warning(f"Error during cascade re-evaluation: {e}")

    def _save_blocked_job(self, frame_id, sensing_date, ksc_id):
        """Save a blocked download job for timer retry when CCSLCs arrive."""
        try:
            save_blocked_download_job(
                eu=self.es_conn,
                job_type="hysds-io-disp_s1_k_cycle_evaluator",
                release_version="__TAG__",
                product_type="DISP_S1",
                params={
                    "frame_id": frame_id,
                    "sensing_date": sensing_date,
                    "k": self.k,
                    "m": self.m,
                },
                job_queue="opera-job_worker-disp_s1_k_cycle_evaluator",
                job_name=f"blocked-{ksc_id}",
                add_attributes={
                    "frame_id": frame_id,
                    "sensing_date": sensing_date,
                },
            )
        except Exception as e:
            logger.error(f"Failed to save blocked job for {ksc_id}: {e}")


@exec_wrapper
def evaluate():
    """HySDS job entry point."""
    jc = JobContext("_context.json")
    job_context = jc.ctx

    product_metadata = job_context.get("product_metadata", {})
    metadata = product_metadata.get("metadata", {})
    input_dataset_id = job_context.get("input_dataset_id")
    dataset_type = job_context.get("dataset_type", "")
    force_publish = job_context.get("force_publish", False)

    k = int(job_context.get("k", 15))
    m = int(job_context.get("m", 6))

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = DispS1KCycleEvaluator(es_conn, k=k, m=m)
    evaluator.evaluate(input_dataset_id, metadata, dataset_type,
                       force_publish=force_publish)


if __name__ == "__main__":
    evaluate()
