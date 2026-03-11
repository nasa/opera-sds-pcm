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

        # Step 2: Build cycle_state_configs list and collect product paths
        cycle_state_configs = []
        all_cslc_paths = []
        window_sensing_dates = []

        for csc in window_cscs:
            csc_meta = csc.get("metadata", csc)
            sd = csc_meta.get(c.SENSING_DATE)
            window_sensing_dates.append(sd)
            cycle_state_configs.append({
                "id": make_csc_id(frame_id, sd),
                c.SENSING_DATE: sd,
                c.ACQUISITION_CYCLE: csc_meta.get(c.ACQUISITION_CYCLE),
                c.IS_COMPLETE: csc_meta.get(c.IS_COMPLETE, False),
                c.EXPECTED_BURST_IDS: csc_meta.get(c.EXPECTED_BURST_IDS, []),
                c.FOUND_BURST_IDS: csc_meta.get(c.FOUND_BURST_IDS, []),
                c.CSLC_PRODUCT_PATHS: csc_meta.get(c.CSLC_PRODUCT_PATHS, []),
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
            cycle_state_configs=cycle_state_configs,
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
        """Query ES for k nearest CSCs: k-1 older + the trigger = k total.

        Returns list of CSC metadata dicts sorted by sensing_date ascending.
        CSCs are included regardless of is_complete flag.
        """
        all_cscs = query_cscs_for_frame(self.es_conn, frame_id)

        if not all_cscs:
            return []

        # Extract metadata from ES hits
        csc_list = []
        for hit in all_cscs:
            source = hit.get("_source", hit)
            meta = source.get("metadata", source)
            csc_list.append(meta)

        # Sort by sensing_date ascending
        csc_list.sort(key=lambda x: x.get(c.SENSING_DATE, ""))

        # Find the index of the triggering sensing_date
        trigger_idx = None
        for i, csc in enumerate(csc_list):
            if csc.get(c.SENSING_DATE) == sensing_date:
                trigger_idx = i
                break

        if trigger_idx is None:
            # Triggering CSC not yet in ES — it will be published by the
            # cycle evaluator. For now, use the triggering date as the newest.
            logger.warning(f"Triggering CSC for sensing_date={sensing_date} not "
                           f"found in ES. Using available CSCs.")
            # Take k-1 most recent CSCs before this date
            older = [csc for csc in csc_list
                     if csc.get(c.SENSING_DATE, "") < sensing_date]
            # Take k-1 nearest
            window = older[-(self.k - 1):] if len(older) >= self.k - 1 else older
            return window

        # Take up to k-1 older CSCs + the trigger CSC itself
        start = max(0, trigger_idx - (self.k - 1))
        window = csc_list[start:trigger_idx + 1]

        return window

    def _get_compressed_cslcs(self, frame_id, sensing_date):
        """Query ES for m-1 CCSLCs for this frame.

        Returns (satisfied, ccslc_ids, ccslc_paths).
        """
        try:
            # Check if this is early in the series (no CCSLCs needed)
            frame = self.frame_to_bursts.get(frame_id)
            if frame is None:
                return False, [], []

            day_indices = sorted(set(frame.sensing_datetime_days_index))

            # For the first window (fewer than m prior dates), no CCSLCs needed
            all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
            csc_dates = sorted(set(
                hit.get("_source", hit).get("metadata", hit).get(c.SENSING_DATE, "")
                for hit in (all_cscs or [])
            ))
            trigger_pos = None
            for i, d in enumerate(csc_dates):
                if d == sensing_date:
                    trigger_pos = i
                    break

            if trigger_pos is not None and trigger_pos < self.m:
                logger.info(f"Early window (position={trigger_pos} < m={self.m}). "
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

    def _determine_save_compressed(self, frame_id, sensing_date):
        """Determine whether this job should save compressed CSLCs.

        Uses position in the ES-derived sensing_date sequence:
        save_compressed_cslc = (position + 1) % k == 0
        """
        try:
            all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
            if not all_cscs:
                return False

            csc_dates = sorted(set(
                hit.get("_source", hit).get("metadata", hit).get(c.SENSING_DATE, "")
                for hit in all_cscs
            ))

            if sensing_date in csc_dates:
                position = csc_dates.index(sensing_date)
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

    k = job_context.get("k", 15)
    m = job_context.get("m", 6)

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = DispS1KCycleEvaluator(es_conn, k=k, m=m)
    evaluator.evaluate(input_dataset_id, metadata, dataset_type,
                       force_publish=force_publish)


if __name__ == "__main__":
    evaluate()
