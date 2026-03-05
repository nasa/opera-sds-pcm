"""DISP-S1 K-Cycle Evaluator.

Triggered by per-cycle state-config ingestion (when cycle_complete=true).
Determines which K-group the completed cycle belongs to, then creates or
updates the K-group state-config tracking completeness across all K cycles.

Also checks compressed CSLC (CCSLC) availability.  When all K cycles are
complete AND CCSLCs are satisfied, sets is_complete=true which triggers the
downstream DISP-S1 job via a HySDS trigger rule.

The K-group state-config is ALWAYS published (even when incomplete) so that
progress is visible in ES.
"""

import logging

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_cycle_state_config_id,
    make_k_group_state_config_id,
    find_cycle_state_config,
    find_k_group_state_config,
    create_k_group_state_config,
    update_k_group_state_config,
)
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    save_blocked_download_job,
)
from data_subscriber import es_conn_util

logger = logging.getLogger(__name__)


class DispS1KCycleEvaluator:
    """Evaluates K-cycle completeness for DISP-S1 processing."""

    def __init__(self, es_conn, k=15, m=6):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.es_conn = es_conn
        self.k = k
        self.m = m

    def evaluate(self, metadata):
        """Main entry point.  Called when a per-cycle state-config with
        cycle_complete=true is ingested.

        Args:
            metadata: The metadata dict from the triggering per-cycle state-config.
        """
        frame_id = metadata.get(c.FRAME_ID)
        acquisition_cycle = metadata.get(c.ACQUISITION_CYCLE)

        logger.info(f"K-cycle evaluator triggered: frame={frame_id}, "
                    f"acq_cycle={acquisition_cycle}, k={self.k}, m={self.m}")

        k_group_index, acquisition_cycles = self._determine_k_group(
            frame_id, acquisition_cycle
        )

        if k_group_index is None:
            logger.warning(f"Could not determine K-group for frame={frame_id}, "
                           f"acq_cycle={acquisition_cycle}. Skipping.")
            return

        self._evaluate_k_group(frame_id, k_group_index, acquisition_cycles)

    def _determine_k_group(self, frame_id, acquisition_cycle):
        """Determine which K-group this acquisition cycle belongs to.

        Uses sensing_datetime_days_index from constDB.  Groups K consecutive
        acquisition cycles together.  Returns the K-group index (1-based) and
        the list of acquisition cycles in the group.

        Returns:
            (k_group_index, [acquisition_cycles]) or (None, None) if not determinable.
        """
        frame = self.frame_to_bursts.get(frame_id)
        if frame is None:
            logger.error(f"Frame {frame_id} not found in constDB")
            return None, None

        day_indices = sorted(set(frame.sensing_datetime_days_index))

        if acquisition_cycle not in day_indices:
            logger.error(f"Acquisition cycle {acquisition_cycle} not in constDB "
                         f"day indices for frame {frame_id}")
            return None, None

        cycle_pos = day_indices.index(acquisition_cycle)

        # K-group index: which group of K cycles does this cycle fall into?
        # Group 1 = cycles[0:k], Group 2 = cycles[1:k+1], etc. (sliding window)
        # For now, use non-overlapping groups: group_index = cycle_pos // k
        # The first K-group that includes this cycle:
        k_group_index = cycle_pos // self.k

        start_idx = k_group_index * self.k
        end_idx = min(start_idx + self.k, len(day_indices))
        group_cycles = day_indices[start_idx:end_idx]

        logger.info(f"K-group determined: k_group_index={k_group_index}, "
                    f"cycles={group_cycles}")

        return k_group_index, group_cycles

    def _evaluate_k_group(self, frame_id, k_group_index, acquisition_cycles):
        """Evaluate completeness for a K-group of acquisition cycles."""
        state_config_id = make_k_group_state_config_id(frame_id, k_group_index)

        # Read each per-cycle state-config to build cycle_completeness
        cycle_completeness = {}
        cycle_state_config_ids = []
        total_cslcs_found = 0
        total_cslcs_expected = 0

        for acq_cycle in acquisition_cycles:
            cycle_sc_id = make_cycle_state_config_id(frame_id, acq_cycle)
            cycle_state_config_ids.append(cycle_sc_id)

            cycle_metadata, _ = find_cycle_state_config(self.es_conn, cycle_sc_id)

            if cycle_metadata:
                is_complete = cycle_metadata.get(c.CYCLE_COMPLETE, False)
                cycle_completeness[str(acq_cycle)] = is_complete
                total_cslcs_found += cycle_metadata.get(c.COVERAGE_ACTUAL, 0)
                total_cslcs_expected += cycle_metadata.get(c.COVERAGE_EXPECTED, 0)
            else:
                cycle_completeness[str(acq_cycle)] = False
                # Still count expected bursts from constDB
                total_cslcs_expected += len(self.frame_to_bursts[frame_id].burst_ids)

        # Check compressed CSLC satisfaction
        all_cycles_complete = all(cycle_completeness.values())
        compressed_cslc_satisfied = False
        compressed_cslc_ids = []

        if all_cycles_complete:
            compressed_cslc_satisfied, compressed_cslc_ids = (
                self._check_compressed_cslcs(frame_id, acquisition_cycles)
            )

        # Check if K-group state-config already exists
        existing_metadata, _ = find_k_group_state_config(self.es_conn, state_config_id)

        if not existing_metadata:
            logger.info(f"Creating new K-group state-config: {state_config_id}")
            _, metadata = create_k_group_state_config(
                frame_id=frame_id,
                k_group_index=k_group_index,
                k=self.k,
                m=self.m,
                acquisition_cycles=acquisition_cycles,
                cycle_state_config_ids=cycle_state_config_ids,
                cycle_completeness=cycle_completeness,
                total_cslcs_found=total_cslcs_found,
                total_cslcs_expected=total_cslcs_expected,
                compressed_cslc_satisfied=compressed_cslc_satisfied,
                compressed_cslc_ids=compressed_cslc_ids,
                start_time=None,
            )
        else:
            logger.info(f"Updating K-group state-config: {state_config_id}")
            _, metadata = update_k_group_state_config(
                existing_metadata=existing_metadata,
                cycle_completeness=cycle_completeness,
                total_cslcs_found=total_cslcs_found,
                total_cslcs_expected=total_cslcs_expected,
                compressed_cslc_satisfied=compressed_cslc_satisfied,
                compressed_cslc_ids=compressed_cslc_ids,
                frame_id=frame_id,
                k_group_index=k_group_index,
                start_time=None,
            )

        # If all cycles complete but CCSLCs not ready, save as blocked job
        if all_cycles_complete and not compressed_cslc_satisfied:
            logger.info(f"All {self.k} cycles complete but compressed CSLCs not "
                        f"satisfied for {state_config_id}. Saving blocked job.")
            self._save_blocked_job(frame_id, k_group_index, state_config_id)

    def _check_compressed_cslcs(self, frame_id, acquisition_cycles):
        """Check if M compressed CSLCs are available for the K-group.

        Returns:
            (satisfied: bool, ccslc_ids: list)
        """
        try:
            from data_subscriber.cslc.cslc_dependency import CSLCDependency

            # The first acquisition cycle in the group determines the CCSLC check
            first_cycle = min(acquisition_cycles)
            day_indices = self.frame_to_bursts[frame_id].sensing_datetime_days_index

            if first_cycle in day_indices:
                day_idx = day_indices.index(first_cycle)
            else:
                logger.warning(f"First cycle {first_cycle} not in day indices "
                               f"for frame {frame_id}")
                return False, []

            # Check if we have M compressed CSLCs before this K-group
            satisfied = False
            ccslc_ids = []

            # If this is the very first K-group (index 0), there are no prior
            # compressed CSLCs to check — satisfaction depends on whether this
            # is the initial processing run
            if day_idx < self.m:
                logger.info(f"First K-group (day_idx={day_idx} < m={self.m}). "
                            f"No prior compressed CSLCs required.")
                return True, []

            # Query ES for compressed CSLCs
            # Use the existing CSLCDependency pattern
            result = self.es_conn.query(
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                                {"term": {"metadata.frame_id": frame_id}},
                            ]
                        }
                    },
                    "size": self.m,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                },
                index="grq_*_l2_cslc_s1_compressed",
            )

            if result and len(result) >= self.m:
                satisfied = True
                ccslc_ids = [r["_id"] for r in result[:self.m]]

            return satisfied, ccslc_ids

        except Exception as e:
            logger.warning(f"Error checking compressed CSLCs: {e}")
            return False, []

    def _save_blocked_job(self, frame_id, k_group_index, state_config_id):
        """Save a blocked download job for timer retry when CCSLCs arrive."""
        try:
            save_blocked_download_job(
                eu=self.es_conn,
                job_type="hysds-io-disp_s1_k_cycle_evaluator",
                release_version="__TAG__",
                product_type="DISP_S1",
                params={
                    "frame_id": frame_id,
                    "k_group_index": k_group_index,
                },
                job_queue="opera-job_worker-disp_s1_k_cycle_evaluator",
                job_name=f"blocked-{state_config_id}",
                add_attributes={
                    "frame_id": frame_id,
                    "k_group_index": k_group_index,
                },
            )
        except Exception as e:
            logger.error(f"Failed to save blocked job for {state_config_id}: {e}")


@exec_wrapper
def evaluate():
    """HySDS job entry point."""
    jc = JobContext("_context.json")
    job_context = jc.ctx

    product_metadata = job_context.get("product_metadata", {})
    metadata = product_metadata.get("metadata", {})

    k = job_context.get("k", 15)
    m = job_context.get("m", 6)

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = DispS1KCycleEvaluator(es_conn, k=k, m=m)
    evaluator.evaluate(metadata)


if __name__ == "__main__":
    evaluate()
