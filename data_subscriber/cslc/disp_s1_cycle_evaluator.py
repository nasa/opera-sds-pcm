"""DISP-S1 Per-Cycle Evaluator.

Triggered by L2_CSLC_S1 ingestion into GRQ ES.  For each arriving CSLC burst,
creates or updates a per-cycle state-config that tracks burst completeness for
the burst's frame(s) and acquisition cycle.

The state-config dataset is ALWAYS published (even when incomplete) so that
progress is visible in ES.  The downstream K-cycle evaluator is triggered only
when cycle_complete=true via a HySDS trigger rule filter.
"""

import logging

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_cycle_state_config_id,
    find_cycle_state_config,
    create_cycle_state_config,
    update_cycle_state_config,
)
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    parse_cslc_native_id,
)
from data_subscriber import es_conn_util

logger = logging.getLogger(__name__)


class DispS1CycleEvaluator:
    """Evaluates burst completeness for a single CSLC acquisition cycle."""

    def __init__(self, es_conn):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.es_conn = es_conn

    def evaluate(self, input_dataset_id, metadata):
        """Main entry point.  Called once per L2_CSLC_S1 ingestion event.

        Args:
            input_dataset_id: The _id of the triggering L2_CSLC_S1 dataset (native_id).
            metadata: The metadata dict from the triggering dataset (_source.metadata).
        """
        native_id = input_dataset_id

        burst_id, acquisition_dts, acquisition_cycles, frame_ids = parse_cslc_native_id(
            native_id, self.burst_to_frames, self.frame_to_bursts
        )

        # Extract CSLC product path from metadata
        product_s3_paths = metadata.get("product_s3_paths", [])
        cslc_product_path = product_s3_paths[0] if product_s3_paths else ""

        start_time = acquisition_dts.isoformat()
        cslc_granule_id = native_id

        logger.info(f"Evaluating CSLC: {native_id}")
        logger.info(f"  burst_id={burst_id}, frames={frame_ids}, "
                    f"acquisition_cycles={acquisition_cycles}")

        # A burst can belong to up to 2 frames (11.7% of bursts overlap)
        for frame_id in frame_ids:
            acquisition_cycle = acquisition_cycles[frame_id]
            self._evaluate_cycle(
                frame_id=frame_id,
                acquisition_cycle=acquisition_cycle,
                burst_id=burst_id,
                cslc_granule_id=cslc_granule_id,
                cslc_product_path=cslc_product_path,
                start_time=start_time,
            )

    def _evaluate_cycle(self, frame_id, acquisition_cycle, burst_id,
                        cslc_granule_id, cslc_product_path, start_time):
        """Evaluate a single frame + acquisition cycle for burst completeness."""
        state_config_id = make_cycle_state_config_id(frame_id, acquisition_cycle)
        expected_burst_ids = sorted(self.frame_to_bursts[frame_id].burst_ids)

        logger.info(f"Evaluating cycle: frame={frame_id}, acq_cycle={acquisition_cycle}, "
                    f"burst={burst_id}")

        # Check if state-config already exists in ES
        existing_metadata, _ = find_cycle_state_config(self.es_conn, state_config_id)

        if not existing_metadata:
            # First burst for this frame+cycle — create new state-config
            logger.info(f"Creating new per-cycle state-config: {state_config_id}")
            create_cycle_state_config(
                frame_id=frame_id,
                acquisition_cycle=acquisition_cycle,
                expected_burst_ids=expected_burst_ids,
                found_burst_ids=[burst_id],
                found_cslc_granule_ids=[cslc_granule_id],
                cslc_product_paths=[cslc_product_path],
                start_time=start_time,
            )
        else:
            # State-config exists — check if this burst is already tracked
            found_bursts = existing_metadata.get(c.FOUND_BURST_IDS, [])
            if burst_id in found_bursts:
                logger.info(f"Burst {burst_id} already tracked in {state_config_id}. "
                            f"Idempotent skip.")
                # Still re-publish to ensure dataset is on disk for HySDS post-processing
                update_cycle_state_config(
                    existing_metadata=existing_metadata,
                    new_burst_id=burst_id,
                    new_cslc_granule_id=cslc_granule_id,
                    new_cslc_product_path=cslc_product_path,
                    frame_id=frame_id,
                    acquisition_cycle=acquisition_cycle,
                    start_time=start_time,
                )
            else:
                logger.info(f"Adding burst {burst_id} to {state_config_id}")
                update_cycle_state_config(
                    existing_metadata=existing_metadata,
                    new_burst_id=burst_id,
                    new_cslc_granule_id=cslc_granule_id,
                    new_cslc_product_path=cslc_product_path,
                    frame_id=frame_id,
                    acquisition_cycle=acquisition_cycle,
                    start_time=start_time,
                )


@exec_wrapper
def evaluate():
    """HySDS job entry point."""
    jc = JobContext("_context.json")
    job_context = jc.ctx

    product_metadata = job_context.get("product_metadata", {})
    metadata = product_metadata.get("metadata", {})
    input_dataset_id = job_context.get("input_dataset_id")

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = DispS1CycleEvaluator(es_conn)
    evaluator.evaluate(input_dataset_id, metadata)


if __name__ == "__main__":
    evaluate()
