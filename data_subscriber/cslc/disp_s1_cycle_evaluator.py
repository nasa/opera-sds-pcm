"""DISP-S1 Per-Cycle Evaluator.

Triggered by L2_CSLC_S1 ingestion (Rule 1) or by on-demand re-evaluation from
an existing CSC.  For each triggering event, queries ES for ALL L2_CSLC_S1
matching the frame's expected burst_ids at the sensing_date, computes coverage
from scratch, and creates a per-cycle state-config (CSC).

The CSC is ALWAYS published (even when incomplete) so that progress is visible
in ES.  The downstream K-cycle evaluator is triggered only when is_complete=true
via a HySDS trigger rule filter.
"""

import logging

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_csc_id,
    find_csc,
    create_csc,
)
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    parse_cslc_native_id,
)
from data_subscriber import es_conn_util
from util.common_util import backoff_wrapper

logger = logging.getLogger(__name__)


class DispS1CycleEvaluator:
    """Evaluates burst completeness for a single CSLC acquisition cycle."""

    def __init__(self, es_conn):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.es_conn = es_conn

    def evaluate(self, input_dataset_id, metadata, dataset_type, force_publish=False):
        """Main entry point.  Handles dual triggers.

        Args:
            input_dataset_id: The _id of the triggering dataset.
            metadata: The metadata dict from the triggering dataset.
            dataset_type: The dataset_type of the triggering dataset.
            force_publish: If True, bypass skip logic for on-demand re-evaluation.
        """
        if dataset_type == c.CSLC_S1_CYCLE_STATE_CONFIG:
            # Input B: Re-evaluation from existing CSC
            frame_id = metadata.get(c.FRAME_ID)
            sensing_date = metadata.get(c.SENSING_DATE)
            acquisition_cycle = metadata.get(c.ACQUISITION_CYCLE)
            logger.info(f"CSC re-evaluation triggered: frame={frame_id}, "
                        f"sensing_date={sensing_date}")
            self._evaluate_cycle(frame_id, acquisition_cycle, sensing_date,
                                 force_publish=force_publish)
        else:
            # Input A: Triggered by L2_CSLC_S1
            native_id = input_dataset_id
            burst_id, acquisition_dts, acquisition_cycles, frame_ids = parse_cslc_native_id(
                native_id, self.burst_to_frames, self.frame_to_bursts
            )
            sensing_date = acquisition_dts.strftime("%Y%m%d")
            start_time = acquisition_dts.isoformat()

            logger.info(f"Evaluating CSLC: {native_id}")
            logger.info(f"  burst_id={burst_id}, frames={frame_ids}, "
                        f"sensing_date={sensing_date}")

            # A burst can belong to up to 2 frames (11.7% of bursts overlap)
            for frame_id in frame_ids:
                acquisition_cycle = acquisition_cycles[frame_id]
                self._evaluate_cycle(frame_id, acquisition_cycle, sensing_date,
                                     force_publish=force_publish)

    def _evaluate_cycle(self, frame_id, acquisition_cycle, sensing_date,
                        force_publish=False):
        """Evaluate a single frame + sensing_date for burst completeness.

        Always re-assesses from scratch by querying ES for all L2_CSLC_S1
        matching the frame's burst_ids at this sensing_date.
        """
        csc_id = make_csc_id(frame_id, sensing_date)
        expected_burst_ids = sorted(self.frame_to_bursts[frame_id].burst_ids)

        logger.info(f"Evaluating cycle: frame={frame_id}, "
                    f"sensing_date={sensing_date}, csc_id={csc_id}")

        # Skip logic: if CSC already exists with is_complete=true, skip
        if not force_publish:
            existing_metadata, _ = find_csc(self.es_conn, csc_id)
            if existing_metadata.get(c.IS_COMPLETE, False):
                logger.info(f"CSC {csc_id} already complete. Skipping.")
                return

        # Query ES for ALL L2_CSLC_S1 matching expected burst_ids at sensing_date
        found_burst_ids, cslc_product_paths = self._query_cslcs_for_cycle(
            frame_id, expected_burst_ids, sensing_date
        )

        # Compute start_time from sensing_date
        start_time = f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}T00:00:00"

        create_csc(
            frame_id=frame_id,
            acquisition_cycle=acquisition_cycle,
            sensing_date=sensing_date,
            expected_burst_ids=expected_burst_ids,
            found_burst_ids=found_burst_ids,
            cslc_product_paths=cslc_product_paths,
            start_time=start_time,
        )

    def _query_cslcs_for_cycle(self, frame_id, expected_burst_ids, sensing_date):
        """Query ES for all L2_CSLC_S1 matching burst_ids at a sensing_date.

        Returns (found_burst_ids, cslc_product_paths).
        """
        # Build ES query: find all L2_CSLC_S1 for these burst_ids at this sensing_date
        # sensing_date is YYYYMMDD; match on metadata.burst_id and starttime date range
        date_str = f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}"
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"dataset_type.keyword": "L2_CSLC_S1"}},
                        {"terms": {"metadata.burst_id.keyword": list(expected_burst_ids)}},
                    ],
                    "filter": [
                        {"range": {"starttime": {
                            "gte": f"{date_str}T00:00:00",
                            "lt": f"{date_str}T23:59:59"
                        }}}
                    ]
                }
            },
            "size": len(expected_burst_ids) * 2,  # safety margin
        }

        results = backoff_wrapper(
            self.es_conn.query,
            body=body,
            index="grq_*_l2_cslc_s1",
        )

        found_burst_ids = []
        cslc_product_paths = []

        if results:
            for hit in results:
                source = hit.get("_source", {})
                meta = source.get("metadata", {})
                burst_id = meta.get("burst_id")
                if burst_id and burst_id in expected_burst_ids:
                    if burst_id not in found_burst_ids:
                        found_burst_ids.append(burst_id)
                    # Get S3 product path
                    urls = source.get("urls", [])
                    s3_url = next((u for u in urls if u.startswith("s3://")), "")
                    if s3_url and s3_url not in cslc_product_paths:
                        cslc_product_paths.append(s3_url)

        return found_burst_ids, cslc_product_paths


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

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = DispS1CycleEvaluator(es_conn)
    evaluator.evaluate(input_dataset_id, metadata, dataset_type,
                       force_publish=force_publish)


if __name__ == "__main__":
    evaluate()
