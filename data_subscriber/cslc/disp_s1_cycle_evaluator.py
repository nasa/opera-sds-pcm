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
from datetime import datetime

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.cslc_blackout import (
    DispS1BlackoutDates,
    localize_disp_blackout_dates,
)
from data_subscriber.cslc.disp_s1_state_config import (
    make_csc_id,
    find_csc,
    create_csc,
)
from data_subscriber.cslc_utils import (
    burst_db_exclusion_enabled,
    latest_cslc_per_burst,
    localize_disp_burst_db_assessed_end,
    localize_disp_frame_burst_hist,
    localize_frame_geojson_map,
    get_geojson_for_frame,
    parse_cslc_native_id,
)
from data_subscriber import es_conn_util
from util.common_util import backoff_wrapper, create_info_message_files

logger = logging.getLogger(__name__)


class DispS1CycleEvaluator:
    """Evaluates burst completeness for a single CSLC acquisition cycle."""

    def __init__(self, es_conn):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.frame_geojson_map = localize_frame_geojson_map()
        self.blackout_dates = DispS1BlackoutDates(
            localize_disp_blackout_dates(), self.frame_to_bursts, self.burst_to_frames
        )
        self.es_conn = es_conn
        self.msgs = []
        self.msg_details = ""

    def _msg(self, short, detail=""):
        """Append a terse message for Figaro and an optional detail line."""
        self.msgs.append(short)
        if detail:
            self.msg_details += detail + "\n"

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
            self._msg(
                f"re-eval f{frame_id} {sensing_date}",
                f"Re-evaluation from existing CSC: frame={frame_id}, sensing_date={sensing_date}",
            )
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

            if len(frame_ids) > 1:
                self._msg(
                    f"evaluating {len(frame_ids)} frames",
                    f"Burst {burst_id} belongs to {len(frame_ids)} frames: {frame_ids}",
                )

            # A burst can belong to up to 2 frames (11.7% of bursts overlap)
            for frame_id in frame_ids:
                acquisition_cycle = acquisition_cycles[frame_id]
                self._evaluate_cycle(frame_id, acquisition_cycle, sensing_date,
                                     force_publish=force_publish,
                                     acquisition_dts=acquisition_dts)

        if self.msgs:
            create_info_message_files(msg=self.msgs, msg_details=self.msg_details)

    def _sensing_datetime_for_blackout(self, frame_id, sensing_date):
        """Best full-precision sensing datetime for the blackout decision.

        Blackout-window boundaries carry the frame's acquisition
        time-of-day, and ``is_in_blackout`` compares sub-day acquisition
        indices — a midnight datetime would sort before the window-start
        timestamp and miss the first blacked-out acquisition of every
        window. Prefer the frame's recorded sensing datetime on that
        calendar date; otherwise combine the date with the frame's
        (effectively constant) acquisition time-of-day.
        """
        target = datetime.strptime(sensing_date, "%Y%m%d")
        frame = self.frame_to_bursts[frame_id]
        sensing_datetimes = getattr(frame, "sensing_datetimes", None) or []
        for sdt in sensing_datetimes:
            if sdt.date() == target.date():
                return sdt
        if sensing_datetimes:
            return datetime.combine(target.date(), sensing_datetimes[0].time())
        return target

    def _evaluate_cycle(self, frame_id, acquisition_cycle, sensing_date,
                        force_publish=False, acquisition_dts=None):
        """Evaluate a single frame + sensing_date for burst completeness.

        Always re-assesses from scratch by querying ES for all L2_CSLC_S1
        matching the frame's burst_ids at this sensing_date.

        ``acquisition_dts`` is the full-precision acquisition datetime when
        the trigger provides one (L2_CSLC_S1 path); the CSC re-evaluation
        path reconstructs it from the frame's sensing history.
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
                self._msg(
                    f"f{frame_id} already complete",
                    f"CSC {csc_id} already complete, skipped",
                )
                return

        # Query ES for ALL L2_CSLC_S1 matching expected burst_ids at sensing_date
        found_burst_ids, cslc_product_paths = self._query_cslcs_for_cycle(
            frame_id, expected_burst_ids, sensing_date
        )

        # Compute start_time from sensing_date
        start_time = f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}T00:00:00"

        # Blackout is an orthogonal fact recorded on the CSC: is_complete keeps
        # its burst-coverage meaning, while the blackout flag drives exclusion
        # from DISP-S1 k-cycles downstream (KSC trigger rule, k-window
        # construction, lineage-gap check). Full precision matters at window
        # boundaries: blackout windows carry the frame's acquisition
        # time-of-day, so use the trigger's acquisition datetime when
        # available and reconstruct it otherwise.
        blackout_dts = acquisition_dts or self._sensing_datetime_for_blackout(
            frame_id, sensing_date
        )
        in_blackout, blackout_window = self.blackout_dates.is_in_blackout(
            frame_id, blackout_dts
        )
        if in_blackout:
            w_start, w_end = blackout_window
            logger.warning(
                f"Frame {frame_id} sensing_date={sensing_date} falls in blackout "
                f"window {w_start.date()}..{w_end.date()}; CSC published with "
                f"blackout=true and excluded from DISP-S1 k-cycles."
            )
            self._msg(
                f"f{frame_id} {sensing_date} blackout",
                f"CSC {csc_id}: sensing_date in blackout window "
                f"{w_start.date()}..{w_end.date()}; published for audit, "
                f"excluded from DISP-S1 k-cycles",
            )

        # Blackout is resolved first and wins: a blacked-out acquisition is also absent
        # from sensing_time_list, so testing absence alone would relabel every snow-season
        # date as a partial-coverage exclusion and put a false reason in the record.
        db_excluded, db_excluded_reason = False, ""
        if not in_blackout and burst_db_exclusion_enabled():
            assessed_end = localize_disp_burst_db_assessed_end()
            frame = self.frame_to_bursts.get(frame_id)
            listed = {dt.strftime("%Y%m%d")
                      for dt in (getattr(frame, "sensing_datetimes", None) or [])}
            if isinstance(assessed_end, str) and assessed_end and listed \
                    and sensing_date not in listed and sensing_date <= assessed_end:
                db_excluded = True
                db_excluded_reason = (
                    f"absent from the consistent burst database, which surveyed through "
                    f"{assessed_end}; the database excluded this acquisition, typically "
                    f"because the pass covers only part of the frame"
                )
                logger.warning(
                    f"Frame {frame_id} sensing_date={sensing_date} is not listed in the "
                    f"consistent burst database and falls inside the range it surveyed "
                    f"(through {assessed_end}); CSC published with db_excluded=true and "
                    f"excluded from DISP-S1 k-cycles."
                )
                self._msg(
                    f"f{frame_id} {sensing_date} db_excluded",
                    f"CSC {csc_id}: not listed in the burst database (surveyed through "
                    f"{assessed_end}); published for audit, excluded from DISP-S1 k-cycles",
                )

        frame_geojson = get_geojson_for_frame(frame_id, self.frame_geojson_map)

        create_csc(
            frame_id=frame_id,
            acquisition_cycle=acquisition_cycle,
            sensing_date=sensing_date,
            expected_burst_ids=expected_burst_ids,
            found_burst_ids=found_burst_ids,
            cslc_product_paths=cslc_product_paths,
            start_time=start_time,
            geojson=frame_geojson,
            blackout=in_blackout,
            db_excluded=db_excluded,
            db_excluded_reason=db_excluded_reason,
        )

        n_found = len(found_burst_ids)
        n_expected = len(expected_burst_ids)
        if n_found == n_expected:
            self._msg(
                f"f{frame_id} complete {n_found}/{n_expected}",
                f"CSC {csc_id}: complete {n_found}/{n_expected} bursts",
            )
        else:
            missing = sorted(set(expected_burst_ids) - set(found_burst_ids))
            self._msg(
                f"f{frame_id} incomplete {n_found}/{n_expected}",
                f"CSC {csc_id}: incomplete {n_found}/{n_expected} bursts, "
                f"missing: {missing}",
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
            index="grq_*_l2_cslc_s1-*",
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
                    # Get the ASF S3 path to the .h5 file (not the HySDS dataset dir URL)
                    product_s3_paths = meta.get("product_s3_paths", [])
                    s3_url = product_s3_paths[0] if product_s3_paths else ""
                    if s3_url and s3_url not in cslc_product_paths:
                        cslc_product_paths.append(s3_url)

        # found_burst_ids is deduplicated by burst, but the paths were only
        # deduplicated by URL -- and a reprocessed granule has a different URL for the
        # same burst, so both survived and the SAS refused the stack. Keep the newest.
        return found_burst_ids, latest_cslc_per_burst(cslc_product_paths)


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
