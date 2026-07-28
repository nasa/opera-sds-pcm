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
import os
import re
from datetime import datetime
from pathlib import Path

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_rotation import (
    compute_projected_pending_boundaries,
)
from data_subscriber.cslc.disp_s1_state_config import (
    make_csc_id,
    make_ksc_id,
    find_csc,
    find_ksc,
    create_ksc,
    query_cscs_for_frame,
    query_incomplete_kscs_with_sensing_date,
    query_stale_window_kscs,
    query_blocked_kscs_for_frame,
    query_kscs_pending_ccslc_rotation,
)
from data_subscriber.cslc.disp_s1_phases import PhaseKind, lineage_start_pos
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    localize_frame_geo_json,
    localize_frame_geojson_map,
    get_bounding_box_for_frame,
    get_geojson_for_frame,
    parse_ccslc_doc_id_dates,
)
from data_subscriber import es_conn_util
from util.common_util import backoff_wrapper, create_info_message_files

logger = logging.getLogger(__name__)


class DispS1KCycleEvaluator:
    """Evaluates K-cycle completeness for DISP-S1 processing."""

    def __init__(self, es_conn, k=15, m=6):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.frame_geo_map = localize_frame_geo_json()
        self.frame_geojson_map = localize_frame_geojson_map()
        self.es_conn = es_conn
        self.k = k
        self.m = m
        self._catalog_cache = {}  # frame_id -> catalog_by_date
        self._dates_cache = {}  # frame_id -> sorted list of all dates
        self._static_layers_cache = {}  # frame_id -> (satisfied, s3_urls)
        self._large_gap_threshold = None  # lazily read from settings
        self._window_blackout_dates = {}  # frame_id -> set of blackout dates seen
        self._phase_positions = {}  # frame_id -> {sensing_date: position in the burst DB}
        self.msgs = []
        self.msg_details = ""

    def _msg(self, short, detail=""):
        """Append a terse message for Figaro and an optional detail line."""
        self.msgs.append(short)
        if detail:
            self.msg_details += detail + "\n"

    def _refresh_ksc_index(self):
        """Force OpenSearch to refresh the KSC index before a cascade read.

        The default refresh_interval (1s) means writes from a sibling kce
        worker may not yet be visible to a search. The cascade is triggered
        by a fan-out of N CCSLC publications (one per burst, 16-27 docs per
        boundary) firing the trigger-disp_s1_k_cycle_evaluator_on_ccslc rule
        N times in rapid succession; without this refresh, sibling kce jobs
        all see compressed_cslc_final=false on a KSC that an earlier sibling
        has already finalized, then each re-fires the SCIFLO with a divergent
        compressed_cslc_ids snapshot.
        """
        try:
            self.es_conn.es.indices.refresh(
                index=f"grq_*_{c.DISP_S1_KCYCLE_STATE_CONFIG}*",
                ignore_unavailable=True,
                allow_no_indices=True,
                expand_wildcards="open",
            )
        except Exception as e:
            # Refresh failure is non-fatal — the guard inside
            # _evaluate_k_cycle still catches some races even with stale reads.
            logger.warning(f"KSC index refresh failed (continuing): {e}")

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
            self._msg(
                f"re-eval f{frame_id} {sensing_date}",
                f"Re-evaluation from existing KSC: frame={frame_id}, sensing_date={sensing_date}",
            )
            self._evaluate_k_cycle(frame_id, sensing_date,
                                   force_publish=force_publish, cascade=False)
        elif dataset_type == "L2_CSLC_S1_COMPRESSED":
            # Input C: CCSLC ingested — re-evaluate every KSC for this frame
            # whose compressed-CSLC rotation is not yet final. That covers
            # both the historical "blocked" case (cycles complete but
            # missing CCSLCs) and the bulk-bootstrap case where a later
            # KSC's compressed_cslc_pending list contains this CCSLC's
            # last_date.
            frame_id = metadata.get(c.FRAME_ID)
            logger.info(f"CCSLC ingested for frame={frame_id}. "
                        f"Re-evaluating KSCs with pending rotation.")
            self._msg(
                f"CCSLC re-eval f{frame_id}",
                f"CCSLC ingested for frame={frame_id}, "
                f"re-evaluating non-final KSCs",
            )
            self._re_evaluate_kscs_on_ccslc_publish(frame_id)
        else:
            # Input A: Triggered by CSC with is_complete=true
            frame_id = metadata.get(c.FRAME_ID)
            sensing_date = metadata.get(c.SENSING_DATE)
            logger.info(f"K-cycle evaluator triggered by CSC: frame={frame_id}, "
                        f"sensing_date={sensing_date}, k={self.k}, m={self.m}")
            self._evaluate_k_cycle(frame_id, sensing_date,
                                   force_publish=force_publish, cascade=True)

        if self.msgs:
            create_info_message_files(msg=self.msgs, msg_details=self.msg_details)

    def _evaluate_k_cycle(self, frame_id, sensing_date, force_publish=False,
                          cascade=True):
        """Evaluate K-cycle completeness for a frame at a given sensing_date.

        Always re-assesses from scratch using nearest-neighbor CSC queries.
        """
        ksc_id = make_ksc_id(frame_id, sensing_date, self.k, self.m)

        existing_metadata, _ = find_ksc(self.es_conn, ksc_id)

        # Rotation-lock guard: skip even on force_publish=True when the KSC
        # is already compressed_cslc_final. The cascade fan-out (N CCSLC
        # publications -> N concurrent kce jobs -> each iterates non-final
        # KSCs with force_publish=True) lets a sibling worker finalize this
        # KSC moments before we got here. Re-running would re-index the doc
        # with a possibly-divergent compressed_cslc_ids snapshot (as more
        # CCSLCs publish during the cascade window) and fire a duplicate
        # SCIFLO_L3_DISP_S1 -- producing duplicate L3 + CCSLC outputs and
        # breaking the KSC <-> L3 audit pairing opera-handel relies on.
        # compressed_cslc_final=True implies is_complete=True, so this also
        # covers the "already complete" case.
        if existing_metadata.get(c.COMPRESSED_CSLC_FINAL, False):
            logger.info(
                f"KSC {ksc_id} already final (rotation locked by sibling "
                f"cascade re-eval). Skipping to avoid duplicate SCIFLO."
            )
            self._msg(
                f"KSC already final",
                f"KSC {ksc_id} already final, skipped to avoid duplicate trigger",
            )
            return

        # Skip logic: if KSC already exists with is_complete=true, skip
        # (force_publish=True bypasses this so cascade re-eval can shrink
        # compressed_cslc_pending on a KSC that is already is_complete=True
        # but not yet final).
        if not force_publish:
            if existing_metadata.get(c.IS_COMPLETE, False):
                logger.info(f"KSC {ksc_id} already complete. Skipping.")
                self._msg(
                    f"KSC already complete",
                    f"KSC {ksc_id} already complete, skipped",
                )
                return

        # A blacked-out acquisition must never anchor a k-cycle. The
        # trigger-disp_s1_k_cycle_evaluator rule already excludes blackout
        # CSCs, but re-evaluation paths (cascade fan-out, on-demand
        # force_publish) reach here without that rule filter.
        trigger_csc, _ = find_csc(self.es_conn, make_csc_id(frame_id, sensing_date))
        if trigger_csc.get(c.BLACKOUT, False):
            logger.info(
                f"Frame {frame_id} sensing_date={sensing_date}: triggering CSC "
                f"is blacked out; skipping KSC evaluation."
            )
            self._msg(
                f"f{frame_id} {sensing_date} blackout, skip",
                f"Triggering CSC for frame={frame_id} sensing_date={sensing_date} "
                f"is blacked out; no KSC created",
            )
            return

        # Dates the burst database labels as historical are processed as batch ministacks by the
        # historical processing job, which produces their L3 and compressed CSLC products itself.
        # The KSC is still written for bookkeeping, but marked superseded so the
        # trigger-SCIFLO_L3_DISP_S1 rule does not fire a duplicate forward job racing the batch.
        # no_run dates are never processed at all, so a stray CSLC ingest must not fire either.
        trigger_phase = self._frame_phase(frame_id, sensing_date)
        phase_superseded_by = None
        if trigger_phase is not None and trigger_phase.kind is not PhaseKind.FORWARD:
            phase_superseded_by = c.SUPERSEDED_BY_HISTORICAL_PROCESSING
            logger.info(
                f"Frame {frame_id} sensing_date={sensing_date}: date belongs to phase "
                f"{trigger_phase.label}; marking KSC superseded_by={phase_superseded_by}"
            )
            self._msg(
                f"f{frame_id} {sensing_date} {trigger_phase.label}, superseded",
                f"Frame {frame_id} sensing_date={sensing_date} is in phase "
                f"{trigger_phase.label}; historical processing owns it, forward trigger suppressed",
            )

        # Step 1: Get k-1 nearest older CSCs + the triggering CSC = k total
        window_cscs = self._get_window_cscs(frame_id, sensing_date)

        if not window_cscs:
            logger.warning(f"No CSCs found for frame={frame_id}. Cannot create KSC.")
            self._msg(
                f"no CSCs for f{frame_id}",
                f"No CSCs found for frame={frame_id}, cannot create KSC",
            )
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

        n_complete = sum(1 for e in window_entries if e.get(c.IS_COMPLETE))
        from_csc = sum(1 for e in window_entries if e["source"] == "csc")
        from_catalog = sum(1 for e in window_entries if e["source"] == "cslc_catalog")
        n_window = len(window_entries)
        date_range = f"{window_sensing_dates[0]}..{window_sensing_dates[-1]}" if window_sensing_dates else ""
        self._msg(
            f"window {n_window}/{self.k} dates",
            f"K-window: {n_window}/{self.k} dates [{date_range}] "
            f"({from_csc} CSC, {from_catalog} catalog, {n_complete} complete)",
        )

        # Flag (never block) large temporal gaps in the gathered inputs so
        # operators can track affected frames across jobs.
        large_gap, large_gap_detail = self._check_window_large_gaps(
            frame_id, window_sensing_dates
        )

        # Step 3: Query m-1 CCSLCs
        compressed_cslc_satisfied, compressed_cslc_ids, ccslc_paths, ccslc_detail = (
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
        # If a CCSLC already exists at this exact k-boundary (same frame,
        # last_secondary == sensing_date), mark this KSC superseded by the
        # existing CCSLC. The trigger-SCIFLO_L3_DISP_S1 user_rule excludes any
        # KSC where superseded_by is set, so the SCIFLO job won't fire and
        # we avoid emitting duplicate L3 + CCSLC products. is_complete
        # retains its structural meaning. This happens, for example, when
        # the trailing-CSLC seed from within an imported CCSLC's date range
        # fills the k=15 window at the boundary date itself.
        superseded_by = phase_superseded_by
        if superseded_by is not None:
            save_compressed_cslc = False
        if superseded_by is None and save_compressed_cslc and self._ccslc_exists_at_boundary(
            frame_id, sensing_date
        ):
            superseded_by = c.SUPERSEDED_BY_EXISTING_CCSLC
            logger.info(
                f"Frame {frame_id} sensing_date={sensing_date}: CCSLC "
                f"already exists at this boundary; marking KSC "
                f"superseded_by={superseded_by} to avoid duplicate "
                f"L3 and CCSLC products"
            )
            self._msg(
                "superseded by existing ccslc",
                f"CCSLC already at boundary {sensing_date}; "
                f"trigger suppressed via superseded_by",
            )
            save_compressed_cslc = False

        # Step 7: Resolve static layers from CMR
        static_satisfied, static_s3_urls = self._resolve_static_layers(frame_id)
        if static_satisfied:
            self._msg(
                f"static layers ok",
                f"Static layers: {len(static_s3_urls)} URLs for frame {frame_id}",
            )
        else:
            self._msg(
                f"static layers missing",
                f"Static layers: not resolved for frame {frame_id}",
            )

        # Step 8: Resolve ionosphere files from CDDIS
        iono_satisfied, iono_s3_urls = self._resolve_ionosphere_files(all_cslc_paths)
        if iono_satisfied:
            self._msg(
                f"ionosphere ok",
                f"Ionosphere: {len(iono_s3_urls)} files resolved",
            )
        else:
            self._msg(
                f"ionosphere missing",
                f"Ionosphere: not resolved",
            )

        # Step 9: Build product_paths
        product_paths = {
            "L2_CSLC_S1": sorted(set(all_cslc_paths)),
            "L2_CSLC_S1_COMPRESSED": sorted(set(ccslc_paths)),
            "L2_CSLC_S1_STATIC": static_s3_urls,
            "IONOSPHERE_TEC": iono_s3_urls,
        }

        # Detect partial CSCs anywhere in this k-cycle's lineage (including
        # dates that have aged out of the current window). The
        # trigger-SCIFLO_L3_DISP_S1 user_rule blocks on this flag — without it, an
        # orphan SCIFLO job can fire after a partial CSC slides out of the
        # k=15 window, producing an L3 product that spans an unresolved gap.
        gap_unresolved, gap_detail = self._check_lineage_gap_unresolved(
            frame_id, sensing_date
        )
        if gap_unresolved:
            self._msg("gap_unresolved", f"Gap: {gap_detail}")

        # Compute start_time from sensing_date
        start_time = (
            f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}T00:00:00"
        )

        # Block SCIFLO until every earlier k-boundary KSC's CCSLC is
        # published. Without this, in bulk-bootstrap scenarios where every
        # KSC across the forward timeline is created within seconds of
        # catalog ingest, each KSC would freeze its compressed_cslc_ids
        # using only the imported CCSLCs and the SCIFLO would consume a
        # stale rotation. The trigger-SCIFLO_L3_DISP_S1 user_rule gates on
        # compressed_cslc_final=true, computed from this list inside
        # create_ksc; subsequent CCSLC publications cascade through
        # _re_evaluate_kscs_on_ccslc_publish below to clear the list.
        pending_boundaries = self._get_pending_ccslc_boundaries(
            frame_id, sensing_date
        )
        if pending_boundaries:
            self._msg(
                f"pending CCSLCs {len(pending_boundaries)}",
                f"Pending earlier-boundary CCSLCs: {pending_boundaries}",
            )

        # Step 10: Create KSC
        # Resolve GeoJSON geometry for the frame (visible on Tosca)
        frame_geojson = get_geojson_for_frame(frame_id, self.frame_geojson_map)

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
            ccslc_detail=ccslc_detail,
            static_layers_satisfied=static_satisfied,
            ionosphere_satisfied=iono_satisfied,
            gap_unresolved=gap_unresolved,
            gap_detail=gap_detail,
            large_gap=large_gap,
            large_gap_detail=large_gap_detail,
            superseded_by=superseded_by,
            compressed_cslc_pending=pending_boundaries,
            geojson=frame_geojson,
        )

        if ksc_metadata.get(c.IS_COMPLETE):
            self._msg(
                f"KSC complete",
                f"KSC {ksc_id}: complete, DISP-S1 job will trigger",
            )
        else:
            reason = ksc_metadata.get(c.COMPLETENESS_REASON, "unknown")
            self._msg(
                f"KSC incomplete",
                f"KSC {ksc_id}: incomplete ({reason})",
            )

        # Step 11: Cascade re-evaluation of affected incomplete KSCs
        if cascade:
            self._re_evaluate_affected_kscs(frame_id, sensing_date)

    def _large_gap_threshold_days(self):
        """Days between consecutive k-window dates above which the gap is
        flagged to operators (processing continues). Configurable via
        settings.yaml DISP_S1_LARGE_GAP_THRESHOLD_DAYS; defaults to 730
        (~2 years)."""
        if self._large_gap_threshold is not None:
            return self._large_gap_threshold
        threshold = 730
        try:
            from util.conf_util import SettingsConf
            threshold = int(
                SettingsConf().cfg.get("DISP_S1_LARGE_GAP_THRESHOLD_DAYS", 730)
            )
        except Exception as e:
            logger.warning(
                f"Could not read DISP_S1_LARGE_GAP_THRESHOLD_DAYS from "
                f"settings ({e}); using default {threshold}"
            )
        self._large_gap_threshold = threshold
        return threshold

    def _check_window_large_gaps(self, frame_id, window_sensing_dates):
        """Flag (never block) unusually large temporal gaps between
        consecutive sensing dates in the k-window.

        A large gap means the frame has a real acquisition hole — nothing was
        missed on the CSLC side. The KSC is still created and processing
        continues; the flag gives operators a facetable marker
        (metadata.large_gap, persisted across jobs per frame) plus a Figaro
        message that makes the condition visible on the job itself.

        Returns ``(large_gap, detail)``.
        """
        threshold = self._large_gap_threshold_days()
        dates = sorted(window_sensing_dates or [])
        worst_days, worst_pair = 0, None
        for prev, curr in zip(dates, dates[1:]):
            days = (
                datetime.strptime(curr, "%Y%m%d")
                - datetime.strptime(prev, "%Y%m%d")
            ).days
            if days > worst_days:
                worst_days, worst_pair = days, (prev, curr)

        if worst_pair and worst_days > threshold:
            detail = (
                f"large temporal gap: {worst_days} days between "
                f"{worst_pair[0]} and {worst_pair[1]} exceeds the "
                f"{threshold}-day threshold"
            )
            # Blackout-excluded acquisitions inside the span are not a data
            # loss — annotate so operators don't chase them as one.
            n_blackout_in_span = sum(
                1 for d in self._window_blackout_dates.get(frame_id, ())
                if worst_pair[0] < d < worst_pair[1]
            )
            if n_blackout_in_span:
                detail += (
                    f" ({n_blackout_in_span} blackout-excluded date(s) "
                    f"within the span)"
                )
            logger.warning(f"Frame {frame_id}: {detail}")
            self._msg(
                f"f{frame_id} LARGE GAP {worst_days}d",
                f"Frame {frame_id}: {detail}; processing continues",
            )
            return True, detail
        return False, ""

    def _frame_phase(self, frame_id, sensing_date):
        """Processing-mode phase of the burst database containing sensing_date.

        Returns None whenever the phase is unknown — an un-annotated burst database, the
        DISP_S1_PROCESSING_MODE_ENABLED master switch being off (both leave every frame without
        phases), or a date the database does not list. All phase-aware behavior keys off this,
        so an unknown phase means exactly today's behavior.
        """
        frame = self.frame_to_bursts.get(frame_id)
        phases = getattr(frame, "phases", None) if frame is not None else None
        if not phases:
            return None

        if frame_id not in self._phase_positions:
            self._phase_positions[frame_id] = {
                dt.strftime("%Y%m%d"): i for i, dt in enumerate(frame.sensing_datetimes)
            }

        position = self._phase_positions[frame_id].get(sensing_date)
        if position is None:
            return None

        for phase in phases:
            if phase.start_pos <= position < phase.end_pos:
                return phase
        return None

    def _lineage_start_date(self, frame_id, sensing_date):
        """First sensing date, as YYYYMMDD, of the compressed CSLC lineage containing sensing_date.

        A new historical phase starts a fresh lineage, so nothing published before it may be
        selected as input, anchor a k-boundary count, or occupy a k-window slot. Returns '' when
        the frame carries no phases, which lower-bounds nothing.

        A date past the last annotated one -- forward production running ahead of the burst
        database -- belongs to the frame's last chunk, which is how the download side counts its
        k-cycle for that same date. A date inside the annotated range that the database does not
        list, such as a blacked-out or partial acquisition, bounds nothing.
        """
        frame = self.frame_to_bursts.get(frame_id)
        phases = getattr(frame, "phases", None) if frame is not None else None
        if not phases:
            return ""

        phase = self._frame_phase(frame_id, sensing_date)
        if phase is not None:
            start_pos = lineage_start_pos(phases, phase.start_pos)
        elif sensing_date > frame.sensing_datetimes[-1].strftime("%Y%m%d"):
            start_pos = lineage_start_pos(phases, len(frame.sensing_datetimes))
        else:
            return ""

        return frame.sensing_datetimes[start_pos].strftime("%Y%m%d")

    def _get_window_cscs(self, frame_id, sensing_date):
        """Build the k-element window for a given frame and sensing_date.

        First queries CSCs (forward processing state-configs).  For any
        sensing dates in the window that lack a CSC, falls back to the
        cslc_catalog (populated by historical processing).

        Returns list of CSC-compatible metadata dicts sorted by
        sensing_date ascending.
        """
        # Step 1: Collect all known sensing dates from CSCs. Blacked-out
        # cycles never occupy a k-slot — the window composes from the k
        # nearest non-blackout cycles, mirroring the blackout-filtered
        # historical catalog.
        all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
        csc_by_date = {}
        blackout_dates = set()
        for hit in (all_cscs or []):
            source = hit.get("_source", hit)
            meta = source.get("metadata", source)
            sd = meta.get(c.SENSING_DATE)
            if not sd:
                continue
            if meta.get(c.BLACKOUT, False):
                blackout_dates.add(sd)
                continue
            csc_by_date[sd] = meta
        if blackout_dates:
            logger.info(
                f"Frame {frame_id}: excluded {len(blackout_dates)} blacked-out "
                f"CSC(s) from k-window candidates"
            )
        self._window_blackout_dates[frame_id] = blackout_dates

        # Step 2: Collect all known sensing dates from cslc_catalog
        catalog_by_date = self._query_cslc_catalog(frame_id)

        # Step 3: Merge — CSC takes precedence over catalog. A date whose CSC
        # is blacked out must not sneak back in via a stale catalog entry
        # (the catalog is blackout-filtered only against the blackout file in
        # force when it was written; blackout files are re-issued seasonally).
        merged = {}
        for sd, meta in catalog_by_date.items():
            if sd not in csc_by_date and sd not in blackout_dates:
                merged[sd] = meta
        merged.update(csc_by_date)

        if not merged:
            return []

        # Step 4: Sort by sensing_date, find trigger, take k window. Dates from before the
        # current lineage belong to a different compressed CSLC chain -- a k-window must never
        # straddle a lineage break, even when dates arrive out of order.
        sorted_dates = sorted(merged.keys())
        lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
        if lineage_start_date:
            excluded = len(sorted_dates)
            sorted_dates = [d for d in sorted_dates if d >= lineage_start_date]
            excluded -= len(sorted_dates)
            if excluded:
                logger.info(
                    f"Frame {frame_id}: excluded {excluded} date(s) before lineage start "
                    f"{lineage_start_date} from k-window candidates"
                )

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
                    # Practical upper bound: ~500 dates per frame (20+ years × 24 cycles/year).
                    # size=10000 provides ample headroom without needing scroll/scan.
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

    def _check_lineage_gap_unresolved(self, frame_id, sensing_date):
        """Detect partial CSCs in this k-cycle's lineage.

        Returns ``(gap_unresolved, detail)``. ``gap_unresolved`` is True if
        any CSC with sensing_date in the range
        ``(most_recent_CCSLC.last_date, sensing_date]`` is incomplete (i.e.,
        has fewer found bursts than expected). This catches both partial CSCs
        still in the current k=15 window AND partial CSCs that have aged out
        — which is the case the existing ``is_complete`` flag misses and the
        trigger rule needs to block to avoid orphan disp_s1 jobs.

        Bounded below by the most-recent CCSLC's ``last_date`` because older
        partials are part of the historical archive, not the current forward
        run. If no CCSLC exists, the lineage extends back unbounded.
        """
        lower_bound = self._get_lineage_lower_bound(frame_id, sensing_date)

        range_clause = {"lte": sensing_date}
        if lower_bound:
            range_clause["gt"] = lower_bound

        # Partial CSCs from before a phase break are part of a lineage this block does not
        # depend on, so they must not block it -- this bound also covers the first ministack
        # of a new phase, where no in-lineage CCSLC exists yet to bound the search.
        lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
        if lineage_start_date and lineage_start_date > lower_bound:
            range_clause.pop("gt", None)
            range_clause["gte"] = lineage_start_date

        try:
            result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"bool": {
                        "must": [
                            {"term": {"metadata.frame_id": frame_id}},
                            {"term": {
                                "dataset_type.keyword": c.CSLC_S1_CYCLE_STATE_CONFIG
                            }},
                            {"term": {"metadata.is_complete": False}},
                            {"range": {"metadata.sensing_date": range_clause}},
                        ],
                        # A blacked-out incomplete CSC is not a gap: it is
                        # excluded from DISP-S1 entirely and must not block
                        # firing.
                        "must_not": [
                            {"term": {"metadata.blackout": True}}
                        ],
                    }},
                    "size": 100,
                    "_source": [
                        "metadata.sensing_date",
                        "metadata.expected_burst_ids",
                        "metadata.found_burst_ids",
                    ],
                    "sort": [{"metadata.sensing_date": {"order": "asc"}}],
                },
                index="grq_*_cslc_s1-cycle-state-config*",
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error checking lineage gaps: {e}. "
                f"Treating as no gap to avoid blocking on transient ES errors."
            )
            return False, ""

        # ES hits from backoff_wrapper(es_conn.query, ...) on the CSC index
        # have the standard {_id, _source: {metadata: {...}}} shape.
        partial_dates = []
        for hit in (result or []):
            meta = hit["_source"]["metadata"]
            sd = meta.get(c.SENSING_DATE, "")
            expected = len(meta.get(c.EXPECTED_BURST_IDS, []) or [])
            found = len(meta.get(c.FOUND_BURST_IDS, []) or [])
            if expected > found:
                partial_dates.append(f"{sd} ({found}/{expected})")

        if partial_dates:
            detail = (
                f"partial CSC(s) in lineage (after CCSLC last_date "
                f"{lower_bound or 'none'}): {', '.join(partial_dates)}"
            )
            logger.info(f"Frame {frame_id} sensing_date={sensing_date}: {detail}")
            return True, detail
        return False, ""

    def _get_pending_ccslc_boundaries(self, frame_id, sensing_date):
        """Return sorted YYYYMMDD list of earlier k-boundary KSCs for the
        frame whose CCSLC has not yet been published.

        A "pending" boundary is one where an earlier KSC has
        ``save_compressed_cslc=true`` and no ``superseded_by`` marker, yet
        no CCSLC with matching ``last_date`` exists in GRQ. While at least
        one such boundary is pending, this KSC's compressed-CSLC rotation
        is not yet final — a later CCSLC publication may rotate it in —
        so the SCIFLO must wait. When the list empties, the cached
        ``compressed_cslc_ids`` on the KSC are guaranteed to equal what
        SCIFLO will consume.

        Uses the KCE's own bookkeeping (earlier KSCs already marked as
        k-boundary) rather than computing expected boundaries from
        cadence; this is robust to missed acquisitions, superseded
        boundaries, and parameter-driven k/m settings.
        """
        # A boundary from before a phase break can never rotate into this lineage
        boundary_range = {"lt": sensing_date}
        lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
        if lineage_start_date:
            boundary_range["gte"] = lineage_start_date

        try:
            ksc_result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"bool": {
                        "must": [
                            {"term": {"dataset_type.keyword": c.DISP_S1_KCYCLE_STATE_CONFIG}},
                            {"term": {"metadata.frame_id": frame_id}},
                            {"term": {"metadata.save_compressed_cslc": True}},
                            {"range": {"metadata.sensing_date": boundary_range}},
                        ],
                        "must_not": [
                            {"exists": {"field": "metadata." + c.SUPERSEDED_BY}},
                        ],
                    }},
                    "size": 1000,
                    "_source": ["metadata.sensing_date"],
                },
                index="grq_*_disp_s1-kcycle*",
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error querying earlier k-boundary "
                f"KSCs for pending check: {e}. Treating as no pending."
            )
            return []

        earlier_boundary_dates = sorted({
            r.get("_source", {}).get("metadata", {}).get(c.SENSING_DATE)
            for r in (ksc_result or [])
            if r.get("_source", {}).get("metadata", {}).get(c.SENSING_DATE)
        })
        if not earlier_boundary_dates:
            return []

        # Which of those dates already have a CCSLC published?
        try:
            ccslc_result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"bool": {"must": [
                        {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                        {"term": {"metadata.frame_id": frame_id}},
                    ]}},
                    "size": 5000,
                    "_source": False,
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error querying CCSLCs for pending "
                f"check: {e}. Marking all earlier boundaries as pending."
            )
            return list(earlier_boundary_dates)

        published_last_dates = set()
        for r in (ccslc_result or []):
            dates = parse_ccslc_doc_id_dates(r.get("_id", ""))
            # A compressed CSLC from before a lineage break can neither anchor this lineage's
            # projection nor satisfy one of its boundaries
            if dates and dates[2] >= lineage_start_date:
                published_last_dates.add(dates[2])

        existing_pending = [
            d for d in earlier_boundary_dates if d not in published_last_dates
        ]

        # Also project the *expected* k-boundaries from the actual date
        # sequence and treat any whose CCSLC isn't published yet as pending.
        # The query above only sees earlier boundary KSCs that already exist;
        # under out-of-order parallel cascade a later KSC can be evaluated
        # before an earlier in-window boundary KSC is created, so that boundary
        # is invisible and the KSC finalizes (compressed_cslc_final=True)
        # without waiting for its CCSLC -- then it is permanently locked out of
        # every fix-up re-eval. Projecting from the date sequence makes the
        # wait order-independent. This is strictly additive: it can only add to
        # the pending list, never cause premature finalization.
        try:
            # Counted from the lineage start, so the k-strides land on this block's boundaries
            # rather than on the absolute grid of the whole series
            all_dates = self._get_all_dates_sorted(frame_id, lineage_start_date)
            projected = compute_projected_pending_boundaries(
                all_dates, published_last_dates, self.k, self.m, sensing_date
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: projected pending-boundary computation "
                f"failed: {e}. Falling back to existing-KSC pending only."
            )
            projected = []

        return sorted(set(existing_pending) | set(projected))

    def _ccslc_exists_at_boundary(self, frame_id, last_date):
        """Return True if a CCSLC for the frame already exists with
        ``last_secondary == last_date`` (the k-boundary the CCSLC sits on).

        Used by ``_evaluate_k_cycle`` to suppress the SCIFLO job for a KSC
        whose sensing_date lands on an already-processed k-boundary, which
        would otherwise re-emit a duplicate CCSLC and a duplicate L3
        product.
        """
        try:
            result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"bool": {"must": [
                        {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                        {"term": {"metadata.frame_id": frame_id}},
                    ]}},
                    "size": 200,
                    "_source": False,
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error checking CCSLC existence at "
                f"boundary {last_date}: {e}. Treating as not-exists "
                f"(may regenerate)."
            )
            return False

        for r in (result or []):
            dates = parse_ccslc_doc_id_dates(r.get("_id", ""))
            if dates and dates[2] == last_date:
                return True
        return False

    def _get_lineage_lower_bound(self, frame_id, sensing_date):
        """Return the most-recent CCSLC last_date strictly before sensing_date,
        as YYYYMMDD, or '' if no CCSLC exists for the frame.

        Used by ``_check_lineage_gap_unresolved`` to bound the partial-CSC
        search to the current k-cycle's lineage. A CCSLC published before a
        phase break belongs to a different lineage and is ignored.
        """
        lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
        try:
            result = backoff_wrapper(
                self.es_conn.query,
                body={
                    "query": {"bool": {"must": [
                        {"term": {"dataset_type.keyword": "L2_CSLC_S1_COMPRESSED"}},
                        {"term": {"metadata.frame_id": frame_id}},
                    ]}},
                    "size": 200,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                    "_source": False,
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )
        except Exception as e:
            logger.warning(
                f"Frame {frame_id}: error looking up CCSLC lineage bound: {e}"
            )
            return ""

        prior_last_dates = []
        for r in (result or []):
            doc_id = r.get("_id", "")
            dates = parse_ccslc_doc_id_dates(doc_id)
            if dates is None:
                logger.warning(
                    f"CCSLC {doc_id} has unexpected ID format; "
                    f"skipping for lineage lower-bound."
                )
                continue
            # dates is (ref, first_secondary, last_secondary, creation)
            last_date = dates[2]
            if last_date < sensing_date and last_date >= lineage_start_date:
                prior_last_dates.append(last_date)
        return max(prior_last_dates) if prior_last_dates else ""

    def _get_compressed_cslcs(self, frame_id, sensing_date):
        """Query ES for CCSLCs for this frame and select the most recent ones.

        Selects the m-1 most recent CCSLC sets whose last_date is strictly
        before the trigger sensing_date.  This approach uses whatever CCSLCs
        actually exist in ES rather than computing expected boundary positions
        from the date sequence, making it robust to:
          - Bootstrapping forward processing from an existing system (POP1)
          - Out-of-order CSLC ingestion
          - Partial date sequences

        If fewer than m-1 CCSLC sets exist before this date, all available
        sets are used (early window — first ministack needs no CCSLCs).

        Returns (satisfied, ccslc_ids, ccslc_paths, completeness_detail).
        """
        try:
            needed_sets = self.m - 1
            if needed_sets <= 0:
                self._msg(
                    f"CCSLCs not required (m=1)",
                    f"CCSLCs: not required (m=1)",
                )
                return True, [], [], "no CCSLCs required (m=1)"

            # Query ES for ALL compressed CSLCs for this frame
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
                    "size": 1000,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )

            # Parse CCSLCs: collect IDs, paths, and date ranges keyed by
            # last_date (each k-boundary produces one set across all bursts).
            all_ccslc_records = []  # [(id, path, first_date, last_date), ...]
            ccslc_sets = {}  # {last_date: (first_date, last_date)}
            ccslc_counts = {}  # {last_date: count} — per-burst CCSLC count
            for r in (result or []):
                rid = r["_id"]
                product_s3_paths = r.get("_source", {}).get("metadata", {}).get("product_s3_paths", [])
                s3_url = product_s3_paths[0] if product_s3_paths else ""
                date_match = re.search(
                    r'_(\d{8})T000000Z_(\d{8})T000000Z_(\d{8})T000000Z_(\d{8})T',
                    rid
                )
                if date_match:
                    first_date = date_match.group(2)
                    last_date = date_match.group(3)
                    all_ccslc_records.append((rid, s3_url, first_date, last_date))
                    ccslc_sets[last_date] = (first_date, last_date)
                    ccslc_counts[last_date] = ccslc_counts.get(last_date, 0) + 1

            # Find all CCSLC boundary dates strictly before the trigger date.
            # These are CCSLCs that can be used as input (not produced by
            # this job). Anything published before the current lineage began
            # belongs to a different chain and must never be selected.
            lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
            prior_boundary_dates = sorted(
                d for d in ccslc_sets if d < sensing_date and d >= lineage_start_date
            )

            if not prior_boundary_dates:
                # No CCSLCs exist before this date — early window.
                self._msg(
                    f"CCSLCs not required (early)",
                    f"CCSLCs: no prior CCSLCs found before {sensing_date}",
                )
                return True, [], [], "no CCSLCs required (early window)"

            # Select the m-1 most recent prior boundary dates
            required_boundary_dates = prior_boundary_dates[-(needed_sets):]

            # Validate burst coverage: each k-boundary should have one
            # CCSLC per burst in the frame.
            frame_info = self.frame_to_bursts.get(frame_id)
            expected_bursts = len(frame_info.burst_ids) if frame_info else 0
            if expected_bursts > 0:
                incomplete_boundaries = [
                    f"{d} ({ccslc_counts.get(d, 0)}/{expected_bursts})"
                    for d in required_boundary_dates
                    if ccslc_counts.get(d, 0) < expected_bursts
                ]
                if incomplete_boundaries:
                    detail = (
                        f"CCSLCs incomplete burst coverage: "
                        f"{', '.join(incomplete_boundaries)}"
                    )
                    logger.info(f"CCSLC burst gap for {sensing_date}: {detail}")
                    self._msg(
                        f"CCSLCs incomplete bursts",
                        f"CCSLCs: {detail}",
                    )
                    return False, [], [], detail

            # All required CCSLCs exist with full burst coverage —
            # collect their IDs and paths
            ccslc_ids = []
            ccslc_paths = []
            for rid, s3_url, first_date, last_date in all_ccslc_records:
                if last_date in required_boundary_dates:
                    ccslc_ids.append(rid)
                    if s3_url:
                        ccslc_paths.append(s3_url)

            selected_ranges = [ccslc_sets[d] for d in required_boundary_dates]
            detail = (
                f"{len(required_boundary_dates)} CCSLCs "
                f"[{', '.join(f'{f}..{l}' for f, l in selected_ranges)}]"
            )
            logger.info(
                f"Selected CCSLCs for {sensing_date}: "
                f"{required_boundary_dates}"
            )
            self._msg(
                f"CCSLCs {len(required_boundary_dates)}/{needed_sets} ok",
                f"CCSLCs: {detail}",
            )
            return True, ccslc_ids, ccslc_paths, detail

        except Exception as e:
            logger.warning(f"Error checking compressed CSLCs: {e}")
            self._msg(
                f"CCSLCs error",
                f"CCSLCs: error checking — {e}",
            )
            return False, [], [], f"error: {e}"

    def _compute_bounding_box(self, frame_id):
        """Compute bounding box for the frame."""
        try:
            return get_bounding_box_for_frame(frame_id, self.frame_geo_map)
        except Exception as e:
            logger.warning(f"Could not compute bounding box for frame {frame_id}: {e}")
            return []

    def _get_all_dates_sorted(self, frame_id, lineage_start_date=""):
        """Get the full sorted date sequence for a frame.

        Merges CSC dates with cslc_catalog dates to get the complete
        sequence, including historical dates that predate CSC creation.
        Results are cached per frame_id for the lifetime of this evaluator.

        Blacked-out CSC dates are excluded so the k-boundary math
        (save_compressed counting, projected pending boundaries) sees the
        SAME date sequence as k-window construction — otherwise a projected
        boundary could land on a blacked-out date where no KSC/CCSLC can
        ever be produced, permanently stranding later KSCs' pending lists.

        lineage_start_date, when given, drops dates from before the current lineage so the
        k-boundary math of a post-gap block counts from that block, not from the whole series.
        """
        if frame_id in self._dates_cache:
            return self._bounded_dates(self._dates_cache[frame_id], lineage_start_date)

        all_cscs = query_cscs_for_frame(self.es_conn, frame_id)
        csc_dates = set()
        blackout_dates = set()
        for hit in (all_cscs or []):
            meta = hit.get("_source", hit).get("metadata", hit)
            sd = meta.get(c.SENSING_DATE, "")
            if not sd:
                continue
            if meta.get(c.BLACKOUT, False):
                blackout_dates.add(sd)
                continue
            csc_dates.add(sd)
        catalog_dates = (
            set(self._query_cslc_catalog(frame_id).keys()) - blackout_dates
        )
        result = sorted(csc_dates | catalog_dates)
        self._dates_cache[frame_id] = result
        return self._bounded_dates(result, lineage_start_date)

    @staticmethod
    def _bounded_dates(dates, lineage_start_date):
        """Drop dates from before the lineage; an empty bound keeps everything."""
        if not lineage_start_date:
            return dates
        return [d for d in dates if d >= lineage_start_date]

    def _get_date_position(self, frame_id, sensing_date):
        """Get the position of sensing_date in the full date sequence.

        Counted from the start of the lineage containing sensing_date, so a post-gap block that
        does not begin on the absolute k grid still closes its ministacks on k-sized boundaries.

        Returns the 0-based index, or None if not found.
        """
        all_dates = self._get_all_dates_sorted(
            frame_id, self._lineage_start_date(frame_id, sensing_date))

        if sensing_date in all_dates:
            return all_dates.index(sensing_date)
        return None

    def _determine_save_compressed(self, frame_id, sensing_date):
        """Determine whether this job should save compressed CSLCs.

        Counts sensing dates from the last existing CCSLC's last_date to
        determine when the next k-boundary falls.  This continues the
        CCSLC chain from wherever historical processing (POP1) left off,
        rather than computing positions from the start of the local date
        sequence.

        If no prior CCSLCs exist (first ministack), falls back to
        position-based logic from the start of the date sequence.
        """
        try:
            # Query for the most recent CCSLC with last_date < sensing_date
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
                    "size": 1000,
                    "_source": ["_id"],
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )

            # Parse CCSLC last_dates. A CCSLC from before the current lineage cannot anchor this
            # block's boundary count -- the block starts its chain over.
            lineage_start_date = self._lineage_start_date(frame_id, sensing_date)
            prior_last_dates = set()
            for r in (result or []):
                date_match = re.search(
                    r'_(\d{8})T000000Z_(\d{8})T000000Z_(\d{8})T000000Z_(\d{8})T',
                    r["_id"]
                )
                if date_match:
                    last_date = date_match.group(3)
                    if last_date < sensing_date and last_date >= lineage_start_date:
                        prior_last_dates.add(last_date)

            if not prior_last_dates:
                # No prior CCSLCs — first ministack, use position-based
                position = self._get_date_position(frame_id, sensing_date)
                if position is not None:
                    return (position + 1) % self.k == 0
                return False

            # Count sensing dates between the last CCSLC's last_date
            # (exclusive) and current sensing_date (inclusive)
            anchor = max(prior_last_dates)
            all_dates = self._get_all_dates_sorted(frame_id, lineage_start_date)
            dates_after_anchor = [d for d in all_dates if anchor < d <= sensing_date]
            count = len(dates_after_anchor)

            return count > 0 and count % self.k == 0

        except Exception as e:
            logger.warning(f"Error determining save_compressed: {e}")
            return False

    def _resolve_static_layers(self, frame_id):
        """Query CMR for CSLC-S1 Static Layer S3 URLs for a frame's burst_ids.

        Static layers are per-burst and don't change over time, so the same
        set of URLs applies to every KSC for a given frame.  Results are
        cached in-memory so cascade re-evaluations skip the CMR query.

        Returns (satisfied, s3_urls).
        """
        if frame_id in self._static_layers_cache:
            logger.info(f"Using cached static layers for frame {frame_id}")
            return self._static_layers_cache[frame_id]

        try:
            import requests as req
            from util.conf_util import SettingsConf
            from data_subscriber.cmr import get_cmr_token

            frame = self.frame_to_bursts.get(frame_id)
            if not frame:
                logger.warning(f"Frame {frame_id} not in constDB")
                return False, []

            burst_ids = sorted(frame.burst_ids)
            if not burst_ids:
                return False, []

            settings = SettingsConf().cfg
            cmr_hostname, token, _, _, _ = get_cmr_token("OPS", settings)

            # Build native_id patterns per burst_id
            native_ids = [f"OPERA_L2_CSLC-S1-STATIC_{bid}*" for bid in burst_ids]

            all_s3_urls = []
            chunk_size = 50

            for i in range(0, len(native_ids), chunk_size):
                chunk = native_ids[i:i + chunk_size]
                params = {
                    "provider": "ASF",
                    "ShortName": "OPERA_L2_CSLC-S1-STATIC_V1",
                    "native-id[]": chunk,
                    "options[native-id][pattern]": "true",
                    "page_size": len(chunk) * 2,
                    "token": token,
                }
                resp = req.get(
                    f"https://{cmr_hostname}/search/granules.umm_json",
                    params=params,
                    timeout=120,
                )
                resp.raise_for_status()
                items = resp.json().get("items", [])

                for item in items:
                    for url_entry in item.get("umm", {}).get("RelatedUrls", []):
                        url = url_entry.get("URL", "")
                        if url.startswith("s3://") and url.endswith(".h5"):
                            if url not in all_s3_urls:
                                all_s3_urls.append(url)

            if all_s3_urls:
                logger.info(f"Resolved {len(all_s3_urls)} static layer S3 URLs "
                            f"from CMR for frame {frame_id}")
                result = (True, sorted(all_s3_urls))
                self._static_layers_cache[frame_id] = result
                return result

            logger.warning(f"No static layer S3 URLs found in CMR for frame {frame_id}")
            return False, []

        except Exception as e:
            logger.warning(f"Failed to resolve static layers for frame {frame_id}: {e}")
            return False, []

    @staticmethod
    def _candidate_iono_filenames(doy, year, provider):
        """Return candidate uncompressed ionosphere filenames for a date.

        Covers both legacy and new naming conventions for FIN and RAP types.
        Ordered by preference (FIN before RAP, legacy before new).
        """
        yy = year[2:]
        p = provider.lower()
        # Map provider to legacy prefix
        legacy = {"jpl": ("jplg", "jprg"), "esa": ("esag", "esrg"),
                  "cod": ("codg", "corg")}
        fin_pfx, rap_pfx = legacy.get(p, ("jplg", "jprg"))
        # JPL uses 2-hour resolution; ESA/COD use 1-hour (per CDDIS naming convention)
        hour = "02" if p == "jpl" else "01"

        return [
            f"{fin_pfx}{doy}0.{yy}i",                                       # legacy FIN
            f"{provider}0OPSFIN_{year}{doy}0000_01D_{hour}H_GIM.INX",       # new FIN
            f"{rap_pfx}{doy}0.{yy}i",                                       # legacy RAP
            f"{provider}0OPSRAP_{year}{doy}0000_01D_{hour}H_GIM.INX",       # new RAP
        ]

    def _resolve_ionosphere_files(self, cslc_paths):
        """Download ionosphere files from CDDIS and upload to S3.

        For each unique acquisition date across the CSLC paths, downloads the
        corresponding ionosphere correction file from CDDIS and uploads it to
        the OPERA S3 bucket.  Skips dates whose ionosphere files already exist
        on S3 (checked via targeted head_object, not directory listing).

        Returns (satisfied, s3_urls).
        """
        try:
            import boto3
            from botocore.exceptions import ClientError
            from util.conf_util import SettingsConf
            from data_subscriber.ionosphere_download import download_ionosphere_correction_file
            from util.aws_util import concurrent_s3_client_try_upload_file
            from datetime import datetime

            settings = SettingsConf().cfg
            bucket = settings["DATASET_BUCKET"]
            provider = settings.get("IONEX_PROVIDER", "JPL")
            s3_prefix = "tmp/disp_s1/ionosphere"

            # Extract one representative CSLC path per unique date
            dates_to_path = {}
            for path in cslc_paths:
                filename = os.path.basename(path)
                date_match = re.search(r'_(\d{8})T', filename)
                if date_match and date_match.group(1) not in dates_to_path:
                    dates_to_path[date_match.group(1)] = path

            if not dates_to_path:
                logger.warning("No dates extracted from CSLC paths for ionosphere")
                return False, []

            # Check S3 for existing ionosphere files using targeted head_object
            # calls rather than listing the entire (flat) prefix.
            s3_client = boto3.client("s3")
            already_on_s3 = {}  # date_str -> s3_path
            dates_to_download = {}  # date_str -> cslc_path

            for date_str, cslc_path in dates_to_path.items():
                dt = datetime.strptime(date_str, "%Y%m%d")
                year = str(dt.year)
                doy = f"{dt.timetuple().tm_yday:03d}"
                candidates = self._candidate_iono_filenames(doy, year, provider)

                found = False
                for candidate in candidates:
                    key = f"{s3_prefix}/{candidate}"
                    try:
                        s3_client.head_object(Bucket=bucket, Key=key)
                        already_on_s3[date_str] = f"s3://{bucket}/{key}"
                        found = True
                        break
                    except ClientError:
                        continue

                if not found:
                    dates_to_download[date_str] = cslc_path

            if already_on_s3:
                logger.info(f"Found {len(already_on_s3)} ionosphere files already on S3, "
                            f"{len(dates_to_download)} to download")

            # Download missing ionosphere files
            downloads_dir = Path("downloads/ionosphere")
            downloads_dir.mkdir(parents=True, exist_ok=True)

            iono_files = []
            for date_str in sorted(dates_to_download):
                try:
                    iono_file = download_ionosphere_correction_file(
                        downloads_dir, dates_to_download[date_str]
                    )
                    if iono_file:
                        iono_files.append(Path(iono_file))
                except Exception as e:
                    logger.warning(f"Failed to download ionosphere for {date_str}: {e}")

            # Upload newly downloaded files
            new_s3_paths = []
            if iono_files:
                new_s3_paths = concurrent_s3_client_try_upload_file(
                    bucket=bucket,
                    key_prefix=s3_prefix,
                    files=iono_files,
                )

            all_s3_paths = sorted(set(list(already_on_s3.values()) + new_s3_paths))

            if not all_s3_paths:
                logger.warning("No ionosphere files resolved")
                return False, []

            logger.info(f"Resolved {len(all_s3_paths)} ionosphere S3 URLs "
                        f"({len(already_on_s3)} cached, {len(new_s3_paths)} new)")
            return True, all_s3_paths

        except Exception as e:
            logger.warning(f"Failed to resolve ionosphere files: {e}")
            return False, []

    def _re_evaluate_kscs_on_ccslc_publish(self, frame_id):
        """Re-evaluate KSCs for the frame whose compressed-CSLC rotation
        isn't final yet.

        Triggered when a CCSLC is ingested for the frame. Catches:

        - **Blocked** KSCs (all_cycles_complete=true but is_complete=false)
          that may now be unblocked by the new CCSLC.
        - **Pending-rotation** KSCs (is_complete=true but
          compressed_cslc_final=false) where the new CCSLC was on the
          pending list — re-evaluation drops it from pending and flips
          compressed_cslc_final to true, firing the SCIFLO trigger.

        KSCs whose ``compressed_cslc_final`` is already true are
        intentionally skipped: their SCIFLO trigger has already fired (or
        is about to), and re-evaluating them would risk a second trigger
        with a different compressed_cslc_ids snapshot — breaking the
        KSC↔L3 audit pairing opera-handel relies on.
        """
        try:
            # Force OS refresh so a sibling kce worker's earlier write to
            # compressed_cslc_final is visible to this query. Without this
            # the must_not filter on compressed_cslc_final=true can return
            # KSCs that have already been finalized within the last second.
            self._refresh_ksc_index()
            pending = query_kscs_pending_ccslc_rotation(self.es_conn, frame_id)

            if not pending:
                logger.info(
                    f"No KSCs with pending rotation for frame={frame_id}"
                )
                self._msg(
                    f"no pending KSCs",
                    f"No KSCs with pending rotation for frame={frame_id}",
                )
                return

            logger.info(
                f"Re-evaluating {len(pending)} non-final KSCs for frame={frame_id}"
            )
            self._msg(
                f"re-eval {len(pending)} non-final KSCs",
                f"Re-evaluating {len(pending)} non-final KSCs for frame={frame_id}",
            )

            for hit in pending:
                source = hit.get("_source", hit)
                meta = source.get("metadata", source)
                ksc_sensing_date = meta.get(c.SENSING_DATE)
                if ksc_sensing_date:
                    logger.info(
                        f"Re-evaluating non-final KSC for "
                        f"sensing_date={ksc_sensing_date}"
                    )
                    self._evaluate_k_cycle(
                        frame_id, ksc_sensing_date,
                        force_publish=True, cascade=False,
                    )

        # Intentionally non-fatal: a failed re-evaluation for one KSC should not
        # crash the evaluator. The on_ccslc trigger provides the retry mechanism.
        except Exception as e:
            logger.warning(
                f"Error during pending KSC re-evaluation: {e}"
            )

    def _re_evaluate_affected_kscs(self, frame_id, sensing_date):
        """Find incomplete KSCs containing this sensing_date and re-evaluate them.

        When a CSC becomes complete, other KSCs whose window includes this
        sensing_date may now also become complete.

        Also finds KSCs with stale windows — those with sensing_date after
        this date whose window has fewer than k entries.  This handles
        out-of-order CSLC ingestion: if CSLCs arrive for later dates first,
        their KSCs get created with incomplete windows; when earlier CSLCs
        arrive later, we re-evaluate those stale KSCs so their windows can
        now include the earlier dates.
        """
        try:
            # Force OS refresh so a sibling kce worker's recent KSC writes
            # are visible to the affected/stale queries before we decide
            # which KSCs to re-evaluate with force_publish=True.
            self._refresh_ksc_index()

            # Part 1: KSCs that already have this date in their window
            affected = query_incomplete_kscs_with_sensing_date(
                self.es_conn, frame_id, self.k, self.m, sensing_date,
                exclude_reference_date=sensing_date,
            )

            # Part 2: KSCs after this date with stale (incomplete) windows
            stale = query_stale_window_kscs(
                self.es_conn, frame_id, sensing_date, self.k,
            )

            # Merge, dedup by sensing_date
            seen = set()
            to_reeval = []
            for hit in (affected or []) + (stale or []):
                source = hit.get("_source", hit)
                meta = source.get("metadata", source)
                sd = meta.get(c.SENSING_DATE)
                if sd and sd not in seen:
                    seen.add(sd)
                    to_reeval.append(sd)

            if not to_reeval:
                return

            logger.info(f"Re-evaluating {len(to_reeval)} affected incomplete KSCs "
                        f"for frame={frame_id}, sensing_date={sensing_date}")
            self._msg(
                f"cascade {len(to_reeval)} KSCs",
                f"Cascade: re-evaluating {len(to_reeval)} incomplete KSCs "
                f"(window contains {sensing_date} or stale window)",
            )

            for ksc_sensing_date in to_reeval:
                logger.info(f"Re-evaluating KSC for sensing_date={ksc_sensing_date}")
                self._evaluate_k_cycle(
                    frame_id, ksc_sensing_date,
                    force_publish=True, cascade=False,
                )

        # Intentionally non-fatal: a failed re-evaluation for one KSC should not
        # crash the evaluator. The on_ccslc trigger provides the retry mechanism.
        except Exception as e:
            logger.warning(f"Error during cascade re-evaluation: {e}")

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
