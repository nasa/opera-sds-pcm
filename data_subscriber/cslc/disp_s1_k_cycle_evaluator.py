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
from pathlib import Path

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
    query_blocked_kscs_for_frame,
)
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    localize_frame_geo_json,
    get_bounding_box_for_frame,
    save_blocked_download_job,
)
from data_subscriber import es_conn_util
from util.common_util import backoff_wrapper, create_info_message_files

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
        self._static_layers_cache = {}  # frame_id -> (satisfied, s3_urls)
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
            # Input C: CCSLC ingested — re-evaluate blocked KSCs for this frame
            frame_id = metadata.get(c.FRAME_ID)
            logger.info(f"CCSLC ingested for frame={frame_id}. "
                        f"Re-evaluating blocked KSCs.")
            self._msg(
                f"CCSLC re-eval f{frame_id}",
                f"CCSLC ingested for frame={frame_id}, re-evaluating blocked KSCs",
            )
            self._re_evaluate_blocked_kscs(frame_id)
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

        # Skip logic: if KSC already exists with is_complete=true, skip
        if not force_publish:
            existing_metadata, _ = find_ksc(self.es_conn, ksc_id)
            if existing_metadata.get(c.IS_COMPLETE, False):
                logger.info(f"KSC {ksc_id} already complete. Skipping.")
                self._msg(
                    f"KSC already complete",
                    f"KSC {ksc_id} already complete, skipped",
                )
                return

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

        # Compute start_time from sensing_date
        start_time = (
            f"{sensing_date[:4]}-{sensing_date[4:6]}-{sensing_date[6:]}T00:00:00"
        )

        # Step 10: Create KSC
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

        # If all cycles complete but KSC still incomplete, save as blocked job
        if ksc_metadata.get(c.ALL_CYCLES_COMPLETE) and not ksc_metadata.get(c.IS_COMPLETE):
            logger.info(f"All cycles complete but KSC incomplete for "
                        f"{ksc_id} ({ksc_metadata.get(c.COMPLETENESS_REASON)}). "
                        f"Saving blocked job.")
            self._save_blocked_job(frame_id, sensing_date, ksc_id)

        # Step 11: Cascade re-evaluation of affected incomplete KSCs
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

        Returns (satisfied, ccslc_ids, ccslc_paths, completeness_detail).
        completeness_detail is a string describing the CCSLC state for
        inclusion in the KSC completeness_reason.
        """
        try:
            # Determine position using all known dates (CSCs + catalog)
            trigger_pos = self._get_date_position(frame_id, sensing_date)

            if trigger_pos is not None and trigger_pos < self.k:
                logger.info(f"Early window (position={trigger_pos} < k={self.k}). "
                            f"No prior CCSLCs required.")
                self._msg(
                    f"CCSLCs not required (early)",
                    f"CCSLCs: not required, early window "
                    f"(position={trigger_pos} < k={self.k})",
                )
                return True, [], [], "no CCSLCs required (early window)"

            # Query ES for compressed CSLCs.
            # needed = number of CCSLC *sets* (one set per prior k-window).
            # Each set has one CCSLC record per burst, so query enough to
            # cover needed sets × bursts_per_frame.
            needed_sets = self.m - 1
            if needed_sets <= 0:
                self._msg(
                    f"CCSLCs not required (m=1)",
                    f"CCSLCs: not required (m=1)",
                )
                return True, [], [], "no CCSLCs required (m=1)"

            frame = self.frame_to_bursts.get(frame_id)
            num_bursts = len(frame.burst_ids) if frame else 1
            query_size = needed_sets * num_bursts

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
                    "size": query_size,
                    "sort": [{"metadata.acquisition_cycle": {"order": "desc"}}],
                },
                index="grq_*_l2_cslc_s1_compressed*",
            )

            # Count distinct CCSLC sets by acquisition_cycle
            ccslc_ids = []
            ccslc_paths = []
            found_cycles = set()
            for r in (result or []):
                cycle = r.get("_source", {}).get("metadata", {}).get("acquisition_cycle")
                found_cycles.add(cycle)
                ccslc_ids.append(r["_id"])
                # Use the .h5 file path (not the HySDS dataset dir URL)
                product_s3_paths = r.get("_source", {}).get("metadata", {}).get("product_s3_paths", [])
                s3_url = product_s3_paths[0] if product_s3_paths else ""
                if s3_url:
                    ccslc_paths.append(s3_url)

            found_sets = len(found_cycles)

            if found_sets >= needed_sets:
                self._msg(
                    f"CCSLCs {found_sets}/{needed_sets} ok",
                    f"CCSLCs: {found_sets}/{needed_sets} sets satisfied",
                )
                return True, ccslc_ids, ccslc_paths, f"{found_sets} CCSLCs"

            # Position-based check: CCSLCs are produced at every k-th
            # position (k-1, 2k-1, ...).  For a date at position P,
            # at most P // k sets could exist from prior processing.
            # If we already have all that could exist, treat as satisfied.
            if trigger_pos is not None:
                max_possible_sets = trigger_pos // self.k
                if found_sets >= max_possible_sets:
                    not_expected = needed_sets - max_possible_sets
                    logger.info(
                        f"CCSLCs {found_sets}/{needed_sets} sets but "
                        f"{not_expected} not yet produced "
                        f"(position={trigger_pos}, "
                        f"max possible CCSLC sets: {max_possible_sets})"
                    )
                    detail = (
                        f"CCSLCs {found_sets}/{needed_sets}, "
                        f"{not_expected} not yet produced (pos {trigger_pos})"
                    )
                    self._msg(
                        f"CCSLCs {found_sets}/{needed_sets} ok",
                        f"CCSLCs: {detail}",
                    )
                    return True, ccslc_ids, ccslc_paths, detail

            self._msg(
                f"CCSLCs {found_sets}/{needed_sets} missing",
                f"CCSLCs: {found_sets}/{needed_sets} sets, not satisfied",
            )
            return False, [], [], f"CCSLCs {found_sets}/{needed_sets}"

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

    def _re_evaluate_blocked_kscs(self, frame_id):
        """Re-evaluate incomplete KSCs where all cycles are complete.

        Triggered when a CCSLC is ingested for this frame.  Queries for
        KSCs that are blocked (all_cycles_complete=true but is_complete=false)
        and re-evaluates each from scratch.
        """
        try:
            blocked = query_blocked_kscs_for_frame(self.es_conn, frame_id)

            if not blocked:
                logger.info(f"No blocked KSCs for frame={frame_id}")
                self._msg(
                    f"no blocked KSCs",
                    f"No blocked KSCs found for frame={frame_id}",
                )
                return

            logger.info(f"Re-evaluating {len(blocked)} blocked KSCs "
                        f"for frame={frame_id}")
            self._msg(
                f"re-eval {len(blocked)} blocked KSCs",
                f"Re-evaluating {len(blocked)} blocked KSCs for frame={frame_id}",
            )

            for hit in blocked:
                source = hit.get("_source", hit)
                meta = source.get("metadata", source)
                ksc_sensing_date = meta.get(c.SENSING_DATE)
                if ksc_sensing_date:
                    logger.info(f"Re-evaluating blocked KSC for "
                                f"sensing_date={ksc_sensing_date}")
                    self._evaluate_k_cycle(
                        frame_id, ksc_sensing_date,
                        force_publish=True, cascade=False,
                    )

        except Exception as e:
            logger.warning(f"Error during blocked KSC re-evaluation: {e}")

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
            self._msg(
                f"cascade {len(affected)} KSCs",
                f"Cascade: re-evaluating {len(affected)} incomplete KSCs "
                f"whose window contains sensing_date={sensing_date}",
            )

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
