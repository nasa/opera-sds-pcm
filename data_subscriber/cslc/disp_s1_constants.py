"""Constants for DISP-S1 evaluator state-config metadata fields."""

# State-config types (used as ES index suffix via grq_*_{type})
CSLC_S1_CYCLE_STATE_CONFIG = "cslc_s1-cycle-state-config"
DISP_S1_KCYCLE_STATE_CONFIG = "disp_s1-kcycle-state-config"

# Shared fields
STATE_CONFIG_TYPE = "state_config_type"
FRAME_ID = "frame_id"
ACQUISITION_CYCLE = "acquisition_cycle"
SENSING_DATE = "sensing_date"
IS_COMPLETE = "is_complete"
COMPLETENESS_REASON = "completeness_reason"

# CSC fields
# Orthogonal blackout marker: the acquisition falls inside the frame's
# blackout window (snow season). Burst-coverage completeness (IS_COMPLETE) is
# unaffected; downstream k-cycle logic excludes blacked-out cycles from
# DISP-S1 windows.
BLACKOUT = "blackout"
EXPECTED_BURST_IDS = "expected_burst_ids"
FOUND_BURST_IDS = "found_burst_ids"
MISSING_BURST_IDS = "missing_burst_ids"
CSLC_PRODUCT_PATHS = "cslc_product_paths"
COVERAGE_ACTUAL = "coverage_actual"
COVERAGE_EXPECTED = "coverage_expected"

# KSC fields
K = "k"
M = "m"
WINDOW_SENSING_DATES = "window_sensing_dates"
WINDOW_ENTRIES = "window_entries"
CYCLES_COMPLETE = "cycles_complete"
CYCLES_EXPECTED = "cycles_expected"
ALL_CYCLES_COMPLETE = "all_cycles_complete"
PRODUCT_PATHS = "product_paths"
COMPRESSED_CSLC_SATISFIED = "compressed_cslc_satisfied"
COMPRESSED_CSLC_IDS = "compressed_cslc_ids"
BOUNDING_BOX = "bounding_box"
SAVE_COMPRESSED_CSLC = "save_compressed_cslc"
FORCE_PUBLISH = "force_publish"
STATIC_LAYERS_SATISFIED = "static_layers_satisfied"
IONOSPHERE_SATISFIED = "ionosphere_satisfied"

# True when a partial CSC (expected_bursts > found_bursts) exists in this
# KSC's lineage — either currently in the window or already aged out since
# the most recent CCSLC boundary. Used by the trigger-SCIFLO_L3_DISP_S1 user_rule
# to block orphan disp_s1 jobs after partial dates age out of the k-cycle
# window.
GAP_UNRESOLVED = "gap_unresolved"

# Informational (never gates the trigger): the k-window contains a pair of
# consecutive sensing dates separated by more than the configured
# large-gap threshold — a real acquisition hole operators should know about.
LARGE_GAP = "large_gap"

# Generic state-config supersession marker. SUPERSEDED_BY value is a short
# string identifying what superseded the doc (e.g. "existing_ccslc" when a
# KSC's sensing_date matches the last_date of a CCSLC already in GRQ — the
# SCIFLO would re-emit duplicate L3 + CCSLC products). SUPERSEDED_AT is the
# wall-clock timestamp of the supersession. The trigger-SCIFLO_L3_DISP_S1
# user_rule treats `must_not exists superseded_by` as "skip this doc",
# leaving is_complete to retain its structural meaning. Extensible — add
# new short values when new supersession patterns are introduced.
SUPERSEDED_BY = "superseded_by"
SUPERSEDED_AT = "superseded_at"
# Recognised values for SUPERSEDED_BY.
SUPERSEDED_BY_EXISTING_CCSLC = "existing_ccslc"
# Set when the burst database's processing-mode annotations place the KSC's sensing_date inside a
# historical (or unprocessable no_run) phase: the historical batch job owns that date's L3 and
# compressed CSLC products, so the forward SCIFLO must not also fire for it.
SUPERSEDED_BY_HISTORICAL_PROCESSING = "historical_processing"

# Gate ensuring this KSC's compressed-CSLC rotation is locked-in before
# the SCIFLO can fire. ``compressed_cslc_pending`` lists the YYYYMMDD
# sensing_dates of earlier k-boundary KSCs (save_compressed_cslc=true,
# not superseded) whose CCSLC has not yet been published. When a CCSLC
# publishes, the KCE removes the matching date from each downstream KSC's
# pending list; when the list empties, ``compressed_cslc_final`` flips to
# True and the trigger-SCIFLO_L3_DISP_S1 user_rule fires. This guarantees the
# SCIFLO uses exactly the CCSLCs cached on the KSC, preserving the KSC ↔
# L3 audit trail used by opera-handel.
COMPRESSED_CSLC_PENDING = "compressed_cslc_pending"
COMPRESSED_CSLC_FINAL = "compressed_cslc_final"
