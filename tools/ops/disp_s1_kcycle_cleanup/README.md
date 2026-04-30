# DISP-S1 K-Cycle Cleanup Tools

Tools for identifying and preparing cleanup of DISP-S1 and CCSLC products
affected by changes to the DISP-S1 consistent burst database (constDB).

When the constDB is updated and adds new sensing times to a frame, all DISP-S1
products and CCSLCs derived from K-cycles at or after the inserted index must be
deleted and regenerated. These tools identify exactly what to delete on a
per-frame basis.

## Background

A frame's `frame_state` is a 0-based index into its sensing-time list. K-cycle
N covers indices `[N×K, (N+1)×K)` (with K=15 for DISP-S1). When a new sensing
time is inserted at index `i`, the affected K-cycle is `i // K`, and every
K-cycle from there to the end of the list is invalidated because indices shift.

To clean up:
1. Identify affected K-cycles per frame
2. Find DISP-S1 products on GRQ + CMR for those K-cycles
3. Find CCSLC products on GRQ for those K-cycles
4. Reset `frame_state` to `K × affected_kcycle` so historical processing
   restarts from the right place
5. Re-run historical processing

## Pipeline Overview

```
diff_priority*.txt + old constDB
        │
        ▼
  find_affected_kcycles.py        # Step 1
        │
        ▼
  affected_kcycles.json
        │
        ├──── validate_kcycles.py  # (optional) sanity-check the math
        │
        ▼
  query_products_for_deletion.py  # Step 2 (queries GRQ + CMR)
        │
        ▼
  disp_products_to_delete.json
  ccslc_products_to_delete.json
  frames_no_products.json
        │
        ▼
  merge_cleanup_artifacts.py      # Step 3
        │
        ▼
  cleanup_manifest.json           # Final operator document
```

## Scripts

### `find_affected_kcycles.py`

**Purpose**: Parse `diff_priority*.txt` files and identify which K-cycle is
the earliest affected one for each frame that gained sensing times.

**Logic**:
- Parses each diff file for `<<< FILE2 MORE` blocks (frames where the new
  constDB has more sensing times than the old one)
- For each added sensing time, uses `bisect.bisect_left` on the old constDB
  list to find the would-be insertion index
- Computes `affected_kcycle = insertion_index // K`
- All K-cycles from `affected_kcycle` to the last existing K-cycle are
  marked for deletion (because indices shift downstream)
- Computes `new_frame_state = K × affected_kcycle`

**Inputs**:
- `diff_priority*.txt` files in the same directory as the script
- The old constDB JSON at `<parent>/disp_s1_consistent_burst_db/opera-disp-s1-consistent-burst-ids-2025-06-30-2016-07-01_to_2024-12-31.json`

**Output**: `affected_kcycles.json` — list of entries like:
```json
{
  "frame_id": 3072,
  "priority": "3a",
  "earliest_added_sensing_time": "2023-01-16T12:50:12",
  "insertion_index": 186,
  "affected_kcycle": 12,
  "total_sensing_times": 232,
  "total_kcycles": 16,
  "all_added_sensing_times": ["2023-01-16T12:50:12"],
  "kcycles_to_delete": [12, 13, 14, 15],
  "new_frame_state": 180
}
```

### `validate_kcycles.py` (optional)

**Purpose**: Sanity-check `affected_kcycles.json` — verifies
`new_frame_state == affected_kcycle × K` and that `new_frame_state` is a
multiple of K for every frame.

### `query_products_for_deletion.py`

**Purpose**: For each frame in `affected_kcycles.json`, query GRQ and CMR
to find every DISP-S1 and CCSLC product whose `acquisition_cycle` (a day
index from the first sensing time) falls within the affected K-cycles.

**Important**: K-cycles are indexed by sensing-time position, but products
are indexed by **day_index**. The script:
1. Loads the old constDB to get the sensing time → day_index mapping
2. For each affected K-cycle, computes the minimum day_index covering it
3. Queries GRQ/CMR for products with `acquisition_cycle >= min_day_index`

**Sources**:
- GRQ OpenSearch (default `http://localhost:9201`):
  - `grq_v1.0_l3_disp_s1-*` for DISP-S1 products (current cluster only)
  - `grq_1_l2_cslc_s1_compressed-*` for CCSLCs
- CMR (`https://cmr.earthdata.nasa.gov/search`):
  - Collection `C3294057315-ASF` (OPERA_L3_DISP-S1_V1) — covers all clusters
- CCSLCs are not delivered to CMR; only GRQ deletion needed

**Outputs**:
- `disp_products_to_delete.json` — DISP-S1 from GRQ + CMR
- `ccslc_products_to_delete.json` — CCSLCs from GRQ
- `frames_no_products.json` — frames with nothing to delete (state reset only)

### `merge_cleanup_artifacts.py`

**Purpose**: Merge the 3 source files into a single per-frame operator
manifest. Each entry is self-contained: what to delete, what to set
`frame_state` to.

**Output**: `cleanup_manifest.json` — list of per-frame entries (see
`CLEANUP_MANIFEST_README.md` for the structure).

## How to run

### Prerequisites

1. **SSH tunnel** to the ops cluster GRQ ES (port 9201 by default):
   ```bash
   ssh -L 9201:<grq-host>:9200 -i <keypair>.pem hysdsops@<mozart-ip>
   ```

2. **Working directory** containing:
   - The 6 `diff_priority*.txt` files (output from comparing old vs new constDB)
   - A sibling directory `disp_s1_consistent_burst_db/` with the old constDB JSON
   - Copies of the scripts (or run them in place from their checked-out location)

   Layout:
   ```
   working_dir/
   ├── diff_priority0.txt
   ├── diff_priority1.txt
   ├── diff_priority2.txt
   ├── diff_priority3a.txt
   ├── diff_priority3b.txt
   ├── diff_priority4.txt
   ├── find_affected_kcycles.py
   ├── validate_kcycles.py
   ├── query_products_for_deletion.py
   └── merge_cleanup_artifacts.py
   ../disp_s1_consistent_burst_db/
       └── opera-disp-s1-consistent-burst-ids-2025-06-30-2016-07-01_to_2024-12-31.json
   ```

   Note: the scripts assume the constDB lives in a sibling directory one level
   up from the scripts. Adjust `OLD_CONSTDB` in
   `find_affected_kcycles.py` and `query_products_for_deletion.py` if your
   layout differs.

3. **Python 3.9+** with stdlib only (no extra packages needed — uses
   `urllib`, `json`, `bisect`).

### Run the pipeline

```bash
# Step 1: identify affected K-cycles per frame
python find_affected_kcycles.py

# Optional: sanity-check the result
python validate_kcycles.py

# Step 2: query GRQ + CMR for products to delete (slowest step)
python query_products_for_deletion.py

# Step 3: merge into the final operator manifest
python merge_cleanup_artifacts.py
```

The final `cleanup_manifest.json` is what an operator uses to do the actual
deletion. It groups all the inputs into one self-contained per-frame document.

## Cleanup procedure (for the operator)

For each entry in `cleanup_manifest.json`:

1. **Delete DISP-S1 GRQ products** (`disp_grq_products`):
   - `aws s3 rm --recursive <s3_dir>`
   - `DELETE /<index>/_doc/<id>` on GRQ ES
2. **Delete DISP-S1 CMR granules** (`disp_cmr_granules`):
   - Delete by `concept_id` from CMR
   - Coordinate with ASF DAAC for the S3 side if needed
3. **Delete CCSLC GRQ products** (`ccslc_grq_products`):
   - `aws s3 rm --recursive <s3_dir>`
   - `DELETE /<index>/_doc/<id>` on GRQ ES
4. **Reset frame_state**: in the `batch_proc` ES doc that contains this frame,
   set `frame_states.<frame_id>` to `new_frame_state`
5. **Re-run historical processing** — the next run will regenerate the
   affected K-cycles using the updated constDB

Frames with empty product lists (all 3 counts are 0) only need step 4.

## Notes & caveats

- **K-cycle math**: `new_frame_state = K × affected_kcycle`, where K=15 for
  DISP-S1. The formula assumes every K-cycle has K sensing times except
  possibly the last (partial cycle).
- **Day-index vs sensing-time-index**: K-cycles are by position in the
  sensing-time list, but products are indexed by day-index in CMR/GRQ. The
  query script handles the conversion using the constDB.
- **CCSLCs vs CMR**: CCSLCs are not delivered to the DAAC; only GRQ deletion
  is needed.
- **Previous-cluster products**: some frames have DISP products in CMR but
  not in GRQ — those were processed on a prior cluster and only exist at the
  DAAC.
- **Frames with no prior sensing times**: e.g., frame 30966 in the original
  run — `affected_kcycle=0`, `kcycles_to_delete=[]`. Both
  `query_products_for_deletion.py` and `merge_cleanup_artifacts.py` skip
  these gracefully; they just need `frame_state=0` set.
- **constDB path is hardcoded**: edit the `OLD_CONSTDB` constant near the
  top of `find_affected_kcycles.py` and `query_products_for_deletion.py`
  if your constDB file lives elsewhere or has a different name.
- **GRQ URL is hardcoded**: edit `GRQ_URL` near the top of
  `query_products_for_deletion.py` if you're not using the default
  SSH-tunnel layout.

## Reference run (2026-04 cleanup)

For the constDB update covering priorities 0, 1, 2, 3a, 3b, 4:

| Metric | Count |
|--------|------:|
| Affected frames | 204 |
| DISP-S1 GRQ products to delete | 158 |
| DISP-S1 CMR granules to delete | 6,098 |
| CCSLC GRQ products to delete | 10,234 |
| Frames with nothing to delete (state reset only) | 21 |

## Related docs

- `docs/disp_s1_kcycle_cleanup_new_frame_state.md` (in the `~/dev/opera/`
  workspace, not this repo) — diagrams explaining the `new_frame_state`
  formula and end-to-end flow
