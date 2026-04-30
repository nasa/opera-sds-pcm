# cleanup_manifest.json — Operator Guide

## What is this file?

A JSON array of 204 frames that need cleanup due to the DISP-S1 consistent burst database (constDB) update. Each entry tells you exactly what to delete and what to reset for that frame.

## Totals

- 204 frames across priorities 0, 1, 2, 3a, 3b, 4
- 158 DISP-S1 GRQ products to delete
- 6,098 DISP-S1 CMR granules to delete
- 10,234 CCSLC GRQ products to delete
- 21 frames with no products to delete (frame_state reset only)

## Structure of each entry

```json
{
  "frame_id": 840,
  "priority": "4",
  "new_frame_state": 120,
  "affected_kcycle": 8,
  "kcycles_to_delete": [8, 9],
  "disp_grq_products": [...],
  "disp_cmr_granules": [...],
  "ccslc_grq_products": [...],
  "summary": {
    "disp_grq_count": 0,
    "disp_cmr_count": 2,
    "ccslc_grq_count": 7
  }
}
```

### Field descriptions

| Field | Description |
|-------|-------------|
| `frame_id` | The DISP-S1 frame number |
| `priority` | Processing priority group (0, 1, 2, 3a, 3b, 4) |
| `new_frame_state` | Value to set in the `batch_proc` ES doc after deletion |
| `affected_kcycle` | Earliest K-cycle affected by the constDB change |
| `kcycles_to_delete` | All K-cycles whose products must be deleted (from `affected_kcycle` through the last) |
| `disp_grq_products` | DISP-S1 products to delete from GRQ (current ops cluster) |
| `disp_cmr_granules` | DISP-S1 granules to delete from CMR (delivered to ASF DAAC) |
| `ccslc_grq_products` | Compressed CSLC products to delete from GRQ (not in CMR) |
| `summary` | Product counts per category for quick reference |

### Product fields

**`disp_grq_products`** entries:
- `id` — GRQ document ID (also the product name)
- `index` — ES index name (e.g. `grq_v1.0_l3_disp_s1-2026.02`)
- `s3_dir` — S3 dataset directory to delete

**`disp_cmr_granules`** entries:
- `title` — CMR granule title
- `concept_id` — CMR concept ID (e.g. `G3629098434-ASF`)
- `s3_dir` — S3 path at the DAAC (for reference; DAAC manages this)

**`ccslc_grq_products`** entries:
- `id` — GRQ document ID
- `index` — ES index name (e.g. `grq_1_l2_cslc_s1_compressed-2025.07`)
- `s3_dir` — S3 dataset directory to delete

## Cleanup procedure per frame

For each entry in the manifest:

### Step 1 — Delete DISP-S1 products from GRQ

For each item in `disp_grq_products`:
1. Delete the S3 directory: `aws s3 rm --recursive <s3_dir>`
2. Delete the ES document: `DELETE /<index>/_doc/<id>` on the GRQ ES endpoint

### Step 2 — Delete DISP-S1 granules from CMR

For each item in `disp_cmr_granules`:
1. Delete the granule from CMR using `concept_id`
2. The DAAC (ASF) manages the S3 side — coordinate with them if needed

### Step 3 — Delete CCSLC products from GRQ

For each item in `ccslc_grq_products`:
1. Delete the S3 directory: `aws s3 rm --recursive <s3_dir>`
2. Delete the ES document: `DELETE /<index>/_doc/<id>` on the GRQ ES endpoint

### Step 4 — Reset frame_state

Update the `batch_proc` ES document for this frame:
- Set `frame_states.<frame_id>` to `new_frame_state`
- This tells historical processing to restart from the earliest affected K-cycle

### Step 5 — Re-run historical processing

Once products are deleted and frame_state is reset, historical processing will regenerate the affected K-cycles using the updated constDB.

## Frames with no products

21 frames have empty product lists (all three counts are 0). These only need the frame_state reset (Step 4). They represent K-cycles that haven't been processed yet on the current cluster.

## Notes

- CCSLCs are not delivered to CMR — only GRQ deletion is needed for them
- Some frames have DISP products only in CMR (not in GRQ) because they were processed on a previous cluster
- The `priority` field can be used to stage the cleanup in batches if preferred
- Frame 30966 has an empty `kcycles_to_delete` list (new frame with 0 prior sensing times) — it only needs frame_state set to 0
