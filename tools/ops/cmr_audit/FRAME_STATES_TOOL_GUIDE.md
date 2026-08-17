# DISP-S1 Frame States Tool Guide

This guide explains how to use the CMR audit tool to extract frame states from CMR and compare them with batch proc configurations.

## Table of Contents

- [Overview](#overview)
- [Use Cases](#use-cases)
- [Prerequisites](#prerequisites)
- [Tool 1: CMR Audit with Frame States Export](#tool-1-cmr-audit-with-frame-states-export)
  - [Basic Usage](#basic-usage)
  - [Command-Line Options](#command-line-options)
  - [Examples](#examples)
  - [Output Format](#output-format)
- [Tool 2: Frame States Comparison](#tool-2-frame-states-comparison)
  - [Basic Usage](#basic-usage-1)
  - [Example Output](#example-output)
  - [Interpreting Results](#interpreting-results)
- [Tool 3: Diagnose Frame Products](#tool-3-diagnose-frame-products)
  - [Basic Usage](#basic-usage-2)
  - [Command-Line Options](#command-line-options-1)
  - [Example Output](#example-output-1)
  - [When to Use This Tool](#when-to-use-this-tool)
- [Common Workflows](#common-workflows)
  - [Workflow 1: Resume Historical Processing After System Restart](#workflow-1-resume-historical-processing-after-system-restart)
  - [Workflow 2: Audit Processing Progress](#workflow-2-audit-processing-progress)
  - [Workflow 3: Create New Batch Proc from CMR State](#workflow-3-create-new-batch-proc-from-cmr-state)
- [Troubleshooting](#troubleshooting)
- [Technical Details](#technical-details)

---

## Overview

The DISP-S1 historical processing system tracks progress using **frame states** stored in Elasticsearch. Each frame state represents the position in the sensing time list that has been processed.

These tools allow you to:

1. **Extract frame states from CMR** - Query CMR to determine what DISP-S1 products have actually been published
2. **Compare frame states** - Compare CMR state with batch proc configuration to identify discrepancies
3. **Diagnose frame products** - Investigate specific frames to identify products causing unexpected frame states

---

## Use Cases

| Scenario | Solution |
|----------|----------|
| System crashed and batch proc state is unknown | Use `--frame-states-only` for fast frame state extraction |
| GRQ database is empty but products exist in CMR | Use `--frame-states-only` or `--burst-data-source cmr` |
| Need to verify batch proc matches actual progress | Use comparison tool |
| Creating new batch proc for existing frames | Export frame states from CMR with `--frame-states-only` |
| Debugging why processing isn't progressing | Compare expected vs actual states |
| Frame state is not a multiple of k | Use diagnose tool to identify problematic products |
| Verify DISP-S1 products used correct input CSLCs | Run default (no flags) or use `--burst-data-source cmr` |

---

## Prerequisites

1. **Python environment** with required dependencies installed
2. **Network access** to CMR (cmr.earthdata.nasa.gov)
3. **PYTHONPATH** set correctly if running outside the deployed environment:

```bash
export PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH
```

---

## Tool 1: CMR Audit with Frame States Export

### Basic Usage

For full burst-level audit (default, backwards compatible):

```bash
# Default: Uses GRQ Elasticsearch for burst data
python cmr_audit_disp_s1.py \
    --start-datetime <START_DATE> \
    --end-datetime <END_DATE> \
    --processing-mode historical \
    --frames-only <FRAME_LIST>

# Using CMR ISO XML for burst data (when GRQ is not available)
python cmr_audit_disp_s1.py \
    --start-datetime <START_DATE> \
    --end-datetime <END_DATE> \
    --processing-mode historical \
    --frames-only <FRAME_LIST> \
    --burst-data-source cmr
```

For extracting frame states only (fast, no burst-level validation):

```bash
python cmr_audit_disp_s1.py \
    --start-datetime <START_DATE> \
    --end-datetime <END_DATE> \
    --processing-mode historical \
    --frames-only <FRAME_LIST> \
    --output-frame-states <OUTPUT_FILE> \
    --frame-states-only
```

### Command-Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--start-datetime` | Yes | Start date for query (ISO 8601 format: `YYYY-MM-DDTHH:MM:SSZ`) |
| `--end-datetime` | Yes | End date for query (ISO 8601 format) |
| `--processing-mode` | Yes | Must be `historical`, `forward`, or `reprocessing` |
| `--frames-only` | No | Comma-separated list of frame IDs to query |
| `--output-frame-states` | No | Path to output JSON file for frame states |
| `--frame-states-only` | No | Skip burst-level validation, only extract frame-level metadata from CMR. Cannot be combined with `--burst-data-source` |
| `--burst-data-source` | No | Source for burst/lineage data: `grq` (default, fast, needs GRQ) or `cmr` (slower, fetches ISO XML). Ignored when `--frame-states-only` is used |
| `--k` | No | K parameter (default: 15) |
| `--validate-with-grq` | No | Query GRQ instead of CMR for DISP-S1 product list (not burst data) |
| `--log-level` | No | Logging level: DEBUG, INFO, WARNING, ERROR |

### Examples

#### Example 1: Extract frame states for multiple frames (frame-states-only mode)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860,18902,18903,1088,1089 \
    --output-frame-states frame_states_output.json \
    --frame-states-only
```

**Console Output:**

```
INFO: Performing DISP-S1 audit
INFO: Frame-states-only mode: Extracting DISP-S1 metadata directly from CMR (skipping burst-level validation)
INFO: Using user-provided date range: 2016-07-01 00:00:00 to 2025-12-01 00:00:00
INFO: Frames to validate: {1088, 1089, 10860, 18902, 18903}
INFO: Found 1590 DISP-S1 products in CMR
INFO: Extracted metadata for 1590 DISP-S1 products from CMR
INFO: Frame-states-only mode: Skipping detailed validation (no burst-level matching)
INFO: Fully published (granules) (DISP-S1): len(disp_s1_products)=1,590
INFO: Missing (granules) (DISP-S1): len(disp_s1_products_miss)=0
INFO: Frame-states-only mode. Skipping missing products file generation.
INFO: Calculating expected frame states from audit results...
INFO: Frame 1088: max_acq_day_index=2928, index_position=344, frame_state=345
INFO: Frame 1089: max_acq_day_index=2940, index_position=344, frame_state=345
INFO: Frame 10860: max_acq_day_index=3084, index_position=359, frame_state=360
INFO: Frame 18902: max_acq_day_index=3048, index_position=314, frame_state=315
INFO: Frame 18903: max_acq_day_index=3072, index_position=314, frame_state=315
INFO: Frame states written to frame_states_output.json
INFO: Total frames: 5, With products: 5, Without products: 0
```

#### Example 2: Large-scale frame state extraction with many frames

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 14884,14883,14882,18907,18908,14878,14881,42269,42268,42267,42266,42265,42264,42263,42262,7090,7091,7092,7093,7094,7095,7096,7097,14877,14879,14880,10854,10855,10856,34481,34480,34479,34478,34477,26692,26693,26694,26695,26697 \
    --output-frame-states frame_states_output-priority_1.json \
    --frame-states-only
```

**Console Output (abbreviated):**

```
INFO: Performing DISP-S1 audit
INFO: Frame-states-only mode: Extracting DISP-S1 metadata directly from CMR (skipping burst-level validation)
INFO: Using user-provided date range: 2016-07-01 00:00:00 to 2025-12-01 00:00:00
INFO: Frames to validate: {7090, 7091, 7092, 7093, 7094, 7095, 7096, 7097, 10854, 10855, 10856, ...}
INFO: Found 36635 DISP-S1 products in CMR
INFO: Extracted metadata for 36635 DISP-S1 products from CMR
INFO: Frame-states-only mode: Skipping detailed validation (no burst-level matching)
INFO: Fully published (granules) (DISP-S1): len(disp_s1_products)=36,635
INFO: Missing (granules) (DISP-S1): len(disp_s1_products_miss)=0
INFO: Burst audit not requested. Skipping missing products file generation.
INFO: Calculating expected frame states from audit results...
INFO: Frame 7090: max_acq_day_index=2952, index_position=209, frame_state=210
INFO: Frame 7091: max_acq_day_index=2952, index_position=209, frame_state=210
INFO: Frame 7092: max_acq_day_index=2928, index_position=239, frame_state=240
...
INFO: Frame 42268: max_acq_day_index=2988, index_position=239, frame_state=240
INFO: Frame 42269: max_acq_day_index=2988, index_position=239, frame_state=240
INFO: Frame states written to frame_states_output-priority_1.json
INFO: Total frames: 175, With products: 175, Without products: 0
```

#### Example 3: Full burst-level audit using CMR ISO XML (when GRQ not available)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2024-01-01T00:00:00Z \
    --end-datetime 2024-03-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860 \
    --burst-data-source cmr
```

**Console Output (abbreviated):**

```
INFO: Performing DISP-S1 audit
...
Querying CMR for time range 2024-01-01T00:00:00Z to 2024-03-01T00:00:00Z.
Fetching granules: 100%|████████████████████████████████████████████████| 4/4 [00:00<00:00, 26.02it/s]
Granule fetching complete.
...
INFO: Total number of DISP-S1 products that should have been generated: 4
INFO: Earliest acquisition date: 2024-01-13 12:34:29, Latest acquisition date: 2024-02-18 12:34:52
INFO: Using CMR ISO XML for burst/lineage data
INFO: Found 4 DISP-S1 products in CMR
INFO: Processing ISO XML for product 1/4: OPERA_L3_DISP-S1_IW_F10860_VV_20230927T123432Z_20240206T123428Z_v1.0_20250624T033524Z
INFO: Extracted metadata for 4 DISP-S1 products from ISO XML
INFO: Frame 10860 Acq Index 2760 K-Set 22 is not k-complete so will ignore during validation.
INFO: Frame 10860 Acq Index 2832, which is the last acq index in that k-set to cover the DISP-S1 products.
...
INFO: Fully published (granules) (DISP-S1): len(disp_s1_products)=4
INFO: Missing (granules) (DISP-S1): len(disp_s1_products_miss)=0
```

#### Example 4: Full burst-level audit using GRQ (default, backwards compatible)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2024-01-01T00:00:00Z \
    --end-datetime 2024-03-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860
```

This runs the full burst-level audit using GRQ Elasticsearch (the default and backwards-compatible behavior).

**Note:** If GRQ Elasticsearch doesn't have the DISP-S1 products, you'll see a warning with a suggestion to use `--burst-data-source cmr` instead:

```
WARNING: NO DISP-S1 PRODUCTS FOUND IN GRQ ELASTICSEARCH DATABASE
WARNING: This typically happens when:
WARNING:   1. The GRQ database is empty or doesn't contain DISP-S1 products
WARNING:   2. The products exist in CMR but haven't been ingested into GRQ
WARNING:   3. You're running against a different environment than where products were generated
WARNING: SUGGESTION: Try using --burst-data-source cmr to fetch data from CMR instead
```

### Output Format

The `--output-frame-states` option produces a JSON file with the following structure:

**Example `frame_states_output.json`:**

```json
{
    "frame_states": {
        "1088": 345,
        "1089": 345,
        "3611": 195,
        "5118": 135,
        "5653": 360,
        "5654": 360,
        "5655": 345,
        "8887": 345,
        "10860": 360,
        "14885": 240,
        "15422": 375,
        "15423": 375,
        "16938": 135,
        "16941": 300,
        "16942": 300,
        "18902": 315,
        "18903": 315,
        "20688": 345,
        "23210": 345,
        "23211": 360,
        "24721": 75,
        "25254": 360,
        "33038": 375,
        "33039": 375,
        "33040": 375,
        "34482": 240,
        "38246": 300,
        "38247": 300,
        "38502": 315,
        "42809": 270,
        "42810": 270,
        "44855": 90,
        "46292": 300
    },
    "k": 15,
    "audit_start_date": "2016-07-01T00:00:00Z",
    "audit_end_date": "2025-12-01T00:00:00Z",
    "processing_mode": "historical",
    "total_frames": 33,
    "frames_with_products": 33,
    "frames_without_products": 0
}
```

| Field | Description |
|-------|-------------|
| `frame_states` | Dictionary mapping frame ID (string) to frame state (integer) |
| `k` | K parameter used for the audit |
| `audit_start_date` | Start date used for the CMR query |
| `audit_end_date` | End date used for the CMR query |
| `processing_mode` | Processing mode used |
| `total_frames` | Total number of frames in the output |
| `frames_with_products` | Number of frames that have at least one product |
| `frames_without_products` | Number of frames with no products (state = 0) |

---

## Tool 2: Frame States Comparison

### Basic Usage

```bash
python compare_disp_s1_frame_states.py <cmr_audit_output.json> <batch_proc.json>
```

### Example Output

```bash
python compare_disp_s1_frame_states.py frame_states_output-priority_1.json ~/DISP-S1/catchup/priority_1.json
```

**Output:**

```
====================================================================================================
FRAME STATE COMPARISON
====================================================================================================
CMR Audit File:  frame_states_output-priority_1.json
Batch Proc File: /export/home/hysdsops/DISP-S1/catchup/priority_1.json
K value: 15
====================================================================================================

  Frame ID |    CMR State |  Batch State |   Difference |  K-Cycles Diff | Status
----------------------------------------------------------------------------------------------------
      1093 |          225 |          225 |            0 |              0 | MATCH ✓
      1094 |          225 |          225 |            0 |              0 | MATCH ✓
      1095 |          225 |          225 |            0 |              0 | MATCH ✓
      1096 |          225 |          225 |            0 |              0 | MATCH ✓
      1097 |          225 |          225 |            0 |              0 | MATCH ✓
      1098 |          225 |          225 |            0 |              0 | MATCH ✓
      1099 |          225 |          225 |            0 |              0 | MATCH ✓
      1100 |          225 |          225 |            0 |              0 | MATCH ✓
      1101 |          225 |          225 |            0 |              0 | MATCH ✓
      1102 |           90 |           90 |            0 |              0 | MATCH ✓
      3060 |           75 |           75 |            0 |              0 | MATCH ✓
      3061 |           75 |           75 |            0 |              0 | MATCH ✓
      ...
      8622 |          242 |          240 |           +2 |           +0.1 | CMR AHEAD
      8881 |          225 |          225 |            0 |              0 | MATCH ✓
      8882 |          225 |          225 |            0 |              0 | MATCH ✓
      ...
     46293 |          300 |          300 |            0 |              0 | MATCH ✓
----------------------------------------------------------------------------------------------------

====================================================================================================
SUMMARY
====================================================================================================
Total frames compared: 175
  Matching:            174
  CMR ahead:           1
  Batch ahead:         0
  Only in CMR audit:   0
  Only in batch proc:  0

INTERPRETATION:
  - 1 frame(s) have MORE products in CMR than batch proc expects.
    This means the batch proc is behind and should be updated to continue from where CMR left off.

To update batch proc with CMR frame states, copy the 'frame_states' from the CMR audit output.
====================================================================================================
```

### Interpreting Results

| Status | Meaning | Action |
|--------|---------|--------|
| **MATCH ✓** | CMR and batch proc agree | No action needed |
| **CMR AHEAD** | More products in CMR than batch proc expects | Update batch proc to CMR state to avoid reprocessing |
| **BATCH AHEAD** | Batch proc is ahead of CMR | Investigate: jobs may have failed or products not published |
| **ONLY IN CMR** | Frame exists in CMR audit but not in batch proc | Add frame to batch proc if needed |
| **ONLY IN BATCH** | Frame exists in batch proc but not in CMR audit | May be expected if frame wasn't in audit query |

---

## Tool 3: Diagnose Frame Products

This tool investigates DISP-S1 products for a specific frame to identify products causing unexpected frame state values (e.g., non-multiples of k).

### Basic Usage

```bash
python diagnose_disp_s1_frame_products.py --frame <FRAME_ID>
```

### Command-Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--frame` | Yes | Frame ID to diagnose |
| `--start-datetime` | No | Start date for CMR query (default: 2016-07-01T00:00:00Z) |
| `--end-datetime` | No | End date for CMR query (default: 2025-12-01T00:00:00Z) |
| `--k` | No | K parameter (default: 15) |
| `--show-last` | No | Number of most recent products to display (default: 20) |

### Example Output

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python diagnose_disp_s1_frame_products.py --frame 8622
```

**Output:**

```
Loading DISP burst map...
Querying CMR for frame 8622 DISP-S1 products...
  Date range: 2016-07-01T00:00:00Z to 2025-12-01T00:00:00Z

Found 239 DISP-S1 products for frame 8622

Frame 8622 info:
  Total sensing times in database: 243
  First sensing time: 2016-07-16 22:50:42
  Last sensing time: 2024-12-25 22:51:20

========================================================================================================================
ANALYSIS FOR FRAME 8622
========================================================================================================================
Highest index position found: 241
This means frame_state = 242
Expected k-aligned state (k=15): 240 or 255
Is frame_state a multiple of k? NO

⚠️  Frame state 242 is NOT a multiple of 15
   It is 2 positions past the last complete k-cycle boundary (240)
   This suggests 2 product(s) from an incomplete k-cycle or from forward/reprocessing

========================================================================================================================
LAST 20 DISP-S1 PRODUCTS FOR FRAME 8622 (sorted by sensing time index, descending)
========================================================================================================================
 Index Pos |  K-Cycle | Pos in K |  Day Index |                  End Date | Product ID
------------------------------------------------------------------------------------------------------------------------
       241 |       16 |        1 |       3072 |      2024-12-13T22:51:21Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241213
       240 |       16 |        0 |       3060 |      2024-12-01T22:51:22Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241201
       239 |       15 |       14 |       3048 |      2024-11-19T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241119
       238 |       15 |       13 |       3036 |      2024-11-07T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241107
       237 |       15 |       12 |       3024 |      2024-10-26T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241026
       236 |       15 |       11 |       3012 |      2024-10-14T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241014
       235 |       15 |       10 |       3000 |      2024-10-02T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241002
       234 |       15 |        9 |       2988 |      2024-09-20T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240920
       233 |       15 |        8 |       2976 |      2024-09-08T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240908
       232 |       15 |        7 |       2964 |      2024-08-27T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240827
       231 |       15 |        6 |       2952 |      2024-08-15T22:51:22Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240815
       230 |       15 |        5 |       2940 |      2024-08-03T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240803
       229 |       15 |        4 |       2928 |      2024-07-22T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240722
       228 |       15 |        3 |       2916 |      2024-07-10T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240710
       227 |       15 |        2 |       2904 |      2024-06-28T22:51:23Z | OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20240628
       226 |       15 |        1 |       2892 |      2024-06-16T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20231219T225125Z_20240616
       225 |       15 |        0 |       2880 |      2024-06-04T22:51:24Z | OPERA_L3_DISP-S1_IW_F08622_VV_20231219T225125Z_20240604
       224 |       14 |       14 |       2868 |      2024-05-23T22:51:25Z | OPERA_L3_DISP-S1_IW_F08622_VV_20231219T225125Z_20240523
       223 |       14 |       13 |       2856 |      2024-05-11T22:51:25Z | OPERA_L3_DISP-S1_IW_F08622_VV_20231219T225125Z_20240511
       222 |       14 |       12 |       2844 |      2024-04-29T22:51:25Z | OPERA_L3_DISP-S1_IW_F08622_VV_20231219T225125Z_20240429
------------------------------------------------------------------------------------------------------------------------

========================================================================================================================
PRODUCTS BEYOND LAST COMPLETE K-CYCLE (index >= 240)
These products are causing the non-k-aligned frame state
========================================================================================================================
Index 240 (K-cycle 16, position 0 within cycle)
  Product: OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241201T225122Z_v1.0_20250420T151945Z
  End date: 2024-12-01T22:51:22Z
  Day index: 3060

Index 241 (K-cycle 16, position 1 within cycle)
  Product: OPERA_L3_DISP-S1_IW_F08622_VV_20240616T225124Z_20241213T225121Z_v1.0_20250420T151945Z
  End date: 2024-12-13T22:51:21Z
  Day index: 3072
```

### When to Use This Tool

Use this tool when:

1. **Frame state is not a multiple of k** - The comparison tool shows a frame state like 242 instead of 240 or 255
2. **Investigating CMR AHEAD discrepancies** - To identify exactly which products are causing the mismatch
3. **Debugging forward/reprocessing overlap** - To see if forward processing products got mixed with historical products
4. **Understanding product distribution** - To see the k-cycle breakdown of products for a frame

The output clearly identifies:
- The highest index position found and resulting frame state
- Whether the frame state aligns to k boundaries
- The specific products that are beyond the last complete k-cycle (the "problematic" ones)

---

## Common Workflows

### Workflow 1: Resume Historical Processing After System Restart

When the batch processing system is restarted and you need to continue from where it left off:

```bash
# Step 1: Extract current state from CMR (fast, frame-states-only)
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860,18904,18905 \
    --output-frame-states cmr_frame_states.json \
    --frame-states-only

# Step 2: Compare with existing batch proc (optional)
python compare_disp_s1_frame_states.py cmr_frame_states.json /path/to/batch_proc.json

# Step 3: Update batch proc with CMR frame states
# Manually copy the "frame_states" from cmr_frame_states.json to your batch_proc.json
# Or use the CMR frame states to create a new batch proc
```

### Workflow 2: Audit Processing Progress

To verify that processing is on track:

```bash
# Step 1: Get current CMR state (fast, frame-states-only)
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860,18904,18905 \
    --output-frame-states audit_results.json \
    --frame-states-only

# Step 2: Compare with batch proc
python compare_disp_s1_frame_states.py audit_results.json batch_proc.json

# Step 3: Review the comparison output for discrepancies
```

### Workflow 3: Create New Batch Proc from CMR State

When setting up a new environment or creating a fresh batch proc:

```bash
# Step 1: Extract frame states from CMR (fast, frame-states-only)
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860,18904,18905,16669,44328 \
    --output-frame-states cmr_states.json \
    --frame-states-only

# Step 2: Create batch proc JSON using the frame_states
# Use the output as a starting point for your batch_proc.json
```

Example batch proc template with frame states:

```json
{
    "enabled": true,
    "label": "DISP-S1 Historical Processing",
    "processing_mode": "historical",
    "data_start_date": "2016-07-01T00:00:00",
    "data_end_date": "2025-01-01T00:00:00",
    "k": 15,
    "m": 6,
    "frames": [10860, 18904, 18905, 16669, 44328],
    "wait_between_acq_cycles_mins": 10,
    "job_type": "cslc_query_hist",
    "job_queue": "opera-job_worker-cslc_data_query_hist",
    "frame_states": {
        "10860": 360,
        "18904": 180,
        "18905": 0,
        "16669": 225,
        "44328": 45
    }
}
```

---

## Troubleshooting

### Issue: "No DISP-S1 products found in GRQ ES"

**Cause:** The GRQ Elasticsearch database doesn't contain the DISP-S1 products.

**Solution:** Use `--frame-states-only` for fast frame state extraction, or use `--burst-data-source cmr` for full audits without GRQ:

```bash
# For frame states only (fast, no burst validation)
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860 \
    --output-frame-states output.json \
    --frame-states-only

# For full burst-level audit without GRQ
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860 \
    --burst-data-source cmr
```

### Issue: "ModuleNotFoundError"

**Cause:** PYTHONPATH is not set correctly.

**Solution:** Set PYTHONPATH to include the opera-sds-pcm directory:

```bash
export PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH
```

Or prepend it to the command:

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py ...
```

### Issue: Frame state is 0 but products exist

**Cause:** The products may be outside the queried date range, or the frame ID may not match.

**Solution:**
1. Expand the date range to cover the full historical period (e.g., from `2016-07-01` to present)
2. Verify the frame ID is correct
3. Check the log output for warnings about frames not found in the disp_burst_map

### Issue: CMR query returns no products

**Cause:** Date range may not overlap with product temporal extent.

**Solution:**
1. Use a broader date range (e.g., from 2016-07-01 to present)
2. Verify the frame IDs are correct
3. Check CMR directly to confirm products exist

---

## Technical Details

### How Frame State is Calculated

1. **Query CMR** for DISP-S1 products within the date range
2. **Extract metadata** from each product:
   - Frame ID (from `AdditionalAttributes.FRAME_NUMBER`)
   - End date (from `TemporalExtent.RangeDateTime.EndingDateTime`)
3. **Derive day index** from end date using the DISP burst map
4. **Find position** of the day index in the frame's `sensing_datetime_days_index` list
5. **Frame state** = position + 1 (count of processed sensing times)

### Frame State Formula

```
frame_state = index_of(max(day_index for frame)) + 1
```

Where:
- `day_index` = days since first sensing time for the frame
- `index_of()` = position in the `sensing_datetime_days_index` list

### Relationship to K-Cycles

```
k_cycles_completed = frame_state // k
next_k_cycle = frame_state // k
```

Example with k=15 and frame_state=360:
- 360 // 15 = 24 complete k-cycles
- Next job will process k-cycle 24 (sensing time indices 360-374)

---

## Quick Reference

### Extract Frame States from CMR (fast, no burst validation)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only <FRAME_IDS> \
    --output-frame-states output.json \
    --frame-states-only
```

### Full Burst-Level Audit (default, using GRQ)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only <FRAME_IDS>
```

### Full Burst-Level Audit (using CMR ISO XML, no GRQ needed)

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only <FRAME_IDS> \
    --burst-data-source cmr
```

### Compare Frame States

```bash
python compare_disp_s1_frame_states.py <cmr_output.json> <batch_proc.json>
```

### Diagnose Frame Products

```bash
PYTHONPATH=/path/to/opera-sds-pcm:$PYTHONPATH python diagnose_disp_s1_frame_products.py --frame <FRAME_ID>
```
