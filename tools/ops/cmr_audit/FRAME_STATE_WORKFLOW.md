# DISP-S1 Frame State Determination & Burst Validation Workflow

This document provides a detailed technical explanation of how frame states are calculated and how burst validation works in the CMR audit tools.

---

## Table of Contents

1. [Overview](#overview)
2. [Key Data Structures](#key-data-structures)
3. [Pipeline Diagram](#pipeline-diagram)
4. [Step-by-Step Workflow](#step-by-step-workflow)
5. [Frame State Calculation Algorithm](#frame-state-calculation-algorithm)
6. [Burst Validation Algorithm](#burst-validation-algorithm)
7. [Example Walkthrough](#example-walkthrough)

---

## Overview

The DISP-S1 CMR audit system determines **frame states** by:
1. Querying CMR for DISP-S1 products
2. Extracting metadata (frame ID, end date) from each product
3. Mapping the end date to a position in the frame's sensing time list
4. Computing the frame state as `position + 1`

**Burst validation** additionally verifies that each DISP-S1 product used the correct input CSLC granules.

---

## Key Data Structures

### 1. DISP Burst Map (`frame_to_bursts`)

The **DISP Burst Map** is loaded from `opera-disp-s1-consistent-burst-ids-with-datetimes.json` and contains the ground truth for each frame.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        _HistBursts Object (per frame)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  frame_number: int                  # e.g., 10860                           │
│  burst_ids: set[str]                # e.g., {"T064-135524-IW1", ...}        │
│  sensing_datetimes: list[datetime]  # Sorted list of all sensing times      │
│  sensing_datetime_days_index: list[int]  # Day indices (0, 12, 24, ...)     │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Example for Frame 10860:**

```
sensing_datetimes (first 5):           sensing_datetime_days_index (first 5):
├─ [0]  2016-07-04 12:34:56           ├─ [0]  0
├─ [1]  2016-07-16 12:34:57           ├─ [1]  12
├─ [2]  2016-07-28 12:34:58           ├─ [2]  24
├─ [3]  2016-08-09 12:34:59           ├─ [3]  36
├─ [4]  2016-08-21 12:35:00           ├─ [4]  48
...                                    ...
├─ [359] 2024-12-25 12:34:XX          ├─ [359] 3084
```

### 2. CMR UMM Object (DISP-S1 Product)

```json
{
  "umm": {
    "GranuleUR": "OPERA_L3_DISP-S1_IW_F10860_VV_20230927T123432Z_20240206T123428Z_v1.0_20250624T033524Z",
    "TemporalExtent": {
      "RangeDateTime": {
        "BeginningDateTime": "2023-09-27T12:34:32Z",
        "EndingDateTime": "2024-02-06T12:34:28Z"      <-- Key field for frame state
      }
    },
    "AdditionalAttributes": [
      {
        "Name": "FRAME_NUMBER",
        "Values": ["10860"]                           <-- Frame ID
      }
    ],
    "RelatedUrls": [
      {
        "URL": "https://...OPERA_L3_DISP-S1...iso.xml",  <-- ISO XML for burst validation
        "Type": "EXTENDED METADATA"
      }
    ]
  }
}
```

### 3. GRQ Elasticsearch Document (DISP-S1 Product)

```json
{
  "_source": {
    "id": "OPERA_L3_DISP-S1_IW_F10860_VV_20230927T123432Z_20240206T123428Z_v1.0",
    "metadata": {
      "frame_id": 10860,
      "acquisition_cycle": 2784,                      <-- Day index (pre-computed)
      "input_granule_id": "f10860_a2784_f10860_a2772_f10860_a2760_...",
      "lineage": [
        "s3://bucket/OPERA_L2_CSLC-S1_T064-135524-IW1_20240206T123428Z_....h5",
        "s3://bucket/OPERA_L2_CSLC-S1_T064-135525-IW1_20240206T123428Z_....h5",
        ...
      ]
    }
  }
}
```

### 4. Result DataFrame Structure

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Result DataFrame                                        │
├──────────────┬──────────┬───────────────────┬─────────────────────┬────────────────────────┤
│ Product ID   │ Frame ID │ Last Acq Day Index│ All Acq Day Indices │ All Bursts             │
├──────────────┼──────────┼───────────────────┼─────────────────────┼────────────────────────┤
│ OPERA_L3_... │ 10860    │ 2784              │ [2760,2772,2784]    │ [CSLC_T064-135524...]  │
│ OPERA_L3_... │ 10860    │ 2796              │ [2772,2784,2796]    │ [CSLC_T064-135524...]  │
│ UNPROCESSED  │ 10860    │ 2808              │ N/A                 │ [expected CSLCs...]    │
└──────────────┴──────────┴───────────────────┴─────────────────────┴────────────────────────┘
```

---

## Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                           FRAME STATE DETERMINATION PIPELINE                                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘

                                    User Input
                                        │
                                        ▼
                    ┌─────────────────────────────────────┐
                    │  --start-datetime  --end-datetime   │
                    │  --frames-only  --processing-mode   │
                    │  --frame-states-only (or)           │
                    │  --burst-data-source [grq|cmr]      │
                    └─────────────────────────────────────┘
                                        │
                                        ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: Load DISP Burst Map                                                              │
│  ─────────────────────────────                                                            │
│  localize_disp_frame_burst_hist() → frame_to_bursts, burst_to_frames                      │
│                                                                                           │
│  Contains: burst_ids, sensing_datetimes, sensing_datetime_days_index for each frame       │
└───────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                         ┌──────────────┴──────────────┐
                         │                             │
                         ▼                             ▼
              ┌─────────────────────┐      ┌─────────────────────────┐
              │  --frame-states-only│      │  Full Burst Validation  │
              │  (Fast Mode)        │      │  (Default or --burst-   │
              └─────────────────────┘      │   data-source cmr)      │
                         │                 └─────────────────────────┘
                         │                             │
                         ▼                             ▼
┌───────────────────────────────────┐  ┌─────────────────────────────────────────────────────┐
│  STEP 2a: Query CMR for DISP-S1   │  │  STEP 2b: Query CMR for CSLCs (for all frame bursts)│
│  ─────────────────────────────────│  │  ────────────────────────────────────────────────── │
│  retrieve_disp_s1_from_cmr()      │  │  For each burst_id in frame:                        │
│  with return_full_umm=True        │  │    Query: OPERA_L2_CSLC-S1_{burst_id}*              │
│                                   │  │  Build: frame_to_dayindex_to_granule map            │
└───────────────────────────────────┘  └─────────────────────────────────────────────────────┘
                         │                             │
                         ▼                             ▼
┌───────────────────────────────────┐  ┌─────────────────────────────────────────────────────┐
│  STEP 3a: Extract Metadata        │  │  STEP 3b: Query for DISP-S1 Products                │
│  from CMR UMM Objects             │  │  ─────────────────────────────────────              │
│  ──────────────────────────────── │  │  retrieve_disp_s1_from_cmr() or                     │
│  extract_disp_s1_metadata_from_   │  │  retrieve_disp_s1_from_grq()                        │
│  cmr()                            │  │                                                     │
│                                   │  │  Then get burst lineage from:                       │
│  For each product:                │  │    - GRQ ES (burst_data_source='grq')               │
│    - Extract frame_id             │  │    - ISO XML (burst_data_source='cmr')              │
│    - Extract EndingDateTime       │  │                                                     │
│    - Compute day_index            │  │  extract_disp_s1_metadata_from_iso_xml() or         │
│                                   │  │  query GRQ ES for lineage                           │
└───────────────────────────────────┘  └─────────────────────────────────────────────────────┘
                         │                             │
                         │                             ▼
                         │             ┌─────────────────────────────────────────────────────┐
                         │             │  STEP 4b: Burst Validation (match_up_disp_s1)       │
                         │             │  ─────────────────────────────────────────────────  │
                         │             │  Compare DISP-S1 input CSLCs vs expected CSLCs      │
                         │             │  Mark products as MATCHED or find UNPROCESSED       │
                         │             └─────────────────────────────────────────────────────┘
                         │                             │
                         ▼                             ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 5: Calculate Frame States                                                             │
│  ──────────────────────────────                                                             │
│  calculate_expected_frame_states(result_df, k)                                              │
│                                                                                             │
│  For each frame_id in result_df:                                                            │
│    1. Find max(Last Acq Day Index) among processed products                                 │
│    2. Look up index_position in sensing_datetime_days_index                                 │
│    3. frame_state = index_position + 1                                                      │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                    ┌─────────────────────────────────────┐
                    │  Output: frame_states.json          │
                    │  {                                  │
                    │    "frame_states": {                │
                    │      "10860": 360,                  │
                    │      "18902": 315                   │
                    │    },                               │
                    │    "k": 15,                         │
                    │    ...                              │
                    │  }                                  │
                    └─────────────────────────────────────┘
```

---

## Step-by-Step Workflow

### Step 1: Load DISP Burst Map

```python
frame_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()
```

This loads the ground truth database containing:
- All burst IDs for each frame
- All sensing times (sorted chronologically)
- Day indices for each sensing time

### Step 2: Query CMR for Products

**Frame-States-Only Mode:**
```python
cmr_umm_objects = retrieve_disp_s1_from_cmr(
    start_date, end_date,
    output_endpoint,
    frames_to_validate,
    return_full_umm=True
)
```

**Burst Validation Mode:**
First query for all CSLCs, then query for DISP-S1 products.

### Step 3: Extract Metadata

**From CMR (Frame-States-Only):**
```python
for umm_obj in cmr_umm_objects:
    frame_id = extract_frame_number(umm_obj)
    end_date = extract_ending_datetime(umm_obj)
    day_index = sensing_time_day_index(end_date, frame_id, frame_to_bursts)
```

**From GRQ ES:**
```python
metadata = disp_s1["_source"]["metadata"]
day_index = metadata["acquisition_cycle"]  # Pre-computed
lineage = metadata["lineage"]              # Input CSLC files
```

**From ISO XML:**
```python
iso_xml_url = get_iso_xml_url_from_umm(umm_obj)
cslc_inputs = fetch_cslc_input_granules_from_iso_xml(iso_xml_url)
```

### Step 4: Burst Validation (if enabled)

Compare expected CSLCs vs actual CSLCs used by DISP-S1 products.

### Step 5: Calculate Frame States

```python
for frame_id in processed_df['Frame ID'].unique():
    max_acq_day_index = frame_data['Last Acq Day Index'].max()
    index_position = frame.sensing_datetime_days_index.index(max_acq_day_index)
    frame_state = index_position + 1
```

---

## Frame State Calculation Algorithm

### Formula

```
frame_state = index_of(max_day_index) + 1
```

Where:
- `max_day_index` = highest "Last Acq Day Index" among processed DISP-S1 products for a frame
- `index_of()` = position in the frame's `sensing_datetime_days_index` list

### Visual Example

```
Frame 10860 sensing_datetime_days_index:
┌─────────────────────────────────────────────────────────────────────────────┐
│ Index:     0    1    2    3    4    5   ...  359                            │
│ Day Index: 0   12   24   36   48   60   ... 3084                            │
│            │    │    │    │    │    │        │                              │
│            ▼    ▼    ▼    ▼    ▼    ▼        ▼                              │
│ Date:   Jul04 Jul16 Jul28 Aug09 Aug21 Sep02  Dec25                          │
│         2016  2016  2016  2016  2016  2016   2024                           │
└─────────────────────────────────────────────────────────────────────────────┘

If the latest DISP-S1 product has EndingDateTime = 2024-12-25T12:34:XX:
  → day_index = 3084
  → index_position = 359 (found by looking up 3084 in the list)
  → frame_state = 359 + 1 = 360
```

### Day Index Calculation

```python
def sensing_time_day_index(sensing_time, frame_number, frame_to_bursts):
    frame = frame_to_bursts[frame_number]
    first_sensing_time = frame.sensing_datetimes[0]

    delta = sensing_time - first_sensing_time
    seconds = delta.total_seconds()
    day_index_high_precision = seconds / (24 * 3600)

    # Round to nearest day (Sentinel-1 revisit is ~12 days)
    day_index = round(day_index_high_precision)

    return day_index
```

### K-Cycle Relationship

For historical processing with k=15:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        K-Cycle Structure (k=15)                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  K-Cycle 0:  indices 0-14   (sensing times 1-15)                           │
│  K-Cycle 1:  indices 15-29  (sensing times 16-30)                          │
│  K-Cycle 2:  indices 30-44  (sensing times 31-45)                          │
│  ...                                                                       │
│  K-Cycle 23: indices 345-359 (sensing times 346-360)                       │
│                                                                            │
│  frame_state = 360 means:                                                  │
│    - 360 sensing times have been processed                                 │
│    - 24 complete k-cycles (360 / 15 = 24)                                  │
│    - Next k-cycle to process: 24 (indices 360-374)                         │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Burst Validation Algorithm

### Purpose

Verify that each DISP-S1 product used the correct input CSLC granules.

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BURST VALIDATION FLOW                               │
└─────────────────────────────────────────────────────────────────────────────┘

     Expected CSLCs (from CMR query)              Actual CSLCs (from DISP-S1)
     ────────────────────────────────             ──────────────────────────────
                    │                                          │
                    ▼                                          ▼
     ┌──────────────────────────────┐         ┌──────────────────────────────┐
     │ frame_to_dayindex_to_granule │         │ DISP-S1 product metadata     │
     │ {                            │         │ {                            │
     │   10860: {                   │         │   "All Bursts": [            │
     │     2760: {CSLC_A, CSLC_B},  │         │     "CSLC_A", "CSLC_B", ...  │
     │     2772: {CSLC_C, CSLC_D},  │         │   ],                         │
     │     2784: {CSLC_E, CSLC_F}   │         │   "All Acq Day Indices":     │
     │   }                          │         │     [2760, 2772, 2784]       │
     │ }                            │         │ }                            │
     └──────────────────────────────┘         └──────────────────────────────┘
                    │                                          │
                    └───────────────────┬──────────────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────┐
                         │   match_up_disp_s1()     │
                         │                          │
                         │   For each DISP-S1:      │
                         │   - Compare bursts       │
                         │   - Count matches        │
                         │   - Find unmatched       │
                         └──────────────────────────┘
                                        │
                                        ▼
     ┌─────────────────────────────────────────────────────────────────────┐
     │                         VALIDATION RESULT                           │
     │                                                                     │
     │  Product: OPERA_L3_DISP-S1_IW_F10860_VV_...                         │
     │  All Bursts Count: 405                                              │
     │  Matching Bursts Count: 405  ✓                                      │
     │  Unmatching Bursts Count: 0                                         │
     │                                                                     │
     │  OR                                                                 │
     │                                                                     │
     │  Product: UNPROCESSED                                               │
     │  Frame ID: 10860                                                    │
     │  Last Acq Day Index: 2808                                           │
     │  All Bursts: [expected CSLCs that weren't processed]                │
     └─────────────────────────────────────────────────────────────────────┘
```

### ISO XML Structure (for CMR burst data source)

```xml
<gmi:MI_Metadata>
  <gmd:fileIdentifier>
    <gco:CharacterString>OPERA_L3_DISP-S1_IW_F10860_VV_...</gco:CharacterString>
  </gmd:fileIdentifier>
  ...
  <gmd:FileName>OPERA_L2_CSLC-S1_T064-135524-IW1_20240206T123428Z_...h5</gmd:FileName>
  <gmd:FileName>OPERA_L2_CSLC-S1_T064-135525-IW1_20240206T123428Z_...h5</gmd:FileName>
  <gmd:FileName>OPERA_L2_CSLC-S1_T064-135526-IW1_20240206T123428Z_...h5</gmd:FileName>
  ...
</gmi:MI_Metadata>
```

The `fetch_cslc_input_granules_from_iso_xml()` function:
1. Fetches the ISO XML from the URL
2. Parses all `<FileName>` elements
3. Filters for CSLC files (excluding STATIC and COMPRESSED)
4. Returns normalized granule IDs

---

## Example Walkthrough

### Scenario: Determine frame state for Frame 10860

**Input:**
```bash
python cmr_audit_disp_s1.py \
    --start-datetime 2016-07-01T00:00:00Z \
    --end-datetime 2025-12-01T00:00:00Z \
    --processing-mode historical \
    --frames-only 10860 \
    --output-frame-states output.json \
    --frame-states-only
```

**Step 1: Load DISP Burst Map**

```
Frame 10860:
  burst_ids: {T064-135524-IW1, T064-135524-IW2, T064-135524-IW3, ...} (27 bursts)
  sensing_datetimes: [2016-07-04T12:34:56Z, 2016-07-16T12:34:57Z, ...]  (360 entries)
  sensing_datetime_days_index: [0, 12, 24, 36, ..., 3084]               (360 entries)
```

**Step 2: Query CMR**

```
GET https://cmr.earthdata.nasa.gov/search/granules.umm_json
  ?provider=ASF
  &short_name=OPERA_L3_DISP-S1_V1
  &temporal=2016-07-01T00:00:00Z,2025-12-01T00:00:00Z
  &attribute[]=int,FRAME_NUMBER,10860
```

Returns 24 DISP-S1 products for frame 10860.

**Step 3: Extract Metadata**

For each product, extract:

| Product ID | Frame ID | EndingDateTime | Day Index | Index Position |
|------------|----------|----------------|-----------|----------------|
| OPERA_L3_DISP-S1_...20240113 | 10860 | 2024-01-13T12:34:28Z | 2748 | 329 |
| OPERA_L3_DISP-S1_...20240125 | 10860 | 2024-01-25T12:34:29Z | 2760 | 330 |
| OPERA_L3_DISP-S1_...20240206 | 10860 | 2024-02-06T12:34:28Z | 2772 | 331 |
| ... | ... | ... | ... | ... |
| OPERA_L3_DISP-S1_...20241225 | 10860 | 2024-12-25T12:34:XX | 3084 | 359 |

**Step 4: Calculate Day Index**

For the latest product with EndingDateTime = 2024-12-25T12:34:XX:

```python
first_sensing_time = datetime(2016, 7, 4, 12, 34, 56)  # From burst map
end_date = datetime(2024, 12, 25, 12, 34, XX)

delta = end_date - first_sensing_time
# delta ≈ 3096 days

day_index = round(delta.days)  # Adjusted for exact calculation
# day_index = 3084
```

**Step 5: Find Index Position**

```python
sensing_datetime_days_index = [0, 12, 24, 36, ..., 3072, 3084]
index_position = sensing_datetime_days_index.index(3084)
# index_position = 359
```

**Step 6: Calculate Frame State**

```python
frame_state = index_position + 1
# frame_state = 360
```

**Output:**

```json
{
    "frame_states": {
        "10860": 360
    },
    "k": 15,
    "audit_start_date": "2016-07-01T00:00:00Z",
    "audit_end_date": "2025-12-01T00:00:00Z",
    "processing_mode": "historical",
    "total_frames": 1,
    "frames_with_products": 1,
    "frames_without_products": 0
}
```

**Interpretation:**
- Frame 10860 has processed 360 sensing times
- 360 / 15 = 24 complete k-cycles
- Next k-cycle to process: 24 (sensing time indices 360-374)

---

## Summary Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FRAME STATE CALCULATION SUMMARY                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   DISP-S1 Product                      DISP Burst Map                       │
│   ────────────────                     ──────────────                       │
│   EndingDateTime ──────────────────────► Day Index ─────────► Index Position│
│   2024-12-25                              3084                   359        │
│                                                                    │        │
│                                                                    ▼        │
│                                                             Frame State     │
│                                                             = 359 + 1       │
│                                                             = 360           │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  Frame 10860 Timeline                                               │   │
│   │                                                                     │   │
│   │  Index:  0   1   2  ... 329 330 331 ... 358 359 [360 361 ...]       │   │
│   │          │   │   │      │   │   │       │   │    │                  │   │
│   │          ▼   ▼   ▼      ▼   ▼   ▼       ▼   ▼    │                  │   │
│   │  Date: Jul Aug Sep ... Jan Jan Feb ... Dec Dec  [Next to process]   │   │
│   │        04  09  02      13  25  06      13  25                       │   │
│   │       2016 ...        2024 ─────────────────────                    │   │
│   │                            Processed (frame_state=360)              │   │
│   │                                                                     │   │
│   │  K-Cycles: [0-14] [15-29] ... [330-344] [345-359] [360-374]         │   │
│   │            K=0    K=1    ...   K=22      K=23      K=24             │   │
│   │            ════════════════════════════════════                     │   │
│   │                    24 Complete K-Cycles                             │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `cmr_audit_disp_s1.py` | Main audit script, `calculate_expected_frame_states()` |
| `opv_disp_s1.py` | Validation logic, `validate_disp_s1()`, `match_up_disp_s1()` |
| `cslc_utils.py` | `sensing_time_day_index()`, `localize_disp_frame_burst_hist()` |
| `cmr_iso_xml_utils.py` | `fetch_cslc_input_granules_from_iso_xml()` |
| `opera-disp-s1-consistent-burst-ids-with-datetimes.json` | Ground truth database |
