# DISP-S1 Completeness Audit Guide

This guide covers the DISP-S1 completeness audit tools that verify product generation against expected outputs based on CSLC availability and K-cycle logic.

## Overview

The completeness audit determines:
1. Which DISP-S1 products **should** exist based on available CSLCs
2. Which products **actually** exist in CMR
3. Why products may be missing (actionable vs. blocked by upstream gaps)
4. Whether existing products need reprocessing (stale k-cycle references)

## Consistent Burst Database

The audit relies on the **consistent burst database** (`opera-disp-s1-consistent-burst-ids-with-datetimes.json`), which is the authoritative source for:

- Frame definitions (which bursts belong to which frame)
- Valid sensing times (dates when all bursts in a frame have complete CSLC data)
- K-cycle boundaries (derived from the sensing time index positions)

### What the Database Contains

For each frame, the database provides:

| Field | Description |
|-------|-------------|
| `burst_ids` | List of burst IDs that belong to this frame |
| `sensing_datetimes` | List of sensing times when all bursts in the frame have CSLC data |
| `sensing_datetime_days_index` | Day index for each sensing time (days since first sensing) |

### How the Audit Uses It

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONSISTENT BURST DATABASE                            │
│  Frame 44327:                                                               │
│    burst_ids: [T064-136231-IW1, T064-136232-IW1, ... ] (27 bursts)          │
│    sensing_datetimes: [2016-07-14, 2016-07-26, 2016-08-07, ...]             │
│    sensing_datetime_days_index: [0, 12, 24, ...]                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┴───────────────────────┐
            ▼                                               ▼
┌───────────────────────────┐               ┌───────────────────────────────┐
│      QUERY CMR FOR        │               │     FRAME CONFIGURATION       │
│    CSLCs BY BURST ID      │               │                               │
│                           │               │  num_bursts = 27              │
│  For each burst_id:       │               │  sensing_times from database  │
│  Query OPERA_L2_CSLC-S1_* │               │  K-cycle boundaries           │
│  matching that burst      │               │                               │
└───────────────────────────┘               └───────────────────────────────┘
            │                                               │
            └───────────────────────┬───────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CALCULATE EXPECTED PRODUCTS                              │
│                                                                             │
│  1. GROUP CSLCs BY SENSING TIME                                             │
│     Parse each CSLC ID to get burst_id and acquisition date                 │
│     Map to day_index using the database's sensing_datetime_days_index       │
│     CSLCs with dates not in database are tracked as "skipped"               │
│                                                                             │
│  2. CHECK COMPLETENESS PER SENSING TIME                                     │
│     For each sensing time: count CSLCs found vs num_bursts expected         │
│     Complete = all 27 bursts have CSLCs for that date                       │
│                                                                             │
│  3. GROUP BY K-CYCLE                                                        │
│     K-cycle 0: indices 0-14                                                 │
│     K-cycle 1: indices 15-29                                                │
│     K-cycle N: indices N*k to (N+1)*k - 1                                   │
│                                                                             │
│  4. CHECK K-CYCLE TRIGGERABILITY                                            │
│     K-cycle is triggerable only if ALL k sensing times are complete         │
│     (all 27 bursts present for each of the k sensing times)                 │
│                                                                             │
│  5. EXPECTED PRODUCTS                                                       │
│     If K-cycle is triggerable → expect products for ALL k sensing times     │
│       EXCEPT index 0 (first sensing time is displacement reference)         │
│     If K-cycle not triggerable → no products expected (blocked)             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COMPARE WITH ACTUAL PRODUCTS                             │
│                                                                             │
│  Query CMR for DISP-S1 products for this frame                              │
│  For each product, fetch ISO XML to validate CSLC inputs                    │
│  Compare expected vs actual to identify missing/unexpected products         │
│  Detect stale K-cycle references (BeginningDateTime mismatch)               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Concepts

**Index Position vs Day Index:**
- `day_index`: Days since the frame's first sensing time (e.g., 0, 12, 24, 1962...)
- `index_position`: Sequential position in the sensing_datetimes list (e.g., 0, 1, 2, 150...)
- K-cycles are calculated from `index_position`, not `day_index`

**Why This Matters:**
- If the database is updated with a new sensing time inserted in the past, all subsequent index positions shift
- This changes k-cycle boundaries for all affected products
- Products generated before the update have "stale" k-cycle references

**First Sensing Time (Index 0):**
- Index 0 is the very first sensing time for a frame and serves as the reference point for displacement measurements
- No DISP-S1 product can be generated for index 0 because there's no previous acquisition to measure displacement against
- The audit excludes index 0 from expected products

### Database Loading

The database is loaded at startup via:

```python
from data_subscriber.cslc_utils import localize_disp_frame_burst_hist

frame_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()
```

This downloads the latest database from S3 if not already cached locally.

## Tools

| Tool | Description |
|------|-------------|
| `audit_disp_s1_completeness.py` | Main audit tool for individual frames |
| `audit_batch_procs_completeness.sh` | Wrapper script for batch processing multiple frames |

## Quick Start

### Single Frame Audit

```bash
cd /path/to/opera-sds-pcm
PYTHONPATH=".:$PYTHONPATH" python tools/ops/cmr_audit/audit_disp_s1_completeness.py \
    --frames 44327 \
    --start 2016-07-01T00:00:00Z \
    --end 2025-12-31T00:00:00Z
```

### Batch Audit with Caching

```bash
./tools/ops/cmr_audit/audit_batch_procs_completeness.sh \
    ~/DISP-S1/batch_procs \
    ./audit_results \
    --workers 30 \
    --iso-cache-dir ./iso_cache
```

## CLI Reference

### audit_disp_s1_completeness.py

```
usage: audit_disp_s1_completeness.py [options]

Required:
  --frames FRAMES       Comma-separated list of frame IDs to audit

Optional:
  --start DATE          Start datetime (default: 2016-07-01T00:00:00Z)
  --end DATE            End datetime (default: 2025-12-31T00:00:00Z)
  --endpoint {OPS,UAT}  CMR endpoint (default: OPS)
  --k K                 K parameter for K-cycle logic (default: 15)
  --max-workers N       Parallel workers for ISO XML fetching (default: 20)
  --output FILE         Output JSON file path
  --iso-cache-dir DIR   Directory for caching ISO XML files
  --debug               Enable debug logging
  --verbose             Show progress bars for CMR queries and ISO XML fetching
  --low-memory          Low memory mode: stream results to JSONL, omit large CSLC lists
```

### audit_batch_procs_completeness.sh

```
usage: audit_batch_procs_completeness.sh <batch_proc_dir> [output_dir] [options]

Options:
  --workers N           Parallel workers (default: 20)
  --end-date DATE       End date for queries (default: 2025-12-31T00:00:00Z)
  --iso-cache-dir DIR   Directory for caching ISO XML files
  --low-memory          Enable low memory mode (recommended for batch procs with 100+ frames)
```

## Report Categories

The audit classifies products into the following categories:

### Complete
Products found in CMR with all K sensing times having all bursts (100% completeness).

### Incomplete
Products found in CMR but some sensing times have missing bursts. The report shows which sensing times are incomplete and how many bursts are missing.

### Stale
Products found in CMR but generated with outdated data. These need reprocessing because the burst database has been updated. The `stale_reason` field indicates the specific issue:

| Reason | Description |
|--------|-------------|
| `reference_shifted` | K-cycle boundary has changed (BeginningDateTime mismatch) |
| `content_shifted` | K-cycle boundary correct but content affected by database update |

**reference_shifted** is detected when:
- The product's BeginningDateTime (K-cycle reference) doesn't match the expected reference
- Example: Product at index 345 should reference K-cycle 23's start (index 344), but references K-cycle 22's start (index 329)

**content_shifted** is detected in two cases:
1. **CSLCs from wrong indices**: Product contains CSLCs from indices outside the expected K-cycle range (database shifted sensing times since processing)
2. **Missing added sensing times**: Product is missing sensing times that now have complete CSLC coverage (sensing times added to database after processing)

**Priority**: stale > incomplete > complete. A product that is both stale and incomplete is classified as stale because it must be deleted regardless of completeness.

**Report output format**:
```
content_shifted (14):
  (Indices without suffix = CSLCs from wrong K-cycle; (+) = sensing time added after processing)
     Idx |  K-Cyc |    Pct |          Affected Indices | Product ID
  ---------------------------------------------------------------------------------------------------------
     330 |     22 |    93% |                       345 | OPERA_L3_DISP-S1_IW_F46290_VV_20250721T1...

reference_shifted (1):
     Idx |  K-Cyc |    Pct | Actual Begin |   Expected | Product ID
  --------------------------------------------------------------------------------------------------------------
     345 |     23 |     7% |          329 |        344 | OPERA_L3_DISP-S1_IW_F46290_VV_20250721T1...
```

Both types indicate the burst database was updated after the product was generated. The product should be deleted and reprocessed to use the correct inputs.

### Missing (Actionable)
Products that **should** exist but don't:
- The K-cycle was triggerable (all K sensing times have complete CSLC coverage)
- The DISP-S1 job should have triggered and generated this product
- **These are actionable** - can be reprocessed

### Not Triggerable
Products that can't be generated due to CSLC gaps in the K-cycle:
- One or more sensing times in the K-cycle are missing complete CSLCs
- The DISP-S1 job cannot trigger until all K sensing times have data
- **Not actionable** without upstream CSLC generation

### Unexpected
Products in CMR that weren't expected:
- Products for sensing times not in the burst database (forward processing)
- Products whose day_index doesn't exist in the consistent database

### Anomalies
Products that have anomalous CSLC inputs:
- CSLCs that don't belong to the queried frame
- CSLCs for unexpected sensing times (outside the product's K-cycle)
- CSLCs with sensing times not in the burst database
- CSLCs that couldn't be parsed

**Note**: For products classified as stale with `content_shifted` reason, the "unexpected sensing time" anomalies are suppressed since they are explained by the content shift.

### Skipped CSLCs
CSLCs found in CMR whose sensing times are not in the burst database:
- These represent forward processing (new data not yet in the database)
- The CSLCs are not used in the expected products calculation
- If a DISP-S1 product exists for these dates, it will appear as "unexpected"

### Found Despite Untriggerable
Products that exist in CMR but whose K-cycle appears "not triggerable" based on current CSLC data:
- The product exists, but the current CSLC query shows gaps in the K-cycle
- This can happen when:
  - The burst database was updated after product generation (new sensing times added that created gaps)
  - CSLCs were available when the product was generated but are no longer in CMR
  - Product was generated through manual reprocessing
- These products are still categorized as complete/incomplete/stale_reference, but flagged

## K-Cycle Based Processing

### How DISP-S1 Processing Works

DISP-S1 jobs are triggered at **K-cycle boundaries**, not for individual sensing times:

```
K-cycle 0: indices 0-14   → Job triggers when index 14 has data
K-cycle 1: indices 15-29  → Job triggers when index 29 has data
K-cycle 2: indices 30-44  → Job triggers when index 44 has data
...
```

When a K-cycle boundary is reached:
1. Check if ALL K sensing times in the cycle have complete CSLC coverage
2. If yes → Job triggers and generates products for ALL K sensing times in that cycle
3. If no → Job cannot trigger (cycle is "not triggerable")

### Key Points

- A single job generates **K products** (one for each sensing time in the cycle)
- ALL sensing times in the cycle must have complete data for the job to trigger
- If even one sensing time is missing CSLCs, the entire cycle is blocked

### Example

For K=15 and K-cycle 10 (indices 150-164):
```
Index:  150  151  152  153  154  155  156  157  158  159  160  161  162  163  164
CSLCs:   27   27   27    0   27   27   27   27   27   27   27   27   27   27   27
                        ↑
                        Missing CSLCs at index 153
```

- K-cycle 10 spans indices 150-164 (15 sensing times)
- Index 153 has 0 CSLCs (missing data)
- **Result**: Entire K-cycle 10 is "not triggerable"
- All 15 products (indices 150-164) are marked as `not_triggerable`

**But what if products already exist for K-cycle 10?**

If DISP-S1 products are found in CMR for indices 150-164:
- They are categorized as `complete`, `incomplete`, or `stale` based on their actual ISO XML inputs
- They are flagged with `found_despite_untriggerable: true`
- This indicates the K-cycle was triggerable when the products were generated, but current CSLC data shows gaps

## ISO XML Metadata

### What ISO XML Contains

Each DISP-S1 product in CMR has an associated ISO XML metadata file that contains:
- Product metadata (production time, version, etc.)
- **Input granule list** - the CSLC files used to generate the product

Example ISO XML structure (simplified):
```xml
<gmi:MI_Metadata>
  <gmd:identificationInfo>
    <gmd:MD_DataIdentification>
      <!-- Input granules listed as FileName elements -->
      <FileName>OPERA_L2_CSLC-S1_T064-136231-IW1_20211115T014141Z_...</FileName>
      <FileName>OPERA_L2_CSLC-S1_T064-136232-IW1_20211115T014141Z_...</FileName>
      <!-- ... more CSLC inputs -->
    </gmd:MD_DataIdentification>
  </gmd:identificationInfo>
</gmi:MI_Metadata>
```

### How the Audit Uses ISO XML

The audit analyzes CSLC inputs from the ISO XML to provide comprehensive validation:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ISO XML VALIDATION FLOW                             │
│                                                                             │
│  1. FETCH ISO XML                                                           │
│     For each DISP-S1 product found in CMR, fetch its ISO XML metadata       │
│                                                                             │
│  2. EXTRACT CSLC INPUTS                                                     │
│     Parse all FileName elements containing "CSLC"                           │
│     Excludes: STATIC-CSLC and COMPRESSED-CSLC (these are expected)          │
│     Analyzes: All regular CSLC inputs                                       │
│                                                                             │
│  3. CATEGORIZE CSLC INPUTS                                                  │
│     For each CSLC, determine:                                               │
│     - Does it belong to this frame? (check burst_id)                        │
│     - What sensing time/day_index does it correspond to?                    │
│     - Is that sensing time expected for this product's K-cycle?             │
│                                                                             │
│  4. CHECK COMPLETENESS ACROSS ALL K SENSING TIMES                           │
│     A DISP-S1 product should have:                                          │
│     - K sensing times (e.g., 15)                                            │
│     - Each sensing time should have all bursts (e.g., 27)                   │
│     - Total: K × num_bursts CSLCs (e.g., 15 × 27 = 405)                     │
│                                                                             │
│  5. IDENTIFY ANOMALOUS INPUTS                                               │
│     Flag CSLCs that don't fit expectations:                                 │
│     - burst not in frame: CSLC burst_id not in this frame                   │
│     - unexpected sensing time: CSLC for wrong K-cycle                       │
│     - sensing time not in database: CSLC date not recognized                │
│     - parse error: Could not parse CSLC ID                                  │
│                                                                             │
│  6. CALCULATE COMPLETENESS                                                  │
│     complete_sensing_times / K × 100%                                       │
│     - 100% = Complete (all K sensing times have all bursts)                 │
│     - < 100% = Incomplete (some sensing times missing bursts)               │
│                                                                             │
│  7. RESOLVE DUPLICATES                                                      │
│     Duplicates = same reference date (begin) AND sensing date (end)         │
│     Detected by comparing (begin_date, end_date) from granule ID            │
│     Keep the "best": most complete sensing times, then newest production    │
│     Others are listed for deletion in deletion_lists                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Completeness Across K Sensing Times

A DISP-S1 product is considered **complete** when:
- ALL K sensing times in the product's K-cycle have ALL bursts
- For K=15 and 27 bursts, that means 405 CSLCs total

**Incomplete** products have:
- One or more sensing times with missing bursts
- Report shows which sensing times are incomplete and how many bursts are missing

### Anomalous CSLC Detection

The audit flags any CSLC inputs that don't fit expectations:

| Anomaly Type | Description |
|--------------|-------------|
| `burst not in frame` | CSLC's burst_id doesn't belong to the queried frame |
| `unexpected sensing time` | CSLC is for a sensing time outside this product's K-cycle |
| `sensing time not in database` | CSLC's acquisition date isn't in the burst database |
| `parse error` | Could not parse the CSLC ID |

These anomalies are tracked in the `anomalies` section of the report.

### Example

For a DISP-S1 product with:
- Frame 44327 (27 bursts)
- K-cycle 10 (indices 150-164)
- ISO XML lists 410 CSLC inputs

The audit:
1. Parses all 410 CSLCs
2. Categorizes by sensing time: 405 CSLCs for frame's K-cycle, 5 anomalous
3. Checks 15 sensing times: all have 27 bursts
4. Completeness: 15/15 = 100% → **Complete**
5. Anomalies: 5 CSLCs flagged (e.g., burst not in frame)

## Low Memory Mode

When auditing many frames (e.g., 500+), the process can run out of memory because it accumulates all frame reports in memory. Use `--low-memory` to enable streaming output and reduced memory usage.

### Enabling Low Memory Mode

```bash
python audit_disp_s1_completeness.py --frames 1,2,3,...,758 --output report.json --low-memory
```

### What Low Memory Mode Does

1. **Streams results to JSONL**: Instead of accumulating all reports in memory and writing a single JSON file, results are streamed to a JSONL (JSON Lines) file. Each line contains one frame's report.

2. **Omits large CSLC lists**: The full lists of CSLC IDs (`cslc_input_ids`, `available_cslcs`) are omitted from the report to reduce memory. Counts are still available.

3. **Explicit garbage collection**: Memory is explicitly cleared between frames.

4. **Output file format**: When `--output report.json` is specified with `--low-memory`, the output is saved as `report.jsonl` instead.

### Deletion Lists in Low Memory Mode

Most deletion lists are **still available** in low-memory mode:
- DISP-S1 products by anomaly reason
- DISP-S1 products found despite untriggerable K-cycle
- DISP-S1 duplicate products
- Anomalous CSLCs by reason

**NOT available** in low-memory mode:
- `cslcs_from_untriggerable_products` - This would require storing all CSLC IDs for each product, which is the main memory consumer. If you need this list, run without `--low-memory` for specific frames.

### JSONL Format

The JSONL file contains:
- First line: Header with audit parameters
- Subsequent lines: One frame report per line

```json
{"type": "header", "audit_datetime": "...", "parameters": {...}}
{"type": "frame_report", "frame_id": 1234, "report": {...}}
{"type": "frame_report", "frame_id": 5678, "report": {...}}
```

### Reading JSONL Output

```bash
# Extract all frame summaries
cat report.jsonl | jq -c 'select(.type == "frame_report") | {frame: .frame_id, summary: .report.summary}'

# Count total missing products across all frames
cat report.jsonl | jq -s '[.[] | select(.type == "frame_report") | .report.summary.missing] | add'
```

## ISO XML Caching

Fetching ISO XML metadata from CMR is slow. The caching feature stores downloaded XML files locally for faster re-runs.

### Enabling Cache

```bash
# Python script
python audit_disp_s1_completeness.py --frames 44327 --iso-cache-dir ./iso_cache

# Shell wrapper
./audit_batch_procs_completeness.sh ./batch_procs ./results --iso-cache-dir ./iso_cache
```

### Cache Behavior

- **First run**: Downloads ISO XML files and saves to cache directory
- **Subsequent runs**: Reads from cache (100% hit rate for unchanged products)
- **Cache files**: Named with product ID + hash for readability
  ```
  OPERA_L3_DISP-S1_IW_F44327_VV_20210519T014141Z_20211115_v1.0_..._a1b2c3d4.xml
  ```

### Cache Statistics

After each run, statistics are printed:
```
------------------------------------------------------------
ISO XML CACHE STATISTICS
------------------------------------------------------------
Cache directory: ./iso_cache
Cache hits: 149
Cache misses: 0
Hit rate: 100.0%
```

## Understanding the Report

### Summary Section

```
------------------------------------------------------------------------------------------------------------------------
SUMMARY
------------------------------------------------------------------------------------------------------------------------
Expected: 360  |  Triggerable: 314  |  Found: 314
Coverage: 100.0% (of triggerable)  |  Total Coverage: 87.2% (of expected)
  Complete: 314  |  Incomplete: 0  |  Stale: 0
  Missing: 0  |  Not Triggerable: 46  |  Unexpected: 0
  Products with Anomalous Inputs: 2
  Found Despite Untriggerable K-cycle: 5 (CSLC gaps in current data)
  Skipped CSLCs (not in database): 204 CSLCs across 11 sensing times
```

**Key metrics:**
- **Expected**: Total products based on sensing times in the burst database
- **Triggerable**: Products that could actually be generated (Expected - Not Triggerable)
- **Coverage (of triggerable)**: `(Complete + Incomplete + Stale) / Triggerable` - what percentage of products that COULD be generated were actually found
- **Total Coverage (of expected)**: `(Complete + Incomplete + Stale) / Expected` - overall completeness including products blocked by CSLC gaps
- **Missing**: Products that can be reprocessed (K-cycle was triggerable but no product found)
- **Not Triggerable**: Products blocked by upstream CSLC gaps
- **Anomalous Inputs**: Products whose CSLC inputs contain unexpected data
- **Found Despite Untriggerable**: Products that exist but current CSLC data shows gaps in their K-cycle
- **Skipped CSLCs**: CSLCs found in CMR but not used because their sensing times aren't in the database

### Missing Products Section

```
------------------------------------------------------------------------------------------------------------------------
MISSING PRODUCTS (3) - Actionable
------------------------------------------------------------------------------------------------------------------------
   Idx |      Day | K-Cyc |  Pos | Sensing Date |      CSLCs
------------------------------------------------------------------------------------------------------------------------
   150 |     1962 |    10 |    0 |   2021-11-27 |      27/27
   151 |     1974 |    10 |    1 |   2021-12-09 |      27/27
   152 |     1986 |    10 |    2 |   2021-12-21 |      27/27
```

**Columns:**
- **Idx**: Position in the frame's sensing_datetimes list (index_position)
- **Day**: Days since first sensing time (day_index)
- **K-Cyc**: Which K-cycle this product belongs to
- **Pos**: Position within the K-cycle (0 to K-1)
- **Sensing Date**: The product's end sensing time
- **CSLCs**: Available/Expected CSLCs for that sensing time

### Not Triggerable Section

This section groups entries by K-cycle and shows per-sensing-time CSLC counts:

```
------------------------------------------------------------------------------------------------------------------------
NOT TRIGGERABLE (46) - K-cycles with incomplete CSLC coverage
------------------------------------------------------------------------------------------------------------------------

K-Cycle 21 (indices 315-329): 0/15 sensing times have complete CSLCs
     Idx | Sensing Date |  CSLCs Found
  ----------------------------------------
     315 |   2025-01-09 |         0/26
     316 |   2025-02-02 |         0/26
     317 |   2025-02-26 |         0/26
     ...
     328 |   2025-06-14 |         0/26
     329 |   2025-06-20 |         0/26

K-Cycle 24 (indices 360-360): 14/15 sensing times have complete CSLCs
     Idx | Sensing Date |  CSLCs Found
  ----------------------------------------
     360 |   2025-12-29 |         0/26
```

**How to read this:**
- **K-Cycle header**: "0/15 sensing times have complete CSLCs" means none of the 15 sensing times in this K-cycle have all their bursts
- **CSLCs Found**: Shows X/Y where X is CSLCs found for that specific sensing date and Y is the expected count (num_bursts)
- **0/26** means no CSLCs were found in CMR for that sensing date

A K-cycle can only trigger when ALL 15 sensing times have complete CSLC coverage (all bursts present for each date).

### Anomalies Section

```
------------------------------------------------------------------------------------------------------------------------
PRODUCTS WITH ANOMALOUS CSLC INPUTS (2)
------------------------------------------------------------------------------------------------------------------------
Product: OPERA_L3_DISP-S1_IW_F44327_VV_20211115T014141Z_...
  - burst not in frame: OPERA_L2_CSLC-S1_T123-999999-IW1_...
  - unexpected sensing time (idx 180, expected 150-164): OPERA_L2_CSLC-S1_...
```

This section shows products that have CSLC inputs that don't match expectations.

### Found Despite Untriggerable Section

```
------------------------------------------------------------------------------------------------------------------------
FOUND DESPITE UNTRIGGERABLE K-CYCLE (5)
(Products exist but current CSLC data shows gaps in their K-cycle)
------------------------------------------------------------------------------------------------------------------------
   Idx | K-Cyc | CSLC Gaps  |       Status | Product ID
------------------------------------------------------------------------------------------------------------------------
   150 |    10 |          1 |     complete | OPERA_L3_DISP-S1_IW_F44327_VV_20211127T...
   151 |    10 |          1 |     complete | OPERA_L3_DISP-S1_IW_F44327_VV_20211209T...
   152 |    10 |          1 |     complete | OPERA_L3_DISP-S1_IW_F44327_VV_20211221T...
```

This section shows products that exist despite their K-cycle appearing untriggerable:
- **CSLC Gaps**: Number of sensing times in the K-cycle missing complete CSLC coverage (in current data)
- **Status**: The product's actual state (complete/incomplete/stale)

This typically indicates the burst database was updated after product generation, or CSLCs were available when generated but are no longer in CMR.

### Skipped CSLCs Section

```
------------------------------------------------------------------------------------------------------------------------
SKIPPED CSLCs (54 CSLCs) - Sensing times not in burst database
------------------------------------------------------------------------------------------------------------------------
 Day Index |  Approx Date |   CSLCs | Sample CSLC ID
------------------------------------------------------------------------------------------------------------------------
      3456 |   2025-12-15 |      27 | OPERA_L2_CSLC-S1_T064-136231-IW1_20251215T...
      3468 |   2025-12-27 |      27 | OPERA_L2_CSLC-S1_T064-136231-IW1_20251227T...
```

This section shows CSLCs found in CMR whose sensing times are not in the burst database:
- **Day Index**: The calculated day index (days since first sensing time)
- **Approx Date**: Approximate sensing date
- **CSLCs**: Number of CSLCs for this sensing time
- **Sample CSLC ID**: Example CSLC ID for reference

These are typically forward processing data (new acquisitions not yet added to the consistent burst database).

### Duplicates Section

```
------------------------------------------------------------------------------------------------------------------------
DUPLICATES (2)
(Products with same reference date AND sensing date)
------------------------------------------------------------------------------------------------------------------------
Day 1234 (ref=20160719, end=20211115): 2 products
  Best (100.0%): OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20211115T014141Z_v1.0_20250410T120000Z
  Other: OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20211115T014141Z_v1.0_20250408T163425Z
```

**Duplicate Detection Logic:**
- Duplicates are detected by comparing `(begin_date, end_date)` from the granule ID
- Both dates are extracted as `YYYYMMDD` (date only, not full timestamp)
- Products with the **same reference date AND same sensing date** are duplicates

**Duplicates vs Stale Products:**
| Scenario | Reference Date | Sensing Date | Classification |
|----------|----------------|--------------|----------------|
| True duplicate | Same | Same | **Duplicate** - produced multiple times for identical purpose |
| Stale product | Different | Same | **Stale** (reference_shifted) - K-cycle boundary changed after production |

**Best Product Selection:**
1. Most complete sensing times (highest `completeness_pct`)
2. Newest production datetime (tie-breaker)

The "Best" product is kept; "Others" are added to the deletion list (`disp_s1_duplicate_products`).

### Deletion Lists Section

When products are stale, have anomalies, or are duplicates, the audit generates deletion lists organized by reason:

```
========================================================================================================================
DELETION LISTS
========================================================================================================================

--------------------------------------------------------------------------------
STALE PRODUCTS (delete for reprocessing)
--------------------------------------------------------------------------------

--- DISP-S1 Stale Products 'reference_shifted' (3) ---
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160905T135210Z_v1.0_20250408T163425Z
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160812T135208Z_v1.0_20250408T163425Z
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20161210T135210Z_v1.0_20250408T163425Z

--- DISP-S1 Stale Products 'content_shifted' (2) ---
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20161222T135210Z_v1.0_20250408T163425Z
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20170103T135210Z_v1.0_20250408T163425Z

--- DISP-S1 Products with 'burst not in frame' (2) ---
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160905T135210Z_v1.0_20250408T163425Z
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160812T135208Z_v1.0_20250408T163425Z

--- DISP-S1 Products 'found despite untriggerable K-cycle' (5) ---
OPERA_L3_DISP-S1_IW_F18904_VV_20250109T135207Z_20250620T135210Z_v1.0_20250408T163425Z

--- DISP-S1 Duplicate Products (2) ---
(Duplicates have same reference date AND sensing date; "best" kept, others listed here)
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160905T135210Z_v1.0_20250101T000000Z
OPERA_L3_DISP-S1_IW_F18904_VV_20160719T135207Z_20160905T135210Z_v1.0_20250102T000000Z

------------------------------------------------------------------------------------------------------------------------
ANOMALOUS CSLCs FOR POTENTIAL REMOVAL
------------------------------------------------------------------------------------------------------------------------

--- CSLCs with 'burst not in frame' (3) ---
OPERA_L2_CSLC-S1_T123-999999-IW1_20211115T014141Z_20211115T014209Z_S1A_VV_v1.1
OPERA_L2_CSLC-S1_T123-999998-IW1_20211115T014141Z_20211115T014209Z_S1A_VV_v1.1

------------------------------------------------------------------------------------------------------------------------
CSLCs FROM UNTRIGGERABLE K-CYCLE PRODUCTS (405)
(These CSLCs may need deletion if reproduced for that time period)
------------------------------------------------------------------------------------------------------------------------
OPERA_L2_CSLC-S1_T064-136231-IW1_20240923T135210Z_20240923T135237Z_S1A_VV_v1.1
OPERA_L2_CSLC-S1_T064-136232-IW1_20240923T135210Z_20240923T135237Z_S1A_VV_v1.1
...
```

**Deletion list reasons:**
| Reason | Description |
|--------|-------------|
| `reference_shifted` (stale) | K-cycle boundary changed after product was generated |
| `content_shifted` (stale) | Sensing times within K-cycle shifted after product was generated |
| `burst not in frame` | Product contains CSLCs from bursts not belonging to the queried frame |
| `unexpected sensing time` | Product contains CSLCs from sensing times outside its K-cycle |
| `sensing time not in database` | Product contains CSLCs with dates not in the burst database |
| `parse error` | Product contains CSLCs that couldn't be parsed |
| `found despite untriggerable K-cycle` | Product exists but current CSLC data shows gaps in its K-cycle |
| `duplicate` | Multiple products with same reference date AND sensing date (from granule ID); "best" kept, others listed for deletion |

These lists provide full product/CSLC IDs for easy copy/paste when submitting deletion requests to CMR.

## Workflow Examples

### 1. Initial Audit

```bash
# Run audit with caching enabled
./audit_batch_procs_completeness.sh ~/batch_procs ./results --iso-cache-dir ./iso_cache

# Review summary
cat ./results/completeness_summary.txt
```

### 2. Investigate Missing Products

```bash
# Check specific frame with detailed output
python audit_disp_s1_completeness.py --frames 44327 --debug 2>&1 | tee frame_44327.log

# Use diagnose tool to see actual products in CMR
python diagnose_disp_s1_frame_products.py --frame 44327 --start 2021-11-01 --end 2022-01-15
```

### 3. Re-run After Processing

```bash
# Re-run uses cached ISO XML (fast!)
./audit_batch_procs_completeness.sh ~/batch_procs ./results_v2 --iso-cache-dir ./iso_cache

# Compare results
diff ./results/completeness_summary.txt ./results_v2/completeness_summary.txt
```

### 4. Check for Stale Products After Database Update

After updating the burst database with new sensing times:

```bash
# Audit will detect stale products (both reference_shifted and content_shifted)
python audit_disp_s1_completeness.py --frames 44327

# Look for "STALE PRODUCTS" section in output
```

## JSON Output Format

When using `--output`, the JSON structure is:

```json
{
  "audit_datetime": "2025-01-08T10:30:00",
  "parameters": {
    "frames": [44327],
    "start": "2016-07-01T00:00:00Z",
    "end": "2025-12-31T00:00:00Z",
    "endpoint": "OPS",
    "k": 15
  },
  "reports": {
    "44327": {
      "frame_id": 44327,
      "num_bursts": 27,
      "k": 15,
      "expected_count": 360,
      "actual_count": 314,
      "summary": {
        "expected": 360,
        "triggerable": 314,
        "found": 314,
        "complete": 314,
        "incomplete": 0,
        "stale": 0,
        "stale_by_reason": {},
        "missing": 0,
        "not_triggerable": 46,
        "unexpected": 0,
        "duplicates": 0,
        "anomalies": 2,
        "found_despite_untriggerable": 5,
        "skipped_cslcs": 54,
        "skipped_sensing_times": 2,
        "coverage_pct": 100.0,
        "total_coverage_pct": 87.2
      },
      "missing": [...],
      "not_triggerable": [...],
      "incomplete": [...],
      "complete": [...],
      "stale": [...],
      "unexpected": [...],
      "duplicates": [...],
      "anomalies": [...],
      "skipped_cslcs": {
        "skipped_sensing_times": [...],
        "parse_errors": [...],
        "total_skipped_cslcs": 54,
        "total_skipped_sensing_times": 2
      },
      "deletion_lists": {
        "disp_s1_stale_products": {
          "reference_shifted": [
            "OPERA_L3_DISP-S1_IW_F44327_VV_..."
          ],
          "content_shifted": [
            "OPERA_L3_DISP-S1_IW_F44327_VV_..."
          ]
        },
        "disp_s1_products_by_reason": {
          "burst not in frame": [
            "OPERA_L3_DISP-S1_IW_F44327_VV_..."
          ]
        },
        "disp_s1_products_found_despite_untriggerable": [
          "OPERA_L3_DISP-S1_IW_F44327_VV_...",
          "OPERA_L3_DISP-S1_IW_F44327_VV_..."
        ],
        "disp_s1_duplicate_products": [
          "OPERA_L3_DISP-S1_IW_F44327_VV_...",
          "OPERA_L3_DISP-S1_IW_F44327_VV_..."
        ],
        "cslcs_from_untriggerable_products": [
          "OPERA_L2_CSLC-S1_T064-136231-IW1_20240923T135210Z_...",
          "OPERA_L2_CSLC-S1_T064-136232-IW1_20240923T135210Z_..."
        ],
        "anomalous_cslcs_by_reason": {
          "burst not in frame": [
            "OPERA_L2_CSLC-S1_T123-999999-IW1_..."
          ],
          "unexpected sensing time": [
            "OPERA_L2_CSLC-S1_T064-136231-IW1_..."
          ]
        }
      }
    }
  }
}
```

### Extracting Deletion Lists from JSON

```bash
# Extract all stale products (both reasons)
jq -r '.reports[].deletion_lists.disp_s1_stale_products[][]' report.json

# Extract stale products with 'reference_shifted' reason
jq -r '.reports[].deletion_lists.disp_s1_stale_products.reference_shifted[]' report.json

# Extract stale products with 'content_shifted' reason
jq -r '.reports[].deletion_lists.disp_s1_stale_products.content_shifted[]' report.json

# Extract all DISP-S1 products with 'burst not in frame' anomaly
jq -r '.reports[].deletion_lists.disp_s1_products_by_reason["burst not in frame"][]' report.json

# Extract all products found despite untriggerable K-cycle
jq -r '.reports[].deletion_lists.disp_s1_products_found_despite_untriggerable[]' report.json

# Extract CSLCs from untriggerable K-cycle products (may need deletion if reproducing)
jq -r '.reports[].deletion_lists.cslcs_from_untriggerable_products[]' report.json

# Extract duplicate products
jq -r '.reports[].deletion_lists.disp_s1_duplicate_products[]' report.json

# Extract all anomalous CSLCs
jq -r '.reports[].deletion_lists.anomalous_cslcs_by_reason[][]' report.json

# Get all deletion list product IDs across all reasons
jq -r '.reports[].deletion_lists | (.disp_s1_stale_products[][], .disp_s1_products_by_reason[][], .disp_s1_products_found_despite_untriggerable[], .disp_s1_duplicate_products[])' report.json
```

## Real-World Example: Frame 46290 Stale Detection

This example shows how the audit detects stale products after a burst database update.

### Scenario

The burst database was updated to add a new sensing time at index 339. Before this update:
- K-cycle 22 had indices 330-344 (but index 339 didn't exist in the database)
- Products 330-344 were generated with CSLCs for 14 sensing times (missing 339)
- Product 345 was generated as part of the K-cycle that started at index 330

### Running the Audit

```bash
python audit_disp_s1_completeness.py --frames 46290 --end 2026-01-01T23:59:59Z
```

### Results

```
SUMMARY
------------------------------------------------------------------------------------------------------------------------
Expected: 356  |  Triggerable: 356  |  Found: 344
Coverage: 96.6% (of triggerable)  |  Total Coverage: 96.6% (of expected)
  Complete: 329  |  Incomplete: 0  |  Stale: 15 (content_shifted: 14, reference_shifted: 1)
  Missing: 12  |  Not Triggerable: 0  |  Unexpected: 0
```

### Understanding the Stale Products

**content_shifted (14 products)** - Indices 330-344:
- These products are in K-cycle 22 (indices 330-344)
- They contain CSLCs from index 345 (which is now in K-cycle 23)
- They're missing index 339 (was added to database after processing)
- Affected indices shown as `345` (CSLCs from wrong K-cycle)

**reference_shifted (1 product)** - Index 345:
- This product is now in K-cycle 23 (indices 345-359)
- Its BeginningDateTime references K-cycle 22's start (index 329)
- Should reference K-cycle 23's start (index 344)
- Shows `Actual Begin: 329` vs `Expected: 344`

### Deletion List

The audit generates a deletion list for cleanup:

```
--- DISP-S1 Stale Products 'content_shifted' (14) ---
OPERA_L3_DISP-S1_IW_F46290_VV_20250721T134318Z_20250727T134423Z_v1.0_20251125T161851Z
OPERA_L3_DISP-S1_IW_F46290_VV_20250721T134318Z_20250802T134318Z_v1.0_20251125T161851Z
... (12 more)

--- DISP-S1 Stale Products 'reference_shifted' (1) ---
OPERA_L3_DISP-S1_IW_F46290_VV_20250721T134318Z_20251025T134320Z_v1.0_20251125T161851Z
```

### Cleanup Workflow

1. Delete all 15 stale products from CMR
2. Trigger reprocessing for the affected K-cycles
3. New products will be generated with:
   - Correct K-cycle boundaries
   - Index 339 included in K-cycle 22 products
   - Product 345+ using K-cycle 23 reference

## Troubleshooting

### High "Not Triggerable" Count

This indicates upstream CSLC gaps. Investigate:
1. Which sensing times are missing CSLCs?
2. Was there a Sentinel-1 data gap during that period?
3. Did CSLC processing fail for those dates?

### High "Unexpected" Count with Early Dates

Products from 2016-2017 often show as "unexpected" because they were generated with `index_position < k-1` (insufficient history). This is expected behavior for early products.

### Stale Products After Database Update

If products show as "stale" after a burst database update:
1. **reference_shifted**: K-cycle boundary has changed. The `actual_begin_idx` vs `expected_begin_idx` shows the discrepancy.
2. **content_shifted**: K-cycle boundary is correct but sensing times within the cycle have shifted. The `shifted_indices` field shows which indices are affected.

Both types need to be deleted from CMR so reprocessing can generate correct outputs with the updated database.

### Slow First Run

The first run downloads ISO XML for every product. Use `--iso-cache-dir` to cache these files:
- First run: ~5-10 minutes per 100 products
- Cached runs: ~30 seconds per 100 products

## Related Tools

| Tool | Purpose |
|------|---------|
| `diagnose_disp_s1_frame_products.py` | Detailed product analysis for a frame |
| `cmr_audit_disp_s1.py` | Frame state calculation and gap detection |
| `cmr_iso_xml_utils.py` | Shared ISO XML fetching utilities |
