# DIST-S1 Input Tool

A CLI tool for querying and selecting RTC-S1 input files for DIST-S1 (Displacement) processing using historical lookback windows.

## Overview

The DIST-S1 algorithm requires historical RTC-S1 data from three lookback windows:
- **w1**: Files from ~1 year ago (closest to t0 - 365 days)
- **w2**: Files from ~2 years ago (closest to t0 - 730 days)
- **w3**: Files from ~3 years ago (closest to t0 - 1095 days)

This tool queries CMR (Common Metadata Repository) to find available RTC-S1 granules in these windows and selects the optimal files for DIST-S1 processing. For each burst+subswath combination in a tile, the tool independently selects files from each lookback window, generating "baseline products."

## Key Features

- **Automatic spatial filtering**: Derives bounding boxes from MGRS tile IDs for efficient CMR queries
- **Burst-level processing**: Generates independent baseline products for each burst+subswath combination
- **Track identification**: Automatically identifies Sentinel-1 tracks and validates burst coverage
- **Three operating modes**: Single query, batch processing, and temporal window forecasting
- **Multiple output formats**: Human-readable text, structured JSON, or granule IDs only
- **RTC cache support**: Optional integration with GRQ RTC cache for faster queries (cluster-only)

## Installation

Requires Python 3.9+ with dependencies from the OPERA PCM environment.

**Recommended:** Install using the `cmr_audit` extras from the PCM setup:

```bash
# From the opera-sds-pcm repository root
pip install -e .[cmr_audit]
```

This installs all required dependencies including:
- `python-dateutil` - Date parsing and manipulation
- `mgrs` - MGRS tile coordinate conversion
- `aiohttp` - Async HTTP client for CMR queries
- `backoff` - Retry logic for network requests
- `elasticsearch` - RTC cache support (cluster environments)
- Other supporting libraries

**Alternative:** Install individual dependencies:

```bash
pip install python-dateutil mgrs aiohttp backoff
```

## Usage Modes

### 1. Single Query Mode

Query for a specific MGRS tile at a specific acquisition time.

**By tile ID and time:**
```bash
python3 dist_s1_input_tool.py T168 2025-09-25T12:00:00Z
```

**By DIST-S1 native ID** (automatically extracts tile ID and time):
```bash
python3 dist_s1_input_tool.py --native-id OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250924T224019Z_S1A_30_v1.0
```

**Output:**
- Lists baseline products (one per burst+subswath)
- Shows selected RTC granules for w1, w2, w3 windows
- Displays diagnostics (track info, burst coverage, missing bursts)

### 2. Batch Mode

Process multiple queries from a file containing DIST-S1 native IDs (one per line).

```bash
python3 dist_s1_input_tool.py --input-file missing_products.txt
```

**Input file format:**
```
OPERA_L3_DIST-ALERT-S1_T20QLE_20250924T222019Z_20250924T224019Z_S1A_30_v1.0
OPERA_L3_DIST-S1_T031SGR_20250925T120000Z_20250925T122000Z_S1B_30_v1.0
OPERA_L3_DIST-ALERT-S1_T168_20250926T080000Z_20250926T082000Z_S1A_30_v1.0
```

**Features:**
- Processes queries concurrently (default: 3 concurrent queries)
- Automatically filters results to find confirmed missing products
- Saves validated results to file

### 3. Temporal Window Mode

Forecast how many DIST-S1 jobs would be triggered across all tiles between a start and end date.

```bash
python3 dist_s1_input_tool.py --temporal-window \
  --start-date 2025-09-01T00:00:00Z \
  --end-date 2025-09-08T00:00:00Z
```

**What it does:**
1. Queries CMR for all RTC granules in the time period
2. Maps bursts to MGRS tiles using the DIST-S1 burst database
3. Identifies unique (tile, acquisition_time) pairs
4. For each pair, checks if sufficient historical data exists in lookback windows
5. Reports statistics on jobs that would have sufficient vs. insufficient inputs

**Output includes:**
- Total unique tiles with RTC data
- Total acquisition times analyzed
- Jobs with sufficient inputs (can run)
- Jobs with insufficient inputs (would fail)
- Breakdown by tile and by date
- Detailed diagnostics for each job

**Use case:** Planning, capacity forecasting, identifying data gaps

## Common Options

### Window Configuration

**`--window-size DAYS`** (default: 60)
- Size of each lookback window in days
- Each window looks backward from the target date (t0 - N years)
- Example: `--window-size 30` uses 30-day windows instead of 60-day

**`--max-files W1,W2,W3`** (default: 4,3,3)
- Maximum files to select from each window
- Format: comma-separated list for w1, w2, w3
- Example: `--max-files 5,4,4` selects up to 5 files from w1, 4 from w2/w3

### Output Options

**`--output FORMAT`** (default: text)
- `text`: Human-readable output with statistics
- `json`: Structured JSON with full details
- `ids`: Granule IDs only (one per line)

**`--output-file PATH`**
- Save output to file
- Auto-generates filename if not specified (for JSON mode)
- Example: `--output json --output-file results.json`

**`--log-file PATH`**
- Save logs to file in addition to console output
- Useful for debugging or record-keeping

### Performance Options

**`--max-concurrent N`** (default: 3)
- Maximum concurrent queries in batch/temporal modes
- Each query makes 3 CMR requests (one per window)
- Higher values = faster but more load on CMR

**`--use-rtc-cache`**
- Use GRQ RTC cache instead of CMR queries
- Only available when running in the OPERA SDS cluster
- Significantly faster for lookback window queries

## Output Formats

### Text Output (Default)

```
================================================================================
DIST-S1 Lookback Window Query
================================================================================
Tile ID: T168
Reference time (t0): 2025-09-25T12:00:00
Window size: -60 days
Max files per window: w1=4, w2=3, w3=3
Bounding box: Will auto-derive from tile ID
================================================================================

Step 1: Finding RTC bursts at acquisition time...
Found 16 bursts at t0
Identified track: T168_359429_IW123 (16 bursts)

Step 2: Querying historical data across all windows...
Retrieved 245 granules for w1, 198 for w2, 156 for w3

Generated 48 baseline products (16 bursts × 3 subswaths)

Summary:
  Total baseline products: 48
  Total granules selected: 480 (w1=192, w2=144, w3=144)
```

### JSON Output

```json
{
  "query": {
    "tile_id": "T168",
    "reference_time": "2025-09-25T12:00:00",
    "window_size_days": 60,
    "max_files": [4, 3, 3]
  },
  "baseline_products": {
    "359429-IW1": {
      "burst_id": "359429",
      "subswath": "IW1",
      "w1": [
        {"granule_id": "OPERA_L2_RTC-S1_...", "acquisition_time": "2024-09-20T..."},
        ...
      ],
      "w2": [...],
      "w3": [...]
    },
    ...
  },
  "diagnostics": {
    "track_id": "T168_359429_IW123",
    "total_bursts_at_t0": 16,
    "expected_bursts_for_track": 16,
    "missing_bursts": []
  }
}
```

### IDs Output

```
OPERA_L2_RTC-S1_T168-359429-IW1_20240920T120530Z_20240922T001234Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T168-359429-IW1_20240906T120530Z_20240908T001234Z_S1A_30_v1.0
...
```

## Examples

### Basic usage
```bash
# Query for a specific tile and time
python3 dist_s1_input_tool.py T031SGR 2024-02-29T12:00:00Z

# Use a DIST-S1 product ID
python3 dist_s1_input_tool.py --native-id OPERA_L3_DIST-S1_T168_20250925T120000Z_...
```

### Custom window configuration
```bash
# Use 30-day windows instead of default 60
python3 dist_s1_input_tool.py T102 2025-09-25T12:00:00Z --window-size 30

# Select more files per window
python3 dist_s1_input_tool.py T168 2025-09-25T12:00:00Z --max-files 5,4,4
```

### Batch processing
```bash
# Process multiple products
python3 dist_s1_input_tool.py --input-file missing_products.txt --max-concurrent 5

# Save results to JSON
python3 dist_s1_input_tool.py --input-file products.txt --output json --output-file results.json
```

### Temporal forecasting
```bash
# Weekly forecast
python3 dist_s1_input_tool.py --temporal-window \
  --start-date 2025-09-01T00:00:00Z \
  --end-date 2025-09-08T00:00:00Z

# Monthly forecast with JSON output
python3 dist_s1_input_tool.py --temporal-window \
  --start-date 2025-09-01T00:00:00Z \
  --end-date 2025-10-01T00:00:00Z \
  --output json --output-file september_forecast.json
```

### Using RTC cache (cluster only)
```bash
# Faster queries using cache
python3 dist_s1_input_tool.py T168 2025-09-25T12:00:00Z --use-rtc-cache

# Batch mode with cache
python3 dist_s1_input_tool.py --input-file products.txt --use-rtc-cache --max-concurrent 10
```

## Algorithm Details

### Lookback Window Selection

For each burst+subswath combination:

1. **Calculate target dates**:
   - Window 1 ends at t0 - 365 days
   - Window 2 ends at t0 - 730 days
   - Window 3 ends at t0 - 1095 days

2. **Query CMR**: Find all RTC granules in each window (target_date ± window_size)

3. **Select closest files**: For each window, select up to N files with acquisition times closest to the window's end date (target date)

4. **Deduplicate**: Keep only the latest processing version for each acquisition time

### Track Identification

The tool uses the DIST-S1 burst database to:
- Map MGRS tiles to expected burst IDs
- Identify Sentinel-1 tracks based on active bursts at t0
- Validate complete burst coverage for the track
- Report missing or unexpected bursts

### Baseline Products

A "baseline product" represents one DIST-S1 processing unit:
- **One burst+subswath** (e.g., "359429-IW2")
- **Selected granules** from w1, w2, w3 windows
- **Independent selection** for each burst+subswath

For a tile with 16 bursts and 3 subswaths, this generates 48 baseline products.

## Troubleshooting

### "No RTC bursts found at acquisition time"
- RTC data may not exist for this tile/time
- Check if the acquisition time is correct
- Verify the tile ID is valid

### "Track identification failed"
- The burst database may not include this tile
- Falls back to processing all found bursts
- Consider updating the burst database

### "Incomplete burst coverage"
- Some bursts expected for the track are missing at t0
- DIST-S1 processing may still succeed with partial coverage
- Check CMR for data availability issues

### "Insufficient historical data in window"
- No RTC granules found in one or more lookback windows
- May indicate a data gap in the archive
- Consider adjusting `--window-size` to search a wider range

### CMR query timeouts
- Reduce `--max-concurrent` to avoid overwhelming CMR
- Use `--use-rtc-cache` if running in cluster
- Check network connectivity

## Technical Notes

- **Bounding boxes**: Automatically derived from MGRS tile IDs using the `mgrs` library
- **CMR queries**: Uses ASF provider and OPERA_L2_RTC-S1_V1 collection
- **Acquisition time tolerance**: Groups granules within ±10 minutes
- **Deduplication**: Automatically handles reprocessed granules
- **Concurrency**: Async/await for efficient parallel CMR queries

## Related Tools

- **DIST-S1 PGE**: Processes the selected RTC inputs to generate displacement products
- **CMR Audit Tool**: Validates DIST-S1 products exist for all expected acquisitions
- **RTC-S1 Input Tool**: Similar tool for selecting SLC inputs for RTC-S1 processing
