# `slc_to_mgrs_tiles.py` Usage

This document describes how to run the SLC-to-MGRS tile resolution script and sample command lines.

## Purpose

`slc_to_mgrs_tiles.py` is an end-to-end processing script that:

- reads input Sentinel-1 SLC granule names from a text file,
- queries NASA CMR (ASF) for OPERA L2 RTC-S1 granules derived from those SLC inputs,
- validates product provenance using UMM input granule metadata,
- extracts unique OPERA Burst IDs (e.g., `T064-135225-IW1`) directly from confirmed RTC granules,
- fetches or reuses the OPERA MGRS burst lookup Parquet table from AWS S3 (with direct HTTPS fallback and offline handling),
- maps extracted Burst IDs to MGRS Tile Acquisition Groups (e.g., `37NBB_2`).

## Workflow diagram

```
   [ Input SLC Names File ]
              │
              ▼
  ┌──────────────────────────┐
  │  1. NASA CMR Query       │ ──► Search OPERA_L2_RTC-S1_V1 metadata
  └──────────────────────────┘
              │
              ▼
  ┌──────────────────────────┐
  │  2. Extract Burst IDs    │ ──► Parse GranuleURs for 'TXXX-XXXXXX-IWX'
  └──────────────────────────┘
              │
              ▼
  ┌──────────────────────────┐
  │  3. S3 / HTTPS Fetch     │ ──► Unsigned S3 -> HTTPS fallback -> Local Cache
  └──────────────────────────┘
              │
              ▼
  ┌──────────────────────────┐
  │  4. Parquet Lookup Join  │ ──► Map Bursts -> MGRS Tile Acquisition Groups
  └──────────────────────────┘
              │
              ▼
   [ Output: MGRS Tile Groups ]
```

## Basic usage

From the script directory:

```bash
python slc_to_mgrs_tiles.py \
  --input slc_names.txt
```

## Command line options

| Option | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--input` | `-i` | *(Required)* | Text file with one SLC granule name per line. |
| `--output-json` | `-o` | `rtc_query_results.json` | Path for summary JSON output file. |
| `--output-tiles` | `-t` | `matched_tile_acq_groups.txt` | Path for output MGRS Tile Acquisition Groups text file. |
| `--provider` | | `ASF` | CMR provider name. |
| `--short-name` | | `OPERA_L2_RTC-S1_V1` | CMR collection short name. |
| `--delay` | | `0.25` | Delay in seconds between consecutive CMR requests. |
| `--debug` | | *(Flag)* | Print CMR request URLs and page response status. |

## Sample command lines

### Standard run with default outputs

```bash
python slc_to_mgrs_tiles.py \
  --input /path/to/slc_granules.txt
```

### Custom output paths and request delay

```bash
python slc_to_mgrs_tiles.py \
  --input slc_list.txt \
  --output-json outputs/slc_rtc_mapping.json \
  --output-tiles outputs/target_mgrs_groups.txt \
  --delay 0.5
```

### Debug run

```bash
python slc_to_mgrs_tiles.py \
  --input slc_list.txt \
  --debug
```

## Input file format

The input file should be a plain text file with one Sentinel-1 SLC granule name per line. Empty lines and lines starting with `#` are ignored:

```text
# Sentinel-1 SLC Granules
S1A_IW_SLC__1SDV_20230512T051220_20230512T051247_048510_05D5E1_1A2B
S1B_IW_SLC__1SDV_20210410T170112_20210410T170139_026408_0327AF_4C8D
```

## Generated outputs

- **`matched_tile_acq_groups.txt`**: Sorted list of unique MGRS Tile Acquisition Groups (e.g., `11SDA_1`).
- **`rtc_query_results.json`**: Detailed JSON report summarizing confirmed/unconfirmed RTC granules and missing scenes.
- **`rtc_query_results_granule_ids.txt`**: Plain text list of all confirmed `GranuleUR` strings.
