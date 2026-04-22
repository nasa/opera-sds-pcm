# run_dist_s1_historical_processing.py Usage

This document describes how to run the DIST-S1 historical processing wrapper and sample command lines.

## Purpose

`run_dist_s1_historical_processing.py` (also available as `run_dist_s1_hist.py` in some deployments) is a wrapper script that:

- creates a temporary run directory,
- runs a `daac_data_subscriber.py query` command for the requested DIST-S1 date range,
- optionally ingests `DIST_S1_state-config*` files in that temporary directory,
- supports dry-run mode,
- optionally preserves the temporary directory,
- optionally writes logs to a file.

## Basic usage

From the repository root:

```bash
python tools/run_dist_s1_historical_processing.py \
  --start-date=2026-01-01T00:00:00Z \
  --end-date=2026-02-01T00:00:00Z \
  --filter-tiles 18NUF 18FWH 44SQA 44RQU \
  --keep-dir
```

This runs the data subscriber query for the `DIST_S1` product and keeps the temporary run directory after completion.

## Bounds example

```bash
python tools/run_dist_s1_historical_processing.py \
  --start-date=2026-01-01T00:00:00Z \
  --end-date=2026-02-01T00:00:00Z \
  --bounds=-119.0,31.67,-114.02,36.05 \
  --dry-run \
  --keep-dir
```

This prints the constructed `daac_data_subscriber.py query` command without running it, and preserves the temporary directory.

## Command behavior

The wrapper builds a data subscriber query similar to:

```bash
daac_data_subscriber.py query \
  --collection-shortname=OPERA_L2_RTC-S1_V1 \
  --product=DIST_S1 \
  --endpoint=OPS \
  --start-date=2026-01-01T00:00:00Z \
  --end-date=2026-02-01T00:00:00Z \
  --job-queue=opera-job_worker-rtc_for_dist_data_download \
  --chunk-size=1 \
  --use-temporal \
  --transfer-protocol=auto \
  --processing-mode=historical \
  --grace-min=1
```

If `--filter-tiles` is supplied, the command adds:

```bash
--filter-tiles 18NUF 18FWH 44SQA 44RQU
```

If `--bounds` is supplied, the command adds:

```bash
--bound=-119.0,31.67,-114.02,36.05
```

## Optional flags

- `--dry-run`: build and print the query command, but do not execute it or ingest state-config files.
- `--keep-dir`: preserve the temporary run directory created during execution.
- `--log-file <path>`: write logs to the specified file. If omitted, the wrapper saves logs to a default file next to the temp directory.

## Notes

- The script requires the `daac_data_subscriber.py` path to resolve correctly.
- The wrapper creates a temporary directory named like `dist_s1_hist_run_YYYYMMDDHHMMSS`.
- If no `DIST_S1_state-config*` files exist in the temp directory, the script logs a warning and continues.
