# DISP-S1 large-gap purge utility

`delete_disp_s1_large_gap.py` removes CCSLC and DISP-S1 products belonging to
k-cycles that span a large (> 2 year) sensing-time gap, and produces a removal
list for the DAAC.

## Why

Frames that stopped acquiring — most of them across the S1B outage — and
resumed years later were processed on the absolute k=15 grid. The ministack
that straddles the gap phase-links pre-gap and post-gap acquisitions, which is
not a valid interferometric stack, and every CCSLC after it inherits the
problem through the lineage. Those products have to come out of the system (and
off the DAAC) before the affected frames can be reprocessed with gap-aware
phased historical processing.

Everything at or before the last clean pre-gap k-cycle is valid history and is
kept.

## What counts as affected

Per frame, in order:

1. **`gap_start_date`** — the last sensing date before the frame's first gap
   longer than `--gap-days` (default 730), from the consistent burst database.
   When the frame is not in the database, the gap window from a `--gap-list`
   report is used instead.
2. **`last_clean_boundary_date`** — the most recent CCSLC `last_date` at or
   before `gap_start_date`. This is anchored on the CCSLCs that actually exist
   rather than on burst-database positions, because the database vintage used
   for processing is not always the one used for analysis. With no CCSLCs on
   hand it falls back to burst-database k-cycle math, and a frame with fewer
   than k pre-gap dates has no clean boundary at all.
3. **affected CCSLCs** — `last_date` after `gap_start_date`.
4. **affected DISP-S1 products** — secondary date after
   `last_clean_boundary_date`; every product when there is no clean boundary.

Products are matched by parsing granule ids, so GRQ, S3 and CMR are each
inventoried independently and reconciled. A product that aged out of GRQ but is
still in S3 (or vice versa) is still found.

## Safety model

* Every subcommand is read-only by default. `execute` deletes nothing without
  `--execute`, and then only after a typed `DELETE` confirmation (`--yes` skips
  the prompt for scripted runs).
* Selection happens once, in `audit`. `execute` consumes the reviewed manifests
  and never recomputes what to delete.
* Before any delete, `execute` writes an NDJSON backup of every document and
  copies them into `parked_disp_s1_large_gap_*` indices (`--skip-park` opts out
  of the copy; the NDJSON is always written).
* `--max` caps how many items a single run may delete (default 10,000, counting
  GRQ docs plus S3 datasets); `--force` overrides it.
* S3 prefixes shorter than a dataset directory are refused outright.
* Deletes are idempotent — re-running over an already-purged manifest succeeds.

S3 objects are not recoverable once deleted. GRQ documents are, from the parked
indices or the NDJSON backup.

## Running it

Run on the Mozart instance. Use the mozart virtualenv — the system
`/usr/bin/python3` is 3.6 and has neither `boto3` nor `opensearch-py`:

```bash
cd ~/mozart/ops/opera-pcm
RUN=~/DISP-S1/large_gap_2yrs/opera_2610_run
CBDB=~/DISP-S1/large_gap_2yrs/opera-disp-s1-consistent-burst-ids-2026-08-10-2016-07-01_to_2026-04-30.json

# 1. inventory and classify (read-only)
~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py audit \
    --gap-list ~/DISP-S1/large_gap_2yrs/prior0_large_gap.txt \
    --cbdb $CBDB --run-dir $RUN

# 2. review $RUN/summary_prior0.md and the manifests, then rehearse
~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py execute \
    --run-dir $RUN --ccslc --l3

# 3. delete for real
~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py execute \
    --run-dir $RUN --ccslc --l3 --execute

# 4. confirm the affected sets are gone and the kept sets are intact
~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py verify \
    --run-dir $RUN --cbdb $CBDB

# 5. removal list for the DAAC
~/mozart/bin/python3 tools/delete_disp_s1_large_gap.py asf-list --run-dir $RUN
```

Start with one frame (`--frames 24726`) before running a whole priority list.

### Frame input

Any one of:

* `--gap-list prior0_large_gap.txt` — the large-gap report (`FRAME` / `GAP:`
  lines). The gap windows in the file are used when a frame is missing from the
  burst database.
* `--frames-file priority0_frames.json` — `{"frames": [...]}`.
* `--frames 24726,44325`.

The run label defaults to the input filename (`prior0_large_gap.txt` →
`prior0`) and names every output file. `asf-list`, `execute` and `verify` infer
it from the run directory when only one audit is present.

### Connection settings

Read from `~/.sds/config` (`GRQ_ES_*`, `GRQ_PVT_IP`, `LTS_BUCKET`,
`DATASET_BUCKET`) and `~/.netrc-os` for OpenSearch credentials when the cluster
speaks https. Override with `--es-url`, `--es-user`, `--es-password`,
`--lts-bucket`, `--rs-bucket`.

## Outputs

All written to `--run-dir`:

| file | contents |
| --- | --- |
| `audit_<label>.json` | per-frame inventory, boundaries, counts, anomalies |
| `manifest_ccslc_<label>.jsonl` | one row per CCSLC to delete |
| `manifest_l3_<label>.jsonl` | one row per DISP-S1 product to delete |
| `manifest_csc_<label>.jsonl` | cycle-state-configs (only with `--state`) |
| `summary_<label>.md` | totals, per-frame table, anomalies |
| `asf_removal_<label>.csv` | DAAC removal list |
| `backup_<label>_<phase>_<ts>.ndjson` | full documents, written before deleting |
| `results_<label>_<ts>.json` | what each execute phase actually did |
| `verify_<label>.md` / `.json` | post-purge verification |

## Anomalies worth reading

`audit` records these per frame instead of guessing:

| anomaly | meaning |
| --- | --- |
| `not-in-burst-db` | frame absent from the burst database supplied |
| `no-gap-in-burst-db` | no gap over the threshold — nothing to purge |
| `gap-from-input-list` | gap window taken from the `--gap-list` report |
| `no-clean-boundary-every-product-affected` | fewer than k pre-gap dates |
| `ccslc-in-grq-not-s3` / `ccslc-in-s3-not-grq` | the two stores disagree |
| `duplicate-ccslc-windows` | same burst and window, several creation times |
| `partial-ccslc-boundaries` | a boundary with fewer CCSLCs than the frame has bursts |
| `unparseable-ccslc-id` / `unparseable-l3-id` | not a valid granule name; never deleted |

## Scope notes

* `--state` collects cycle-state-config documents, which are scoped by frame and
  safe to remove. Staged `L2_CSLC` metadata is deliberately **not** touched: a
  burst belongs to several frames, so deleting those documents by frame could
  damage a frame that is not in scope. Clear them from the reprocessing side,
  where the full scope is known.
* Nothing here re-enables or modifies batch procs. Do not re-enable the historical
  catchup batch procs for gap frames — the deployed release has no gap guard and
  would recreate exactly the products this tool removes.

## Tests

```bash
python3 -m pytest tests/unit/test_delete_disp_s1_large_gap.py
```
