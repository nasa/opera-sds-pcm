# OPERA analysis mirror

A single-node OpenSearch instance holding read-only copies of the OPS-FWD and
OPS-POP1 clusters, so analysts and `opera-monitoring` can query operational data
without touching production.

Data arrives via the hourly snapshots the OPS clusters already write to S3. The
mirror registers those repositories read-only and restores from them; it never
writes to the buckets and never connects to the OPS clusters.

## Venue prefixes

**Every index is prefixed with its venue — `fwd_*` or `pop1_*`. No index keeps
its canonical HySDS name.**

Both venues run a cluster named `common_cluster` and **162 index names collide**:
most of the operational time-series (`worker_status`, `logstash`, `task_status`,
`job_status`, `event_status`) plus several `grq_` product indices such as
`grq_v1.0_l2_rtc_s1`, `grq_1_l1_s1_slc`, and `grq_v1.1_l2_cslc_s1`. The colliding
`grq_` indices hold genuinely different documents per venue — forward products
versus reprocessed ones — so merging them would be wrong, not merely untidy.

Prefixing makes a venue-less query match nothing instead of silently returning
one venue's data and looking complete:

```
one venue    fwd_job_status-*
both venues  *job_status-*
canonical    job_status-*        <- matches nothing, by design
```

## Layout

| | |
|---|---|
| Mirror | `https://100.104.82.14:9200` (TLS, single node) |
| Repositories | `snapshot-repo-fwd` -> `s3://opera-ops-es-bucket/opera-ops-fwd/cluster`<br>`snapshot-repo-pop1` -> `s3://opera-ops-es-bucket/opera-ops-pop1/cluster` (both `readonly: true`) |
| Credentials | `~/.netrc` — **keyed to the IP, not `localhost`**. `curl --netrc-file` against `https://localhost:9200` silently returns `Unauthorized`. |
| State | `~/.opera_mirror_refresh_state.json` |
| Lock | `~/.opera_mirror_refresh.lock` |

## Incremental refresh

`mirror_refresh.py` brings the mirror up to date. Standard library only — the
mirror host has no mozart/conda environment for analyst accounts.

```bash
./run_mirror_refresh.sh                    # both venues
./run_mirror_refresh.sh --dry-run          # preview
./run_mirror_refresh.sh --venue fwd=snapshot-repo-fwd   # one venue
```

Cron (the wrapper is `flock`-guarded, so a slow run will not double up):

```
0 */2 * * * $HOME/analysis/run_mirror_refresh.sh >> $HOME/mirror_refresh.log 2>&1
```

### How change detection works

Each index is fingerprinted as `file_count:size_in_bytes` from the snapshot
`_status` API's **total** stats, and compared against the fingerprint recorded on
its last successful restore.

Comparing against our own last-restored state — rather than against the previous
snapshot's *incremental* stats — means **skipped refresh cycles are safe**. If
the cron is down for six hours, everything that changed in the meantime is still
detected on the next run. A fingerprint is recorded **only after a successful
restore**, so a failed index is retried on the next run instead of being quietly
adopted as current.

Changed indices are deleted locally and restored directly from the snapshot,
which reproduces the snapshot's own mappings exactly. There is no reindex step
and no dependency on registered index templates.

Indices that disappear from the source (ISM roll-off) are **kept** on the mirror
— retaining history that OPS has already dropped is part of the point — but are
dropped from state tracking so they stop being reported.

### After a bulk seed

A fresh state file makes every index look changed. Adopt the state of the
snapshots the seed actually restored from, rather than the latest:

```bash
python3 mirror_refresh.py --init-state \
  --init-snapshot fwd=hourly-snapshot-2026-07-21-12:00-ep92grvj \
  --init-snapshot pop1=hourly-snapshot-2026-07-21-12:00-5fauzhum
```

Pinning matters: fingerprinting the *latest* snapshot instead would mark indices
current that were never restored, and anything that went quiet in between would
stay stale indefinitely.

The `--max-changed` guard (default 250) aborts a run where an implausible number
of indices changed at once — usually a lost or reset state file. Re-run with
`--init-state` to adopt current state, or `--force` to restore anyway.

## Full re-seed

If the mirror must be rebuilt from scratch, restore each venue in one pass with
the same prefix rename:

```bash
curl -sk --netrc-file ~/.netrc -X POST \
  "https://100.104.82.14:9200/_snapshot/snapshot-repo-fwd/<snapshot>/_restore?wait_for_completion=false" \
  -H 'Content-Type: application/json' -d '{
    "indices":"*", "ignore_unavailable":true,
    "include_global_state":false, "include_aliases":false,
    "rename_pattern":"(.+)", "rename_replacement":"fwd_$1",
    "index_settings":{"index.number_of_replicas":0}}'
```

Two settings are required first, and the first one is not obvious:

- **`cluster.routing.allocation.node_initial_primaries_recoveries`** (default
  **4**) is what throttles a snapshot restore — *not*
  `node_concurrent_recoveries`. Raising it to 20 is roughly a 5x speedup.
- **`cluster.max_shards_per_node`** must exceed the combined primary count
  (~2600 for both venues); the default of 1000 strands the restore partway.
  This is set persistently on the mirror and must stay.

Then follow with `--init-state --init-snapshot ...` as above so the first
incremental run does not re-restore everything.

Two `.opensearch-sap-*-config` system indices carry a replica that cannot be
placed on a single node, holding the cluster yellow. Drop it:

```bash
curl -sk --netrc-file ~/.netrc -X PUT \
  "https://100.104.82.14:9200/.opensearch-sap-log-types-config,.opensearch-sap-pre-packaged-rules-config/_settings" \
  -H 'Content-Type: application/json' -d '{"index.number_of_replicas":0}'
```

## Version compatibility

OpenSearch snapshots are readable only by an **equal or newer** version. The
mirror and both OPS venues are 2.9.0 today.

**Any OpenSearch upgrade of an OPS venue is a breaking change for this mirror.**
If OPS moves ahead, the mirror silently stops ingesting — the refresh fails at
change detection with `illegal_state_exception: Can't get text on a START_ARRAY`
and nothing surfaces to users. Upgrade the mirror in lockstep, and monitor mirror
freshness rather than assuming that healthy snapshot *writes* imply healthy
*reads*.
