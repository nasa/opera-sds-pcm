# OPERA analysis mirror

A single-node OpenSearch instance holding read-only copies of the OPS-FWD,
OPS-POP1 and INT-FWD clusters, so analysts and `opera-monitoring` can query
operational data without touching production.

Data arrives via the hourly snapshots the clusters already write to S3. The
mirror registers those repositories read-only and restores from them; it never
writes to the buckets and never connects to the clusters.

**Before trusting a query, check `mirror_status`** (see
[Freshness and alerts](#freshness-and-alerts)): it says how current each venue's
data is.

## Venue prefixes

**Every index is prefixed with its venue — `fwd_*`, `pop1_*` or `intfwd_*`. No
index keeps its canonical HySDS name.**

OPS-FWD and OPS-POP1 both run a cluster named `common_cluster`, and **162 index
names collide** between them: most of the operational time-series
(`worker_status`, `logstash`, `task_status`, `job_status`, `event_status`) plus
several `grq_` product indices such as `grq_v1.0_l2_rtc_s1`, `grq_1_l1_s1_slc`,
and `grq_v1.1_l2_cslc_s1`. The colliding
`grq_` indices hold genuinely different documents per venue — forward products
versus reprocessed ones — so merging them would be wrong, not merely untidy.

Prefixing makes a venue-less query match nothing instead of silently returning
one venue's data and looking complete:

```
one venue    fwd_job_status-*
all venues   *job_status-*
canonical    job_status-*        <- matches nothing, by design
```

## Layout

| | |
|---|---|
| Mirror | `https://100.104.82.14:9200` (TLS, single node) |
| Repositories | `snapshot-repo-fwd` -> `s3://opera-ops-es-bucket/opera-ops-fwd/cluster`<br>`snapshot-repo-pop1` -> `s3://opera-ops-es-bucket/opera-ops-pop1/cluster`<br>`snapshot-repo-intfwd` -> `s3://opera-int-es-bucket/opera-int-fwd/cluster` (all `readonly: true`) |
| Freshness | `mirror_status` index, one document per venue |
| Credentials | `~/.netrc` — **keyed to the IP, not `localhost`**. `curl --netrc-file` against `https://localhost:9200` silently returns `Unauthorized`. |
| State | `~/.opera_mirror_refresh_state.json` |
| Lock | `~/.opera_mirror_refresh.lock` |

## Incremental refresh

`mirror_refresh.py` brings the mirror up to date. Standard library only — the
mirror host has no mozart/conda environment for analyst accounts.

```bash
./run_mirror_refresh.sh                    # all three venues
./run_mirror_refresh.sh --dry-run          # preview
./run_mirror_refresh.sh --venue fwd=snapshot-repo-fwd   # one venue
```

Cron (the wrapper is `flock`-guarded, so a slow run will not double up):

```
0 */2 * * * $HOME/analysis/run_mirror_refresh.sh --alert-cmd $HOME/analysis/alert.sh >> $HOME/mirror_refresh.log 2>&1
```

### Which snapshots are used

A venue's repository can hold several snapshot series. Clusters on OpenSearch
3.x run **two** snapshot-management (SM) policies, `hourly-snapshot-grq` and
`hourly-snapshot-mozart-metrics`, each writing its own snapshots into the same
repository, so no single snapshot covers the venue. Each run takes the newest
`SUCCESS` snapshot of every SM policy and restores each index from whichever of
those holds it. It ignores:

- snapshots with no SM policy (manual and teardown snapshots), and
- a policy whose newest snapshot lags the venue's newest by more than
  `--series-max-lag-hours` (default 24). A redeployed cluster leaves its old
  policy's last snapshots behind in the repository; using them would bring back
  indices the source has since deleted.

A repository with no SM-policy snapshots at all falls back to its single newest
snapshot.

### How change detection works

Each index is fingerprinted as `file_count:size_in_bytes` from the snapshot
`_status` API's **total** stats, and compared against the fingerprint recorded on
its last successful restore.

Comparing against our own last-restored state — rather than against the previous
snapshot's *incremental* stats — means **skipped refresh cycles are safe**. If
the cron is down for six hours, everything that changed in the meantime is still
detected on the next run. A fingerprint is recorded **only after a successful
restore**, so a failed index is retried on the next run instead of being quietly
adopted as current. An index **missing from the mirror** is always restored,
whatever the state file says, so a wiped or rebuilt mirror re-seeds itself.

Changed indices are deleted locally and restored directly from the snapshot,
which reproduces the snapshot's own mappings exactly. There is no reindex step
and no dependency on registered index templates.

### Retention

**Daily indices** (names ending in `YYYY.MM.DD`: `job_status`, `task_status`,
`event_status`, `worker_status`, `logstash`, ...) are kept for
`--daily-retention-days` (default **30**) and then deleted from the mirror,
whether or not the source still has them. `0` keeps them forever.

This cap is what keeps the mirror under the cluster shard limit. Without it the
mirror kept every daily index the sources ever had: by September 2026 that was
935 dailies holding about 3800 of the node's 5000 shards. Once the limit is
reached, every *new* index fails to restore (`this action would add [N] total
shards, but this cluster currently has [5000]/[5000] maximum shards open`), so a
new month's `grq_*` product indices never appear even though existing ones keep
refreshing.

Every other index that disappears from the source is **kept** — retaining
history the source has dropped is part of the point — and dropped from state
tracking so it stops being reported.

### The `--max-changed` guard

A run where more than `--max-changed` indices (default 250) changed at once is
aborted for that venue. That usually means the state file was lost, the mirror
was wiped, or the source venue was redeployed. Re-run with `--force` to restore
them all, or with `--init-state` to adopt the current state without restoring.

`--init-state` is for after restoring indices by hand. Pin it to the snapshots
the data actually came from, rather than the newest ones:

```bash
python3 mirror_refresh.py --init-state \
  --init-snapshot fwd=<grq snapshot> --init-snapshot fwd=<mozart-metrics snapshot>
```

Fingerprinting newer snapshots instead would mark indices current that were
never restored, and anything that went quiet in between would stay stale
indefinitely. `--init-state` changes nothing on the mirror; expired dailies are
pruned on the next normal run.

## Freshness and alerts

After each run, every venue has a document in the **`mirror_status`** index
(readable with `hysds-ro`):

| field | meaning |
|---|---|
| `data_as_of` | time of the oldest snapshot the venue is fully caught up to |
| `lag_hours` | hours between `data_as_of` and the run |
| `stale` | `lag_hours` over `--stale-hours` (default 6), or never caught up |
| `last_success` / `last_run` / `last_run_ok` | when the last clean run was, and how the latest went |
| `snapshots` | the snapshot used from each series |
| `errors` | the first errors of the latest run |
| `mirror_active_shards` / `mirror_shard_limit` | shard headroom |

```bash
curl -sk --netrc-file ~/.netrc "https://100.104.82.14:9200/mirror_status/_search?pretty"
```

A venue is only marked caught up when a run finishes with **no** failures, so a
partly failing venue (for example, new indices hitting the shard limit) goes
stale instead of looking healthy.

When a venue goes stale, `--alert-cmd` (or `$MIRROR_ALERT_CMD`) is run with the
message on stdin and a one-line subject in `$MIRROR_ALERT_SUBJECT`. It runs again
every `--alert-repeat-hours` (default 24) while the venue stays stale, and once
when it recovers. With no command configured, alerts only go to the log. If the
mirror itself is unreachable, the venues still go stale and alert. A failed
alert command is retried on the next run.

The mirror host has outbound HTTPS, so a Slack incoming webhook is the simplest
target. Keep the URL out of the crontab:

```bash
#!/bin/bash
# ~/analysis/alert.sh -- post the mirror alert to Slack
python3 -c 'import json,sys; print(json.dumps({"text": sys.stdin.read()}))' \
  | curl -sf -X POST -H 'Content-Type: application/json' \
         --data @- "$(cat ~/.mirror_alert_webhook)"
```

`aws sns publish --topic-arn <arn> --subject "$MIRROR_ALERT_SUBJECT" --message file:///dev/stdin`
works too if the instance role is allowed to publish.

## Adding a venue

1. Register the venue's snapshot repository read-only (the instance role needs
   `s3:ListBucket` and `s3:GetObject` on the bucket). For example, INT-POP1:

   ```bash
   curl -sk --netrc-file ~/.netrc -X PUT \
     "https://100.104.82.14:9200/_snapshot/snapshot-repo-intpop1" \
     -H 'Content-Type: application/json' -d '{
       "type": "s3",
       "settings": {"bucket": "opera-int-es-bucket",
                    "base_path": "opera-int-pop1/cluster", "readonly": true}}'
   ```

2. Add it to `DEFAULT_VENUES` in `mirror_refresh.py`, or pass **every** venue
   on the cron line — `--venue` replaces the defaults:

   ```
   ... run_mirror_refresh.sh --venue fwd=snapshot-repo-fwd \
       --venue pop1=snapshot-repo-pop1 --venue intfwd=snapshot-repo-intfwd \
       --venue intpop1=snapshot-repo-intpop1
   ```

3. The first run finds every index of the new venue missing and stops at
   `--max-changed`; run it once by hand with `--force` to seed it.

A venue that is **redeployed with a fresh GRQ** (common on INT) looks the same:
most indices change at once and the guard stops the run. Re-run with `--force`.
Indices the new deployment no longer has stay on the mirror under the venue
prefix; delete them by hand if they are unwanted.

## Full re-seed

On an empty mirror every index is missing, so the refresh itself re-seeds it.
Register the repositories, set the cluster settings below, remove any old state
file, and run once by hand with `--force` to get past the `--max-changed` guard:

```bash
rm -f ~/.opera_mirror_refresh_state.json
~/analysis/run_mirror_refresh.sh --force >> ~/mirror_refresh.log 2>&1
```

Indices restore one at a time and the run takes hours (it is bounded by
`indices.recovery.max_bytes_per_sec`); the cron ticks that fall inside it skip
on the lock. Daily indices past retention are never restored. The November
2026 re-seed of all three venues restored 635 indices (3600 shards, 2.3 TB) in
6.5 hours.

Each venue's data dates from the snapshots chosen when that venue's turn
starts, so the venue restored first is usually reported **stale** when a long
re-seed ends. The next cron run catches it up and clears the alert.

Persistent cluster settings, before the first restore:

```bash
curl -sk --netrc-file ~/.netrc -X PUT "https://100.104.82.14:9200/_cluster/settings" \
  -H 'Content-Type: application/json' -d '{"persistent": {
    "cluster.routing.allocation.node_initial_primaries_recoveries": 10,
    "indices.recovery.max_bytes_per_sec": "200mb",
    "cluster.max_shards_per_node": 5000}}'
```

- **`node_initial_primaries_recoveries`** (default **4**) is what throttles a
  snapshot restore — *not* `node_concurrent_recoveries`.
- **`indices.recovery.max_bytes_per_sec`** (default 40mb) is too slow for the
  largest current-month `grq_` indices (100+ GB) to restore within
  `--index-timeout`.
- **`cluster.max_shards_per_node`** (default 1000) must cover every venue's
  primaries within retention. Each run logs `mirror shards: N/limit`, and
  `mirror_status` records it.

A system index that carries a replica cannot be placed on a single node and
holds the cluster yellow. Set its replicas to 0:

```bash
curl -sk --netrc-file ~/.netrc "https://100.104.82.14:9200/_cat/shards?h=index,prirep,state" | grep UNASSIGNED
curl -sk --netrc-file ~/.netrc -X PUT "https://100.104.82.14:9200/<index>/_settings" \
  -H 'Content-Type: application/json' -d '{"index.number_of_replicas":0}'
```

## Version compatibility

OpenSearch snapshots are readable only by an **equal or newer** version, so the
mirror must run a version at least as new as **every** venue it mirrors.

**Any OpenSearch upgrade of a venue is a breaking change for this mirror.** If a
venue moves ahead, the refresh fails at change detection with
`illegal_state_exception: Can't get text on a START_ARRAY`. This happened when
OPS-FWD (2026-10-08) and OPS-POP1 (2026-11-05) were redeployed on OpenSearch
3.6.0 while the mirror was still on 2.9.0. Upgrade the mirror first; the venue
then goes stale and alerts (see above) instead of failing silently.
