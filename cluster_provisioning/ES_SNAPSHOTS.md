# OpenSearch snapshots: what is preserved, and how to get it back

Every cluster registers its snapshot repositories and lifecycle policies during provisioning
(`modules/common/mozart.tf`, the "Snapshot repositories and lifecycles" provisioner). This
document covers what those policies capture, how to restore from them, and the procedures that
provisioning cannot perform for you: retrofitting a running cluster, hardening the bucket, and
promoting across an OpenSearch major version.

Commands below run from a cluster's mozart as `hysdsops`. OpenSearch is authenticated, so REST
calls need `curl -k --netrc-file ~/.netrc-os` and the tool needs no extra flags. The GRQ endpoint
for a combined cluster is the GRQ instance's private IP on port 9200; for a three-cluster
deployment, take each endpoint from `~/.sds/config` (`GRQ_ES_PVT_IP`, `MOZART_ES_PVT_IP`,
`METRICS_ES_PVT_IP`).

## What gets snapshotted

Retention differs by what the data costs to rebuild. Product and catalog indices - call them the
preserved set - are snapshotted with **no deletion block**, so Snapshot Management never trims
them. Job, worker and log indices keep the rolling 60-day window they have always had.

The preserved set is defined by **exclusion**, not by an allowlist, so an index family introduced
later is captured by default instead of being silently missed:

    *,-.*,-jobs_accountability_catalog*

`-.*` drops system and hidden indices. `jobs_accountability_catalog*` is excluded deliberately:
the project does not preserve it, and on a busy cluster it is the largest thing in the archive.
If someone proposes simplifying the pattern to `*,-.*`, that exclusion is the reason it is there.

### Combined cluster (`es_cluster_mode = true`, the default)

One repository, `snapshot-repo`, at `s3://<es_snapshot_bucket>/<project>-<venue>-<counter>/cluster`,
holding two policies:

| policy | schedule (UTC) | indices | deletion |
|---|---|---|---|
| `hourly-snapshot-grq` | `0 * * * *` | preserved set (above) | none - kept until deleted deliberately |
| `hourly-snapshot-mozart-metrics` | `30 * * * *` | `*_status-*,user_rules-*,job_specs,hysds_ios-*,containers,logstash-*,sdswatch-*,mozart-logs-*,factotum-logs-*,grq-logs-*` | 60d / min 10 / max 60 |

The half-hour stagger keeps two policies on one repository off the same minute.

Neither policy sets a `time_limit`, which matches OpenSearch's own default of no limit. A limit
does not cancel a snapshot that overruns it: Snapshot Management marks the run
`TIME_LIMIT_EXCEEDED` and resets its workflow while the snapshot carries on in the cluster, so a
limit shorter than a snapshot really takes reports failures for work that is succeeding and lets
the next scheduled run start a second snapshot over the top of the first. The first snapshot of a
large cluster is a full copy and can run for hours. `--time-limit` and `--delete-time-limit` are
there if a cluster ever wants a bound.

### Three-cluster deployment (`es_cluster_mode = false`)

Three repositories (`grq-snapshot-repo`, `mozart-snapshot-repo`, `metrics-snapshot-repo`) under
`.../<project>-<venue>-<counter>/{grq,mozart,metrics}`, each with a `daily-snapshot` policy at
`0 5 * * *` UTC. The GRQ policy carries the preserved set and no deletion block; mozart and
metrics keep their existing patterns and rolling deletion.

Schedules are **5-field UNIX cron**. Snapshot Management stores only the leading five fields, so a
6-field Quartz expression loses its last one: `0 0 5 * * ?`, intended as 5 AM daily, is stored as
`0 0 5 * *` and fires monthly on the 5th.

### Paths and redeploys

`base_path` is `<project>-<venue>-<counter>/...`. `counter` is the discriminator between clusters
inside one venue (ops runs `fwd` and `pop1` concurrently), so it is load-bearing - dropping it
would collide two clusters' archives. Redeploying the same venue and counter lands on the same
path, re-registers the repository, and exposes every prior snapshot.

Repositories are registered with `shard_path_type: FIXED`, which keeps shard blobs under
`base_path`. OpenSearch 3.x otherwise writes them to a hashed prefix at the **root** of the
bucket, where prefix-scoped cleanup and archival miss them entirely. A repository that already
existed on a 3.x cluster before that setting was introduced keeps writing hashed prefixes for
indices it has already snapshotted; only a new repository (or a redeploy) is clean.

## Inspecting the archive

```bash
# policies currently installed
curl -k --netrc-file ~/.netrc-os "https://<GRQ>:9200/_plugins/_sm/policies?pretty"

# snapshots in a repository, newest successful one, and what an individual snapshot holds
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    display-snapshots --repository snapshot-repo
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    newest-snapshot --repository snapshot-repo
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    snapshot-indices --repository snapshot-repo --snapshot <name>
```

`snapshot-indices` is the check that a pattern change did what you expected: the date-rolled
catalogs (`*_catalog-YYYY.MM`) should appear in a preserved-set snapshot, and
`jobs_accountability_catalog*` should not.

The point of selecting by exclusion is that this check keeps passing for families nobody thought
to list. Measured on the ops clusters in August 2026, the allowlist it replaces was already
capturing all but one or two indices per cluster - `job_failed` on both, and
`parked_ccslc_orphans_*` on pop1 - so the immediate gain is small and the real value is that the
next index family to appear is captured without anyone editing a pattern.

## Taking a snapshot by hand

Before maintenance or a teardown, take one explicitly rather than waiting for the schedule. Name
it so it is distinguishable from policy-generated snapshots:

```bash
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    create-snapshot --repository snapshot-repo \
    --snapshot cluster-$(date -u +%Y.%m.%d%H%M%S)-manual \
    --index-pattern "*,-.*,-jobs_accountability_catalog*" --wait
```

`--wait` blocks until it completes; check the result before proceeding with anything destructive.

## Restoring

### Selected indices, into a live cluster

Restore under new names so nothing in use is clobbered. No index has to be closed for this:

```bash
curl -k --netrc-file ~/.netrc-os -XPOST \
  "https://<GRQ>:9200/_snapshot/snapshot-repo/<snapshot>/_restore" \
  -H 'Content-Type: application/json' -d '{
    "indices": "grq_*,-.*",
    "include_global_state": false,
    "rename_pattern": "(.+)",
    "rename_replacement": "restored_$1"}'
```

### A whole cluster, from another cluster's archive

Register a **separate** repository pointing at the source cluster's `base_path` - never re-register
the running cluster's own write repository there, or two clusters end up writing one archive.
Then close the target indices, restore, and reopen:

```bash
# 1. read-only repo at the SOURCE path. Registered by hand rather than with create-repository,
#    because "readonly" is what keeps this cluster from writing into another cluster's archive
#    and the tool does not expose it.
curl -k --netrc-file ~/.netrc-os -XPUT "https://<GRQ>:9200/_snapshot/restore-repo" \
  -H 'Content-Type: application/json' -d '{"type":"s3","settings":{
    "bucket":"<es_snapshot_bucket>","base_path":"<project>-<source-venue>-<source-counter>/cluster",
    "region":"us-west-2","role_arn":"<es_bucket_role_arn>","shard_path_type":"FIXED","readonly":true}}'

# 2. pick the newest successful snapshot
SNAP=$(~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
         newest-snapshot --repository restore-repo)

# 3. close -> restore -> reopen
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    close-indices --index-pattern "*,-.*"
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    restore --repository restore-repo --snapshot $SNAP
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    open-indices --index-pattern "*,-.*"
```

Validate before declaring success: document counts on the indices that matter, `_cat/indices`
green, and the `grq` alias present.

## Re-enabling rolling deletion

Reverting to a trimmed archive for the preserved set is one policy update. Snapshot Management
requires the sequence number and primary term from a read:

```bash
curl -k --netrc-file ~/.netrc-os \
  "https://<GRQ>:9200/_plugins/_sm/policies/hourly-snapshot-grq?pretty"   # note _seq_no, _primary_term

curl -k --netrc-file ~/.netrc-os -XPUT \
  "https://<GRQ>:9200/_plugins/_sm/policies/hourly-snapshot-grq?if_seq_no=<N>&if_primary_term=<M>" \
  -H 'Content-Type: application/json' -d '<the policy body, plus a deletion block>'
```

Equivalently, re-run the provisioning command without `--no-deletion`. The deletion block looks
like the one `hourly-snapshot-mozart-metrics` carries.

To remove specific snapshots instead, delete them through the API, which is scriptable by date:

```bash
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    delete-snapshot --repository snapshot-repo --snapshot <name>
```

**Never expire snapshot objects with an S3 lifecycle rule.** Segment files are shared across
snapshots; deleting them from the S3 side corrupts the repository for every snapshot that
referenced them. Deletion goes through the `_snapshot` API only. Transitions between Standard and
Standard-IA are safe; Glacier and Deep Archive are not, because a restore reads the objects
directly.

## Teardown behavior

`terraform destroy` runs `null_resource.destroy_es_snapshots`, which branches on
`es_snapshot_destroy_action`:

| value | behavior | venues |
|---|---|---|
| `create-new` | takes a final teardown snapshot of the preserved set, leaves the archive | ops, pst |
| `leave` | does nothing | dev-e2e |
| `purge` | deletes the venue's S3 prefix and the GRQ repository | dev, modules default |

`purge` refuses to run for a venue named `ops*`, on top of the per-venue default, because that
archive is not reproducible. The final pre-teardown snapshot for a production cluster should still
be taken and verified by hand - see "Taking a snapshot by hand" - rather than trusted to a
destroy-time provisioner.

## Retrofitting a running cluster

Provisioning changes only reach clusters built after them. To move a running cluster onto the
current policies, install them directly. Delete the old policy first - the ids differ, so leaving
it in place would keep its deletion schedule trimming the archive:

```bash
# archive the existing policy for the record, then remove it
curl -k --netrc-file ~/.netrc-os "https://<GRQ>:9200/_plugins/_sm/policies/hourly-snapshot?pretty" \
  > ~/hourly-snapshot.before.json
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    delete-lifecycle --policy-id hourly-snapshot

# install the current pair (same commands mozart.tf runs)
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    create-lifecycle --repository snapshot-repo --policy-id hourly-snapshot-grq \
    --snapshot common-cluster-grq-backup \
    --index-pattern "*,-.*,-jobs_accountability_catalog*,-*_status-*,-user_rules-*,-job_specs,-hysds_ios-*,-containers,-logstash-*,-sdswatch-*,-mozart-logs-*,-factotum-logs-*,-grq-logs-*" \
    --schedule="0 * * * *" --no-deletion --time-limit 2h
~/mozart/bin/snapshot_es_data.py --engine opensearch --es-url https://<GRQ>:9200 \
    create-lifecycle --repository snapshot-repo --policy-id hourly-snapshot-mozart-metrics \
    --snapshot common-cluster-mozart-metrics-backup \
    --index-pattern "*_status-*,user_rules-*,job_specs,hysds_ios-*,containers,logstash-*,sdswatch-*,mozart-logs-*,factotum-logs-*,grq-logs-*" \
    --schedule="30 * * * *"
```

For a three-cluster deployment, do the same per cluster against its own endpoint and repository,
replacing `daily-snapshot` on each.

Two caveats. The cluster's `~/mozart/bin/snapshot_es_data.py` is whatever was installed at deploy
time; if it predates `--no-deletion`, either update the checkout or `PUT` the policy body directly
(read one back from a cluster that has it, or from the provisioning command above). And if a policy
installed before this change carries `creation.time_limit` or `deletion.time_limit`, drop those
keys while you are updating it.

A retrofit does not trigger a full snapshot. The repository is the same one, and the indices the
new pattern selects have almost all been snapshotted into it already, so the first run afterwards
is incremental like any other. Measured on both ops clusters in August 2026, the exclusion pattern
selected within one or two indices of what the old allowlist did. A full copy happens on a **new**
repository - a fresh venue, or the new cluster an OpenSearch major upgrade brings - and that is the
case to leave unbounded, since it is the whole cluster rather than an hour of change.

## Bucket hardening

The snapshot buckets are not managed by this repository's Terraform, so these are one-time actions
on the account. Worth doing for `opera-ops-es-bucket`:

```bash
aws s3api put-bucket-versioning --bucket opera-ops-es-bucket \
    --versioning-configuration Status=Enabled

aws s3api put-bucket-lifecycle-configuration --bucket opera-ops-es-bucket \
    --lifecycle-configuration '{"Rules":[
      {"ID":"noncurrent-expiry","Status":"Enabled","Filter":{},
       "NoncurrentVersionExpiration":{"NoncurrentDays":30}},
      {"ID":"ia-transition","Status":"Enabled","Filter":{},
       "Transitions":[{"Days":90,"StorageClass":"STANDARD_IA"}]}]}'
```

Versioning makes an accidental object deletion recoverable; expiring noncurrent versions after 30
days keeps that from growing without bound. The transition rule must not be extended to Glacier,
and no rule may expire current versions - see the warning above.

## Promoting across an OpenSearch major version

Each venue gets a new cluster, and the old cluster's snapshots restore into it. OpenSearch 3.x
refuses to restore an index whose **creation** version is Elasticsearch 7.x-era
(`Version id ... must contain OpenSearch mask`) - this keys on where the index was originally
created, not on the version of the cluster that snapshotted it. Indices created on OpenSearch 2.x
restore into 3.x cleanly.

Audit before promoting:

```bash
curl -sk --netrc-file ~/.netrc-os \
  "https://<GRQ>:9200/*/_settings?human=true&filter_path=**.version.created_string"
```

`human=true` is required; without it only the numeric `version.created` is returned. Anything
reporting a 7.x creation version needs `pcm_commons/tools/migrate_opensearch_2_to_3.py`, which
classifies each index, moves the legacy ones by scroll and bulk, and writes the rest to an export
file for a fast parallel restore via `snapshot_es_data.py restore --indices-file`. Its companion
document, `OPENSEARCH_MIGRATION_DATA_INTEGRITY.md`, covers what that preserves.
