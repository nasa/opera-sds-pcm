# daac_catalog: DAAC-sourced catalog of delivered OPERA products

Rebuilds one document per OPERA granule the SDS delivered to the DAACs, from the DAAC S3 archives rather than from
CMR or GRQ, in the shape of the GRQ dataset documents HySDS writes at product creation. Existing GRQ consumers can
query the `catalog` alias with their usual queries, and the same documents can be restored into `grq_*` indices.

Collections: ASF `OPERA_L2_CSLC-S1_V1`, `OPERA_L2_CSLC-S1-STATIC_V1`, `OPERA_L2_RTC-S1_V1`, `OPERA_L2_RTC-S1-STATIC_V1`,
`OPERA_L3_DISP-S1_V1`, `OPERA_L3_DISP-S1-STATIC_V1`, `OPERA_L3_DIST-ALERT-S1_V1`, `OPERA_L4_TROPO-ZENITH_V1`;
PO.DAAC `OPERA_L3_DSWX-HLS_V1`, `OPERA_L3_DSWX-S1_V1`.

## How a document is built

| tier | reads | adds |
|---|---|---|
| 0 | S3 listings of the products bucket, the browse (ASF) or public (PO.DAAC) bucket, and ASF `UMMG/` | identity, `metadata.Files` (same extractor and regexes as HySDS), sizes, DAAC URLs, `daac_*`, natural key |
| 1 | ASF UMM-G sidecar, ISO XML (both DAACs) | versions, time and space, input granules and lineage, per-type fields; DISP-S1 k-set id and acquisition cycle when a burst database is available |
| 2 | DISP-S1 `.nc` (ranged reads of `metadata/pge_runconfig`) | the embedded PGE RunConfig in `catalog.pge_runconfig` and its CSLC input list |

Every document says what it could not reproduce: `catalog.approximated` lists fields carrying a DAAC-side proxy (for
example `creation_timestamp` is the PGE processing time), `catalog.omitted` lists GRQ fields that only the HySDS job
knew (`pcm_version`, accountability, runconfig), and `catalog.derivation` names the source of each proxy.
`catalog.files` keeps the full listing, so a document can be rebuilt at a higher tier without listing again.

Known, documented differences from live GRQ documents: the RTC-S1-STATIC `.h5` is never delivered; the TROPO `.nc`
and the DSWx-S1 `_BROWSE.tif` are delivered but missing from GRQ's `metadata.Files`; GRQ's TROPO `model` regex group
holds part of a worker path and its TROPO `pge_version` has an extra `.0`.

## Running it

From a PCM checkout with the repo root as the working directory:

```bash
python -m tools.ops.daac_catalog <command> [options]       # on a mozart: ~/mozart/bin/python3 -m ...
```

Credentials:

* ASF products and `UMMG/` are listed and read with the host's instance role (`--creds auto`, the default).
* The ASF browse bucket and both PO.DAAC buckets need DAAC `s3credentials`, fetched with the Earthdata Login entry in
  `~/.netrc` (`machine urs.earthdata.nasa.gov`). The one-hour keys refresh automatically. They only work in us-west-2.
* OpenSearch: `--es-url https://localhost:9200 --netrc-os ~/.netrc-os` (the defaults).

Common options: `--dataset <name|a,b|all>`, `--workdir <dir>` (default `/data/daac_catalog` when writable, else
`~/daac_catalog`, or `$DAAC_CATALOG_WORKDIR`), `--run-id inv-YYYYMMDDTHHMMZ` (default: newest run, new for `list`),
`--workers N`, `--shard-filter <glob>`, `--ids-file <file>`, `--limit N`, `--index-prefix <prefix>`, `--dry-run`.

| command | does |
|---|---|
| `template` | installs the `catalog` composable index template (`--restore` also checks the `grq` template exists) |
| `plan` | shards per dataset and request estimates |
| `list` | tier-0 listing into `listing/<dataset>/<shard>.jsonl.gz`, resumable per shard (`--sources products,browse,ummg`) |
| `build` | manifests to documents at `--tier 0|1|2` (`--tier1-sources ummg,iso[,md5]`, `--burst-db`, `--parquet`) |
| `index` | bulk-load into `catalog_*` (`--mode catalog`) or `grq_*` (`--mode grq`), from a run or `--bundle <dir|s3>`; op `create` unless `--replace` or a delta bundle |
| `enrich` | rebuild stored documents at a higher tier in place (`--tier`, `--since YYYY-MM`, `--until YYYY-MM`) |
| `export` | write a bundle `--from store` (`--since <time>` for a delta) or `--from build` |
| `push` / `pull` | move a bundle through S3; `pull` verifies every file before anything is loaded |
| `reconcile` | new, deleted and reappeared granules from a newer listing; deletions land in `catalog.deleted_at` and `catalog.lifecycle` |
| `validate` | `--counts`, `--against-grq <url>` or `--grq-docs <dump>` field comparison, `--query-pack`, `--bundle` counts |
| `report` | per-run summary, `--store-sizes` for bytes per document |

## Workflows

First build of a collection:

```bash
W=/data/daac_catalog; D=L3_DISP_S1_STATIC
python -m tools.ops.daac_catalog template
python -m tools.ops.daac_catalog list     --dataset $D --workdir $W            # prints the run id
python -m tools.ops.daac_catalog build    --dataset $D --workdir $W --tier 1 --workers 64 --parquet
python -m tools.ops.daac_catalog index    --dataset $D --workdir $W
python -m tools.ops.daac_catalog validate --dataset $D --workdir $W --counts --query-pack
python -m tools.ops.daac_catalog report   --workdir $W --store-sizes
```
`list`, `build` and `index` write a `_DONE` marker per shard and skip finished shards, so rerunning after an
interruption continues where it stopped.

Large collections (RTC-S1, DSWx-HLS): load tier 0 first, then enrich month by month, newest first:

```bash
python -m tools.ops.daac_catalog build  --dataset L2_RTC_S1 --workdir $W --tier 0
python -m tools.ops.daac_catalog index  --dataset L2_RTC_S1 --workdir $W --bulk-size 2000 --concurrency 3
python -m tools.ops.daac_catalog enrich --dataset L2_RTC_S1 --tier 1 --since 2026-09 --workers 64
```

Weekly maintenance:

```bash
python -m tools.ops.daac_catalog list      --dataset all --workdir $W
python -m tools.ops.daac_catalog reconcile --dataset all --workdir $W
python -m tools.ops.daac_catalog build     --dataset all --workdir $W --tier 1 --ids-file $W/<run>/reconcile/<dataset>.new_ids.txt
python -m tools.ops.daac_catalog index     --dataset all --workdir $W
```

Restore delivered history into GRQ on a new cluster generation:

```bash
python -m tools.ops.daac_catalog index --mode grq --dataset L3_DISP_S1 --workdir $W   # op create: existing products untouched
```

Move a catalog to another cluster without OpenSearch snapshots:

```bash
# source host
python -m tools.ops.daac_catalog export --dataset L3_DISP_S1 --from store --workdir $W   # -> $W/bundles/<bundle-id>
python -m tools.ops.daac_catalog push   --bundle $W/bundles/<bundle-id> --to s3://<bucket>/daac_catalog/bundles
# destination host
python -m tools.ops.daac_catalog template
python -m tools.ops.daac_catalog index    --bundle s3://<bucket>/daac_catalog/bundles/L3_DISP_S1/<bundle-id>
python -m tools.ops.daac_catalog validate --bundle s3://<bucket>/daac_catalog/bundles/L3_DISP_S1/<bundle-id>
```
`export --since <time>` writes a delta of documents changed since then (`catalog.updated_at`), which `index --bundle`
applies with overwrite semantics. Without a shared bucket, copy the bundle directory with `rsync` or `tar`; it is
plain files.

Compare with GRQ where the reference cluster's credentials live elsewhere: dump the reference hits read-only on that
cluster's mozart, copy the JSON-lines file, and run `validate --grq-docs <file> --sample-from run`.

## Querying

The `catalog` alias (and `catalog_<dataset>`) answers GRQ query shapes, for example:

```json
{"query": {"term": {"metadata.frame_id": 16655}}, "sort": [{"creation_timestamp": "desc"}]}
{"size": 0, "aggs": {"dups": {"terms": {"field": "catalog.natural_key", "min_doc_count": 2, "size": 1000}}}}
{"query": {"exists": {"field": "catalog.deleted_at"}}}
{"query": {"term": {"catalog.tier": 0}}}
```

## Tests

`tests/unit/tools/ops/daac_catalog/`, including a golden test that rebuilds ten live GRQ documents (one per dataset
type) from DAAC-side fixtures and compares them field by field (`compare.py`).
