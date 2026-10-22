# daac_catalog: DAAC-sourced catalog of delivered OPERA products

Rebuilds one document per OPERA granule the SDS delivered to the DAACs, from the DAAC S3 archives rather than from
CMR or GRQ, in the shape of the GRQ dataset documents HySDS writes at product creation. Existing GRQ consumers can
query the `catalog` alias with their usual queries, and the same documents can be restored into `grq_*` indices.

Collections: ASF `OPERA_L2_CSLC-S1_V1`, `OPERA_L2_CSLC-S1-STATIC_V1`, `OPERA_L2_RTC-S1_V1`, `OPERA_L2_RTC-S1-STATIC_V1`,
`OPERA_L3_DISP-S1_V1`, `OPERA_L3_DISP-S1-STATIC_V1`, `OPERA_L3_DIST-ALERT-S1_V1`, `OPERA_L4_TROPO-ZENITH_V1`;
PO.DAAC `OPERA_L3_DSWX-HLS_V1`, `OPERA_L3_DSWX-S1_V1`.

A collection's granules can live under more than one bucket prefix. PO.DAAC publishes the 2023-24 DSWx-HLS v1.0
products (about 3.4 million) in the V1 collection but keeps their files under `OPERA_L3_DSWX-HLS_PROVISIONAL_V1/`, so
the registry lists that prefix too (`extra_prefixes`) and a document's URLs and `catalog.product_prefix` point where
its files are. `OPERA_L3_DSWX-HLS_PROVISIONAL_V0/` holds v0.0 test products CMR never published and is not listed.

## How a document is built

| tier | reads | adds |
|---|---|---|
| 0 | S3 listings of the products bucket, the browse (ASF) or public (PO.DAAC) bucket, and ASF `UMMG/` | identity, `metadata.Files` (same extractor and regexes as HySDS), sizes, DAAC URLs, `daac_*`, natural key |
| 1 | ASF UMM-G sidecar, ISO XML (both DAACs) | versions, time and space, input granules and lineage, per-type fields; DISP-S1 k-set id and acquisition cycle when a burst database is available |
| 2 | DISP-S1 `.nc` (ranged reads of `metadata/pge_runconfig`) | the embedded PGE RunConfig in `catalog.pge_runconfig` and its CSLC input list |

Every document says what it could not reproduce: `catalog.approximated` lists fields carrying a DAAC-side proxy (for
example `creation_timestamp` is the PGE processing time), `catalog.omitted` lists GRQ fields that only the HySDS job
knew (`pcm_version`, accountability, runconfig), and `catalog.derivation` names the source of each proxy.
`catalog.files` keeps the full listing, so a document can be rebuilt at a higher tier without listing again, and
`catalog.files_digest` fingerprints it (bucket, key, size and ETag of every file) so a later listing shows whether
the granule changed at the DAAC.

Known, documented differences from live GRQ documents: the RTC-S1-STATIC `.h5` is never delivered; the TROPO `.nc`
and the DSWx-S1 `_BROWSE.tif` are delivered but missing from GRQ's `metadata.Files`; GRQ's TROPO `model` regex group
holds part of a worker path and its TROPO `pge_version` has an extra `.0`. `metadata.sas_version` comes from the ISO
XML, which the PGE writes from the value it gives GRQ; ASF's UMM-G for CSLC-S1 from PGE 2.1.4 says SAS 0.5.6 where
the product was made with 0.5.7. grq2's dataset API adds `city`, `continent` and `temporal_span` at ingest; bulk
loads (restore mode included) do not go through it, so documents loaded from the catalog do not have them.

`daac_delivery_status: SUCCESS` means the DAAC archive holds the granule. At ASF that also means CMR has it (every
archived granule has its UMM-G record), but PO.DAAC keeps no UMM-G the catalog can read, and its archive also holds
products that never reached CMR (4,873 DSWx-HLS products after "Rate Exceeded" CNM-R errors in 2026). The optional
`cmr` command records CMR's view per granule in `catalog.cmr` (below).

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
`--ids-file` restricts `build`, `index` and `export` to the ids it lists; an empty file selects nothing.

| command | does |
|---|---|
| `template` | installs the `catalog` composable index template (`--restore` also checks the `grq` template exists) |
| `plan` | shards per dataset and request estimates |
| `list` | tier-0 listing into `listing/<dataset>/<shard>.jsonl.gz`, resumable per shard (`--sources products,browse,ummg`; `--bucket-prefixes` lists some of a dataset's prefixes, and such a run is not complete: `reconcile` skips it and `export --from build` writes a delta) |
| `build` | manifests to documents at `--tier 0|1|2` (`--tier1-sources ummg,iso[,md5]`, `--burst-db`, `--parquet`); `--from-reconcile` builds only what this run's `reconcile` found new or changed |
| `index` | bulk-load into `catalog_*` (`--mode catalog`) or `grq_*` (`--mode grq`), from a run or `--bundle <dir|s3>`; op `create` unless `--replace` or a delta bundle; in catalog mode, granules `reconcile` found changed replace their stored documents |
| `enrich` | rebuild stored documents in place (`--tier`, `--since YYYY-MM`, `--until YYYY-MM`; `--all-tiers` also rebuilds documents already at the tier, `--with-problems` selects documents whose build recorded problems); a rebuild never lowers a document's tier |
| `cmr` | record which granules CMR publishes in `catalog.cmr` (`--from store`, or `--from build` to rewrite a run's documents before `export --from build`; `--grace-hours`, `--relist`, `--force`) |
| `export` | write a bundle `--from store` (`--since <time>` for a delta) or `--from build` (a delta when the run listed some of the dataset's bucket prefixes) |
| `push` / `pull` | move a bundle through S3; `pull` verifies every file before anything is loaded |
| `reconcile` | new, changed, deleted and reappeared granules from a newer listing (`reconcile/<dataset>.<kind>_ids.txt`); deletions land in `catalog.deleted_at` and `catalog.lifecycle` |
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
interruption continues where it stopped; a shard that was being written when the run stopped is redone from its
start. A shard built for selected ids only (`--ids-file`, `--from-reconcile`) is rebuilt by a later `build` without a
selection. `--force` redoes finished shards.

Each run directory starts from its own listing, and `build` does not look at the store: without a selection it
builds every listed granule. `index` never overwrites a stored document unless asked (`create`; an existing id is
counted as a conflict), so a full rebuild is safe but costs a full build. To add only what changed since the last
run, use `reconcile` and `build --from-reconcile` (below).

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
python -m tools.ops.daac_catalog build     --dataset all --workdir $W --tier 1 --from-reconcile
python -m tools.ops.daac_catalog index     --dataset all --workdir $W
python -m tools.ops.daac_catalog enrich    --dataset all --workdir $W --with-problems
```

`reconcile` compares the new listing with the store shard by shard: ids not stored are new; stored ids whose files
(bucket, key, size, ETag) differ from `catalog.files_digest` are changed; stored ids missing from a completely
listed shard get `catalog.deleted_at`; deleted ids listed again are reappeared. `build --from-reconcile` builds the
new and changed granules and refuses to run when `reconcile` has not run for the dataset in this run directory.
`index` creates the new documents and replaces the changed ones, keeping `catalog.first_seen` and the lifecycle and
adding a `changed` event (the stored copy is deleted when the rebuilt document lands in another month's index).
A granule that could not be parsed is never stored, so the next `reconcile` lists it as new again. A source read that
failed leaves a lower-tier document with `catalog.problems`, which `enrich --with-problems` retries.

CMR check (PO.DAAC collections; anonymous CMR search, so not the restricted ASF collections):

```bash
python -m tools.ops.daac_catalog cmr --dataset L3_DSWx_S1 --workdir $W --procs 4               # stored documents
python -m tools.ops.daac_catalog cmr --dataset L3_DSWx_HLS --workdir $W --procs 4 --from build  # before export --from build
```
`cmr` lists the collection in CMR one listing shard (granule UR prefix) at a time with the references format
(`granules.xml`: granule UR, concept id, revision id), keeps a shard's listing only when its items add up to CMR-Hits,
and joins it to the shard's documents:

* in CMR: `catalog.cmr = {concept_id, revision_id, checked_at}`, and `daac_catalog_id` / `daac_catalog_url` as the
  CNM-R response gives them;
* not in CMR: `catalog.cmr = {checked_at}` without a concept id;
* not checked: no `catalog.cmr`. A granule missing from CMR whose newest archived file is younger than `--grace-hours`
  (default 24) keeps its previous state.

`checked_at` is when the listing that set the current state started; a document changes, and gets a new
`catalog.updated_at`, only when its CMR state changes, so later checks add only the changes to a delta export. The
CMR listing is kept per shard under `cmr/listing/` in the run directory and reused unless `--relist`. The run writes
`cmr/<dataset>.<store|build>.summary.json` (counts, including granules only CMR has, by product version) and
`cmr/<dataset>.<store|build>.missing.tsv` (each granule not in CMR, and whether CMR has another granule with the same
natural key, that is the same product made at another time). `enrich` and the replacement of changed granules keep
the CMR state.

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

Add the granules of a bucket prefix to a catalog that has a full bundle without them, as a delta made after that
bundle:

```bash
python -m tools.ops.daac_catalog list   --dataset L3_DSWx_HLS --workdir $W --bucket-prefixes OPERA_L3_DSWX-HLS_PROVISIONAL_V1/
python -m tools.ops.daac_catalog build  --dataset L3_DSWx_HLS --workdir $W
python -m tools.ops.daac_catalog export --dataset L3_DSWx_HLS --workdir $W --from build   # kind delta
```

Compare with GRQ where the reference cluster's credentials live elsewhere: dump the reference hits read-only on that
cluster's mozart, copy the JSON-lines file, and run `validate --grq-docs <file> --sample-from run`.

## Querying

The `catalog` alias (and `catalog_<dataset>`) answers GRQ query shapes, for example:

```json
{"query": {"term": {"metadata.frame_id": 16655}}, "sort": [{"creation_timestamp": "desc"}]}
{"size": 0, "aggs": {"dups": {"terms": {"field": "catalog.natural_key", "min_doc_count": 2, "size": 1000}}}}
{"query": {"exists": {"field": "catalog.deleted_at"}}}
{"query": {"term": {"catalog.tier": 0}}}
{"query": {"bool": {"filter": [{"exists": {"field": "catalog.cmr.checked_at"}}],
                    "must_not": [{"exists": {"field": "catalog.cmr.concept_id"}}]}}}
```
The last one finds archived granules CMR did not have at their last check.

## Tests

`tests/unit/tools/ops/daac_catalog/`, including a golden test that rebuilds ten live GRQ documents (one per dataset
type) from DAAC-side fixtures and compares them field by field (`compare.py`).
