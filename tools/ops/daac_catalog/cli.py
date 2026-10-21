"""Command line: `python -m tools.ops.daac_catalog <command> [options]` (see README.md)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from tools.ops.daac_catalog import __version__

PACKAGE_LOGGER = "tools.ops.daac_catalog"
logger = logging.getLogger(PACKAGE_LOGGER + ".cli")

# Rough collection sizes (folders or CMR granules, 2026-10-20) and objects per granule across all sources, used
# only by `plan` to estimate request counts.
SCALE = {
    "L2_CSLC_S1": (8_700_000, 11), "L2_CSLC_S1_STATIC": (44_000, 5), "L2_RTC_S1": (76_600_000, 17),
    "L2_RTC_S1_STATIC": (371_000, 21), "L3_DISP_S1": (206_000, 12), "L3_DISP_S1_STATIC": (1_400, 15),
    "L3_DIST_S1": (239_000, 25), "L4_TROPO": (15_800, 11), "L3_DSWx_HLS": (23_000_000, 24),
    "L3_DSWx_S1": (5_200_000, 15),
}


def default_workdir() -> Path:
    if os.environ.get("DAAC_CATALOG_WORKDIR"):
        return Path(os.environ["DAAC_CATALOG_WORKDIR"])
    return Path("/data/daac_catalog") if Path("/data").is_dir() and os.access("/data", os.W_OK) \
        else Path.home() / "daac_catalog"


def setup_logging(level: str, log_file: Path | None):
    package = logging.getLogger(PACKAGE_LOGGER)
    package.setLevel(level.upper())
    package.propagate = False
    for handler in list(package.handlers):
        package.removeHandler(handler)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(fmt)
    package.addHandler(console)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(fmt)
        package.addHandler(handler)


def emit(value):
    print(json.dumps(value, indent=1, default=str))


# ---- argument parsing -------------------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dataset", default=None, help="dataset name, comma list, or 'all'")
    common.add_argument("--workdir", type=Path, default=None, help="run directories root")
    common.add_argument("--run-id", default=None, help="inv-YYYYMMDDTHHMMZ (default: newest; new for list)")
    common.add_argument("--workers", type=int, default=32, help="threads in total, split across --procs")
    common.add_argument("--procs", type=int, default=1, help="worker processes (list, build, index)")
    common.add_argument("--creds", choices=["auto", "role", "daac"], default="auto")
    common.add_argument("--aws-profile", default=None)
    common.add_argument("--es-url", default="https://localhost:9200")
    common.add_argument("--netrc-os", default="~/.netrc-os")
    common.add_argument("--index-prefix", default=None, help="index family prefix (default catalog_ or grq_)")
    common.add_argument("--shard-filter", default=None, help="glob over shard names, e.g. 'T11*'")
    common.add_argument("--ids-file", type=Path, default=None, help="only these granule ids (an empty file: none)")
    common.add_argument("--limit", type=int, default=None)
    common.add_argument("--dry-run", action="store_true")
    common.add_argument("--log-level", default="INFO")

    parser = argparse.ArgumentParser(prog="python -m tools.ops.daac_catalog",
                                     description=f"DAAC-sourced OPERA product catalog ({__version__})")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("template", parents=[common], help="install the catalog index template")
    p.add_argument("--restore", action="store_true", help="also check the grq template restore mode relies on")

    sub.add_parser("plan", parents=[common], help="show listing shards and request estimates")

    p = sub.add_parser("list", parents=[common], help="tier-0 listing of the DAAC buckets")
    p.add_argument("--sources", default=None, help="comma list of products,browse,ummg (default: all)")
    p.add_argument("--force", action="store_true", help="re-list shards already done")

    p = sub.add_parser("build", parents=[common], help="manifests to core documents")
    p.add_argument("--tier", type=int, choices=[0, 1, 2], default=1)
    p.add_argument("--tier1-sources", default="ummg,iso", help="comma list of ummg,iso,md5")
    p.add_argument("--burst-db", default="settings",
                   help="DISP-S1 consistent burst DB (path, s3:// URL, 'settings' or 'none')")
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--from-reconcile", action="store_true",
                   help="only the granules this run's reconcile found new or changed")

    p = sub.add_parser("index", parents=[common], help="bulk-load documents into OpenSearch")
    p.add_argument("--mode", choices=["catalog", "grq"], default="catalog")
    p.add_argument("--bundle", default=None, help="load from a bundle directory or s3:// prefix")
    p.add_argument("--replace", action="store_true", help="overwrite existing ids")
    p.add_argument("--allow-schema-mismatch", action="store_true")
    p.add_argument("--bulk-size", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=3)
    p.add_argument("--force", action="store_true", help="re-index shards already indexed")

    p = sub.add_parser("enrich", parents=[common], help="upgrade stored documents to a higher tier")
    p.add_argument("--tier", type=int, choices=[1, 2], default=1)
    p.add_argument("--tier1-sources", default="ummg,iso")
    p.add_argument("--burst-db", default="settings")
    p.add_argument("--since", default=None, help="YYYY-MM, creation month lower bound")
    p.add_argument("--until", default=None, help="YYYY-MM, creation month upper bound (exclusive)")
    p.add_argument("--all-tiers", action="store_true", help="also rebuild documents already at the tier")
    p.add_argument("--with-problems", action="store_true",
                   help="rebuild documents whose build recorded problems, at any tier")

    p = sub.add_parser("export", parents=[common], help="write a bundle")
    p.add_argument("--from", dest="source", choices=["store", "build"], default="store")
    p.add_argument("--since", default=None, help="delta: documents with catalog.updated_at >= this time")
    p.add_argument("--with-listing", action="store_true")
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--slices", type=int, default=1)
    p.add_argument("--out", type=Path, default=None, help="bundle directory (default <workdir>/bundles/<id>)")

    p = sub.add_parser("push", parents=[common], help="upload a bundle to S3")
    p.add_argument("--bundle", type=Path, required=True)
    p.add_argument("--to", required=True, help="s3://bucket/prefix")

    p = sub.add_parser("pull", parents=[common], help="download and verify a bundle from S3")
    p.add_argument("--from", dest="source", required=True, help="s3://bucket/prefix/<dataset>/<bundle-id>")

    p = sub.add_parser("reconcile", parents=[common], help="new, deleted and reappeared granules")
    p.add_argument("--touch-last-seen", action="store_true")
    p.add_argument("--no-apply", action="store_true", help="report only, do not update the store")

    p = sub.add_parser("validate", parents=[common], help="counts, GRQ comparison, query pack")
    p.add_argument("--counts", action="store_true")
    p.add_argument("--bundle", default=None)
    p.add_argument("--against-grq", default=None, help="URL of the OpenSearch holding the reference GRQ")
    p.add_argument("--grq-netrc", default="~/.netrc-os")
    p.add_argument("--grq-pattern", default=None)
    p.add_argument("--grq-docs", type=Path, default=None,
                   help="JSON-lines dump (file or directory) of reference GRQ hits, instead of --against-grq")
    p.add_argument("--sample", type=int, default=1000)
    p.add_argument("--sample-from", choices=["store", "run"], default="store")
    p.add_argument("--query-pack", action="store_true")
    p.add_argument("--query-target", action="append", default=None, help="label=index; repeatable")

    p = sub.add_parser("report", parents=[common], help="summarize a run")
    p.add_argument("--store-sizes", action="store_true")
    return parser


# ---- helpers ----------------------------------------------------------------------------------------------------

def _workdir(args) -> Path:
    return (args.workdir or default_workdir()).expanduser()


def _specs(args, required=True):
    from tools.ops.daac_catalog.registry import resolve_datasets
    if not args.dataset:
        if required:
            raise SystemExit("--dataset is required (a name, a comma list, or 'all')")
        return []
    return resolve_datasets(args.dataset)


def _run(args, create: bool = False):
    from tools.ops.daac_catalog.runs import RunDir, latest_run_id, new_run_id
    workdir = _workdir(args)
    run_id = args.run_id or (new_run_id() if create else latest_run_id(workdir))
    if not run_id:
        raise SystemExit(f"no run directory under {workdir}; run `list` first or pass --run-id")
    return RunDir(workdir, run_id)


def _client(args, url=None, netrc_path=None):
    from tools.ops.daac_catalog.store import make_client
    return make_client(url or args.es_url, netrc_path or args.netrc_os)


def _clients(args):
    from tools.ops.daac_catalog.creds import S3Clients
    return S3Clients(args.creds, max_pool_connections=max(10, args.workers * 2), profile=args.aws_profile)


def _plain_s3(args):
    import boto3
    from tools.ops.daac_catalog.creds import client_config
    session = boto3.Session(profile_name=args.aws_profile) if args.aws_profile else boto3.Session()
    return session.client("s3", config=client_config())


def _ids(args):
    from tools.ops.daac_catalog.runs import read_ids_file
    return read_ids_file(args.ids_file)


def _reconciled_ids(run, dataset: str) -> set[str] | None:
    """New and changed ids from this run's reconcile, or None when reconcile has not run for the dataset."""
    from tools.ops.daac_catalog.runs import read_ids_file
    if not run.reconcile_summary(dataset).exists():
        return None
    ids = set()
    for kind in ("new", "changed"):
        path = run.reconcile_ids(dataset, kind)
        if path.exists():
            ids |= read_ids_file(path)
    return ids


def _frame_to_bursts(args, spec, workdir: Path):
    from tools.ops.daac_catalog.registry import Dataset
    if spec.dataset != Dataset.L3_DISP_S1 or args.burst_db in (None, "none"):
        return None
    from tools.ops.daac_catalog.tier2 import fetch_burst_db, load_burst_db
    source = args.burst_db
    if source == "settings":
        from util.conf_util import SettingsConf
        source = SettingsConf().cfg["DISP_S1_BURST_DB_S3PATH"]
    try:
        path = fetch_burst_db(source, workdir / "cache", _plain_s3(args) if source.startswith("s3://") else None)
        return load_burst_db(str(path))
    except Exception as err:
        logger.warning(f"DISP-S1 burst DB unavailable ({err}); acquisition_cycle and k-set ids will be omitted")
        return None


def _fetch_options(args, tier: int):
    from tools.ops.daac_catalog.fetch import FetchOptions
    chosen = {s.strip() for s in (args.tier1_sources or "").split(",") if s.strip()}
    return FetchOptions(ummg="ummg" in chosen, iso="iso" in chosen, md5="md5" in chosen, tier2=tier >= 2)


# ---- commands ---------------------------------------------------------------------------------------------------

def cmd_template(args):
    from tools.ops.daac_catalog import store
    client = _client(args)
    if args.dry_run:
        emit(store.load_template())
        return 0
    result = {"catalog_template": store.install_template(client)}
    if args.restore:
        result["grq_template_present"] = store.check_restore_template(client)
    emit(result)
    return 0


def cmd_plan(args):
    from tools.ops.daac_catalog.listing import select_shards
    from tools.ops.daac_catalog.runs import shard_name
    rows = []
    for spec in _specs(args):
        shards = select_shards(spec, args.shard_filter)
        granules, objects = SCALE.get(spec.dataset.value, (0, 10))
        if args.shard_filter:
            granules = granules * len(shards) // max(1, len(spec.shards()))
        list_requests = granules * objects // 1000 + len(shards) * len(spec.sources())
        tier1_gets = granules * (2 if spec.ummg_prefix else 1)
        rows.append({"dataset": spec.dataset.value, "daac": spec.daac.value, "shards": len(shards),
                     "first_shards": [shard_name(spec, s) for s in shards[:3]], "est_granules": granules,
                     "est_list_requests": list_requests, "est_tier1_gets": tier1_gets,
                     "est_list_minutes_at_32_workers": round(list_requests * 0.15 / 32 / 60, 1)})
    emit(rows)
    return 0


def cmd_list(args):
    from tools.ops.daac_catalog.listing import default_sources, run_listing, select_shards
    run = _run(args, create=True)
    setup_logging(args.log_level, run.logs() / f"list-{args.dataset}.log")
    clients = _clients(args)
    results = []
    for spec in _specs(args):
        sources = default_sources(spec, args.sources.split(",") if args.sources else None)
        if args.dry_run:
            results.append({"dataset": spec.dataset.value, "shards": len(select_shards(spec, args.shard_filter)),
                            "sources": [s.value for s in sources],
                            "uses_daac_credentials": [s.value for s in sources
                                                      if clients.uses_daac_credentials(spec, s)]})
            continue
        results.append(run_listing(spec, run, clients, sources, workers=args.workers,
                                   shard_filter=args.shard_filter, limit=args.limit, force=args.force,
                                   procs=args.procs))
    emit({"run_id": run.run_id, "workdir": str(run.workdir), "results": results})
    return 0 if all(not r.get("failed") for r in results) else 1


def cmd_build(args):
    from tools.ops.daac_catalog.build import BuildContext
    from tools.ops.daac_catalog.fetch import SourceFetcher
    from tools.ops.daac_catalog.stages import BuildOptions, run_build
    if args.from_reconcile and args.ids_file:
        raise SystemExit("--from-reconcile and --ids-file select granules differently; pass one")
    run = _run(args)
    setup_logging(args.log_level, run.logs() / f"build-{args.dataset}.log")
    results = []
    for spec in _specs(args):
        ids = _ids(args)
        if args.from_reconcile:
            ids = _reconciled_ids(run, spec.dataset.value)
            if ids is None:
                results.append({"dataset": spec.dataset.value, "error": f"no reconcile output in {run.run_id}"})
                continue
        fetcher = SourceFetcher(_clients(args), _fetch_options(args, args.tier)) if args.tier >= 1 else None
        ctx = BuildContext(run_id=run.run_id, listed_at=run.listed_at,
                           frame_to_bursts=_frame_to_bursts(args, spec, run.workdir) if args.tier >= 1 else None)
        options = BuildOptions(tier=args.tier, workers=args.workers, procs=args.procs, shard_filter=args.shard_filter,
                               ids=ids, parquet=args.parquet, force=args.force)
        if args.dry_run:
            results.append({"dataset": spec.dataset.value, "listing_shards": len(run.listing_shards(
                spec.dataset.value)), "tier": args.tier})
            continue
        results.append(run_build(spec, run, fetcher, ctx, options))
    emit({"run_id": run.run_id, "results": results})
    return 0 if all(not r.get("failed") and not r.get("error") for r in results) else 1


def cmd_index(args):
    from tools.ops.daac_catalog import store
    if args.bundle:
        return _index_bundle(args)
    from tools.ops.daac_catalog.stages import IndexOptions, run_index
    run = _run(args)
    setup_logging(args.log_level, run.logs() / f"index-{args.dataset}.log")
    client = _client(args)
    results = []
    for spec in _specs(args):
        options = IndexOptions(mode=args.mode, prefix=args.index_prefix, replace=args.replace,
                               chunk_size=args.bulk_size, threads=args.concurrency, procs=args.procs,
                               shard_filter=args.shard_filter, ids=_ids(args), force=args.force)
        if args.dry_run:
            results.append({"dataset": spec.dataset.value, "prefix": store.index_prefix(args.mode, args.index_prefix),
                            "docs_shards": len(run.docs_shards(spec.dataset.value))})
            continue
        results.append(run_index(spec, run, client, options, client_factory=lambda: _client(args)))
    emit({"run_id": run.run_id, "results": results})
    return 0 if all(not r.get("errors") for r in results) else 1


def _index_bundle(args):
    from tools.ops.daac_catalog import store
    from tools.ops.daac_catalog.bundle import BundleReader
    setup_logging(args.log_level, _workdir(args) / "logs" / "index-bundle.log")
    reader = BundleReader.open(args.bundle, _plain_s3(args) if args.bundle.startswith("s3://") else None)
    reader.check_schema(args.allow_schema_mismatch)
    prefix = store.index_prefix(args.mode, args.index_prefix)
    op_type = "index" if (args.replace or reader.kind == "delta") else "create"
    ids = _ids(args)
    summary = {"bundle_id": reader.manifest["bundle_id"], "kind": reader.kind, "prefix": prefix,
               "op_type": op_type, "bundle_docs": reader.manifest["doc_count"]}
    if args.dry_run:
        emit(summary | {"indices": list(reader.manifest["indices"])})
        return 0
    client = _client(args)
    docs = (d for d in reader.iter_docs() if ids is None or d["id"] in ids)
    result = store.bulk_load(client, store.bulk_actions(docs, prefix, op_type, restamp=False),
                             chunk_size=args.bulk_size, threads=args.concurrency)
    summary.update(result.to_dict())
    store.refresh(client, result.indices)
    summary["alias"] = store.ensure_alias(client, prefix, reader.manifest["dataset"])
    emit(summary)
    return 0 if not result.errors else 1


def cmd_enrich(args):
    from tools.ops.daac_catalog import store
    from tools.ops.daac_catalog.build import BuildContext
    from tools.ops.daac_catalog.fetch import SourceFetcher
    from tools.ops.daac_catalog.stages import EnrichOptions, run_enrich
    workdir = _workdir(args)
    now = datetime.now(timezone.utc)
    setup_logging(args.log_level, workdir / "logs" / f"enrich-{args.dataset}-{now:%Y%m%dT%H%MZ}.log")
    client = _client(args)
    results = []
    for spec in _specs(args):
        # tier-2 reads stay enabled: a document already at tier 2 is rebuilt at tier 2 whatever --tier says
        fetcher = SourceFetcher(_clients(args), _fetch_options(args, 2))
        ctx = BuildContext(run_id=f"enr-{now:%Y%m%dT%H%MZ}", listed_at=now,
                           frame_to_bursts=_frame_to_bursts(args, spec, workdir))
        options = EnrichOptions(tier=args.tier, since=args.since, until=args.until, only_missing=not args.all_tiers,
                                with_problems=args.with_problems, workers=args.workers, limit=args.limit,
                                prefix=store.index_prefix("catalog", args.index_prefix))
        if args.dry_run:
            from tools.ops.daac_catalog.stages import enrich_query
            pattern = store.dataset_pattern(options.prefix, spec.dataset.value)
            results.append({"dataset": spec.dataset.value, "pattern": pattern,
                            "would_rebuild": store.count(client, pattern, enrich_query(spec.dataset.value, options))})
            continue
        results.append(run_enrich(spec, client, fetcher, ctx, options,
                                  workdir / "reports" / f"enrich-{spec.dataset.value}-{now:%Y%m%dT%H%MZ}.json"))
    emit(results)
    return 0


def cmd_export(args):
    from tools.ops.daac_catalog import store
    from tools.ops.daac_catalog.bundle import BundleWriter
    from tools.ops.daac_catalog.export import export_from_run, export_from_store, listing_files, new_bundle_dir
    specs = _specs(args)
    if len(specs) != 1:
        raise SystemExit("export takes exactly one --dataset")
    spec = specs[0]
    workdir = _workdir(args)
    run = _run(args) if (args.source == "build" or args.with_listing) else None
    setup_logging(args.log_level, workdir / "logs" / f"export-{spec.dataset.value}.log")
    directory = args.out or new_bundle_dir(workdir / "bundles", spec.dataset.value, args.source == "build",
                                           run.run_id if run else None)
    kind = "delta" if args.since else "full"
    source = {"kind": args.source, "run_id": run.run_id if run else None, "since": args.since,
              "cluster": args.es_url if args.source == "store" else None}
    if args.dry_run:
        emit({"bundle": str(directory), "kind": kind, "source": source})
        return 0
    writer = BundleWriter(directory, spec.dataset.value, kind=kind, source=source, parquet=args.parquet)
    if args.source == "store":
        export_from_store(spec, _client(args), writer, store.index_prefix("catalog", args.index_prefix),
                          since=args.since, ids=_ids(args), slices=args.slices)
    else:
        export_from_run(spec, run, writer, args.shard_filter, _ids(args))
    if args.with_listing and run:
        writer.add_listing(listing_files(run, spec.dataset.value))
    manifest = writer.close()
    emit({"bundle": str(directory), "doc_count": manifest["doc_count"], "kind": manifest["kind"],
          "indices": len(manifest["indices"]), "files": len(manifest["files"])})
    return 0


def cmd_push(args):
    from tools.ops.daac_catalog.bundle import push
    if args.dry_run:
        emit({"bundle": str(args.bundle), "to": args.to})
        return 0
    emit({"pushed": push(args.bundle, args.to, _plain_s3(args))})
    return 0


def cmd_pull(args):
    from tools.ops.daac_catalog.bundle import pull
    target = pull(args.source, _workdir(args) / "bundles", _plain_s3(args))
    emit({"pulled": str(target)})
    return 0


def cmd_reconcile(args):
    from tools.ops.daac_catalog import store
    from tools.ops.daac_catalog.reconcile import reconcile_dataset
    run = _run(args)
    setup_logging(args.log_level, run.logs() / f"reconcile-{args.dataset}.log")
    client = _client(args)
    prefix = store.index_prefix("catalog", args.index_prefix)
    results = [reconcile_dataset(spec, run, client, prefix, touch_last_seen=args.touch_last_seen,
                                 apply=not (args.no_apply or args.dry_run)) for spec in _specs(args)]
    emit(results)
    return 0


def cmd_validate(args):
    from tools.ops.daac_catalog import store, validate
    from tools.ops.daac_catalog.bundle import BundleReader
    workdir = _workdir(args)
    run = None
    if args.counts or args.sample_from == "run":
        try:
            run = _run(args)
        except SystemExit:
            run = None
    client = _client(args)
    prefix = store.index_prefix("catalog", args.index_prefix)
    sections = {}
    if args.bundle:
        reader = BundleReader.open(args.bundle, _plain_s3(args) if args.bundle.startswith("s3://") else None)
        sections["bundle counts"] = validate.validate_bundle_counts(reader, client, prefix)
    for spec in _specs(args, required=not args.bundle and not args.query_pack):
        if args.counts:
            sections[f"{spec.dataset.value} counts"] = validate.validate_counts(spec, run, client, prefix)
        if args.against_grq or args.grq_docs:
            grq_docs = validate.load_grq_dump(args.grq_docs) if args.grq_docs else None
            sample = validate.sample_run_docs(spec, run, args.sample, only_ids=set(grq_docs) if grq_docs else None) \
                if args.sample_from == "run" and run else validate.sample_catalog_docs(spec, client, prefix, args.sample)
            if grq_docs is not None:
                sections[f"{spec.dataset.value} vs GRQ"] = validate.validate_against_grq(spec, sample,
                                                                                          grq_docs=grq_docs)
            else:
                sections[f"{spec.dataset.value} vs GRQ"] = validate.validate_against_grq(
                    spec, sample, _client(args, args.against_grq, args.grq_netrc), args.grq_pattern)
    if args.query_pack:
        targets = dict(t.split("=", 1) for t in args.query_target) if args.query_target else {"catalog": "catalog"}
        sections["query pack"] = validate.run_query_pack(client, targets)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
    path = validate.write_report(run, workdir, f"validate_{args.dataset or 'bundle'}_{stamp}.md",
                                 validate.markdown_report(f"daac_catalog validation {stamp}", sections))
    emit({"report": str(path), "sections": sections})
    return 0


def cmd_report(args):
    from tools.ops.daac_catalog.runs import read_done
    run = _run(args)
    datasets = [s.dataset.value for s in _specs(args, required=False)] or \
        sorted(p.name for p in run.path("listing").glob("*") if p.is_dir())
    rows = []
    for dataset in datasets:
        listing = [read_done(run.listing(dataset, s)) for s in run.listing_shards(dataset)]
        docs = [read_done(run.docs(dataset, s)) for s in run.docs_shards(dataset)]
        from tools.ops.daac_catalog import store as store_mod
        family_prefix = store_mod.index_prefix("catalog", args.index_prefix)
        index = [read_done(run.index_result(dataset, s, family_prefix)) for s in run.docs_shards(dataset)]
        listing, docs, index = [x for x in listing if x], [x for x in docs if x], [x for x in index if x]
        objects, requests = {}, {}
        for x in listing:
            for k, v in x["objects"].items():
                objects[k] = objects.get(k, 0) + v
            for k, v in x["requests"].items():
                requests[k] = requests.get(k, 0) + v
        rows.append({
            "dataset": dataset,
            "listing": {"shards": len(listing), "granules": sum(x["granules"] for x in listing), "objects": objects,
                        "requests": requests, "seconds": round(sum(x["seconds"] for x in listing), 1),
                        "unparsed": sum(x["unparsed_count"] for x in listing)},
            "build": {"shards": len(docs), "docs": sum(x["docs"] for x in docs),
                      "rejects": sum(x["rejects"] for x in docs), "seconds": round(sum(x["seconds"] for x in docs), 1),
                      "coverage": _sum_dicts(x.get("coverage") or {} for x in docs)},
            "index": {"shards": len(index), "ok": sum(x["ok"] for x in index),
                      "conflicts": sum(x["conflicts"] for x in index), "errors": sum(x["errors"] for x in index)},
        })
    if args.store_sizes:
        from tools.ops.daac_catalog import store
        client = _client(args)
        prefix = store.index_prefix("catalog", args.index_prefix)
        for row in rows:
            stats = client.cat.indices(index=store.dataset_pattern(prefix, row["dataset"]), format="json",
                                       h="index,docs.count,store.size,pri.store.size", bytes="b")
            docs = sum(int(s["docs.count"] or 0) for s in stats)
            primary = sum(int(s["pri.store.size"] or 0) for s in stats)
            row["store"] = {"indices": len(stats), "docs": docs, "primary_bytes": primary,
                            "bytes_per_doc": round(primary / docs, 1) if docs else None}
    from tools.ops.daac_catalog.validate import markdown_report, write_report
    path = write_report(run, run.workdir, f"run_{run.run_id}.md", markdown_report(f"Run {run.run_id}", {
        r["dataset"]: r for r in rows}))
    emit({"report": str(path), "datasets": rows})
    return 0


def _sum_dicts(dicts) -> dict:
    out = {}
    for d in dicts:
        for k, v in d.items():
            out[k] = out.get(k, 0) + v
    return out


COMMANDS = {
    "template": cmd_template, "plan": cmd_plan, "list": cmd_list, "build": cmd_build, "index": cmd_index,
    "enrich": cmd_enrich, "export": cmd_export, "push": cmd_push, "pull": cmd_pull, "reconcile": cmd_reconcile,
    "validate": cmd_validate, "report": cmd_report,
}


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level, None)
    return COMMANDS[args.command](args)
