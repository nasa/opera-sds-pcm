"""Incremental runs: file digests, changed granules, id selections, replacing changed documents, enrich retries."""
import json
from dataclasses import replace

import pytest

from daac_catalog_testlib import context, granule_ids, listing, spec_of
from tools.ops.daac_catalog import cli, export, reconcile, stages, store
from tools.ops.daac_catalog.build import build_doc
from tools.ops.daac_catalog.parallel import fork_map
from tools.ops.daac_catalog.parse import GranuleListing, ListedObject, files_digest, listing_digest, parse_granule
from tools.ops.daac_catalog.runs import JsonlGzWriter, RunDir, mark_done, read_done, read_ids_file, read_jsonl_gz, \
    shard_name

RUN_ID = "inv-20261027T2100Z"


def _doc(gid: str) -> dict:
    return build_doc(parse_granule(spec_of(gid), listing(gid)), context())


def _write_listing(run: RunDir, spec, shard_prefix: str, records: list[dict]) -> str:
    shard = shard_name(spec, shard_prefix)
    path = run.listing(spec.dataset.value, shard)
    with JsonlGzWriter(path) as w:
        for record in records:
            w.write(record)
    mark_done(path, {"granules": len(records), "complete": True, "shard_prefix": shard_prefix})
    return shard


def _two_shards(run: RunDir, gid: str):
    """The fixture granule's shard and a second shard holding one granule that is not the fixture's."""
    spec = spec_of(gid)
    own = next(p for p in spec.shards() if gid.startswith(p))
    other = next(p for p in spec.shards() if p != own)
    a = _write_listing(run, spec, own, [listing(gid).to_record()])
    b = _write_listing(run, spec, other, [{"id": f"{other}X", "dataset": spec.dataset.value, "files": []}])
    return spec, a, b


# ---- digests ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("gid", granule_ids())
def test_document_digest_matches_its_listing_and_its_files(gid):
    doc = _doc(gid)
    digest = doc["catalog"]["files_digest"]
    assert digest == listing_digest(listing(gid))
    assert digest == files_digest((f["bucket"], f["key"], f["size"], f.get("etag")) for f in doc["catalog"]["files"])


def test_digest_ignores_order_last_modified_and_foreign_files():
    gid = granule_ids()[0]
    base = listing(gid)
    shuffled = GranuleListing(gid, base.dataset, [replace(o, last_modified="2030-01-01T00:00:00+00:00")
                                                  for o in reversed(base.files)])
    foreign = GranuleListing(gid, base.dataset, base.files + [ListedObject("products", "bkt", "x/OTHER.h5", 1, "")])
    assert listing_digest(shuffled) == listing_digest(foreign) == listing_digest(base)
    marker = GranuleListing(gid, base.dataset, base.files + [ListedObject("products", "bkt", f"x/{gid}/", 0, "")])
    assert parse_granule(spec_of(gid), marker).problems == [] and listing_digest(marker) == listing_digest(base)
    grown = GranuleListing(gid, base.dataset, [replace(base.files[0], size=base.files[0].size + 1)] + base.files[1:])
    assert listing_digest(grown) != listing_digest(base)


# ---- reconcile: changed granules --------------------------------------------------------------------------------

def _record(gid: str, size: int = 10) -> dict:
    return {"id": gid, "dataset": "L4_TROPO",
            "files": [{"src": "products", "bucket": "bkt", "key": f"p/{gid}.nc", "size": size,
                       "last_modified": "2026-10-01T00:00:00+00:00", "etag": "e"}]}


def _digest(record: dict) -> str:
    return listing_digest(GranuleListing.from_record(record))


def test_reconcile_writes_changed_granules(tmp_path, monkeypatch):
    spec = spec_of(next(g for g in granule_ids() if "TROPO" in g))
    run = RunDir(tmp_path, RUN_ID)
    listed = [_record("same"), _record("grown", 11), _record("legacy_same"), _record("legacy_grown", 11),
              _record("back")]
    _write_listing(run, spec, spec.shards()[0], listed)
    known = {gid: {"index": "i", "deleted_at": None, "lifecycle": [], "files_digest": _digest(_record(gid))}
             for gid in ("same", "grown", "back")}
    known["back"]["deleted_at"] = "2026-10-20T21:00:00Z"
    for gid in ("legacy_same", "legacy_grown"):
        known[gid] = {"index": "i", "deleted_at": None, "lifecycle": [], "files_digest": None}
    monkeypatch.setattr(reconcile, "stored_ids", lambda client, prefix, ds, id_prefix: known)

    def fake_find(client, index, ids, source=None):
        return {gid: {"_id": gid, "_index": "i", "_source": {"catalog": {"files": _record(gid)["files"]}}}
                for gid in ids}

    monkeypatch.setattr(store, "find", fake_find)
    updates = []
    monkeypatch.setattr(store, "partial_updates", lambda client, u: updates.extend(u) or store.BulkResult())
    summary = reconcile.reconcile_dataset(spec, run, object())
    assert summary["changed"] == 2 and summary["digests_from_files"] == 2 and summary["reappeared"] == 1
    # the unchanged legacy document gets its digest stored, so the fallback runs once
    assert summary["digests_stored"] == 1
    assert [(gid, f["files_digest"]) for _, gid, f in updates if "files_digest" in f] == [
        ("legacy_same", _digest(_record("legacy_same")))]
    assert read_ids_file(run.reconcile_ids(spec.dataset.value, "changed")) == {"grown", "legacy_grown"}
    assert read_ids_file(run.reconcile_ids(spec.dataset.value, "new")) == set()


# ---- build: selections ------------------------------------------------------------------------------------------

def test_build_selection_by_ids(tmp_path):
    gid = granule_ids()[0]
    run = RunDir(tmp_path, RUN_ID)
    spec, a, b = _two_shards(run, gid)
    dataset = spec.dataset.value

    def build(ids):
        return stages.run_build(spec, run, None, context(), stages.BuildOptions(tier=0, workers=2, ids=ids))

    summary = build(set())                      # an empty selection builds nothing
    assert (summary["shards"], summary["unselected"]) == (0, 2)
    assert not run.docs(dataset, a).exists() and not run.docs(dataset, b).exists()

    summary = build({gid})                      # only the shard that can hold the id
    assert (summary["shards"], summary["unselected"]) == (1, 1)
    assert [d["id"] for d in read_jsonl_gz(run.docs(dataset, a))] == [gid]
    assert read_done(run.docs(dataset, a))["filtered"] is True

    summary = build(None)                       # a filtered shard is not done for a full build
    assert (summary["shards"], summary["skipped"], summary["rejects"]) == (2, 0, 1)

    summary = build({gid})                      # a full build covers any selection
    assert (summary["shards"], summary["skipped"], summary["unselected"]) == (0, 1, 1)


def test_cli_build_from_reconcile(tmp_path, capsys):
    gid = granule_ids()[0]
    run = RunDir(tmp_path, RUN_ID)
    spec, a, b = _two_shards(run, gid)
    dataset = spec.dataset.value
    argv = ["build", "--dataset", dataset, "--workdir", str(tmp_path), "--run-id", RUN_ID, "--tier", "0",
            "--from-reconcile"]
    assert cli.main(argv) == 1                  # reconcile has not run: refuse rather than build everything
    assert "no reconcile output" in json.loads(capsys.readouterr().out)["results"][0]["error"]

    run.reconcile_summary(dataset).parent.mkdir(parents=True, exist_ok=True)
    run.reconcile_summary(dataset).write_text("{}")
    run.reconcile_ids(dataset, "new").write_text("")
    run.reconcile_ids(dataset, "changed").write_text(f"{gid}\n")
    assert cli.main(argv) == 0
    result = json.loads(capsys.readouterr().out)["results"][0]
    assert (result["selected_ids"], result["docs"], result["unselected"]) == (1, 1, 1)

    with pytest.raises(SystemExit):
        cli.main(argv + ["--ids-file", str(run.reconcile_ids(dataset, "new"))])


def test_empty_selections_select_nothing(tmp_path):
    empty = tmp_path / "ids.txt"
    empty.write_text("")
    assert read_ids_file(empty) == set() and read_ids_file(None) is None
    assert {"terms": {"id.keyword": []}} in export.export_query("L4_TROPO", ids=set())["bool"]["filter"]
    assert len(export.export_query("L4_TROPO")["bool"]["filter"]) == 1
    assert fork_map(lambda group: group, [], {}) == []


# ---- index: changed documents replace the stored ones -----------------------------------------------------------

def test_index_replaces_changed_documents(tmp_path, monkeypatch):
    gid = granule_ids()[0]
    spec = spec_of(gid)
    dataset = spec.dataset.value
    run = RunDir(tmp_path, RUN_ID)
    changed, new = _doc(gid), _doc(gid)
    new["id"] = "NEW"
    path = run.docs(dataset, "S")
    with JsonlGzWriter(path) as w:
        w.write(changed)
        w.write(new)
    mark_done(path, {"docs": 2, "rejects": 0})
    run.reconcile_ids(dataset, "changed").parent.mkdir(parents=True, exist_ok=True)
    run.reconcile_ids(dataset, "changed").write_text(f"{gid}\n")

    old_catalog = {"first_seen": "2026-01-01T00:00:00Z",
                   "lifecycle": [{"event": "reappeared", "at": "2026-02-01T00:00:00Z"}]}
    lookups = []

    def fake_find(client, index, ids, source=None):
        lookups.append(list(ids))
        return {gid: {"_id": gid, "_index": "catalog_old-2020.01", "_source": {"catalog": old_catalog}}}

    sent = []

    def fake_bulk_load(client, actions, **kw):
        batch = list(actions)
        sent.extend(batch)
        return store.BulkResult(ok=len(batch))

    monkeypatch.setattr(store, "find", fake_find)
    monkeypatch.setattr(store, "bulk_load", fake_bulk_load)
    monkeypatch.setattr(store, "refresh", lambda client, indices: None)
    monkeypatch.setattr(store, "ensure_alias", lambda client, prefix, ds: None)

    summary = stages.run_index(spec, run, object(), stages.IndexOptions())
    assert (summary["replaced"], summary["moved"]) == (1, 1) and lookups == [[gid]]
    by_op = {(a["_op_type"], a["_id"]): a for a in sent}
    assert set(by_op) == {("create", "NEW"), ("index", gid), ("delete", gid)}
    replaced = by_op[("index", gid)]["_source"]["catalog"]
    assert replaced["first_seen"] == "2026-01-01T00:00:00Z"
    assert replaced["lifecycle"][0] == old_catalog["lifecycle"][0]
    assert replaced["lifecycle"][-1] == {"event": "changed", "at": "2026-10-27T21:00:00Z", "run_id": RUN_ID}
    assert by_op[("delete", gid)]["_index"] == "catalog_old-2020.01"

    sent.clear()                                # restore mode never overwrites GRQ documents
    summary = stages.run_index(spec, run, object(), stages.IndexOptions(mode="grq"))
    assert summary["replaced"] == 0 and {a["_op_type"] for a in sent} == {"create"} and len(lookups) == 1


# ---- validate: counts for full and incremental runs --------------------------------------------------------------

def test_counts_hold_for_incremental_runs(tmp_path, monkeypatch):
    from tools.ops.daac_catalog import validate
    gid = granule_ids()[0]
    spec = spec_of(gid)
    run = RunDir(tmp_path, RUN_ID)
    _write_listing(run, spec, spec.shards()[0], [_record("a"), _record("b"), _record("c")])
    path = run.docs(spec.dataset.value, "S")             # this run built one new granule and rejected one
    with JsonlGzWriter(path) as w:
        w.write({"id": "c"})
    mark_done(path, {"docs": 1, "rejects": 1, "filtered": True})
    counts = {None: 3, "live": 2}                        # three stored, one of them marked deleted
    monkeypatch.setattr(store, "count", lambda client, index, query=None: counts["live" if query else None])
    result = validate.validate_counts(spec, run, object(), "catalog_")
    assert result["ok"] and (result["store"], result["store_live"]) == (3, 2)
    counts["live"] = 1
    assert not validate.validate_counts(spec, run, object(), "catalog_")["ok"]


def test_store_sample_takes_the_reference_ids(monkeypatch):
    from tools.ops.daac_catalog import validate
    spec = spec_of(granule_ids()[0])
    stored = {f"g{i}": {"id": f"g{i}"} for i in range(1200)}
    calls = []

    def find(client, index, ids, source=None):
        calls.append(len(ids))
        return {i: {"_source": stored[i]} for i in ids if i in stored}

    monkeypatch.setattr(store, "find", find)
    wanted = {f"g{i}" for i in range(0, 1200, 2)} | {"not-stored"}
    sample = validate.sample_catalog_docs(spec, object(), "catalog_", 1000, only_ids=wanted)
    assert len(sample) == 600 and {d["id"] for d in sample} <= wanted
    assert calls == [500, 101]
    assert len(validate.sample_catalog_docs(spec, object(), "catalog_", 10, only_ids=wanted)) == 10
    assert validate.sample_catalog_docs(spec, object(), "catalog_", 10, only_ids=set()) == []


# ---- enrich: documents with problems, no tier downgrade ---------------------------------------------------------

def test_enrich_with_problems_query():
    query = stages.enrich_query("L3_DISP_S1", stages.EnrichOptions(tier=1, with_problems=True))
    filters = query["bool"]["filter"]
    assert {"exists": {"field": "catalog.problems"}} in filters
    assert not any("range" in f for f in filters)


def test_enrich_never_lowers_a_documents_tier(monkeypatch):
    gid = granule_ids()[0]
    spec = spec_of(gid)
    doc = _doc(gid)
    doc["catalog"]["tier"] = 2
    doc["catalog"]["problems"] = ["listed but missing at build time: x"]
    monkeypatch.setattr(store, "scan", lambda client, index, query=None, **kw: iter(
        [{"_source": json.loads(json.dumps(doc)), "_index": "i", "_id": gid}]))
    tiers = []

    def fake_enrich_doc(spec, old, fetcher, ctx, tier, prefix):
        tiers.append(tier)
        return json.loads(json.dumps(old)), "i"

    monkeypatch.setattr(stages, "enrich_doc", fake_enrich_doc)
    monkeypatch.setattr(store, "bulk_load", lambda client, actions, **kw: store.BulkResult(ok=len(list(actions))))
    monkeypatch.setattr(store, "refresh", lambda client, indices: None)
    stages.run_enrich(spec, object(), None, context(), stages.EnrichOptions(tier=1, with_problems=True, workers=1))
    assert tiers == [2]
