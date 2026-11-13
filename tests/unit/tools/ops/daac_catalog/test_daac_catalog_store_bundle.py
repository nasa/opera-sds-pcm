"""Store actions, the index template, bundles (write, verify, push, pull), export and reconcile."""
import gzip
import json
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from daac_catalog_testlib import context, granule_ids, grq_hit, listing, spec_of, tier1
from tools.ops.daac_catalog import SCHEMA_VERSION, export, reconcile, store
from tools.ops.daac_catalog.build import build_doc, index_name
from tools.ops.daac_catalog.bundle import MANIFEST, BundleError, BundleReader, BundleWriter, pull, push
from tools.ops.daac_catalog.parse import files_digest, parse_granule
from tools.ops.daac_catalog.runs import JsonlGzWriter, RunDir, mark_done
from tools.ops.daac_catalog.stages import listing_from_doc


@pytest.fixture(scope="module")
def docs():
    return [build_doc(parse_granule(spec_of(g), listing(g)), context(), tier1(g)) for g in granule_ids()]


# ---- store ------------------------------------------------------------------------------------------------------

def test_index_names(docs):
    for doc in docs:
        assert index_name(doc).startswith(f"catalog_{doc['version']}_{doc['dataset'].lower()}-")
        assert index_name(doc, "grq_") == grq_hit(doc["id"])["_index"]
        assert index_name(doc, "") == index_name(doc).removeprefix("catalog_")


def test_bulk_actions_use_source_and_stamp(docs):
    now = datetime(2026, 10, 21, 6, 0, tzinfo=timezone.utc)
    actions = list(store.bulk_actions([json.loads(json.dumps(d)) for d in docs], "grq_vtest_", "create", now))
    assert all(a["_op_type"] == "create" and a["_id"] == a["_source"]["id"] for a in actions)
    assert all(a["_index"].startswith("grq_vtest_v1.") for a in actions)
    assert all(a["_source"]["catalog"]["updated_at"] == "2026-10-21T06:00:00Z" for a in actions)
    assert all("id" not in a or a["id"] == a["_source"]["id"] for a in actions)


def test_index_prefix_and_patterns():
    assert store.index_prefix("catalog") == "catalog_"
    assert store.index_prefix("grq") == "grq_"
    assert store.index_prefix("grq", "grq_vtest_") == "grq_vtest_"
    assert store.dataset_pattern("catalog_", "L3_DSWx_HLS") == "catalog_*_l3_dswx_hls-*"


def test_template_mappings():
    template = store.load_template()
    assert template["index_patterns"] == ["catalog_*"]
    props = template["template"]["mappings"]["properties"]
    assert props["location"]["type"] == "geo_shape" and props["center"]["type"] == "geo_shape"
    assert props["creation_timestamp"]["type"] == "date"
    assert props["metadata"]["properties"]["frame_id"]["type"] == "long"
    assert props["metadata"]["properties"]["FileSize"]["type"] == "float"
    catalog = props["catalog"]["properties"]
    for key in ("updated_at", "first_seen", "last_seen", "deleted_at"):
        assert catalog[key]["type"] == "date"
    assert catalog["pge_runconfig"]["index"] is False
    assert {"src", "bucket", "key", "role"} <= set(catalog["files"]["properties"])
    assert template["template"]["aliases"] == {"catalog": {}}


def test_documents_round_trip_to_a_listing(docs):
    for doc in docs:
        rebuilt = listing_from_doc(doc)
        assert sorted(f.key for f in rebuilt.files) == sorted(f.key for f in listing(doc["id"]).files)
        again = build_doc(parse_granule(spec_of(doc["id"]), rebuilt), context(), tier1(doc["id"]))
        assert again == doc


# ---- bundles ----------------------------------------------------------------------------------------------------

def _write_bundle(path, docs, kind="full", shard_docs=50_000):
    writer = BundleWriter(path, docs[0]["dataset"], kind=kind, source={"kind": "test"}, shard_docs=shard_docs)
    for doc in docs:
        writer.write(doc)
    return writer.close()


def test_bundle_round_trip(tmp_path, docs):
    same = [d for d in docs if d["dataset"] == docs[0]["dataset"]]
    many = [json.loads(json.dumps(same[0])) for _ in range(5)]
    for i, doc in enumerate(many):
        doc["id"] = f"{doc['id']}-{i}"
    manifest = _write_bundle(tmp_path / "b1", many, shard_docs=2)
    assert manifest["doc_count"] == 5 and manifest["kind"] == "full" and manifest["schema_version"] == SCHEMA_VERSION
    suffix = index_name(many[0], "")
    assert manifest["indices"][suffix]["count"] == 5
    assert len(manifest["indices"][suffix]["files"]) == 3     # rolled every 2 documents
    reader = BundleReader.open(str(tmp_path / "b1"))
    assert reader.verify() == 5
    assert [d["id"] for d in reader.iter_docs()] == [d["id"] for d in many]


def test_bundle_refuses_tampering_and_incomplete(tmp_path, docs):
    _write_bundle(tmp_path / "b2", docs[:1])
    rel = BundleReader.open(str(tmp_path / "b2")).doc_files()[0]
    shard = tmp_path / "b2" / rel
    shard.write_bytes(gzip.compress(b'{"id": "forged"}\n'))
    with pytest.raises(BundleError, match="sha256"):
        list(BundleReader.open(str(tmp_path / "b2")).iter_docs())
    (tmp_path / "b3" / "docs").mkdir(parents=True)
    with pytest.raises(BundleError, match="no bundle.json"):
        BundleReader.open(str(tmp_path / "b3"))
    with pytest.raises(BundleError, match="already holds"):
        BundleWriter(tmp_path / "b2", "L2_CSLC_S1")


def test_bundle_schema_check(tmp_path, docs):
    _write_bundle(tmp_path / "b4", docs[:1])
    manifest_path = tmp_path / "b4" / MANIFEST
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = "2.0"
    manifest_path.write_text(json.dumps(manifest))
    reader = BundleReader.open(str(tmp_path / "b4"))
    with pytest.raises(BundleError, match="schema"):
        reader.check_schema()
    reader.check_schema(allow_mismatch=True)


def test_push_pull_through_s3(tmp_path, docs):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(Bucket="bundles", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        same = [d for d in docs if d["dataset"] == "L3_DISP_S1"]
        _write_bundle(tmp_path / "src" / "L3_DISP_S1-exp-20261021T0600Z", same)
        url = push(tmp_path / "src" / "L3_DISP_S1-exp-20261021T0600Z", "s3://bundles/daac_catalog", s3)
        assert url == "s3://bundles/daac_catalog/L3_DISP_S1/L3_DISP_S1-exp-20261021T0600Z"
        streamed = BundleReader.open(url, s3)
        assert [d["id"] for d in streamed.iter_docs()] == [d["id"] for d in same]
        target = pull(url, tmp_path / "dst", s3)
        assert BundleReader.open(str(target)).verify() == len(same)
        with pytest.raises(BundleError):
            pull(url, tmp_path / "dst", s3)
        # shards without a manifest are not a bundle
        s3.put_object(Bucket="bundles", Key="half/L3_DISP_S1/x/docs/a.jsonl.gz", Body=b"")
        with pytest.raises(BundleError, match="bundle.json"):
            BundleReader.open("s3://bundles/half/L3_DISP_S1/x", s3)

        class DroppingClient:            # an upload that silently went missing
            def __getattr__(self, name):
                return getattr(s3, name)

            def upload_file(self, filename, bucket, key):
                if "/docs/" not in key:
                    s3.upload_file(filename, bucket, key)

        with pytest.raises(BundleError, match="missing or have the wrong size"):
            push(tmp_path / "src" / "L3_DISP_S1-exp-20261021T0600Z", "s3://bundles/dropped", DroppingClient())
        assert "Contents" not in s3.list_objects_v2(Bucket="bundles", Prefix="dropped/")


# ---- export -----------------------------------------------------------------------------------------------------

def test_export_query():
    q = export.export_query("L3_DISP_S1", since="2026-10-21T06:00:00Z")
    assert {"range": {"catalog.updated_at": {"gte": "2026-10-21T06:00:00Z"}}} in q["bool"]["filter"]
    q = export.export_query("L3_DISP_S1", ids={"b", "a"})
    assert {"terms": {"id.keyword": ["a", "b"]}} in q["bool"]["filter"]


def test_export_from_store_and_from_run(tmp_path, docs, monkeypatch):
    disp = [d for d in docs if d["dataset"] == "L3_DISP_S1"]
    calls = []

    def fake_scan(client, index, query=None, source=None, **kw):
        calls.append((index, query))
        return iter({"_source": d, "_index": index_name(d), "_id": d["id"]} for d in disp)

    monkeypatch.setattr(store, "scan", fake_scan)
    writer = BundleWriter(tmp_path / "s", "L3_DISP_S1", kind="delta", source={"kind": "store"})
    assert export.export_from_store(spec_of(disp[0]["id"]), object(), writer, since="2026-10-21") == 1
    assert writer.close()["kind"] == "delta"
    assert calls[0][0] == "catalog_*_l3_disp_s1-*"

    run = RunDir(tmp_path, "inv-20261020T2100Z")
    path = run.docs("L3_DISP_S1", "16")
    with JsonlGzWriter(path) as w:
        for d in disp:
            w.write(d)
    mark_done(path, {"docs": 1, "rejects": 0})
    writer = BundleWriter(tmp_path / "r", "L3_DISP_S1", source={"kind": "build"})
    assert export.export_from_run(spec_of(disp[0]["id"]), run, writer) == 1
    assert writer.close()["indices"]


# ---- reconcile --------------------------------------------------------------------------------------------------

def test_reconcile_transitions(tmp_path, monkeypatch):
    spec = spec_of(granule_ids()[0])
    dataset = spec.dataset.value
    run = RunDir(tmp_path, "inv-20261027T2100Z")
    shard_prefix = spec.shards()[20]
    path = run.listing(dataset, "S")
    with JsonlGzWriter(path) as w:
        for gid in ("present", "back", "new"):
            w.write({"id": gid, "dataset": dataset, "files": []})
    mark_done(path, {"granules": 3, "complete": True, "shard_prefix": shard_prefix, "objects": {},
                     "requests": {}, "seconds": 1, "unparsed_count": 0})
    incomplete = run.listing(dataset, "X")
    with JsonlGzWriter(incomplete) as w:
        w.write({"id": "other", "dataset": dataset, "files": []})
    mark_done(incomplete, {"granules": 1, "complete": False, "shard_prefix": "x"})
    no_files = files_digest([])
    known = {"present": {"index": "i", "deleted_at": None, "lifecycle": [], "files_digest": no_files},
             "back": {"index": "i", "deleted_at": "2026-10-20T21:00:00Z",
                      "lifecycle": [{"event": "deleted", "at": "2026-10-20T21:00:00Z"}], "files_digest": no_files},
             "gone": {"index": "i", "deleted_at": None, "lifecycle": [], "files_digest": no_files}}
    monkeypatch.setattr(reconcile, "stored_ids", lambda client, prefix, ds, id_prefix: known)
    updates = []
    monkeypatch.setattr(store, "partial_updates", lambda client, u: updates.extend(u) or store.BulkResult())
    summary = reconcile.reconcile_dataset(spec, run, object(), now=datetime(2026, 10, 27, 22, tzinfo=timezone.utc))
    assert summary["new"] == 1 and summary["deleted"] == 1 and summary["reappeared"] == 1 and summary["changed"] == 0
    assert summary["incomplete_shards"] == ["X"]
    by_id = {gid: fields for _, gid, fields in updates}
    assert by_id["gone"]["deleted_at"] == "2026-10-27T21:00:00Z"
    assert by_id["gone"]["lifecycle"][-1]["event"] == "deleted"
    assert by_id["back"]["deleted_at"] is None and by_id["back"]["lifecycle"][-1]["event"] == "reappeared"
    assert "present" not in by_id
    assert (run.path("reconcile") / f"{dataset}.new_ids.txt").read_text() == "new\n"


# ---- validate ---------------------------------------------------------------------------------------------------

def test_validate_against_a_grq_dump(tmp_path, docs):
    from tools.ops.daac_catalog import validate
    dump = tmp_path / "hits.jsonl"
    older = json.loads(json.dumps(grq_hit(docs[0]["id"])))
    older["_index"] = "grq_v1.0_old-2020.01"
    older["_source"]["metadata"]["sas_version"] = "0.0.0"   # an older generation of the same id loses
    dump.write_text("\n".join(json.dumps(x) for x in [older] + [grq_hit(d["id"]) for d in docs]) + "\n")
    grq_docs = validate.load_grq_dump(dump)
    assert len(grq_docs) == len(docs)
    assert grq_docs[docs[0]["id"]]["metadata"]["sas_version"] != "0.0.0"
    for doc in docs:
        result = validate.validate_against_grq(spec_of(doc["id"]), [doc], grq_docs=grq_docs)
        assert result["found_in_grq"] == 1 and result["exact_rate"] == 1.0 and result["approx_rate"] == 1.0, result


def test_grq_without_cnm_r_is_a_known_state(docs):
    from tools.ops.daac_catalog.compare import compare_docs
    doc = docs[0]
    grq = json.loads(json.dumps(grq_hit(doc["id"])["_source"]))
    for key in [k for k in grq if k.startswith("daac_")] + ["archive_product_urls"]:
        grq.pop(key, None)
    grq["metadata"]["tags"] = ["PGE"]
    result = compare_docs(doc, grq)
    assert result.ok, result.lines()
    assert "GRQ has no CNM-R record for this delivered product" in result.known_artifacts


# ---- enrich -----------------------------------------------------------------------------------------------------

def test_enrich_upgrades_in_place_and_moves_across_months(monkeypatch):
    from tools.ops.daac_catalog import stages
    gid = granule_ids()[0]
    spec = spec_of(gid)
    tier0 = build_doc(parse_granule(spec, listing(gid)), context())
    tier0["catalog"]["first_seen"] = "2026-01-01T00:00:00Z"
    tier0["catalog"]["lifecycle"] = [{"event": "reappeared", "at": "2026-02-01T00:00:00Z"}]
    tier0["creation_timestamp"] = "2025-01-01T00:00:00.000"         # pretend the tier-0 proxy fell in another month
    old_index = index_name(tier0)

    class FakeFetcher:
        def tier1(self, parsed):
            return tier1(parsed.id), []

        def tier2(self, parsed):
            return None, []

    monkeypatch.setattr(store, "scan", lambda client, index, query=None, **kw: iter(
        [{"_source": json.loads(json.dumps(tier0)), "_index": old_index, "_id": gid}]))
    sent = []
    monkeypatch.setattr(store, "bulk_load", lambda client, actions, **kw: sent.extend(actions) or store.BulkResult(
        ok=len(sent)))
    summary = stages.run_enrich(spec, object(), FakeFetcher(), context(), stages.EnrichOptions(tier=1, workers=2))
    assert summary["scanned"] == 1 and summary["failed"] == 0 and summary["moved"] == 1
    index_action, delete_action = sent
    new = index_action["_source"]
    assert index_action["_op_type"] == "index" and index_action["_index"] != old_index
    assert delete_action == {"_op_type": "delete", "_index": old_index, "_id": gid}
    assert new["catalog"]["tier"] == 1
    assert new["catalog"]["first_seen"] == "2026-01-01T00:00:00Z"
    assert new["catalog"]["lifecycle"] == tier0["catalog"]["lifecycle"]
    assert "location" in new and new["metadata"]["pge_version"]


def test_enrich_query():
    from tools.ops.daac_catalog.stages import EnrichOptions, enrich_query
    q = enrich_query("L2_RTC_S1", EnrichOptions(tier=1, since="2026-09", until="2026-10"))
    filters = q["bool"]["filter"]
    assert {"range": {"catalog.tier": {"lt": 1}}} in filters
    assert {"range": {"creation_timestamp": {"gte": "2026-09-01T00:00:00", "lt": "2026-10-01T00:00:00"}}} in filters


def test_bundle_loads_keep_the_source_stamp(docs):
    doc = json.loads(json.dumps(docs[0]))
    doc["catalog"]["updated_at"] = "2026-10-21T06:00:00Z"
    action = next(store.bulk_actions([doc], "catalog_xfer_", "create", restamp=False))
    assert action["_source"]["catalog"]["updated_at"] == "2026-10-21T06:00:00Z"
    action = next(store.bulk_actions([json.loads(json.dumps(doc))], "catalog_", "create"))
    assert action["_source"]["catalog"]["updated_at"] != "2026-10-21T06:00:00Z"


def test_index_results_are_tracked_per_family(tmp_path):
    run = RunDir(tmp_path, "inv-20261020T2100Z")
    assert run.index_result("L4_TROPO", "2026") != run.index_result("L4_TROPO", "2026", "grq_vtest_")
    assert run.index_result("L4_TROPO", "2026", "grq_").parts[-3] == "grq"


def test_bulk_load_retries_retryable_items(monkeypatch):
    calls = []

    def fake_parallel_bulk(client, actions, **kw):
        batch = list(actions)
        calls.append([a["_id"] for a in batch])
        for a in batch:
            if a["_id"] == "busy" and len(calls) == 1:
                yield False, {"create": {"_index": a["_index"], "_id": a["_id"], "status": 429, "error": "queue"}}
            elif a["_id"] == "dup":
                yield False, {"create": {"_index": a["_index"], "_id": a["_id"], "status": 409, "error": "exists"}}
            elif a["_id"] == "bad":
                yield False, {"create": {"_index": a["_index"], "_id": a["_id"], "status": 400, "error": "mapper"}}
            else:
                yield True, {"create": {"_index": a["_index"], "_id": a["_id"], "status": 201}}

    from opensearchpy import helpers
    monkeypatch.setattr(helpers, "parallel_bulk", fake_parallel_bulk)
    actions = [{"_op_type": "create", "_index": "i", "_id": x, "_source": {}} for x in ("ok", "busy", "dup", "bad")]
    result = store.bulk_load(object(), actions, backoff_seconds=0)
    assert calls == [["ok", "busy", "dup", "bad"], ["busy"]]
    assert (result.ok, result.conflicts, result.errors) == (2, 1, 1)
    assert result.error_samples[0]["id"] == "bad"
