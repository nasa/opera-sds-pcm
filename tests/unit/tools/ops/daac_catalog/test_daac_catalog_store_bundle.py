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
from tools.ops.daac_catalog.parse import parse_granule
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
    known = {"present": {"index": "i", "deleted_at": None, "lifecycle": []},
             "back": {"index": "i", "deleted_at": "2026-10-20T21:00:00Z",
                      "lifecycle": [{"event": "deleted", "at": "2026-10-20T21:00:00Z"}]},
             "gone": {"index": "i", "deleted_at": None, "lifecycle": []}}
    monkeypatch.setattr(reconcile, "stored_ids", lambda client, prefix, ds, id_prefix: known)
    updates = []
    monkeypatch.setattr(store, "partial_updates", lambda client, u: updates.extend(u) or store.BulkResult())
    summary = reconcile.reconcile_dataset(spec, run, object(), now=datetime(2026, 10, 27, 22, tzinfo=timezone.utc))
    assert summary["new"] == 1 and summary["deleted"] == 1 and summary["reappeared"] == 1
    assert summary["incomplete_shards"] == ["X"]
    by_id = {gid: fields for _, gid, fields in updates}
    assert by_id["gone"]["deleted_at"] == "2026-10-27T21:00:00Z"
    assert by_id["gone"]["lifecycle"][-1]["event"] == "deleted"
    assert by_id["back"]["deleted_at"] is None and by_id["back"]["lifecycle"][-1]["event"] == "reappeared"
    assert "present" not in by_id
    assert (run.path("reconcile") / f"{dataset}.new_ids.txt").read_text() == "new\n"
