"""Optional CMR check: references paging, the CMR-Hits check, catalog.cmr states, build and store runs."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from daac_catalog_testlib import context, gid_for, grq_doc, listing, spec_of, tier1
from tools.ops.daac_catalog import cmr, stages, store
from tools.ops.daac_catalog.build import build_doc
from tools.ops.daac_catalog.compare import compare_docs
from tools.ops.daac_catalog.parse import parse_granule
from tools.ops.daac_catalog.runs import JsonlGzWriter, RunDir, mark_done, read_done, read_jsonl_gz, shard_name

RUN_ID = "inv-20261027T2100Z"
GID = gid_for("L3_DSWx_HLS")
SPEC = spec_of(GID)
PREFIX = next(p for p in SPEC.shards() if GID.startswith(p))
SHARD = shard_name(SPEC, PREFIX)
LATER = GID.replace("_20260702T012549Z_", "_20260709T000000Z_")    # the same natural key, made a week later
OTHER = GID.replace("_20260627T224423Z_", "_20260601T224423Z_")    # another acquisition of the tile


def _doc(gid: str = GID) -> dict:
    doc = build_doc(parse_granule(SPEC, listing(GID)), context(), tier1(GID))
    if gid != GID:
        doc = json.loads(json.dumps(doc).replace(GID, gid))
        doc["catalog"]["natural_key"] = doc["catalog"]["natural_key"].replace("20260627T224423Z", gid.split("_")[4])
    return doc


def _references(hits: int, items: list[dict]) -> bytes:
    refs = "".join(f"<reference><name>{i['id']}</name><id>{i['concept_id']}</id>"
                   f"<location>https://cmr.example/search/concepts/{i['concept_id']}/{i['revision_id']}</location>"
                   f"<revision-id>{i['revision_id']}</revision-id></reference>" for i in items)
    return f'<?xml version="1.0" encoding="UTF-8"?><results><hits>{hits}</hits><took>5</took>' \
           f"<references>{refs}</references></results>".encode()


class FakeResponse:
    def __init__(self, status: int, content: bytes = b"", headers: dict | None = None):
        self.status_code, self.content, self.headers = status, content, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeCmr:
    """CMR granule search over `granules`, `page` items per page, search-after as the next offset."""

    def __init__(self, granules: list[dict], page: int = 2, extra_hits: int = 0, statuses: list[int] | None = None):
        self.granules, self.page, self.extra_hits = granules, page, extra_hits
        self.statuses = list(statuses or [])
        self.calls = []

    def get(self, url, params, headers, timeout):
        self.calls.append((params, dict(headers)))
        if self.statuses:
            return FakeResponse(self.statuses.pop(0))
        assert params["options[granule_ur][pattern]"] == "true" and params["provider"] == "POCLOUD"
        matched = sorted((g for g in self.granules if g["id"].startswith(params["granule_ur[]"][:-1])),
                         key=lambda g: g["id"])
        start = int(headers.get("CMR-Search-After", 0))
        page = matched[start:start + self.page]
        more = {"CMR-Search-After": str(start + len(page))} if start + len(page) < len(matched) else {}
        return FakeResponse(200, _references(len(matched) + self.extra_hits, page), more)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _granule(gid: str, concept: str, revision: int = 1) -> dict:
    return {"id": gid, "concept_id": concept, "revision_id": revision}


# ---- listing ----------------------------------------------------------------------------------------------------

def test_references_parse_and_page_by_search_after():
    granules = [_granule(f"{PREFIX}{n}", f"G{n}-POCLOUD") for n in range(5)] + [_granule("OPERA_X", "G9-POCLOUD")]
    session = FakeCmr(granules)
    items = cmr.list_prefix(session, SPEC, PREFIX)
    assert [i["concept_id"] for i in items] == [f"G{n}-POCLOUD" for n in range(5)]
    assert items[0] == {"id": f"{PREFIX}0", "concept_id": "G0-POCLOUD", "revision_id": 1}
    assert [c[1].get("CMR-Search-After") for c in session.calls] == [None, "2", "4"]
    assert all(c[0]["short_name"] == "OPERA_L3_DSWX-HLS_V1" and c[1]["Client-Id"] for c in session.calls)


def test_a_listing_short_of_cmr_hits_is_refused():
    with pytest.raises(cmr.CmrError, match="CMR-Hits 4"):
        cmr.list_prefix(FakeCmr([_granule(f"{PREFIX}{n}", f"G{n}") for n in range(3)], page=5, extra_hits=1),
                        SPEC, PREFIX)


def test_retryable_statuses_are_retried(monkeypatch):
    monkeypatch.setattr(cmr.time, "sleep", lambda s: None)
    session = FakeCmr([_granule(f"{PREFIX}0", "G0")], statuses=[503, 429])
    assert len(cmr.list_prefix(session, SPEC, PREFIX)) == 1
    with pytest.raises(RuntimeError, match="HTTP 400"):
        cmr.list_prefix(FakeCmr([], statuses=[400]), SPEC, PREFIX)


# ---- documents --------------------------------------------------------------------------------------------------

def test_found_missing_and_unchanged_states():
    doc = _doc()
    assert "daac_catalog_id" in doc["catalog"]["omitted"] and "daac_catalog_id" not in doc
    assert cmr.set_cmr(doc, _granule(GID, "G1-POCLOUD", 3), "2026-10-27T21:00:00Z")
    assert doc["catalog"]["cmr"] == {"concept_id": "G1-POCLOUD", "revision_id": 3, "checked_at": "2026-10-27T21:00:00Z"}
    assert doc["daac_catalog_id"] == "G1-POCLOUD"
    assert doc["daac_catalog_url"] == "https://cmr.earthdata.nasa.gov/search/concepts/G1-POCLOUD.umm_json"
    assert not {"daac_catalog_id", "daac_catalog_url"} & set(doc["catalog"]["omitted"])
    assert not cmr.set_cmr(doc, _granule(GID, "G1-POCLOUD", 3), "2026-11-03T21:00:00Z")
    assert doc["catalog"]["cmr"]["checked_at"] == "2026-10-27T21:00:00Z"
    assert cmr.set_cmr(doc, _granule(GID, "G1-POCLOUD", 4), "2026-11-03T21:00:00Z")
    assert cmr.set_cmr(doc, None, "2026-11-10T21:00:00Z")
    assert doc["catalog"]["cmr"] == {"checked_at": "2026-11-10T21:00:00Z"}
    assert "daac_catalog_id" not in doc and {"daac_catalog_id", "daac_catalog_url"} <= set(doc["catalog"]["omitted"])
    assert not cmr.set_cmr(doc, None, "2026-11-17T21:00:00Z")


def test_a_recent_granule_missing_from_cmr_is_left_alone():
    doc = _doc()
    newest = cmr.newest_file(doc)
    state, changed = cmr.check_doc(doc, None, newest + timedelta(hours=2), timedelta(hours=24))
    assert (state, changed) == ("recent", False) and "cmr" not in doc["catalog"]
    state, changed = cmr.check_doc(doc, None, newest + timedelta(hours=25), timedelta(hours=24))
    assert (state, changed) == ("missing", True) and doc["catalog"]["cmr"] == {
        "checked_at": (newest + timedelta(hours=25)).strftime("%Y-%m-%dT%H:%M:%SZ")}


def test_rebuilds_keep_the_cmr_state():
    old = _doc()
    cmr.set_cmr(old, _granule(GID, "G1-POCLOUD", 3), "2026-10-27T21:00:00Z")
    rebuilt = _doc()
    cmr.carry_cmr(old, rebuilt)
    assert rebuilt["catalog"]["cmr"] == old["catalog"]["cmr"]
    assert rebuilt["daac_catalog_url"] == old["daac_catalog_url"] and rebuilt["catalog"]["omitted"] == \
        old["catalog"]["omitted"]
    unchecked = _doc()
    cmr.carry_cmr(_doc(), unchecked)
    assert "cmr" not in unchecked["catalog"]


def test_enrich_and_replacement_carry_the_cmr_state(monkeypatch):
    old = _doc()
    cmr.set_cmr(old, None, "2026-10-27T21:00:00Z")

    class Fetcher:
        def tier1(self, parsed):
            return tier1(GID), []

    doc, _ = stages.enrich_doc(SPEC, json.loads(json.dumps(old)), Fetcher(), context(), 1, store.CATALOG_PREFIX)
    assert doc["catalog"]["cmr"] == {"checked_at": "2026-10-27T21:00:00Z"}
    found = _doc()
    cmr.set_cmr(found, _granule(GID, "G1-POCLOUD", 3), "2026-10-27T21:00:00Z")
    hit = {"_index": "catalog_v1.1_l3_dswx_hls-2026.07", "_id": GID,
           "_source": {"catalog": {"first_seen": "2026-10-20T21:00:00Z", "cmr": found["catalog"]["cmr"]},
                       "daac_catalog_url": found["daac_catalog_url"]}}
    monkeypatch.setattr(store, "find", lambda client, index, ids, source=None: {GID: hit})
    counts = {"replaced": 0, "moved": 0}
    actions = list(stages._replace_actions(object(), [_doc()], store.CATALOG_PREFIX, RunDir("/x", RUN_ID), counts))
    replaced = actions[0]["_source"]
    assert replaced["catalog"]["cmr"] == found["catalog"]["cmr"] and replaced["daac_catalog_id"] == "G1-POCLOUD"


def test_cmr_fields_are_compared_when_both_sides_have_them():
    grq = grq_doc(GID)
    ours = _doc()
    assert compare_docs(ours, grq).exact_ok
    cmr.set_cmr(ours, _granule(GID, grq["daac_catalog_id"], 1), "2026-10-27T21:00:00Z")
    assert compare_docs(ours, grq).exact_ok
    cmr.set_cmr(ours, _granule(GID, "G1-POCLOUD", 1), "2026-10-27T21:00:00Z")
    assert any(m.startswith("daac_catalog_id") for m in compare_docs(ours, grq).exact_mismatches)


# ---- runs -------------------------------------------------------------------------------------------------------

def _cmr_granules() -> list[dict]:
    # GID and the later version of it are archived, OTHER is archived but not in CMR, and CMR has one granule the
    # archive lacks
    return [_granule(GID, "G1-POCLOUD"), _granule(LATER, "G2-POCLOUD"), _granule(f"{PREFIX}KS_ONLY_IN_CMR", "G3")]


def _options(**kw) -> cmr.CmrOptions:
    return cmr.CmrOptions(**({"grace": timedelta(hours=24)} | kw))


def test_check_rewrites_a_built_shard(tmp_path, monkeypatch):
    run = RunDir(tmp_path, RUN_ID)
    path = run.docs(SPEC.dataset.value, SHARD)
    docs = [_doc(), _doc(LATER), _doc(OTHER)]
    missing_later = _doc(LATER.replace("_20260709T", "_20260710T"))   # not in CMR, but its version is
    with JsonlGzWriter(path) as w:
        for doc in docs + [missing_later]:
            w.write(doc)
    mark_done(path, {"docs": 4})
    session = FakeCmr(_cmr_granules())
    monkeypatch.setattr(cmr.requests, "Session", lambda: session)
    summary = cmr.run_cmr(SPEC, run, _options(source="build", shard_filter=SHARD))
    assert summary["failed"] == 0 and summary["shards"] == 1
    assert {k: summary[k] for k in ("docs", "found", "missing", "recent", "changed", "cmr_items", "cmr_only")} == \
        {"docs": 4, "found": 2, "missing": 2, "recent": 0, "changed": 4, "cmr_items": 3, "cmr_only": 1}
    assert summary["missing_other_version_in_cmr"] == 1 and summary["cmr_only_by_version"] == {"unparsed": 1}
    rewritten = {d["id"]: d for d in read_jsonl_gz(path)}
    assert rewritten[GID]["daac_catalog_id"] == "G1-POCLOUD"
    assert rewritten[LATER]["catalog"]["cmr"]["concept_id"] == "G2-POCLOUD"
    assert "concept_id" not in rewritten[OTHER]["catalog"]["cmr"]
    assert read_done(path) == {"docs": 4}
    lines = run.cmr_missing(SPEC.dataset.value, "build").read_text().splitlines()
    assert lines == sorted([f"{OTHER}\tno_other_version", f"{missing_later['id']}\tother_version_in_cmr"])
    calls = len(session.calls)
    again = cmr.run_cmr(SPEC, run, _options(source="build", shard_filter=SHARD))
    assert again["skipped"] == 1 and again["found"] == 2 and len(session.calls) == calls
    forced = cmr.run_cmr(SPEC, run, _options(source="build", shard_filter=SHARD, force=True))
    assert forced["changed"] == 0 and len(session.calls) == calls     # the shard's CMR listing is reused


def test_check_refuses_an_unbuilt_shard(tmp_path, monkeypatch):
    monkeypatch.setattr(cmr.requests, "Session", lambda: FakeCmr(_cmr_granules()))
    summary = cmr.run_cmr(SPEC, RunDir(tmp_path, RUN_ID), _options(source="build", shard_filter=SHARD))
    assert summary["failed"] == 1 and "no finished build output" in summary["errors"][0]


def test_check_updates_only_changed_stored_documents(tmp_path, monkeypatch):
    run = RunDir(tmp_path, RUN_ID)
    unchanged = _doc(LATER)
    cmr.set_cmr(unchanged, _granule(LATER, "G2-POCLOUD"), "2026-10-20T21:00:00Z")
    stored = [_doc(), unchanged, _doc(OTHER)]
    queries = []

    def fake_scan(client, index, query=None, **kw):
        queries.append((index, query))
        return iter({"_source": json.loads(json.dumps(d)), "_index": "catalog_v1.1_l3_dswx_hls-2026.07",
                     "_id": d["id"]} for d in stored)

    loaded = []

    def fake_bulk_load(client, actions, **kw):
        loaded.extend(actions)
        return store.BulkResult(ok=len(loaded), indices={"catalog_v1.1_l3_dswx_hls-2026.07": len(loaded)})

    monkeypatch.setattr(store, "scan", fake_scan)
    monkeypatch.setattr(store, "bulk_load", fake_bulk_load)
    monkeypatch.setattr(store, "refresh", lambda client, indices: None)
    monkeypatch.setattr(cmr.requests, "Session", lambda: FakeCmr(_cmr_granules()))
    summary = cmr.run_cmr(SPEC, run, _options(shard_filter=SHARD), client=object())
    assert {k: summary[k] for k in ("docs", "found", "missing", "changed")} == \
        {"docs": 3, "found": 2, "missing": 1, "changed": 2}
    assert queries[0][0] == "catalog_*_l3_dswx_hls-*"
    assert {"prefix": {"id.keyword": PREFIX}} in queries[0][1]["bool"]["filter"]
    assert sorted(a["_id"] for a in loaded) == sorted([GID, OTHER])
    assert all(a["_op_type"] == "index" and a["_index"] == "catalog_v1.1_l3_dswx_hls-2026.07" for a in loaded)
    stamped = datetime.strptime(loaded[0]["_source"]["catalog"]["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
    assert stamped.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc) - timedelta(minutes=5)
