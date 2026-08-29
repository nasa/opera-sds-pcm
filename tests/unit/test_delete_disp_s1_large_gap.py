"""Unit tests for tools/delete_disp_s1_large_gap.py.

The fixtures mirror frame 24726 on ops-pop1: 86 pre-gap sensing dates (so the
last complete k-cycle ends at index 74), a gap of about 3.5 years, then a
resumed series.  The compressed CSLCs form five clean boundaries plus one whose
window spans the gap, which is exactly the set the purge has to remove.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools import delete_disp_s1_large_gap as mod  # noqa: E402


# ---------------------------------------------------------------------------
# fixture data
# ---------------------------------------------------------------------------

K = 15
PRE_GAP_COUNT = 86           # -> h01 = 75, boundary at index 74
POST_GAP_COUNT = 23
# chosen so the last pre-gap date lands on 2021-12-16 and the gap is 1259 days,
# the same shape frame 24726 has on ops-pop1
PRE_GAP_START = datetime(2021, 12, 16) - timedelta(days=12 * (PRE_GAP_COUNT - 1))
POST_GAP_START = datetime(2025, 5, 29)
EARLY = datetime(2016, 7, 9)  # for series built independently of the fixture frame
BURSTS = ["T093-197801-IW1", "T093-197801-IW2"]
FRAME = 24726


def _dates() -> list[datetime]:
    pre = [PRE_GAP_START + timedelta(days=12 * i) for i in range(PRE_GAP_COUNT)]
    post = [POST_GAP_START + timedelta(days=12 * i) for i in range(POST_GAP_COUNT)]
    return pre + post


DATES = _dates()
GAP_START = DATES[PRE_GAP_COUNT - 1].strftime("%Y%m%d")     # last pre-gap date
BOUNDARY = DATES[74].strftime("%Y%m%d")                     # end of the 5th k-cycle
FIRST_POST_GAP = DATES[PRE_GAP_COUNT].strftime("%Y%m%d")


def make_cbdb(frame_id=FRAME, dates=None, n_bursts=len(BURSTS)) -> dict:
    dates = DATES if dates is None else dates
    return {
        str(frame_id): {
            "burst_id_list": [f"B{i}" for i in range(n_bursts)],
            "sensing_time_list": [d.strftime("%Y-%m-%dT%H:%M:%S") for d in dates],
        }
    }


def ccslc_id(burst, first, last, creation="20250409T071831Z", frame=FRAME):
    return (
        f"OPERA_L2_COMPRESSED-CSLC-S1_F{frame:05d}_{burst}_"
        f"{last}T000000Z_{first}T000000Z_{last}T000000Z_{creation}_VV_v1.0"
    )


def l3_id(ref, sec, creation="20250408T163934Z", frame=FRAME):
    return (
        f"OPERA_L3_DISP-S1_IW_F{frame:05d}_VV_{ref}T013316Z_{sec}T013234Z_"
        f"v1.0_{creation}"
    )


def clean_boundary_ccslcs() -> list[str]:
    """Two copies (different creation times) per burst per clean boundary."""
    out = []
    for cycle in range(5):
        last = DATES[K * (cycle + 1) - 1].strftime("%Y%m%d")
        first = DATES[K * cycle].strftime("%Y%m%d")
        for burst in BURSTS:
            out.append(ccslc_id(burst, first, last, "20250319T131230Z"))
            out.append(ccslc_id(burst, first, last, "20250409T071831Z"))
    return out


def gap_spanning_ccslcs() -> list[str]:
    """The ministack that phase-linked across the gap."""
    first = DATES[75].strftime("%Y%m%d")
    last = DATES[PRE_GAP_COUNT + 3].strftime("%Y%m%d")
    return [ccslc_id(burst, first, last) for burst in BURSTS]


def kept_l3s() -> list[str]:
    ref = DATES[0].strftime("%Y%m%d")
    return [l3_id(ref, DATES[i].strftime("%Y%m%d")) for i in range(1, 75)]


def affected_l3s() -> list[str]:
    ref = DATES[0].strftime("%Y%m%d")
    return [
        l3_id(ref, DATES[i].strftime("%Y%m%d"))
        for i in range(75, PRE_GAP_COUNT + 4)
    ]


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------

class FakeES:
    """Minimal OpenSearch stand-in that records every mutating call."""

    def __init__(self, ccslc=None, l3=None, csc=None):
        # granule -> [index, ...]
        self.ccslc = ccslc or {}
        self.l3 = l3 or {}
        self.csc = csc or {}
        self.deleted: list[tuple[str, list[str]]] = []
        self.reindexed: list[tuple[str, str]] = []

    # -- read ------------------------------------------------------------
    def info(self):
        return {"version": {"number": "2.9.0"}}

    def _hits(self, index, body):
        query = body.get("query", {})
        hits = []
        if "prefix" in query:
            prefix = query["prefix"]["id.keyword"]
            source = self.ccslc if "compressed" in index else self.l3
            for granule, indices in sorted(source.items()):
                if granule.startswith(prefix):
                    for idx in indices:
                        hits.append({"_id": granule, "_index": idx})
        elif "term" in query and "metadata.frame_id" in query["term"]:
            frame_id = query["term"]["metadata.frame_id"]
            for doc_id, info in sorted(self.csc.items()):
                if info["frame_id"] == frame_id:
                    hits.append(
                        {
                            "_id": doc_id,
                            "_index": info["index"],
                            "_source": {"metadata": {"sensing_date": info["sensing_date"]}},
                        }
                    )
        elif "terms" in query and "_id" in query["terms"]:
            wanted = set(query["terms"]["_id"])
            for source in (self.ccslc, self.l3):
                for granule, indices in sorted(source.items()):
                    if granule in wanted and index in indices:
                        hits.append({"_id": granule, "_index": index, "_source": {"id": granule}})
        return hits

    def search(self, index=None, body=None, scroll=None, ignore_unavailable=False):
        return {"_scroll_id": "scroll-1", "hits": {"hits": self._hits(index, body or {})}}

    def scroll(self, scroll_id=None, scroll=None):
        return {"_scroll_id": scroll_id, "hits": {"hits": []}}

    def clear_scroll(self, scroll_id=None):
        pass

    # -- write -----------------------------------------------------------
    def delete_by_query(self, index=None, body=None, **kwargs):
        ids = body["query"]["terms"]["_id"]
        self.deleted.append((index, list(ids)))
        removed = 0
        for source in (self.ccslc, self.l3, self.csc):
            for doc_id in list(ids):
                entry = source.get(doc_id)
                if entry is None:
                    continue
                indices = entry if isinstance(entry, list) else [entry["index"]]
                if index in indices:
                    removed += 1
                    if isinstance(entry, list):
                        entry.remove(index)
                        if not entry:
                            del source[doc_id]
                    else:
                        del source[doc_id]
        return {"deleted": removed, "failures": []}

    def reindex(self, body=None, **kwargs):
        src = body["source"]["index"]
        dest = body["dest"]["index"]
        n = len(body["source"]["query"]["terms"]["_id"])
        self.reindexed.append((src, dest))
        return {"created": n, "updated": 0}


class FakePaginator:
    def __init__(self, store):
        self.store = store

    def paginate(self, Bucket=None, Prefix=None, Delimiter=None):
        keys = [k for k in self.store.get(Bucket, []) if k.startswith(Prefix or "")]
        if Delimiter:
            prefixes = set()
            for key in keys:
                rest = key[len(Prefix):]
                if Delimiter in rest:
                    prefixes.add(Prefix + rest.split(Delimiter)[0] + Delimiter)
            yield {"CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)]}
        else:
            yield {"Contents": [{"Key": k, "Size": 10} for k in sorted(keys)]}


class FakeS3:
    def __init__(self, store=None):
        # bucket -> [key, ...]
        self.store = store or {}
        self.deleted: list[tuple[str, str]] = []

    def get_paginator(self, _name):
        return FakePaginator(self.store)

    def delete_objects(self, Bucket=None, Delete=None):
        for obj in Delete["Objects"]:
            self.deleted.append((Bucket, obj["Key"]))
            if obj["Key"] in self.store.get(Bucket, []):
                self.store[Bucket].remove(obj["Key"])
        return {}


LTS = "opera-ops-lts-pop1"
RS = "opera-ops-rs-pop1"


def build_world(*, drop_from_s3=(), drop_from_grq=(), extra_s3=()):
    """Build a matching GRQ + S3 + CMR world for the fixture frame."""
    ccslc_granules = clean_boundary_ccslcs() + gap_spanning_ccslcs()
    l3_granules = kept_l3s() + affected_l3s()

    es_ccslc = {
        g: ["grq_1_l2_cslc_s1_compressed-2025.04"]
        for g in ccslc_granules
        if g not in drop_from_grq
    }
    es_l3 = {
        g: ["grq_v1.0_l3_disp_s1-2026.06"] for g in l3_granules if g not in drop_from_grq
    }

    s3_keys = {LTS: [], RS: []}
    for g in ccslc_granules:
        if g in drop_from_s3:
            continue
        s3_keys[LTS].append(f"{mod.CCSLC_S3_ROOT}/{g}/{g}.h5")
    for g in l3_granules:
        if g in drop_from_s3:
            continue
        s3_keys[RS].append(f"{mod.L3_S3_ROOT}/F{FRAME:05d}/{g}/{g}.nc")
    for bucket, key in extra_s3:
        s3_keys[bucket].append(key)

    cmr = {
        g: {"concept_id": f"G{i}-ASF", "size_bytes": 385875968, "end_time": None}
        for i, g in enumerate(l3_granules)
    }
    return FakeES(ccslc=es_ccslc, l3=es_l3), FakeS3(s3_keys), cmr


def run_audit_frame(es, s3, cmr, *, cbdb=None, include_state=False, frame_in=None):
    return mod.audit_frame(
        frame_in or mod.FrameInput(frame_id=FRAME),
        priority="prior0",
        cbdb=make_cbdb() if cbdb is None else cbdb,
        gap_days=730,
        k=K,
        es=es,
        s3_client=s3,
        lts_bucket=LTS,
        rs_bucket=RS,
        use_cmr=False,
        cmr_endpoint="OPS",
        include_state=include_state,
    )


# ---------------------------------------------------------------------------
# burst-database gap math
# ---------------------------------------------------------------------------

class TestGapMath:
    def test_finds_gap_and_k_aligned_block(self):
        out = mod.cbdb_gap_analysis(make_cbdb(), FRAME, 730, K)
        assert out["in_cbdb"] is True
        assert out["gap_count"] == 1
        assert out["pregap_len"] == PRE_GAP_COUNT
        assert out["h01_len"] == 75
        assert out["gap_start_date"] == GAP_START
        assert out["gap_end_date"] == FIRST_POST_GAP
        assert out["cbdb_boundary_date"] == BOUNDARY

    def test_pregap_exactly_k_aligned_puts_boundary_at_gap_start(self):
        dates = [EARLY + timedelta(days=12 * i) for i in range(135)]
        dates += [POST_GAP_START + timedelta(days=12 * i) for i in range(11)]
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 730, K)
        assert out["pregap_len"] == 135
        assert out["h01_len"] == 135
        assert out["cbdb_boundary_date"] == out["gap_start_date"]

    def test_gap_just_under_threshold_is_not_a_large_gap(self):
        dates = [EARLY, EARLY + timedelta(days=730)]
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 730, K)
        assert out["gap_count"] == 0
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 547, K)
        assert out["gap_count"] == 1

    def test_short_pregap_has_no_clean_boundary(self):
        dates = [EARLY + timedelta(days=12 * i) for i in range(4)]
        dates += [POST_GAP_START + timedelta(days=12 * i) for i in range(30)]
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 730, K)
        assert out["h01_len"] == 0
        assert out["cbdb_boundary_date"] is None

    def test_first_gap_wins_when_there_are_several(self):
        dates = [EARLY + timedelta(days=12 * i) for i in range(30)]
        dates += [datetime(2020, 1, 1) + timedelta(days=12 * i) for i in range(20)]
        dates += [datetime(2025, 6, 1) + timedelta(days=12 * i) for i in range(20)]
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 730, K)
        assert out["gap_count"] == 2
        assert out["gap_start_date"] == dates[29].strftime("%Y%m%d")
        assert out["pregap_len"] == 30

    def test_no_gap_reports_nothing_to_purge(self):
        dates = [EARLY + timedelta(days=12 * i) for i in range(200)]
        out = mod.cbdb_gap_analysis(make_cbdb(dates=dates), FRAME, 730, K)
        assert out["gap_count"] == 0
        assert "gap_start_date" not in out

    def test_frame_missing_from_burst_db(self):
        assert mod.cbdb_gap_analysis({}, FRAME, 730, K) == {"in_cbdb": False}


# ---------------------------------------------------------------------------
# granule id parsing
# ---------------------------------------------------------------------------

class TestParsing:
    def test_ccslc_roundtrip(self):
        granule = ccslc_id("T093-197801-IW1", "20210818", "20250704")
        meta = mod.parse_ccslc_id(granule)
        assert meta["frame_id"] == FRAME
        assert meta["burst"] == "T093-197801-IW1"
        assert meta["first_date"] == "20210818"
        assert meta["last_date"] == "20250704"

    def test_ccslc_accepts_dual_polarization_and_h5_suffix(self):
        base = ccslc_id("T093-197801-IW1", "20210818", "20250704").replace("_VV_", "_VV+VH_")
        assert mod.parse_ccslc_id(base) is not None
        assert mod.parse_ccslc_id(base + ".h5") is not None

    def test_l3_roundtrip(self):
        meta = mod.parse_l3_id(l3_id("20160709", "20250704"))
        assert meta["frame_id"] == FRAME
        assert meta["ref_date"] == "20160709"
        assert meta["sec_date"] == "20250704"

    @pytest.mark.parametrize(
        "granule",
        [
            "not-a-granule",
            "OPERA_L3_DISP-S1_IW_F24726_VV_20160709T013316Z_v1.0_20250408T163934Z",
            "OPERA_L2_COMPRESSED-CSLC-S1_F24726_T093-197801-IW1_20210818T000000Z",
            "OPERA_L3_DISP-S1-STATIC_F24726_VV_v1.0",
        ],
    )
    def test_rejects_malformed(self, granule):
        assert mod.parse_ccslc_id(granule) is None
        assert mod.parse_l3_id(granule) is None


# ---------------------------------------------------------------------------
# boundary derivation
# ---------------------------------------------------------------------------

class TestBoundary:
    def test_prefers_ccslc_evidence_over_burst_db(self):
        # processing ran on an older database, so the real boundary is earlier
        boundary, source = mod.derive_boundary(
            ["20200120", "20210806"], gap_start_date="20211216", cbdb_boundary="20211130"
        )
        assert boundary == "20210806"
        assert source == "ccslc-evidence"

    def test_falls_back_to_burst_db_without_ccslcs(self):
        boundary, source = mod.derive_boundary([], "20211216", "20210806")
        assert (boundary, source) == ("20210806", "burst-db")

    def test_ignores_ccslcs_after_the_gap(self):
        boundary, source = mod.derive_boundary(
            ["20210806", "20250704"], "20211216", None
        )
        assert boundary == "20210806"
        assert source == "ccslc-evidence"

    def test_no_evidence_and_no_burst_db_means_no_boundary(self):
        assert mod.derive_boundary([], "20211216", None) == (None, "none")

    def test_no_gap_means_no_boundary_from_evidence(self):
        boundary, source = mod.derive_boundary(["20210806"], None, None)
        assert (boundary, source) == (None, "none")


# ---------------------------------------------------------------------------
# classification
# ---------------------------------------------------------------------------

class TestClassification:
    def test_only_gap_spanning_ccslcs_are_affected(self):
        es, s3, cmr = build_world()
        audit, ccslc_rows, l3_rows, _ = run_audit_frame(es, s3, cmr)

        assert audit.last_clean_boundary_date == BOUNDARY
        assert audit.boundary_source == "ccslc-evidence"
        assert len(ccslc_rows) == len(gap_spanning_ccslcs())
        assert {r["granule"] for r in ccslc_rows} == set(gap_spanning_ccslcs())
        # the five clean boundaries, two copies each, survive
        assert audit.counts["ccslc_keep"] == len(clean_boundary_ccslcs())

    def test_products_after_the_boundary_are_affected(self):
        es, s3, cmr = build_world()
        _, _, l3_rows, _ = run_audit_frame(es, s3, cmr)
        assert {r["granule"] for r in l3_rows} == set(affected_l3s())
        for row in l3_rows:
            assert row["sec_date"] > BOUNDARY

    def test_kept_products_never_appear_in_a_manifest(self):
        es, s3, cmr = build_world()
        _, ccslc_rows, l3_rows, _ = run_audit_frame(es, s3, cmr)
        manifested = {r["granule"] for r in ccslc_rows} | {r["granule"] for r in l3_rows}
        assert manifested.isdisjoint(set(kept_l3s()))
        assert manifested.isdisjoint(set(clean_boundary_ccslcs()))

    def test_frame_without_clean_boundary_purges_everything(self):
        dates = [PRE_GAP_START + timedelta(days=12 * i) for i in range(4)]
        dates += [POST_GAP_START + timedelta(days=12 * i) for i in range(30)]
        es, s3, cmr = build_world()
        audit, ccslc_rows, l3_rows, _ = run_audit_frame(
            es, s3, cmr, cbdb=make_cbdb(dates=dates)
        )
        assert audit.last_clean_boundary_date is None
        assert "no-clean-boundary-every-product-affected" in audit.anomalies
        assert len(l3_rows) == len(kept_l3s()) + len(affected_l3s())

    def test_frame_missing_from_burst_db_uses_the_reported_gap(self):
        es, s3, cmr = build_world()
        frame_in = mod.FrameInput(
            frame_id=FRAME,
            reported_gaps=[
                {
                    "start": DATES[PRE_GAP_COUNT - 1].strftime("%Y-%m-%dT01:33:07"),
                    "end": DATES[PRE_GAP_COUNT].strftime("%Y-%m-%dT01:32:39"),
                    "days": 1259,
                }
            ],
        )
        audit, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr, cbdb={}, frame_in=frame_in)
        assert "not-in-burst-db" in audit.anomalies
        assert "gap-from-input-list" in audit.anomalies
        assert audit.gap_start_date == GAP_START
        assert {r["granule"] for r in ccslc_rows} == set(gap_spanning_ccslcs())

    def test_frame_with_nothing_left_is_a_no_op(self):
        es, s3 = FakeES(), FakeS3({LTS: [], RS: []})
        audit, ccslc_rows, l3_rows, _ = run_audit_frame(es, s3, {})
        assert (ccslc_rows, l3_rows) == ([], [])
        assert audit.counts["ccslc_affected"] == 0
        assert audit.boundary_source == "burst-db"

    def test_state_configs_after_the_boundary_are_collected(self):
        es, s3, cmr = build_world()
        kept = DATES[74].strftime("%Y%m%d")          # exactly on the boundary
        affected = DATES[PRE_GAP_COUNT].strftime("%Y%m%d")   # first post-gap date
        index = "grq_1_cslc_s1-cycle-state-config-2026.07"
        es.csc = {
            f"cslc_s1-cycle-f{FRAME}-{kept}-state-config": {
                "frame_id": FRAME, "index": index, "sensing_date": kept,
            },
            f"cslc_s1-cycle-f{FRAME}-{affected}-state-config": {
                "frame_id": FRAME, "index": index, "sensing_date": affected,
            },
        }
        _, _, _, csc_rows = run_audit_frame(es, s3, cmr, include_state=True)
        assert [r["sensing_date"] for r in csc_rows] == [affected]


# ---------------------------------------------------------------------------
# reconciling GRQ against S3
# ---------------------------------------------------------------------------

class TestReconciliation:
    def test_s3_only_granule_is_still_selected(self):
        target = gap_spanning_ccslcs()[0]
        es, s3, cmr = build_world(drop_from_grq=(target,))
        audit, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr)
        row = next(r for r in ccslc_rows if r["granule"] == target)
        assert row["es_indices"] == []
        assert row["in_s3"] is True
        assert any(a.startswith("ccslc-in-s3-not-grq") for a in audit.anomalies)

    def test_grq_only_granule_is_still_selected(self):
        target = gap_spanning_ccslcs()[0]
        es, s3, cmr = build_world(drop_from_s3=(target,))
        audit, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr)
        row = next(r for r in ccslc_rows if r["granule"] == target)
        assert row["es_indices"] != []
        assert row["in_s3"] is False
        assert any(a.startswith("ccslc-in-grq-not-s3") for a in audit.anomalies)

    def test_duplicate_windows_are_reported(self):
        es, s3, cmr = build_world()
        audit, _, _, _ = run_audit_frame(es, s3, cmr)
        # five boundaries x two bursts, each with two creation times
        assert "duplicate-ccslc-windows:10" in audit.anomalies

    def test_same_granule_in_two_indices_is_deleted_from_both(self):
        es, s3, cmr = build_world()
        target = gap_spanning_ccslcs()[0]
        es.ccslc[target] = [
            "grq_1_l2_cslc_s1_compressed-2025.04",
            "grq_1_l2_cslc_s1_compressed-2025.07",
        ]
        _, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr)
        row = next(r for r in ccslc_rows if r["granule"] == target)
        assert len(row["es_indices"]) == 2
        by_index = mod._ids_by_index(ccslc_rows, "granule")
        assert target in by_index["grq_1_l2_cslc_s1_compressed-2025.04"]
        assert target in by_index["grq_1_l2_cslc_s1_compressed-2025.07"]

    def test_unparseable_granule_is_reported_not_deleted(self):
        # a truncated dataset directory that still sits under the frame prefix
        junk_dataset = f"OPERA_L2_COMPRESSED-CSLC-S1_F{FRAME:05d}_TRUNCATED"
        junk = f"{mod.CCSLC_S3_ROOT}/{junk_dataset}/file.h5"
        es, s3, cmr = build_world(extra_s3=((LTS, junk),))
        audit, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr)
        assert f"unparseable-ccslc-id:{junk_dataset}" in audit.anomalies
        assert all("TRUNCATED" not in r["granule"] for r in ccslc_rows)

    def test_granule_from_another_frame_is_never_selected(self):
        # the frame-scoped prefix must not pick up a neighbour frame's datasets
        other = f"{mod.CCSLC_S3_ROOT}/OPERA_L2_COMPRESSED-CSLC-S1_F24727_T093-197801-IW1_"
        es, s3, cmr = build_world(extra_s3=((LTS, other + "x/y.h5"),))
        _, ccslc_rows, _, _ = run_audit_frame(es, s3, cmr)
        assert all(r["frame_id"] == FRAME for r in ccslc_rows)
        assert all("F24727" not in r["granule"] for r in ccslc_rows)


# ---------------------------------------------------------------------------
# end to end through the CLI
# ---------------------------------------------------------------------------

@pytest.fixture
def wired(monkeypatch, tmp_path):
    """audit -> execute -> asf-list wired up against the fakes."""
    es, s3, cmr = build_world()
    monkeypatch.setattr(mod, "build_opensearch_client", lambda args, cfg: es)
    monkeypatch.setattr(mod.boto3, "client", lambda *a, **k: s3)
    monkeypatch.setattr(mod, "cmr_l3_for_frame", lambda frame_id, endpoint="OPS": cmr)
    monkeypatch.setattr(
        mod, "load_sds_config", lambda path: {"LTS_BUCKET": LTS, "DATASET_BUCKET": RS}
    )
    cbdb_path = tmp_path / "cbdb.json"
    cbdb_path.write_text(json.dumps({"data": make_cbdb()}))
    gap_list = tmp_path / "prior0_large_gap.txt"
    gap_list.write_text(
        f"FRAME {FRAME}\n  GAP: 2021-12-16T01:33:07 -> 2025-05-29T01:32:39 (1259 days)\n"
    )
    run_dir = tmp_path / "run"
    return {
        "es": es, "s3": s3, "cmr": cmr, "run_dir": run_dir,
        "cbdb": cbdb_path, "gap_list": gap_list,
    }


def _audit_argv(w):
    return [
        "audit",
        "--gap-list", str(w["gap_list"]),
        "--cbdb", str(w["cbdb"]),
        "--run-dir", str(w["run_dir"]),
    ]


class TestEndToEnd:
    def test_audit_writes_manifests_and_summary(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        run_dir = wired["run_dir"]
        audit = json.loads((run_dir / "audit_prior0.json").read_text())
        assert audit["label"] == "prior0"
        assert audit["summary"]["frames"] == 1
        ccslc_rows = mod.read_jsonl(run_dir / "manifest_ccslc_prior0.jsonl")
        l3_rows = mod.read_jsonl(run_dir / "manifest_l3_prior0.jsonl")
        assert len(ccslc_rows) == len(gap_spanning_ccslcs())
        assert len(l3_rows) == len(affected_l3s())
        assert (run_dir / "summary_prior0.md").exists()
        # audit is read-only
        assert es_untouched(wired)

    def test_dry_run_execute_changes_nothing(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        before_es = dict(wired["es"].ccslc)
        before_s3 = list(wired["s3"].store[LTS])

        rc = mod.main(
            ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc", "--l3", "--yes"]
        )
        assert rc == 0
        assert es_untouched(wired)
        assert wired["es"].ccslc == before_es
        assert wired["s3"].store[LTS] == before_s3

    def test_explicit_dry_run_flag_is_accepted(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc", "--dry-run", "--yes"]
        )
        assert rc == 0
        assert es_untouched(wired)

    def test_dry_run_and_execute_together_is_rejected(self, wired):
        with pytest.raises(SystemExit):
            mod.parse_args(
                ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc",
                 "--dry-run", "--execute"]
            )

    def test_execute_deletes_only_the_manifest_rows(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            [
                "execute", "--run-dir", str(wired["run_dir"]),
                "--ccslc", "--l3", "--execute", "--yes",
            ]
        )
        assert rc == 0
        es, s3 = wired["es"], wired["s3"]
        # every gap-spanning CCSLC is gone, every clean one survives
        for granule in gap_spanning_ccslcs():
            assert granule not in es.ccslc
        for granule in clean_boundary_ccslcs():
            assert granule in es.ccslc
        for granule in affected_l3s():
            assert granule not in es.l3
        for granule in kept_l3s():
            assert granule in es.l3
        surviving = s3.store[LTS]
        assert all(
            not any(g in key for g in gap_spanning_ccslcs()) for key in surviving
        )
        assert len(surviving) == len(clean_boundary_ccslcs())

    def test_execute_backs_up_and_parks_before_deleting(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        mod.main(
            ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc", "--execute", "--yes"]
        )
        backups = list(wired["run_dir"].glob("backup_prior0_ccslc_*.ndjson"))
        assert len(backups) == 1
        assert backups[0].read_text().strip()
        assert any(
            dest == f"{mod.PARKED_PREFIX}_ccslc" for _, dest in wired["es"].reindexed
        )

    def test_execute_is_idempotent(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        argv = ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc", "--execute", "--yes"]
        assert mod.main(argv) == 0
        assert mod.main(argv) == 0  # nothing left, still succeeds

    def test_hard_cap_blocks_a_large_manifest(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            [
                "execute", "--run-dir", str(wired["run_dir"]), "--ccslc",
                "--execute", "--yes", "--max", "1",
            ]
        )
        assert rc == 2
        assert es_untouched(wired)

    def test_force_overrides_the_hard_cap(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            [
                "execute", "--run-dir", str(wired["run_dir"]), "--ccslc",
                "--execute", "--yes", "--max", "1", "--force",
            ]
        )
        assert rc == 0
        assert wired["es"].deleted

    def test_typed_confirmation_is_required_without_yes(self, wired, monkeypatch):
        assert mod.main(_audit_argv(wired)) == 0
        monkeypatch.setattr("builtins.input", lambda *_: "no thanks")
        rc = mod.main(
            ["execute", "--run-dir", str(wired["run_dir"]), "--ccslc", "--execute"]
        )
        assert rc == 1
        assert es_untouched(wired)

    def test_execute_can_be_restricted_to_one_frame(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            [
                "execute", "--run-dir", str(wired["run_dir"]), "--ccslc",
                "--execute", "--yes", "--frames", "99999",
            ]
        )
        assert rc == 0
        assert es_untouched(wired)

    def test_asf_list_holds_only_affected_cmr_granules(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        assert mod.main(["asf-list", "--run-dir", str(wired["run_dir"])]) == 0
        csv_text = (wired["run_dir"] / "asf_removal_prior0.csv").read_text()
        lines = csv_text.strip().splitlines()
        assert lines[0].startswith("granule_ur,")
        assert len(lines) - 1 == len(affected_l3s())
        for granule in affected_l3s():
            assert granule in csv_text
        for granule in kept_l3s():
            assert granule not in csv_text

    def test_verify_passes_after_a_real_execute(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        mod.main(
            [
                "execute", "--run-dir", str(wired["run_dir"]),
                "--ccslc", "--l3", "--execute", "--yes",
            ]
        )
        rc = mod.main(
            ["verify", "--run-dir", str(wired["run_dir"]), "--cbdb", str(wired["cbdb"])]
        )
        assert rc == 0
        report = (wired["run_dir"] / "verify_prior0.md").read_text()
        assert "1/1 frames pass" in report

    def test_verify_fails_when_the_purge_did_not_happen(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        rc = mod.main(
            ["verify", "--run-dir", str(wired["run_dir"]), "--cbdb", str(wired["cbdb"])]
        )
        assert rc == 1
        assert "FAIL" in (wired["run_dir"] / "verify_prior0.md").read_text()

    def test_label_is_inferred_from_the_run_directory(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        args = mod.parse_args(["asf-list", "--run-dir", str(wired["run_dir"])])
        assert args.priority_label == "prior0"

    def test_ambiguous_label_is_an_error(self, wired):
        assert mod.main(_audit_argv(wired)) == 0
        (wired["run_dir"] / "audit_prior9.json").write_text("{}")
        with pytest.raises(SystemExit):
            mod.parse_args(["asf-list", "--run-dir", str(wired["run_dir"])])


def es_untouched(wired) -> bool:
    return not wired["es"].deleted and not wired["s3"].deleted


# ---------------------------------------------------------------------------
# input parsing
# ---------------------------------------------------------------------------

class TestFrameInput:
    def test_reads_the_large_gap_report(self, tmp_path):
        path = tmp_path / "prior0_large_gap.txt"
        path.write_text(
            "FRAME 24726\n"
            "  GAP: 2021-12-16T01:33:07 -> 2025-05-29T01:32:39 (1259 days)\n"
            "\n"
            "FRAME 44325\n"
            "  GAP: 2021-12-21T01:20:00 -> 2025-01-28T01:19:00 (1134 days)\n"
            "  GAP: 2016-01-01T00:00:00 -> 2018-01-01T00:00:00 (731 days)\n"
        )
        frames = mod.read_gap_list(path)
        assert [f.frame_id for f in frames] == [24726, 44325]
        assert frames[0].reported_gaps[0]["days"] == 1259
        assert len(frames[1].reported_gaps) == 2

    def test_reads_a_frames_json(self, tmp_path):
        path = tmp_path / "priority0_frames.json"
        path.write_text(json.dumps({"frames": [24726, 44325]}))
        assert [f.frame_id for f in mod.read_frames_file(path)] == [24726, 44325]

    def test_accepts_the_frame_singular_key(self, tmp_path):
        path = tmp_path / "priority4_frames.json"
        path.write_text(json.dumps({"frame": [11109]}))
        assert [f.frame_id for f in mod.read_frames_file(path)] == [11109]

    def test_duplicate_frames_collapse(self, tmp_path):
        path = tmp_path / "prior0_large_gap.txt"
        path.write_text("FRAME 24726\nFRAME 24726\n")

        class Args:
            gap_list = path
            frames_file = None
            frames = None

        assert [f.frame_id for f in mod.resolve_frame_inputs(Args())] == [24726]


# ---------------------------------------------------------------------------
# safety rails
# ---------------------------------------------------------------------------

class TestSafety:
    @pytest.mark.parametrize("prefix", ["", "products/", "products/DISP_S1"])
    def test_refuses_dangerous_s3_prefixes(self, prefix):
        s3 = FakeS3({RS: ["products/DISP_S1/F24726/x/y.nc"]})
        deleted, errors = mod.delete_s3_prefix(s3, RS, prefix, dry_run=False)
        assert (deleted, errors) == (0, 1)
        assert s3.deleted == []

    def test_deletes_a_well_formed_dataset_prefix(self):
        key = "products/DISP_S1/F24726/granule/file.nc"
        s3 = FakeS3({RS: [key]})
        deleted, errors = mod.delete_s3_prefix(
            s3, RS, "products/DISP_S1/F24726/granule/", dry_run=False
        )
        assert (deleted, errors) == (1, 0)
        assert s3.deleted == [(RS, key)]

    def test_s3_dry_run_reports_without_deleting(self):
        key = "products/DISP_S1/F24726/granule/file.nc"
        s3 = FakeS3({RS: [key]})
        deleted, errors = mod.delete_s3_prefix(
            s3, RS, "products/DISP_S1/F24726/granule/", dry_run=True
        )
        assert (deleted, errors) == (1, 0)
        assert s3.deleted == []

    def test_es_dry_run_reports_without_deleting(self):
        es = FakeES(ccslc={"g": ["i"]})
        planned, errors = mod.delete_es_docs(es, {"i": ["g"]}, dry_run=True)
        assert (planned, errors) == (1, 0)
        assert es.deleted == []
