"""The DISP-S1 forward ledger: one outcome per sensing date, every L3 tied to a date and checked
in GRQ, S3 and delivery, and a frame state with the next action an operator should take.

Classification is a pure function of the date's state configs, products and SCIFLO runs, so
it is tested directly; account() and main() are driven with in-memory stand-ins for GRQ,
Mozart, S3 and CMR.
"""

import csv
import json
from datetime import datetime, timezone

import pytest

import tools.disp_s1_forward_ledger as ledger
from data_subscriber.cslc import disp_s1_constants as c

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
OLD = "2026-10-13T10:00:00.000"          # well before NOW
YOUNG = "2026-10-14T11:55:00.000"        # five minutes before NOW
FRAME = 9154
BURSTS = ["T035-073251-IW1", "T035-073251-IW2", "T035-073252-IW1"]
BUCKET = "opera-dev-rs-fwd-x"


def csc(date, complete=True, **kw):
    found = BURSTS if complete else BURSTS[:1]
    meta = {c.FRAME_ID: FRAME, c.SENSING_DATE: date, c.IS_COMPLETE: complete,
            c.COVERAGE_ACTUAL: len(found), c.COVERAGE_EXPECTED: len(BURSTS),
            c.MISSING_BURST_IDS: sorted(set(BURSTS) - set(found)), "_created": OLD,
            c.COMPLETENESS_REASON: "complete" if complete else f"incomplete: {len(found)}/{len(BURSTS)} bursts"}
    meta.update(kw)
    return meta


def ksc(date, fires=True, **kw):
    meta = {"_id": f"disp_s1-kcycle-k15-m6-f{FRAME}-{date}-state-config", "_created": OLD,
            c.FRAME_ID: FRAME, c.SENSING_DATE: date, c.REGION_ID: "0",
            c.IS_COMPLETE: fires, c.COMPRESSED_CSLC_FINAL: fires, c.GAP_UNRESOLVED: False,
            c.SAVE_COMPRESSED_CSLC: False, c.CYCLES_COMPLETE: 15, c.CYCLES_EXPECTED: 15,
            c.COMPRESSED_CSLC_PENDING: [], c.COMPLETENESS_REASON: "ready"}
    meta.update(kw)
    return meta


def l3(date, ref="20260101", created=OLD, delivery="SUCCESS", forward=True, frame=FRAME):
    pid = f"OPERA_L3_DISP-S1_IW_F{frame:05d}_VV_{ref}T140841Z_{date}T140744Z_v1.0_20261013T175558Z"
    return {"id": pid, "frame": frame, "ref": ref, "sec": date,
            "input": f"disp_s1-kcycle-k15-m6-f{frame}-{date}-state-config" if forward else "hist-job-input",
            "delivery": delivery, "created": created, "bucket": BUCKET,
            "key": f"products/DISP_S1/F{frame:05d}/{pid}"}


def classify(date="20260508", csc_=None, ksc_=None, fwd=(), hist=(), jobs=(), nb=3, bursts=0, markers=0,
             whitelist=("0",)):
    return ledger.classify(date, csc_, ksc_, list(fwd), list(hist), list(jobs), nb, bursts, markers,
                           list(whitelist) if whitelist is not None else None, NOW)


def job(status, msg="", error=""):
    return {"job_id": "x", "status": status, "msg": msg, "error": error, "queued": OLD}


# -- classify ------------------------------------------------------------------------------------

def test_one_forward_l3_is_produced():
    assert classify(csc_=csc("20260508"), ksc_=ksc("20260508"), fwd=[l3("20260508")])[0] == ledger.PRODUCED


def test_a_boundary_needs_its_full_ccslc_set_and_one_marker():
    k = ksc("20260508", **{c.SAVE_COMPRESSED_CSLC: True})
    assert classify(ksc_=k, fwd=[l3("20260508")], bursts=3, markers=1)[0] == ledger.PRODUCED
    outcome, note = classify(ksc_=k, fwd=[l3("20260508")], bursts=3, markers=0)
    assert outcome == ledger.UNEXPLAINED and "0 marker" in note
    assert classify(ksc_=k, fwd=[l3("20260508")], bursts=2, markers=1,
                    jobs=[job("job-started")])[0] == ledger.IN_PROGRESS


def test_two_l3s_for_one_date_are_unexplained():
    outcome, note = classify(ksc_=ksc("20260508"), fwd=[l3("20260508"), l3("20260508", ref="20251201")])
    assert outcome == ledger.UNEXPLAINED and "duplicate" in note


def test_an_l3_from_the_historical_sciflo():
    assert classify(ksc_=ksc("20260508", **{c.SUPERSEDED_BY: c.SUPERSEDED_BY_HISTORICAL_PROCESSING}),
                    hist=[l3("20260508", forward=False)])[0] == ledger.PRODUCED_HIST


@pytest.mark.parametrize("flags, outcome", [
    ({c.BLACKOUT: True}, ledger.BLACKOUT),
    ({c.DB_EXCLUDED: True}, ledger.EXCLUDED_DB),
])
def test_dates_left_out_by_design(flags, outcome):
    assert classify(csc_=csc("20260508", complete=False, **flags))[0] == outcome


def test_a_skipped_date_carries_its_reason():
    outcome, note = classify(csc_=csc("20260508", complete=False, skipped=True, skip_reason="2 bursts not imaged",
                                      skipped_by="op", skipped_at="2026-10-15T12:00:00Z"))
    assert outcome == ledger.SKIPPED_OUT
    assert "2 bursts not imaged" in note and "op" in note


def test_a_partial_csc_with_no_ksc_stops_the_frame():
    assert classify(csc_=csc("20260508", complete=False))[0] == ledger.STOPPED


def test_a_complete_csc_with_no_ksc():
    outcome, note = classify(csc_=csc("20260508"))
    assert outcome == ledger.UNEXPLAINED and "missed k-cycle trigger" in note
    assert classify(csc_=csc("20260508", _created=YOUNG))[0] == ledger.PENDING


def test_superseded():
    outcome, note = classify(csc_=csc("20260508"), ksc_=ksc("20260508", **{c.SUPERSEDED_BY: "existing_ccslc"}))
    assert outcome == ledger.SUPERSEDED and "existing_ccslc" in note


def test_a_ksc_that_would_fire_outside_the_whitelist():
    k = ksc("20260508", **{c.REGION_ID: "3"})
    assert classify(ksc_=k)[0] == ledger.NOT_WHITELISTED
    assert classify(ksc_=k, whitelist=None)[0] == ledger.UNEXPLAINED      # no whitelist: it should have fired


@pytest.mark.parametrize("jobs, created, outcome, text", [
    ([job("job-queued")], OLD, ledger.IN_PROGRESS, "queued"),
    ([job("job-failed", error="OOM")], OLD, ledger.UNEXPLAINED, "SCIFLO failed: OOM"),
    ([job("job-completed")], OLD, ledger.UNEXPLAINED, "without publishing"),
    ([], YOUNG, ledger.IN_PROGRESS, "trigger pending"),
    ([], OLD, ledger.UNEXPLAINED, "no SCIFLO job"),
])
def test_a_fired_ksc_without_its_l3(jobs, created, outcome, text):
    got, note = classify(ksc_=ksc("20260508", _created=created), jobs=jobs)
    assert got == outcome and text in note


def test_held_behind_a_gap():
    k = ksc("20260520", fires=True, **{c.GAP_UNRESOLVED: True})
    assert classify("20260520", ksc_=k)[0] == ledger.HELD


@pytest.mark.parametrize("meta, outcome", [
    ({c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: False, c.COMPRESSED_CSLC_PENDING: ["20260502"]},
     ledger.AWAITING_CCSLC),
    ({c.IS_COMPLETE: False, c.COMPRESSED_CSLC_FINAL: False, c.CYCLES_COMPLETE: 9}, ledger.FILLING),
    ({c.IS_COMPLETE: False, c.COMPRESSED_CSLC_FINAL: False, c.COMPLETENESS_REASON: "missing ionosphere"},
     ledger.AWAITING_INPUTS),
])
def test_waiting_kscs(meta, outcome):
    assert classify(ksc_=ksc("20260508", **meta))[0] == outcome


# -- account -------------------------------------------------------------------------------------

def data(cscs=(), kscs=(), l3s=(), ccslc=None, markers=None, jobs=None):
    return {"csc": {(FRAME, x[c.SENSING_DATE]): x for x in cscs},
            "ksc": {(FRAME, x[c.SENSING_DATE]): x for x in kscs},
            "l3": list(l3s), "ccslc": ccslc or {}, "markers": markers or {}, "jobs": jobs or {}}


def run(d, s3=None, cmr=None, **kw):
    return ledger.account([FRAME], d, {FRAME: BURSTS}, ["0"], NOW, s3=s3, cmr=cmr, **kw)


def by_date(rows):
    return {r["date"]: r for r in rows}


class FakeCmr:
    def __init__(self, by_date, imaged=()):
        self.by_date, self.imaged_set = by_date, set(imaged)

    def dates(self, frame, start):
        return {d: set(b) for d, b in self.by_date.items()}, {d: {"S1D"} for d in self.by_date}

    def imaged(self, bursts, date):
        return set(bursts) & self.imaged_set


def stopped_frame():
    """Produced through 0502, partial 0508 with no KSC, then a held KSC on 0520."""
    return data(cscs=[csc("20260502"), csc("20260508", complete=False), csc("20260520")],
                kscs=[ksc("20260502"),
                      ksc("20260520", **{c.GAP_UNRESOLVED: True, c.COMPLETENESS_REASON:
                                         "ready; gap_unresolved: partial CSC(s) in lineage (after CCSLC "
                                         "last_date 20260101): 20260508 (1/3)"})],
                l3s=[l3("20260502")])


def test_a_held_ksc_names_the_partial_date_holding_it():
    rows, _, info = run(stopped_frame(), s3=None)
    r = by_date(rows)
    assert r["20260508"]["outcome"] == ledger.STOPPED
    assert r["20260520"]["outcome"] == ledger.HELD
    assert r["20260520"]["blocker"] == "20260508"
    assert info[FRAME]["state"] == "stopped 20260508"
    assert "tools/skip_disp_s1_csc.py --frame-id 9154 --date 20260508" in info[FRAME]["hint"]


def held_frame(blocker_csc, blocker_l3s=()):
    """A KSC on 0520 still gap_unresolved on 0508, whatever has happened to 0508 since."""
    return data(cscs=[csc("20260502"), blocker_csc, csc("20260520")],
                kscs=[ksc("20260502"),
                      ksc("20260520", **{c.GAP_UNRESOLVED: True, c.COMPLETENESS_REASON:
                                         "ready; gap_unresolved: partial CSC(s) in lineage (after CCSLC "
                                         "last_date 20260101): 20260508 (1/3)"})],
                l3s=[l3("20260502")] + list(blocker_l3s))


def test_a_resolved_gap_points_at_re_evaluating_the_held_kscs():
    """0508's CSLCs arrived and it was produced, but the KSC behind it is still held: skipping is the wrong
    advice, re-evaluating the held KSCs is the right one."""
    d = held_frame(csc("20260508"), [l3("20260508")])
    d["ksc"][(FRAME, "20260508")] = ksc("20260508")
    rows, _, info = run(d)
    assert by_date(rows)["20260520"]["outcome"] == ledger.HELD
    assert info[FRAME]["state"] == "held by 20260508"
    assert "the gap at 20260508 is resolved" in info[FRAME]["hint"]
    assert "--frame-id 9154 --date 20260508 --reevaluate-only --apply" in info[FRAME]["hint"]


def test_a_skipped_blocker_also_points_at_re_evaluating():
    d = held_frame(csc("20260508", complete=False, **{ledger.SKIPPED: True, ledger.SKIP_REASON: "not imaged"}))
    _, _, info = run(d)
    assert "--reevaluate-only" in info[FRAME]["hint"]


def test_a_skipped_date_is_never_a_boundary():
    """A boundary KSC written before the date was skipped never publishes its CCSLC set."""
    d = data(cscs=[csc("20260502"), csc("20260508", **{ledger.SKIPPED: True, ledger.SKIP_REASON: "bad input"})],
             kscs=[ksc("20260502"), ksc("20260508", fires=False, **{c.SAVE_COMPRESSED_CSLC: True})],
             l3s=[l3("20260502")])
    rows, _, info = run(d)
    r = by_date(rows)["20260508"]
    assert r["outcome"] == ledger.SKIPPED_OUT
    assert r["boundary"] is False
    assert r["ccslc_bursts"] is None
    assert info[FRAME]["summary"]["boundaries"] == 0


def test_only_the_first_partial_date_stops_the_frame():
    d = stopped_frame()
    d["csc"][(FRAME, "20260526")] = csc("20260526", complete=False)
    rows, _, info = run(d)
    r = by_date(rows)
    assert r["20260508"]["outcome"] == ledger.STOPPED
    assert r["20260526"]["outcome"] == ledger.BEHIND_STOP
    assert r["20260526"]["blocker"] == "20260508"
    assert info[FRAME]["state"] == "stopped 20260508"


def test_cmr_tells_a_partly_catalogued_date_from_another_partial_one():
    d = stopped_frame()
    d["csc"][(FRAME, "20260526")] = csc("20260526", complete=False)
    d["csc"][(FRAME, "20260601")] = csc("20260601", complete=False)
    cmr = FakeCmr({"20260508": BURSTS[:1], "20260526": BURSTS, "20260601": BURSTS[:2]})
    r = by_date(run(d, cmr=cmr)[0])
    assert "not fully catalogued yet" in r["20260526"]["note"]
    assert "another partial date" in r["20260601"]["note"]


def test_cmr_says_why_it_stopped_and_what_is_not_catalogued():
    cmr = FakeCmr({"20260502": BURSTS, "20260508": BURSTS[:1], "20260520": BURSTS, "20260526": BURSTS},
                  imaged=BURSTS[1:])
    rows, _, info = run(stopped_frame(), cmr=cmr)
    r = by_date(rows)
    assert r["20260508"]["root_cause"].startswith("OPS production gap: 2 burst(s)")
    assert r["20260508"]["missing_bursts"] == " ".join(BURSTS[1:])
    assert r["20260526"]["outcome"] == ledger.BEHIND_STOP
    assert "behind the stop at 20260508" in r["20260526"]["note"]
    assert info[FRAME]["hint"].startswith("get the missing CSLCs produced")


def test_cmr_dates_before_forward_processing_are_not_owed():
    d = stopped_frame()
    d["csc"][(FRAME, "20260420")] = csc("20260420")
    d["ksc"][(FRAME, "20260420")] = ksc("20260420", **{c.SUPERSEDED_BY: "existing_ccslc"})
    cmr = FakeCmr({"20260402": BURSTS, "20260420": BURSTS, "20260508": BURSTS[:1], "20260526": BURSTS})
    rows, _, info = run(d, cmr=cmr)
    assert info[FRAME]["forward_start"] == "20260502"
    dates = by_date(rows)
    assert "20260402" not in dates
    assert dates["20260526"]["outcome"] == ledger.BEHIND_STOP


def test_a_date_in_cmr_not_catalogued_yet_is_pending():
    d = data(cscs=[csc("20260502")], kscs=[ksc("20260502")], l3s=[l3("20260502")])
    rows, _, info = run(d, cmr=FakeCmr({"20260502": BURSTS, "20260508": BURSTS}))
    r = by_date(rows)["20260508"]
    assert r["outcome"] == ledger.PENDING and r["cmr_now"] == "3/3"
    assert info[FRAME]["state"] == "in progress"


def test_not_imaged_means_skip():
    cmr = FakeCmr({"20260508": BURSTS[:1]})
    _, _, info = run(stopped_frame(), cmr=cmr)
    assert info[FRAME]["hint"].startswith("skip 20260508")


def test_a_partial_date_the_frame_got_past_blocks_nothing():
    d = data(cscs=[csc("20260502", complete=False), csc("20260508")], kscs=[ksc("20260508")],
             l3s=[l3("20260508")])
    rows, _, info = run(d)
    assert by_date(rows)["20260502"]["outcome"] == ledger.PARTIAL_PASSED
    assert info[FRAME]["state"] == "up to date"
    assert ledger.exit_code(rows, []) == 0


def test_products_are_checked_in_s3_and_delivery():
    good, missing, late, young = l3("20260502"), l3("20260508"), l3("20260514", delivery=None), \
        l3("20260520", delivery=None, created=YOUNG)
    d = data(kscs=[ksc(x) for x in ("20260502", "20260508", "20260514", "20260520")],
             l3s=[good, missing, late, young])
    stray = f"OPERA_L3_DISP-S1_IW_F{FRAME:05d}_VV_20260101T000000Z_20260526T000000Z_v1.0_20261014T010000Z"
    older = f"OPERA_L3_DISP-S1_IW_F{FRAME:05d}_VV_20250101T000000Z_20250126T000000Z_v1.0_20250301T010000Z"
    s3 = {good["id"]: {"a.nc"}, late["id"]: {"a.nc"}, young["id"]: {"a.nc"}, stray: {"b.nc"}, older: {"c.nc"}}
    rows, products, info = run(d, s3=s3, products_since="2026-10-13T15:50:00Z")
    problems = {p["id"]: p["problem"] for p in products if p["problem"]}
    assert problems == {missing["id"]: "not in S3", late["id"]: "not delivered (no CNM status)",
                        stray: "in S3, not in GRQ"}
    assert older not in {p["id"] for p in products}       # before the campaign: not ours
    assert info[FRAME]["summary"]["product_problems"] == 3
    assert ledger.exit_code(rows, products) == 1


def test_a_product_still_being_indexed_is_not_an_orphan():
    produced = (NOW - ledger.timedelta(minutes=5)).strftime("%Y%m%dT%H%M%SZ")
    fresh = f"OPERA_L3_DISP-S1_IW_F{FRAME:05d}_VV_20260502T000000Z_20260508T000000Z_v1.0_{produced}"
    rows, products, _ = run(data(kscs=[ksc("20260502")], l3s=[l3("20260502")]),
                            s3={l3("20260502")["id"]: {"a.nc"}, fresh: {"a.nc"}},
                            products_since="2026-10-13T15:50:00Z")
    row = next(p for p in products if p["id"] == fresh)
    assert row["problem"] == "" and row["outcome"].startswith("publishing")
    assert ledger.exit_code(rows, products) == 0


def test_a_partial_csc_still_filling_is_not_a_stop():
    # a neighbouring frame catalogued its shared bursts a few minutes ago; this frame's own CSLCs are still arriving
    rows, _, info = run(data(cscs=[csc("20260502", complete=False, _created=YOUNG)]))
    row = by_date(rows)["20260502"]
    assert row["outcome"] == ledger.PENDING and "still filling" in row["note"]
    assert not info[FRAME]["stops"]
    rows, _, _ = run(data(cscs=[csc("20260502", complete=False)]))
    assert by_date(rows)["20260502"]["outcome"] == ledger.STOPPED


def test_a_historical_k_set_closes_on_its_last_date():
    # one historical job: reference 20250529, L3s for every later date of its k-set, its CCSLC set on the last one
    dates = ["20250610", "20250622", "20250704"]
    hist = [l3(x, ref="20250529", forward=False) for x in dates]
    kscs = [ksc(x, fires=False, **{c.SUPERSEDED_BY: "historical_processing"}) for x in ["20250529"] + dates]
    full = {(FRAME, "20250704"): set(BURSTS)}
    rows, _, info = run(data(kscs=kscs, l3s=hist, ccslc=full, markers={(FRAME, "20250704"): 1}))
    r = by_date(rows)
    assert [x for x in dates if r[x]["boundary"]] == ["20250704"]
    assert r["20250704"]["boundary_ok"] and r["20250704"]["ccslc_bursts"] == len(BURSTS)
    assert info[FRAME]["summary"]["boundaries"] == 1 and info[FRAME]["summary"]["boundaries_ok"] == 1


def test_a_historical_boundary_without_its_ccslc_set_is_not_ok():
    hist = [l3(x, ref="20250529", forward=False) for x in ("20250610", "20250622")]
    rows, _, info = run(data(l3s=hist))
    r = by_date(rows)["20250622"]
    assert r["boundary"] and not r["boundary_ok"] and r["ccslc_bursts"] == 0
    assert info[FRAME]["summary"]["boundaries_ok"] == 0


def test_two_set_markers_on_a_historical_boundary_are_unexplained():
    hist = [l3("20250610", ref="20250529", forward=False)]
    rows, _, _ = run(data(l3s=hist, ccslc={(FRAME, "20250610"): set(BURSTS)}, markers={(FRAME, "20250610"): 2}))
    assert by_date(rows)["20250610"]["outcome"] == ledger.UNEXPLAINED


def test_exit_codes():
    rows, products, _ = run(data(kscs=[ksc("20260502")], l3s=[l3("20260502")]))
    assert ledger.exit_code(rows, products) == 0
    rows, products, _ = run(stopped_frame())
    assert ledger.exit_code(rows, products) == 2
    rows, products, _ = run(data(cscs=[csc("20260502")]))
    assert ledger.exit_code(rows, products) == 1


def test_manifest_has_one_row_per_l3_with_its_s3_files():
    p = l3("20260502")
    _, products, _ = run(data(kscs=[ksc("20260502")], l3s=[p]),
                         s3={p["id"]: {p["id"] + ".nc", p["id"] + ".png"}})
    (row,) = ledger.manifest_rows(products)
    prefix = f"s3://{BUCKET}/products/DISP_S1/F{FRAME:05d}/{p['id']}/"
    assert row["s3_prefix"] == prefix
    assert row["nc_url"] == prefix + p["id"] + ".nc"
    assert row["file_urls"].split() == [prefix + p["id"] + ".nc", prefix + p["id"] + ".png"]
    assert (row["frame"], row["reference_date"], row["secondary_date"]) == (FRAME, "20260101", "20260502")


def test_s3_location_reads_the_grq_url_form():
    assert ledger.s3_location(["http://x/y", "s3://s3-us-west-2.amazonaws.com:80/bkt/products/DISP_S1/F1/P"]) == \
        ("bkt", "products/DISP_S1/F1/P")
    assert ledger.s3_location(["s3://bkt/products/P/"]) == ("bkt", "products/P")


def test_whitelist_from_the_rule_query():
    q = {"bool": {"must": [{"term": {"x": 1}}, {"terms": {"metadata.region_id": ["0", 3]}}]}}
    assert ledger.whitelist_from_query(json.dumps(q)) == ["0", "3"]
    assert ledger.whitelist_from_query({"bool": {"must": []}}) is None


# -- main, end to end --------------------------------------------------------------------------------

class FakeEs:
    """Answers the ledger's searches by index; one page per query."""

    def __init__(self, docs):
        self.docs = docs

    def search(self, index=None, body=None, **kwargs):
        for key, hits in self.docs.items():
            if key in index:
                return {"hits": {"hits": hits}, "_scroll_id": "s"}
        return {"hits": {"hits": []}, "_scroll_id": "s"}

    def scroll(self, scroll_id=None, scroll=None):
        return {"hits": {"hits": []}}

    def clear_scroll(self, scroll_id=None):
        pass


def grq_docs():
    def hit(meta, _id="x"):
        # state configs are stamped creation_time, not creation_timestamp
        return {"_id": _id, "_source": {"metadata": {k: v for k, v in meta.items() if not k.startswith("_")},
                                        "creation_time": OLD + "000Z"}}
    p = l3("20260502")
    l3_hit = {"_id": p["id"], "_source": {"metadata": {"frame_id": FRAME, "input_granule_id": p["input"]},
                                           "daac_delivery_status": "SUCCESS", "creation_timestamp": OLD,
                                           "urls": [f"s3://s3-us-west-2.amazonaws.com:80/{BUCKET}/{p['key']}"]}}
    k = ksc("20260502")
    return p, FakeEs({
        "kcycle": [hit(k, k["_id"])],
        "cycle-state-config": [hit(csc("20260502")), hit(csc("20260508", complete=False))],
        "l3_disp_s1": [l3_hit],
    })


def run_main(tmp_path):
    p, grq = grq_docs()
    mozart = FakeEs({"user_rules": [{"_source": {"query_string": json.dumps(
        {"bool": {"must": [{"terms": {"metadata.region_id": ["0"]}}]}})}}]})
    code = ledger.main(["--region", "0", "--until", "20261012", "--json", str(tmp_path / "l.json"),
                        "--csv", str(tmp_path / "l.csv"), "--manifest", str(tmp_path / "m.csv")],
                       grq=grq, mozart=mozart, s3={p["id"]: {p["id"] + ".nc"}}, frame_bursts={FRAME: BURSTS},
                       region_db={FRAME: "0", 11115: "1"}, now=NOW)
    assert code == 2                                                         # stopped on 20260508
    out = json.load(open(tmp_path / "l.json"))
    assert out["whitelist"] == ["0"]
    assert out["frames"][str(FRAME)]["state"] == "stopped 20260508"
    assert out["frames"][str(FRAME)]["track"] == 35
    rows = list(csv.DictReader(open(tmp_path / "l.csv")))
    assert [(r["date"], r["outcome"]) for r in rows] == [("20260502", "produced"), ("20260508", "stopped")]
    (m,) = list(csv.DictReader(open(tmp_path / "m.csv")))
    assert m["nc_url"].endswith(p["id"] + ".nc")
    return tmp_path / "l.json"


def test_s3_is_listed_one_frame_prefix_at_a_time():
    asked = []

    def lister(bucket, prefix):
        asked.append((bucket, prefix))
        pid = f"OPERA_L3_DISP-S1_IW_F{int(prefix[-6:-1]):05d}_VV_x"
        return [f"{prefix}{pid}/{pid}.nc", f"{prefix}{pid}/{pid}_BROWSE.png", f"{prefix}stray.txt"]

    prods = ledger.load_s3(BUCKET, [9154, 46291], lister=lister)
    assert asked == [(BUCKET, "products/DISP_S1/F09154/"), (BUCKET, "products/DISP_S1/F46291/")]
    assert prods == {"OPERA_L3_DISP-S1_IW_F09154_VV_x": {"OPERA_L3_DISP-S1_IW_F09154_VV_x.nc",
                                                          "OPERA_L3_DISP-S1_IW_F09154_VV_x_BROWSE.png"},
                     "OPERA_L3_DISP-S1_IW_F46291_VV_x": {"OPERA_L3_DISP-S1_IW_F46291_VV_x.nc",
                                                          "OPERA_L3_DISP-S1_IW_F46291_VV_x_BROWSE.png"}}


def test_default_scope_is_an_hour_before_the_earliest_ksc():
    kscs = {(FRAME, "20260502"): ksc("20260502", _created="2026-10-13T19:02:26.278"),
            (FRAME, "20260508"): ksc("20260508", _created="2026-10-13T16:40:00.000"),
            (FRAME, "20260514"): ksc("20260514", _created=None)}
    assert ledger.default_scope_since(kscs) == "2026-10-13T15:40:00Z"
    assert ledger.default_scope_since({}) is None


class RecordingEs(FakeEs):
    def __init__(self, docs):
        super().__init__(docs)
        self.queries = []

    def search(self, index=None, body=None, **kwargs):
        self.queries.append((index, body))
        return super().search(index=index, body=body, **kwargs)


def job_and_marker_scopes(argv):
    _, grq = grq_docs()
    grq = RecordingEs(grq.docs)
    mozart = RecordingEs({"user_rules": [{"_source": {"query_string": json.dumps({"bool": {"must": []}})}}]})
    ledger.main(["--frame-id", str(FRAME), "--until", "20261012", "--no-s3"] + argv,
                grq=grq, mozart=mozart, frame_bursts={FRAME: BURSTS}, now=NOW)
    (jobs,) = [b for i, b in mozart.queries if i == ledger.JOB_STATUS_INDEX]
    (markers,) = [b for i, b in grq.queries if i == ledger.CCSLC_SET_INDEX]
    return jobs["query"]["bool"]["must"], markers["query"]


def test_without_products_since_jobs_and_markers_start_from_the_earliest_ksc():
    jobs, markers = job_and_marker_scopes([])
    since = "2026-10-13T09:00:00Z"                                  # OLD minus an hour
    assert {"range": {"job.job_info.time_queued": {"gte": since}}} in jobs
    assert markers == {"range": {"creation_timestamp": {"gte": since}}}


def test_products_since_scopes_jobs_and_markers_as_given():
    jobs, markers = job_and_marker_scopes(["--products-since", "2026-10-13T15:50:00Z"])
    assert {"range": {"job.job_info.time_queued": {"gte": "2026-10-13T15:50:00Z"}}} in jobs
    assert markers == {"range": {"creation_timestamp": {"gte": "2026-10-13T15:50:00Z"}}}


def test_state_configs_are_dated_by_creation_time():
    assert ledger.created_of({"creation_time": "2026-10-13T17:04:29.275098Z", "@timestamp": "x"}) == \
        "2026-10-13T17:04:29.275098Z"
    assert ledger.created_of({"creation_timestamp": "2026-10-14T00:43:55Z"}) == "2026-10-14T00:43:55Z"
    assert ledger.created_of({"@timestamp": "2026-10-14T00:45:10Z"}) == "2026-10-14T00:45:10Z"
    assert ledger.created_of({}) is None


def test_the_cmr_clients_max_pages_warning_is_dropped():
    import logging
    f = ledger.NoMaxPagesWarning()
    rec = logging.LogRecord("opera_pcm", logging.WARNING, __file__, 1,
                            "Reached max pages limit (%d). Not all search results exhausted.", (1,), None)
    assert f.filter(rec) is False
    other = logging.LogRecord("opera_pcm", logging.WARNING, __file__, 1, "CMR returned 500", (), None)
    assert f.filter(other) is True


def test_main_writes_the_ledger_csv_json_and_manifest(tmp_path):
    assert run_main(tmp_path).exists()


def test_frame_selection_is_required():
    with pytest.raises(SystemExit):
        ledger.main([], grq=FakeEs({}), mozart=FakeEs({}), frame_bursts={})


def test_plotter_renders_the_json(tmp_path):
    pytest.importorskip("matplotlib")
    import tools.plot_disp_s1_forward_ledger as plot
    path = run_main(tmp_path)
    plot.main(str(path), str(tmp_path / "ledger.png"))
    assert (tmp_path / "ledger.png").stat().st_size > 10000
