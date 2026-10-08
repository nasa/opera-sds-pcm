"""Unit tests for tools/disp_s1_duplicate_audit.py."""

from unittest.mock import MagicMock

from tools.disp_s1_duplicate_audit import (
    audit,
    duplicate_ccslc_sets,
    duplicate_ccslcs,
    duplicate_l3s,
    duplicate_sciflo_runs,
    main,
)

KSC = "disp_s1-kcycle-k15-m6-f36541-20260627-state-config"


def _l3(sec, created, frame="36541", version="v1.0"):
    return f"OPERA_L3_DISP-S1_IW_F{frame}_VV_20250703T140200Z_{sec}T140210Z_{version}_{created}"


def _ccslc(burst, created, last="20260627"):
    return (f"OPERA_L2_COMPRESSED-CSLC-S1_F46288_{burst}_20250703T000000Z_20250703T000000Z_"
            f"{last}T000000Z_{created}_VV_v1.0")


def _job(ksc, ts, status="job-completed", msg=None):
    return {"job_id": f"SCIFLO_L3_DISP_S1__r6.0.6-{ksc}-{ts}", "status": status, "msg": msg}


def test_duplicate_l3s_keys_on_frame_pol_dates_and_version():
    ids = [
        _l3("20260627", "20261005T100000Z"),
        _l3("20260627", "20261005T100016Z"),
        _l3("20260615", "20261005T100000Z"),
        # a reprocessed version is not a duplicate
        _l3("20260615", "20261006T090000Z", version="v1.1"),
        "OPERA_L3_DISP-S1-STATIC_F36541_20140403_S1A_v1.0",
    ]
    assert duplicate_l3s(ids) == {
        ("36541", "VV", "20250703", "20260627", "v1.0"): sorted(ids[:2]),
    }


def test_duplicate_ccslcs_keys_on_burst_and_dates():
    ids = [
        _ccslc("T064-135520-IW1", "20261005T100000Z"),
        _ccslc("T064-135520-IW1", "20261005T100016Z"),
        _ccslc("T064-135520-IW2", "20261005T100000Z"),
        _ccslc("T064-135520-IW1", "20261005T100000Z", last="20260709"),
    ]
    dups = duplicate_ccslcs(ids)
    assert list(dups.values()) == [sorted(ids[:2])]
    assert list(dups)[0][:2] == ("46288", "T064-135520-IW1")


def test_duplicate_ccslc_sets_flags_a_boundary_run_twice():
    ids = [
        "disp_s1-ccslc-set-f46288-20250703-20250703-20260627-20261005T100000Z",
        "disp_s1-ccslc-set-f46288-20250703-20250703-20260627-20261005T100016Z",
        "disp_s1-ccslc-set-f46288-20250703-20260709-20261121-20261121T100000Z",
    ]
    assert duplicate_ccslc_sets(ids) == {("46288", "20260627"): ids[:2]}


def test_duplicate_sciflo_runs_ignore_bails_and_failures():
    jobs = [
        _job(KSC, "20261005T100000.1Z"),
        _job(KSC, "20261005T100000.2Z", status="job-started"),
        _job(KSC, "20261005T100000.3Z", msg="dup skip: L3_DISP_S1"),
        _job(KSC, "20261005T100000.5Z", status="job-failed"),
        _job(KSC, "20261005T100000.6Z", status="job-deduped"),
        _job("disp_s1-kcycle-k15-m6-f36541-20260615-state-config", "20261005T100000Z"),
    ]
    assert duplicate_sciflo_runs(jobs) == {KSC: sorted(j["job_id"] for j in jobs[:2])}


def _fake_es(hits_by_index):
    es = MagicMock()

    def search(index, body, **kwargs):
        return {"_scroll_id": f"sid-{index}", "hits": {"hits": hits_by_index.get(index, [])}}

    es.search.side_effect = search
    es.scroll.return_value = {"_scroll_id": "done", "hits": {"hits": []}}
    return es


def test_audit_reports_each_category_and_filters_jobs_by_frame():
    grq = _fake_es({
        "grq_*_l3_disp_s1-*": [{"_id": _l3("20260627", t)} for t in ("20261005T100000Z", "20261005T100016Z")],
        "grq_*_l2_cslc_s1_compressed-*": [{"_id": _ccslc("T064-135520-IW1", "20261005T100000Z")}],
        "grq_*_disp_s1-ccslc-set-*": [],
    })
    mozart = _fake_es({"job_status-*": [
        {"_source": _job(KSC, "a")}, {"_source": _job(KSC, "b")},
        {"_source": _job("disp_s1-kcycle-k15-m6-f11114-20260627-state-config", "a")},
        {"_source": _job("disp_s1-kcycle-k15-m6-f11114-20260627-state-config", "b")},
    ]})

    report = audit(grq, mozart, frame_ids=[36541])

    assert len(report["L3_DISP_S1"]) == 1
    assert report["L2_CSLC_S1_COMPRESSED"] == {}
    assert report["disp_s1-ccslc-set"] == {}
    assert list(report["SCIFLO_L3_DISP_S1"]) == [KSC]
    l3_query = next(c.kwargs["body"]["query"] for c in grq.search.call_args_list
                    if c.kwargs["index"] == "grq_*_l3_disp_s1-*")
    assert {"terms": {"metadata.frame_id": [36541]}} in l3_query["bool"]["must"]


def test_main_exit_code(monkeypatch, capsys):
    import sys
    import types

    clean = _fake_es({})
    es_connection = types.ModuleType("opera_commons.es_connection")
    es_connection.get_grq_es = lambda logger=None: MagicMock(es=clean)
    es_connection.get_mozart_es = lambda logger=None: MagicMock(es=clean)
    monkeypatch.setitem(sys.modules, "opera_commons.es_connection", es_connection)
    assert main([]) == 0
    assert "CLEAN" in capsys.readouterr().out

    l3_dups = [{"_id": _l3("20260627", t)} for t in ("20261005T100000Z", "20261005T100016Z")]
    dirty = _fake_es({"grq_*_l3_disp_s1-*": l3_dups})
    es_connection.get_grq_es = lambda logger=None: MagicMock(es=dirty)
    assert main(["--json"]) == 1
