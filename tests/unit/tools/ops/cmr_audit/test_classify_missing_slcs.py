"""classify_missing_slcs: from a burst coverage report and FWD state to one fix per missing scene.

The classes mirror how the forward pipeline loses a CSLC: the query never ingests the SLC, the download
never lands, the ionosphere job (which submits the forward CSLC job) never gets to it, the CSLC job fails,
or the products never reach CMR. Each needs a different OPS action, so the tool must tell them apart from
the cluster's own records, read-only.
"""
import csv
import json

import pytest

from tools.ops.cmr_audit import classify_missing_slcs as cms

VV = "S1C_IW_SLC__1SDV_20261021T004700_20261021T004731_009976_013E4D_0CCC"
HH = "S1C_IW_SLC__1SDH_20260807T104726_20260807T104757_008888_011A18_F21F"


def _scene(sid=VV, bursts=(("T078-165489-IW1", "VV"), ("T078-165489-IW2", "VV"))):
    return cms.MissingScene(sid, sid[:3], "2026-10-21", set(bursts))


def _doc(**kw):
    doc = {"id": VV + "-r1", "created": "2026-10-21T07:39:06.12", "processing_mode": "forward", "na": True,
           "iono": False, "s3_prefix": ("opera-ops-rs-fwd", f"inputs/SLC/2026/10/21/{VV}-r1/")}
    doc.update(kw)
    return doc


@pytest.mark.parametrize("facts, expected", [
    (cms.FwdFacts(), "not_ingested"),
    (cms.FwdFacts(in_catalog=True), "download_not_ingested"),
    (cms.FwdFacts(in_catalog=True, catalog_na=False), "outside_gate"),
    (cms.FwdFacts(in_catalog=True, slc_docs=[_doc(na=None)]), "outside_gate"),
    (cms.FwdFacts(in_catalog=True, slc_docs=[_doc()]), "iono_stranded"),
    (cms.FwdFacts(in_catalog=True, slc_docs=[_doc()], inputs_in_s3=False), "inputs_aged_out"),
    (cms.FwdFacts(in_catalog=True, slc_docs=[_doc(iono=True)]), "trigger_lost"),
    (cms.FwdFacts(in_catalog=True, slc_docs=[_doc(processing_mode="reprocessing")]), "trigger_lost"),
    (cms.FwdFacts(slc_docs=[_doc()], cslc_jobs=[{"status": "job-started", "queued": "2026-11-04T22:46"}]),
     "cslc_job_in_progress"),
    (cms.FwdFacts(slc_docs=[_doc()], cslc_jobs=[{"status": "job-completed", "queued": "2026-11-04T22:00"},
                                                {"status": "job-failed", "queued": "2026-11-04T22:46"}]),
     "cslc_job_failed"),
    (cms.FwdFacts(slc_docs=[_doc()], cslc_jobs=[{"status": "job-completed", "queued": "2026-11-04T22:46"}]),
     "cslc_job_completed"),
    (cms.FwdFacts(slc_docs=[_doc()], cslc_bursts={("T078-165489-IW1", "VV"), ("T078-165489-IW2", "VV")}),
     "produced_not_in_cmr"),
    (cms.FwdFacts(slc_docs=[_doc()], cslc_bursts={("T078-165489-IW1", "VV")}), "partial_output"),
])
def test_each_pipeline_state_gets_its_class(facts, expected):
    assert cms.classify(_scene(), facts) == expected


def test_the_newest_doc_and_job_decide():
    f = cms.FwdFacts(slc_docs=[_doc(id=HH + "-r1", created="2026-08-07T16:40:00", iono=True),
                               _doc(id=HH + "-r5", created="2026-11-04T22:57:00", processing_mode="historical")])
    assert f.newest_doc["id"].endswith("-r5")
    assert cms.classify(_scene(HH, {("T040-084571-IW1", "HH")}), f) == "trigger_lost"


def test_iono_window_is_the_creation_minute():
    assert cms.iono_window("2026-10-21T07:39:06.724230") == "--start-date=2026-10-21T07:39:00Z --end-date=2026-10-21T07:40:00Z"
    assert cms.iono_window("2026-08-07T16:59:59Z") == "--start-date=2026-08-07T16:59:00Z --end-date=2026-08-07T17:00:00Z"


def test_s3_prefix_from_the_slc_doc_urls():
    urls = ["http://opera-ops-rs-fwd.s3-website-us-west-2.amazonaws.com/inputs/SLC/2026/10/21/X-r1",
            "s3://s3-us-west-2.amazonaws.com:80/opera-ops-rs-fwd/inputs/SLC/2026/10/21/X-r1"]
    assert cms.s3_prefix_from_urls(urls) == ("opera-ops-rs-fwd", "inputs/SLC/2026/10/21/X-r1/")
    assert cms.s3_prefix_from_urls(["s3://bucket/key/X-r1/"]) == ("bucket", "key/X-r1/")
    assert cms.s3_prefix_from_urls(["https://example"]) is None


class FakeS3:
    def __init__(self, keys):
        self.keys, self.calls = keys, []

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        self.calls.append((Bucket, Prefix))
        return {"Contents": [{"Key": k} for k in self.keys if k.startswith(Prefix)]}


def test_s3_inputs_are_checked_only_for_ionosphere_rerun_candidates():
    gone = cms.FwdFacts(slc_docs=[_doc()])
    there = cms.FwdFacts(slc_docs=[_doc(s3_prefix=("b", "p/there/"))])
    has_job = cms.FwdFacts(slc_docs=[_doc(s3_prefix=("b", "p/job/"))], cslc_jobs=[{"status": "job-failed"}])
    s3 = FakeS3(["p/there/x.zip", "p/job/x.zip"])
    cms.check_s3_inputs({"gone": gone, "there": there, "job": has_job}, s3)
    assert (gone.inputs_in_s3, there.inputs_in_s3, has_job.inputs_in_s3) == (False, True, None)
    assert ("b", "p/job/") not in s3.calls


# --- report loading ---------------------------------------------------------------------------------

def _missing(sid, burst, pol, frames=None):
    item = {"burst_pattern": burst, "acquisition_time": "2026-10-21T00:47:00+00:00", "platform": sid[:3],
            "polarization": pol, "slc_native_id": sid + "-SLC"}
    if frames is not None:
        item["cbdb_frames"] = frames
    return item


def test_jsonl_and_json_reports_group_missing_bursts_by_scene(tmp_path):
    jsonl = tmp_path / "a.jsonl"
    jsonl.write_text("\n".join(json.dumps(r) for r in [
        {"_type": "metadata"},
        {"_type": "chunk_result", "product_type": "CSLC-S1", "missing": [
            _missing(VV, "T078-165489-IW1", "VV", [20685]), _missing(VV, "T078-165489-IW2", "VV", [])],
         "unverified": [_missing(HH, "T040-084571-IW1", "HH")]},
        {"_type": "chunk_result", "product_type": "RTC-S1", "missing": [_missing(HH, "T040-084571-IW2", "HH")]},
        {"_type": "chunk_error", "chunk_start": "2026-10-22T00:00:00+00:00"},
    ]) + "\n")
    whole = tmp_path / "b.json"
    whole.write_text(json.dumps({"metadata": {"failed_chunks": []}, "products": {"CSLC-S1": {
        "missing": [_missing(HH, "T040-084571-IW1", "HH")], "unverified": []}}}))

    scenes, notes = cms.load_missing_scenes([str(jsonl), str(whole)])
    assert sorted(scenes) == [HH, VV]
    assert scenes[VV].bursts == {("T078-165489-IW1", "VV"), ("T078-165489-IW2", "VV")}
    assert scenes[VV].cbdb_frames == {20685}
    assert scenes[HH].bursts == {("T040-084571-IW1", "HH")}   # the RTC row is not a CSLC gap
    assert (notes["unverified"], notes["unverified_days"], notes["failed_days"]) == (1, {"2026-10-21"}, {"2026-10-22"})


# --- FWD lookups ------------------------------------------------------------------------------------

class FakeES:
    """Answers the classifier's searches from canned docs, keyed by index pattern."""

    def __init__(self, docs, job_pages=None):
        self.docs, self.job_pages, self.bodies = docs, list(job_pages or []), []

    def search(self, index, body, scroll=None):
        self.bodies.append((index, body))
        if index == cms.JOB_INDEX:
            page = self.job_pages.pop(0) if self.job_pages else []
            return {"_scroll_id": "s1", "hits": {"total": {"value": len(page)}, "hits": page}}
        hits = self.docs.get(index, [])
        return {"hits": {"total": {"value": len(hits)}, "hits": hits}}

    def scroll(self, scroll_id, scroll):
        page = self.job_pages.pop(0) if self.job_pages else []
        return {"_scroll_id": scroll_id, "hits": {"hits": page}}

    def clear_scroll(self, scroll_id):
        self.cleared = scroll_id


def _job(sid_rev, status, queued, error=None):
    src = {"type": "job-SCIFLO_L2_CSLC_S1:6.0.7", "status": status,
           "job": {"params": {"input_dataset_id": sid_rev}, "job_info": {"time_queued": queued}}}
    if error:
        src["short_error"] = error + "\nTraceback ..."
    return {"_source": src}


def test_fwd_facts_come_from_catalog_grq_and_a_job_scan():
    grq = FakeES({
        cms.CATALOG_INDEX: [{"_source": {"granule_id": HH + "-SLC", "intersects_north_america": True}}],
        cms.SLC_INDEX: [{"_id": HH + "-r5", "_source": {
            "id": HH + "-r5", "creation_timestamp": "2026-11-04T22:57:00", "metadata": {
                "id": HH, "processing_mode": "historical", "intersects_north_america": True},
            "urls": [f"s3://s3-us-west-2.amazonaws.com:80/opera-ops-rs-fwd/inputs/SLC/2026/08/07/{HH}-r5"]}}],
        cms.CSLC_INDEX: [{"_source": {"metadata": {"input_granule_id": VV, "burst_id": "T078-165489-IW1",
                                                   "polarization": "VV"}}}],
    })
    mozart = FakeES({}, job_pages=[[_job(HH + "-r1", "job-failed", "2026-11-04T22:46:00", "Failed to download SAFE (404)"),
                                    _job("S1A_other-r1", "job-completed", "2026-11-04T22:00:00")],
                                   [_job(HH + "-r5", "job-started", "2026-11-04T23:12:00")]])
    facts = cms.gather_fwd_facts([HH, VV], grq, mozart)

    hh, vv = facts[HH], facts[VV]
    assert hh.in_catalog and hh.catalog_na is True and not vv.in_catalog
    assert hh.newest_doc["id"] == HH + "-r5" and hh.newest_doc["s3_prefix"][0] == "opera-ops-rs-fwd"
    assert vv.cslc_bursts == {("T078-165489-IW1", "VV")}
    assert [j["status"] for j in hh.cslc_jobs] == ["job-failed", "job-started"]
    assert hh.cslc_jobs[0]["error"] == "Failed to download SAFE (404)"
    assert mozart.cleared == "s1"
    # the job scan is bounded by the oldest SLC doc's creation day
    job_body = next(b for i, b in mozart.bodies if i == cms.JOB_INDEX)
    assert job_body["query"]["bool"]["filter"][1]["range"]["job.job_info.time_queued"]["gte"] == "2026-11-04"
    assert cms.classify(_scene(HH, {("T040-084571-IW1", "HH")}), hh) == "cslc_job_in_progress"


def test_a_truncated_grq_answer_is_an_error_not_a_quiet_gap():
    class Truncated(FakeES):
        def search(self, index, body, scroll=None):
            return {"hits": {"total": {"value": 50}, "hits": [{"_source": {"granule_id": VV + "-SLC"}}]}}
    with pytest.raises(RuntimeError, match="lower --batch-size"):
        cms.gather_fwd_facts([VV], Truncated({}), FakeES({}))


# --- outputs ----------------------------------------------------------------------------------------

def test_outputs_one_row_per_scene_and_one_list_per_class(tmp_path):
    scenes = {VV: _scene(), HH: _scene(HH, {("T040-084571-IW1", "HH")})}
    scenes[HH].cbdb_frames = {10572}
    facts = {VV: cms.FwdFacts(in_catalog=True, slc_docs=[_doc()]), HH: cms.FwdFacts()}
    rows = [cms.row_for(scenes[s], facts[s]) for s in sorted(scenes)]
    out, lists = tmp_path / "c.csv", tmp_path / "lists"
    cms.write_outputs(rows, str(out), str(lists))

    got = {r["slc_id"]: r for r in csv.DictReader(out.open())}
    assert got[VV]["class"] == "iono_stranded" and got[VV]["iono_window"].startswith("--start-date=2026-10-21T07:39:00Z")
    assert got[HH]["class"] == "not_ingested" and got[HH]["cbdb_frames"] == "10572"
    assert (lists / "slcs-iono_stranded.txt").read_text() == VV + "-SLC\n"
    assert (lists / "slcs-not_ingested.txt").read_text() == HH + "-SLC\n"
    assert "# 1 scene(s)" in (lists / "iono-windows.txt").read_text()
