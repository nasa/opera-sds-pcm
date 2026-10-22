"""The operator tool that skips a DISP-S1 sensing date and re-evaluates the k-cycles behind it.

A skip may never contradict a product already made: it is refused while a fired KSC holds the
date in its window, and an un-skip is refused once a later KSC has fired without it. Dry run is
the default and changes nothing; --apply marks the CSC and re-evaluates the frame's unfired KSCs
after the date one at a time, oldest first, stopping at the first that does not complete.
"""

import csv
import io
from unittest.mock import MagicMock, patch

import pytest

import tools.skip_disp_s1_csc as tool
from data_subscriber.cslc import disp_s1_constants as c

FRAME = 9154
DATE = "20260508"
CSC_ID = f"cslc_s1-cycle-f{FRAME}-{DATE}-state-config"


def csc_hit(**meta):
    base = {c.FRAME_ID: FRAME, c.SENSING_DATE: DATE, c.IS_COMPLETE: False,
            c.EXPECTED_BURST_IDS: ["b1", "b2", "b3"], c.FOUND_BURST_IDS: ["b1"]}
    base.update(meta)
    return {"_id": CSC_ID, "_index": "grq_1_cslc_s1-cycle-state-config-2026.05",
            "_source": {"dataset": c.CSLC_S1_CYCLE_STATE_CONFIG, "urls": [], "metadata": base}}


def ksc_hit(date, fired=False, window=(), **meta):
    base = {c.FRAME_ID: FRAME, c.SENSING_DATE: date, c.WINDOW_SENSING_DATES: list(window),
            c.IS_COMPLETE: fired, c.COMPRESSED_CSLC_FINAL: fired, c.GAP_UNRESOLVED: not fired,
            c.COMPLETENESS_REASON: "ready" if fired else "gap_unresolved"}
    base.update(meta)
    return {"_id": f"disp_s1-kcycle-k15-m6-f{FRAME}-{date}-state-config",
            "_source": {"dataset": c.DISP_S1_KCYCLE_STATE_CONFIG, "urls": ["s3://bucket/ksc"],
                        "metadata": base}}


class FakeEs:
    """GRQ stand-in: the CSC takes the tool's partial updates; KSC lookups filter on the date."""

    def __init__(self, csc=None, kscs=(), keep_csc=True):
        self.csc = csc
        self.kscs = list(kscs)
        self.indices = MagicMock()
        self.update = MagicMock(side_effect=self._update if keep_csc else None)

    def _update(self, index=None, id=None, body=None, refresh=None):
        self.csc["_source"]["metadata"].update(body["doc"]["metadata"])

    def search(self, index=None, body=None):
        if "kcycle" in index:
            must = (body["query"].get("bool") or {}).get("must", [])
            dates = [t["term"]["metadata.sensing_date"] for t in must
                     if "term" in t and "metadata.sensing_date" in t["term"]]
            hits = [h for h in self.kscs if not dates or h["_source"]["metadata"][c.SENSING_DATE] == dates[0]]
            return {"hits": {"hits": hits}}
        return {"hits": {"hits": [self.csc] if self.csc else []}}


@pytest.fixture
def mozart():
    """Mozart stand-in: every submission succeeds and every job completes."""
    with patch.object(tool.requests, "post") as post, \
         patch.object(tool, "wait_for_job", return_value="job-completed") as wait:
        post.return_value.json.side_effect = [{"success": True, "result": f"job-{i}"} for i in range(50)]
        yield post, wait


def run(es, *argv):
    return tool.main(["--frame-id", str(FRAME), "--date", DATE, "--mozart-ip", "1.2.3.4",
                      "--job-release", "test", *argv], es=es)


def submitted_dates(post):
    import json
    return [json.loads(call.kwargs["data"]["params"])["product_metadata"]["metadata"][c.SENSING_DATE]
            for call in post.call_args_list]


# -- plan ---------------------------------------------------------------------

def test_skip_re_evaluates_the_unfired_kscs_after_the_date_oldest_first():
    kscs = [ksc_hit("20260520"), ksc_hit("20260514"), ksc_hit("20260502", fired=True)]
    kscs.sort(key=lambda h: h["_source"]["metadata"][c.SENSING_DATE])
    refusal, todo = tool.plan(FRAME, DATE, csc_hit(), kscs, unskip=False)
    assert refusal == ""
    assert [h["_source"]["metadata"][c.SENSING_DATE] for h in todo] == ["20260514", "20260520"]


def test_skip_is_refused_while_a_fired_ksc_holds_the_date():
    refusal, todo = tool.plan(FRAME, DATE, csc_hit(),
                              [ksc_hit("20260514", fired=True, window=["20260502", DATE, "20260514"])],
                              unskip=False)
    assert "KSCs that fire hold" in refusal
    assert todo == []


def test_a_fired_ksc_that_never_held_the_date_does_not_block_a_skip():
    # e.g. a partial cycle that arrived after later dates had already fired
    refusal, _ = tool.plan(FRAME, DATE, csc_hit(), [ksc_hit("20260514", fired=True, window=["20260514"])],
                           unskip=False)
    assert refusal == ""


def test_an_already_skipped_date_is_not_skipped_again():
    refusal, _ = tool.plan(FRAME, DATE, csc_hit(**{c.SKIPPED: True, c.SKIP_REASON: "x"}), [], unskip=False)
    assert "already skipped" in refusal


def test_unskip_is_refused_once_a_later_ksc_fired_without_the_date():
    refusal, _ = tool.plan(FRAME, DATE, csc_hit(**{c.SKIPPED: True}), [ksc_hit("20260514", fired=True)],
                           unskip=True)
    assert "already fired without it" in refusal


def test_unskip_of_a_date_that_is_not_skipped_is_refused():
    refusal, _ = tool.plan(FRAME, DATE, csc_hit(), [], unskip=True)
    assert "is not skipped" in refusal


def test_ksc_fired_mirrors_the_sciflo_trigger_rule():
    assert tool.ksc_fired({c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: True})
    assert not tool.ksc_fired({c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: True, c.GAP_UNRESOLVED: True})
    assert not tool.ksc_fired({c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: True, c.SUPERSEDED_BY: "x"})
    assert not tool.ksc_fired({c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: False})


# -- main ---------------------------------------------------------------------

def test_dry_run_changes_nothing(mozart):
    post, _ = mozart
    es = FakeEs(csc_hit(), [ksc_hit("20260514")])
    assert run(es, "--reason", "bursts never imaged") == 0
    es.update.assert_not_called()
    post.assert_not_called()


def test_a_skip_needs_a_reason():
    with pytest.raises(SystemExit):
        run(FakeEs(csc_hit()))


def test_no_csc_means_nothing_to_skip(mozart):
    assert run(FakeEs(None), "--reason", "x", "--apply") == 1


def test_apply_marks_the_csc_and_re_evaluates_in_date_order(mozart):
    post, wait = mozart
    es = FakeEs(csc_hit(), [ksc_hit("20260514"), ksc_hit("20260520")])
    assert run(es, "--reason", "2 bursts not imaged", "--by", "gmanipon", "--apply") == 0

    kwargs = es.update.call_args.kwargs
    assert kwargs["id"] == CSC_ID
    assert kwargs["index"] == "grq_1_cslc_s1-cycle-state-config-2026.05"
    fields = kwargs["body"]["doc"]["metadata"]
    assert fields[c.SKIPPED] is True
    assert fields[c.SKIP_REASON] == "2 bursts not imaged"
    assert fields[c.SKIPPED_BY] == "gmanipon"
    assert fields[c.SKIPPED_AT].endswith("Z")

    assert submitted_dates(post) == ["20260514", "20260520"]
    assert wait.call_count == 2
    form = post.call_args.kwargs["data"]
    assert form["type"] == "job-disp_s1_k_cycle_evaluator:test"
    assert "enable_dedup=false" in post.call_args.args[0]


def test_re_evaluation_stops_at_the_first_job_that_does_not_complete(mozart):
    post, wait = mozart
    wait.side_effect = ["job-completed", "job-failed", "job-completed"]
    es = FakeEs(csc_hit(), [ksc_hit("20260514"), ksc_hit("20260520"), ksc_hit("20260526")])
    assert run(es, "--reason", "x", "--apply") == 1
    assert submitted_dates(post) == ["20260514", "20260520"]


def test_no_reevaluate_only_marks_the_csc(mozart):
    post, _ = mozart
    es = FakeEs(csc_hit(), [ksc_hit("20260514")])
    assert run(es, "--reason", "x", "--apply", "--no-reevaluate") == 0
    es.update.assert_called_once()
    post.assert_not_called()


def test_unskip_of_a_complete_date_clears_the_skip_and_re_drives_the_date_first(mozart):
    post, _ = mozart
    es = FakeEs(csc_hit(**{c.SKIPPED: True, c.SKIP_REASON: "x", c.IS_COMPLETE: True,
                           c.FOUND_BURST_IDS: ["b1", "b2", "b3"]}), [ksc_hit("20260514")])
    assert run(es, "--unskip", "--apply") == 0
    fields = es.update.call_args.kwargs["body"]["doc"]["metadata"]
    assert fields == {c.SKIPPED: False, c.SKIP_REASON: None, c.SKIPPED_BY: None, c.SKIPPED_AT: None}
    assert submitted_dates(post) == [DATE, "20260514"]
    import json
    first = json.loads(post.call_args_list[0].kwargs["data"]["params"])
    assert first["dataset_type"] == c.CSLC_S1_CYCLE_STATE_CONFIG


def test_unskip_of_a_partial_date_gives_it_no_ksc(mozart):
    # the k-cycle rule never fires on a partial CSC, so neither does the tool; the later KSCs go back
    # to being held by it
    post, _ = mozart
    es = FakeEs(csc_hit(**{c.SKIPPED: True, c.SKIP_REASON: "x"}), [ksc_hit("20260514")])
    assert run(es, "--unskip", "--apply") == 0
    assert submitted_dates(post) == ["20260514"]


def test_reevaluate_only_resumes_without_touching_the_csc(mozart):
    post, wait = mozart
    wait.side_effect = ["job-completed", "job-failed"]
    es = FakeEs(csc_hit(), [ksc_hit("20260514"), ksc_hit("20260520"), ksc_hit("20260526")])
    assert run(es, "--reason", "x", "--apply") == 1                 # stops at 20260520
    assert submitted_dates(post) == ["20260514", "20260520"]
    assert es.csc["_source"]["metadata"][c.SKIPPED] is True

    post.reset_mock()
    wait.side_effect = None
    wait.return_value = "job-completed"
    es.update.reset_mock()
    # the date is skipped now, so a second skip is refused; --reevaluate-only finishes the job
    assert run(es, "--reason", "x", "--apply") == 1
    post.assert_not_called()
    assert run(es, "--reevaluate-only", "--apply") == 0
    es.update.assert_not_called()
    assert submitted_dates(post) == ["20260514", "20260520", "20260526"]


def test_reevaluate_only_needs_no_reason_and_refuses_nothing(mozart):
    post, _ = mozart
    # not skipped at all: the gap was resolved by late CSLCs and the KSCs behind it are still held
    es = FakeEs(csc_hit(**{c.IS_COMPLETE: True}), [ksc_hit("20260514")])
    assert run(es, "--reevaluate-only", "--apply") == 0
    assert submitted_dates(post) == ["20260514"]


def test_reevaluate_only_and_unskip_do_not_go_together():
    with pytest.raises(SystemExit):
        run(FakeEs(csc_hit()), "--reevaluate-only", "--unskip")


def test_each_job_is_followed_by_the_ksc_state(mozart, capsys):
    es = FakeEs(csc_hit(), [ksc_hit("20260514")])
    assert run(es, "--reason", "x", "--apply") == 0
    assert "20260514  job-0  job-completed  -> still held by a gap" in capsys.readouterr().out


def test_a_ksc_a_job_cannot_recompute_is_reported_not_counted(mozart, capsys):
    awaiting = ksc_hit("20260514", **{c.IS_COMPLETE: True, c.COMPRESSED_CSLC_FINAL: False,
                                      c.GAP_UNRESOLVED: False, c.COMPRESSED_CSLC_PENDING: ["20260502"]})
    es = FakeEs(csc_hit(), [awaiting])
    assert run(es, "--reason", "x", "--apply") == 0
    out = capsys.readouterr().out
    assert "not recomputed (complete, awaiting a CCSLC set" in out
    assert "20260514" in out.split("not recomputed")[1]


def test_a_skip_that_does_not_stick_is_reported(mozart):
    post, _ = mozart
    es = FakeEs(csc_hit(), [ksc_hit("20260514")], keep_csc=False)     # the update is lost
    assert run(es, "--reason", "x", "--apply") == 1
    post.assert_not_called()


def test_a_job_that_times_out_stops_the_run(mozart):
    post, wait = mozart
    wait.return_value = "timeout"
    es = FakeEs(csc_hit(), [ksc_hit("20260514"), ksc_hit("20260520")])
    assert run(es, "--reason", "x", "--apply") == 1
    assert submitted_dates(post) == ["20260514"]


def test_submitted_params_match_the_job_spec(mozart):
    import json
    import pathlib
    spec = json.loads((pathlib.Path(__file__).resolve().parents[3] / "docker"
                       / "job-spec.json.disp_s1_k_cycle_evaluator").read_text())
    post, _ = mozart
    assert run(FakeEs(csc_hit(), [ksc_hit("20260514")]), "--reason", "x", "--apply") == 0
    sent = json.loads(post.call_args.kwargs["data"]["params"])
    assert set(sent) == {p["name"] for p in spec["params"]}


def test_a_refused_skip_changes_nothing(mozart):
    post, _ = mozart
    es = FakeEs(csc_hit(), [ksc_hit("20260514", fired=True, window=[DATE, "20260514"])])
    assert run(es, "--reason", "x", "--apply") == 1
    es.update.assert_not_called()
    post.assert_not_called()


def test_list_reports_every_skipped_date_as_csv(capsys):
    es = FakeEs(csc_hit(**{c.SKIPPED: True, c.SKIP_REASON: "bursts never imaged", c.SKIPPED_BY: "op",
                           c.SKIPPED_AT: "2026-10-15T12:00:00Z"}))
    assert tool.main(["--list", "--csv"], es=es) == 0
    rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert rows == [{"frame_id": str(FRAME), "sensing_date": DATE, "coverage": "1/3",
                     "skip_reason": "bursts never imaged", "skipped_by": "op",
                     "skipped_at": "2026-10-15T12:00:00Z"}]
