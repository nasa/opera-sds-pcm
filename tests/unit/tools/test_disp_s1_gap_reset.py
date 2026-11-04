"""Gap reset: restarting a DISP-S1 frame's historical processing after a multi-year gap.

The post-gap data is processed as a fresh compressed CSLC lineage -- its first ministack takes no
compressed CSLCs at all -- and the run is started by a phased batch proc whose date range begins
inside the gap. These tests drive that from the batch proc alone, against the two real cases as
the 2026-10-27 annotated burst database labels them:

* 24726: historical_01[0, 75) forward_01[75, 86) historical_02[86, 116) forward_02[116, 125).
  The post-gap phase starts at 86, which is not a multiple of k.
* 25278 (Okmok): historical_01[0, 45) historical_02[45, 60) forward_02[60, 67). Its post-gap
  ministack is 12 dates from June-October 2025 and 3 from June-July 2026: it spans the annual
  winter blackout.
"""

import logging
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

for _cluster_module in ("hysds", "hysds.celery", "hysds_commons", "hysds_commons.elasticsearch_utils",
                        "pcm_commons", "pcm_commons.query", "pcm_commons.query.ancillary_utility"):
    sys.modules.setdefault(_cluster_module, MagicMock())

from data_subscriber import cslc_utils  # noqa: E402
from data_subscriber.cslc.cslc_dependency import CSLCDependency  # noqa: E402
from data_subscriber.cslc_utils import build_ccslc_m_index  # noqa: E402
import tools.run_disp_s1_historical_processing as hist  # noqa: E402

TEST_DATA = Path(__file__).parents[1] / "data_subscriber" / "test_data"
BURST_DB = str(TEST_DATA / "disp_s1_with_processing_mode_2026_10_27_excerpt.json")
BLACKOUT = str(TEST_DATA / "disp_s1_blackout_dates_f25278.json")

K = 15
M = 6
SALOME, OKMOK = 24726, 25278
SALOME_RESET, SALOME_FORWARD, SALOME_END = 86, 116, 125
OKMOK_RESET, OKMOK_FORWARD, OKMOK_END = 45, 60, 67
NOW = datetime(2026, 11, 1, 12, 0, 0)


def load(use_processing_modes):
    return cslc_utils.process_disp_frame_burst_hist(BURST_DB, use_processing_modes=use_processing_modes)[0]


@pytest.fixture
def annotated(monkeypatch):
    frame_to_bursts = load(True)
    monkeypatch.setattr(hist, "disp_burst_map", frame_to_bursts)
    monkeypatch.setattr(hist, "blackout_dates_obj", None)
    monkeypatch.setattr(hist, "JOB_RELEASE", "test-release", raising=False)
    return frame_to_bursts


@pytest.fixture
def unannotated(monkeypatch):
    frame_to_bursts = load(False)
    monkeypatch.setattr(hist, "disp_burst_map", frame_to_bursts)
    monkeypatch.setattr(hist, "blackout_dates_obj", None)
    monkeypatch.setattr(hist, "JOB_RELEASE", "test-release", raising=False)
    return frame_to_bursts


class FakeEs:
    """In-memory GRQ: compressed CSLCs, k-cycle state configs and batch proc documents."""

    def __init__(self):
        self.published_ccslc = set()
        self.queried_ccslc = []
        self.kscs = {}
        self.docs = {}

    def query(self, index=None, body=None):
        must = body["query"]["bool"]["must"]
        if "compressed" in index:
            ccslc_m_index = must[0]["term"]["metadata.ccslc_m_index.keyword"]
            self.queried_ccslc.append(ccslc_m_index)
            return [{"_id": ccslc_m_index}] if ccslc_m_index in self.published_ccslc else []
        if "kcycle" in index:
            key = (must[0]["term"]["metadata.frame_id"], str(must[1]["term"]["metadata.sensing_date"]))
            meta = self.kscs.get(key)
            return [{"_source": {"metadata": meta}}] if meta else []
        return []

    def update_document(self, id=None, body=None, index=None):
        doc = self.docs.setdefault(id, {})
        for key, value in body["doc"].items():
            if isinstance(value, dict) and isinstance(doc.get(key), dict):
                doc[key].update(value)
            else:
                doc[key] = value

    def publish_boundary(self, frame, position):
        day_index = frame.sensing_datetime_days_index[position]
        for burst_id in frame.burst_ids:
            self.published_ccslc.add(build_ccslc_m_index(burst_id, day_index))

    def publish_ksc(self, frame_id, sensing_date):
        self.kscs[(frame_id, sensing_date)] = {
            "is_complete": True, "compressed_cslc_final": True, "save_compressed_cslc": False,
            "gap_unresolved": False, "cycles_complete": K, "cycles_expected": K}


def reset_batch_proc(frame_id, data_start_date="2025-01-01T00:00:00", phased=True):
    """A batch proc as an operator writes it for a gap reset: no frame_states, no cursor."""

    return {
        "enabled": True,
        "label": f"gap-reset-f{frame_id}",
        "processing_mode": "historical",
        "phased": phased,
        "job_type": hist.JOB_TYPE,
        "job_queue": "opera-job_worker-cslc_data_query_hist",
        "download_job_queue": "opera-job_worker-cslc_data_download_hist",
        "include_regions": "",
        "exclude_regions": "",
        "temporal": True,
        "chunk_size": 1,
        "k": K,
        "m": M,
        "frames": [frame_id],
        "data_start_date": data_start_date,
        "data_end_date": "2026-10-16T00:00:00",
        "wait_between_acq_cycles_mins": 0,
    }


def poll(es, source, submissions, monkeypatch):
    def fake_submit(job_name, job_spec, job_params, queue, tags, priority=0):
        submissions.append({"spec": job_spec, "params": job_params, "tags": tags})
        return f"job-{len(submissions)}"

    monkeypatch.setattr(hist, "submit_job", fake_submit)
    hist.proc_once(es, [{"_id": "bp1", "_source": source}], types.SimpleNamespace(dry_run=False))
    source.update(es.docs.get("bp1", {}))


def cursor(source, frame_id):
    states = source["frame_states"]
    return states.get(frame_id, states.get(str(frame_id)))


def run_to_completion(es, source, frame_id, monkeypatch, max_polls=100):
    """Poll until the batch proc disables itself, publishing what each submitted job would produce."""

    frame = hist.disp_burst_map[frame_id]
    submissions = []
    for _ in range(max_polls):
        before = len(submissions)
        poll(es, source, submissions, monkeypatch)
        if not source.get("enabled", True):
            break
        if len(submissions) > before:
            last = submissions[-1]
            if hist.JOB_TYPE in last["spec"]:
                es.publish_boundary(frame, cursor(source, frame_id) - 1)
            else:
                es.publish_ksc(frame_id, last["params"]["start_date"][:10].replace("-", ""))
    return submissions


def window(frame, start, end):
    return (f"--start-date={hist.convert_datetime(frame.sensing_datetimes[start] - timedelta(minutes=30))}",
            f"--end-date={hist.convert_datetime(frame.sensing_datetimes[end] + timedelta(minutes=30))}")


# ---------------------------------------------------------------------------
# the fixture is the real labeling
# ---------------------------------------------------------------------------

def test_fixture_frames_are_labeled_as_the_2026_10_27_database_labels_them():
    frame_to_bursts = load(True)
    layout = {frame_id: [(p.label, p.start_pos, p.end_pos) for p in frame_to_bursts[frame_id].phases]
              for frame_id in (SALOME, OKMOK)}

    assert layout[SALOME] == [("historical_01", 0, 75), ("forward_01", 75, 86),
                              ("historical_02", 86, 116), ("forward_02", 116, 125)]
    assert layout[OKMOK] == [("historical_01", 0, 45), ("historical_02", 45, 60), ("forward_02", 60, 67)]

    okmok = frame_to_bursts[OKMOK].sensing_datetimes[OKMOK_RESET:OKMOK_FORWARD]
    assert [d.year for d in okmok].count(2025) == 12 and [d.year for d in okmok].count(2026) == 3


# ---------------------------------------------------------------------------
# phased reset, from the batch proc alone
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("frame_id", [SALOME, OKMOK])
def test_reset_batch_proc_is_accepted(annotated, frame_id):
    assert cslc_utils.validate_phased_batch_proc(reset_batch_proc(frame_id), annotated) is True


@pytest.mark.parametrize("frame_id,reset,end", [(SALOME, SALOME_RESET, SALOME_RESET + K - 1),
                                                (OKMOK, OKMOK_RESET, OKMOK_FORWARD - 1)])
def test_first_poll_submits_the_post_gap_ministack_with_no_compressed_cslcs(annotated, monkeypatch,
                                                                           frame_id, reset, end):
    es = FakeEs()
    source = reset_batch_proc(frame_id)
    submissions = []

    poll(es, source, submissions, monkeypatch)

    assert len(submissions) == 1
    job = submissions[0]
    assert hist.JOB_TYPE in job["spec"]
    assert job["params"]["m"] == "--m=1"
    assert (job["params"]["start_datetime"], job["params"]["end_datetime"]) == window(annotated[frame_id], reset, end)
    assert "historical_02" in job["tags"]
    assert [t["phase"] for t in source["lineage_transitions"]] == ["historical_02"]
    assert cursor(source, frame_id) == reset + K
    assert es.queried_ccslc == []  # the reset depends on nothing


def test_salome_reset_runs_to_completion_on_its_own_compressed_cslcs(annotated, monkeypatch):
    es = FakeEs()
    frame = annotated[SALOME]
    # Pre-gap boundaries exist in GRQ; none of them may be picked up after the reset
    for boundary in range(K - 1, 75, K):
        es.publish_boundary(frame, boundary)
    pre_gap = set(es.published_ccslc)
    source = reset_batch_proc(SALOME)

    submissions = run_to_completion(es, source, SALOME, monkeypatch)

    kinds = ["historical" if hist.JOB_TYPE in s["spec"] else "forward" for s in submissions]
    assert kinds == ["historical"] * 2 + ["forward"] * (SALOME_END - SALOME_FORWARD)
    assert [s["params"]["m"] for s in submissions if hist.JOB_TYPE in s["spec"]] == ["--m=1", "--m=2"]
    assert not pre_gap & set(es.queried_ccslc)
    assert cursor(source, SALOME) == SALOME_END
    assert source["enabled"] is False


def test_okmok_reset_spans_the_winter_and_then_goes_forward(annotated, monkeypatch):
    es = FakeEs()
    source = reset_batch_proc(OKMOK, data_start_date="2022-01-01T00:00:00")

    submissions = run_to_completion(es, source, OKMOK, monkeypatch)

    kinds = ["historical" if hist.JOB_TYPE in s["spec"] else "forward" for s in submissions]
    assert kinds == ["historical"] + ["forward"] * (OKMOK_END - OKMOK_FORWARD)
    start, end = submissions[0]["params"]["start_datetime"], submissions[0]["params"]["end_datetime"]
    assert start.startswith("--start-date=2025-06-10") and end.startswith("--end-date=2026-07-18")
    assert source["enabled"] is False


def test_start_date_just_before_the_first_post_gap_acquisition_still_includes_it(annotated, monkeypatch):
    """The k-set's first acquisition is compared, not its query window, which opens 30 minutes earlier."""

    first = annotated[SALOME].sensing_datetimes[SALOME_RESET]
    source = reset_batch_proc(SALOME, data_start_date=(first - timedelta(minutes=10)).strftime(hist.ES_DATETIME_FORMAT))
    submissions = []

    poll(FakeEs(), source, submissions, monkeypatch)

    assert len(submissions) == 1 and submissions[0]["params"]["m"] == "--m=1"


def test_start_date_inside_the_post_gap_ministack_is_rejected(annotated):
    message = cslc_utils.validate_phased_batch_proc(
        reset_batch_proc(SALOME, data_start_date="2025-07-01T00:00:00"), annotated)

    assert "falls inside k-set 2025-05-29..2025-11-25 of historical_02" in message
    assert "2025-05-29T01:32:39 or earlier" in message


def after_position(frame, position):
    return (frame.sensing_datetimes[position] + timedelta(days=1)).strftime(hist.ES_DATETIME_FORMAT)


def test_start_date_at_a_later_post_gap_k_set_needs_a_preset_cursor(annotated):
    """Starting at the second post-gap k-set skips the first, whose compressed CSLCs it needs. A run
    that continues a lineage an earlier one processed says so by presetting the cursor instead."""

    proc = reset_batch_proc(SALOME, data_start_date=after_position(annotated[SALOME], SALOME_RESET + K - 1))

    message = cslc_utils.validate_phased_batch_proc(proc, annotated)
    assert "partway through the compressed CSLC lineage that begins 2025-05-29 (historical_02)" in message
    assert "after 2021-12-16T01:33:07 and on or before 2025-05-29T01:32:39" in message
    assert "frame_states" in message

    proc["frame_states"] = {str(SALOME): SALOME_RESET + K}
    assert cslc_utils.validate_phased_batch_proc(proc, annotated) is True


@pytest.mark.parametrize("frame_id,start,phase,lineage,legal", [
    (SALOME, "2021-09-01T00:00:00", "forward_01", "2016-07-09 (historical_01)",
     "on or before 2016-07-09T01:33:16"),
    (SALOME, "2026-08-01T00:00:00", "forward_02", "2025-05-29 (historical_02)",
     "after 2021-12-16T01:33:07 and on or before 2025-05-29T01:32:39"),
    (OKMOK, "2026-08-01T00:00:00", "forward_02", "2025-06-10 (historical_02)",
     "after 2021-10-17T04:56:15 and on or before 2025-06-10T04:55:46"),
])
def test_start_date_inside_a_forward_phase_is_rejected(annotated, frame_id, start, phase, lineage, legal):
    """A forward date chains onto its chunk's historical k-sets, which all start before data_start_date."""

    message = cslc_utils.validate_phased_batch_proc(reset_batch_proc(frame_id, data_start_date=start), annotated)

    assert f"({phase}), partway through the compressed CSLC lineage that begins {lineage}" in message
    assert legal in message


@pytest.mark.parametrize("frame_id", [SALOME, OKMOK])
def test_validator_and_walk_agree_on_every_start_date(annotated, monkeypatch, frame_id):
    """For a start date at, and just after, every sensing datetime: the validator predicts the
    position the walk first acts on, and a start it accepts lets the first poll submit work (or
    finish a frame with nothing left). A start rejected as mid-lineage leaves the walk waiting."""

    frame = annotated[frame_id]
    for acquired in frame.sensing_datetimes:
        for start in (acquired, acquired + timedelta(minutes=1)):
            data_start = start.strftime(hist.ES_DATETIME_FORMAT)
            p = types.SimpleNamespace(**reset_batch_proc(frame_id, data_start_date=data_start))

            position = 0
            action = hist.plan_frame_action(p, frame_id, position, FakeEs(), NOW)
            while action.skip and not action.finished:
                position = action.next_position
                action = hist.plan_frame_action(p, frame_id, position, FakeEs(), NOW)
            assert position == cslc_utils.first_phased_position_on_or_after(
                frame, 0, datetime.strptime(data_start, hist.ES_DATETIME_FORMAT), K), data_start

            verdict = cslc_utils.validate_phased_batch_proc(reset_batch_proc(frame_id, data_start_date=data_start),
                                                            annotated)
            if verdict is True:
                assert action.submit or action.finished, data_start
            elif "partway through" in verdict:
                assert not action.submit, data_start


def test_walk_names_skipped_work_as_the_cause_when_it_waits(annotated, monkeypatch, caplog):
    """A batch proc that bypassed validation still says why it is not progressing."""

    proc = reset_batch_proc(SALOME, data_start_date=after_position(annotated[SALOME], SALOME_RESET + K - 1))
    submissions = []
    with caplog.at_level(logging.WARNING, logger=hist.logger.name):
        poll(FakeEs(), proc, submissions, monkeypatch)

    assert submissions == []
    assert cursor(proc, SALOME) == SALOME_RESET + K
    assert any("data_start_date" in r.getMessage() and "Gap reset" in r.getMessage() for r in caplog.records)
    # Nothing was submitted, so nothing is reported as processed
    assert source_progress(proc, SALOME) == (0, None)


def source_progress(source, frame_id):
    return source["frame_completion_percentages"][str(frame_id)], source["last_processed_datetimes"][str(frame_id)]


def test_progress_of_a_reset_counts_only_the_post_gap_work(annotated, monkeypatch):
    source = reset_batch_proc(SALOME)
    submissions = []

    poll(FakeEs(), source, submissions, monkeypatch)

    # historical_02 (30 dates) and forward_02 (9) are the work; one 15-date k-set is submitted
    frame = annotated[SALOME]
    assert source_progress(source, SALOME) == (round(15 / 39 * 100), frame.sensing_datetimes[SALOME_RESET + K - 1])
    assert source["progress_percentage"] == round(15 / 39 * 100)


def test_dry_run_of_a_phased_reset_writes_nothing(annotated, monkeypatch):
    es = FakeEs()
    source = reset_batch_proc(SALOME)
    submit = MagicMock()
    monkeypatch.setattr(hist, "submit_job", submit)

    hist.proc_once(es, [{"_id": "bp1", "_source": source}], types.SimpleNamespace(dry_run=True))

    submit.assert_not_called()
    assert es.docs == {}


# ---------------------------------------------------------------------------
# what the query and download jobs of the reset see
# ---------------------------------------------------------------------------

def dependency(frame_to_bursts):
    return CSLCDependency(K, M, frame_to_bursts, None, None, None, None, None)


def test_query_side_gate_passes_every_batch_of_the_reset_ministack():
    """The query job checks one batch of the ministack -- whichever comes first -- before it
    submits the download, so every batch must pass with no compressed CSLCs in GRQ."""

    frame_to_bursts = load(True)
    frame = frame_to_bursts[SALOME]
    for position in range(SALOME_RESET, SALOME_RESET + K):
        assert dependency(frame_to_bursts).compressed_cslc_satisfied(
            SALOME, frame.sensing_datetime_days_index[position], FakeEs())

    # With the master switch off the lineage is the whole series and the reset is blocked
    inert = load(False)
    assert not dependency(inert).compressed_cslc_satisfied(
        SALOME, inert[SALOME].sensing_datetime_days_index[SALOME_RESET], FakeEs())


def test_download_of_the_reset_ministack_takes_nothing_and_writes_its_own_boundary():
    frame_to_bursts = load(True)
    days = frame_to_bursts[SALOME].sensing_datetime_days_index
    last = SALOME_RESET + K - 1

    assert dependency(frame_to_bursts).get_dependent_compressed_cslcs(SALOME, days[last], FakeEs()) == []
    assert dependency(frame_to_bursts).determine_k_cycle(None, days[last], SALOME) == 0
    assert dependency(frame_to_bursts).determine_k_cycle(None, days[last - 1], SALOME) != 0


def test_second_post_gap_ministack_takes_only_the_resets_compressed_cslcs():
    frame_to_bursts = load(True)
    frame = frame_to_bursts[SALOME]
    es = FakeEs()
    for boundary in list(range(K - 1, 75, K)) + [SALOME_RESET + K - 1]:
        es.publish_boundary(frame, boundary)

    ccslcs = dependency(frame_to_bursts).get_dependent_compressed_cslcs(
        SALOME, frame.sensing_datetime_days_index[SALOME_FORWARD - 1], es)

    reset_boundary = frame.sensing_datetime_days_index[SALOME_RESET + K - 1]
    assert len(ccslcs) == len(frame.burst_ids)
    assert set(es.queried_ccslc) == {build_ccslc_m_index(b, reset_boundary) for b in frame.burst_ids}


def test_okmok_historical_query_across_the_winter_returns_one_complete_ministack():
    """The query window runs from June 2025 to July 2026. CMR also holds winter acquisitions
    inside it; the blackout drops them and the full 15-date, 14-burst stack remains."""

    from data_subscriber.cmr import CMR_TIME_FORMAT
    from data_subscriber.cslc.cslc_query import CslcCmrQuery
    from data_subscriber.parser import create_parser
    from data_subscriber.query import DateTimeRange

    frame_to_bursts = load(True)
    frame = frame_to_bursts[OKMOK]
    start = frame.sensing_datetimes[OKMOK_RESET] - timedelta(minutes=30)
    end = frame.sensing_datetimes[OKMOK_FORWARD - 1] + timedelta(minutes=30)
    args = create_parser().parse_args([
        "query", "-c", "OPERA_L2_CSLC-S1_V1", f"--k={K}", "--m=1", "--processing-mode=historical",
        f"--frame-id={OKMOK}", f"--start-date={start:%Y-%m-%dT%H:%M:%SZ}", f"--end-date={end:%Y-%m-%dT%H:%M:%SZ}"])
    q = CslcCmrQuery(args, None, None, None, None, {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60},
                     BURST_DB, BLACKOUT)

    winter = [datetime(2025, 11, 1, 4, 55, 51) + timedelta(days=12 * i) for i in range(18)]
    acquisitions = frame.sensing_datetimes[OKMOK_RESET:OKMOK_FORWARD] + winter

    def factory(use_async=False, secondary=False, args=None, settings=None):
        async def query(timerange, now):
            lo = datetime.strptime(timerange.start_date, CMR_TIME_FORMAT)
            hi = datetime.strptime(timerange.end_date, CMR_TIME_FORMAT)
            return [{"granule_id": f"OPERA_L2_CSLC-S1_{b}_{a:%Y%m%dT%H%M%SZ}_{a + timedelta(days=1):%Y%m%dT%H%M%SZ}"
                                   f"_S1A_VV_v1.1"}
                    for a in acquisitions if lo <= a <= hi for b in frame.burst_ids]
        return query

    q._get_query_func = factory
    granules = q.eliminate_duplicate_granules(
        q._run_query_func(DateTimeRange(f"{start:%Y-%m-%dT%H:%M:%SZ}", f"{end:%Y-%m-%dT%H:%M:%SZ}"), NOW))

    assert {g["acquisition_cycle"] for g in granules} == \
        set(frame.sensing_datetime_days_index[OKMOK_RESET:OKMOK_FORWARD])
    assert len(granules) == K * len(frame.burst_ids)
    batches = {}
    for g in granules:
        batches.setdefault(g["download_batch_id"], []).append(g["granule_id"])
    assert [len(chunk) for chunk in q.get_download_chunks(batches)] == [K]


# ---------------------------------------------------------------------------
# un-phased batch procs that start partway through a frame
# ---------------------------------------------------------------------------

def test_unphased_walk_steps_past_k_sets_before_its_start_date(unannotated, monkeypatch, caplog):
    """The cursor used to stay on the first k-set forever: nothing was submitted for it, and only a
    submission moved the cursor."""

    frame = unannotated[OKMOK]
    es = FakeEs()
    source = reset_batch_proc(OKMOK, data_start_date="2025-06-01T00:00:00", phased=False)
    submissions = []

    with caplog.at_level(logging.WARNING, logger=hist.logger.name):
        poll(es, source, submissions, monkeypatch)

    # Without phases the lineage is the whole series, so k-set 45 waits on the three before it
    assert submissions == []
    assert cursor(source, OKMOK) == OKMOK_RESET
    assert any("data_start_date" in r.getMessage() for r in caplog.records)
    # The skipped k-sets are not work done, and nothing has been submitted yet
    assert source_progress(source, OKMOK) == (0, None)
    assert source["progress_percentage"] == 0

    # Once an earlier run's compressed CSLCs are in GRQ the walk resumes from there
    for boundary in (14, 29, 44):
        es.publish_boundary(frame, boundary)
    poll(es, source, submissions, monkeypatch)

    assert len(submissions) == 1
    assert (submissions[0]["params"]["start_datetime"], submissions[0]["params"]["end_datetime"]) == \
        window(frame, OKMOK_RESET, OKMOK_FORWARD - 1)
    assert cursor(source, OKMOK) == OKMOK_RESET + K


def test_unphased_start_date_after_the_whole_series_finishes_the_frame(unannotated, monkeypatch):
    es = FakeEs()
    source = reset_batch_proc(OKMOK, data_start_date="2026-10-15T00:00:00", phased=False)
    submissions = []

    poll(es, source, submissions, monkeypatch)
    poll(es, source, submissions, monkeypatch)

    assert submissions == []
    assert cursor(source, OKMOK) == OKMOK_END
    assert source["enabled"] is False


def test_unphased_mid_series_start_needs_a_preset_cursor(unannotated, annotated):
    """Without phases the only lineage is the whole series, so a later start is a continuation,
    which the batch proc states by presetting the cursor."""

    proc = reset_batch_proc(OKMOK, data_start_date="2025-06-01T00:00:00", phased=False)
    message = cslc_utils.validate_unphased_batch_proc(proc, unannotated)
    assert "partway through the compressed CSLC lineage that begins 2016-07-21" in message
    assert "on or before 2016-07-21T04:56:23" in message

    proc["frame_states"] = {str(OKMOK): OKMOK_RESET}
    assert cslc_utils.validate_unphased_batch_proc(proc, unannotated) is True

    # With phases, Okmok's post-gap phase starts on the k grid (45), so starting in the gap is a reset
    assert cslc_utils.validate_unphased_batch_proc(
        reset_batch_proc(OKMOK, data_start_date="2022-01-01T00:00:00", phased=False), annotated) is True


@pytest.mark.parametrize("data_start_date,cursor_preset", [
    ("2025-06-01T00:00:00", None),            # skips k-sets, submits nothing
    ("2016-01-01T00:00:00", OKMOK_RESET),     # would submit the k-set at 45
])
def test_unphased_dry_run_writes_nothing(unannotated, monkeypatch, data_start_date, cursor_preset):
    es = FakeEs()
    for boundary in (14, 29, 44):
        es.publish_boundary(unannotated[OKMOK], boundary)
    source = reset_batch_proc(OKMOK, data_start_date=data_start_date, phased=False)
    if cursor_preset is not None:
        source["frame_states"] = {str(OKMOK): cursor_preset}
    submit = MagicMock()
    monkeypatch.setattr(hist, "submit_job", submit)

    hist.proc_once(es, [{"_id": "bp1", "_source": source}], types.SimpleNamespace(dry_run=True))

    submit.assert_not_called()
    assert es.docs == {}


def test_unphased_reset_of_a_phase_off_the_k_grid_is_rejected(annotated):
    # Un-phased k-sets sit at 75 and 90; the one at 75 runs from before the gap to after it
    message = cslc_utils.validate_unphased_batch_proc(reset_batch_proc(SALOME, phased=False), annotated)
    assert "falls inside k-set 2021-08-18..2025-07-04" in message

    # Starting after it, the k-set at 90 begins 4 dates into the lineage that starts at 86
    after = annotated[SALOME].sensing_datetimes[89] + timedelta(days=1)
    message = cslc_utils.validate_unphased_batch_proc(
        reset_batch_proc(SALOME, data_start_date=after.strftime(hist.ES_DATETIME_FORMAT), phased=False), annotated)
    assert "starts 4 dates into the compressed CSLC lineage that begins 2025-05-29 (historical_02)" in message
    assert '"phased": true' in message


def test_unphased_start_date_inside_a_k_set_is_rejected(unannotated):
    inside = unannotated[OKMOK].sensing_datetimes[OKMOK_RESET + 3]
    proc = reset_batch_proc(OKMOK, data_start_date=inside.strftime(hist.ES_DATETIME_FORMAT), phased=False)

    message = cslc_utils.validate_unphased_batch_proc(proc, unannotated)

    assert "falls inside k-set 2025-06-10..2026-07-18" in message
