"""Phase walk of the DISP-S1 historical processing daemon.

A phased batch proc drives each frame through its burst database's phases:
historical blocks as k-set batch jobs, forward blocks one date at a time
through the forward pipeline, unprocessable blocks skipped, and a fresh
compressed CSLC lineage at every new historical phase.
"""

import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# The tool reaches the HySDS/PCM ES connection at module scope; those live only on a cluster.
for _cluster_module in ("hysds", "hysds.celery", "hysds_commons", "hysds_commons.elasticsearch_utils",
                        "pcm_commons", "pcm_commons.query", "pcm_commons.query.ancillary_utility"):
    sys.modules.setdefault(_cluster_module, MagicMock())

from data_subscriber import cslc_utils  # noqa: E402
from data_subscriber.cslc_utils import build_ccslc_m_index  # noqa: E402
import tools.run_disp_s1_historical_processing as hist  # noqa: E402

TEST_DATA = Path(__file__).parents[1] / "data_subscriber" / "test_data"
ANNOTATED_DB = str(TEST_DATA / "disp_s1_consistent_db_with_modes.json")
MALFORMED_DB = str(TEST_DATA / "disp_s1_consistent_db_malformed_modes.json")
LEGACY_DB = str(Path(__file__).parents[2] / "tools" / "test_consistent_db.json")

K = 15
M = 3
FRAME = 16669
# historical_01 [0, 195) forward_01 [195, 206) historical_02 [206, 236) forward_02 [236, 239)
H01, F01, H02, F02 = 0, 195, 206, 236
END = 239

NOW = datetime(2026, 7, 27, 12, 0, 0)


@pytest.fixture
def annotated_map(monkeypatch):
    frame_to_bursts, burst_to_frames, _ = cslc_utils.process_disp_frame_burst_hist(
        ANNOTATED_DB, use_processing_modes=True)
    monkeypatch.setattr(hist, "disp_burst_map", frame_to_bursts)
    monkeypatch.setattr(hist, "blackout_dates_obj", None)
    monkeypatch.setattr(hist, "JOB_RELEASE", "test-release", raising=False)
    return frame_to_bursts


class FakeEs:
    """In-memory GRQ stand-in: answers the compressed CSLC and k-cycle lookups the tool makes."""

    def __init__(self):
        self.published_ccslc = set()   # ccslc_m_index values
        self.kscs = {}                 # (frame_id, sensing_date) -> metadata
        self.docs = {}                 # batch proc doc id -> fields written

    def query(self, index=None, body=None):
        must = body["query"]["bool"]["must"]
        if "compressed" in index:
            ccslc_m_index = must[0]["term"]["metadata.ccslc_m_index.keyword"]
            return [{"_id": ccslc_m_index}] if ccslc_m_index in self.published_ccslc else []
        if "kcycle" in index:
            frame_id = must[0]["term"]["metadata.frame_id"]
            sensing_date = str(must[1]["term"]["metadata.sensing_date"])
            meta = self.kscs.get((frame_id, sensing_date))
            return [{"_source": {"metadata": meta}}] if meta else []
        return []

    def update_document(self, id=None, body=None, index=None):
        """Merge like the real document update does: nested objects merge, they do not replace."""
        doc = self.docs.setdefault(id, {})
        for key, value in body["doc"].items():
            if isinstance(value, dict) and isinstance(doc.get(key), dict):
                for subkey, subvalue in value.items():
                    if isinstance(subvalue, dict) and isinstance(doc[key].get(subkey), dict):
                        doc[key][subkey].update(subvalue)
                    else:
                        doc[key][subkey] = subvalue
            else:
                doc[key] = value

    # -- cluster simulation ------------------------------------------------

    def publish_boundary(self, frame, position):
        """Publish the compressed CSLC set a k-set produces at its last date."""
        day_index = frame.sensing_datetime_days_index[position]
        for burst_id in frame.burst_ids:
            self.published_ccslc.add(build_ccslc_m_index(burst_id, day_index))

    def publish_lineage(self, frame, start_position, end_position):
        """Publish every k-boundary of a lineage from start_position up to end_position."""
        for boundary in range(start_position + K - 1, end_position, K):
            self.publish_boundary(frame, boundary)

    def publish_ksc(self, frame_id, sensing_date, **overrides):
        meta = {"is_complete": True, "compressed_cslc_final": True, "save_compressed_cslc": False,
                "gap_unresolved": False, "cycles_complete": K, "cycles_expected": K}
        meta.update(overrides)
        self.kscs[(frame_id, sensing_date)] = meta


def make_proc(**overrides):
    proc = {
        "enabled": True,
        "label": "phased1",
        "processing_mode": "historical",
        "job_type": hist.JOB_TYPE,
        "job_queue": "opera-job_worker-cslc_data_query_hist",
        "download_job_queue": "opera-job_worker-cslc_data_download_hist",
        "include_regions": "",
        "exclude_regions": "",
        "temporal": True,
        "chunk_size": 1,
        "k": K,
        "m": M,
        "frames": [FRAME],
        "phased": True,
        "data_start_date": "2000-01-01T00:00:00",
        "data_end_date": "2030-01-01T00:00:00",
        "wait_between_acq_cycles_mins": 0,
        "frame_states": {str(FRAME): 0},
    }
    proc.update(overrides)
    return proc


def make_p(**overrides):
    return types.SimpleNamespace(**make_proc(**overrides))


def sensing_date(frame, position):
    return frame.sensing_datetimes[position].strftime(hist.SENSING_DATE_FORMAT)


# ---------------------------------------------------------------------------
# historical phases
# ---------------------------------------------------------------------------

def test_first_k_set_of_the_series_depends_on_nothing(annotated_map):
    action = hist.plan_frame_action(make_p(), FRAME, 0, FakeEs(), NOW)

    frame = annotated_map[FRAME]
    assert action.submit is True
    assert action.next_position == K
    assert action.phase_label == "historical_01"
    assert action.job_spec == f"job-{hist.JOB_TYPE}:test-release"
    assert action.job_params["m"] == "--m=1"
    assert action.job_params["k"] == f"--k={K}"
    assert action.job_params["frame_id"] == f"--frame-id={FRAME}"
    assert action.job_params["start_datetime"] == \
        f"--start-date={hist.convert_datetime(frame.sensing_datetimes[0] - timedelta(minutes=30))}"
    assert action.job_params["end_datetime"] == \
        f"--end-date={hist.convert_datetime(frame.sensing_datetimes[K - 1] + timedelta(minutes=30))}"
    assert action.new_lineage == ""  # the first processed phase is not a reset


def test_k_set_waits_for_the_previous_boundary(annotated_map):
    action = hist.plan_frame_action(make_p(), FRAME, K, FakeEs(), NOW)

    assert action.submit is False
    assert action.next_position == K  # cursor stays put until the compressed CSLCs exist


@pytest.mark.parametrize("stacks_done,expected_m", [(1, 2), (2, 3), (3, 3), (5, 3)])
def test_m_ramps_up_within_a_phase(annotated_map, stacks_done, expected_m):
    es = FakeEs()
    frame = annotated_map[FRAME]
    for stack in range(stacks_done):
        es.publish_boundary(frame, (stack + 1) * K - 1)

    action = hist.plan_frame_action(make_p(), FRAME, stacks_done * K, es, NOW)

    assert action.submit is True
    assert action.job_params["m"] == f"--m={expected_m}"


def test_new_historical_phase_starts_a_fresh_lineage(annotated_map):
    """The post-gap block's first k-set runs with m=1 even though the series is far along."""

    action = hist.plan_frame_action(make_p(), FRAME, H02, FakeEs(), NOW)

    assert action.submit is True
    assert action.phase_label == "historical_02"
    assert action.new_lineage == "historical_02"
    assert action.job_params["m"] == "--m=1"
    assert action.next_position == H02 + K


def test_second_k_set_of_a_new_phase_depends_on_the_post_gap_boundary(annotated_map):
    es = FakeEs()
    frame = annotated_map[FRAME]

    blocked = hist.plan_frame_action(make_p(), FRAME, H02 + K, es, NOW)
    assert blocked.submit is False

    es.publish_boundary(frame, H02 + K - 1)
    unblocked = hist.plan_frame_action(make_p(), FRAME, H02 + K, es, NOW)

    assert unblocked.submit is True
    assert unblocked.job_params["m"] == "--m=2"
    assert unblocked.new_lineage == ""  # only the phase's first k-set is the reset


def test_k_set_beyond_the_data_end_date_finishes_the_frame(annotated_map):
    frame = annotated_map[FRAME]
    p = make_p(data_end_date=frame.sensing_datetimes[10].strftime(hist.ES_DATETIME_FORMAT))

    action = hist.plan_frame_action(p, FRAME, 0, FakeEs(), NOW)

    assert action.submit is False
    assert action.finished is True


def test_k_set_before_the_data_start_date_is_skipped(annotated_map):
    frame = annotated_map[FRAME]
    p = make_p(data_start_date=frame.sensing_datetimes[100].strftime(hist.ES_DATETIME_FORMAT))

    action = hist.plan_frame_action(p, FRAME, 0, FakeEs(), NOW)

    assert action.submit is False
    assert action.next_position == K


# ---------------------------------------------------------------------------
# forward phases
# ---------------------------------------------------------------------------

def test_forward_date_is_submitted_one_at_a_time(annotated_map):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_lineage(frame, H01, F01)  # everything historical_01 produced

    action = hist.plan_frame_action(make_p(), FRAME, F01, es, NOW)

    assert action.submit is True
    assert action.job_spec == f"job-{hist.FORWARD_JOB_TYPE}:test-release"
    assert action.job_params == {
        "frame_ids": str(FRAME),
        "start_date": hist.convert_datetime(frame.sensing_datetimes[F01].replace(hour=0, minute=0, second=0)),
        "end_date": hist.convert_datetime(frame.sensing_datetimes[F01].replace(hour=23, minute=59, second=59)),
    }
    # The date only advances once the cascade has decided its fate
    assert action.next_position == F01
    assert action.inflight["position"] == F01
    assert action.inflight["sensing_date"] == sensing_date(frame, F01)


def test_forward_date_waits_for_its_own_lineage(annotated_map):
    action = hist.plan_frame_action(make_p(), FRAME, F01, FakeEs(), NOW)

    assert action.submit is False
    assert action.inflight is None


def test_forward_date_advances_on_a_terminal_disposition(annotated_map):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_lineage(frame, H01, F01)
    es.publish_ksc(FRAME, sensing_date(frame, F01))
    p = make_p(forward_inflight={str(FRAME): {"position": F01, "sensing_date": sensing_date(frame, F01),
                                              "submitted_at": NOW.strftime(hist.ES_DATETIME_FORMAT)}})

    action = hist.plan_frame_action(p, FRAME, F01, es, NOW)

    assert action.submit is False
    assert action.next_position == F01 + 1
    assert action.clear_inflight is True


@pytest.mark.parametrize("overrides", [
    {"superseded_by": "existing_ccslc", "is_complete": False},
    {"gap_unresolved": True, "is_complete": False},
])
def test_forward_date_advances_when_the_cascade_declines_to_fire(annotated_map, overrides):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_ksc(FRAME, sensing_date(frame, F01), **overrides)
    p = make_p(forward_inflight={str(FRAME): {"position": F01, "sensing_date": sensing_date(frame, F01),
                                              "submitted_at": NOW.strftime(hist.ES_DATETIME_FORMAT)}})

    action = hist.plan_frame_action(p, FRAME, F01, es, NOW)

    assert action.next_position == F01 + 1


def test_forward_date_keeps_waiting_while_the_cascade_works(annotated_map):
    frame = annotated_map[FRAME]
    p = make_p(forward_inflight={str(FRAME): {"position": F01, "sensing_date": sensing_date(frame, F01),
                                              "submitted_at": NOW.strftime(hist.ES_DATETIME_FORMAT)}})

    action = hist.plan_frame_action(p, FRAME, F01, FakeEs(), NOW)

    assert action.submit is False
    assert action.next_position == F01
    assert action.clear_inflight is False
    assert action.stall_reason == ""


def test_forward_date_settles_when_a_partial_window_stops_changing(annotated_map):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_ksc(FRAME, sensing_date(frame, F01), is_complete=False, compressed_cslc_final=False,
                   cycles_complete=4, cycles_expected=K, completeness_reason="window filling")
    inflight = {"position": F01, "sensing_date": sensing_date(frame, F01),
                "submitted_at": (NOW - timedelta(minutes=90)).strftime(hist.ES_DATETIME_FORMAT)}
    p = make_p(forward_inflight={str(FRAME): inflight})

    # First look records the window signature; the date is not terminal yet
    first = hist.plan_frame_action(p, FRAME, F01, es, NOW)
    assert first.next_position == F01
    assert first.inflight["signature"] == [4, "window filling"]

    # Same signature, well past the settle window: the window will never fill
    p.forward_inflight = {str(FRAME): dict(
        first.inflight, stable_since=(NOW - timedelta(minutes=60)).strftime(hist.ES_DATETIME_FORMAT))}
    settled = hist.plan_frame_action(p, FRAME, F01, es, NOW)

    assert settled.next_position == F01 + 1
    assert settled.clear_inflight is True


def test_forward_date_in_flight_too_long_is_reported_as_stalled(annotated_map):
    frame = annotated_map[FRAME]
    inflight = {"position": F01, "sensing_date": sensing_date(frame, F01),
                "submitted_at": (NOW - timedelta(hours=9)).strftime(hist.ES_DATETIME_FORMAT)}
    p = make_p(forward_inflight={str(FRAME): inflight})

    action = hist.plan_frame_action(p, FRAME, F01, FakeEs(), NOW)

    assert action.next_position == F01  # never skipped silently
    assert "in flight" in action.stall_reason


def test_last_forward_date_finishes_the_frame(annotated_map):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_ksc(FRAME, sensing_date(frame, END - 1))
    p = make_p(forward_inflight={str(FRAME): {"position": END - 1,
                                              "sensing_date": sensing_date(frame, END - 1),
                                              "submitted_at": NOW.strftime(hist.ES_DATETIME_FORMAT)}})

    action = hist.plan_frame_action(p, FRAME, END - 1, es, NOW)

    assert action.next_position == END
    assert action.finished is True


# ---------------------------------------------------------------------------
# no_run, quarantine, legacy frames
# ---------------------------------------------------------------------------

def test_no_run_block_is_skipped_whole(annotated_map):
    # Frame 18905 opens with a four-date chunk too short to bootstrap a lineage
    action = hist.plan_frame_action(make_p(), 18905, 0, FakeEs(), NOW)

    assert action.submit is False
    assert action.next_position == 4
    assert action.finished is False


def test_frame_with_nothing_processable_is_trivially_finished(annotated_map):
    action = hist.plan_frame_action(make_p(), 46294, 0, FakeEs(), NOW)

    assert action.submit is False
    assert action.finished is True
    assert action.quarantine_reason == ""


def test_unannotated_frame_in_a_phased_batch_proc_is_quarantined(annotated_map):
    action = hist.plan_frame_action(make_p(), 99999, 0, FakeEs(), NOW)

    assert action.submit is False
    assert "annotation" in action.quarantine_reason


def test_frame_with_bad_annotations_is_quarantined(monkeypatch):
    frame_to_bursts, _, _ = cslc_utils.process_disp_frame_burst_hist(
        MALFORMED_DB, use_processing_modes=True)
    monkeypatch.setattr(hist, "disp_burst_map", frame_to_bursts)

    action = hist.plan_frame_action(make_p(), 1003, 0, FakeEs(), NOW)

    assert action.submit is False
    assert "not a multiple" in action.quarantine_reason


# ---------------------------------------------------------------------------
# persistence and the legacy path
# ---------------------------------------------------------------------------

def test_state_is_persisted_for_a_submitted_forward_date(annotated_map, monkeypatch):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_lineage(frame, H01, F01)
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: "job-1")
    p = make_p(frame_states={str(FRAME): F01})
    args = types.SimpleNamespace(dry_run=False)

    finished, job_success = hist.proc_phased_frame(es, "bp1", p, str(FRAME), F01, args, NOW)

    assert (finished, job_success) == (False, True)
    doc = es.docs["bp1"]
    assert doc["forward_inflight"][str(FRAME)]["job_id"] == "job-1"
    assert doc["forward_inflight"][str(FRAME)]["position"] == F01
    assert "frame_states" not in doc  # the cursor only moves on a terminal disposition


def test_lineage_transition_is_recorded_once(annotated_map, monkeypatch):
    es = FakeEs()
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: "job-2")
    p = make_p(frame_states={str(FRAME): H02})
    args = types.SimpleNamespace(dry_run=False)

    hist.proc_phased_frame(es, "bp1", p, str(FRAME), H02, args, NOW)
    hist.proc_phased_frame(es, "bp1", p, str(FRAME), H02, args, NOW)

    transitions = es.docs["bp1"]["lineage_transitions"]
    assert transitions == [{"frame": FRAME, "phase": "historical_02",
                            "timestamp": NOW.strftime(hist.ES_DATETIME_FORMAT)}]
    assert es.docs["bp1"]["frame_states"][str(FRAME)] == H02 + K


def test_finished_forward_date_is_really_cleared(annotated_map, monkeypatch):
    """The document update merges objects, so a cleared entry has to be written as a null."""

    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_ksc(FRAME, sensing_date(frame, F01))
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: "job-1")
    inflight = {"position": F01, "sensing_date": sensing_date(frame, F01),
                "submitted_at": NOW.strftime(hist.ES_DATETIME_FORMAT),
                "signature": [3, "window filling"],
                "stable_since": (NOW - timedelta(hours=4)).strftime(hist.ES_DATETIME_FORMAT)}
    es.docs["bp1"] = {"forward_inflight": {str(FRAME): dict(inflight)}}
    p = make_p(frame_states={str(FRAME): F01}, forward_inflight={str(FRAME): dict(inflight)})
    args = types.SimpleNamespace(dry_run=False)

    hist.proc_phased_frame(es, "bp1", p, str(FRAME), F01, args, NOW)

    assert es.docs["bp1"]["forward_inflight"][str(FRAME)] is None
    assert hist.live_entries(es.docs["bp1"]["forward_inflight"]) == {}


def test_new_forward_date_does_not_inherit_the_previous_settle_clock(annotated_map, monkeypatch):
    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_lineage(frame, H01, F01)
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: "job-2")
    stale = {"position": F01, "sensing_date": sensing_date(frame, F01),
             "submitted_at": (NOW - timedelta(hours=6)).strftime(hist.ES_DATETIME_FORMAT),
             "signature": [3, "window filling"],
             "stable_since": (NOW - timedelta(hours=5)).strftime(hist.ES_DATETIME_FORMAT)}
    es.docs["bp1"] = {"forward_inflight": {str(FRAME): dict(stale)}}
    p = make_p(frame_states={str(FRAME): F01 + 1}, forward_inflight={})
    args = types.SimpleNamespace(dry_run=False)

    hist.proc_phased_frame(es, "bp1", p, str(FRAME), F01 + 1, args, NOW)

    written = es.docs["bp1"]["forward_inflight"][str(FRAME)]
    assert written["position"] == F01 + 1
    assert written["signature"] is None
    assert written["stable_since"] is None


def test_quarantine_is_lifted_once_the_frame_is_usable(annotated_map, monkeypatch):
    es = FakeEs()
    es.docs["bp1"] = {"quarantined_frames": {str(FRAME): "stale reason"}}
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: "job-3")
    p = make_p(frame_states={str(FRAME): 0}, quarantined_frames={str(FRAME): "stale reason"})
    args = types.SimpleNamespace(dry_run=False)

    hist.proc_phased_frame(es, "bp1", p, str(FRAME), 0, args, NOW)

    assert es.docs["bp1"]["quarantined_frames"][str(FRAME)] is None


def test_failed_submission_does_not_finish_the_frame(annotated_map, monkeypatch):
    """Otherwise proc_once disables the batch proc with the failed k-set still outstanding."""

    es = FakeEs()
    frame = annotated_map[FRAME]
    es.publish_lineage(frame, H02, END)
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: False)
    p = make_p(frame_states={str(FRAME): H02 + K})
    args = types.SimpleNamespace(dry_run=False)

    finished, job_success = hist.proc_phased_frame(es, "bp1", p, str(FRAME), H02 + K, args, NOW)

    assert (finished, job_success) == (False, False)


def test_dry_run_submits_nothing_and_writes_nothing(annotated_map, monkeypatch):
    es = FakeEs()
    submitted = []
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: submitted.append(a) or "job-x")
    p = make_p(frame_states={str(FRAME): 0})
    args = types.SimpleNamespace(dry_run=True)

    hist.proc_phased_frame(es, "bp1", p, str(FRAME), 0, args, NOW)

    assert submitted == []
    assert es.docs == {}
    assert p.frame_states[str(FRAME)] == 0


def test_failed_submission_leaves_the_cursor_alone(annotated_map, monkeypatch):
    es = FakeEs()
    monkeypatch.setattr(hist, "submit_job", lambda *a, **kw: False)
    p = make_p(frame_states={str(FRAME): 0})
    args = types.SimpleNamespace(dry_run=False)

    finished, job_success = hist.proc_phased_frame(es, "bp1", p, str(FRAME), 0, args, NOW)

    assert job_success is False
    assert p.frame_states[str(FRAME)] == 0
    assert "frame_states" not in es.docs.get("bp1", {})


@pytest.mark.parametrize("position", [0, K, H02, F01])
def test_master_switch_off_reproduces_the_unannotated_database(monkeypatch, position):
    """Golden master: deploying the annotated database with the switch off changes nothing."""

    inert_map, _, _ = cslc_utils.process_disp_frame_burst_hist(ANNOTATED_DB, use_processing_modes=False)
    legacy_map, _, _ = cslc_utils.process_disp_frame_burst_hist(LEGACY_DB, use_processing_modes=False)
    p = make_p(phased=False)
    monkeypatch.setattr(hist, "blackout_dates_obj", None)
    monkeypatch.setattr(hist, "JOB_RELEASE", "test-release", raising=False)

    monkeypatch.setattr(hist, "disp_burst_map", inert_map)
    inert_result = hist.form_job_params(p, FRAME, position, None, FakeEs())
    monkeypatch.setattr(hist, "disp_burst_map", legacy_map)
    legacy_result = hist.form_job_params(p, FRAME, position, None, FakeEs())

    assert inert_result == legacy_result


def run_daemon(es, source, monkeypatch, doc_id="bp1", max_polls=200, stop_after=None):
    """Poll proc_once until the batch proc completes, simulating the cluster in between.

    Between polls the batch proc document round-trips through the fake ES exactly as it does on a
    cluster, so this also exercises restarting the daemon from persisted state.
    """
    submissions = []
    frame = hist.disp_burst_map[FRAME]

    def fake_submit(job_name, job_spec, job_params, queue, tags, priority=0):
        submissions.append({"spec": job_spec, "params": job_params, "queue": queue, "tags": tags,
                            "name": job_name})
        return f"job-{len(submissions)}"

    monkeypatch.setattr(hist, "submit_job", fake_submit)
    args = types.SimpleNamespace(dry_run=False)

    for _ in range(max_polls):
        submitted_before = len(submissions)
        hist.proc_once(es, [{"_id": doc_id, "_source": source}], args)
        source.update(es.docs.get(doc_id, {}))

        if not source.get("enabled", True):
            break
        if stop_after is not None and len(submissions) >= stop_after:
            break

        if len(submissions) > submitted_before:
            last = submissions[-1]
            if hist.JOB_TYPE in last["spec"]:
                # the historical batch publishes this k-set's compressed CSLCs at its last date
                es.publish_boundary(frame, source["frame_states"][str(FRAME)] - 1)
            else:
                es.publish_ksc(FRAME, last["params"]["start_date"][:10].replace("-", ""))

    return submissions


def test_full_phase_walk_of_a_four_phase_frame(annotated_map, monkeypatch):
    es = FakeEs()
    source = make_proc()

    submissions = run_daemon(es, source, monkeypatch)

    kinds = ["historical" if hist.JOB_TYPE in s["spec"] else "forward" for s in submissions]
    # historical_01 is 13 k-sets, forward_01 11 dates, historical_02 2 k-sets, forward_02 3 dates
    assert kinds == (["historical"] * 13 + ["forward"] * 11
                     + ["historical"] * 2 + ["forward"] * 3)

    # The post-gap block bootstraps a fresh lineage: its first k-set takes no compressed CSLCs
    post_gap = [s for s in submissions if hist.JOB_TYPE in s["spec"]][13]
    assert post_gap["params"]["m"] == "--m=1"
    assert source["lineage_transitions"] == [{"frame": FRAME, "phase": "historical_02",
                                              "timestamp": source["lineage_transitions"][0]["timestamp"]}]
    assert [t["phase"] for t in source["lineage_transitions"]] == ["historical_02"]

    assert source["frame_states"][str(FRAME)] == END
    assert source["progress_percentage"] == 100
    assert source["enabled"] is False
    assert hist.live_entries(source.get("quarantined_frames")) == {}
    assert hist.live_entries(source.get("forward_inflight")) == {}

    # Nothing is ever submitted twice: submission is not deduplicated by Mozart
    windows = [(s["spec"], s["params"].get("start_datetime") or s["params"]["start_date"])
               for s in submissions]
    assert len(windows) == len(set(windows))


def test_daemon_restart_does_not_resubmit_an_in_flight_forward_date(annotated_map, monkeypatch):
    es = FakeEs()
    source = make_proc()

    # Stop right after the first forward date is submitted, before its disposition lands
    first_pass = run_daemon(es, source, monkeypatch, stop_after=14)
    assert hist.FORWARD_JOB_TYPE in first_pass[-1]["spec"]
    assert source["forward_inflight"][str(FRAME)]["position"] == F01

    # A restarted daemon rebuilds from the document alone
    restarted = run_daemon(es, dict(source), monkeypatch, max_polls=3)

    assert restarted == []  # the date is already in flight; nothing is resubmitted


def test_pure_no_run_frames_complete_without_any_jobs(annotated_map, monkeypatch):
    es = FakeEs()
    source = make_proc(frames=[46294], frame_states={"46294": 0})

    submissions = run_daemon(es, source, monkeypatch, max_polls=5)

    assert submissions == []
    assert source["enabled"] is False
    assert source["frame_completion_percentages"]["46294"] == 100


def test_quarantined_frame_does_not_stop_the_others(annotated_map, monkeypatch):
    es = FakeEs()
    source = make_proc(frames=[FRAME, 99999], frame_states={str(FRAME): H02 + K, "99999": 0})
    es.publish_boundary(annotated_map[FRAME], H02 + K - 1)

    submissions = run_daemon(es, source, monkeypatch, max_polls=30)

    assert "99999" in source["quarantined_frames"]
    assert [s["params"]["m"] for s in submissions if hist.JOB_TYPE in s["spec"]] == ["--m=2"]
    # A quarantined frame keeps the batch proc open for operator attention
    assert source["enabled"] is True


def test_legacy_batch_proc_keeps_the_un_phased_walk(annotated_map):
    """Without the opt-in, the cursor still steps k dates at a time from the series start."""

    p = make_p(phased=False)
    assert hist.batch_proc_is_phased(p) is False

    do_submit, _, _, job_params, _, next_position, _ = hist.form_job_params(
        p, FRAME, H02, None, FakeEs())

    assert next_position == H02 + K
    # The m ramp of the un-phased walk is absolute: deep in the series it is always the full m
    assert job_params["m"] == f"--m={M}"
    assert do_submit is True
