"""k-set walk of the DISP-NI historical processing daemon.

Each frame's sensing dates are submitted k at a time, and a k-set waits until the compressed
GSLCs of the k-sets before it are cataloged. A waiting frame keeps its cursor and records when it
started waiting, so a frame that will never be unblocked is visible to the operator instead of
silently stalling the campaign. Only gslc_query_hist batch procs are touched.
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

from data_subscriber.gslc.gslc_utils import process_disp_ni_frame_db  # noqa: E402
import tools.run_disp_ni_historical_processing as hist  # noqa: E402

DISP_NI_DB = str(Path(__file__).parents[1] / "data_subscriber" / "test_data" / "opera-nisar-disp-consistent-gslc.json")
FRAME_DB = process_disp_ni_frame_db(DISP_NI_DB)

K = 5
M = 6
FRAME = 26410
DATES = FRAME_DB[FRAME].sensing_datetimes
NOW = datetime(2026, 10, 20, 12, 0, 0)
ARGS = types.SimpleNamespace(dry_run=False)


@pytest.fixture(autouse=True)
def job_release(monkeypatch):
    monkeypatch.setattr(hist, "JOB_RELEASE", "test-release")


@pytest.fixture
def submitted(monkeypatch):
    jobs = []

    def fake_submit(job_name, job_spec, job_params, queue, tags, priority=0):
        jobs.append(types.SimpleNamespace(name=job_name, spec=job_spec, params=job_params, queue=queue, tags=tags))
        return f"job-{len(jobs)}"

    monkeypatch.setattr(hist, "submit_job", fake_submit)
    return jobs


class FakeEs:
    """In-memory GRQ: answers compressed-GSLC lookups and merges batch proc updates like the real one."""

    def __init__(self):
        self.published = set()   # (frame_id, YYYY-MM-DD of the compressed GSLC's last date)
        self.docs = {}

    def query(self, index=None, body=None):
        frame_term, date_range = body["query"]["bool"]["must"]
        key = (frame_term["term"]["metadata.frame_id"], date_range["range"]["metadata.last_date_time"]["gte"][:10])
        return [{"_id": str(key)}] if key in self.published else []

    def update_document(self, id=None, body=None, index=None):
        doc = self.docs.setdefault(id, {})
        for key, value in body["doc"].items():
            if isinstance(value, dict) and isinstance(doc.get(key), dict):
                doc[key].update(value)
            else:
                doc[key] = value

    def publish_through(self, frame_id, position):
        """Publish the compressed GSLC of every k-set that ends before this position."""
        for end in range(K - 1, position, K):
            self.published.add((frame_id, FRAME_DB[frame_id].sensing_datetimes[end].strftime("%Y-%m-%d")))


def make_proc(**overrides):
    proc = {
        "enabled": True,
        "label": "disp_ni_hist",
        "processing_mode": "historical",
        "job_type": hist.JOB_TYPE,
        "job_queue": "opera-job_worker-gslc_data_query_hist",
        "download_job_queue": "opera-job_worker-gslc_data_download_hist",
        "include_regions": "north_america_opera",
        "exclude_regions": "",
        "chunk_size": 1,
        "k": K,
        "m": M,
        "frames": [FRAME],
        "data_start_date": "2025-07-01T00:00:00",
        "data_end_date": "2028-01-01T00:00:00",
        "wait_between_acq_cycles_mins": 0,
    }
    proc.update(overrides)
    return proc


def make_p(**overrides):
    return types.SimpleNamespace(**make_proc(**overrides))


def run_once(es, proc, args=ARGS, now=NOW):
    return hist.proc_once(es, [{"_id": "bp1", "_source": proc}], args, FRAME_DB, now=now)


# ---------------------------------------------------------------------------
# one k-set
# ---------------------------------------------------------------------------

def test_initial_frame_states_start_every_frame_at_its_first_date():
    assert hist.generate_initial_frame_states([[26410, 26411], 12573], FRAME_DB) == \
        {"26410": 0, "26411": 0, "12573": 0}


@pytest.mark.parametrize("k_set, m", [(1, 1), (2, 2), (5, 5)])
def test_k_set_window_covers_its_k_dates_and_m_ramps_up(k_set, m):
    position = (k_set - 1) * K
    es = FakeEs()
    es.publish_through(FRAME, position)

    action = hist.form_job_params(make_p(), FRAME, position, es, FRAME_DB)

    assert action.submit is True
    assert action.next_position == position + K
    assert action.job_spec == "job-gslc_query_hist:test-release"
    assert action.job_params["start_datetime"] == \
        f"--start-date={hist.convert_datetime(DATES[position] - timedelta(minutes=30))}"
    assert action.job_params["end_datetime"] == \
        f"--end-date={hist.convert_datetime(DATES[position + K - 1] + timedelta(minutes=30))}"
    assert action.job_params["m"] == f"--m={m}"
    assert action.job_params["k"] == f"--k={K}"
    assert action.job_params["frame_id"] == f"--frame-id={FRAME}"
    assert action.job_params["provider"] == "--provider=ASF"
    assert action.job_params["processing_mode"] == "--processing-mode=historical"
    assert action.job_params["include_regions"] == "--include-regions=north_america_opera"
    assert "exclude_regions" not in action.job_params
    assert action.job_params["no_schedule_download"] == ""


def test_no_schedule_download_on_the_batch_proc_reaches_the_query_job():
    action = hist.form_job_params(make_p(no_schedule_download=True), FRAME, 0, FakeEs(), FRAME_DB)

    assert action.job_params["no_schedule_download"] == "--no-schedule-download"


def test_k_set_waits_for_the_previous_compressed_gslc_and_keeps_its_cursor():
    action = hist.form_job_params(make_p(), FRAME, K, FakeEs(), FRAME_DB)

    assert action.submit is False
    assert action.blocked is True
    assert action.next_position == K
    assert action.finished is False


def test_frame_finishes_when_fewer_than_k_dates_are_left():
    position = len(DATES) - K + 1   # 36 dates, k=5: only 4 left

    action = hist.form_job_params(make_p(), FRAME, position, FakeEs(), FRAME_DB)

    assert action.submit is False
    assert action.finished is True


def test_frame_finishes_at_the_data_end_date():
    end = (DATES[K + 2]).strftime(hist.ES_DATETIME_FORMAT)
    es = FakeEs()
    es.publish_through(FRAME, K)

    action = hist.form_job_params(make_p(data_end_date=end), FRAME, K, es, FRAME_DB)

    assert action.submit is False
    assert action.finished is True


def test_last_full_k_set_is_marked_finished_when_submitted():
    position = 4 * K   # dates 20-24 of 26: no full k-set after it
    es = FakeEs()
    es.publish_through(FRAME, position)

    action = hist.form_job_params(make_p(), FRAME, position, es, FRAME_DB)

    assert action.submit is True
    assert action.finished is True


def test_k_set_before_the_data_start_date_is_stepped_over_without_submitting():
    start = (DATES[K] - timedelta(hours=1)).strftime(hist.ES_DATETIME_FORMAT)

    action = hist.form_job_params(make_p(data_start_date=start), FRAME, 0, FakeEs(), FRAME_DB)

    assert action.submit is False
    assert action.blocked is False
    assert action.next_position == K


def test_frame_missing_from_the_frame_database_is_finished_rather_than_crashing():
    action = hist.form_job_params(make_p(), 99999, 0, FakeEs(), FRAME_DB)

    assert action.submit is False
    assert action.finished is True


# ---------------------------------------------------------------------------
# proc_once
# ---------------------------------------------------------------------------

def test_proc_once_submits_the_first_k_set_and_records_progress(submitted):
    es = FakeEs()

    assert run_once(es, make_proc()) is True

    assert len(submitted) == 1
    assert submitted[0].queue == "opera-job_worker-gslc_data_query_hist"
    assert submitted[0].tags == ["data-subscriber-query-timer", "historical_processing"]
    doc = es.docs["bp1"]
    assert doc["frame_states"] == {str(FRAME): K}
    assert doc["last_run_date"] == NOW.strftime(hist.ES_DATETIME_FORMAT)
    assert doc["frame_completion_percentages"] == {str(FRAME): round(K / 25 * 100)}   # 26 dates, 25 in full k-sets
    assert doc["last_processed_datetimes"] == {str(FRAME): DATES[K - 1]}
    assert doc["frame_blocked_since"] == {}
    assert "enabled" not in doc


def test_blocked_frame_records_when_it_started_waiting_and_clears_it_once_submitted(submitted):
    es = FakeEs()
    proc = make_proc(frame_states={str(FRAME): K})

    run_once(es, proc)
    assert submitted == []
    assert es.docs["bp1"]["frame_blocked_since"] == {str(FRAME): NOW.strftime(hist.ES_DATETIME_FORMAT)}
    assert "frame_states" not in es.docs["bp1"]   # the cursor stays put

    later = NOW + timedelta(hours=5)
    run_once(es, {**proc, "frame_blocked_since": es.docs["bp1"]["frame_blocked_since"]}, now=later)
    assert es.docs["bp1"]["frame_blocked_since"] == {str(FRAME): NOW.strftime(hist.ES_DATETIME_FORMAT)}

    es.publish_through(FRAME, K)
    run_once(es, {**proc, "frame_blocked_since": es.docs["bp1"]["frame_blocked_since"]},
             now=later + timedelta(hours=1))
    assert len(submitted) == 1
    assert es.docs["bp1"]["frame_blocked_since"] == {str(FRAME): None}
    assert es.docs["bp1"]["frame_states"] == {str(FRAME): 2 * K}


def test_proc_once_walks_a_frame_to_the_end_and_disables_the_batch_proc(submitted):
    es = FakeEs()
    proc = make_proc()

    for _ in range(10):
        if es.docs.get("bp1", {}).get("enabled") is False:
            break
        position = es.docs.get("bp1", {}).get("frame_states", {}).get(str(FRAME), 0)
        es.publish_through(FRAME, position)   # the DISP-NI job of the previous k-set completed
        run_once(es, {**proc, **{k: v for k, v in es.docs.get("bp1", {}).items() if k == "frame_states"}})

    assert len(submitted) == 5   # 26 dates, k=5: five full k-sets, one date left over
    assert [j.params["m"] for j in submitted] == [f"--m={m}" for m in (1, 2, 3, 4, 5)]
    assert es.docs["bp1"]["frame_states"] == {str(FRAME): 5 * K}
    assert es.docs["bp1"]["progress_percentage"] == 100
    assert es.docs["bp1"]["enabled"] is False


def test_failed_submission_keeps_the_cursor_and_the_batch_proc_enabled(monkeypatch):
    monkeypatch.setattr(hist, "submit_job", MagicMock(side_effect=Exception("mozart down")))
    es = FakeEs()
    position = 4 * K
    es.publish_through(FRAME, position)

    assert run_once(es, make_proc(frame_states={str(FRAME): position})) is False

    assert "frame_states" not in es.docs["bp1"]
    assert "enabled" not in es.docs["bp1"]


def test_dry_run_submits_nothing_and_writes_nothing(submitted):
    es = FakeEs()

    run_once(es, make_proc(), args=types.SimpleNamespace(dry_run=True))

    assert submitted == []
    assert es.docs == {}


@pytest.mark.parametrize("overrides", [{"job_type": "cslc_query_hist"}, {"enabled": False}],
                         ids=["disp_s1_batch_proc", "disabled"])
def test_other_and_disabled_batch_procs_are_left_alone(submitted, overrides):
    es = FakeEs()

    run_once(es, make_proc(**overrides))

    assert submitted == []
    assert es.docs == {}


def test_batch_proc_waits_between_runs(submitted):
    es = FakeEs()

    run_once(es, make_proc(wait_between_acq_cycles_mins=10,
                           last_run_date=(NOW - timedelta(minutes=5)).strftime(hist.ES_DATETIME_FORMAT)))

    assert submitted == []
    assert es.docs == {}
