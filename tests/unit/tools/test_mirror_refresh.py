"""Unit tests for tools/analysis/mirror_refresh.py (analysis-mirror refresh)."""

import argparse
import importlib.util
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "tools" / "analysis" / "mirror_refresh.py"


@pytest.fixture(scope="module")
def mr():
    spec = importlib.util.spec_from_file_location("_mirror_refresh", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


NOW = datetime(2026, 11, 12, 15, 0, 0)


def ms(moment):
    return int((moment - datetime(1970, 1, 1)).total_seconds() * 1000)


def snap(name, start, policy=None, state="SUCCESS"):
    entry = {"snapshot": name, "state": state, "start_time_in_millis": ms(start)}
    if policy:
        entry["metadata"] = {"sm_policy": policy, "any_key": "any_value"}
    return entry


class FakeClient(object):
    """Just enough of the OpenSearch REST surface for one venue named "v"."""

    def __init__(self, snapshots, statuses, local, list_errors=None):
        self.snapshots = snapshots
        self.statuses = statuses  # snapshot -> {index: (file_count, size)}
        self.local = set(local)  # source names present on the mirror
        self.list_errors = list(list_errors or [])
        self.deleted = []
        self.restored = []  # (snapshot, index)
        self.calls = []

    def request(self, path, method="GET", body=None, timeout=120):
        self.calls.append((method, path))
        if path.startswith("/_snapshot/repo/_all"):
            if self.list_errors:
                return None, self.list_errors.pop(0)
            return {"snapshots": self.snapshots}, None
        match = re.match(r"^/_snapshot/repo/([^/]+)/_status$", path)
        if match:
            indices = self.statuses[match.group(1)]
            return {"snapshots": [{"indices": {
                name: {"stats": {"total": {"file_count": fc, "size_in_bytes": size}}}
                for name, (fc, size) in indices.items()
            }}]}, None
        if path.startswith("/_cat/indices/v_"):
            return [{"index": "v_" + name} for name in sorted(self.local)], None
        match = re.match(r"^/_snapshot/repo/([^/]+)/_restore", path)
        if match:
            self.restored.append((match.group(1), body["indices"]))
            self.local.add(body["indices"])
            return {"accepted": True}, None
        if path.startswith("/_cluster/health/"):
            return {"status": "green"}, None
        if method == "DELETE":
            name = path[1:]
            self.deleted.append(name)
            self.local.discard(name[len("v_"):])
            return {"acknowledged": True}, None
        raise AssertionError("unexpected request %s %s" % (method, path))


def make_args(mr, **overrides):
    values = dict(
        dry_run=False, force=False, init_state=False, init_snapshots={},
        exclude=list(mr.DEFAULT_EXCLUDE_PATTERNS),
        daily_retention_days=30, series_max_lag_hours=24, max_changed=250,
        replicas=0, index_timeout=60,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


# Two active SM series plus the leftovers a 2.9 -> 3.x redeploy leaves behind.
SNAPSHOTS = [
    snap("hourly-snapshot-2026-10-08-20:00-old", datetime(2026, 10, 8, 20), "hourly-snapshot"),
    snap("teardown_2026-10-08", datetime(2026, 10, 8, 20, 19)),
    snap("hourly-snapshot-grq-2026-11-12-13:00-a", datetime(2026, 11, 12, 13), "hourly-snapshot-grq"),
    snap("hourly-snapshot-grq-2026-11-12-14:00-b", datetime(2026, 11, 12, 14), "hourly-snapshot-grq"),
    snap("hourly-snapshot-grq-2026-11-12-15:00-c", datetime(2026, 11, 12, 15), "hourly-snapshot-grq",
         state="PARTIAL"),
    snap("hourly-snapshot-mozart-metrics-2026-11-12-14:30-d", datetime(2026, 11, 12, 14, 30),
         "hourly-snapshot-mozart-metrics"),
]
GRQ = "hourly-snapshot-grq-2026-11-12-14:00-b"
MM = "hourly-snapshot-mozart-metrics-2026-11-12-14:30-d"


def successful(snapshots):
    return [s for s in snapshots if s["state"] == "SUCCESS"]


# ---------------------------------------------------------------------------
# Snapshot selection
# ---------------------------------------------------------------------------

def test_select_takes_newest_successful_snapshot_per_active_policy(mr):
    selected, ignored = mr.select_snapshots(successful(SNAPSHOTS), 24)

    assert [s["snapshot"] for s in selected] == [MM, GRQ]
    assert [s["series"] for s in selected] == ["hourly-snapshot-mozart-metrics",
                                               "hourly-snapshot-grq"]
    notes = " ".join(ignored)
    assert "1 snapshot(s) with no SM policy" in notes
    assert "series hourly-snapshot:" in notes


def test_select_falls_back_to_newest_when_no_policy_snapshots(mr):
    snapshots = [
        snap("manual-a", datetime(2026, 7, 1)),
        snap("manual-b", datetime(2026, 7, 2)),
    ]
    selected, ignored = mr.select_snapshots(snapshots, 24)

    assert [s["snapshot"] for s in selected] == ["manual-b"]
    assert ignored == []


def test_select_empty_repository(mr):
    assert mr.select_snapshots([], 24) == ([], [])


def test_pin_snapshots_reports_unknown_name(mr):
    selected, error = mr.pin_snapshots(successful(SNAPSHOTS), [GRQ, "nope"])
    assert selected is None
    assert "nope" in error


def test_list_snapshots_retries_a_race_with_retention(mr, monkeypatch):
    monkeypatch.setattr(mr.time, "sleep", lambda seconds: None)
    client = FakeClient(SNAPSHOTS, {}, [], list_errors=[
        'HTTP 404: {"type":"snapshot_missing_exception"}',
    ])
    snapshots, error = mr.list_snapshots(client, "repo")

    assert error is None
    assert len(snapshots) == len(successful(SNAPSHOTS))


# ---------------------------------------------------------------------------
# Per-venue refresh
# ---------------------------------------------------------------------------

def statuses():
    return {
        GRQ: {
            "grq_v1.0_l2_rtc_s1-2026.11": (10, 1000),
            "grq_v1.0_l2_rtc_s1-2026.10": (8, 800),
            "rtc_catalog-2026.11": (4, 40),
            "top_queries-2026.11.12-89283": (1, 1),
        },
        MM: {
            "job_status-2026.11.12": (3, 30),
            "job_status-2026.10.01": (3, 30),
            "logstash-2026.11.11": (2, 20),
        },
    }


def test_refresh_restores_changed_and_missing_from_the_right_snapshot(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=[
        "grq_v1.0_l2_rtc_s1-2026.11",  # changed fingerprint
        "logstash-2026.11.11",         # unchanged
        "job_status-2026.10.01",       # daily past retention
        "job_status-2026.09.01",       # daily past retention, already gone from source
        "grq_v1.0_l2_rtc_s1-2026.05",  # monthly gone from source: kept
    ])
    state = {"venues": {"v": {"fingerprints": {
        "grq_v1.0_l2_rtc_s1-2026.11": "9:900",
        "grq_v1.0_l2_rtc_s1-2026.10": "8:800",  # matches, but missing locally
        "logstash-2026.11.11": "2:20",
        "job_status-2026.10.01": "3:30",
        "grq_v1.0_l2_rtc_s1-2026.05": "1:1",
    }, "last_snapshot": "legacy"}}}

    run = mr.refresh_venue(client, "v", "repo", state, make_args(mr), NOW)

    assert run.failed == 0, run.errors
    assert sorted(client.restored) == sorted([
        (GRQ, "grq_v1.0_l2_rtc_s1-2026.10"),
        (GRQ, "grq_v1.0_l2_rtc_s1-2026.11"),
        (GRQ, "rtc_catalog-2026.11"),
        (MM, "job_status-2026.11.12"),
    ])
    assert sorted(client.deleted) == sorted([
        "v_job_status-2026.10.01", "v_job_status-2026.09.01",  # pruned
        "v_grq_v1.0_l2_rtc_s1-2026.11", "v_grq_v1.0_l2_rtc_s1-2026.10",
        "v_rtc_catalog-2026.11", "v_job_status-2026.11.12",  # delete-before-restore
    ])
    assert run.pruned == 2
    assert run.unchanged == 1
    assert "grq_v1.0_l2_rtc_s1-2026.05" in client.local

    venue = state["venues"]["v"]
    assert "last_snapshot" not in venue
    assert "job_status-2026.10.01" not in venue["fingerprints"]
    assert "top_queries-2026.11.12-89283" not in venue["fingerprints"]
    assert venue["fingerprints"]["grq_v1.0_l2_rtc_s1-2026.11"] == "10:1000"
    assert venue["data_as_of"] == "2026-11-12T14:00:00Z"  # the older of the two series
    assert [s["snapshot"] for s in venue["snapshots"]] == [MM, GRQ]


def test_refresh_dry_run_changes_nothing(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=["job_status-2026.10.01"])
    state = {"venues": {}}

    run = mr.refresh_venue(client, "v", "repo", state, make_args(mr, dry_run=True), NOW)

    assert client.deleted == []
    assert client.restored == []
    assert run.pruned == 1
    assert run.restored == 5


def test_refresh_guard_blocks_mass_restore_and_keeps_old_freshness(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=[])
    state = {"venues": {"v": {"data_as_of": "2026-11-01T00:00:00Z"}}}

    run = mr.refresh_venue(client, "v", "repo", state, make_args(mr, max_changed=2), NOW)

    assert run.failed == 1
    assert "--max-changed" in run.errors[0]
    assert client.restored == []
    assert state["venues"]["v"]["data_as_of"] == "2026-11-01T00:00:00Z"

    client = FakeClient(SNAPSHOTS, statuses(), local=[])
    run = mr.refresh_venue(client, "v", "repo", state,
                           make_args(mr, max_changed=2, force=True), NOW)
    assert run.failed == 0
    assert len(client.restored) == 5


def test_refresh_init_state_adopts_without_touching_the_mirror(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=["job_status-2026.10.01"])
    state = {"venues": {}}
    args = make_args(mr, init_state=True, max_changed=1, init_snapshots={"v": [GRQ, MM]})

    run = mr.refresh_venue(client, "v", "repo", state, args, NOW)

    assert run.failed == 0
    assert client.deleted == [] and client.restored == []
    assert set(state["venues"]["v"]["fingerprints"]) == {
        "grq_v1.0_l2_rtc_s1-2026.11", "grq_v1.0_l2_rtc_s1-2026.10",
        "rtc_catalog-2026.11", "job_status-2026.11.12", "logstash-2026.11.11",
    }


def test_refresh_records_restore_failure_and_retries_next_time(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=[])
    original = client.request

    def shard_limit(path, method="GET", body=None, timeout=120):
        if "_restore" in path and body["indices"] == "rtc_catalog-2026.11":
            return None, "HTTP 400: maximum shards open"
        return original(path, method, body, timeout)

    client.request = shard_limit
    state = {"venues": {}}

    run = mr.refresh_venue(client, "v", "repo", state, make_args(mr), NOW)

    assert run.failed == 1
    assert "maximum shards open" in run.errors[0]
    assert "rtc_catalog-2026.11" not in state["venues"]["v"]["fingerprints"]
    assert "data_as_of" not in state["venues"]["v"]


def test_refresh_reports_unreadable_snapshot_status(mr):
    client = FakeClient(SNAPSHOTS, statuses(), local=[])
    original = client.request

    def old_mirror(path, method="GET", body=None, timeout=120):
        if path.endswith("/_status"):
            return None, "HTTP 500: Can't get text on a START_ARRAY"
        return original(path, method, body, timeout)

    client.request = old_mirror
    run = mr.refresh_venue(client, "v", "repo", {"venues": {}}, make_args(mr), NOW)

    assert run.failed == 1
    assert "START_ARRAY" in run.errors[0]


# ---------------------------------------------------------------------------
# Freshness and alerts
# ---------------------------------------------------------------------------

def test_freshness(mr):
    assert mr.freshness({}, NOW, 6) == (None, True)
    lag, stale = mr.freshness({"data_as_of": "2026-11-12T13:00:00Z"}, NOW, 6)
    assert (round(lag, 1), stale) == (2.0, False)
    lag, stale = mr.freshness({"data_as_of": "2026-09-30T00:00:00Z"}, NOW, 6)
    assert stale


def test_alert_lifecycle(mr):
    entry = {}
    assert mr.alert_decision(entry, False, NOW, 24) is None

    assert mr.alert_decision(entry, True, NOW, 24) == "raise"
    mr.record_alert(entry, "raise", NOW)
    assert mr.alert_decision(entry, True, NOW + timedelta(hours=2), 24) is None
    assert mr.alert_decision(entry, True, NOW + timedelta(hours=24), 24) == "repeat"

    assert mr.alert_decision(entry, False, NOW + timedelta(hours=25), 24) == "clear"
    mr.record_alert(entry, "clear", NOW)
    assert entry == {"alerting": False}


def test_alert_message_includes_errors_and_shard_pressure(mr):
    run = mr.VenueRun("fwd", "snapshot-repo-fwd")
    run.fail("restore fwd_x: maximum shards open")
    subject, body = mr.alert_message(
        "raise", run, {"data_as_of": "2026-09-30T00:00:00Z"}, 1023.0, (5000, 5000), 6)

    assert subject == "OPERA analysis mirror: fwd is stale"
    assert "maximum shards open" in body
    assert "5000/5000  <-- near the limit" in body


def test_send_alert_runs_command_with_subject_and_body(mr, tmp_path):
    out = tmp_path / "alert.txt"
    command = 'printf "%%s\\n" "$MIRROR_ALERT_SUBJECT" > %s; cat >> %s' % (out, out)

    assert mr.send_alert(command, "subj", "line one\n")
    assert out.read_text() == "subj\nline one\n"
    assert not mr.send_alert("exit 3", "subj", "body")
    assert mr.send_alert(None, "subj", "body")


def test_daily_date(mr):
    assert mr.daily_date("job_status-2026.11.12") == datetime(2026, 11, 12)
    assert mr.daily_date("grq_v1.0_l2_rtc_s1-2026.11") is None
    assert mr.daily_date("top_queries-2026.11.12-89283") is None
    assert mr.daily_date("bad-2026.13.40") is None
