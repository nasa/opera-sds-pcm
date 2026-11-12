#!/usr/bin/env python3
"""Incrementally refresh the OPERA analysis mirror from cluster snapshots.

The mirror is a single-node OpenSearch instance that serves read-only copies of
OPERA clusters (OPS-FWD and OPS-POP1 by default, plus any venue added with
``--venue``) so analysts can query operational data without touching
production.

All venues are restored into one cluster, so every index carries a venue prefix
(``fwd_*`` / ``pop1_*``). 162 index names collide between OPS-FWD and OPS-POP1
-- most of the operational time-series plus several ``grq_`` product indices
that hold genuinely different documents per venue -- so no index keeps its
canonical name. A query that omits the venue matches nothing rather than
silently returning half the picture.

Snapshot selection: a venue's repository can hold several snapshot series. A
cluster on OpenSearch 3.x runs one snapshot-management (SM) policy for GRQ
indices and another for mozart/metrics indices, each writing its own snapshots
into the same repository, so no single snapshot covers the venue. The refresh
takes the newest successful snapshot of every active SM policy and restores
each index from whichever of those holds it. Snapshots that carry no SM policy
(manual and teardown snapshots) are ignored, and so is any policy whose newest
snapshot lags the venue's newest by more than ``--series-max-lag-hours`` -- a
retired policy's last snapshot would otherwise resurrect indices the source
has since deleted.

Change detection compares a per-index fingerprint (file count + byte size taken
from the snapshot ``_status`` API) against the fingerprint recorded on the last
successful restore. Indices whose fingerprint is unchanged are skipped. Because
the comparison is against our own last-restored state rather than against the
previous snapshot, skipping refresh cycles is safe: anything that changed while
we were not looking is still detected on the next run. An index that is missing
on the mirror is always treated as changed, so a wiped or rebuilt mirror
re-seeds itself instead of trusting stale state.

Each changed index is deleted locally and restored directly from the snapshot,
which preserves the snapshot's own mappings exactly. There is no reindex step
and no dependency on registered index templates.

Retention: daily indices (names ending in ``YYYY.MM.DD``) are kept for
``--daily-retention-days`` and then deleted from the mirror, whether or not the
source still holds them. Without a cap the mirror keeps every daily index the
source ever had, until the cluster shard limit is reached and no new index can
be restored at all. Other indices that leave the source are kept.

Freshness: every run records, per venue, ``data_as_of`` -- the time of the
oldest snapshot the venue is fully caught up to -- in the ``mirror_status``
index on the mirror itself, so consumers can check how current the data is.
When a venue falls more than ``--stale-hours`` behind, ``--alert-cmd`` is run
with the details on stdin; it runs again every ``--alert-repeat-hours`` while
the venue stays stale, and once more when it recovers.

Requires only the Python standard library (Python 3.6+).

Usage:
    # Normal incremental refresh of the default venues
    mirror_refresh.py

    # Preview
    mirror_refresh.py --dry-run

    # Re-seed an empty mirror (every index is missing, so everything restores)
    mirror_refresh.py --force

    # Record current state without restoring (after restoring by hand)
    mirror_refresh.py --init-state
"""

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch

DEFAULT_ES_URL = "https://100.104.82.14:9200"
STATE_FILE = os.path.expanduser("~/.opera_mirror_refresh_state.json")

# venue prefix -> snapshot repository holding that venue's hourly snapshots
DEFAULT_VENUES = {
    "fwd": "snapshot-repo-fwd",
    "pop1": "snapshot-repo-pop1",
}

# Indices never mirrored. The source snapshot policies already omit system
# indices, so ".*" is a backstop. Note we deliberately do NOT exclude
# user_rules-* the way a same-name mirror must: the venue prefix means
# fwd_user_rules-mozart can never be mistaken for a live rule index.
DEFAULT_EXCLUDE_PATTERNS = [
    ".*",
    "restore_temp_*",
    # OpenSearch 3.x query-insights bookkeeping. Not dot-prefixed, so a GRQ
    # policy snapshotting "*" picks it up.
    "top_queries-*",
]

# Refuse to restore more than this many indices in one run unless --force is
# given. Normal hourly churn is a few dozen; a sudden jump to "everything
# changed" usually means the state file was lost, the mirror was wiped, or the
# source venue was redeployed, and blindly acting on it would re-seed the whole
# venue and hammer the box for hours.
DEFAULT_MAX_CHANGED = 250

DEFAULT_DAILY_RETENTION_DAYS = 30
DEFAULT_STALE_HOURS = 6
DEFAULT_SERIES_MAX_LAG_HOURS = 24
DEFAULT_ALERT_REPEAT_HOURS = 24
DEFAULT_STATUS_INDEX = "mirror_status"

# Shard usage at or above this fraction of the cluster limit is called out in
# the log and in alerts; at the limit every new index fails to restore.
SHARD_WARN_FRACTION = 0.9

DAILY_SUFFIX = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})$")

STATUS_INDEX_BODY = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "venue": {"type": "keyword"},
            "repository": {"type": "keyword"},
            "last_run": {"type": "date"},
            "last_run_ok": {"type": "boolean"},
            "last_success": {"type": "date"},
            "data_as_of": {"type": "date"},
            "lag_hours": {"type": "float"},
            "stale": {"type": "boolean"},
            "snapshots": {
                "properties": {
                    "series": {"type": "keyword"},
                    "snapshot": {"type": "keyword"},
                    "start_time": {"type": "date"},
                }
            },
            "restored": {"type": "integer"},
            "failed": {"type": "integer"},
            "pruned": {"type": "integer"},
            "unchanged": {"type": "integer"},
            "errors": {"type": "text"},
            "mirror_active_shards": {"type": "integer"},
            "mirror_shard_limit": {"type": "integer"},
        }
    },
}


def log(msg):
    print("[%s] %s" % (utcnow().strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------------------
# Time helpers (naive UTC throughout)
# ---------------------------------------------------------------------------

EPOCH = datetime(1970, 1, 1)


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ") if moment else None


def parse_iso(value):
    if not value:
        return None
    try:
        return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def from_millis(millis):
    return EPOCH + timedelta(milliseconds=millis)


def daily_date(name):
    """Date encoded in a daily index name (``...-YYYY.MM.DD``), else None."""
    match = DAILY_SUFFIX.search(name)
    if not match:
        return None
    try:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def expired(name, cutoff):
    day = daily_date(name)
    return day is not None and day < cutoff


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def read_netrc_auth(path, host):
    """Return (login, password) for host from a netrc file.

    Hand-parsed rather than using the stdlib ``netrc`` module, which raises on
    the ``macdef`` blocks present in this file. A macdef body runs until the
    next blank line and must be skipped wholesale.
    """
    if not os.path.exists(path):
        return None

    login = password = None
    current = None
    in_macdef = False

    with open(path) as handle:
        for raw in handle:
            if in_macdef:
                if not raw.strip():
                    in_macdef = False
                continue

            tokens = raw.split()
            index = 0
            while index < len(tokens):
                token = tokens[index]
                if token == "macdef":
                    in_macdef = True
                    break
                if token == "machine" and index + 1 < len(tokens):
                    current = tokens[index + 1]
                    index += 2
                    continue
                if current == host and token in ("login", "password") and index + 1 < len(tokens):
                    if token == "login":
                        login = tokens[index + 1]
                    else:
                        password = tokens[index + 1]
                    index += 2
                    continue
                index += 1

    if login and password:
        return login, password
    return None


class Client(object):
    """Minimal OpenSearch client over urllib (TLS verification disabled)."""

    def __init__(self, es_url, auth=None):
        self.es_url = es_url.rstrip("/")
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.auth_header = None
        if auth:
            raw = ("%s:%s" % auth).encode("utf-8")
            self.auth_header = "Basic " + b64encode(raw).decode("ascii")

    def request(self, path, method="GET", body=None, timeout=120):
        """Return (parsed_json, error_string). Exactly one is non-None."""
        url = self.es_url + path
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.auth_header:
            req.add_header("Authorization", self.auth_header)
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self.ctx) as resp:
                payload = resp.read().decode("utf-8")
            return (json.loads(payload) if payload else {}), None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:400]
            return None, "HTTP %s: %s" % (exc.code, detail)
        except Exception as exc:  # noqa: BLE001 - surface transport errors verbatim
            return None, str(exc)


# ---------------------------------------------------------------------------
# Snapshot inspection
# ---------------------------------------------------------------------------

def list_snapshots(client, repository):
    """Return (successful snapshots, error).

    ``ignore_unavailable`` plus a short retry covers a listing that races the
    source's own retention deleting a snapshot mid-read, which otherwise fails
    the whole listing with ``snapshot_missing_exception``.
    """
    path = "/_snapshot/%s/_all?ignore_unavailable=true" % repository
    attempts = 3
    for attempt in range(1, attempts + 1):
        result, error = client.request(path, timeout=300)
        if not error:
            snapshots = [s for s in result.get("snapshots", []) if s.get("state") == "SUCCESS"]
            return snapshots, None
        if attempt < attempts and "snapshot_missing" in error:
            time.sleep(10)
            continue
        return None, error


def series_of(snapshot):
    """The SM policy that wrote a snapshot, or None for a manual snapshot."""
    return (snapshot.get("metadata") or {}).get("sm_policy")


def select_snapshots(snapshots, max_lag_hours):
    """Pick the newest snapshot of every active series.

    Returns (selected, ignored): selected is a newest-first list of
    ``{"series", "snapshot", "start_ms"}``; ignored is a list of notes about
    what was passed over and why.

    When the repository holds no SM-policy snapshots at all, the single newest
    snapshot is used, which is the behaviour a one-policy cluster always had.
    """
    groups = {}
    for snap in snapshots:
        groups.setdefault(series_of(snap), []).append(snap)

    ignored = []
    if any(key is not None for key in groups):
        manual = groups.pop(None, [])
        if manual:
            ignored.append("%d snapshot(s) with no SM policy (manual or teardown)"
                           % len(manual))

    newest = {}
    for key, members in groups.items():
        newest[key] = max(members, key=lambda s: s.get("start_time_in_millis", 0))
    if not newest:
        return [], ignored

    latest_ms = max(s.get("start_time_in_millis", 0) for s in newest.values())
    horizon = latest_ms - int(max_lag_hours * 3600 * 1000)

    selected = []
    for key, snap in newest.items():
        start = snap.get("start_time_in_millis", 0)
        series = key or "latest"
        if start < horizon:
            ignored.append("series %s: newest snapshot %s is from %s"
                           % (series, snap["snapshot"], iso(from_millis(start))))
            continue
        selected.append({"series": series, "snapshot": snap["snapshot"], "start_ms": start})
    selected.sort(key=lambda s: s["start_ms"], reverse=True)
    return selected, ignored


def pin_snapshots(snapshots, names):
    """Resolve explicitly named snapshots. Returns (selected, error)."""
    by_name = dict((s["snapshot"], s) for s in snapshots)
    selected = []
    for name in names:
        snap = by_name.get(name)
        if snap is None:
            return None, "pinned snapshot %s not found (or not SUCCESS)" % name
        selected.append({
            "series": series_of(snap) or "pinned",
            "snapshot": name,
            "start_ms": snap.get("start_time_in_millis", 0),
        })
    selected.sort(key=lambda s: s["start_ms"], reverse=True)
    return selected, None


def snapshot_fingerprints(client, repository, snapshot):
    """Return ({source index -> "<file_count>:<size_in_bytes>"}, error).

    Uses the snapshot's total (not incremental) stats so the value is a property
    of the index content itself and can be compared across arbitrary snapshots.
    """
    result, error = client.request(
        "/_snapshot/%s/%s/_status" % (repository, snapshot), timeout=900
    )
    if error:
        return None, "status of %s: %s" % (snapshot, error)

    snapshots = result.get("snapshots") or []
    if not snapshots:
        return None, "status of %s returned no snapshots" % snapshot

    fingerprints = {}
    for name, info in snapshots[0].get("indices", {}).items():
        total = info.get("stats", {}).get("total", {})
        fingerprints[name] = "%s:%s" % (
            total.get("file_count", 0), total.get("size_in_bytes", 0)
        )
    return fingerprints, None


def source_indices(client, repository, selected):
    """Return ({index -> {"fp", "snapshot"}}, error) across the selected snapshots.

    ``selected`` is newest first, so an index present in more than one of them
    is taken from the newest.
    """
    current = {}
    for entry in selected:
        fingerprints, error = snapshot_fingerprints(client, repository, entry["snapshot"])
        if error:
            return None, error
        for name, fingerprint in fingerprints.items():
            if name not in current:
                current[name] = {"fp": fingerprint, "snapshot": entry["snapshot"]}
    return current, None


def local_indices(client, venue):
    """Return (source names of the venue's indices on the mirror, error)."""
    prefix = venue + "_"
    result, error = client.request(
        "/_cat/indices/%s*?h=index&format=json&expand_wildcards=all" % prefix,
        timeout=120,
    )
    if error:
        return None, error
    names = set()
    for row in result or []:
        index = row.get("index", "")
        if index.startswith(prefix):
            names.add(index[len(prefix):])
    return names, None


def excluded(name, patterns):
    return any(fnmatch(name, pattern) for pattern in patterns)


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

def restore_one(client, repository, snapshot, source, venue, replicas, timeout):
    """Delete the local copy of one index and restore it fresh from the snapshot.

    Returns None on success, else an error string. The delete is intentional:
    OpenSearch refuses to restore over an existing open index, and a direct
    restore reproduces the snapshot's mappings without a reindex.
    """
    target = "%s_%s" % (venue, source)

    _, error = client.request("/%s" % target, method="DELETE", timeout=300)
    if error and "index_not_found" not in error:
        return "delete failed: %s" % error

    body = {
        "indices": source,
        "ignore_unavailable": False,
        "include_global_state": False,
        "include_aliases": False,
        "rename_pattern": "(.+)",
        "rename_replacement": "%s_$1" % venue,
        "index_settings": {"index.number_of_replicas": replicas},
    }
    _, error = client.request(
        "/_snapshot/%s/%s/_restore?wait_for_completion=false" % (repository, snapshot),
        method="POST", body=body, timeout=300,
    )
    if error:
        return error

    # Replicas cannot be assigned on a single-node mirror, so green is only
    # reachable at replicas=0. Accept yellow otherwise rather than burning the
    # whole timeout waiting for a state that can never arrive.
    return wait_for_index(client, target, timeout, require_green=(replicas == 0))


def wait_for_index(client, index, timeout, require_green=True):
    """Poll until the index reaches an acceptable health color.

    Returns None once it does, else an error string.

    Uses *plain* cluster health (no ``wait_for_status``). ``wait_for_status``
    makes OpenSearch return HTTP 408 whenever the target color is not reached
    within its own timeout -- which, for a restore whose shards are still
    recovering behind the ``node_initial_primaries_recoveries`` throttle, is the
    normal case for the first minutes. An earlier version treated that 408 as
    the index being absent and declared the restore failed while it was in fact
    recovering; the fingerprint was then never recorded and the next run deleted
    and re-restored the index in a perpetual churn.

    Plain health instead returns 200 with the current (possibly yellow/red)
    color for an index that exists, and a genuine 404 only when it truly does
    not. So existence and color are read separately, and slow recovery is
    waited out rather than misread.
    """
    acceptable = ("green",) if require_green else ("green", "yellow")
    deadline = time.time() + timeout
    # Only a persistent 404 means the restore never produced the index. Allow a
    # grace period first, since the index appears a moment after the async
    # restore is accepted.
    absent_deadline = time.time() + 180
    last_status = "unknown"

    while time.time() < deadline:
        result, error = client.request("/_cluster/health/%s" % index, timeout=60)
        if error:
            missing = "index_not_found" in error or "HTTP 404" in error
            if missing and time.time() > absent_deadline:
                return "index never appeared after the restore was accepted"
            # Not-yet-created, or a transient transport error: keep polling.
            time.sleep(5)
            continue
        last_status = result.get("status", "unknown")
        if last_status in acceptable:
            return None
        time.sleep(5)

    return "timed out waiting for it to become healthy (last status: %s)" % last_status


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as handle:
                return json.load(handle)
        except ValueError as exc:
            log("WARNING: state file unreadable (%s); treating as empty" % exc)
    return {"venues": {}}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.rename(tmp, STATE_FILE)


# ---------------------------------------------------------------------------
# Per-venue refresh
# ---------------------------------------------------------------------------

class VenueRun(object):
    """Counters and errors from refreshing one venue."""

    def __init__(self, venue, repository):
        self.venue = venue
        self.repository = repository
        self.restored = 0
        self.failed = 0
        self.unchanged = 0
        self.pruned = 0
        self.errors = []

    def fail(self, message):
        log("  ERROR %s" % message)
        self.errors.append(message[:300])
        self.failed += 1


def prune_expired(client, venue, local, known, cutoff, run, dry_run):
    """Delete the venue's daily indices older than cutoff from the mirror."""
    doomed = sorted(name for name in local if expired(name, cutoff))
    for name in list(known):
        if expired(name, cutoff) and name not in local:
            known.pop(name)
    if not doomed:
        return

    verb = "would delete" if dry_run else "deleting"
    log("  %s %d daily indices dated before %s: %s%s"
        % (verb, len(doomed), cutoff.strftime("%Y.%m.%d"),
           ", ".join(doomed[:3]), " ..." if len(doomed) > 3 else ""))
    for name in doomed:
        target = "%s_%s" % (venue, name)
        if not dry_run:
            _, error = client.request("/%s" % target, method="DELETE", timeout=300)
            if error and "index_not_found" not in error:
                run.fail("delete %s: %s" % (target, error))
                continue
        known.pop(name, None)
        local.discard(name)
        run.pruned += 1


def refresh_venue(client, venue, repository, state, args, now):
    """Refresh one venue. Returns a VenueRun."""
    run = VenueRun(venue, repository)
    log("=" * 62)
    log("venue %s (repository %s)" % (venue, repository))

    snapshots, error = list_snapshots(client, repository)
    if error:
        run.fail("listing snapshots in %s: %s" % (repository, error))
        return run

    # When adopting state after a manual restore, fingerprint the snapshots the
    # data actually came from -- not the newest ones. Fingerprinting newer
    # snapshots would mark indices current that we never restored, and anything
    # that went quiet in between would stay stale indefinitely.
    pinned = args.init_snapshots.get(venue) if args.init_state else None
    if pinned:
        selected, error = pin_snapshots(snapshots, pinned)
        if error:
            run.fail(error)
            return run
    else:
        selected, ignored = select_snapshots(snapshots, args.series_max_lag_hours)
        for note in ignored:
            log("  ignoring %s" % note)
        if not selected:
            run.fail("no successful snapshots in %s" % repository)
            return run
    for entry in selected:
        log("  %s snapshot: %s" % (entry["series"], entry["snapshot"]))

    current, error = source_indices(client, repository, selected)
    if error:
        run.fail("reading snapshot %s" % error)
        return run

    local, error = local_indices(client, venue)
    if error:
        run.fail("listing %s_* indices on the mirror: %s" % (venue, error))
        return run

    venue_state = state.setdefault("venues", {}).setdefault(venue, {})
    venue_state.pop("last_snapshot", None)  # single-snapshot state from older versions
    known = venue_state.get("fingerprints", {})

    skipped = [n for n in current if excluded(n, args.exclude)]
    for name in skipped:
        current.pop(name)

    aged = []
    if args.daily_retention_days > 0:
        today = datetime(now.year, now.month, now.day)
        cutoff = today - timedelta(days=args.daily_retention_days)
        aged = [n for n in current if expired(n, cutoff)]
        for name in aged:
            current.pop(name)
        # --init-state promises to change nothing on the mirror; the next normal
        # run prunes.
        if not args.init_state:
            prune_expired(client, venue, local, known, cutoff, run, args.dry_run)

    changed = sorted(n for n, info in current.items()
                     if known.get(n) != info["fp"] or n not in local)
    missing = sum(1 for n in changed if n not in local)
    run.unchanged = len(current) - len(changed)
    gone = sorted(set(known) - set(current))

    log("  %d indices in source: %d changed (%d missing here), %d unchanged, "
        "%d excluded, %d past retention"
        % (len(current), len(changed), missing, run.unchanged, len(skipped), len(aged)))
    if gone:
        # The source dropped these. Keep our copies -- retaining history the
        # source no longer has is part of the mirror's value, and the daily
        # retention above bounds the part of it that grows without limit -- but
        # stop tracking them so they are not re-reported forever.
        log("  %d indices no longer in source (kept here): %s%s"
            % (len(gone), ", ".join(gone[:5]), " ..." if len(gone) > 5 else ""))
        for name in gone:
            known.pop(name, None)

    if changed and args.init_state:
        # Checked before the --max-changed guard: adopting state is a remedy
        # for "everything looks changed", so the guard must not block it.
        log("  --init-state: adopting %d fingerprints without restoring" % len(changed))
        known.update(dict((n, current[n]["fp"]) for n in changed))
        changed = []

    if len(changed) > args.max_changed and not args.force:
        run.fail("%d indices changed, over the --max-changed limit of %d. This "
                 "usually means the state file was lost, the mirror was wiped, or "
                 "the venue was redeployed. Re-run with --force to restore them, or "
                 "--init-state to adopt the current state without restoring."
                 % (len(changed), args.max_changed))
        changed = []

    for position, name in enumerate(changed, 1):
        info = current[name]
        target = "%s_%s" % (venue, name)
        if args.dry_run:
            log("  [%d/%d] DRY-RUN would restore %s from %s"
                % (position, len(changed), target, info["snapshot"]))
            run.restored += 1
            continue

        log("  [%d/%d] restoring %s" % (position, len(changed), target))
        error = restore_one(client, repository, info["snapshot"], name, venue,
                            args.replicas, args.index_timeout)
        if error:
            run.fail("restore %s: %s" % (target, error))
        else:
            # Only record the fingerprint on success, so a failure is retried on
            # the next run rather than being silently adopted as current.
            known[name] = info["fp"]
            run.restored += 1

    venue_state["fingerprints"] = known
    venue_state["snapshots"] = [
        {"series": e["series"], "snapshot": e["snapshot"],
         "start_time": iso(from_millis(e["start_ms"]))}
        for e in selected
    ]
    venue_state["last_refresh"] = iso(now)
    if run.failed == 0:
        venue_state["last_success"] = iso(utcnow())
        venue_state["data_as_of"] = iso(from_millis(min(e["start_ms"] for e in selected)))
    return run


# ---------------------------------------------------------------------------
# Freshness, status and alerts
# ---------------------------------------------------------------------------

def freshness(venue_state, now, stale_hours):
    """Return (lag in hours or None, stale). A venue never caught up is stale."""
    as_of = parse_iso(venue_state.get("data_as_of"))
    if as_of is None:
        return None, True
    lag = (now - as_of).total_seconds() / 3600.0
    return lag, lag > stale_hours


def shard_usage(client, health):
    """Return (active shards, cluster shard limit or None)."""
    active = health.get("active_shards")
    result, error = client.request(
        "/_cluster/settings?include_defaults=true&flat_settings=true", timeout=60
    )
    if error:
        return active, None
    per_node = None
    for section in ("transient", "persistent", "defaults"):
        value = (result.get(section) or {}).get("cluster.max_shards_per_node")
        if value is not None:
            per_node = int(value)
            break
    if per_node is None:
        return active, None
    return active, per_node * max(1, health.get("number_of_data_nodes", 1))


def shards_near_limit(shards):
    active, limit = shards
    return bool(active is not None and limit and active >= SHARD_WARN_FRACTION * limit)


def write_status(client, index, run, venue_state, lag, stale, shards, now, created):
    """Upsert this venue's freshness document into the mirror's status index."""
    if not created:
        _, error = client.request("/%s" % index, timeout=60)
        if error and ("index_not_found" in error or "HTTP 404" in error):
            _, error = client.request("/%s" % index, method="PUT",
                                      body=STATUS_INDEX_BODY, timeout=60)
            if error and "resource_already_exists" not in error:
                log("WARNING: could not create %s: %s" % (index, error))
                return False
        elif error:
            log("WARNING: could not check %s: %s" % (index, error))
            return False

    doc = {
        "venue": run.venue,
        "repository": run.repository,
        "last_run": iso(now),
        "last_run_ok": run.failed == 0,
        "last_success": venue_state.get("last_success"),
        "data_as_of": venue_state.get("data_as_of"),
        "lag_hours": round(lag, 1) if lag is not None else None,
        "stale": stale,
        "snapshots": venue_state.get("snapshots", []),
        "restored": run.restored,
        "failed": run.failed,
        "pruned": run.pruned,
        "unchanged": run.unchanged,
        "errors": run.errors[:10],
        "mirror_active_shards": shards[0],
        "mirror_shard_limit": shards[1],
    }
    _, error = client.request("/%s/_doc/%s" % (index, run.venue), method="PUT",
                              body=doc, timeout=60)
    if error:
        log("WARNING: could not write %s/%s: %s" % (index, run.venue, error))
    return True


def alert_decision(entry, stale, now, repeat_hours):
    """Return "raise", "repeat", "clear" or None for a venue's alert state."""
    alerting = entry.get("alerting", False)
    if stale:
        if not alerting:
            return "raise"
        last = parse_iso(entry.get("alerted_at"))
        if last is None or now - last >= timedelta(hours=repeat_hours):
            return "repeat"
        return None
    return "clear" if alerting else None


def record_alert(entry, action, now):
    if action in ("raise", "repeat"):
        entry["alerting"] = True
        entry["alerted_at"] = iso(now)
    elif action == "clear":
        entry["alerting"] = False
        entry.pop("alerted_at", None)


def alert_message(action, run, venue_state, lag, shards, stale_hours):
    """Return (subject, body) for an alert about one venue."""
    if action == "clear":
        subject = "OPERA analysis mirror: %s is current again" % run.venue
    elif action == "repeat":
        subject = "OPERA analysis mirror: %s is still stale" % run.venue
    else:
        subject = "OPERA analysis mirror: %s is stale" % run.venue

    lines = [
        subject,
        "",
        "venue:            %s (repository %s)" % (run.venue, run.repository),
        "data as of:       %s" % (venue_state.get("data_as_of") or "never"),
        "lag:              %s (alert threshold %sh)"
        % ("%.1fh" % lag if lag is not None else "unknown", stale_hours),
        "last good run:    %s" % (venue_state.get("last_success") or "never"),
        "this run:         restored=%d failed=%d pruned=%d unchanged=%d"
        % (run.restored, run.failed, run.pruned, run.unchanged),
    ]
    if shards[1]:
        lines.append("mirror shards:    %s/%s%s"
                     % (shards[0], shards[1],
                        "  <-- near the limit" if shards_near_limit(shards) else ""))
    if run.errors:
        lines.append("")
        lines.append("errors (first %d of %d):" % (min(5, len(run.errors)), len(run.errors)))
        lines.extend("  " + e for e in run.errors[:5])
    return subject, "\n".join(lines) + "\n"


def send_alert(command, subject, body):
    """Run the operator's alert command. Returns True if it ran cleanly.

    The message goes to the command's stdin and the subject to
    ``MIRROR_ALERT_SUBJECT``. With no command configured the alert is only
    logged, and counts as sent.
    """
    log("ALERT: %s" % subject)
    if not command:
        return True
    env = dict(os.environ)
    env["MIRROR_ALERT_SUBJECT"] = subject
    try:
        proc = subprocess.run(
            command, shell=True, input=body, env=env, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log("WARNING: alert command failed: %s" % exc)
        return False
    if proc.returncode != 0:
        log("WARNING: alert command exited %d: %s"
            % (proc.returncode, (proc.stdout or "").strip()[:300]))
        return False
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_pairs(parser, items, flag):
    pairs = []
    for item in items or []:
        if "=" not in item:
            parser.error("%s expects NAME=VALUE, got %r" % (flag, item))
        pairs.append(tuple(item.split("=", 1)))
    return pairs


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally refresh the OPERA analysis mirror from cluster snapshots."
    )
    parser.add_argument("--es-url", default=DEFAULT_ES_URL,
                        help="mirror OpenSearch URL (default: %(default)s)")
    parser.add_argument("--netrc", default=os.path.expanduser("~/.netrc"),
                        help="netrc file supplying mirror credentials")
    parser.add_argument("--venue", action="append", metavar="NAME=REPO",
                        help="venue to refresh, repeatable; replaces the defaults "
                             "(fwd=snapshot-repo-fwd, pop1=snapshot-repo-pop1)")
    parser.add_argument("--replicas", type=int, default=0,
                        help="replica count for restored indices (default: 0, "
                             "required for green on a single node)")
    parser.add_argument("--exclude", action="append", default=None,
                        metavar="GLOB", help="index glob to skip, repeatable")
    parser.add_argument("--daily-retention-days", type=int,
                        default=DEFAULT_DAILY_RETENTION_DAYS,
                        help="keep daily (YYYY.MM.DD) indices this many days, then "
                             "delete them from the mirror; 0 keeps them forever "
                             "(default: %(default)s)")
    parser.add_argument("--series-max-lag-hours", type=float,
                        default=DEFAULT_SERIES_MAX_LAG_HOURS,
                        help="ignore a snapshot series whose newest snapshot is this "
                             "much older than the venue's newest (default: %(default)s)")
    parser.add_argument("--max-changed", type=int, default=DEFAULT_MAX_CHANGED,
                        help="abort a venue if more than this many indices changed "
                             "(default: %(default)s)")
    parser.add_argument("--index-timeout", type=int, default=3600,
                        help="seconds to wait for one index to go green "
                             "(default: %(default)s). Large current-month grq_ "
                             "product indices (e.g. dswx_hls can exceed 100 GB) "
                             "are re-restored whole each cycle and need headroom.")
    parser.add_argument("--force", action="store_true",
                        help="proceed even if --max-changed is exceeded")
    parser.add_argument("--init-state", action="store_true",
                        help="record current fingerprints without restoring; "
                             "use after restoring indices by hand")
    parser.add_argument("--init-snapshot", action="append", metavar="NAME=SNAPSHOT",
                        help="with --init-state, fingerprint this snapshot for the "
                             "named venue instead of the newest ones -- use the "
                             "snapshots the data was actually restored from; "
                             "repeatable, including for the same venue")
    parser.add_argument("--stale-hours", type=float, default=DEFAULT_STALE_HOURS,
                        help="a venue whose data is older than this is stale "
                             "(default: %(default)s)")
    parser.add_argument("--alert-cmd", default=os.environ.get("MIRROR_ALERT_CMD"),
                        help="shell command run when a venue goes stale, stays "
                             "stale, or recovers; message on stdin, subject in "
                             "$MIRROR_ALERT_SUBJECT (default: $MIRROR_ALERT_CMD)")
    parser.add_argument("--alert-repeat-hours", type=float,
                        default=DEFAULT_ALERT_REPEAT_HOURS,
                        help="repeat the alert this often while a venue stays "
                             "stale (default: %(default)s)")
    parser.add_argument("--status-index", default=DEFAULT_STATUS_INDEX,
                        help="mirror index that records each venue's freshness; "
                             "empty to disable (default: %(default)s)")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="report what would be restored or deleted; change "
                             "nothing and send no alerts")
    args = parser.parse_args()

    if args.exclude is None:
        args.exclude = DEFAULT_EXCLUDE_PATTERNS

    args.init_snapshots = {}
    for name, snap in parse_pairs(parser, args.init_snapshot, "--init-snapshot"):
        args.init_snapshots.setdefault(name, []).append(snap)
    if args.init_snapshots and not args.init_state:
        parser.error("--init-snapshot is only meaningful with --init-state")

    venues = dict(parse_pairs(parser, args.venue, "--venue")) or DEFAULT_VENUES

    host = args.es_url.split("://", 1)[-1].split(":")[0].split("/")[0]
    auth = read_netrc_auth(args.netrc, host)
    if not auth:
        log("ERROR: no credentials for %s in %s" % (host, args.netrc))
        log("       note the mirror netrc is keyed to the IP, not localhost")
        return 2

    state = load_state()
    client = Client(args.es_url, auth)
    shards = (None, None)
    runs = []

    health, error = client.request("/_cluster/health", timeout=60)
    reachable = error is None
    if not reachable:
        # Still evaluate freshness below, so an outage of the mirror itself
        # alerts once the data it would have ingested goes stale.
        log("ERROR: cannot reach mirror at %s: %s" % (args.es_url, error))
        for venue, repository in sorted(venues.items()):
            run = VenueRun(venue, repository)
            run.errors.append("mirror unreachable: %s" % error[:250])
            run.failed = 1
            runs.append(run)
    else:
        log("mirror cluster %r is %s" % (health.get("cluster_name"), health.get("status")))
        shards = shard_usage(client, health)
        if shards[1]:
            log("mirror shards: %s/%s%s" % (shards[0], shards[1],
                " -- WARNING: near the limit; new indices will fail to restore"
                if shards_near_limit(shards) else ""))
        for venue, repository in sorted(venues.items()):
            runs.append(refresh_venue(client, venue, repository, state, args, utcnow()))

    log("=" * 62)
    now = utcnow()
    status_ready = False
    for run in runs:
        venue_state = state.setdefault("venues", {}).setdefault(run.venue, {})
        lag, stale = freshness(venue_state, now, args.stale_hours)
        log("%s: data as of %s (%s)%s"
            % (run.venue, venue_state.get("data_as_of") or "never",
               "%.1fh behind" % lag if lag is not None else "no successful refresh yet",
               " -- STALE" if stale else ""))
        if args.dry_run:
            continue
        if reachable and args.status_index:
            status_ready = write_status(client, args.status_index, run, venue_state,
                                        lag, stale, shards, now, status_ready)
        action = alert_decision(venue_state, stale, now, args.alert_repeat_hours)
        if action:
            subject, body = alert_message(action, run, venue_state, lag, shards,
                                          args.stale_hours)
            if send_alert(args.alert_cmd, subject, body):
                record_alert(venue_state, action, now)

    if not args.dry_run:
        save_state(state)

    totals = {
        "restored": sum(r.restored for r in runs),
        "failed": sum(r.failed for r in runs),
        "unchanged": sum(r.unchanged for r in runs),
        "pruned": sum(r.pruned for r in runs),
    }
    log("summary: restored=%(restored)d failed=%(failed)d unchanged=%(unchanged)d "
        "pruned=%(pruned)d" % totals)
    if not reachable:
        return 2
    return 1 if totals["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
