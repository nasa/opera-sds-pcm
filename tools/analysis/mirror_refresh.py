#!/usr/bin/env python3
"""Incrementally refresh the OPERA analysis mirror from OPS cluster snapshots.

The mirror is a single-node OpenSearch instance that serves read-only copies of
the OPS-FWD and OPS-POP1 clusters so analysts can query operational data without
touching production.

Both venues are restored into one cluster, so every index carries a venue prefix
(``fwd_*`` / ``pop1_*``). 162 index names collide between the two venues -- most
of the operational time-series plus several ``grq_`` product indices that hold
genuinely different documents per venue -- so no index keeps its canonical name.
A query that omits the venue matches nothing rather than silently returning half
the picture.

Change detection compares a per-index fingerprint (file count + byte size taken
from the snapshot ``_status`` API) against the fingerprint recorded on the last
successful restore. Indices whose fingerprint is unchanged are skipped. Because
the comparison is against our own last-restored state rather than against the
previous snapshot, skipping refresh cycles is safe: anything that changed while
we were not looking is still detected on the next run.

Each changed index is deleted locally and restored directly from the snapshot,
which preserves the snapshot's own mappings exactly. There is no reindex step
and no dependency on registered index templates.

Requires only the Python standard library.

Usage:
    # Record current state without restoring (run once after a bulk seed)
    mirror_refresh.py --init-state

    # Normal incremental refresh of both venues
    mirror_refresh.py

    # Preview
    mirror_refresh.py --dry-run
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from datetime import datetime
from fnmatch import fnmatch

DEFAULT_ES_URL = "https://100.104.82.14:9200"
STATE_FILE = os.path.expanduser("~/.opera_mirror_refresh_state.json")

# venue prefix -> snapshot repository holding that venue's hourly snapshots
DEFAULT_VENUES = {
    "fwd": "snapshot-repo-fwd",
    "pop1": "snapshot-repo-pop1",
}

# Indices never mirrored. The OPS snapshot policy already omits system indices,
# so this is a backstop. Note we deliberately do NOT exclude user_rules-* the way
# a same-name mirror must: the venue prefix means fwd_user_rules-mozart can never
# be mistaken for a live rule index.
DEFAULT_EXCLUDE_PATTERNS = [
    ".*",
    "restore_temp_*",
]

# Refuse to restore more than this many indices in one run unless --force is
# given. Normal hourly churn is a few dozen; a sudden jump to "everything
# changed" usually means the state file was lost or reset, and blindly acting on
# it would re-seed the whole mirror and hammer the box for hours.
DEFAULT_MAX_CHANGED = 250


def log(msg):
    print("[%s] %s" % (datetime.utcnow().strftime("%H:%M:%S"), msg), flush=True)


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

def latest_snapshot(client, repository):
    result, error = client.request(
        "/_snapshot/%s/_all" % repository, timeout=300
    )
    if error:
        log("  ERROR listing snapshots in %s: %s" % (repository, error))
        return None
    done = [s for s in result.get("snapshots", []) if s.get("state") == "SUCCESS"]
    if not done:
        log("  ERROR no successful snapshots in %s" % repository)
        return None
    done.sort(key=lambda s: s.get("start_time_in_millis", 0))
    return done[-1]["snapshot"]


def snapshot_fingerprints(client, repository, snapshot):
    """Map source index name -> "<file_count>:<size_in_bytes>" for a snapshot.

    Uses the snapshot's total (not incremental) stats so the value is a property
    of the index content itself and can be compared across arbitrary snapshots.
    """
    result, error = client.request(
        "/_snapshot/%s/%s/_status" % (repository, snapshot), timeout=900
    )
    if error:
        log("  ERROR reading snapshot status: %s" % error)
        return None

    snapshots = result.get("snapshots") or []
    if not snapshots:
        log("  ERROR snapshot status returned no snapshots")
        return None

    fingerprints = {}
    for name, info in snapshots[0].get("indices", {}).items():
        total = info.get("stats", {}).get("total", {})
        fingerprints[name] = "%s:%s" % (
            total.get("file_count", 0), total.get("size_in_bytes", 0)
        )
    return fingerprints


def excluded(name, patterns):
    return any(fnmatch(name, pattern) for pattern in patterns)


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

def restore_one(client, repository, snapshot, source, venue, replicas, timeout):
    """Delete the local copy of one index and restore it fresh from the snapshot.

    Returns True on success. The delete is intentional: OpenSearch refuses to
    restore over an existing open index, and a direct restore reproduces the
    snapshot's mappings without a reindex.
    """
    target = "%s_%s" % (venue, source)

    _, error = client.request("/%s" % target, method="DELETE", timeout=300)
    if error and "index_not_found" not in error:
        log("    delete %s failed: %s" % (target, error))
        return False

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
        log("    restore %s failed: %s" % (target, error))
        return False

    # Replicas cannot be assigned on a single-node mirror, so green is only
    # reachable at replicas=0. Accept yellow otherwise rather than burning the
    # whole timeout waiting for a state that can never arrive.
    return wait_for_index(client, target, timeout, require_green=(replicas == 0))


def wait_for_index(client, index, timeout, require_green=True):
    """Poll until the index reaches an acceptable health color, or timeout.

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
                log("    %s never appeared after restore was accepted" % index)
                return False
            # Not-yet-created, or a transient transport error: keep polling.
            time.sleep(5)
            continue
        last_status = result.get("status", "unknown")
        if last_status in acceptable:
            return True
        time.sleep(5)

    log("    timed out waiting for %s to become healthy (last status: %s)"
        % (index, last_status))
    return False


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

def refresh_venue(client, venue, repository, state, args):
    """Refresh one venue. Returns (restored, failed, unchanged)."""
    log("=" * 62)
    log("venue %s (repository %s)" % (venue, repository))

    # When adopting state after a bulk seed, fingerprint the snapshot the data
    # actually came from -- not the newest one. Fingerprinting a newer snapshot
    # would mark indices current that we never restored, and anything that went
    # quiet in between would stay stale indefinitely.
    snapshot = args.init_snapshots.get(venue) if args.init_state else None
    if snapshot:
        log("  init-state pinned to snapshot: %s" % snapshot)
    else:
        snapshot = latest_snapshot(client, repository)
        if not snapshot:
            return 0, 1, 0
        log("  latest snapshot: %s" % snapshot)

    current = snapshot_fingerprints(client, repository, snapshot)
    if current is None:
        return 0, 1, 0

    venue_state = state.setdefault("venues", {}).setdefault(venue, {})
    known = venue_state.get("fingerprints", {})

    skipped = [n for n in current if excluded(n, args.exclude)]
    for name in skipped:
        current.pop(name)

    changed = sorted(n for n, fp in current.items() if known.get(n) != fp)
    unchanged = len(current) - len(changed)
    gone = sorted(set(known) - set(current))

    log("  %d indices in snapshot: %d changed, %d unchanged, %d excluded"
        % (len(current), len(changed), unchanged, len(skipped)))
    if gone:
        # Source rolled these off (ISM retention). Keep our copies -- retaining
        # history the OPS cluster has already dropped is part of the mirror's
        # value -- but stop tracking them so they are not re-reported forever.
        log("  %d indices no longer in source (kept locally): %s%s"
            % (len(gone), ", ".join(gone[:5]), " ..." if len(gone) > 5 else ""))
        for name in gone:
            known.pop(name, None)

    if not changed:
        venue_state["last_snapshot"] = snapshot
        venue_state["last_refresh"] = datetime.utcnow().isoformat() + "Z"
        venue_state["fingerprints"] = known
        return 0, 0, unchanged

    # Checked before the --max-changed guard: adopting state is the documented
    # remedy for "everything looks changed", so the guard must not block it.
    if args.init_state:
        log("  --init-state: adopting %d fingerprints without restoring" % len(changed))
        known.update({n: current[n] for n in changed})
        venue_state["last_snapshot"] = snapshot
        venue_state["last_refresh"] = datetime.utcnow().isoformat() + "Z"
        venue_state["fingerprints"] = known
        return 0, 0, unchanged

    if len(changed) > args.max_changed and not args.force:
        log("  ABORT: %d indices changed, over the --max-changed limit of %d."
            % (len(changed), args.max_changed))
        log("         This usually means the state file was lost or reset.")
        log("         Re-run with --init-state to adopt current state without")
        log("         restoring, or with --force to restore anyway.")
        return 0, 1, unchanged

    restored = failed = 0
    for position, name in enumerate(changed, 1):
        target = "%s_%s" % (venue, name)
        if args.dry_run:
            log("  [%d/%d] DRY-RUN would restore %s" % (position, len(changed), target))
            restored += 1
            continue

        log("  [%d/%d] restoring %s" % (position, len(changed), target))
        if restore_one(client, repository, snapshot, name, venue,
                       args.replicas, args.index_timeout):
            # Only record the fingerprint on success, so a failure is retried on
            # the next run rather than being silently adopted as current.
            known[name] = current[name]
            restored += 1
        else:
            failed += 1

    venue_state["last_snapshot"] = snapshot
    venue_state["last_refresh"] = datetime.utcnow().isoformat() + "Z"
    venue_state["fingerprints"] = known
    return restored, failed, unchanged


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Incrementally refresh the OPERA analysis mirror from OPS snapshots."
    )
    parser.add_argument("--es-url", default=DEFAULT_ES_URL,
                        help="mirror OpenSearch URL (default: %(default)s)")
    parser.add_argument("--netrc", default=os.path.expanduser("~/.netrc"),
                        help="netrc file supplying mirror credentials")
    parser.add_argument("--venue", action="append", metavar="NAME=REPO",
                        help="venue to refresh, repeatable "
                             "(default: fwd=snapshot-repo-fwd, pop1=snapshot-repo-pop1)")
    parser.add_argument("--replicas", type=int, default=0,
                        help="replica count for restored indices (default: 0, "
                             "required for green on a single node)")
    parser.add_argument("--exclude", action="append", default=None,
                        metavar="GLOB", help="index glob to skip, repeatable")
    parser.add_argument("--max-changed", type=int, default=DEFAULT_MAX_CHANGED,
                        help="abort if more than this many indices changed "
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
                             "run once after a bulk seed to avoid a full re-restore")
    parser.add_argument("--init-snapshot", action="append", metavar="NAME=SNAPSHOT",
                        help="with --init-state, fingerprint this specific snapshot "
                             "for the named venue instead of the latest -- use the "
                             "snapshot the bulk seed actually restored from, "
                             "repeatable")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="report what would be restored, change nothing")
    args = parser.parse_args()

    if args.exclude is None:
        args.exclude = DEFAULT_EXCLUDE_PATTERNS

    args.init_snapshots = {}
    for item in args.init_snapshot or []:
        if "=" not in item:
            parser.error("--init-snapshot expects NAME=SNAPSHOT, got %r" % item)
        name, snap = item.split("=", 1)
        args.init_snapshots[name] = snap
    if args.init_snapshots and not args.init_state:
        parser.error("--init-snapshot is only meaningful with --init-state")

    if args.venue:
        venues = {}
        for item in args.venue:
            if "=" not in item:
                parser.error("--venue expects NAME=REPO, got %r" % item)
            name, repo = item.split("=", 1)
            venues[name] = repo
    else:
        venues = DEFAULT_VENUES

    host = args.es_url.split("://", 1)[-1].split(":")[0].split("/")[0]
    auth = read_netrc_auth(args.netrc, host)
    if not auth:
        log("ERROR: no credentials for %s in %s" % (host, args.netrc))
        log("       note the mirror netrc is keyed to the IP, not localhost")
        return 2

    client = Client(args.es_url, auth)
    result, error = client.request("/_cluster/health", timeout=60)
    if error:
        log("ERROR: cannot reach mirror at %s: %s" % (args.es_url, error))
        return 2
    log("mirror cluster %r is %s" % (result.get("cluster_name"), result.get("status")))

    state = load_state()
    totals = {"restored": 0, "failed": 0, "unchanged": 0}
    for venue, repository in sorted(venues.items()):
        restored, failed, unchanged = refresh_venue(
            client, venue, repository, state, args
        )
        totals["restored"] += restored
        totals["failed"] += failed
        totals["unchanged"] += unchanged

    if not args.dry_run:
        save_state(state)

    log("=" * 62)
    log("summary: restored=%(restored)d failed=%(failed)d unchanged=%(unchanged)d"
        % totals)
    return 1 if totals["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
