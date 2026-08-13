#!/bin/bash
# Cron wrapper for the OPERA analysis-mirror incremental refresh.
#
# flock guards against overlapping runs: a refresh that restores several large
# indices can outlast the cron interval, and two concurrent runs would fight
# over the same delete-then-restore sequence.
#
# Crontab (every 2 hours):
#   0 */2 * * * /path/to/run_mirror_refresh.sh >> $HOME/mirror_refresh.log 2>&1
#
# mirror_refresh.py needs only the standard library, so no venv is activated
# here -- the mirror host has no mozart/conda environment for analyst accounts.

set -euo pipefail

LOCK_FILE="${LOCK_FILE:-$HOME/.opera_mirror_refresh.lock}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec 200>"${LOCK_FILE}"
if ! flock -n 200; then
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] refresh already running, skipping this tick"
    exit 0
fi

echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] starting mirror refresh"
python3 "${SCRIPT_DIR}/mirror_refresh.py" "$@"
rc=$?
echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] mirror refresh finished rc=${rc}"
exit ${rc}
