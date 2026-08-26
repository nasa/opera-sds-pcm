#!/bin/bash

# source functions
source monitor_cslc_job.sh
source monitor_l3_disp_s1_job.sh

DEFAULT_START_DATE="2024-05-01T00:00:00Z"
DEFAULT_END_DATE="2025-01-05T00:00:00Z"
DEFAULT_FRAME="8882"

# repo root of the deployed PCM, used to import cslc_utils for the phase check
PCM_HOME=${PCM_HOME:-${HOME}/mozart/ops/opera-pcm}

usage() {
  echo "Usage: $(basename $0) [--force] [start_date] [end_date] [frame ...]"
  echo ""
  echo "  start_date, end_date  revision-time window (default ${DEFAULT_START_DATE} .. ${DEFAULT_END_DATE})"
  echo "  frame ...             frame ids (default ${DEFAULT_FRAME})"
  echo "  --force               submit even for frames whose dates a historical or no_run phase owns"
  echo ""
  echo "When the burst database carries processing-mode annotations and"
  echo "DISP_S1_PROCESSING_MODE_ENABLED is on, each frame is checked before submission and"
  echo "frames whose dates belong to a historical_NN or no_run phase are skipped."
}

# Preflight one frame against the processing-mode phases in the deployed burst database.
#
# Prints a human readable explanation and exits with:
#   0  no conflict, submit this frame
#   2  nothing to check (un-annotated database, master switch off, quarantined frame, frame not
#      in the database, or the check could not run) -- submit exactly as before
#   3  the requested range overlaps a historical_NN or no_run block -- skip this frame
check_frame_phases() {
  local frame="$1"
  local start_date="$2"
  local end_date="$3"

  PYTHONPATH="${PCM_HOME}:${PYTHONPATH}" python - "${frame}" "${start_date}" "${end_date}" <<'PYCHECK'
import bisect
import sys
from datetime import timezone

import dateutil.parser

from data_subscriber import cslc_utils
from data_subscriber.cslc.disp_s1_phases import PhaseKind, PhaseValidationError, phase_for_position

OK, NO_GUARD, BLOCKED = 0, 2, 3


def naive_utc(dt):
    '''The burst database stores naive sensing times; make the CLI dates comparable to them'''
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


frame_id = int(sys.argv[1])
start = naive_utc(dateutil.parser.isoparse(sys.argv[2]))
end = naive_utc(dateutil.parser.isoparse(sys.argv[3]))

try:
    # honours DISP_S1_PROCESSING_MODE_ENABLED: with the switch off phases come back None
    frame_to_bursts, _, _ = cslc_utils.localize_disp_frame_burst_hist()
except Exception as e:
    print("could not load the burst database (%s)" % e)
    sys.exit(NO_GUARD)

if frame_id not in frame_to_bursts:
    print("frame is not in the burst database")
    sys.exit(NO_GUARD)

frame = frame_to_bursts[frame_id]
phases = frame.phases
if phases is None:
    print("no processing-mode phases (%s)"
          % (frame.phase_error
             or "un-annotated burst database or DISP_S1_PROCESSING_MODE_ENABLED is off"))
    sys.exit(NO_GUARD)

sensing = [naive_utc(dt) for dt in frame.sensing_datetimes]
lo = bisect.bisect_left(sensing, start)
hi = bisect.bisect_right(sensing, end)
if lo >= hi:
    print("no sensing dates in the requested range")
    sys.exit(OK)

blocking = []   # [label, first_pos, last_pos, count]
runnable = []
for pos in range(lo, hi):
    try:
        phase = phase_for_position(phases, pos)
    except PhaseValidationError:
        # past the annotated range: leading-edge dates appended after the database was built
        bucket, label = runnable, "leading edge, past the annotated range"
    else:
        bucket = blocking if phase.kind in (PhaseKind.HISTORICAL, PhaseKind.NO_RUN) else runnable
        label = "%s [%d,%d)" % (phase.label, phase.start_pos, phase.end_pos)
    if bucket and bucket[-1][0] == label:
        bucket[-1][2] = pos
        bucket[-1][3] += 1
    else:
        bucket.append([label, pos, pos, 1])

for label, first, last, count in blocking:
    print("owned by %s: %d sensing date(s), %s .. %s" % (label, count, sensing[first], sensing[last]))
for label, first, last, count in runnable:
    print("forward-eligible %s: %d sensing date(s), %s .. %s" % (label, count, sensing[first], sensing[last]))

sys.exit(BLOCKED if blocking else OK)
PYCHECK
}

# Pull flags out of the arguments; everything left stays positional as before
ARGS=()
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) ARGS+=("$arg") ;;
  esac
done
set -- "${ARGS[@]}"

START_DATE=${1:-$DEFAULT_START_DATE}
FINAL_END_DATE=${2:-$DEFAULT_END_DATE}
shift 2 || true

# Remaining arguments (if any) are frame IDs
FRAMES=("$@")

# Use default frame if none provided
if [ ${#FRAMES[@]} -eq 0 ]; then
  FRAMES=($DEFAULT_FRAME)
fi

echo "Start date: ${START_DATE}"
echo "Final end date: ${FINAL_END_DATE}"
echo "Frames: ${FRAMES[*]}"

# Drop frames whose dates a historical_NN or no_run phase owns. Forward submissions for those
# dates are marked superseded_by=historical_processing by the k-cycle evaluator and produce
# nothing. Frames in an un-annotated database, or any frame when the master switch is off, are
# submitted exactly as before.
echo "Checking processing-mode phases (sensing dates in the deployed burst database)..."
SUBMIT_FRAMES=()
for frame in "${FRAMES[@]}"; do
  phase_info=$(check_frame_phases "${frame}" "${START_DATE}" "${FINAL_END_DATE}")
  rc=$?
  case ${rc} in
    0)
      echo "frame ${frame}: OK"
      if [ -n "${phase_info}" ]; then echo "${phase_info}" | sed "s/^/  /"; fi
      SUBMIT_FRAMES+=("${frame}")
      ;;
    2)
      echo "frame ${frame}: phase check not applicable -- ${phase_info}; submitting as usual"
      SUBMIT_FRAMES+=("${frame}")
      ;;
    3)
      echo "WARNING: frame ${frame}: the requested range overlaps a historical or no_run phase:"
      echo "${phase_info}" | sed "s/^/  /"
      if [ ${FORCE} -eq 1 ]; then
        echo "  --force given, submitting frame ${frame} anyway"
        SUBMIT_FRAMES+=("${frame}")
      else
        echo "  SKIPPING frame ${frame}. Historical phases are submitted by the historical tool"
        echo "  (run_disp_s1_historical_processing.py) and no_run phases are never processed;"
        echo "  forward submissions for those dates are superseded and produce nothing."
        echo "  Re-run with --force to submit anyway."
      fi
      ;;
    *)
      echo "WARNING: frame ${frame}: phase check failed (exit ${rc}); submitting as usual"
      if [ -n "${phase_info}" ]; then echo "${phase_info}" | sed "s/^/  /"; fi
      SUBMIT_FRAMES+=("${frame}")
      ;;
  esac
done

FRAMES=("${SUBMIT_FRAMES[@]}")
if [ ${#FRAMES[@]} -eq 0 ]; then
  echo "No frames left to submit after the processing-mode phase check. Nothing to do."
  exit 1
fi
echo "Frames to submit: ${FRAMES[*]}"

# Convert ISO timestamps to epoch seconds
current_start=$(date -u -d "$START_DATE" +%s)
current_end=$(date -u -d "$START_DATE +10 minutes" +%s)
final_end=$(date -u -d "$FINAL_END_DATE" +%s)

while [ $current_start -lt $final_end ]; do
  start_iso=$(date -u -d "@$current_start" +"%Y-%m-%dT%H:%M:%SZ")
  end_iso=$(date -u -d "@$current_end" +"%Y-%m-%dT%H:%M:%SZ")

  #for frame in $(echo 8882 33065); do
  #for frame in $(echo 11116); do
  for frame in "${FRAMES[@]}"; do
    echo "Running CSLC download for frame ${frame} from ${start_iso} to ${end_iso}..."

    # submit cslc_download jobs
    # note below that start-date and end-date below are revision times not temporal
    python ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py \
      query \
      -c OPERA_L2_CSLC-S1_V1 \
      --chunk-size=1 \
      --k=15 \
      --m=6 \
      --job-queue=opera-job_worker-cslc_data_download \
      --processing-mode=forward \
      --frame-id ${frame} \
      --start-date="${start_iso}" \
      --end-date="${end_iso}" > daac_data_subscriber-${frame}.log 2>&1

    # extract list of cslc_download job UUIDs
    grep "download jobs succeeded=" daac_data_subscriber-${frame}.log | sed "s/.*\[\(.*\)\].*/\\1/" | tr ',' '\n' | sed "s/'//g" | sed 's/^ *//' > jobs-cslc_download-${frame}.txt

    # wait for resulting pipeline to complete
    for cslc_job_id in $(cat jobs-cslc_download-${frame}.txt); do

      # wait for CSLC job to finish (complete or fail)
      echo "cslc_job_id: ${cslc_job_id}"
      result=$(monitor_cslc_job "${cslc_job_id}")
      latest_idx=$(echo "$result" | cut -d'|' -f1)
      status=$(echo "$result" | cut -d'|' -f2)

      # handle the cslc_download result
      if [ "$result" == "ERROR:TIMEOUT" ]; then
          echo "ERROR: Timeout error for job ID: $cslc_job_id"
          # continue to next frame
          continue
      elif [ "$status" == "job-failed" ]; then
          echo "ERROR: Job failed for job ID: $cslc_job_id"
          # continue to next frame
          continue
      elif [ "$status" == "job-completed" ]; then
          echo "SUCCESS: frame=${frame}, latest_idx=${latest_idx}, status=${status}"
      else
          echo "ERROR: Unknown status or result for job ID: $cslc_job_id, result: $result"
          # continue to next frame
          continue
      fi

      # wait for S3_DISP_S1 job to finish (complete or fail)
      result=$(monitor_l3_disp_s1_job "${frame}" "${latest_idx}")
      l3_job_id=$(echo "$result" | cut -d'|' -f1)
      status=$(echo "$result" | cut -d'|' -f2)

      # handle the s3_disp_s1 result
      if [ "$result" == "ERROR:TIMEOUT" ]; then
          echo "ERROR: Timeout error for looking for L3_DISP_S1 job ID for: frame:${frame} latest_idx:${latest_idx}"
          # continue to next frame
          continue
      elif [ "$status" == "job-failed" ]; then
          echo "ERROR: Timeout error for looking for L3_DISP_S1 job ID for: frame:${frame} latest_idx:${latest_idx}"
          # continue to next frame
          continue
      elif [ "$status" == "job-completed" ]; then
          echo "SUCCESS: frame=${frame}, latest_idx=${latest_idx}, l3_job_id:${l3_job_id}, status=${status}"
      else
          echo "ERROR: Unknown status or result for L3_DISP_S1 job ID for: frame:${frame} latest_idx:${latest_idx}"
          # continue to next frame
          continue
      fi
    done
  done   # for frame

  # Increment time window by 10 minutes
  current_start=$current_end
  current_end=$((current_end + 600))
done

echo "✅ All jobs completed up to ${FINAL_END_DATE}."
