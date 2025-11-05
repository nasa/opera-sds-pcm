#!/bin/bash

# source functions
source monitor_cslc_job.sh
source monitor_l3_disp_s1_job.sh

#DEFAULT_START_DATE="2024-05-01T00:00:00Z"
DEFAULT_START_DATE="2024-05-01T14:50:00Z"
DEFAULT_END_DATE="2024-05-02T00:00:00Z"
#DEFAULT_END_DATE="2024-12-27T10:00:00Z"
DEFAULT_FRAME="11116"

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
#      --start-date=2024-05-01T00:00:00Z \
#      --end-date=2024-05-01T02:00:00Z > daac_data_subscriber-${frame}.log 2>&1

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
