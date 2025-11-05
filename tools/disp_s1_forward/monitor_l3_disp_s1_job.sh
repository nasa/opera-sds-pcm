#!/bin/bash
#
# Monitor L3_DISP_S1 job using frame_id and latest_acq_index
# Usage: monitor_l3_disp_s1_job <frame_id> <latest_acq_index> [opensearch_url]
#

monitor_l3_disp_s1_job() {
    local FRAME_ID="${1}"
    local LATEST_IDX="${2}"
    local OPENSEARCH_URL="${3:-http://localhost:9200}"
#    local TIMEOUT_SECONDS="${4:-3600}"  # Default 1 hour
    local TIMEOUT_SECONDS="${4:-10800}"  # Default 3 hours
    
    if [ -z "$FRAME_ID" ] || [ -z "$LATEST_IDX" ]; then
        echo "Error: Frame ID and latest index required" >&2
        echo "Usage: monitor_l3_disp_s1_job <frame_id> <latest_idx> [opensearch_url] [timeout_seconds]" >&2
        return 1
    fi
    
    # Construct job_id pattern
    local JOB_PATTERN="job-WF-SCIFLO_L3_DISP_S1-frame-${FRAME_ID}-latest_acq_index-${LATEST_IDX}-*"
    
    # Exponential backoff: start with 1s, double each iteration, max 64s
    local sleep_time=1
    local max_sleep=64
    local iteration=1
    local start_time=$(date +%s)
    
    echo "Monitoring L3_DISP_S1 job with pattern: $JOB_PATTERN (timeout: ${TIMEOUT_SECONDS}s)" >&2
    
    while true; do
        # Check timeout
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        if [ $elapsed -ge $TIMEOUT_SECONDS ]; then
            echo "Timeout reached after ${elapsed}s (limit: ${TIMEOUT_SECONDS}s)" >&2
            echo "ERROR:TIMEOUT"
            return 2
        fi
        
        # Query OpenSearch for job by job_id pattern
        local response=$(curl -s "${OPENSEARCH_URL}/job_status-current/_search" -X POST -H 'Content-Type: application/json' -d "{
            \"query\": {
                \"wildcard\": {
                    \"job_id\": \"$JOB_PATTERN\"
                }
            }
        }")
        
        # Check if we got a response
        local total=$(echo "$response" | jq -r '.hits.total.value // .hits.total // 0')
        
        if [ "$total" -eq 0 ]; then
            echo "No job found with pattern: $JOB_PATTERN (iteration $iteration, ${elapsed}s elapsed, retrying in ${sleep_time}s...)" >&2
        else
            # Get the first job's status
            local status=$(echo "$response" | jq -r '.hits.hits[0]._source.status // empty')
            local job_id=$(echo "$response" | jq -r '.hits.hits[0]._source.job_id // empty')
            
            if [ -z "$status" ]; then
                echo "Warning: Job found but no status field (iteration $iteration, retrying in ${sleep_time}s...)" >&2
            elif [ "$status" == "job-completed" ]; then
                echo "L3_DISP_S1 job completed successfully" >&2
                echo "Job ID: $job_id" >&2
                echo "${job_id}|job-completed"
                return 0
            elif [ "$status" == "job-failed" ]; then
                echo "L3_DISP_S1 job failed" >&2
                echo "Job ID: $job_id" >&2
                echo "${job_id}|job-failed"
                return 1
            else
                echo "Job status: $status (iteration $iteration, ${elapsed}s elapsed, checking again in ${sleep_time}s...)" >&2
            fi
        fi
        
        # Sleep with exponential backoff
        sleep "$sleep_time"
        sleep_time=$(( sleep_time * 2 ))
        if [ $sleep_time -gt $max_sleep ]; then
            sleep_time=$max_sleep
        fi
        
        ((iteration++))
    done
}

# If script is executed directly (not sourced), run the function
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        echo "Error: jq is required but not installed. Install it with: brew install jq" >&2
        exit 1
    fi
    
    monitor_l3_disp_s1_job "$@"
fi
