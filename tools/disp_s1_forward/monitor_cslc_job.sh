#!/bin/bash
#
# Monitor CSLC download job and extract latest_acq_index
# Usage: monitor_cslc_job <cslc_job_id> [opensearch_url]
#

monitor_cslc_job() {
    local JOB_ID="${1}"
    # DIT default: HTTPS-only on v6.0+ AMIs, auth via ~/.netrc-os
    local OPENSEARCH_URL="${2:-https://localhost:9200}"
    local TIMEOUT_SECONDS="${3:-3600}"  # Default 1 hour
    
    if [ -z "$JOB_ID" ]; then
        echo "Error: Job ID required" >&2
        return 1
    fi
    
    # Exponential backoff: start with 1s, double each iteration, max 64s
    local sleep_time=1
    local max_sleep=64
    local iteration=1
    local start_time=$(date +%s)
    
    echo "Monitoring CSLC job: $JOB_ID (timeout: ${TIMEOUT_SECONDS}s)" >&2
    
    while true; do
        # Check timeout
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        if [ $elapsed -ge $TIMEOUT_SECONDS ]; then
            echo "Timeout reached after ${elapsed}s (limit: ${TIMEOUT_SECONDS}s)" >&2
            echo "ERROR:TIMEOUT"
            return 2
        fi
        
        # Query OpenSearch for job status by UUID
        local response=$(curl -k --netrc-file ~/.netrc-os -s "${OPENSEARCH_URL}/job_status-current/_search" -X POST -H 'Content-Type: application/json' -d "{
            \"query\": {
                \"term\": {
                    \"_id\": \"$JOB_ID\"
                }
            }
        }")
        
        # Check if we got a response and extract status
        local status=$(echo "$response" | jq -r '.hits.hits[0]._source.status // empty')
        local job_id_full=$(echo "$response" | jq -r '.hits.hits[0]._source.job_id // empty')
        
        if [ -z "$status" ]; then
            echo "Warning: Job not found in OpenSearch. Retrying in ${sleep_time}s... (${elapsed}s elapsed)" >&2
        elif [ "$status" == "job-completed" ]; then
            echo "Job completed successfully" >&2
            
            # Extract latest_acq_index from the full job_id
            # Format: job-WF-cslc_download-frame-11116-acq_indices-1056-to-1176-timestamp
            local latest_acq_index=$(echo "$job_id_full" | sed -n 's/.*-acq_indices-[0-9]*-to-\([0-9]*\).*/\1/p')
            
            if [ -z "$latest_acq_index" ]; then
                echo "Warning: Could not extract latest_acq_index from job_id" >&2
                echo "ERROR:MISSING_DATA|job-completed"
                return 1
            fi
            
            echo "${latest_acq_index}|job-completed"
            return 0
            
        elif [ "$status" == "job-failed" ]; then
            echo "Job failed" >&2
            
            # Still try to extract latest_acq_index from the full job_id
            local latest_acq_index=$(echo "$job_id_full" | sed -n 's/.*-acq_indices-[0-9]*-to-\([0-9]*\).*/\1/p')
            
            if [ -n "$latest_acq_index" ]; then
                echo "${latest_acq_index}|job-failed"
            else
                echo "ERROR:NO_DATA|job-failed"
            fi
            
            return 1
            
        else
            echo "Job status: $status (iteration $iteration, ${elapsed}s elapsed, checking again in ${sleep_time}s...)" >&2
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
    
    monitor_cslc_job "$@"
fi
