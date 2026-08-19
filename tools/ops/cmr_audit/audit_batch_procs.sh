#!/bin/bash
#
# audit_batch_procs.sh
#
# Audit multiple batch proc JSON files against CMR and compare frame states.
# Supports both frame-states-only mode (fast) and full burst validation mode.
#
# Usage:
#     ./audit_batch_procs.sh <batch_proc_dir> [output_dir] [--burst-validation] [--workers N]
#
# Options:
#     --burst-validation    Enable burst-level validation using CMR ISO XML
#                          (slower but verifies DISP-S1 products used correct input CSLCs)
#     --workers N          Number of parallel workers for ISO XML fetching (default: 20)
#                          Higher values speed up burst validation but use more connections
#
# Examples:
#     # Fast frame-states-only audit (default)
#     ./audit_batch_procs.sh ~/DISP-S1/catchup ./audit_results
#
#     # Full burst validation audit
#     ./audit_batch_procs.sh ~/DISP-S1/catchup ./audit_results --burst-validation
#
#     # Full burst validation with more parallel workers
#     ./audit_batch_procs.sh ~/DISP-S1/catchup ./audit_results --burst-validation --workers 50
#

set -e

# Parse arguments
BATCH_PROC_DIR=""
OUTPUT_DIR="./audit_results"
BURST_VALIDATION=false
WORKERS=20

while [[ $# -gt 0 ]]; do
    case $1 in
        --burst-validation)
            BURST_VALIDATION=true
            shift
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        *)
            if [[ -z "$BATCH_PROC_DIR" ]]; then
                BATCH_PROC_DIR="$1"
            elif [[ "$OUTPUT_DIR" == "./audit_results" ]]; then
                OUTPUT_DIR="$1"
            fi
            shift
            ;;
    esac
done

if [[ -z "$BATCH_PROC_DIR" ]]; then
    echo "Usage: $0 <batch_proc_dir> [output_dir] [--burst-validation] [--workers N]"
    exit 1
fi

OPERA_PCM_PATH="/export/home/hysdsops/tmp/opera-sds-pcm"
START_DATE="2016-07-01T00:00:00Z"
END_DATE="2025-12-01T00:00:00Z"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Summary file
SUMMARY_FILE="$OUTPUT_DIR/audit_summary.txt"
echo "DISP-S1 Batch Proc Audit Summary" > "$SUMMARY_FILE"
echo "================================" >> "$SUMMARY_FILE"
echo "Run date: $(date)" >> "$SUMMARY_FILE"
if [[ "$BURST_VALIDATION" == true ]]; then
    echo "Mode: Full burst validation (using CMR ISO XML)" >> "$SUMMARY_FILE"
else
    echo "Mode: Frame-states-only (fast)" >> "$SUMMARY_FILE"
fi
echo "" >> "$SUMMARY_FILE"

# Find all JSON files in the batch proc directory
for batch_proc_file in "$BATCH_PROC_DIR"/*.json; do
    if [[ ! -f "$batch_proc_file" ]]; then
        echo "No JSON files found in $BATCH_PROC_DIR"
        exit 1
    fi

    filename=$(basename "$batch_proc_file" .json)
    echo "========================================"
    echo "Processing: $filename"
    echo "========================================"

    # Extract frames from the batch proc JSON
    frames=$(python3 -c "
import json
with open('$batch_proc_file') as f:
    data = json.load(f)
    frames = data.get('frames', [])
    print(','.join(str(f) for f in frames))
")

    if [[ -z "$frames" ]]; then
        echo "  WARNING: No frames found in $batch_proc_file, skipping..."
        echo "$filename: SKIPPED (no frames)" >> "$SUMMARY_FILE"
        continue
    fi

    frame_count=$(echo "$frames" | tr ',' '\n' | wc -l)
    echo "  Found $frame_count frames"

    # Output file for CMR audit
    audit_output="$OUTPUT_DIR/cmr_audit_${filename}.json"
    compare_output="$OUTPUT_DIR/compare_${filename}.txt"
    burst_audit_output="$OUTPUT_DIR/burst_audit_${filename}.txt"

    if [[ "$BURST_VALIDATION" == true ]]; then
        # Run CMR audit with full burst validation using CMR ISO XML
        echo "  Running CMR audit with burst validation (workers=$WORKERS)..."
        PYTHONPATH="$OPERA_PCM_PATH:$PYTHONPATH" python "$OPERA_PCM_PATH/tools/ops/cmr_audit/cmr_audit_disp_s1.py" \
            --start-datetime "$START_DATE" \
            --end-datetime "$END_DATE" \
            --processing-mode historical \
            --frames-only "$frames" \
            --output-frame-states "$audit_output" \
            --burst-data-source cmr \
            --workers "$WORKERS" \
            2>&1 | tee "$OUTPUT_DIR/cmr_audit_${filename}.log"

        # Extract burst validation stats from log
        published_count=$(grep -oP "Fully published.*len\(disp_s1_products\)=\K[\d,]+" "$OUTPUT_DIR/cmr_audit_${filename}.log" | tr -d ',' || echo "0")
        missing_count=$(grep -oP "Missing.*len\(disp_s1_products_miss\)=\K[\d,]+" "$OUTPUT_DIR/cmr_audit_${filename}.log" | tr -d ',' || echo "0")
    else
        # Run CMR audit (frame-states-only mode, no burst validation)
        echo "  Running CMR audit (frame-states-only)..."
        PYTHONPATH="$OPERA_PCM_PATH:$PYTHONPATH" python "$OPERA_PCM_PATH/tools/ops/cmr_audit/cmr_audit_disp_s1.py" \
            --start-datetime "$START_DATE" \
            --end-datetime "$END_DATE" \
            --processing-mode historical \
            --frames-only "$frames" \
            --output-frame-states "$audit_output" \
            --frame-states-only \
            2>&1 | tee "$OUTPUT_DIR/cmr_audit_${filename}.log"
    fi

    # Run comparison
    echo "  Running comparison..."
    python "$OPERA_PCM_PATH/tools/ops/cmr_audit/compare_disp_s1_frame_states.py" \
        "$audit_output" \
        "$batch_proc_file" \
        2>&1 | tee "$compare_output"

    # Extract summary stats from comparison output
    match_count=$(grep -oP 'Matching:\s+\K\d+' "$compare_output" || echo "0")
    cmr_ahead=$(grep -oP 'CMR ahead:\s+\K\d+' "$compare_output" || echo "0")
    batch_ahead=$(grep -oP 'Batch ahead:\s+\K\d+' "$compare_output" || echo "0")

    echo "" >> "$SUMMARY_FILE"
    echo "$filename:" >> "$SUMMARY_FILE"
    echo "  Frames: $frame_count" >> "$SUMMARY_FILE"
    echo "  Match: $match_count, CMR ahead: $cmr_ahead, Batch ahead: $batch_ahead" >> "$SUMMARY_FILE"

    if [[ "$BURST_VALIDATION" == true ]]; then
        echo "  Published: $published_count, Missing: $missing_count" >> "$SUMMARY_FILE"
        echo "  Done: Match=$match_count, CMR ahead=$cmr_ahead, Batch ahead=$batch_ahead, Published=$published_count, Missing=$missing_count"
    else
        echo "  Done: Match=$match_count, CMR ahead=$cmr_ahead, Batch ahead=$batch_ahead"
    fi
    echo ""
done

echo "========================================"
echo "Audit complete. Results in: $OUTPUT_DIR"
echo "========================================"
echo ""
cat "$SUMMARY_FILE"
