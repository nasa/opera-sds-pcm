#!/bin/bash
#
# audit_batch_procs_completeness.sh
#
# Audit multiple batch proc JSON files for DISP-S1 completeness.
# For each batch proc, determines expected DISP-S1 products based on CSLC availability
# and compares against actual products in CMR.
#
# Usage:
#     ./audit_batch_procs_completeness.sh <batch_proc_dir> [output_dir] [--workers N] [--end-date DATE] [--iso-cache-dir DIR] [--low-memory]
#
# Options:
#     --workers N          Number of parallel workers for CMR/ISO XML queries (default: 20)
#     --end-date DATE      End date for CMR queries (default: 2025-12-31T00:00:00Z)
#     --iso-cache-dir DIR  Directory for caching ISO XML files (speeds up re-runs)
#     --low-memory         Enable low memory mode (recommended for large batch procs with many frames)
#
# Examples:
#     # Basic usage
#     ./audit_batch_procs_completeness.sh ~/DISP-S1/catchup ./completeness_results
#
#     # With more parallel workers
#     ./audit_batch_procs_completeness.sh ~/DISP-S1/catchup ./completeness_results --workers 50
#
#     # With custom end date
#     ./audit_batch_procs_completeness.sh ~/DISP-S1/catchup ./completeness_results --end-date 2025-06-01T00:00:00Z
#
#     # With ISO XML caching (recommended for re-runs)
#     ./audit_batch_procs_completeness.sh ~/DISP-S1/catchup ./completeness_results --iso-cache-dir ./iso_cache
#
#     # With low memory mode (recommended for batch procs with 100+ frames)
#     ./audit_batch_procs_completeness.sh ~/DISP-S1/catchup ./completeness_results --low-memory --iso-cache-dir ./iso_cache
#

set -e

# Parse arguments
BATCH_PROC_DIR=""
OUTPUT_DIR="./completeness_results"
WORKERS=20
START_DATE="2016-07-01T00:00:00Z"
END_DATE="2025-12-31T00:00:00Z"
ISO_CACHE_DIR=""
LOW_MEMORY=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --iso-cache-dir)
            ISO_CACHE_DIR="$2"
            shift 2
            ;;
        --low-memory)
            LOW_MEMORY="--low-memory"
            shift
            ;;
        *)
            if [[ -z "$BATCH_PROC_DIR" ]]; then
                BATCH_PROC_DIR="$1"
            elif [[ "$OUTPUT_DIR" == "./completeness_results" ]]; then
                OUTPUT_DIR="$1"
            fi
            shift
            ;;
    esac
done

if [[ -z "$BATCH_PROC_DIR" ]]; then
    echo "Usage: $0 <batch_proc_dir> [output_dir] [--workers N] [--end-date DATE] [--iso-cache-dir DIR] [--low-memory]"
    exit 1
fi

OPERA_PCM_PATH="/export/home/hysdsops/tmp/opera-sds-pcm"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Summary file
SUMMARY_FILE="$OUTPUT_DIR/completeness_summary.txt"
echo "DISP-S1 Completeness Audit Summary" > "$SUMMARY_FILE"
echo "===================================" >> "$SUMMARY_FILE"
echo "Run date: $(date)" >> "$SUMMARY_FILE"
echo "Date range: $START_DATE to $END_DATE" >> "$SUMMARY_FILE"
echo "Workers: $WORKERS" >> "$SUMMARY_FILE"
if [[ -n "$LOW_MEMORY" ]]; then
    echo "Low memory mode: enabled" >> "$SUMMARY_FILE"
fi
echo "" >> "$SUMMARY_FILE"

# Totals for overall summary
TOTAL_FRAMES=0
TOTAL_EXPECTED=0
TOTAL_FOUND=0
TOTAL_MISSING=0
TOTAL_NOT_TRIGGERABLE=0
TOTAL_INCOMPLETE=0
TOTAL_COMPLETE=0
TOTAL_STALE=0
TOTAL_UNEXPECTED=0

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

    # Output files
    json_output="$OUTPUT_DIR/completeness_${filename}.json"
    log_output="$OUTPUT_DIR/completeness_${filename}.log"

    # Run completeness audit
    echo "  Running completeness audit (workers=$WORKERS)..."
    CACHE_ARG=""
    if [[ -n "$ISO_CACHE_DIR" ]]; then
        CACHE_ARG="--iso-cache-dir $ISO_CACHE_DIR"
        echo "  Using ISO XML cache: $ISO_CACHE_DIR"
    fi
    if [[ -n "$LOW_MEMORY" ]]; then
        echo "  Using low memory mode"
    fi
    PYTHONPATH="$OPERA_PCM_PATH:$PYTHONPATH" python "$OPERA_PCM_PATH/tools/ops/cmr_audit/audit_disp_s1_completeness.py" \
        --frames "$frames" \
        --start "$START_DATE" \
        --end "$END_DATE" \
        --max-workers "$WORKERS" \
        --output "$json_output" \
        $CACHE_ARG \
        $LOW_MEMORY \
        2>&1 | tee "$log_output"

    # Determine output file (JSONL in low-memory mode, JSON otherwise)
    if [[ -n "$LOW_MEMORY" ]]; then
        actual_output="${json_output%.json}.jsonl"
    else
        actual_output="$json_output"
    fi

    # Extract summary stats from JSON/JSONL output
    stats=$(python3 -c "
import json
import sys
try:
    total_expected = 0
    total_found = 0
    total_missing = 0
    total_not_triggerable = 0
    total_incomplete = 0
    total_complete = 0
    total_stale = 0
    total_unexpected = 0

    output_file = '$actual_output'
    is_jsonl = output_file.endswith('.jsonl')

    if is_jsonl:
        # JSONL format: one JSON object per line
        with open(output_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if obj.get('type') == 'frame_report':
                    summary = obj.get('report', {}).get('summary', {})
                    total_expected += summary.get('expected', 0)
                    total_found += summary.get('found', 0)
                    total_missing += summary.get('missing', 0)
                    total_not_triggerable += summary.get('not_triggerable', 0)
                    total_incomplete += summary.get('incomplete', 0)
                    total_complete += summary.get('complete', 0)
                    total_stale += summary.get('stale', 0)
                    total_unexpected += summary.get('unexpected', 0)
    else:
        # Standard JSON format
        with open(output_file) as f:
            data = json.load(f)
        for frame_id, report in data.get('reports', {}).items():
            summary = report.get('summary', {})
            total_expected += summary.get('expected', 0)
            total_found += summary.get('found', 0)
            total_missing += summary.get('missing', 0)
            total_not_triggerable += summary.get('not_triggerable', 0)
            total_incomplete += summary.get('incomplete', 0)
            total_complete += summary.get('complete', 0)
            total_stale += summary.get('stale', 0)
            total_unexpected += summary.get('unexpected', 0)

    # Coverage based on matched (complete + incomplete + stale_ref) vs effective expected
    # Effective expected excludes not_triggerable (those couldn't be generated)
    matched = total_complete + total_incomplete + total_stale
    effective_expected = total_expected - total_not_triggerable
    coverage = (matched / effective_expected * 100) if effective_expected > 0 else 0
    print(f'{total_expected},{total_found},{total_missing},{total_not_triggerable},{total_incomplete},{total_complete},{total_stale},{total_unexpected},{coverage:.1f}')
except Exception as e:
    print('0,0,0,0,0,0,0,0,0.0', file=sys.stderr)
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
")

    IFS=',' read -r expected found missing not_triggerable incomplete complete stale unexpected coverage <<< "$stats"

    # Update totals
    TOTAL_FRAMES=$((TOTAL_FRAMES + frame_count))
    TOTAL_EXPECTED=$((TOTAL_EXPECTED + expected))
    TOTAL_FOUND=$((TOTAL_FOUND + found))
    TOTAL_MISSING=$((TOTAL_MISSING + missing))
    TOTAL_NOT_TRIGGERABLE=$((TOTAL_NOT_TRIGGERABLE + not_triggerable))
    TOTAL_INCOMPLETE=$((TOTAL_INCOMPLETE + incomplete))
    TOTAL_COMPLETE=$((TOTAL_COMPLETE + complete))
    TOTAL_STALE=$((TOTAL_STALE + stale))
    TOTAL_UNEXPECTED=$((TOTAL_UNEXPECTED + unexpected))

    # Write to summary file
    echo "" >> "$SUMMARY_FILE"
    echo "$filename:" >> "$SUMMARY_FILE"
    echo "  Frames: $frame_count" >> "$SUMMARY_FILE"
    echo "  Expected: $expected, Found: $found, Coverage: ${coverage}%" >> "$SUMMARY_FILE"
    echo "  Complete: $complete, Incomplete: $incomplete, Stale: $stale, Unexpected: $unexpected" >> "$SUMMARY_FILE"
    echo "  Not Triggerable: $not_triggerable, Missing: $missing" >> "$SUMMARY_FILE"

    echo "  Done: Expected=$expected, Found=$found, Coverage=${coverage}%"
    echo "        Complete=$complete, Incomplete=$incomplete, Stale=$stale, Unexpected=$unexpected"
    echo "        Not Triggerable=$not_triggerable, Missing=$missing"
    echo ""
done

# Write overall totals
echo "" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"
echo "OVERALL TOTALS" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"
echo "Total frames audited: $TOTAL_FRAMES" >> "$SUMMARY_FILE"
echo "Total expected products: $TOTAL_EXPECTED" >> "$SUMMARY_FILE"
echo "Total found in CMR: $TOTAL_FOUND" >> "$SUMMARY_FILE"
echo "  - Complete: $TOTAL_COMPLETE" >> "$SUMMARY_FILE"
echo "  - Incomplete: $TOTAL_INCOMPLETE" >> "$SUMMARY_FILE"
echo "  - Stale: $TOTAL_STALE" >> "$SUMMARY_FILE"
echo "  - Unexpected: $TOTAL_UNEXPECTED" >> "$SUMMARY_FILE"
echo "Total not triggerable (CSLC gaps): $TOTAL_NOT_TRIGGERABLE" >> "$SUMMARY_FILE"
echo "Total missing (actionable): $TOTAL_MISSING" >> "$SUMMARY_FILE"
if [[ $TOTAL_EXPECTED -gt 0 ]]; then
    # Coverage based on matched (complete + incomplete + stale) vs effective expected
    # Effective expected excludes not_triggerable (those couldn't be generated)
    TOTAL_MATCHED=$((TOTAL_COMPLETE + TOTAL_INCOMPLETE + TOTAL_STALE))
    EFFECTIVE_EXPECTED=$((TOTAL_EXPECTED - TOTAL_NOT_TRIGGERABLE))
    if [[ $EFFECTIVE_EXPECTED -gt 0 ]]; then
        OVERALL_COVERAGE=$(python3 -c "print(f'{$TOTAL_MATCHED / $EFFECTIVE_EXPECTED * 100:.1f}')")
    else
        OVERALL_COVERAGE="N/A"
    fi
    echo "Overall coverage: ${OVERALL_COVERAGE}%" >> "$SUMMARY_FILE"
fi

echo "========================================"
echo "Audit complete. Results in: $OUTPUT_DIR"
echo "========================================"
echo ""
cat "$SUMMARY_FILE"
