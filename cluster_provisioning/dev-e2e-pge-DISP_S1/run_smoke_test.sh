#!/bin/bash
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-pge-DISP_S1"

# check args
if [ "$#" -eq 1 ]; then
  config_file=${1}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ${config_file}

# fail on any errors
set -ex

cd ~/.sds/files

# backup settings.yaml
cp ~/mozart/ops/opera-pcm/conf/settings.yaml ~/mozart/ops/opera-pcm/conf/settings.yaml.bak

# disable simulation mode
sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ~/mozart/ops/opera-pcm/conf/settings.yaml

# propagate settings change
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

# test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json

# change to test directory
cd ${TEST_DIR}

# ============================================================
# Phase 1: Historical Processing
# ============================================================

# install test batch_proc
python ~/mozart/ops/opera-pcm/tools/pcm_batch.py create --file disp_s1_test_batch_proc.json

# run processing
nohup python ~/mozart/ops/opera-pcm/tools/run_disp_s1_historical_processing.py &
HIST_PID=$!

# verify historical datasets (~4 hours for SCIFLO_L3_DISP_S1 jobs to complete)
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json hist --max_time 14400 /tmp/datasets_hist.txt

# stop the historical processor (it loops forever after completing)
kill $HIST_PID 2>/dev/null || true

# ============================================================
# Phase 2: Forward Processing (Evaluator Pipeline)
# ============================================================
# Submit a cslc_catalog_ingest job to create metadata-only L2_CSLC_S1
# datasets from CMR for forward sensing dates. HySDS publishes these to
# GRQ, triggering the evaluator pipeline:
#   L2_CSLC_S1 → cycle_evaluator → CSC → k_cycle_evaluator → KSC → SCIFLO_L3_DISP_S1
#
# Frame 31241 with k=15, m=3 (smoke test override; OPS default is m=6).
# Using m=3 reduces the CCSLC requirement to 2 sets, making the smoke
# test feasible with limited historical data.  The date range covers
# enough sensing dates to produce L3_DISP_S1 products.
#
# The evaluators are idempotent (always re-assess from scratch) and safe
# for concurrent triggers — the same pattern as NISAR's RRST evaluator.

MOZART_PVT_IP=$(grep ^MOZART_PVT_IP ~/.sds/config | awk '{print $2}')
JOB_RELEASE=$(grep 'JOB_RELEASE' ~/.sds/config | head -1 | awk '{print $2}')
MOZART_ES_URL="https://${MOZART_PVT_IP}:9200"

# Override m=6 → m=3 on ALL k-cycle evaluator trigger rules for smoke test.
# Both rules must be updated: the CSC-triggered rule and the CCSLC-triggered rule.
# If only the CSC rule is updated, CCSLC ingestions that occur after m is restored
# will trigger k-cycle evaluator jobs with the default m=6.
K_CYCLE_RULES=("trigger-disp_s1_k_cycle_evaluator" "trigger-disp_s1_k_cycle_evaluator_on_ccslc")

echo "Setting m=3 on all k-cycle evaluator trigger rules for smoke test"
for rule in "${K_CYCLE_RULES[@]}"; do
  echo "  Setting m=3 on ${rule}"
  curl -k --netrc-file ~/.netrc-os -XPOST "${MOZART_ES_URL}/user_rules-grq/_update_by_query?refresh=true" \
    -H 'Content-Type: application/json' \
    -d "{
      \"script\": {
        \"source\": \"ctx._source.kwargs = \\\"{\\\\\\\"m\\\\\\\": 3}\\\"\",
        \"lang\": \"painless\"
      },
      \"query\": {
        \"term\": {\"rule_name\": \"${rule}\"}
      }
    }"
done

restore_m_default() {
  echo "Restoring m=6 on all k-cycle evaluator trigger rules"
  for rule in "${K_CYCLE_RULES[@]}"; do
    echo "  Restoring m=6 on ${rule}"
    curl -k --netrc-file ~/.netrc-os -XPOST "${MOZART_ES_URL}/user_rules-grq/_update_by_query?refresh=true" \
      -H 'Content-Type: application/json' \
      -d "{
        \"script\": {
          \"source\": \"ctx._source.kwargs = \\\"{}\\\"\",
          \"lang\": \"painless\"
        },
        \"query\": {
          \"term\": {\"rule_name\": \"${rule}\"}
        }
      }"
  done
}
trap restore_m_default EXIT

# Serialized forward simulation: ingest ONE sensing date at a time and wait for
# its L3_DISP_S1 (and CCSLC at k-boundaries) to publish before ingesting the
# next date — mirroring real forward operations.  This drains each date before
# the next, so every KSC sees its in-window CCSLC already published and never
# finalizes on a stale CCSLC.  (The previous bulk cslc_catalog_ingest over the
# whole range flooded the system with out-of-order CSLCs, producing the
# CCSLC-rotation flicker seen in the count-only smoke.)
python ~/mozart/ops/opera-pcm/tools/run_disp_s1_forward_serial.py \
  --frame-id 31241 \
  --start-date 2017-10-23T00:00:00Z \
  --end-date 2019-06-01T00:00:00Z \
  --mozart-ip "${MOZART_PVT_IP}" \
  --job-release "${JOB_RELEASE}" \
  --ksc-timeout-mins 30 \
  --l3-timeout-mins 120 \
  --continue-on-timeout || true

# Verify forward datasets.  Expected counts assume all DISP-S1 jobs succeed
# including early post-CCSLC windows (pending ADT dolphin fix).  Until then,
# this check will timeout — that's expected.
# (~3 hours for forward pipeline to complete including CCSLC rotation)
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json fwd --max_time 14400 /tmp/datasets_fwd.txt || true

# ============================================================
# Phase 3: Visualization
# ============================================================
# Download RunConfig files from successful and failed L3_DISP_S1 jobs
# and generate forward processing timeline visualization.

DATASET_BUCKET=$(grep "^DATASET_BUCKET:" ~/mozart/ops/opera-pcm/conf/settings.yaml | awk "{print \$2}" | tr -d "'")
TRIAGE_BUCKET=$(grep "^TRIAGE_BUCKET:" ~/mozart/ops/opera-pcm/conf/settings.yaml | awk "{print \$2}" | tr -d "'")
RUN_CONFIGS_DIR="/tmp/disp_s1_run_configs"
FAILED_CONFIGS_DIR="/tmp/disp_s1_run_configs_failed"
rm -rf "${RUN_CONFIGS_DIR}" "${FAILED_CONFIGS_DIR}"
mkdir -p "${RUN_CONFIGS_DIR}" "${FAILED_CONFIGS_DIR}"

# Download .rc.yaml files from successful L3_DISP_S1 products
echo "Downloading RunConfig files from s3://${DATASET_BUCKET}/products/DISP_S1/"
aws s3 cp "s3://${DATASET_BUCKET}/products/DISP_S1/" "${RUN_CONFIGS_DIR}/" \
  --recursive --exclude "*" --include "*.rc.yaml"

# Flatten: move .rc.yaml files from subdirectories to the top level
find "${RUN_CONFIGS_DIR}" -mindepth 2 -name "*.rc.yaml" -exec mv {} "${RUN_CONFIGS_DIR}/" \;
find "${RUN_CONFIGS_DIR}" -mindepth 1 -type d -empty -delete

echo "Downloaded $(ls "${RUN_CONFIGS_DIR}"/*.rc.yaml 2>/dev/null | wc -l) successful RunConfig files"

# Download RunConfig.yaml from failed (triaged) DISP-S1 jobs
echo "Downloading RunConfig files from failed jobs in s3://${TRIAGE_BUCKET}/"
for triage_dir in $(aws s3 ls "s3://${TRIAGE_BUCKET}/" | grep "SCIFLO_L3_DISP_S1" | awk '{print $2}'); do
  rc_file=$(aws s3 ls "s3://${TRIAGE_BUCKET}/${triage_dir}pge_runconfig_dir/" 2>/dev/null | grep "RunConfig.yaml" | awk '{print $4}')
  if [ -n "$rc_file" ]; then
    sd=$(echo "$triage_dir" | grep -o 'f[0-9]*-[0-9]*' | grep -o '[0-9]*$')
    aws s3 cp "s3://${TRIAGE_BUCKET}/${triage_dir}pge_runconfig_dir/RunConfig.yaml" \
      "${FAILED_CONFIGS_DIR}/FAILED_F31241_${sd}.rc.yaml" 2>/dev/null
  fi
done

n_failed=$(ls "${FAILED_CONFIGS_DIR}"/*.rc.yaml 2>/dev/null | wc -l)
echo "Downloaded ${n_failed} failed RunConfig files"

# Generate timeline visualization
FAILED_OPT=""
if [ "${n_failed}" -gt 0 ]; then
  FAILED_OPT="--failed ${FAILED_CONFIGS_DIR}"
fi

echo "Generating forward processing timeline visualization"
python ~/mozart/ops/opera-pcm/tools/analyze_disp_s1_forward_processing_timeline.py \
  "${RUN_CONFIGS_DIR}" ${FAILED_OPT}

# Copy visualization to /tmp for download by terraform
if [ -f "${RUN_CONFIGS_DIR}/timeline_diagrams/"*.png ]; then
  cp "${RUN_CONFIGS_DIR}/timeline_diagrams/"*.png /tmp/disp_s1_timeline.png
  echo "Timeline visualization saved to /tmp/disp_s1_timeline.png"
fi
