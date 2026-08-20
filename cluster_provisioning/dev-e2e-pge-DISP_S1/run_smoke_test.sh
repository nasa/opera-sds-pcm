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

##############################################################################
# Phased historical processing.
#
# The stage above runs with DISP_S1_PROCESSING_MODE_ENABLED off, which is the
# shipping default: the burst database's mode labels are dropped at load and
# the walk steps k dates at a time from the start of the series. That stage is
# the behavior-neutrality check and must run BEFORE the switch is flipped.
#
# This stage turns the master switch on and walks a frame phase by phase.
#
# It walks frame 24718, which discriminates the two paths absolutely rather than
# merely exercising the phased one. Its annotations open with no_run[11] and then
# historical_03[15] at 2025-05-29:
#
#   phased    submits idx 11..25 = 2025-05-29 .. 2025-12-31, one clean ministack
#   un-phased submits idx  0..14 = 2016-08-14 .. 2025-07-16, which contains a
#             1656-day and a 1367-day acquisition gap; the SAS rejects that stack
#             outright with InputValidationError and the SCIFLO fails
#
# So a regression that silently reverted to the absolute grid cannot pass: the
# phased path yields 14 products, the legacy path yields a failed job and none.
# This is the failure that was reported against this branch, so the stage doubles
# as its regression test. 24718 carries 3 bursts, and its products cannot collide
# with the 31241 products the surrounding stages assert on.
#
# The switch is venue-wide, so it is restored before the forward stage below.
# With it on, every KSC whose sensing date falls in a historical phase is
# marked superseded_by=historical_processing -- and 31241's forward window
# (2017-10-23 .. 2019-06-01) lies entirely inside its historical_01 block, so
# leaving the switch on here would supersede every forward SCIFLO and produce
# no products at all.
##############################################################################
SETTINGS=~/mozart/ops/opera-pcm/conf/settings.yaml

set_processing_mode() {
  local value=$1
  sed -i "s/^DISP_S1_PROCESSING_MODE_ENABLED:.*/DISP_S1_PROCESSING_MODE_ENABLED: ${value}/" ${SETTINGS}
  grep -E "^DISP_S1_PROCESSING_MODE_ENABLED:" ${SETTINGS}
  cd ~/.sds/files
  fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
  sds ship
  cd ${TEST_DIR}
}

restore_processing_mode() {
  echo "Restoring DISP_S1_PROCESSING_MODE_ENABLED to false"
  set_processing_mode false
}

echo "Enabling DISP_S1_PROCESSING_MODE_ENABLED for the phased stage"
set_processing_mode true
trap restore_processing_mode EXIT

python ~/mozart/ops/opera-pcm/tools/pcm_batch.py create --file disp_s1_test_batch_proc_phased.json

nohup python ~/mozart/ops/opera-pcm/tools/run_disp_s1_historical_processing.py &
HIST_PHASED_PID=$!

# check_datasets_file.py raises RuntimeError and exits non-zero when a count is
# not met, and this script runs under `set -e`. Without `|| true` a phased-stage
# failure would abort before the forward stage below and take its coverage with
# it. The ERROR line is still written to the result file, and check_pcm.py turns
# that into a reported test failure.
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json hist_phased --max_time 14400 /tmp/datasets_hist_phased.txt || true

kill $HIST_PHASED_PID 2>/dev/null || true

# Counts alone do not prove the walk skipped the no_run block rather than simply
# failing on it, so assert the phase structure too: the compressed CSLC boundary
# at the phase-relative position, no products on any no_run date, and every
# historical/no_run phase KSC superseded. Must run before the switch goes back
# off -- the check reads the frame's phases, which only exist while it is on.
cd ~/mozart/ops/opera-pcm
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_disp_s1_phases.py \
  --frame-id 24718 --k 15 --out /tmp/phases_hist_phased.txt || true
cd ${TEST_DIR}

restore_processing_mode
trap - EXIT

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

# Set initial whitelist for testing
python ~/mozart/ops/opera-pcm/tools/disp_s1_set_whitelist.py --whitelist-regions 4

disable_whitelist(){
  echo "Disabling whitelist"
  python ~/mozart/ops/opera-pcm/tools/disp_s1_set_whitelist.py --disable-whitelist
}
trap disable_whitelist EXIT

# Serialized forward simulation: ingest ONE sensing date at a time and wait for
# its L3_DISP_S1 (and CCSLC at k-boundaries) to publish before ingesting the
# next date — mirroring real forward operations.  This drains each date before
# the next, so every KSC sees its in-window CCSLC already published and never
# finalizes on a stale CCSLC.  (The previous bulk cslc_catalog_ingest over the
# whole range flooded the system with out-of-order CSLCs, producing the
# CCSLC-rotation flicker seen in the count-only smoke.)
# Forward driver mode: full-serial (faithful drain, default) or boundary-serial
# (only block at CCSLC boundaries; Stage B/C scale).  Read from ~/.serial_mode so
# a venue can be switched without code change — write 'boundary-serial' to
# ~/.serial_mode any time before this phase (the ~hours-long historical phase
# gives ample margin).  Missing file -> full-serial.
SERIAL_MODE="$(cat $HOME/.serial_mode 2>/dev/null || echo full-serial)"
echo "DISP-S1 forward driver mode: ${SERIAL_MODE}"
python -u ~/mozart/ops/opera-pcm/tools/run_disp_s1_forward_serial.py \
  --frame-id 31241 \
  --start-date 2017-10-23T00:00:00Z \
  --end-date 2019-06-01T00:00:00Z \
  --mozart-ip "${MOZART_PVT_IP}" \
  --job-release "${JOB_RELEASE}" \
  --mode "${SERIAL_MODE}" \
  --ksc-timeout-mins 60 \
  --l3-timeout-mins 120 \
  --region-whitelist 4 \
  --continue-on-timeout || true

# Verify forward datasets.  Expected counts assume all DISP-S1 jobs succeed
# including early post-CCSLC windows (pending ADT dolphin fix).  Until then,
# this check will timeout — that's expected.
# (~3 hours for forward pipeline to complete including CCSLC rotation)
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json fwd --max_time 14400 /tmp/datasets_fwd.txt || true

# Get the number of KSCs triggerable using the current DISP-S1 trigger rule
get_triggerable_ksc_count() {
  # Query the GRQ rules index for the DISP-S1 trigger
  trigger_rule_resp=$(curl -sk --netrc-file ~/.netrc-os -XPOST "${MOZART_ES_URL}/user_rules-grq/_search" \
    -H 'Content-Type: application/json' \
    -d "{
      \"query\": {
        \"term\": {\"rule_name\": \"trigger-SCIFLO_L3_DISP_S1\"}
      }
    }")

  # Verify that we have exactly one document
  hits=$(echo "$trigger_rule_resp" | jq '.hits.total.value')

  if [[ "$hits" -ne "1" ]]; then
    echo "Could not find DISP-S1 trigger rule definition"
    exit 255
  fi

  # Extract and parse the trigger rule query string
  trigger_rule_qs=$(echo "$trigger_rule_resp" | jq '.hits.hits[0]._source.query_string | fromjson')

  # Query the KSC indices with the trigger rule's query string and get the returned count
  curl -sk --netrc-file ~/.netrc-os -XPOST "${MOZART_ES_URL}/grq_1_disp_s1-kcycle-state-config-*/_count" \
  -H 'Content-Type: application/json' \
  -d "$(echo "$trigger_rule_qs" | jq '{"query": .}')" | jq '.count'
}

# Flip the whitelist to exclude frame 31241
python ~/mozart/ops/opera-pcm/tools/disp_s1_set_whitelist.py --whitelist-regions 0

initial_triggerable_ksc_count=$(get_triggerable_ksc_count)

python -u ~/mozart/ops/opera-pcm/tools/run_disp_s1_forward_serial.py \
  --frame-id 31241 \
  --start-date 2019-06-01T00:00:00Z \
  --end-date 2019-06-13T00:00:00Z \
  --mozart-ip "${MOZART_PVT_IP}" \
  --job-release "${JOB_RELEASE}" \
  --mode "${SERIAL_MODE}" \
  --ksc-timeout-mins 60 \
  --l3-timeout-mins 120 \
  --region-whitelist 4 \
  --continue-on-timeout || true

post_submission_triggerable_ksc_count=$(get_triggerable_ksc_count)

if [[ "$initial_triggerable_ksc_count" -ne "$post_submission_triggerable_ksc_count" ]]; then
  echo "ERROR: DISP_S1 jobs were triggered despite not being in a whitelisted region"
  exit 1
fi

# Disable whitelist to not interfere with any future testing
python ~/mozart/ops/opera-pcm/tools/disp_s1_set_whitelist.py --disable-whitelist

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
