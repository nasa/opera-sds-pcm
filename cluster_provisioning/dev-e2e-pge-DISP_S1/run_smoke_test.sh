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
MOZART_ES_URL="http://${MOZART_PVT_IP}:9200"

# Override m=6 → m=3 on ALL k-cycle evaluator trigger rules for smoke test.
# Both rules must be updated: the CSC-triggered rule and the CCSLC-triggered rule.
# If only the CSC rule is updated, CCSLC ingestions that occur after m is restored
# will trigger k-cycle evaluator jobs with the default m=6.
K_CYCLE_RULES=("trigger-disp_s1_k_cycle_evaluator" "trigger-disp_s1_k_cycle_evaluator_on_ccslc")

echo "Setting m=3 on all k-cycle evaluator trigger rules for smoke test"
for rule in "${K_CYCLE_RULES[@]}"; do
  echo "  Setting m=3 on ${rule}"
  curl -XPOST "${MOZART_ES_URL}/user_rules-grq/_update_by_query?refresh=true" \
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

# Function to restore m=6 (called on exit or after test)
restore_m_default() {
  echo "Restoring m=6 on all k-cycle evaluator trigger rules"
  for rule in "${K_CYCLE_RULES[@]}"; do
    echo "  Restoring m=6 on ${rule}"
    curl -XPOST "${MOZART_ES_URL}/user_rules-grq/_update_by_query?refresh=true" \
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

curl --insecure \
  "https://${MOZART_PVT_IP}/mozart/api/v0.1/job/submit?enable_dedup=false" \
  --form 'queue="opera-job_worker-cslc_data_download"' \
  --form 'priority="0"' \
  --form 'tags="[\"e2e-test\",\"forward-processing\"]"' \
  --form "type=\"job-cslc_catalog_ingest:${JOB_RELEASE}\"" \
  --form 'params="{\"frame_ids\":\"31241\",\"start_date\":\"2017-10-23T00:00:00Z\",\"end_date\":\"2019-06-01T00:00:00Z\"}"' \
  --form 'name="e2e-cslc_catalog_ingest-fwd-f31241"'

# verify forward datasets
# (~6 hours for forward pipeline to complete including CCSLC rollover)
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json fwd --max_time 21600 /tmp/datasets_fwd.txt
