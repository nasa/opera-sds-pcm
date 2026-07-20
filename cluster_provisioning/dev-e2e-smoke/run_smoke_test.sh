#!/bin/bash
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-smoke"

# check args
if [ "$#" -eq 1 ]; then
  config_file=${1}
else
  echo "Invalid number of arguments ($#) $*" 1>&2
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

# ============================================================
# Phase 1: DSWx-HLS Processing
# ============================================================

# Scale up DSWx-HLS workers
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py \
  ${project}-${venue}-${counter}-opera-job_worker-sciflo-l3_dswx_hls --desired-capacity 2

# --- L30 subscriber (Landsat) ---
L30_LAMBDA="${project}-${venue}-${counter}-opera-pcm-l30-data-subscriber-query-timer"

# Set SMOKE_RUN mode on the Lambda
aws lambda update-function-configuration --function-name "${L30_LAMBDA}" \
  --environment "Variables={SMOKE_RUN=true,DRY_RUN=false,NO_SCHEDULE_DOWNLOAD=false,USE_TEMPORAL=true,TEMPORAL_START_DATETIME_MARGIN_DAYS=,MINUTES=rate(60 minutes)}"

# Invoke L30 subscriber with known test time
aws lambda invoke --function-name "${L30_LAMBDA}" \
  --payload '{"id":"cid/smoke-test-l30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/l30_invoke_result.json

# Reset Lambda
aws lambda update-function-configuration --function-name "${L30_LAMBDA}" \
  --environment "Variables={SMOKE_RUN=false,DRY_RUN=false,NO_SCHEDULE_DOWNLOAD=false,USE_TEMPORAL=false,TEMPORAL_START_DATETIME_MARGIN_DAYS=3,MINUTES=rate(60 minutes)}"

# --- S30 subscriber (Sentinel-2) ---
S30_LAMBDA="${project}-${venue}-${counter}-opera-pcm-s30-data-subscriber-query-timer"

aws lambda update-function-configuration --function-name "${S30_LAMBDA}" \
  --environment "Variables={SMOKE_RUN=true,DRY_RUN=false,NO_SCHEDULE_DOWNLOAD=false,USE_TEMPORAL=true,TEMPORAL_START_DATETIME_MARGIN_DAYS=,MINUTES=rate(60 minutes)}"

aws lambda invoke --function-name "${S30_LAMBDA}" \
  --payload '{"id":"cid/smoke-test-s30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/s30_invoke_result.json

aws lambda update-function-configuration --function-name "${S30_LAMBDA}" \
  --environment "Variables={SMOKE_RUN=false,DRY_RUN=false,NO_SCHEDULE_DOWNLOAD=false,USE_TEMPORAL=false,TEMPORAL_START_DATETIME_MARGIN_DAYS=3,MINUTES=rate(60 minutes)}"

# ============================================================
# Verify DSWx-HLS product outputs
# ============================================================
# check_datasets_file.py polls GRQ ES with exponential backoff until
# expected counts are met or max_time is exceeded.
# --max_time 3600 = 1 hour timeout (query + download + PGE execution)

~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py \
  --crid=${crid} \
  ${TEST_DIR}/datasets_e2e.json \
  dswx_hls \
  --max_time 3600 \
  /tmp/datasets_smoke.txt

# ============================================================
# Phase 2: CNM Verification
# ============================================================
# After products are confirmed, verify CNM-S was sent and mock CNM-R

python ${TEST_DIR}/verify_cnm.py \
  --es-host 127.0.0.1 \
  --cnm-r-topic-arn "${CNM_R_TOPIC_ARN}" \
  --products "OPERA_L3_DSWx-HLS_T54PVQ_20220101T005855Z_,OPERA_L3_DSWx-HLS_T53HQV_20220101T003711Z_" \
  --index "grq_v2.0_l3_dswx_hls-*" \
  --result-file /tmp/datasets_cnm.txt