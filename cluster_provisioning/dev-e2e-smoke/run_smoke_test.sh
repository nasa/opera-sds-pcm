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

# Helper: update specific env vars on a Lambda without wiping the rest.
# Reads current env, merges overrides, writes back.
# Usage: lambda_env_update <function-name> KEY1=val1 KEY2=val2 ...
lambda_env_update() {
  local fn="$1"; shift
  local current
  current=$(aws lambda get-function-configuration --function-name "${fn}" \
    --query "Environment.Variables" --output json)
  local merged
  merged=$(python3 -c "
import sys, json
d = json.loads(sys.argv[1])
for kv in sys.argv[2:]:
    k, v = kv.split('=', 1)
    d[k] = v
print(json.dumps(d))
" "${current}" "$@")
  aws lambda update-function-configuration --function-name "${fn}" \
    --environment "{\"Variables\": ${merged}}" > /dev/null
  aws lambda wait function-updated --function-name "${fn}"
}

# --- L30 subscriber (Landsat) ---
L30_LAMBDA="${project}-${venue}-${counter}-hlsl30-query-timer"

# Set SMOKE_RUN mode (preserves MOZART_URL, JOB_QUEUE, etc.)
lambda_env_update "${L30_LAMBDA}" \
  SMOKE_RUN=true USE_TEMPORAL=true TEMPORAL_START_DATETIME_MARGIN_DAYS=

# Invoke L30 subscriber with known test time
aws lambda invoke --function-name "${L30_LAMBDA}" \
  --payload '{"id":"cid/smoke-test-l30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/l30_invoke_result.json

# Reset Lambda
lambda_env_update "${L30_LAMBDA}" \
  SMOKE_RUN=false USE_TEMPORAL=false TEMPORAL_START_DATETIME_MARGIN_DAYS=30

# --- S30 subscriber (Sentinel-2) ---
S30_LAMBDA="${project}-${venue}-${counter}-hlss30-query-timer"

lambda_env_update "${S30_LAMBDA}" \
  SMOKE_RUN=true USE_TEMPORAL=true TEMPORAL_START_DATETIME_MARGIN_DAYS=

aws lambda invoke --function-name "${S30_LAMBDA}" \
  --payload '{"id":"cid/smoke-test-s30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/s30_invoke_result.json

lambda_env_update "${S30_LAMBDA}" \
  SMOKE_RUN=false USE_TEMPORAL=false TEMPORAL_START_DATETIME_MARGIN_DAYS=30

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
  --cnm-r-topic-arn "${cnm_r_topic_arn}" \
  --products "OPERA_L3_DSWx-HLS_T54PVQ_20220101T005855Z_,OPERA_L3_DSWx-HLS_T53HQV_20220101T003711Z_" \
  --index "grq_v1.1_l3_dswx_hls-*" \
  --result-file /tmp/datasets_cnm.txt