#!/bin/bash
# Self-contained DSWx-HLS smoke test.
# Scales PGE workers, triggers HLS subscriber Lambdas, waits for products,
# verifies the CNM-S/R round-trip, and runs assertions.
#
# Contract:
#   $1 = config file (sourced for project/venue/counter/crid/cnm_r_topic_arn)
#   Writes: /tmp/datasets_dswx_hls.txt, /tmp/cnm_dswx_hls.txt
#   JUnit:  /tmp/check_pcm_dswx_hls.xml
#
# Can be run standalone or dispatched by dev-e2e-smoke/run_smoke_test.sh.

source $HOME/.bash_profile

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <config_file>" 1>&2
  exit 1
fi

source ${1}

set -ex

TEST_DIR="$(cd "$(dirname "$0")" && pwd)"
SMOKE_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-smoke"

# ============================================================
# Scale up DSWx-HLS PGE workers
# ============================================================

~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py \
  ${project}-${venue}-${counter}-opera-job_worker-sciflo-l3_dswx_hls --desired-capacity 2

# ============================================================
# Trigger HLS subscriber Lambdas
# ============================================================

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

# Helper: invoke a Lambda with guaranteed env restoration.
# Usage: safe_lambda_invoke <function-name> <payload-json> <output-file> \
#          RESTORE_KEY1=val1 RESTORE_KEY2=val2 ...
safe_lambda_invoke() {
  local fn="$1"; shift
  local payload="$1"; shift
  local output="$1"; shift
  # remaining args are the restore key=value pairs

  if ! aws lambda invoke --function-name "${fn}" \
       --payload "${payload}" "${output}"; then
    echo "ERROR: Lambda invocation failed for ${fn} — restoring env"
    lambda_env_update "${fn}" "$@"
    return 1
  fi
  lambda_env_update "${fn}" "$@"
}

# --- L30 subscriber (Landsat) ---
L30_LAMBDA="${project}-${venue}-${counter}-hlsl30-query-timer"

lambda_env_update "${L30_LAMBDA}" \
  SMOKE_RUN=true USE_TEMPORAL=true TEMPORAL_START_DATETIME_MARGIN_DAYS=

safe_lambda_invoke "${L30_LAMBDA}" \
  '{"id":"cid/smoke-test-l30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/l30_invoke_result.json \
  SMOKE_RUN=false USE_TEMPORAL=false TEMPORAL_START_DATETIME_MARGIN_DAYS=30

# --- S30 subscriber (Sentinel-2) ---
S30_LAMBDA="${project}-${venue}-${counter}-hlss30-query-timer"

lambda_env_update "${S30_LAMBDA}" \
  SMOKE_RUN=true USE_TEMPORAL=true TEMPORAL_START_DATETIME_MARGIN_DAYS=

safe_lambda_invoke "${S30_LAMBDA}" \
  '{"id":"cid/smoke-test-s30","detail-type":"Scheduled Event","source":"aws.events","time":"2022-01-01T01:00:00Z","region":"us-west-2","resources":["arn:aws:events:us-west-2:000000000000:rule/smoke"],"detail":{}}' \
  /tmp/s30_invoke_result.json \
  SMOKE_RUN=false USE_TEMPORAL=false TEMPORAL_START_DATETIME_MARGIN_DAYS=30

# ============================================================
# Verification phase
# ============================================================
# Disable errexit so that failures in one step don't prevent
# subsequent steps from running. pytest must always execute to
# generate JUnit XML for Jenkins.
set +e

# check_datasets_file.py polls GRQ ES with exponential backoff until
# expected counts are met or max_time is exceeded.
# --max_time 3600 = 1 hour timeout (query + download + PGE execution)

~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py \
  --crid=${crid} \
  ${TEST_DIR}/datasets_e2e.json \
  dswx_hls \
  --max_time 3600 \
  /tmp/datasets_dswx_hls.txt

# CNM: verify CNM-S was sent and mock/verify CNM-R

python3 ${SMOKE_DIR}/verify_cnm.py \
  --es-host 127.0.0.1 \
  --cnm-r-topic-arn "${cnm_r_topic_arn}" \
  --products "OPERA_L3_DSWx-HLS_T54PVQ_20220101T005855Z_,OPERA_L3_DSWx-HLS_T53HQV_20220101T003711Z_" \
  --index "grq_v1.1_l3_dswx_hls-*" \
  --collection "OPERA_L3_DSWx-HLS" \
  --result-file /tmp/cnm_dswx_hls.txt

# ============================================================
# Assertions
# ============================================================
# pytest exit code becomes the script's exit code so the
# orchestrator can detect pass/fail.

pytest ${TEST_DIR}/check_pcm.py
