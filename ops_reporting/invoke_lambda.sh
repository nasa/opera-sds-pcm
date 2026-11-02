#!/bin/bash

set -e

function_arn=$(python -c 'from util.conf_util import SettingsConf; cfg=SettingsConf().cfg; print(cfg["OPS_REPORTING_LAMBDA"] or "")')

if [[ -z "${function_arn}" ]]; then
  echo "No reporting function defined in the settings. Set and ship OPS_REPORTING_LAMBDA or disable the reporting lambda trigger rule."
  exit 1
fi

if [ ! -f "_context.json" ]; then
  echo "Cannot find context file"
  exit 1
fi

aws lambda invoke --function-name "${function_arn}" --payload "file://_context.json" --log-type Tail result.json

jq < result.json
