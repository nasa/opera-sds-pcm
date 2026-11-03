#!/bin/bash

set -e

function_arn=$(python -c 'from util.conf_util import SettingsConf; cfg=SettingsConf().cfg; print(cfg["OPS_REPORTING_LAMBDA"] or "")')

# TEST
python -c 'from util.conf_util import SettingsConf; cfg=SettingsConf().cfg; import json; print(json.dumps(cfg, indent=2))'

if [[ -z "${function_arn}" ]]; then
  echo "No reporting function defined in the settings. Set and ship OPS_REPORTING_LAMBDA or disable the reporting lambda trigger rule."
  exit 1
fi

echo "Determined lambda function ${function_arn}"

if [ ! -f "_context.json" ]; then
  echo "Cannot find context file"
  exit 1
fi

jq '{dataset_urls: .dataset_urls, product_metadata: .product_metadata.metadata}' < "_context.json" > lambda_event.json

echo "Created lambda event JSON:"
jq < lambda_event.json

aws lambda invoke --region us-west-2 --function-name "${function_arn}" --payload "file://lambda_event.json" --log-type Tail result.json

jq < result.json
