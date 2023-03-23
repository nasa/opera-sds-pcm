#!/bin/bash
source $HOME/.bash_profile

# check args
if [ "$#" -eq 19 ]; then
  project=${1}
  environment=${2}
  venue=${3}
  counter=${4}
  use_artifactory=${5}
  artifactory_base_url=${6}
  artifactory_repo=${7}
  artifactory_mirror_url=${8}
  pcm_repo=${9}
  pcm_branch=${10}
  product_delivery_repo=${11}
  product_delivery_branch=${12}
  mozart_private_ip=${13}
  isl_bucket=${14}
  source_event_arn=${15}
  daac_delivery_proxy=${16}
  use_daac_cnm=${17}
  crid=${18}
  cluster_type=${19}
#  data_query_timer_trigger_frequency=${20}
#  data_download_timer_trigger_frequency=${21}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

# ingest Sacramento AOI to test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json --force

# submit test hello world job to CPU queue/ASG
python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_hello_world_job.py ${pcm_branch}

# submit test hello world job to GPU queue/ASG to exercise GPU-capability
#python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_hello_world_job-gpu.py ${pcm_branch}

# Get yesterday and tomorrow's date to feed into the ISL report to account for crossing the day boundary
# when running this smoke test
#yesterday="$(date --date="yesterday" "+%Y-%m-%d")"
#tomorrow="$(date --date="next day" "+%Y-%m-%d")"

#start_date_time="2022-01-01T00:00:00"
#end_date_time="2025-01-01T00:00:00"

#data_start="${yesterday}T00:00:00"
#data_end="${tomorrow}T00:00:00"

#opera_bach_ui_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach-ui/data-summary/incoming)
#opera_bach_api_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach-api/ancillary/list)

#if [[ "$opera_bach_ui_status_code" -ne 200 ]] ; then
#  echo "FAILURE: Could not reach bach-ui" > /tmp/opera_bach_ui_status_code.txt
#else
#  echo "SUCCESS" > /tmp/opera_bach_ui_status_code.txt
#fi
#if [[ "$opera_bach_api_status_code" -ne 200 ]] ; then
#  echo "FAILURE: Could not reach bach-api" > /tmp/opera_bach_api_status_code.txt
#else
#  echo "SUCCESS" > /tmp/opera_bach_api_status_code.txt
#fi

