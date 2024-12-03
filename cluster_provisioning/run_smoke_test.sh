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

cnm_datasets=L3_DSWx_HLS
cd ~/.sds/files

# for GPU instances, require on-demand since requesting a spot instance take a while (high usage)
# TODO chrisjrd: uncomment
#for i in workflow_profiler job_worker-gpu; do
#  ~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-${i} --cli-input-json test/modify_on-demand_base.json
#done

# to simulate always-on OPERA reserved instances, prime a subset of all the ASGs by
# setting the min size and desired capacity to the same value
# TODO chrisjrd: uncomment
#~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-gpu --desired-capacity 1
#~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-small --desired-capacity 5
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-send_cnm_notify --desired-capacity 5
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-rcv_cnm_notify --desired-capacity 5

# ingest Sacramento AOI to test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json --force

# import trigger rules for mozart and grq
#cd ~/.sds/files/test
#curl -XDELETE http://${mozart_private_ip}:9200/user_rules-grq
#curl -XDELETE http://${mozart_private_ip}:9200/user_rules-mozart
#fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index
#./import_rules.sh
#./import_rules-mozart.sh
#./import_product_delivery_rules.sh

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

