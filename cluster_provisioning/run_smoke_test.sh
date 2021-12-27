#!/bin/bash
source $HOME/.bash_profile

# check args
if [ "$#" -eq 28 ]; then
  project=$1
  environment=$2
  venue=$3
  counter=$4
  use_artifactory=$5
  artifactory_base_url=$6
  artifactory_repo=$7
  artifactory_mirror_url=$8
  pcm_repo=$9
  pcm_branch=${10}
  product_delivery_repo=${11}
  product_delivery_branch=${12}
  delete_old_cop_catalog=${13}
  delete_old_tiurdrop_catalog=${14}
  delete_old_rost_catalog=${15}
  delete_old_pass_catalog=${16}
  delete_old_observation_catalog=${17}
  delete_old_track_frame_catalog=${18}
  delete_old_radar_mode_catalog=${19}
  mozart_private_ip=${20}
  isl_bucket=${21}
  source_event_arn=${22}
  daac_delivery_proxy=${23}
  use_daac_cnm=${24}
  crid=${25}
  cluster_type=${26}
  l0a_timer_trigger_frequency=${27}
  obs_acct_report_timer_trigger_frequency=${28}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

cnm_datasets=L0A_L_RRST,L0B_L_RRSD,COP,SCLKSCET,STUF

# If we're deploying a forward cluster, push out a modified version of the settings.yaml
# in order to test the timers. Additionally, we should temporarily shorten the timers to something small for smoke test purposes
if [ "${cluster_type}" = "forward" ]; then
  aws events put-rule --name ${project}-${venue}-${counter}-l0a-timer-Trigger --schedule-expression "rate(5 minutes)"
  aws events put-rule --name ${project}-${venue}-${counter}-l0b-timer-Trigger --schedule-expression "rate(5 minutes)"
  aws events put-rule --name ${project}-${venue}-${counter}-l0b-urgent_response-timer-Trigger --schedule-expression "rate(5 minutes)"
  aws events put-rule --name ${project}-${venue}-${counter}-rslc-timer-Trigger --schedule-expression "rate(5 minutes)"
  aws events put-rule --name ${project}-${venue}-${counter}-network_pair-timer-Trigger --schedule-expression "rate(5 minutes)"
  
  echo "Making a copy of the original settings.yaml and pushing out a modified version out to the cluster"
  cp ~/mozart/ops/opera-pcm/conf/settings.yaml ~/mozart/ops/opera-pcm/conf/settings.yaml.bak
  sed -i 's/    RRST_EVALUATOR: .*/    RRST_EVALUATOR: 3/g' ~/mozart/ops/opera-pcm/conf/settings.yaml
  sed -i 's/    DATATAKE_EVALUATOR: .*/    DATATAKE_EVALUATOR: 10/g' ~/mozart/ops/opera-pcm/conf/settings.yaml
  sed -i 's/    TRACK_FRAME_EVALUATOR: .*/    TRACK_FRAME_EVALUATOR: 10/g' ~/mozart/ops/opera-pcm/conf/settings.yaml
  sed -i 's/    NETWORK_PAIR_EVALUATOR: .*/    NETWORK_PAIR_EVALUATOR: 5/g' ~/mozart/ops/opera-pcm/conf/settings.yaml

  fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
  sds ship
fi

# build/import CNM product delivery
if [ "${use_artifactory}" = true ]; then
  ~/download_artifact.sh -m ${artifactory_mirror_url} -b ${artifactory_base_url} "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/nisar/sds/pcm/hysds_pkgs/container-iems-sds_cnm_product_delivery-${product_delivery_branch}.sdspkg.tar"
  sds pkg import container-iems-sds_cnm_product_delivery-${product_delivery_branch}.sdspkg.tar
  rm -rf container-iems-sds_cnm_product_delivery-${product_delivery_branch}.sdspkg.tar
else
  sds ci add_job -b ${product_delivery_branch} --token https://${product_delivery_repo} s3
  sds ci build_job -b ${product_delivery_branch} https://${product_delivery_repo}
  sds ci remove_job -b ${product_delivery_branch} https://${product_delivery_repo}
fi

cd ~/.sds/files

# for GPU instances, require on-demand since requesting a spot instance take a while (high usage)
for i in workflow_profiler job_worker-gpu; do
  ~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-${i} --cli-input-json test/modify_on-demand_base.json
done

# to simulate always-on OPERA reserved instances, prime a subset of all the ASGs by
# setting the min size and desired capacity to the same value 
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-gpu --desired-capacity 1
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-small --desired-capacity 7
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-sciflo-l0a --desired-capacity 7
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-send_cnm_notify --desired-capacity 7
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-rcv_cnm_notify --desired-capacity 7
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-timer --desired-capacity 1

# no jobs currently being submitted to these ASGs but left here commented out for future use
#aws autoscaling update-auto-scaling-group --auto-scaling-group-name ${project}-${venue}-${counter}-opera-job_worker-large --desired-capacity 7
#aws autoscaling update-auto-scaling-group --auto-scaling-group-name ${project}-${venue}-${counter}-opera-workflow_profiler --desired-capacity 1

# build/import opera-pcm
lowercase_pcm_branch=`echo "${pcm_branch}" | awk '{ print tolower($0); }'`

if [ "${use_artifactory}" = true ]; then
  ~/download_artifact.sh -m ${artifactory_mirror_url} -b ${artifactory_base_url} "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/opera/sds/pcm/hysds_pkgs/container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar"
  sds pkg import container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar
  rm -rf container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar
  # Loads the opera-pcm container to the docker registry
  fab -f ~/.sds/cluster.py -R mozart load_container_in_registry:"container-iems-sds_opera-pcm:${lowercase_pcm_branch}"
else
  sds -d ci add_job -b ${pcm_branch} --token https://${pcm_repo} s3
  sds -d ci build_job -b ${pcm_branch} https://${pcm_repo}
  sds -d ci remove_job -b ${pcm_branch} https://${pcm_repo}
fi

# create COP catalog
if [ "${delete_old_cop_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/cop/create_cop_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/cop/create_cop_catalog.py
fi

# create TIURDROP COP catalog
if [ "${delete_old_tiurdrop_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/cop/create_cop_catalog.py --tiurdrop --delete_old_tiurdrop_catalog
else
  python ~/mozart/ops/opera-pcm/cop/create_cop_catalog.py --tiurdrop
fi

# create ROST catalog
if [ "${delete_old_rost_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/rost/create_rost_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/rost/create_rost_catalog.py
fi

if [ "${delete_old_pass_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/pass_accountability/create_pass_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/pass_accountability/create_pass_accountability_catalog.py
fi

if [ "${delete_old_observation_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/observation_accountability/create_observation_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/observation_accountability/create_observation_accountability_catalog.py
fi

if [ "${delete_old_track_frame_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/Track_Frame_Accountability/create_track_frame_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/Track_Frame_Accountability/create_track_frame_accountability_catalog.py
fi

if [ "${delete_old_radar_mode_catalog}" = true ]; then
  python ~/mozart/ops/opera-pcm/radar_mode/create_radar_mode_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/opera-pcm/radar_mode/create_radar_mode_catalog.py
fi


# ingest Sacramento AOI to test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json

# submit test hello world job to CPU queue/ASG
python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_hello_world_job.py ${pcm_branch}

# submit test hello world job to GPU queue/ASG to exercise GPU-capability
python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_hello_world_job-gpu.py ${pcm_branch}

# import trigger rules for mozart and grq
cd ~/.sds/files/test
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-grq
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-mozart
fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index
./import_rules.sh
./import_rules-mozart.sh
./import_product_delivery_rules.sh

# stage radar config files
./stage_radar_config_files_to_s3.sh ${isl_bucket}
# stage SROST, OFS and SCLKSCET files
./stage_pre-req_ancillary_files_to_s3.sh ${isl_bucket}
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} datasets_e2e.json 1 /tmp/datasets.txt

# verify Radar Mode catalog
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_catalog.py radar_mode_catalog 204 /tmp/radar_mode_catalog.txt

# stage ancillary/auxiliary files to ISL for ingest and to trigger COP and ROST cataloging
./stage_ancillary_files_to_s3.sh ${isl_bucket}

# verify number of ingested ancillary/auxiliary products
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} datasets_e2e.json 1,2 /tmp/datasets.txt

# verify COP, TIURDROP and ROST catalog
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_catalog.py cop_catalog 238 /tmp/cop_catalog.txt
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_catalog.py tiurdrop_catalog 5 /tmp/tiurdrop_catalog.txt
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_catalog.py rost_catalog 235 /tmp/rost_catalog.txt

# stage NEN_L_RRST files to ISL for ingest and to trigger the downstream PGE pipelines
./stage_nen_to_s3.sh ${isl_bucket}

# stage met_required files to ISL
./stage_met_required_files_to_s3.sh ${isl_bucket}

# verify number of ingested ancillary/auxiliary, NEN, and LDF
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3 /tmp/datasets.txt

# verify number of ingested ancillary/auxiliary, NEN, LDF, L0-L2 products and met_required
if [ "${cluster_type}" = "forward" ]; then
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_FWD /tmp/datasets.txt
else
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_REPROC /tmp/datasets.txt
fi

if [ "${cluster_type}" = "forward" ]; then
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_FWD,5,L0B_FWD,L1_L2_FWD /tmp/datasets.txt
else
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_REPROC,5,L0B_REPROC,L1_L2_REPROC /tmp/datasets.txt
fi

# ingest neighbor RSLC for network_pair
./stage_network-pair_input.sh ${isl_bucket} ${crid}

if [ "${cluster_type}" = "forward" ]; then
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_FWD,5,L0B_FWD,NETWORK_PAIR_FWD /tmp/datasets.txt
else
  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${crid} datasets_e2e.json 1,2,3,4,L0A_REPROC,5,L0B_REPROC,NETWORK_PAIR_REPROC /tmp/datasets.txt
fi


# verify accountability table counts
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_accountability.py --max_time=2700 pass_accountability_catalog 12 /tmp/pass_accountability_catalog.txt


# simulate reception of CNM-R messages from the DAAC and submit jobs for stamping their response on the dataset
if [ "${use_daac_cnm}" = false ]; then
  python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_cnm_r_msg.py --datasets ${cnm_datasets} ${source_event_arn} /tmp/cnm_r_stamped_dataset.json
else
  python ~/mozart/ops/opera-pcm/conf/sds/files/test/submit_cnm_r_msg.py --datasets ${cnm_datasets} --no_simulation  ${source_event_arn} /tmp/cnm_r_stamped_dataset.json
fi

# check that the datasets got stamped
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_stamped_dataset.py /tmp/cnm_r_stamped_dataset.json daac_delivery_status /tmp/check_stamped_dataset_result.txt

# check that the ISL was cleaned out by the trigger rule
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_empty_isl.py ${isl_bucket} /tmp/check_empty_isl_result.txt

# Get yesterday and tomorrow's date to feed into the ISL report to account for crossing the day boundary
# when running this smoke test
yesterday="$(date --date="yesterday" "+%Y-%m-%d")"
tomorrow="$(date --date="next day" "+%Y-%m-%d")"

# generate latency report of files dropped in the ISL
python ~/mozart/ops/opera-pcm/report/get_isl_report.py --start_date ${yesterday} --end_date ${tomorrow} --output_file /tmp/isl_report.csv --verbose_level WARNING

# print out latency report of files dropped in the ISL
~/mozart/ops/opera-pcm/conf/sds/files/test/dump_isl_report.py /tmp/isl_report.csv

start_date_time="2022-01-01T00:00:00"
end_date_time="2025-01-01T00:00:00"

data_start="${yesterday}T00:00:00"
data_end="${tomorrow}T00:00:00"

python ~/mozart/ops/opera-pcm/report/accountability_report_cli.py ObservationAccountabilityReport --start ${start_date_time} --end ${end_date_time} --format_type=xml
cat oad_*.xml
python ~/mozart/ops/opera-pcm/report/accountability_report_cli.py DataAccountabilityReport --start ${data_start} --end ${data_end} --format_type=xml 
cat dar_*.xml

bach_ui_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach_ui/1.0/dataSummary/incoming)
bach_api_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach-api/1.0/ancillary/list)
#opera_bach_ui_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach_ui/2.0/data-summary/incoming)
#opera_bach_api_status_code=$(curl -k --write-out %{http_code} --silent --output /dev/null https://${mozart_private_ip}/bach-api/2.0/ancillary/list)

if [[ "$bach_ui_status_code" -ne 200 ]] ; then
  echo "FAILURE: Could not reach bach_ui v1.0" > /tmp/bach_ui_status_code.txt
else
  echo "SUCCESS" > /tmp/bach_ui_status_code.txt
fi
if [[ "$bach_api_status_code" -ne 200 ]] ; then
  echo "FAILURE: Could not reach bach-api v1.0" > /tmp/bach_api_status_code.txt
else
  echo "SUCCESS" > /tmp/bach_api_status_code.txt
fi
#if [[ "$opera_bach_ui_status_code" -ne 200 ]] ; then
#  echo "FAILURE: Could not reach bach_ui v2.0" > /tmp/opera_bach_ui_status_code.txt
#else
#  echo "SUCCESS" > /tmp/opera_bach_ui_status_code.txt
#fi
#if [[ "$opera_bach_api_status_code" -ne 200 ]] ; then
#  echo "FAILURE: Could not reach bach-api v2.0" > /tmp/opera_bach_api_status_code.txt
#else
#  echo "SUCCESS" > /tmp/opera_bach_api_status_code.txt
#fi

# Test auto generation of the Observation Accountability Report
#if [ "${cluster_type}" = "forward" ]; then
#  python ~/mozart/ops/opera-pcm/conf/sds/files/test/update_lambda.py ${project}-${venue}-${counter}-obs-acct-report-timer "{\"USER_START_TIME\": \"${start_date_time}Z\", \"USER_END_TIME\": \"${end_date_time}Z\"}"
#  aws events put-rule --name ${project}-${venue}-${counter}-obs-acct-report-timer-Trigger --schedule-expression "rate(30 minutes)"
#  ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --max_time=600 --crid=${crid} datasets_e2e.json OBS_REPORT /tmp/report_datasets.txt
#else
#  echo "SUCCESS: No daily observation accountability report expected to be found." > /tmp/report_datasets.txt
#fi

# If we're deploying a forward cluster, restore the original settings.yaml to the cluster
if [ "${cluster_type}" = "forward" ]; then
  aws events put-rule --name ${project}-${venue}-${counter}-l0a-timer-Trigger --schedule-expression "${l0a_timer_trigger_frequency}"
  python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_forced_state_configs.py datasets_e2e_force_submits.json LDF,datatake,trackframe,network_pair /tmp/check_expected_force_submits.txt
  echo "Restoring original settings.yaml and pushing it out to the cluster"
  cp ~/mozart/ops/opera-pcm/conf/settings.yaml.bak ~/mozart/ops/opera-pcm/conf/settings.yaml
  fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
  sds ship
else
  echo "SUCCESS: No force submit state configs expected to be found." > /tmp/check_expected_force_submits.txt
fi

# If we're deploying a forward cluster, restore the original settings that will create the daily observational
# accountability reports
#if [ "${cluster_type}" = "forward" ]; then
#  echo "Restoring original settings to generate daily Observation Accountability Reports"
#  python ~/mozart/ops/opera-pcm/conf/sds/files/test/update_lambda.py ${project}-${venue}-${counter}-obs-acct-report-timer "{\"USER_START_TIME\": \"\", \"USER_END_TIME\": \"\"}"
#  aws events put-rule --name ${project}-${venue}-${counter}-obs-acct-report-timer-Trigger --schedule-expression "${obs_acct_report_timer_trigger_frequency}"
#fi

# Restore OnDemandPercentageAboveBaseCapacity back to 0 for GPU instances
cd ~/.sds/files
for i in workflow_profiler job_worker-gpu; do
  ~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-${i} --cli-input-json test/restore_on-demand_base.json
done
