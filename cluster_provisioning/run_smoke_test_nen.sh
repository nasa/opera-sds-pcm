#!/usr/bin/env bash
source $HOME/.bash_profile

# check args
if [ "$#" -eq 31 ]; then
  project=$1
  environment=$2
  venue=$3
  counter=$4
  use_artifactory=$5
  artifactory_base_url=$6
  artifactory_repo=$7
  artifactory_mirror_url=$8
  nisar_pcm_repo=$9
  nisar_pcm_branch=${10}
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
  l0b_timer_trigger_frequency=${28}
  rslc_timer_trigger_frequency=${29}
  l0b_urgent_response_timer_trigger_frequency=${30}
  network_pair_timer_trigger_frequency=${31}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

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

# prime a subset of the CNM, ingest and L0A ASGs to simulate always-on NISAR reserved instances
~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-nisar-job_worker-send_cnm_notify --desired-capacity 10
~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-nisar-job_worker-rcv_cnm_notify --desired-capacity 10
~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-nisar-job_worker-small --desired-capacity 10

# build/import nisar-pcm
lowercase_nisar_pcm_branch=`echo "${nisar_pcm_branch}" | awk '{ print tolower($0); }'`

# build/import nisar-pcm
if [ "${use_artifactory}" = true ]; then
  ~/download_artifact.sh -m ${artifactory_mirror_url} -b ${artifactory_base_url} "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/nisar/sds/pcm/hysds_pkgs/container-iems-sds_nisar-pcm-${nisar_pcm_branch}.sdspkg.tar"
  sds pkg import container-iems-sds_nisar-pcm-${nisar_pcm_branch}.sdspkg.tar
  rm -rf container-iems-sds_nisar-pcm-${nisar_pcm_branch}.sdspkg.tar
  # Loads the nisar-pcm container to the docker registry
  fab -f ~/.sds/cluster.py -R mozart load_container_in_registry:"container-iems-sds_nisar-pcm:${lowercase_nisar_pcm_branch}"
else
  sds -d ci add_job -b ${nisar_pcm_branch} --token https://${nisar_pcm_repo} s3
  sds -d ci build_job -b ${nisar_pcm_branch} https://${nisar_pcm_repo}
  sds -d ci remove_job -b ${nisar_pcm_branch} https://${nisar_pcm_repo}
fi

# create COP catalog
if [ "${delete_old_cop_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/cop/create_cop_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/cop/create_cop_catalog.py
fi

# create TIURDROP COP catalog
if [ "${delete_old_tiurdrop_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/cop/create_cop_catalog.py --tiurdrop --delete_old_tiurdrop_catalog
else
  python ~/mozart/ops/nisar-pcm/cop/create_cop_catalog.py --tiurdrop
fi

# create ROST catalog
if [ "${delete_old_rost_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/rost/create_rost_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/rost/create_rost_catalog.py
fi

if [ "${delete_old_pass_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/pass_accountability/create_pass_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/pass_accountability/create_pass_accountability_catalog.py
fi

if [ "${delete_old_observation_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/observation_accountability/create_observation_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/observation_accountability/create_observation_accountability_catalog.py
fi

if [ "${delete_old_track_frame_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/Track_Frame_Accountability/create_track_frame_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/Track_Frame_Accountability/create_track_frame_accountability_catalog.py
fi

# create Radar Mode catalog
if [ "${delete_old_radar_mode_catalog}" = true ]; then
  python ~/mozart/ops/nisar-pcm/radar_mode/create_radar_mode_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/nisar-pcm/radar_mode/create_radar_mode_catalog.py
fi

# ingest Sacramento AOI to test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json

# submit test hello world job to CPU queue/ASG
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/submit_hello_world_job.py ${nisar_pcm_branch}

# submit test hello world job to GPU queue/ASG to exercise GPU-capability
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/submit_hello_world_job-gpu.py ${nisar_pcm_branch}


# import trigger rules for mozart and grq
cd ~/.sds/files/test
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-grq
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-mozart
fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index
./import_rules_nen.sh
./import_rules-mozart.sh
./import_product_delivery_rules.sh

# stage radar config files
./stage_radar_config_files_to_s3.sh ${isl_bucket}
~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} datasets_e2e_nen.json 1 /tmp/datasets.txt

# verify Radar Mode catalog
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py radar_mode_catalog 204 /tmp/radar_mode_catalog.txt

# stage ancillary/auxiliary files to ISL for ingest and to trigger COP and ROST cataloging
./stage_ancillary_files_to_s3.sh ${isl_bucket}

# verify number of ingested ancillary/auxiliary products
~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets.py v1.0,2.0,${crid},0,1,2,3 42 /tmp/datasets.txt

# verify COP and ROST catalog
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py cop_catalog 238 /tmp/cop_catalog.txt
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py tiurdrop_catalog 5 /tmp/tiurdrop_catalog.txt
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py rost_catalog 235 /tmp/rost_catalog.txt

# stage NITT-02 test NEN data files
cd ~/mozart/ops/nisar-pcm
cd cluster_provisioning/dev-e2e-nen
./stage_files_to_s3.sh ${isl_bucket}

# verify number of ingested ancillary/auxiliary/L0-L2 products
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --max_time=21600 --max_value=1500 --crid=${crid} datasets_e2e_nen.json 1,2 /tmp/datasets.txt

# check that the ISL was cleaned out by the trigger rule
python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_empty_isl.py ${isl_bucket} /tmp/check_empty_isl_result.txt
 
# generate latency report of files dropped in the ISL
python ~/mozart/ops/nisar-pcm/report/get_isl_report.py --output_file /tmp/isl_report.csv --verbose_level WARNING
 
# print out latency report of files dropped in the ISL
~/mozart/ops/nisar-pcm/conf/sds/files/test/dump_isl_report.py /tmp/isl_report.csv
