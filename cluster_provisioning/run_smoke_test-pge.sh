#!/bin/bash
source $HOME/.bash_profile

# check args
if [ "$#" -eq 20 ]; then
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
  pge_test_package=${20}
#  delete_old_job_catalog=${13}
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

# for GPU instances, require on-demand since requesting a spot instance take a while (high usage)
#for i in workflow_profiler job_worker-sciflo-rslc job_worker-sciflo-insar; do
#  ~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-nisar-${i} --cli-input-json test/modify_on-demand_base.json
#done

# build/import opera-pcm
lowercase_pcm_branch=`echo "${pcm_branch}" | awk '{ print tolower($0); }'`

if [ "${use_artifactory}" = true ]; then
  ~/download_artifact.sh -m ${artifactory_mirror_url} -b ${artifactory_base_url} "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/${var.project}/sds/pcm/hysds_pkgs/container-nasa_${var.project}-sds-pcm-${pcm_branch}.sdspkg.tar"
#  ~/download_artifact.sh -m ${artifactory_mirror_url} -b ${artifactory_base_url} "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/opera/sds/pcm/hysds_pkgs/container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar"
  sds pkg import container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar
  rm -rf container-iems-sds_opera-pcm-${pcm_branch}.sdspkg.tar
  # Loads the opera-pcm container to the docker registry
  fab -f ~/.sds/cluster.py -R mozart load_container_in_registry:"container-nasa_${var.project}-sds-pcm:${lowercase_pcm_branch}"
else
  sds -d ci add_job -b ${pcm_branch} --token https://${pcm_repo} s3
  sds -d ci build_job -b ${pcm_branch} https://${pcm_repo}
  sds -d ci remove_job -b ${pcm_branch} https://${pcm_repo}


if [ "${delete_old_job_catalog}" = true ]; then
  python ~/mozart/ops/${var.project}-pcm/job_accountability/create_job_accountability_catalog.py --delete_old_catalog
else
  python ~/mozart/ops/${var.project}-pcm/job_accountability/create_job_accountability_catalog.py
fi

~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json --force

# Set PGE_SIMULATION_MODE to false so we call the PGE Docker image in the PGE jobs
echo "************Turning off PGE_SIMULATION_MODE**************"
sed -i 's/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g' ~/mozart/ops/${var.proejct}-pcm/conf/settings.yaml
fab -f ~/.sds/cluster.py -R mozart,factotum update_opera_packages
sds -d ship

~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-small --desired-capacity 2
#~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-sciflo-l3_dswx_hls --desired-capacity 2
#~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-opera-job_worker-dswx-hls-acct --desired-capacity 2

cd ~/.sds/files/test
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-grq
curl -XDELETE http://${mozart_private_ip}:9200/user_rules-mozart

fab -f ~/.sds/cluster.py -R mozart,grq create_all_user_rules_index
./import_rules.sh

cd ~/.sds/files/test/pge

# Extracting folder names from the test package names
#l0a_test_folder=`basename ${l0a_test_package} .tgz`

# Download test packages
#for i in ${l0a_test_package};
#do
#  if [ ! -f "$i" ];
#  then
#    ~/download_artifact.sh -m "${artifactory_mirror_url}" -b "${artifactory_base_url}" "${artifactory_base_url}/${artifactory_repo}/gov/nasa/jpl/nisar/sds/pge/${pge_test_package}/$i"
#  fi

#  dir_name=`basename $i .tgz`
#  if [ ! -d "$dir_name" ];
#    then
#        mkdir $dir_name
#  fi

#  tar -xzvf $i -C $dir_name
#done

#./stage_pre-req_ancillaries.sh ${isl_bucket}

# Stage radar config and SCLKSCET from L0B test suite package
#cd ${l0b_test_folder}/input

# Now stage these files to the ISL
#aws s3 cp id_01-00-0701_radar-configuration_v45-14.xml s3://${isl_bucket}/
#aws s3 cp id_06-00-0701_chirp-parameter_v45-14.xml s3://${isl_bucket}/
#aws s3 cp id_ff-00-ff01_waveform_v45-14.xml s3://${isl_bucket}/
#aws s3 cp NISAR_198900_SCLKSCET_LRCLK.00003 s3://${isl_bucket}/

# TODO: eventually undo this kludge to handle incorrect filename convention from R2 test dataset
#mv -f NISAR_L0_RRST_VC25_20210707T02000_20210707T021000_D00200_001.bin NISAR_L0_RRST_VC25_20210707T020000_20210707T021000_D00200_001.bin


#cd ~/.sds/files/test/pge

#~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} datasets_e2e_pge.json 1 /tmp/ancillary_datasets.txt

# verify Radar Mode catalog
#python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py radar_mode_catalog 204 /tmp/radar_mode_catalog.txt

# Stage other ancillaries
#./stage_ancillaries.sh ${isl_bucket}

# verify COP and ROST catalog
#python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py cop_catalog 16 /tmp/cop_catalog.txt
#python ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_catalog.py rost_catalog 18 /tmp/rost_catalog.txt

# verify number of ingested ancillary/auxiliary products
#~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} datasets_e2e_pge.json 1,2 /tmp/ancillary_datasets.txt

# Stage test NEN file
#for file in ${l0a_test_folder}/input/*.vc*
#do
#  echo "$file"
#  checksum_value=`openssl md5 -binary $file | base64`
#  aws s3api put-object --bucket ${isl_bucket} --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
#done

# Manually create LDF
#python ~/mozart/ops/nisar-pcm/cluster_provisioning/dev-e2e-pge/create_nisar_ldf_file.py ${isl_bucket} ~/.sds/files/test/pge/${l0a_test_folder}/input

# Stage input L0As, met, and signal for L0B PGE
#for i in ${l0b_test_folder}/input/NISAR_L0_RRST_* ~/mozart/ops/nisar-pcm/tests/pge/l0b/*.met ~/mozart/ops/nisar-pcm/tests/pge/l0b/*.signal; do
#  aws s3 cp $i s3://${isl_bucket}/met_required/
#done

# Update and force submit the Data Take State Config products because
# these test dataset is only one VCIDs
# This is done so that:
# - L0B product is generated from the R2 test dataset inputs
# - set force_submit to True and reingest so that the trigger-Datatake_Accountability trigger rule fires off
#python update_test_datatake_state_config.py dtid_2021188015513_state-config
#python ~/mozart/ops/hysds/scripts/ingest_dataset.py dtid_2021188015513_state-config $HOME/mozart/etc/datasets.json


# Disable network-pair evaluator trigger rule because the end-to-end L1/L2
# test dataset will produce the secondary and reference RSLC products 
# concurrently and this race condition does not guarantee the reference
# RSLC is produced/published before the network-pair evaulator job is
# triggered by the secondary RSLC.
# Later we re-enable this trigger rule and resubmit trigger rule evaluation
# on the secondary RSLC dataset ID after both RSLCs have been created.
#disabled_rules="trigger-Network_Pair_Evaluator"
#for rule in $disabled_rules; do
#  python ~/mozart/ops/pcm_commons/pcm_commons/tools/update_trigger_rules.py $rule --mozart_url https://${mozart_private_ip} --payload '{"enabled": false}'
#done

# Stage test input L0B, met, and signal for RSLC PGE
#for i in ${l2_test_folder}/input/NISAR_L0_PR_RRSD* ~/mozart/ops/nisar-pcm/tests/pge/rslc/*{.met,.signal}; do
#  aws s3 cp $i s3://${isl_bucket}/met_required/
#done


# Restore OnDemandPercentageAboveBaseCapacity back to 0 for GPU instances
#cd ~/.sds/files
#for i in workflow_profiler job_worker-sciflo-rslc job_worker-sciflo-insar; do
#  ~/mozart/ops/nisar-pcm/conf/sds/files/test/update_asg.py ${project}-${venue}-${counter}-nisar-${i} --cli-input-json test/restore_on-demand_base.json
#done
