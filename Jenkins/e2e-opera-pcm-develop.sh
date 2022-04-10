#!/bin/bash

#----------------------
# E2E-opera-pcm-develop
#
# This shell script is intended for use with the Build step of the
# E2E-opera-pcm-develop Jenkins job. It's contents may be copied as-is
# into the Command text field of the "Execute Shell" Build step.
# ---------------------

source /export/home/hysdsops/verdi/bin/activate

# Get the tag from the end of the GIT_BRANCH
BRANCH="${GIT_BRANCH##*/}"

# Get repo path by removing http://*/ and .git from GIT_URL
REPO="${GIT_URL#*:*/}"
REPO="${REPO%.git}"
REPO="${REPO//\//_}"
IMAGE="container-${REPO,,}"

# These should not typically change
project=opera
venue=ci
ops_password=end2endtest

# This user must match the API key assigned to ARTIFACTORY_FN_API_KEY by Jenkins
artifactory_fn_user=collinss

# This user must match the API key assigned to JENKINS_API_KEY by Jenkins
jenkins_api_user=collinss

# Keypair name must correspond to the keypair assigned to PRIVATE_KEY_FILE by Jenkins
# The public part of the key pair must also be assigned to the .ssh/authorized_keys
# file for the hysdsops account on the CI machine.
keypair_name=collinss

# Cluster security group ID must match to the user identified by keypair_name
# (ex: opera-dev-cluster-sg-collinss for keypair collinss)
cluster_security_group_id=sg-037e6de521a3f4854

# Verdi security group ID should be the private group that also corresponds
# to the keypair user (ex: opera-dev-private-verdi-sg-collinss)
verdi_security_group_id=sg-043e273982d5d8094

# Auto-scaling VPC ID must align with the Verdi security group
asg_vpc=vpc-b5a983cd

# Update accordingly as new hysds releases become available
hysds_release=v4.0.1-beta.8-oraclelinux

# These typically do not need to change for nightly builds
product_delivery_branch=develop
lambda_package_release=develop
cnm_r_event_trigger=sqs
pcm_commons_branch=develop

# clean buckets
for i in rs triage lts osl isl; do
  aws s3 rm --recursive s3://opera-dev-${i}-fwd-${venue}/
done

for i in osl; do
  aws s3 rm --recursive s3://opera-dev-${i}-reproc-${venue}/
done

# build dev
cd cluster_provisioning/dev-e2e

echo "Running terraform init"
/home/hysdsops/bin/terraform init -no-color -force-copy

# provision cluster and run end-to-end
echo "Running terraform apply"
/home/hysdsops/bin/terraform apply --var pcm_branch=${BRANCH} \
  --var private_key_file=${PRIVATE_KEY_FILE} --var project=$project \
  --var venue=$venue --var keypair_name=$keypair_name \
  --var artifactory_fn_user=$artifactory_fn_user \
  --var artifactory_fn_api_key=${ARTIFACTORY_FN_API_KEY} \
  --var git_auth_key=${GIT_OAUTH_TOKEN} --var ops_password=$ops_password \
  --var jenkins_api_user=$jenkins_api_user --var jenkins_api_key=${JENKINS_API_KEY} \
  --var cluster_security_group_id=$cluster_security_group_id \
  --var verdi_security_group_id=$verdi_security_group_id \
  --var asg_vpc=$asg_vpc \
  --var hysds_release=$hysds_release \
  --var product_delivery_branch=$product_delivery_branch \
  --var pcm_commons_branch=$pcm_commons_branch \
  --var cnm_r_event_trigger=$cnm_r_event_trigger \
  --var lambda_package_release=$lambda_package_release -no-color -auto-approve || :

# untaint terraform in case it fails
echo "Running terraform untaint"
/home/hysdsops/bin/terraform untaint null_resource.mozart || :

# clean up resources
echo "Running terraform destroy"
/home/hysdsops/bin/terraform destroy --var pcm_branch=${BRANCH} \
  --var private_key_file=${PRIVATE_KEY_FILE} --var project=$project \
  --var venue=$venue --var keypair_name=$keypair_name \
  --var artifactory_fn_user=$artifactory_fn_user \
  --var artifactory_fn_api_key=${ARTIFACTORY_FN_API_KEY} \
  --var git_auth_key=${GIT_OAUTH_TOKEN} --var ops_password=$ops_password \
  --var jenkins_api_user=$jenkins_api_user --var jenkins_api_key=${JENKINS_API_KEY} \
  --var cluster_security_group_id=$cluster_security_group_id \
  --var verdi_security_group_id=$verdi_security_group_id \
  --var asg_vpc=$asg_vpc \
  --var hysds_release=$hysds_release \
  --var product_delivery_branch=$product_delivery_branch \
  --var pcm_commons_branch=$pcm_commons_branch \
  --var cnm_r_event_trigger=$cnm_r_event_trigger \
  --var lambda_package_release=$lambda_package_release -no-color -auto-approve || :

# print out the check_pcm.xml
cat check_pcm.xml
