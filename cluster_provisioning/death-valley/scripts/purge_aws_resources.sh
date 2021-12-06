#!/bin/bash

# check args
if [ "$#" -eq 9 ]; then
  CODE_BUCKET=$1
  DATASET_BUCKET=$2
  TRIAGE_BUCKET=$3
  LTS_BUCKET=$4
  SMALL_ASG_NAME=$5
  LARGE_ASG_NAME=$6
  CNM_ASG_NAME=$7
  VENUE=$8
  CNM_R_EVENT_TYPE=$9
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

source ~/mozart/bin/activate

# Purge bucket contents but don't delete tfstate
aws s3 rm --recursive --exclude "death-valley-tfstate/*" s3://${CODE_BUCKET} ||:
aws s3 rm --recursive s3://${DATASET_BUCKET} ||:
aws s3 rm --recursive s3://${TRIAGE_BUCKET} ||:
aws s3 rm --recursive s3://${LTS_BUCKET} ||:

# get launch configuration name or launch template id
small_lc=$(aws autoscaling describe-auto-scaling-groups --no-paginate --output text --query AutoScalingGroups[?AutoScalingGroupName==\'${SMALL_ASG_NAME}\'].LaunchConfigurationName)
large_lc=$(aws autoscaling describe-auto-scaling-groups --no-paginate --output text --query AutoScalingGroups[?AutoScalingGroupName==\'${LARGE_ASG_NAME}\'].LaunchConfigurationName)
cnm_lc=$(aws autoscaling describe-auto-scaling-groups --no-paginate --output text --query AutoScalingGroups[?AutoScalingGroupName==\'${CNM_ASG_NAME}\'].LaunchConfigurationName)

#small_lt=$(aws autoscaling describe-auto-scaling-groups --no-paginate --output text --query AutoScalingGroups[?AutoScalingGroupName==\'${SMALL_ASG_NAME}\'].LaunchTemplate.LaunchTemplateId)
#large_lt=$(aws autoscaling describe-auto-scaling-groups --no-paginate --output text --query AutoScalingGroups[?AutoScalingGroupName==\'${LARGE_ASG_NAME}\'].LaunchTemplate.LaunchTemplateId)
# TODO Above commands won't work until aws cli is upgraded past v1.14. Once the aws cli is updated, can use the commands above to lookup the launch template name.
# For now, 'hard-coding' the name of the launch templates
small_lt="${SMALL_ASG_NAME}-launch-template"
large_lt="${LARGE_ASG_NAME}-launch-template"
cnm_lt="${CNM_ASG_NAME}-launch-template"


# delete ASGs
aws autoscaling delete-auto-scaling-group --auto-scaling-group-name ${SMALL_ASG_NAME} --force-delete
aws autoscaling delete-auto-scaling-group --auto-scaling-group-name ${LARGE_ASG_NAME} --force-delete
aws autoscaling delete-auto-scaling-group --auto-scaling-group-name ${CNM_ASG_NAME} --force-delete

# delete launch configs if they exist

aws autoscaling delete-launch-configuration --launch-configuration-name ${small_lc} || true # Don't exit script if command fails
aws autoscaling delete-launch-configuration --launch-configuration-name ${large_lc} || true # Don't exit script if command fails
aws autoscaling delete-launch-configuration --launch-configuration-name ${cnm_lc} || true # Don't exit script if command fails

# delete launch templates if they exist
aws ec2 delete-launch-template --launch-template-name ${small_lt} || true # Don't exit script if command fails
aws ec2 delete-launch-template --launch-template-name ${large_lt} || true # Don't exit script if command fails
aws ec2 delete-launch-template --launch-template-name ${cnm_lt} || true # Don't exit script if command fails

# delete topics
#aws sns delete-topic --topic-arn $(aws sns list-topics | grep ${VENUE}- | cut -d'"' -f4) || true

# delete lambda
#aws lambda delete-function --function-name $(aws lambda list-functions | grep FunctionArn | grep ${VENUE}- | cut -d'"' -f4) || true

# delete Kinesis Stream
#if [ "${CNM_R_EVENT_TYPE}" == "kinesis" ]; then
#    KINESIS_STREAM_ARN=$(aws kinesis list-streams | grep ${VENUE}- | xargs | cut -d ',' -f1)
#    python /export/home/hysdsops/mozart/ops/opera-pcm/cluster_provisioning/delete_event_source_mapping.py ${KINESIS_STREAM_ARN}
#    aws kinesis delete-stream --stream-name ${KINESIS_STREAM_ARN}
#fi
