#!/bin/bash
set -ex

# check args
if [ "$#" -eq 3 ]; then
  SMALL_ASG_NAME=$1
  CNM_ASG_NAME=$2
  ASG_MAX=$3
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ~/mozart/bin/activate

aws autoscaling update-auto-scaling-group --auto-scaling-group-name ${SMALL_ASG_NAME} --max-size ${ASG_MAX}
aws autoscaling update-auto-scaling-group --auto-scaling-group-name ${CNM_ASG_NAME} --max-size ${ASG_MAX}
