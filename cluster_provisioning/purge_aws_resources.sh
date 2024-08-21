#!/bin/bash

# check args
if [ "$#" -eq 4 ]; then
  CODE_BUCKET=$1
  DATASET_BUCKET=$2
  TRIAGE_BUCKET=$3
  OSL_BUCKET=$4
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

# purge bucket contents
aws s3 rm --recursive s3://${CODE_BUCKET} ||:
aws s3 rm --recursive s3://${DATASET_BUCKET} ||:
aws s3 rm --recursive s3://${TRIAGE_BUCKET} ||:
aws s3 rm --recursive s3://${OSL_BUCKET} ||:
