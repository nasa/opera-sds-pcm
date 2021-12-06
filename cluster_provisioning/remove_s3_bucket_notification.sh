#!/bin/bash

# check args
if [ "$#" -eq 1 ]; then
  BUCKET_NAME=$1
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# fail on any errors
set -ex

# put empty bucket notification
aws s3api put-bucket-notification-configuration --bucket=${BUCKET_NAME} --notification-configuration="{}" ||:
