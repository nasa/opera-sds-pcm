#!/bin/bash

set -e

if [ "$#" -eq 1 ]; then
  CC_BUCKET=$1
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  echo "Must provide only the name of the CC bucket to deploy to" 1>&2
  exit 1
fi

echo '
=====================================

Building BPerp Update docker image...

=====================================
'

docker build . -t disp-s1-bperp-update:latest

echo '
=====================================

Saving docker image to tarball...

=====================================
'

docker save disp-s1-bperp-update:latest -o product_update_disp-s1_bperp-latest.tar
gzip -fv product_update_disp-s1_bperp-latest.tar


echo '
=====================================

Pushing image tarball to S3...

=====================================
'

aws s3 cp product_update_disp-s1_bperp-latest.tar.gz "s3://${CC_BUCKET}/"
rm product_update_disp-s1_bperp-latest.tar.gz
