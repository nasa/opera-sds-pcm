#!/bin/bash

set -e

if [ "$#" -eq 1 ]; then
  CC_BUCKET=$1
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  echo "Must provide only the name of the CC bucket to deploy to" 1>&2
  exit 1
fi

IMAGE_TAG='disp-s1-bperp-update:latest'
TARBALL_NAME='product_update_disp-s1_bperp-latest'

echo '
=====================================

Building BPerp Update docker image...

=====================================
'

# Remove the old Docker image, if it exists
EXISTING_IMAGE_ID=$(docker images -q "${IMAGE_TAG}")
if [[ ! -z ${EXISTING_IMAGE_ID} ]]; then
  docker rmi ${EXISTING_IMAGE_ID}
fi

docker build . -t "${IMAGE_TAG}"

echo '
=====================================

Saving docker image to tarball...

=====================================
'

docker save "${IMAGE_TAG}" -o "${TARBALL_NAME}.tar"
gzip -fv "${TARBALL_NAME}.tar"


echo '
=====================================

Pushing image tarball to S3...

=====================================
'

aws s3 cp "${TARBALL_NAME}.tar.gz" "s3://${CC_BUCKET}/"
rm "${TARBALL_NAME}.tar.gz"
