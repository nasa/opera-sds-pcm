#!/bin/bash

project=$1
venue=$2


echo "deleting objects in s3://${project}-dev-cc-fwd-${venue}"
aws s3 rm  --recursive s3://${project}-dev-cc-fwd-${venue} ||:

echo "deleting objects in s3://${project}-dev-isl-fwd-${venue}"
aws s3 rm  --recursive s3://${project}-dev-isl-fwd-${venue} ||:

echo "deleting objects in s3://${project}-dev-lts-fwd-${venue}"
aws s3 rm  --recursive s3://${project}-dev-lts-fwd-${venue} ||:

echo "deleting objects in s3://${project}-dev-osl-fwd-${venue}"
aws s3 rm  --recursive s3://${project}-dev-osl-fwd-${venue} ||:

echo "deleting objects in s3://${project}-dev-rs-fwd-${venue}"
aws s3 rm  --recursive s3://${project}-dev-rs-fwd-${venue} ||:
