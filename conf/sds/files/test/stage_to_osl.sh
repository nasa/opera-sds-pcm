#!/bin/sh

BUCKET=$1
KEY=$2
for i in pass_thru/*; do
  CHECKSUM=( $(md5sum $i | cut -d ' ' -f 1) )
  aws s3 cp $i s3://${BUCKET}/${KEY}/ --metadata md5_checksum=${CHECKSUM}
done

