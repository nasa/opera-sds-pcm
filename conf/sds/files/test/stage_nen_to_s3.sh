#!/bin/bash

# ingest RRSTs and LDF to exercise L0A timer
for file in ~/mozart/ops/opera-pcm/tests/rrst/partial/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

for file in ~/mozart/ops/opera-pcm/tests/ldf/partial/*
do
  echo "$file"
  aws s3 cp $file s3://$1/ldf/
done

# ingest RRSTs for Pass 1
for file in ~/mozart/ops/opera-pcm/tests/rrst/pass1/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

# ingest LDF for Pass 1
for file in ~/mozart/ops/opera-pcm/tests/ldf/pass1/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3 cp $file s3://$1/ldf/
done
# ingest ARP for Pass 1
aws s3 cp ~/mozart/ops/opera-pcm/tests/arp/ASF_NISAR_2022_008_06_30_59.arp s3://$1/arp/
