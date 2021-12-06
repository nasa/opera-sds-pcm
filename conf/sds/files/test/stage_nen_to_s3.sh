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

# ingest RRSTs for Pass 2
for file in ~/mozart/ops/opera-pcm/tests/rrst/pass2/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

# ingest LDF for Pass 2
for file in ~/mozart/ops/opera-pcm/tests/ldf/pass2/*
do
  echo "$file"
  aws s3 cp $file s3://$1/ldf/
done

# ingest ARP for Pass 2
aws s3 cp ~/mozart/ops/opera-pcm/tests/arp/WFF_NISAR_2022_008_07_30_59.arp s3://$1/arp/

# ingest RRSTs for Pass 3
for file in ~/mozart/ops/opera-pcm/tests/rrst/pass3/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

# ingest LDF for Pass 3
for file in ~/mozart/ops/opera-pcm/tests/ldf/pass3/*
do
  echo "$file"
  aws s3 cp $file s3://$1/ldf/
done

# ingest ARP for Pass 3
aws s3 cp ~/mozart/ops/opera-pcm/tests/arp/SGS_NISAR_2022_008_08_30_59.arp s3://$1/arp/

# ingest RRSTs for Pass 4
for file in ~/mozart/ops/opera-pcm/tests/rrst/pass4/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

# ingest LDF for Pass 4
for file in ~/mozart/ops/opera-pcm/tests/ldf/pass4/*
do
  echo "$file"
  aws s3 cp $file s3://$1/ldf/
done

# ingest ARP for Pass 4
aws s3 cp ~/mozart/ops/opera-pcm/tests/arp/PA_NISAR_2022_008_09_30_59.arp s3://$1/arp/

# ingest RRSTs for Pass 5
for file in ~/mozart/ops/opera-pcm/tests/rrst/pass5/*
do
  echo "$file"
  checksum_value=`openssl md5 -binary $file | base64`
  aws s3api put-object --bucket $1 --key tlm/${file##*/} --body $file --content-md5 $checksum_value --metadata md5checksum=$checksum_value
done

# ingest LDF for Pass 5
for file in ~/mozart/ops/opera-pcm/tests/ldf/pass5/*
do
  echo "$file"
  aws s3 cp $file s3://$1/ldf/
done
# ingest ARP for Pass 5
aws s3 cp ~/mozart/ops/opera-pcm/tests/arp/WFF_NISAR_2022_008_17_13_20.arp s3://$1/arp/
