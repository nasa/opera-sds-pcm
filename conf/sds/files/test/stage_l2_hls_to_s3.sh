#!/bin/bash

# ingest HLSL30 to exercise L3_DSWx_HLS
for file in ~/mozart/ops/opera-pcm/tests/L3_DSWx_HLS_PGE/test-files/l30_greenland/*
do
  echo "$file"
  #checksum_value=`openssl md5 -binary $file | base64`
  aws s3 cp $file s3://$1/
done


# ingest HLSS30 
for file in  ~/mozart/ops/opera-pcm/tests/L3_DSWx_HLS_PGE/test-files/s30_louisiana/*
do
  echo "$file"
  aws s3 cp $file s3://$1/
done

