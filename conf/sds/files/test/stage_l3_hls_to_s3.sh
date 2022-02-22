#!/bin/bash

$ ingest L3_DSWx_HLS_PGE outputs
for file in ~/mozart/ops/opera-pcm/tests/L3_DSWx_HLS_PGE/test-files/expected_outputs/*
do
  echo "$file"
  aws s3 cp $file s3://$1/
done

