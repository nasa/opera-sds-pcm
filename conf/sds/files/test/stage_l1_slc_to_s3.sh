#!/bin/bash

# ingest SLC SAFE file(s) to exercise L2_CSLC_S1 PGE
for file in ~/mozart/ops/opera-pcm/tests/L2_CSLC_S1_PGE/test-files/*SLC*.zip
do
  echo "$file"
  aws s3 cp $file s3://$1/
done
