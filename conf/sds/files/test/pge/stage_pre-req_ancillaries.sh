#!/bin/bash

echo "**************** Copying SROST files input to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-11-0100_srost-2023002-c005-d02-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023015-c001-d15-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023016-c001-d16-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023022-c001-d22-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1b-0100_srost-2020081-c001-d81-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1b-0100_srost-2020275-c005-d60-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-0a-0100_srost-2021188-c001-d01-v01.xml s3://$1/gds/

echo "**************** Copying OFS files input to ISL ****************"
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-11-0100_srost-2023002-c005-d02-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023015-c001-d15-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023016-c001-d16-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023022-c001-d22-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1b-0100_srost-2020275-c005-d60-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1b-0100_srost-2020081-c001-d81-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-0a-0100_srost-2021188-c001-d01-v01.tsv s3://$1/gds/

# Commenting out since we want to use the SCLKSCET provided by the PGE provided test data

# echo "**************** Copying SCLKSCET to ISL ****************"
# aws s3 cp $HOME/mozart/ops/opera-pcm/tests/time_corr/test-files/NISAR_198900_SCLKSCET_LRCLK.00004 s3://$1/gds/
