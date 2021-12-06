#!/bin/bash
# Stage SCKLSCET file
aws s3 cp ~/mozart/ops/opera-pcm/tests/time_corr/test-files/NISAR_198900_SCLKSCET_LRCLK.00004 s3://$1/gds/

# Stage SROST files
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023015-c001-d15-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023016-c001-d16-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1a-0100_srost-2023022-c001-d22-v01.xml s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/id_00-1b-0100_srost-2022008-c001-d08-v01.xml s3://$1/gds/
#pushd ~/mozart/ops/opera-pcm/tests/rost/test-files/srost
#for i in ~/mozart/ops/opera-pcm/tests/rost/test-files/srost/*; do
#  if [ -f "$i" ]
#  then
#    aws s3 cp $i s3://$1/gds/
#  fi
#done
#popd

# Stage OFS files
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023015-c001-d15-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023016-c001-d16-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1a-0100_srost-2023022-c001-d22-v01.tsv s3://$1/gds/
aws s3 cp ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/ofs_00-1b-0100_srost-2022008-c001-d08-v01.tsv s3://$1/gds/
#pushd ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs
#for i in ~/mozart/ops/opera-pcm/tests/rost/test-files/ofs/*; do
#  if [ -f "$i" ]
#  then
#    aws s3 cp $i s3://$1/gds/
#  fi
#done
#popd
