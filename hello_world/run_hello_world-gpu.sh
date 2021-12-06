#!/bin/bash
echo "Hello World, on GPUs!"
echo "Hello World, on GPUs! to STDERR" 1>&2

# show GPUs
/usr/bin/nvidia-smi
if [ "$?" -ne 0 ]; then
  echo "Failed to run nvidia-smi."
  exit 1
fi

# generate dataset ID
timestamp=$(date -u +%Y%m%dT%H%M%S.%NZ)
hash=$(echo $timestamp | sha224sum | cut -c1-5)
id=hello_world-product-${timestamp}-${hash}
echo "dataset ID: $id"

# create dataset directory
mkdir $id

# create fake data
fake_data_file=${id}/fake_data.dat
dd if=/dev/urandom of=$fake_data_file bs=1M count=5

# create minimal dataset JSON file
dataset_json_file=${id}/${id}.dataset.json
echo "{\"version\": \"v1.0\"}" > $dataset_json_file

# create minimal metadata file
metadata_json_file=${id}/${id}.met.json
echo "{}" > $metadata_json_file
