#!/usr/bin/env bash

# upload vc and qac files from opera-testdata bucket to isl bucket
python ~/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-nen/copy_vc_qac_files.py $1
echo "copied .vc and .qac files from s3://opera-sds-testdata/LSAR/nen/2020/168 to s3://$1/tlm"

# generating LDF file and uploading it to ilm bucket, tlm subdirectory
python ~/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-nen/create_nsar_nen_ldf_file.py $1
