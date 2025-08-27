#!/bin/bash
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-pge-DISP_S1_hist"

# check args
if [ "$#" -eq 1 ]; then
  config_file=${1}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ${config_file}

# fail on any errors
set -ex

cd ~/.sds/files

# backup settings.yaml
cp ~/mozart/ops/opera-pcm/conf/settings.yaml ~/mozart/ops/opera-pcm/conf/settings.yaml.bak

# disable simulation mode
sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ~/mozart/ops/opera-pcm/conf/settings.yaml

# propagate settings change
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

# test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json

# verify number of datasets
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json all --max_time 600 /tmp/datasets.txt
