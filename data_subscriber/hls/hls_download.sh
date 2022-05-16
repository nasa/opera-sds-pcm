#!/bin/bash

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

ISL_BUCKET_NAME=$1
TILE_IDS="${2}"

SMOKE_RUN="${3:=false}"
if [ $SMOKE_RUN = "true" ]; then
  SMOKE_RUN="--smoke-run"
else
  SMOKE_RUN=""
fi

DRY_RUN="${4:=false}"
if [ $DRY_RUN = "true" ]; then
  DRY_RUN="--dry-run"
else
  DRY_RUN=""
fi

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to download LPDAAC HLS data"
date

# Forward processing use case; download all undownloaded files from ES index
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py download -s $ISL_BUCKET_NAME"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py download \
-s $ISL_BUCKET_NAME \
--tile-ids $TILE_IDS \
$SMOKE_RUN \
$DRY_RUN \
2>&1

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run daac_data_subscriber.py"
  date
  exit 1
fi
