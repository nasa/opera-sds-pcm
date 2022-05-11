#!/bin/bash

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

ISL_BUCKET_NAME=$1
STAGING_AREA=$2
TILE_IDS="${3}"

SMOKE_RUN="${4:=false}"
if [ $SMOKE_RUN = "true" ]; then
  SMOKE_RUN="--smoke-run"
else
  SMOKE_RUN=""
fi

DRY_RUN="${5:=false}"
if [ DRY_RUN = "true" ]; then
  DRY_RUN="--dry-run"
else
  DRY_RUN=""
fi

# TODO chrisjrd: remove after testing
exit 1

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to download data"
date

# Forward processing use case; download all undownloaded files from ES index
echo "python $BASE_PATH/daac_data_subscriber.py -c ALL -s $ISL_BUCKET_NAME --index-mode download"
python $BASE_PATH/daac_data_subscriber.py \
-c ALL \
-s $ISL_BUCKET_NAME \
--index-mode=download \
--tile-ids $TILE_IDS \
$SMOKE_RUN \
$DRY_RUN \
2>&1
STATUS=$?

if [ $STATUS -eq 0 ]; then
  echo "Finished running job"
else
  echo "Failed to run daac_data_subscriber.py"
fi

date
exit $STATUS
