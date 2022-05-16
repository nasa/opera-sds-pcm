#!/bin/bash

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

MINUTES=$1
DOWNLOAD_JOB_RELEASE="${2:=issue_85}"
DOWNLOAD_JOB_QUEUE="${3}"
CHUNK_SIZE="${4}"
CHUNK_SIZE="${CHUNK_SIZE:=2}"

SMOKE_RUN="${5:=false}"
if [ $SMOKE_RUN = "true" ]; then
  SMOKE_RUN="--smoke-run"
else
  SMOKE_RUN=""
fi

DRY_RUN="${6:=false}"
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
echo "Running job to query LPDAAC HLSL30 data"
date

# Forward processing use case; query previous 60 minutes
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -m $MINUTES -c HLSL30"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query \
-m $MINUTES \
-c HLSL30 \
--release-version=$DOWNLOAD_JOB_RELEASE \
--job-queue=$DOWNLOAD_JOB_QUEUE \
--chunk-size=$CHUNK_SIZE \
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

