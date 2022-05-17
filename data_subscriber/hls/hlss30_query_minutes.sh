#!/bin/bash

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

MINUTES=$1
PROVIDER=$2

if [ ! -z "$3" ]; then
  BOUNDING_BOX="-b ${3}"
fi
DOWNLOAD_JOB_RELEASE="${4:=issue_85}"
DOWNLOAD_JOB_QUEUE="${5}"
ISL_BUCKET_NAME="${6}"
CHUNK_SIZE="${7}"
CHUNK_SIZE="${CHUNK_SIZE:=2}"

SMOKE_RUN="${8:=false}"
if [ $SMOKE_RUN = "true" ]; then
  SMOKE_RUN="--smoke-run"
else
  SMOKE_RUN=""
fi

DRY_RUN="${9:=false}"
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
echo "Running job to query LPDAAC HLSS30 data"
date

# Forward processing use case; query previous 60 minutes
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -m $MINUTES -p $PROVIDER -c HLSS30 $BOUNDING_BOX"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query \
-m $MINUTES \
-p $PROVIDER \
-c HLSS30 \
$BOUNDING_BOX \
-c HLSL30 \
--release-version=$DOWNLOAD_JOB_RELEASE \
--job-queue=$DOWNLOAD_JOB_QUEUE \
--s3bucket=$ISL_BUCKET_NAME \
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

