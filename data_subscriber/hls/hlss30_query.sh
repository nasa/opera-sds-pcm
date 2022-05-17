#!/bin/bash

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

START_DATETIME=$1
END_DATETIME=$2
PROVIDER=$3

if [ ! -z "$4" ]; then
  BOUNDING_BOX="-b ${4}"
fi
DOWNLOAD_JOB_RELEASE="${5:=issue_85}"
DOWNLOAD_JOB_QUEUE="${6}"
ISL_BUCKET_NAME="${7}"
CHUNK_SIZE="${8}"
CHUNK_SIZE="${CHUNK_SIZE:=2}"

SMOKE_RUN="${9:=false}"
if [ $SMOKE_RUN = "true" ]; then
  SMOKE_RUN="--smoke-run"
else
  SMOKE_RUN=""
fi

DRY_RUN="${10:=false}"
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
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -s $START_DATETIME -e $END_DATETIME -p $PROVIDER -c HLSS30 $BOUNDING_BOX"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query \
-s $START_DATETIME \
-e $END_DATETIME \
-p $PROVIDER \
-c HLSS30 \
$BOUNDING_BOX \
--release-version=$DOWNLOAD_JOB_RELEASE \
--job-queue=$DOWNLOAD_JOB_QUEUE \
--isl-bucket=$ISL_BUCKET_NAME \
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

