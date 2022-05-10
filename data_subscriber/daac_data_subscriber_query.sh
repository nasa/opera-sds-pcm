#!/bin/bash

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

ISL_BUCKET_NAME=$1
STAGING_AREA=$2
CHUNK_SIZE="${3}"
CHUNK_SIZE="${CHUNK_SIZE:=2}"
JOB_RELEASE="${4:=issue_85}"

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to query LPDAAC data"
date

# Forward processing use case; query previous 60 minutes
echo "python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSL30 -s $ISL_BUCKET_NAME -e L30 --index-mode query"
python $BASE_PATH/daac_data_subscriber.py \
-m 60 \
-c HLSL30 \
-s $ISL_BUCKET_NAME \
-e L30 \
--index-mode query \
--chunk-size=$CHUNK_SIZE \
--release-version=$JOB_RELEASE \
--smoke-run \
2>&1
STATUS_L30=$?

echo "python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSS30 -s $ISL_BUCKET_NAME -e S30 --index-mode query"
python $BASE_PATH/daac_data_subscriber.py \
-m 60 \
-c HLSS30 \
-s $ISL_BUCKET_NAME \
-e S30 \
--index-mode query \
--chunk-size=$CHUNK_SIZE \
--release-version=$JOB_RELEASE \
--smoke-run \
2>&1
STATUS_S30=$?

if [ $STATUS_S30 -eq 0 ] && [ $STATUS_L30 -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run daac_data_subscriber.py"
  date
  exit 1
fi

