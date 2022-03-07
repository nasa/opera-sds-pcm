#!/bin/bash

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

ISL_BUCKET_NAME=$1
STAGING_AREA=$2

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to subscribe to data"
date
echo "Running python code: daac_data_subscriber.py"
# Forward processing use case; query previous 60 minutes
echo "python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSL30 -s $ISL_BUCKET_NAME -e L30 > subscriber.log"
python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSL30 -s $ISL_BUCKET_NAME -e L30 > subscriber.log

echo "python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSS30 -s $ISL_BUCKET_NAME -e S30 > subscriber.log"
python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSS30 -s $ISL_BUCKET_NAME -e S30 > subscriber.log

STATUS=$?
echo "Finished running daac_data_subscriber.py"
date
if [ $STATUS -ne 0 ]; then
  echo "Failed to run daac_data_subscriber.py" 1>&2
  cat daac_data_subscriber.log 1>&2
  echo "{}"
  exit $STATUS
fi
