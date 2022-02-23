#!/bin/bash
#################
# This is a wrapper script for the job that is responsible for creating
# an Accountability Report.
#################

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

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
python $BASE_PATH/daac_data_subscriber.py -m 60 -c HLSS30 -s "opera-dev-isl-fwd-mplotkin" --extensions ".tif" > daac_data_subscriber.log 2>&1
# Demo use case; query specific static range
# python $BASE_PATH/daac_data_subscriber.py -c HLSS30 -s "opera-dev-isl-fwd-mplotkin" --extensions ".tif" -sd 2018-12-31T00:00:00Z -ed 2019-01-01T00:00:00Z > daac_data_subscriber.log 2>&1
STATUS=$?
echo "Finished running daac_data_subscriber.py"
date
if [ $STATUS -ne 0 ]; then
  echo "Failed to run daac_data_subscriber.py" 1>&2
  cat daac_data_subscriber.log 1>&2
  echo "{}"
  exit $STATUS
fi
