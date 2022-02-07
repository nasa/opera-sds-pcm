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

echo "##########################################" 1>&2
echo "Installing dependencies"
pip install .
echo "Running job to subscribe to data"
date 1>&2
echo "Running python code: daac_data_subscriber.py"
python $BASE_PATH/daac_data_subscriber.py -m 10 -c HLSS30 -s "opera-dev-isl-fwd-mplotkin" --extensions ".tif" > daac_data_subscriber.log 2>&1
STATUS=$?
echo "Finished running daac_data_subscriber.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run daac_data_subscriber.py" 1>&2
  cat daac_data_subscriber.log 1>&2
  echo "{}"
  exit $STATUS
fi
