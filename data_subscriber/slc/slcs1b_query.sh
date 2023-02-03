#!/bin/bash

echo "args: $*"

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
echo "Running job to query ASFDAAC SLCS1B data"
date

# Forward processing use case; query previous 60 minutes
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -p ASF -c SENTINEL-1B_SLC $* 2>&1"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -p ASF -c SENTINEL-1B_SLC $* > run_slcs1b_query.log 2>&1

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run daac_data_subscriber.py"
  date
  exit 1
fi

