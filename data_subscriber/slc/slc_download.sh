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
echo "Running job to download ASFDAAC SLC data"
date

# Forward processing use case; download all undownloaded files from ES index
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py download -p ASF $* 2>&1"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py download -p ASF $* > run_slc_download.log 2>&1

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run daac_data_subscriber.py"
  date
  exit 1
fi
