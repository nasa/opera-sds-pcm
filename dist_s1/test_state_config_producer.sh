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
echo "Running job to test state config production"
date

python $OPERA_HOME/dist_s1/state_config_producer.py $* > run_job.log 2>&1

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run state_config_producer.py"
  date
  exit 1
fi
