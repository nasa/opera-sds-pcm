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

source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to submit pending jobs that are ready to be run"
date

python $OPERA_HOME/data_subscriber/submit_pending_jobs.py $* > run_submit_pending_jobs.log 2>&1

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run submit_pending_jobs.py"
  date
  exit 1
fi
