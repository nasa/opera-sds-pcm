#!/bin/bash
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
echo -n "Running job to check for expired state config datasets and force submit"
date 1>&2
echo -n "Running python code: timer_handler.py"
python $BASE_PATH/timer_handler.py > timer_handler.log 2>&1
STATUS=$?
echo -n "Finished running timer_handler.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run timer_handler.py" 1>&2
  cat timer_handler.log 1>&2
  echo "{}"
  exit $STATUS
fi
