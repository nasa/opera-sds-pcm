#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running ROST PGE"
date 1>&2
echo -n "Running python code: rost_pge.py"
python $BASE_PATH/rost_pge.py $* > run_rost_pge.log
STATUS=$?
echo -n "Finished running run_rost_pge.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run run_rost_pge.py" 1>&2
  cat run_rost_pge.log 1>&2
  echo "{}"
  exit $STATUS
fi
