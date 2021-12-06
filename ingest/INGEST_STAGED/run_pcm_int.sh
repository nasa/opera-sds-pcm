#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env

export OPERA_HOME=$(dirname $(dirname "${BASE_PATH}"))
export COMMONS_HOME=$OPERA_HOME/commons
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running $PGE run_pcm_int.py: " 1>&2
date 1>&2
$BASE_PATH/run_pcm_int.py $* > run_pcm_int.log 2>&1
STATUS=$?
echo -n "Finished running $PGE run_pcm_int.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run $PGE run_pcm_int.py." 1>&2
  cat run_pcm_int.log 1>&2
  echo "{}"
  exit $STATUS
fi
