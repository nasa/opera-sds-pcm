#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env

export OPERA_HOME=$(dirname "${BASE_PATH}")
export COMMONS_HOME=$OPERA_HOME/commons
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

# source environment
source ~/.bash_profile

echo "##########################################" 1>&2
echo -n "Running eval_datatake.py: " 1>&2
date 1>&2
$BASE_PATH/eval_datatake.py $* > eval_datatake.log 2>&1
STATUS=$?
echo -n "Finished running eval_datatake.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run eval_datatake.py." 1>&2
  cat eval_datatake.log 1>&2
  echo "{}"
  exit $STATUS
fi
